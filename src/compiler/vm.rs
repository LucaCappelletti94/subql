//! Stack-based VM interpreting [`BytecodeProgram`] against a CDC event.
//!
//! # Type shape
//!
//! [`Vm`] is parameterised on the observed [`Backend`] `B`; the stack holds
//! `Value<B>` and `Tri` slots via `StackValue`. Evaluation is generic over
//! the concrete event: [`Vm::eval`] takes any `E: CdcEvent<Backend = B>`
//! and a [`RowKind`] selecting which row view to read column loads from.
//!
//! # Contract
//!
//! * A compiled program is Backend-scoped and reusable across every
//!   `E: CdcEvent<Backend = B>`.
//! * The final instruction of a well-formed program leaves exactly one
//!   `StackValue::Tri` on the stack, or exactly one
//!   `StackValue::Value` carrying `Value::Null` / `Value::Missing` (both
//!   lift to `Tri::Unknown`). Any other final shape is a compiler bug and
//!   surfaces as [`VmError::MalformedProgram`].
//! * Same-scalar arithmetic only. Cross-scalar operands, or `Missing` /
//!   `Null` operands, collapse to `Value::Null`.
//! * A `LoadColumn` instruction reads its cell through
//!   [`CdcEvent::value_at`], which decodes it against the catalog and
//!   returns an owned [`Value`]. Boolean predicates on a bare column
//!   MUST be lowered by the compiler as an explicit comparison
//!   (`LoadColumn(col)` + `PushLiteral(Bool(true))` + `Equal`)
//!   because the VM does not lift a bare `Value::Bool` on the stack to
//!   `Tri` (that lift is backend-specific: Postgres `Bool = bool`, SQLite
//!   `Bool = i64`).

use super::{
    value_cmp::{compare_ordered_values, values_equal},
    BytecodeProgram, Instruction, Tri,
};
use crate::backend::{Backend, CdcEvent, RowKind, Value};
use alloc::vec::Vec;
use sql_traits::prelude::DatabaseLike;

/// VM evaluation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmError {
    /// Popped from an empty stack.
    StackUnderflow,

    /// Expected one shape (`"Value"` / `"Tri"`) at the stack top, found
    /// another. Indicates a compiler bug that emitted a program whose
    /// stack shape does not match the instruction sequence.
    TypeMismatch {
        /// What the instruction expected at TOS.
        expected: &'static str,
        /// What was actually there.
        got: &'static str,
    },

    /// Column index out of range for the observed event / schema. Currently
    /// unused (`LoadColumn` never emits this: an out-of-range column reads
    /// as `Value::Missing`), but kept for future use.
    InvalidColumnIndex(u16),

    /// Jump offset is invalid: either zero (no forward progress) or lands
    /// past the end of the program.
    BadJump(usize),

    /// Program terminated with a stack shape not reducible to a single
    /// `Tri`. Compiler bug.
    MalformedProgram,

    /// A [`Instruction::TermTruth`] named a slot the caller supplied no truth
    /// for. The caller evaluates once per assignment over the program's term
    /// slots, so a slot outside that vector means the assignment and the
    /// program disagree about how many terms the filter has.
    MissingTermTruth(u16),

    /// A carried cell could not be decoded to its declared type, surfaced
    /// from [`crate::backend::CdcEvent::value_at`].
    Value(crate::ValueError),
}

/// Slot on the VM's evaluation stack.
///
/// `Value(_)` variants come from `PushLiteral`, `LoadColumn`, and the
/// arithmetic instructions. `Tri(_)` variants come from comparison,
/// null-check, and logical instructions.
enum StackValue<B: Backend> {
    /// Scalar value (from literals, column loads, or arithmetic results).
    Value(Value<B>),
    /// Tri-state boolean (from comparisons, null checks, or logical ops).
    Tri(Tri),
}

// `Clone`, `Debug`, and `PartialEq` are hand-implemented for the same
// reason as `Value<B>`: `#[derive(...)]` would defensively require
// `B: Clone` etc., which is not implied by `Backend`.

impl<B: Backend> Clone for StackValue<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Value(v) => Self::Value(v.clone()),
            Self::Tri(t) => Self::Tri(*t),
        }
    }
}

impl<B: Backend> core::fmt::Debug for StackValue<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Value(v) => f.debug_tuple("Value").field(v).finish(),
            Self::Tri(t) => f.debug_tuple("Tri").field(t).finish(),
        }
    }
}

impl<B: Backend> PartialEq for StackValue<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Value(a), Self::Value(b)) => a == b,
            (Self::Tri(a), Self::Tri(b)) => a == b,
            _ => false,
        }
    }
}

/// Stack-based VM for predicate evaluation.
///
/// A single `Vm` instance is reusable across events: [`Vm::eval`] clears
/// the stack at entry and rebuilds it per program. Callers hold one `Vm`
/// per worker thread and evaluate every incoming event through it.
pub struct Vm<B: Backend> {
    /// Value stack (grows during evaluation).
    stack: Vec<StackValue<B>>,
}

impl<B: Backend> Vm<B> {
    /// Construct a fresh VM instance.
    ///
    /// The stack is pre-allocated for the common case (most predicates
    /// touch fewer than 16 slots).
    #[must_use]
    pub fn new() -> Self {
        Self {
            stack: Vec::with_capacity(16),
        }
    }

    /// Evaluate `program` against `event`, reading column loads from the
    /// `row` view of the event.
    ///
    /// Returns [`Tri::True`] when the predicate holds, [`Tri::False`] when
    /// it does not, [`Tri::Unknown`] when SQL three-valued logic collapses
    /// the result (`NULL` / `Missing` operands, NaN, cross-scalar
    /// comparisons).
    ///
    /// # Errors
    ///
    /// Returns [`VmError`] variants only on malformed bytecode. A
    /// well-formed program never errors here.
    pub fn eval<E, DB>(
        &mut self,
        program: &BytecodeProgram<B>,
        event: &E,
        row: RowKind,
        db: &DB,
    ) -> Result<Tri, VmError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        self.eval_with_terms(program, event, row, db, &[])
    }

    /// Evaluate `program` with one truth per membership term slot.
    ///
    /// A membership term answers differently for different subscribers, so its
    /// truth cannot be computed from the row. The caller enumerates the
    /// assignments over the program's `term_columns` and evaluates once per
    /// assignment, taking the union of the subscriber sets the accepting
    /// assignments describe.
    ///
    /// # Errors
    ///
    /// As [`Vm::eval`], plus [`VmError::MissingTermTruth`] when the program
    /// names a slot outside `truths`.
    pub fn eval_with_terms<E, DB>(
        &mut self,
        program: &BytecodeProgram<B>,
        event: &E,
        row: RowKind,
        db: &DB,
        truths: &[Tri],
    ) -> Result<Tri, VmError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        self.stack.clear();

        let instructions = &program.instructions;
        let len = instructions.len();
        let mut ip = 0;

        // Execute instructions with explicit instruction pointer (supports jumps).
        while ip < len {
            match &instructions[ip] {
                Instruction::JumpIfFalse(offset) => {
                    let top = self.peek_tri()?;
                    if top == Tri::False {
                        if *offset == 0 {
                            return Err(VmError::BadJump(ip));
                        }
                        let new_ip = ip.saturating_add(*offset);
                        if new_ip > len {
                            return Err(VmError::BadJump(new_ip));
                        }
                        ip = new_ip;
                        continue;
                    }
                }
                Instruction::JumpIfTrue(offset) => {
                    let top = self.peek_tri()?;
                    if top == Tri::True {
                        if *offset == 0 {
                            return Err(VmError::BadJump(ip));
                        }
                        let new_ip = ip.saturating_add(*offset);
                        if new_ip > len {
                            return Err(VmError::BadJump(new_ip));
                        }
                        ip = new_ip;
                        continue;
                    }
                }
                other => {
                    self.execute(other, event, row, db, truths)?;
                }
            }
            ip += 1;
        }

        match self.stack.pop() {
            Some(StackValue::Tri(result)) => {
                if self.stack.is_empty() {
                    Ok(result)
                } else {
                    Err(VmError::MalformedProgram)
                }
            }
            Some(StackValue::Value(v)) => {
                // Bare `Null` / `Missing` at TOS is a legitimate `WHERE NULL`
                // (or a `value_at` that returned `Value::Missing` fed
                // straight into the WHERE): both collapse to `Unknown`.
                // Any other bare `Value` is a compiler bug: boolean
                // columns must be lowered with an explicit comparison.
                if self.stack.is_empty() && v.is_absent() {
                    Ok(Tri::Unknown)
                } else {
                    Err(VmError::MalformedProgram)
                }
            }
            None => Err(VmError::StackUnderflow),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn execute<E, DB>(
        &mut self,
        instruction: &Instruction<B>,
        event: &E,
        row: RowKind,
        db: &DB,
        truths: &[Tri],
    ) -> Result<(), VmError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        match instruction {
            Instruction::PushLiteral(value) => {
                self.stack.push(StackValue::Value(value.clone()));
            }

            Instruction::LoadColumn(col_id) => {
                let value = event.value_at(db, row, *col_id).map_err(VmError::Value)?;
                self.stack.push(StackValue::Value(value));
            }

            Instruction::Equal => {
                let result = self.compare_values(values_equal)?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::NotEqual => {
                let result = self.compare_values(|a, b| !values_equal(a, b))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThan => {
                let result =
                    self.compare_ordered(|ord| matches!(ord, core::cmp::Ordering::Less))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThanOrEqual => {
                let result =
                    self.compare_ordered(|ord| !matches!(ord, core::cmp::Ordering::Greater))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThan => {
                let result =
                    self.compare_ordered(|ord| matches!(ord, core::cmp::Ordering::Greater))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThanOrEqual => {
                let result =
                    self.compare_ordered(|ord| !matches!(ord, core::cmp::Ordering::Less))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::IsNull => {
                let value = self.pop_value()?;
                let result = if value.is_absent() {
                    Tri::True
                } else {
                    Tri::False
                };
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::IsNotNull => {
                let value = self.pop_value()?;
                let result = if value.is_absent() {
                    Tri::False
                } else {
                    Tri::True
                };
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::And => {
                let b = self.pop_tri()?;
                let a = self.pop_tri()?;
                self.stack.push(StackValue::Tri(a.and(b)));
            }

            Instruction::Or => {
                let b = self.pop_tri()?;
                let a = self.pop_tri()?;
                self.stack.push(StackValue::Tri(a.or(b)));
            }

            Instruction::Not => {
                let a = self.pop_tri()?;
                self.stack.push(StackValue::Tri(a.not()));
            }

            Instruction::In(literals) => {
                let value = self.pop_value()?;

                if value.is_absent() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                let mut has_null_rhs = false;
                let mut found = false;
                for lit in literals {
                    if lit.is_absent() {
                        has_null_rhs = true;
                    } else if values_equal(&value, lit) {
                        found = true;
                        break;
                    }
                }

                let result = if found {
                    Tri::True
                } else if has_null_rhs {
                    // x IN (1, NULL) -> Unknown when x doesn't match 1 (SQL standard).
                    Tri::Unknown
                } else {
                    Tri::False
                };
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::Between => {
                let upper = self.pop_value()?;
                let lower = self.pop_value()?;
                let value = self.pop_value()?;

                if value.is_absent() || lower.is_absent() || upper.is_absent() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                let ge_lower = compare_ordered_values(&value, &lower, |ord| {
                    !matches!(ord, core::cmp::Ordering::Less)
                });
                let le_upper = compare_ordered_values(&value, &upper, |ord| {
                    !matches!(ord, core::cmp::Ordering::Greater)
                });

                let result = ge_lower.and(le_upper);
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::Like { case_sensitive } => {
                let pattern = self.pop_value()?;
                let string = self.pop_value()?;

                if string.is_absent() || pattern.is_absent() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // Only String-scalar operands support LIKE. Anything else
                // is a compiler bug in a well-formed program; degrade to
                // `Unknown` rather than erroring so a malformed schema
                // hint does not take down the whole dispatch loop.
                let (str_val, pat_val) =
                    if let (Value::String(s), Value::String(p)) = (&string, &pattern) {
                        (s.as_ref(), p.as_ref())
                    } else {
                        self.stack.push(StackValue::Tri(Tri::Unknown));
                        return Ok(());
                    };

                let matched = if *case_sensitive {
                    simple_like(str_val, pat_val)
                } else {
                    simple_like(&str_val.to_lowercase(), &pat_val.to_lowercase())
                };

                self.stack.push(StackValue::Tri(if matched {
                    Tri::True
                } else {
                    Tri::False
                }));
            }

            Instruction::Add => self.execute_binary_value_op(arithmetic_add::<B>)?,
            Instruction::Subtract => self.execute_binary_value_op(arithmetic_subtract::<B>)?,
            Instruction::Multiply => self.execute_binary_value_op(arithmetic_multiply::<B>)?,
            Instruction::Divide => self.execute_binary_value_op(arithmetic_divide::<B>)?,
            Instruction::Modulo => self.execute_binary_value_op(arithmetic_modulo::<B>)?,

            Instruction::Negate => {
                let a = self.pop_value()?;
                let result = arithmetic_negate::<B>(a);
                self.stack.push(StackValue::Value(result));
            }

            // Jumps are handled in eval() before execute() is called.
            Instruction::JumpIfFalse(_) | Instruction::JumpIfTrue(_) => {}

            Instruction::TermTruth(slot) => {
                let truth = truths
                    .get(usize::from(*slot))
                    .copied()
                    .ok_or(VmError::MissingTermTruth(*slot))?;
                self.stack.push(StackValue::Tri(truth));
            }
        }

        Ok(())
    }

    fn execute_binary_value_op(
        &mut self,
        op: fn(Value<B>, Value<B>) -> Value<B>,
    ) -> Result<(), VmError> {
        let b = self.pop_value()?;
        let a = self.pop_value()?;
        self.stack.push(StackValue::Value(op(a, b)));
        Ok(())
    }

    fn pop_value(&mut self) -> Result<Value<B>, VmError> {
        match self.stack.pop() {
            Some(StackValue::Value(v)) => Ok(v),
            Some(StackValue::Tri(_)) => Err(VmError::TypeMismatch {
                expected: "Value",
                got: "Tri",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn pop_tri(&mut self) -> Result<Tri, VmError> {
        match self.stack.pop() {
            Some(StackValue::Tri(t)) => Ok(t),
            // `Null` / `Missing` are legitimate operands for logical ops
            // (`NULL AND true` = `Unknown`). Concrete scalar values are
            // NOT — the compiler must lower boolean columns via an
            // explicit comparison. `Bool` on the stack is a compiler bug.
            Some(StackValue::Value(v)) if v.is_absent() => Ok(Tri::Unknown),
            Some(StackValue::Value(_)) => Err(VmError::TypeMismatch {
                expected: "Tri",
                got: "Value",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn peek_tri(&self) -> Result<Tri, VmError> {
        match self.stack.last() {
            Some(StackValue::Tri(t)) => Ok(*t),
            Some(StackValue::Value(v)) if v.is_absent() => Ok(Tri::Unknown),
            Some(StackValue::Value(_)) => Err(VmError::TypeMismatch {
                expected: "Tri",
                got: "Value",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn compare_values<F>(&mut self, f: F) -> Result<Tri, VmError>
    where
        F: FnOnce(&Value<B>, &Value<B>) -> bool,
    {
        let b = self.pop_value()?;
        let a = self.pop_value()?;

        if a.is_absent() || b.is_absent() {
            return Ok(Tri::Unknown);
        }

        Ok(if f(&a, &b) { Tri::True } else { Tri::False })
    }

    fn compare_ordered<F>(&mut self, f: F) -> Result<Tri, VmError>
    where
        F: FnOnce(core::cmp::Ordering) -> bool,
    {
        let b = self.pop_value()?;
        let a = self.pop_value()?;
        Ok(compare_ordered_values(&a, &b, f))
    }
}

impl<B: Backend> Default for Vm<B> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// LIKE
// ============================================================================

/// SQL `LIKE` pattern matching.
///
/// Supports `%` (zero or more characters) and `_` (exactly one character).
/// Does not support `ESCAPE` clauses.
fn simple_like(string: &str, pattern: &str) -> bool {
    let s: Vec<char> = string.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let pn = p.len();

    // dp[j] = true when s[0..i] matches p[0..j].
    let mut dp = vec![false; pn + 1];
    dp[0] = true;

    // Leading '%' can match the empty string.
    for (j, &ch) in p.iter().enumerate() {
        if ch == '%' {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }

    for &sc in &s {
        let mut new_dp = vec![false; pn + 1];
        for j in 0..pn {
            if !(dp[j] || (p[j] == '%' && new_dp[j])) {
                continue;
            }
            match p[j] {
                '%' => {
                    new_dp[j] = true;
                    new_dp[j + 1] = true;
                }
                '_' => {
                    if dp[j] {
                        new_dp[j + 1] = true;
                    }
                }
                ch => {
                    if dp[j] && sc == ch {
                        new_dp[j + 1] = true;
                    }
                }
            }
        }
        dp = new_dp;
    }

    dp[pn]
}

// ============================================================================
// Arithmetic (same-scalar only)
// ============================================================================

/// `Value::Missing` / `Value::Null` on either side propagates to
/// `Value::Null` (SQL NULL propagation).
const fn null_propagate_binary<B: Backend>(a: &Value<B>, b: &Value<B>) -> Option<Value<B>> {
    if a.is_absent() || b.is_absent() {
        Some(Value::Null)
    } else {
        None
    }
}

/// Add: same-scalar only.
fn arithmetic_add<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x + y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x + y),
        _ => Value::Null,
    }
}

/// Subtract: same-scalar only.
fn arithmetic_subtract<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x - y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x - y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x - y),
        _ => Value::Null,
    }
}

/// Multiply: same-scalar only.
fn arithmetic_multiply<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x * y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x * y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x * y),
        _ => Value::Null,
    }
}

/// Divide: same-scalar only. Division by zero yields `Value::Null`.
///
/// Zero is detected by `b - b == b` — an identity that holds only for the
/// additive identity of a `Sub<Output = Self> + PartialEq` type. This
/// avoids adding a `Zero` bound to `Backend`.
fn arithmetic_divide<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Int(x / y)
            }
        }
        (Value::Float(x), Value::Float(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Float(x / y)
            }
        }
        (Value::Decimal(x), Value::Decimal(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Decimal(x / y)
            }
        }
        _ => Value::Null,
    }
}

/// Modulo: `Int % Int` only. Modulo by zero yields `Value::Null`.
fn arithmetic_modulo<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Int(x % y)
            }
        }
        _ => Value::Null,
    }
}

/// Negate: same-scalar only.
fn arithmetic_negate<B: Backend>(a: Value<B>) -> Value<B> {
    if a.is_absent() {
        return Value::Null;
    }
    match a {
        Value::Int(x) => Value::Int(-x),
        Value::Float(x) => Value::Float(-x),
        Value::Decimal(x) => Value::Decimal(-x),
        _ => Value::Null,
    }
}

/// Trait-generic "is zero" check.
///
/// A scalar `x` is zero iff `x - x == x` (holds only for the additive
/// identity of a `Sub<Output = Self> + PartialEq` type). Backend requires
/// both bounds on `Int` / `Float` / `Decimal`, so this specialises cleanly
/// per scalar without a `num_traits::Zero` bound.
fn is_zero_scalar<T>(x: &T) -> bool
where
    T: Clone + PartialEq + core::ops::Sub<Output = T>,
{
    let cleared = x.clone() - x.clone();
    &cleared == x
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::approx_constant
)]
mod tests {
    //! Behavioural tests for the Backend-generic VM.
    //!
    //! Every test builds a concrete [`TestEvent`] carrying a row image,
    //! wires it through [`Vm::eval`], and asserts the resulting [`Tri`].
    //! The tests are pinned to `Postgres` because it has the widest scalar
    //! coverage; cross-backend behaviour is verified separately in
    //! `backend.rs` and the parser tests once Phase 4 lands.

    use super::*;
    use crate::backend::Postgres;
    use crate::testing::TestEvent;
    use crate::types::EventKind;
    use sql_traits::structs::ParserDB;

    /// Trivial catalog for [`Vm::eval`]. `TestEvent` decodes from its own
    /// stored `Value`s and never consults the schema, so any catalog works.
    pub(super) fn pg_catalog() -> ParserDB {
        ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>("CREATE TABLE t (a INT);")
            .expect("catalog parses")
    }

    /// Convenience: make an Insert event with a `Postgres`-typed row image.
    pub(super) fn insert_pg(cells: Vec<Value<Postgres>>) -> TestEvent<Postgres> {
        TestEvent::insert(0, cells)
    }

    // ------------------------------------------------------------------
    // Smoke coverage: a minimal set of tests guarding the VM contract.
    // The exhaustive behavioural suite (~2000 lines) is rewritten in a
    // follow-up delegation and will replace this block.
    // ------------------------------------------------------------------

    #[test]
    fn simple_comparison_int_greater_than_literal() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
        ]);

        let e = insert_pg(vec![Value::Int(25)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::True
        );

        let e = insert_pg(vec![Value::Int(15)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );

        let e = insert_pg(vec![Value::Int(18)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );
    }

    #[test]
    fn null_operand_propagates_to_unknown() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
        ]);

        let e = insert_pg(vec![Value::Null]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::Unknown
        );
    }

    #[test]
    fn missing_operand_propagates_to_unknown() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(5), // out of range
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
        ]);

        let e = insert_pg(vec![Value::Int(25)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::Unknown
        );
    }

    #[test]
    fn cross_scalar_equality_is_false() {
        let mut vm: Vm<Postgres> = Vm::new();
        // Column 0 is Int(5). Compare with String("5") — no coercion.
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::String("5".into())),
            Instruction::Equal,
        ]);

        let e = insert_pg(vec![Value::Int(5)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );
    }

    #[test]
    fn is_null_on_missing_cell() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> =
            BytecodeProgram::new(vec![Instruction::LoadColumn(0), Instruction::IsNull]);

        let e = insert_pg(vec![Value::Null]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::True
        );

        let e = insert_pg(vec![Value::String("hi".into())]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );
    }

    #[test]
    fn in_list_matches_string() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::In(vec![
                Value::String("pending".into()),
                Value::String("active".into()),
            ]),
        ]);

        let e = insert_pg(vec![Value::String("pending".into())]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::True
        );

        let e = insert_pg(vec![Value::String("completed".into())]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );
    }

    #[test]
    fn arithmetic_add_ints_same_scalar() {
        let mut vm: Vm<Postgres> = Vm::new();
        // (col0 + 3) > 10
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(3)),
            Instruction::Add,
            Instruction::PushLiteral(Value::Int(10)),
            Instruction::GreaterThan,
        ]);

        let e = insert_pg(vec![Value::Int(8)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::True
        );

        let e = insert_pg(vec![Value::Int(5)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );
    }

    #[test]
    fn division_by_zero_yields_null_then_unknown() {
        let mut vm: Vm<Postgres> = Vm::new();
        // (col0 / 0) > 1 -> Unknown
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(0)),
            Instruction::Divide,
            Instruction::PushLiteral(Value::Int(1)),
            Instruction::GreaterThan,
        ]);

        let e = insert_pg(vec![Value::Int(10)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::Unknown
        );
    }

    #[test]
    fn pk_kind_rejects_non_pk_column() {
        let mut vm: Vm<Postgres> = Vm::new();
        // Predicate: col0 == 1
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(1)),
            Instruction::Equal,
        ]);
        // Event has one PK column at index 1 (not 0), so RowKind::Pk lookup
        // on column 0 returns Missing per handoff design gotcha 3.
        let e = TestEvent::<Postgres> {
            kind: EventKind::Update,
            table_id: 0,
            pk_columns: vec![1],
            changed_columns: Vec::new(),
            new_row: vec![Value::Int(1), Value::Int(42)],
            old_row: Vec::new(),
            checkpoint: None,
        };
        assert_eq!(
            vm.eval(&program, &e, RowKind::Pk, &pg_catalog()).unwrap(),
            Tri::Unknown
        );
    }

    #[test]
    fn like_pattern_case_sensitive_match() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::String("h%".into())),
            Instruction::Like {
                case_sensitive: true,
            },
        ]);

        let e = insert_pg(vec![Value::String("hello".into())]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::True
        );

        let e = insert_pg(vec![Value::String("world".into())]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()).unwrap(),
            Tri::False
        );
    }

    #[test]
    fn is_zero_scalar_detects_zero() {
        assert!(is_zero_scalar(&0i64));
        assert!(is_zero_scalar(&0.0f64));
        assert!(!is_zero_scalar(&1i64));
        assert!(!is_zero_scalar(&-3.14f64));
    }

    // ------------------------------------------------------------------
    // Membership term slots
    // ------------------------------------------------------------------

    /// The program carrying only a term leaves the supplied truth as the whole
    /// verdict, all three of them, so a term is not quietly read as a boolean.
    #[test]
    fn a_term_slot_evaluates_to_the_supplied_truth() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> =
            BytecodeProgram::with_terms(vec![Instruction::TermTruth(0)], vec![vec![0]]);
        let e = insert_pg(vec![Value::Int(1)]);

        for truth in [Tri::True, Tri::False, Tri::Unknown] {
            assert_eq!(
                vm.eval_with_terms(&program, &e, RowKind::New, &pg_catalog(), &[truth])
                    .unwrap(),
                truth,
                "slot 0 answers with exactly the truth it was handed"
            );
        }
    }

    /// Two slots read their own truth rather than the first one, which is what
    /// makes an assignment vector an assignment rather than one flag.
    #[test]
    fn each_term_slot_reads_its_own_truth() {
        let mut vm: Vm<Postgres> = Vm::new();
        // `term0 OR term1`, spelled the way the compiler spells an OR.
        let program: BytecodeProgram<Postgres> = BytecodeProgram::with_terms(
            vec![
                Instruction::TermTruth(0),
                Instruction::JumpIfTrue(3),
                Instruction::TermTruth(1),
                Instruction::Or,
            ],
            vec![vec![0], vec![1]],
        );
        let e = insert_pg(vec![Value::Int(1)]);

        let eval = |vm: &mut Vm<Postgres>, truths: &[Tri]| {
            vm.eval_with_terms(&program, &e, RowKind::New, &pg_catalog(), truths)
                .unwrap()
        };

        assert_eq!(eval(&mut vm, &[Tri::False, Tri::True]), Tri::True);
        assert_eq!(eval(&mut vm, &[Tri::True, Tri::False]), Tri::True);
        assert_eq!(eval(&mut vm, &[Tri::False, Tri::False]), Tri::False);
    }

    /// A term composes with a row test through the tri-state logic that already
    /// exists, which is the whole reason the term is an instruction rather than
    /// a set intersected afterwards.
    #[test]
    fn a_term_slot_composes_with_a_row_test() {
        let mut vm: Vm<Postgres> = Vm::new();
        // `a > 18 AND term0`, spelled the way the compiler spells an AND.
        let program: BytecodeProgram<Postgres> = BytecodeProgram::with_terms(
            vec![
                Instruction::LoadColumn(0),
                Instruction::PushLiteral(Value::Int(18)),
                Instruction::GreaterThan,
                Instruction::JumpIfFalse(3),
                Instruction::TermTruth(0),
                Instruction::And,
            ],
            vec![vec![1]],
        );

        let matching = insert_pg(vec![Value::Int(25)]);
        let failing = insert_pg(vec![Value::Int(5)]);

        assert_eq!(
            vm.eval_with_terms(
                &program,
                &matching,
                RowKind::New,
                &pg_catalog(),
                &[Tri::True]
            )
            .unwrap(),
            Tri::True,
            "row test holds and the term admits: the filter holds"
        );
        assert_eq!(
            vm.eval_with_terms(
                &program,
                &matching,
                RowKind::New,
                &pg_catalog(),
                &[Tri::False]
            )
            .unwrap(),
            Tri::False,
            "row test holds and the term does not admit: the filter does not hold"
        );
        assert_eq!(
            vm.eval_with_terms(
                &program,
                &failing,
                RowKind::New,
                &pg_catalog(),
                &[Tri::True]
            )
            .unwrap(),
            Tri::False,
            "the row test alone can still refuse the row"
        );
    }

    /// A slot with no truth supplied is an error rather than an `Unknown`.
    /// Answering `Unknown` would read as "did not match" for this evaluation
    /// and hide that the caller never narrowed anything.
    #[test]
    fn a_term_slot_with_no_truth_supplied_is_an_error() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> =
            BytecodeProgram::with_terms(vec![Instruction::TermTruth(1)], vec![vec![0], vec![1]]);
        let e = insert_pg(vec![Value::Int(1)]);

        assert_eq!(
            vm.eval_with_terms(&program, &e, RowKind::New, &pg_catalog(), &[Tri::True]),
            Err(VmError::MissingTermTruth(1)),
            "one truth supplied, slot 1 asked for: the caller is told, not answered"
        );
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()),
            Err(VmError::MissingTermTruth(1)),
            "plain eval supplies no truths at all, so any slot is missing"
        );
    }

    /// `eval` is `eval_with_terms` with no truths, so the 23 existing call
    /// sites keep their behaviour on every term-free program.
    #[test]
    fn eval_agrees_with_eval_with_terms_on_a_term_free_program() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
        ]);
        let e = insert_pg(vec![Value::Int(25)]);

        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()),
            vm.eval_with_terms(&program, &e, RowKind::New, &pg_catalog(), &[]),
        );
    }
}
