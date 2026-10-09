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
//!   `StackValue::Truth` on the stack, or exactly one
//!   `StackValue::Value` carrying `Value::Null` / `Value::Missing` (both
//!   lift to `Tri::Unknown`). Any other final shape is a compiler bug and
//!   surfaces as [`VmError::MalformedProgram`].
//! * Same-scalar arithmetic only, and an integer beside a float read as a
//!   float. Any other pair is refused with
//!   [`EvaluationRefusal::OperandKinds`].
//!   A `Null` operand answers `Null`, and otherwise a `Missing` one answers
//!   `Missing`, so a null test over the result stays unanswered.
//! * A `LoadColumn` instruction reads its cell through
//!   [`CdcEvent::cell_at`], which lends the decoded [`Value`] when the event
//!   caches it and hands an owned one over otherwise. Boolean predicates on a bare column
//!   MUST be lowered by the compiler as an explicit comparison
//!   (`LoadColumn(col)` + `PushLiteral(Bool(true))` + `Equal`)
//!   because the VM does not lift a bare `Value::Bool` on the stack to
//!   `Tri` (that lift is backend-specific: Postgres `Bool = bool`, SQLite
//!   `Bool = i64`).

pub mod arithmetic;
mod possible;
pub mod refusal;

use super::{
    bytecode::{ComparisonRef, FloatResult},
    value_cmp::{compare_ordered_values, values_equal},
    BytecodeProgram, Instruction, Tri,
};
use crate::backend::{Backend, CdcEvent, ComparisonContext, RowKind, Value};
use alloc::{borrow::Cow, vec::Vec};
use arithmetic::{
    arithmetic_add, arithmetic_divide, arithmetic_modulo, arithmetic_multiply, arithmetic_negate,
    arithmetic_subtract,
};
use possible::Possible;
use refusal::{DanglingEscape, EvaluationRefusal};
use sql_traits::prelude::DatabaseLike;

/// One fallible unary arithmetic operation, as the VM dispatches it.
type FallibleUnaryOp<B> = fn(Value<B>) -> Result<Value<B>, EvaluationRefusal>;

/// VM evaluation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmError {
    /// The target engine refuses this evaluation. Not a program defect:
    /// the caller reports it against the subscription whose predicate
    /// asked, and every other subscription on the same event is
    /// unaffected.
    Refused(refusal::EvaluationRefusal),

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

    /// Column index out of range for the observed event / schema.
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
    /// for.
    MissingTermTruth(u16),

    /// A carried cell could not be decoded to its declared type, surfaced
    /// from [`crate::backend::CdcEvent::value_at`].
    Value(crate::ValueError),
}

/// Slot on the VM's evaluation stack.
///
/// `Value(_)` holds arithmetic results and absent operands. A present
/// literal or column cell is referred to rather than copied, by
/// `Literal(_)` and `Cell(_)`. `Truth(_)` variants come from comparison,
/// null-check, and logical instructions.
enum StackValue<B: Backend> {
    /// Scalar value (from arithmetic, or an absent literal or cell).
    Value(Value<B>),
    /// The present literal of the `PushLiteral` at this instruction index.
    Literal(usize),
    /// A present cell of this column, lent by the event for the evaluated row.
    Cell(crate::ColumnId),
    /// The truths a condition may take (from comparisons, null checks, or logical ops).
    Truth(Possible),
}

// `Clone`, `Debug`, and `PartialEq` are hand-implemented for the same
// reason as `Value<B>`: `#[derive(...)]` would defensively require
// `B: Clone` etc., which is not implied by `Backend`.

impl<B: Backend> Clone for StackValue<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Value(v) => Self::Value(v.clone()),
            Self::Literal(ip) => Self::Literal(*ip),
            Self::Cell(col) => Self::Cell(*col),
            Self::Truth(t) => Self::Truth(*t),
        }
    }
}

impl<B: Backend> core::fmt::Debug for StackValue<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Value(v) => f.debug_tuple("Value").field(v).finish(),
            Self::Literal(ip) => f.debug_tuple("Literal").field(ip).finish(),
            Self::Cell(col) => f.debug_tuple("Cell").field(col).finish(),
            Self::Truth(t) => f.debug_tuple("Truth").field(t).finish(),
        }
    }
}

impl<B: Backend> PartialEq for StackValue<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Value(a), Self::Value(b)) => a == b,
            (Self::Literal(a), Self::Literal(b)) => a == b,
            (Self::Cell(a), Self::Cell(b)) => a == b,
            (Self::Truth(a), Self::Truth(b)) => a == b,
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
    /// The first column this evaluation read that the event does not
    /// carry, or the omitted column a term's unknown truth stands for,
    /// cleared at the start of each evaluation.
    ///
    /// Recorded where the cell is read rather than derived from the
    /// program's column list, so a short circuit that never reaches the
    /// absent cell records nothing, and so a `Null` cell, which is a value
    /// the database holds, is never mistaken for an absent one.
    absent_column: Option<crate::ColumnId>,
    /// Whether the last evaluation's omitted cells decide between a match
    /// and none.
    undecided: bool,
    /// Buffers every `LIKE` match reuses, so matching allocates nothing once warm.
    like: LikeScratch,
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
            absent_column: None,
            undecided: false,
            like: LikeScratch::default(),
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
        self.eval_with_terms(program, event, row, db, &[], None)
    }

    /// The column the last evaluation read and the event did not carry,
    /// when the cells the event omitted decide between a match and none.
    ///
    /// `None` when every cell it read was there, and also when the carried
    /// cells settle the answer regardless, as in `body = 'x' AND tag = 'no'`
    /// or `body = 'x' AND tag = NULL` with `body` omitted. Only meaningful
    /// immediately after an evaluation, which resets it. The caller decides
    /// what an absent cell means: the answer is not false, it is missing,
    /// and a caller holding a connector can re-read.
    #[must_use]
    pub const fn absent_column(&self) -> Option<crate::ColumnId> {
        if self.undecided {
            self.absent_column
        } else {
            None
        }
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
    ///
    /// An unknown truth stands for a term whose compared cell the event
    /// omitted, named by `omitted_term`, so [`Vm::absent_column`] reports it
    /// when that term is what leaves the answer open.
    pub fn eval_with_terms<E, DB>(
        &mut self,
        program: &BytecodeProgram<B>,
        event: &E,
        row: RowKind,
        db: &DB,
        truths: &[Tri],
        omitted_term: Option<crate::ColumnId>,
    ) -> Result<Tri, VmError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        self.stack.clear();
        self.absent_column = None;
        self.undecided = false;

        let instructions = &program.instructions;
        let len = instructions.len();
        let mut ip = 0;
        let src = Operands {
            program,
            event,
            row,
            db,
        };
        let terms = Terms {
            truths,
            omitted: omitted_term,
        };

        // Execute instructions with explicit instruction pointer (supports jumps).
        while ip < len {
            match &instructions[ip] {
                Instruction::JumpIfFalse(offset) => {
                    let top = self.peek_possible()?.tri();
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
                    let top = self.peek_possible()?.tri();
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
                    self.execute(ip, other, &src, &terms)?;
                }
            }
            ip += 1;
        }

        match self.stack.pop() {
            Some(StackValue::Truth(result)) => {
                if self.stack.is_empty() {
                    self.undecided = result.undecided();
                    Ok(result.tri())
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
                    self.undecided = v.is_missing();
                    Ok(Tri::Unknown)
                } else {
                    Err(VmError::MalformedProgram)
                }
            }
            // Only a present value is ever referred to rather than held.
            Some(StackValue::Literal(_) | StackValue::Cell(_)) => Err(VmError::MalformedProgram),
            None => Err(VmError::StackUnderflow),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn execute<E, DB>(
        &mut self,
        ip: usize,
        instruction: &Instruction<B>,
        src: &Operands<'_, B, E, DB>,
        terms: &Terms<'_>,
    ) -> Result<(), VmError>
    where
        E: CdcEvent<Backend = B>,
        DB: DatabaseLike,
    {
        let program = src.program;
        match instruction {
            Instruction::PushLiteral(value) => {
                self.stack.push(if worth_referring(value) {
                    StackValue::Literal(ip)
                } else {
                    StackValue::Value(value.clone())
                });
            }

            Instruction::LoadColumn(col_id) => {
                let cell = src
                    .event
                    .cell_at(src.db, src.row, *col_id)
                    .map_err(VmError::Value)?;
                if cell.is_missing() && self.absent_column.is_none() {
                    self.absent_column = Some(*col_id);
                }
                self.stack.push(match cell {
                    Cow::Borrowed(value) if worth_referring(value) => StackValue::Cell(*col_id),
                    cell => StackValue::Value(cell.into_owned()),
                });
            }

            Instruction::Equal(comparison) => {
                let result =
                    self.compare_values(src, *comparison, |ctx, a, b| values_equal(ctx, a, b))?;
                self.replace_compared(result);
            }

            Instruction::NotEqual(comparison) => {
                let result = self.compare_values(src, *comparison, |ctx, a, b| {
                    values_equal(ctx, a, b).map(|equal| !equal)
                })?;
                self.replace_compared(result);
            }

            Instruction::NotDistinct(comparison) => {
                let result = not_distinct(
                    comparison_context(src.program, *comparison)?,
                    peek(&self.stack, 1, src)?,
                    peek(&self.stack, 0, src)?,
                )?;
                self.replace_top(2, result);
            }

            Instruction::Coalesce(count) => self.coalesce(usize::from(*count), src)?,

            Instruction::Truth => {
                let result = value_truth(peek(&self.stack, 0, src)?)?;
                self.replace_top(1, result);
            }

            Instruction::IsTruth { value, negated } => {
                let condition = self.pop_possible()?;
                self.stack
                    .push(StackValue::Truth(condition.is(*value, *negated)));
            }

            Instruction::LessThan(comparison) => {
                let result = self.compare_ordered(src, *comparison, |ord| {
                    matches!(ord, core::cmp::Ordering::Less)
                })?;
                self.replace_compared(result);
            }

            Instruction::LessThanOrEqual(comparison) => {
                let result = self.compare_ordered(src, *comparison, |ord| {
                    !matches!(ord, core::cmp::Ordering::Greater)
                })?;
                self.replace_compared(result);
            }

            Instruction::GreaterThan(comparison) => {
                let result = self.compare_ordered(src, *comparison, |ord| {
                    matches!(ord, core::cmp::Ordering::Greater)
                })?;
                self.replace_compared(result);
            }

            Instruction::GreaterThanOrEqual(comparison) => {
                let result = self.compare_ordered(src, *comparison, |ord| {
                    !matches!(ord, core::cmp::Ordering::Less)
                })?;
                self.replace_compared(result);
            }

            // A null test reads absence as its answer, which is why it is
            // the one operator that must tell `Missing` from `Null`
            // rather than folding them. `NULL IS NULL` is `TRUE`, and a
            // cell the source did not carry is not `NULL`: the row may
            // hold anything, so the verdict is unknown and the caller is
            // told which column to read. Answering `TRUE` would notify a
            // subscription about a row the database would not have
            // selected, and answering `FALSE` for `IS NOT NULL` would
            // lose one it would.
            Instruction::IsNull => {
                let value = peek(&self.stack, 0, src)?;
                let result = if value.is_missing() {
                    Tri::Unknown
                } else if value.is_null() {
                    Tri::True
                } else {
                    Tri::False
                };
                self.replace_top(1, result);
            }

            Instruction::IsNotNull => {
                let value = peek(&self.stack, 0, src)?;
                let result = if value.is_missing() {
                    Tri::Unknown
                } else if value.is_null() {
                    Tri::False
                } else {
                    Tri::True
                };
                self.replace_top(1, result);
            }

            Instruction::And => {
                let b = self.pop_possible()?;
                let a = self.pop_possible()?;
                self.stack.push(StackValue::Truth(a.and(b)));
            }

            Instruction::Or => {
                let b = self.pop_possible()?;
                let a = self.pop_possible()?;
                self.stack.push(StackValue::Truth(a.or(b)));
            }

            Instruction::Not => {
                let a = self.pop_possible()?;
                self.stack.push(StackValue::Truth(a.not()));
            }

            Instruction::In {
                literals,
                comparison,
            } => {
                let value = peek(&self.stack, 0, src)?;
                let ctx = comparison_context(program, *comparison)?;
                let result = 'result: {
                    if value.is_absent() {
                        break 'result Tri::Unknown;
                    }
                    let mut has_null_rhs = false;
                    for lit in literals {
                        if lit.is_absent() {
                            has_null_rhs = true;
                        } else if values_equal(ctx, value, lit).map_err(VmError::Refused)? {
                            break 'result Tri::True;
                        }
                    }
                    // x IN (1, NULL) -> Unknown when x doesn't match 1 (SQL standard).
                    if has_null_rhs {
                        Tri::Unknown
                    } else {
                        Tri::False
                    }
                };
                self.replace_top(1, result);
            }

            Instruction::Between {
                lower: lower_facts,
                upper: upper_facts,
            } => {
                let value = peek(&self.stack, 2, src)?;
                let lower = peek(&self.stack, 1, src)?;
                let upper = peek(&self.stack, 0, src)?;
                // `x BETWEEN low AND high` is `x >= low AND x <= high`, so an
                // absent bound leaves only its own side unknown.
                let side = |bound: &Value<B>, facts, holds: fn(core::cmp::Ordering) -> bool| {
                    if value.is_absent() || bound.is_absent() {
                        return Ok(Tri::Unknown);
                    }
                    compare_ordered_values(comparison_context(program, facts)?, value, bound, holds)
                        .map_err(VmError::Refused)
                };
                let ge_lower = side(lower, *lower_facts, |ord| {
                    !matches!(ord, core::cmp::Ordering::Less)
                })?;
                let le_upper = side(upper, *upper_facts, |ord| {
                    !matches!(ord, core::cmp::Ordering::Greater)
                })?;
                let result = ge_lower.and(le_upper);
                self.replace_top(3, result);
            }

            Instruction::Like { comparison, escape } => {
                // The rule travels on the instruction, resolved at
                // registration from both operands and from which of
                // `LIKE` or `ILIKE` was written.
                let case = comparison
                    .text
                    .map_or(crate::backend::TextCase::Exact, |rule| rule.case);
                let Self { stack, like, .. } = &mut *self;
                let string = peek(stack, 1, src)?;
                let pattern = peek(stack, 0, src)?;
                let result = match (string, pattern) {
                    (string, pattern) if string.is_absent() || pattern.is_absent() => Tri::Unknown,
                    (Value::String(s), Value::String(p)) => {
                        // The walk reports a dangling escape only where the engine
                        // refuses it, which is where the matcher reached it with
                        // input left. What that means is the engine's: PostgreSQL
                        // raises, MySQL answers no-match.
                        let matched = match like.matches(s.as_ref(), p.as_ref(), *escape, case) {
                            Ok(matched) => matched,
                            Err(PatternError::TrailingEscape) => match B::LIKE_DANGLING_ESCAPE {
                                DanglingEscape::Fails => {
                                    return Err(VmError::Refused(
                                        EvaluationRefusal::LikePatternEndsWithEscape,
                                    ))
                                }
                                DanglingEscape::NoMatch => false,
                            },
                        };
                        if matched {
                            Tri::True
                        } else {
                            Tri::False
                        }
                    }
                    // Only String-scalar operands support LIKE. Anything else
                    // is a compiler bug in a well-formed program; degrade to
                    // `Unknown` rather than erroring so a malformed schema
                    // hint does not take down the whole dispatch loop.
                    _ => Tri::Unknown,
                };
                self.replace_top(2, result);
            }

            Instruction::Add(width) => {
                self.execute_binary_value_op(src, arithmetic_add::<B>, *width)?;
            }
            Instruction::Subtract(width) => {
                self.execute_binary_value_op(src, arithmetic_subtract::<B>, *width)?;
            }
            Instruction::Multiply(width) => {
                self.execute_binary_value_op(src, arithmetic_multiply::<B>, *width)?;
            }
            Instruction::Divide(width, quotient) => {
                let quotient = *quotient;
                self.execute_binary_value_op(
                    src,
                    |a, b| arithmetic_divide::<B>(a, b, quotient),
                    *width,
                )?;
            }
            Instruction::Modulo(width) => {
                self.execute_binary_value_op(src, arithmetic_modulo::<B>, *width)?;
            }

            Instruction::Negate(width) => {
                self.execute_unary_value_op(src, arithmetic_negate::<B>, *width)?;
            }

            // Jumps are handled in eval() before execute() is called.
            Instruction::JumpIfFalse(_) | Instruction::JumpIfTrue(_) => {}

            Instruction::TermTruth(slot) => {
                let truth = terms
                    .truths
                    .get(usize::from(*slot))
                    .copied()
                    .ok_or(VmError::MissingTermTruth(*slot))?;
                let possible = if truth == Tri::Unknown {
                    if self.absent_column.is_none() {
                        self.absent_column = terms.omitted;
                    }
                    Possible::ANY
                } else {
                    Possible::of(truth)
                };
                self.stack.push(StackValue::Truth(possible));
            }
        }

        Ok(())
    }

    fn execute_binary_value_op<E: CdcEvent<Backend = B>, DB: DatabaseLike>(
        &mut self,
        src: &Operands<'_, B, E, DB>,
        op: impl FnOnce(Value<B>, Value<B>) -> Result<Value<B>, EvaluationRefusal>,
        width: FloatResult,
    ) -> Result<(), VmError> {
        let b = self.pop_owned(src)?;
        let a = self.pop_owned(src)?;
        let value = op(a, b).map_err(VmError::Refused)?;
        self.stack
            .push(StackValue::Value(hold_float_at::<B>(value, width)));
        Ok(())
    }

    /// The same for a fallible unary operation.
    fn execute_unary_value_op<E: CdcEvent<Backend = B>, DB: DatabaseLike>(
        &mut self,
        src: &Operands<'_, B, E, DB>,
        op: FallibleUnaryOp<B>,
        width: FloatResult,
    ) -> Result<(), VmError> {
        let a = self.pop_owned(src)?;
        let value = op(a).map_err(VmError::Refused)?;
        self.stack
            .push(StackValue::Value(hold_float_at::<B>(value, width)));
        Ok(())
    }

    /// The value on top of the stack, owned, for an operation that consumes it.
    fn pop_owned<E: CdcEvent<Backend = B>, DB: DatabaseLike>(
        &mut self,
        src: &Operands<'_, B, E, DB>,
    ) -> Result<Value<B>, VmError> {
        match self.stack.pop() {
            Some(StackValue::Value(v)) => Ok(v),
            Some(referring) => referred(&referring, src).cloned().map_err(Into::into),
            None => Err(VmError::StackUnderflow),
        }
    }

    /// Replace the top `count` slots with the first whose value is not `Null`,
    /// a `Missing` one ahead of it standing for the answer.
    fn coalesce<E: CdcEvent<Backend = B>, DB: DatabaseLike>(
        &mut self,
        count: usize,
        src: &Operands<'_, B, E, DB>,
    ) -> Result<(), VmError> {
        let base = self
            .stack
            .len()
            .checked_sub(count)
            .ok_or(VmError::StackUnderflow)?;
        let mut answer = None;
        for depth in (0..count).rev() {
            if !peek(&self.stack, depth, src)?.is_null() {
                answer = Some(base + (count - 1 - depth));
                break;
            }
        }
        if let Some(index) = answer {
            self.stack.swap(base, index);
            self.stack.truncate(base + 1);
        } else {
            self.stack.truncate(base);
            self.stack.push(StackValue::Value(Value::Null));
        }
        Ok(())
    }

    /// Drop the `consumed` operands on top of the stack and push `result`,
    /// which an omitted operand leaves open to every truth when it is unknown.
    #[expect(
        clippy::inline_always,
        reason = "measured under cachegrind: the per-operator operand path costs more instructions when left to the heuristic"
    )]
    #[inline(always)]
    fn replace_top(&mut self, consumed: usize, result: Tri) {
        let possible = if result == Tri::Unknown && self.omits_operand(consumed) {
            Possible::ANY
        } else {
            Possible::of(result)
        };
        // Popped one by one: `truncate` does not inline, and this is per operator.
        for _ in 0..consumed {
            self.stack.pop();
        }
        self.stack.push(StackValue::Truth(possible));
    }

    /// Whether one of the `consumed` operands on top of the stack is a cell the event omitted.
    #[inline(never)]
    fn omits_operand(&self, consumed: usize) -> bool {
        self.stack
            .iter()
            .rev()
            .take(consumed)
            .any(|slot| matches!(slot, StackValue::Value(value) if value.is_missing()))
    }

    /// Replace a comparison's two operands with `result`.
    ///
    /// A comparison with a `NULL` operand is unknown whatever the other
    /// holds, so an omitted operand beside it leaves nothing open.
    #[expect(
        clippy::inline_always,
        reason = "measured under cachegrind: the per-operator operand path costs more instructions when left to the heuristic"
    )]
    #[inline(always)]
    fn replace_compared(&mut self, result: Tri) {
        if result == Tri::Unknown && self.compares_null() {
            self.stack.pop();
            self.stack.pop();
            self.stack
                .push(StackValue::Truth(Possible::of(Tri::Unknown)));
        } else {
            self.replace_top(2, result);
        }
    }

    /// Whether one of the two compared operands on top of the stack is `NULL`.
    #[inline(never)]
    fn compares_null(&self) -> bool {
        self.stack
            .iter()
            .rev()
            .take(2)
            .any(|slot| matches!(slot, StackValue::Value(value) if value.is_null()))
    }

    fn pop_possible(&mut self) -> Result<Possible, VmError> {
        self.stack
            .pop()
            .map_or(Err(VmError::StackUnderflow), |slot| truths_of(&slot))
    }

    fn peek_possible(&self) -> Result<Possible, VmError> {
        self.stack
            .last()
            .map_or(Err(VmError::StackUnderflow), truths_of)
    }

    fn compare_values<E: CdcEvent<Backend = B>, DB: DatabaseLike, F>(
        &self,
        src: &Operands<'_, B, E, DB>,
        comparison: ComparisonRef,
        f: F,
    ) -> Result<Tri, VmError>
    where
        F: FnOnce(
            ComparisonContext<'_, B>,
            &Value<B>,
            &Value<B>,
        ) -> Result<bool, crate::compiler::vm::refusal::EvaluationRefusal>,
    {
        let a = peek(&self.stack, 1, src)?;
        let b = peek(&self.stack, 0, src)?;
        Ok(if a.is_absent() || b.is_absent() {
            Tri::Unknown
        } else if f(comparison_context(src.program, comparison)?, a, b).map_err(VmError::Refused)? {
            Tri::True
        } else {
            Tri::False
        })
    }

    fn compare_ordered<E: CdcEvent<Backend = B>, DB: DatabaseLike, F>(
        &self,
        src: &Operands<'_, B, E, DB>,
        comparison: ComparisonRef,
        f: F,
    ) -> Result<Tri, VmError>
    where
        F: FnOnce(core::cmp::Ordering) -> bool,
    {
        let a = peek(&self.stack, 1, src)?;
        let b = peek(&self.stack, 0, src)?;
        compare_ordered_values(comparison_context(src.program, comparison)?, a, b, f)
            .map_err(VmError::Refused)
    }
}

/// Why a stack slot names no value, kept small so the operand path returns
/// in registers and widened to [`VmError`] at the instruction.
#[derive(Clone, Copy)]
enum BadOperand {
    Underflow,
    NotAValue,
    Malformed,
}

impl From<BadOperand> for VmError {
    fn from(bad: BadOperand) -> Self {
        match bad {
            BadOperand::Underflow => Self::StackUnderflow,
            BadOperand::NotAValue => Self::TypeMismatch {
                expected: "Value",
                got: "Tri",
            },
            BadOperand::Malformed => Self::MalformedProgram,
        }
    }
}

/// The value the slot `depth` below the stack top stands for.
#[expect(
    clippy::inline_always,
    reason = "measured under cachegrind: the per-operator operand path costs more instructions when left to the heuristic"
)]
#[inline(always)]
fn peek<'s, B: Backend, E: CdcEvent<Backend = B>, DB: DatabaseLike>(
    stack: &'s [StackValue<B>],
    depth: usize,
    src: &'s Operands<'_, B, E, DB>,
) -> Result<&'s Value<B>, BadOperand> {
    let slot = stack
        .len()
        .checked_sub(depth + 1)
        .and_then(|index| stack.get(index))
        .ok_or(BadOperand::Underflow)?;
    match slot {
        StackValue::Value(value) => Ok(value),
        referring => referred(referring, src),
    }
}

/// The value a referring slot names, in the program's literals or the
/// event's lent cells. Out of line so the held-value path stays small.
#[inline(never)]
fn referred<'s, B: Backend, E: CdcEvent<Backend = B>, DB: DatabaseLike>(
    slot: &StackValue<B>,
    src: &'s Operands<'_, B, E, DB>,
) -> Result<&'s Value<B>, BadOperand> {
    match *slot {
        StackValue::Literal(ip) => match src.program.instructions.get(ip) {
            Some(Instruction::PushLiteral(value)) => Ok(value),
            _ => Err(BadOperand::Malformed),
        },
        // `LoadColumn` refers only to a cell it read lent, and a lent cell stays lent.
        StackValue::Cell(col) => match src.event.cell_at(src.db, src.row, col) {
            Ok(Cow::Borrowed(value)) => Ok(value),
            _ => Err(BadOperand::Malformed),
        },
        StackValue::Value(_) | StackValue::Truth(_) => Err(BadOperand::NotAValue),
    }
}

/// `a IS NOT DISTINCT FROM b`: `Null` is a value here and `Missing` is still
/// no answer, since the row may hold anything in a cell the source did not
/// carry.
fn not_distinct<B: Backend>(
    context: ComparisonContext<'_, B>,
    a: &Value<B>,
    b: &Value<B>,
) -> Result<Tri, VmError> {
    Ok(if a.is_missing() || b.is_missing() {
        Tri::Unknown
    } else if a.is_null() || b.is_null() {
        if a.is_null() && b.is_null() {
            Tri::True
        } else {
            Tri::False
        }
    } else if values_equal(context, a, b).map_err(VmError::Refused)? {
        Tri::True
    } else {
        Tri::False
    })
}

/// A boolean value's truth where a condition is read, unknown when absent.
fn value_truth<B: Backend>(value: &Value<B>) -> Result<Tri, VmError> {
    match value {
        Value::Bool(flag) => Ok(if crate::backend::ScalarTruth::scalar_truth(flag) {
            Tri::True
        } else {
            Tri::False
        }),
        absent if absent.is_absent() => Ok(Tri::Unknown),
        _ => Err(VmError::TypeMismatch {
            expected: "Bool",
            got: "Value",
        }),
    }
}

/// The truths a slot read as a condition may take.
///
/// `Null` / `Missing` are legitimate operands for logical ops
/// (`NULL AND true` = `Unknown`), a `Missing` one open to every truth.
/// Concrete scalar values are NOT, the compiler must lower boolean columns
/// via an explicit comparison. `Bool` on the stack is a compiler bug.
const fn truths_of<B: Backend>(slot: &StackValue<B>) -> Result<Possible, VmError> {
    match slot {
        StackValue::Truth(possible) => Ok(*possible),
        StackValue::Value(value) if value.is_missing() => Ok(Possible::ANY),
        StackValue::Value(value) if value.is_null() => Ok(Possible::of(Tri::Unknown)),
        StackValue::Value(_) | StackValue::Literal(_) | StackValue::Cell(_) => {
            Err(VmError::TypeMismatch {
                expected: "Tri",
                got: "Value",
            })
        }
    }
}

/// Where a referring stack slot's value lives: the program's literals and
/// the cells the event lends for the evaluated row image.
struct Operands<'a, B: Backend, E, DB> {
    program: &'a BytecodeProgram<B>,
    event: &'a E,
    row: RowKind,
    db: &'a DB,
}

/// The truths an evaluation's membership terms take, and the column whose
/// omission leaves a term unknown.
struct Terms<'a> {
    truths: &'a [Tri],
    omitted: Option<crate::ColumnId>,
}

/// Whether a present value costs more to copy onto the stack than to refer
/// to: the text, byte, decimal, document and custom carriers allocate on clone.
const fn worth_referring<B: Backend>(value: &Value<B>) -> bool {
    matches!(
        value,
        Value::String(_)
            | Value::Bytes(_)
            | Value::Decimal(_)
            | Value::Json(_)
            | Value::Jsonb(_)
            | Value::Custom(_)
    )
}

/// One arithmetic result, held at the width the compiler resolved for it.
///
/// The narrowing itself is the backend's, because only it knows whether its
/// float carrier can be put on the float4 grid.
fn hold_float_at<B: Backend>(value: Value<B>, width: FloatResult) -> Value<B> {
    match (value, width) {
        (Value::Float(float), Some(crate::backend::FloatWidth::Single)) => {
            Value::Float(B::hold_float_at_single(float))
        }
        (value, _) => value,
    }
}

/// The facts the compiler interned for one comparison's operands.
///
/// # Errors
///
/// [`VmError::MalformedProgram`] when either side names a slot the program
/// does not carry.
fn comparison_context<B: Backend>(
    program: &BytecodeProgram<B>,
    comparison: ComparisonRef,
) -> Result<ComparisonContext<'_, B>, VmError> {
    Ok(ComparisonContext {
        left: program
            .comparison_at(comparison.left)
            .map_err(|_| VmError::MalformedProgram)?,
        right: program
            .comparison_at(comparison.right)
            .map_err(|_| VmError::MalformedProgram)?,
        // Resolved at registration and carried by the instruction, so the
        // comparator reads a rule rather than a collation.
        text: comparison.text,
    })
}

impl<B: Backend> Default for Vm<B> {
    fn default() -> Self {
        Self::new()
    }
}

// LIKE

/// One step of a compiled `LIKE` pattern.
///
/// Parsing the pattern before matching is what makes the escape rule a
/// property of the pattern rather than of every step of the match: once a
/// character is escaped it is a [`Self::Literal`], indistinguishable from
/// any other literal character.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PatternAtom {
    /// `%`: zero or more characters.
    AnySequence,
    /// `_`: exactly one character.
    AnyChar,
    /// One character, matched as itself.
    Literal(char),
    /// The escape character with nothing left to escape, which only the
    /// final position can hold. It matches nothing, and reaching it with
    /// input still to read is what PostgreSQL refuses.
    DanglingEscape,
}

/// A `LIKE` pattern the matcher reached but cannot answer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PatternError {
    /// PostgreSQL's matcher would reach a [`PatternAtom::DanglingEscape`]
    /// with input still to read.
    ///
    /// PostgreSQL raises `LIKE pattern must not end with escape character`
    /// exactly here, and answers false when the input ran out before the
    /// matcher arrived, which is why this is reported from matching rather
    /// than from parsing. MySQL answers false either way. Keeping it
    /// distinct from a no-match is what lets a per-subscription evaluation
    /// failure report it once the engine carries one, without revisiting
    /// the match.
    TrailingEscape,
}

/// The buffers one `LIKE` match needs, kept between matches.
#[derive(Default)]
struct LikeScratch {
    atoms: Vec<PatternAtom>,
    text: Vec<char>,
    finder: SegmentFinder,
}

impl LikeScratch {
    /// Compile `pattern` into `atoms` under `escape`, the engine's default
    /// escape character.
    ///
    /// With `escape` `None` every character is ordinary, which is SQLite's
    /// rule: a backslash in a pattern matches a backslash.
    fn compile(&mut self, pattern: &str, escape: Option<char>) {
        self.atoms.clear();
        let mut chars = pattern.chars();
        while let Some(ch) = chars.next() {
            if Some(ch) == escape {
                // The escape applies to whatever follows, wildcard or not:
                // both engines that have it answer `'ab' LIKE 'a\b'` true.
                self.atoms.push(
                    chars
                        .next()
                        .map_or(PatternAtom::DanglingEscape, PatternAtom::Literal),
                );
                continue;
            }
            self.atoms.push(match ch {
                '%' => PatternAtom::AnySequence,
                '_' => PatternAtom::AnyChar,
                literal => PatternAtom::Literal(literal),
            });
        }
    }

    /// SQL `LIKE` pattern matching under one engine's default escape character.
    ///
    /// Supports `%` (zero or more characters), `_` (exactly one character) and
    /// the default escape. An explicit `ESCAPE` clause is refused before
    /// reaching here.
    ///
    /// The pattern splits at every `%` into segments of fixed length. The
    /// first must sit at the start of the text, the last at its end, and each
    /// one between is taken at its leftmost place after the one before, which
    /// is never worse than a later place. Each search reads the text once, so
    /// the cost is the text times the words a segment's positions fill.
    fn matches(
        &mut self,
        string: &str,
        pattern: &str,
        escape: Option<char>,
        case: crate::backend::TextCase,
    ) -> Result<bool, PatternError> {
        self.compile(pattern, escape);
        self.text.clear();
        self.text.extend(string.chars());
        let Self {
            atoms,
            text,
            finder,
        } = self;
        let Some(PatternAtom::DanglingEscape) = atoms.last() else {
            return Ok(fits_pattern(atoms, text, case, finder));
        };
        // PostgreSQL's matcher reaches the escape with input left exactly when
        // a proper prefix of the text matches everything before it, which is
        // when the text less its last character matches that followed by `%`.
        let Some(shorter) = text.len().checked_sub(1) else {
            return Ok(false);
        };
        let last = atoms.len() - 1;
        atoms[last] = PatternAtom::AnySequence;
        if fits_pattern(atoms, &text[..shorter], case, finder) {
            Err(PatternError::TrailingEscape)
        } else {
            Ok(false)
        }
    }
}

/// Whether `text` matches the compiled `atoms`, none of them a dangling escape.
fn fits_pattern(
    atoms: &[PatternAtom],
    text: &[char],
    case: crate::backend::TextCase,
    finder: &mut SegmentFinder,
) -> bool {
    let any = |atom: &PatternAtom| *atom == PatternAtom::AnySequence;
    let (Some(first_any), Some(last_any)) =
        (atoms.iter().position(any), atoms.iter().rposition(any))
    else {
        return atoms.len() == text.len() && fits_segment(atoms, text, case);
    };
    let head = &atoms[..first_any];
    let tail = &atoms[last_any + 1..];
    let Some(until) = text
        .len()
        .checked_sub(tail.len())
        .filter(|until| *until >= head.len())
    else {
        return false;
    };
    if !fits_segment(head, &text[..head.len()], case) || !fits_segment(tail, &text[until..], case) {
        return false;
    }
    let mut from = head.len();
    for segment in atoms[first_any..last_any]
        .split(any)
        .filter(|s| !s.is_empty())
    {
        match finder.find(segment, &text[from..until], case) {
            Some(end) => from += end,
            None => return false,
        }
    }
    true
}

/// Whether `text` matches the `%`-free `segment` character for character.
fn fits_segment(segment: &[PatternAtom], text: &[char], case: crate::backend::TextCase) -> bool {
    segment.iter().zip(text).all(|(atom, sc)| match *atom {
        PatternAtom::AnyChar => true,
        PatternAtom::Literal(ch) => same_character(*sc, ch, case),
        PatternAtom::AnySequence | PatternAtom::DanglingEscape => false,
    })
}

/// A bit-parallel search for one `%`-free segment, one bit per segment position.
///
/// Bit `i` of the state is set when the segment's first `i + 1` positions
/// match the text ending at the character just read, so reading a character
/// shifts the state up and keeps the positions that character can fill.
#[derive(Default)]
struct SegmentFinder {
    /// The segment's literal characters under the case rule, each with its position.
    keys: Vec<(char, usize)>,
    /// Where each ASCII character's mask starts in `masks`, 0 when the segment never names it.
    ascii: Vec<usize>,
    /// Where each other character's mask starts, sorted by character.
    others: Vec<(char, usize)>,
    /// Masks of `words` words each. The first holds the `_` positions, which
    /// every character fills, and is the mask of any character the segment
    /// never names.
    masks: Vec<u64>,
    state: Vec<u64>,
    /// State words updated so far, which is what the cost bound counts.
    #[cfg(test)]
    words_read: usize,
}

impl SegmentFinder {
    /// How far into `text` the leftmost occurrence of the non-empty `segment` ends.
    fn find(
        &mut self,
        segment: &[PatternAtom],
        text: &[char],
        case: crate::backend::TextCase,
    ) -> Option<usize> {
        let words = segment.len().div_ceil(64);
        self.prepare(segment, words, case);
        let top = (segment.len() - 1) / 64;
        let top_bit = 1u64 << ((segment.len() - 1) % 64);
        self.state.clear();
        self.state.resize(words, 0);
        for (read, &sc) in text.iter().enumerate() {
            let mask = self.mask_of(fold(sc, case));
            #[cfg(test)]
            {
                self.words_read += words;
            }
            let mut carry = 1;
            for (word, allowed) in self.state.iter_mut().zip(&self.masks[mask..mask + words]) {
                let shifted = (*word << 1) | carry;
                carry = *word >> 63;
                *word = shifted & allowed;
            }
            if self.state[top] & top_bit != 0 {
                return Some(read + 1);
            }
        }
        None
    }

    fn prepare(&mut self, segment: &[PatternAtom], words: usize, case: crate::backend::TextCase) {
        self.masks.clear();
        self.masks.resize(words, 0);
        self.keys.clear();
        for (position, atom) in segment.iter().enumerate() {
            match *atom {
                PatternAtom::AnyChar => self.masks[position / 64] |= 1 << (position % 64),
                PatternAtom::Literal(ch) => self.keys.push((fold(ch, case), position)),
                PatternAtom::AnySequence | PatternAtom::DanglingEscape => {}
            }
        }
        self.keys.sort_unstable();
        self.ascii.clear();
        self.ascii.resize(128, 0);
        self.others.clear();
        let mut previous = None;
        for &(ch, position) in &self.keys {
            if previous != Some(ch) {
                previous = Some(ch);
                let start = self.masks.len();
                self.masks.extend_from_within(..words);
                match ascii_code(ch) {
                    Some(code) => self.ascii[code] = start,
                    None => self.others.push((ch, start)),
                }
            }
            let start = self.masks.len() - words;
            self.masks[start + position / 64] |= 1 << (position % 64);
        }
    }

    fn mask_of(&self, ch: char) -> usize {
        ascii_code(ch).map_or_else(
            || {
                self.others
                    .binary_search_by_key(&ch, |(key, _)| *key)
                    .map_or(0, |found| self.others[found].1)
            },
            |code| self.ascii[code],
        )
    }
}

fn ascii_code(ch: char) -> Option<usize> {
    u8::try_from(ch).ok().filter(u8::is_ascii).map(usize::from)
}

/// A character as the case rule compares it.
const fn fold(ch: char, case: crate::backend::TextCase) -> char {
    match case {
        crate::backend::TextCase::Exact => ch,
        crate::backend::TextCase::AsciiNoCase => ch.to_ascii_lowercase(),
    }
}

/// Whether two pattern characters match under this engine's case rule.
///
/// Folded per character as the walk reaches it, rather than by lowercasing
/// either operand: a lowercased copy is an allocation per row, and a full
/// Unicode fold is also the wrong answer. Measured on PostgreSQL,
/// `lower('İ')` is one character where Rust's fold gives two, so folding
/// the whole string can change its length and let `_` match a character
/// the server never produced.
///
/// ASCII folding needs no such copy, because it maps each character to
/// exactly one character.
const fn same_character(left: char, right: char, case: crate::backend::TextCase) -> bool {
    match case {
        crate::backend::TextCase::Exact => left == right,
        crate::backend::TextCase::AsciiNoCase => left.eq_ignore_ascii_case(&right),
    }
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

    use super::arithmetic::is_zero_scalar;
    use super::{ComparisonRef, LikeScratch, PatternError, Vm, VmError};
    use crate::backend::{Postgres, RowKind, Value};
    use crate::compiler::{BytecodeProgram, Instruction, Tri};
    use crate::testing::TestEvent;
    use crate::types::EventKind;
    use alloc::vec::Vec;
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

    // Smoke coverage: a minimal set of tests guarding the VM contract.
    // The exhaustive behavioural suite (~2000 lines) is rewritten in a
    // follow-up delegation and will replace this block.

    #[test]
    fn simple_comparison_int_greater_than_literal() {
        let mut vm: Vm<Postgres> = Vm::new();
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan(ComparisonRef::NONE),
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
            Instruction::GreaterThan(ComparisonRef::NONE),
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
            Instruction::GreaterThan(ComparisonRef::NONE),
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
            Instruction::Equal(ComparisonRef::NONE),
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
            Instruction::In {
                literals: vec![
                    Value::String("pending".into()),
                    Value::String("active".into()),
                ],
                comparison: ComparisonRef::NONE,
            },
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
            Instruction::Add(None),
            Instruction::PushLiteral(Value::Int(10)),
            Instruction::GreaterThan(ComparisonRef::NONE),
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

    /// Division by zero under the PostgreSQL backend is a refusal, not a
    /// null: measured, the server raises `division by zero`. The VM
    /// surfaces it so the caller can report it against the one
    /// subscription that asked.
    #[test]
    fn division_by_zero_is_refused_under_postgres() {
        let mut vm: Vm<Postgres> = Vm::new();
        // (col0 / 0) > 1
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(0)),
            Instruction::Divide(None, crate::compiler::bytecode::Quotient::FromTheOperands),
            Instruction::PushLiteral(Value::Int(1)),
            Instruction::GreaterThan(ComparisonRef::NONE),
        ]);

        let e = insert_pg(vec![Value::Int(10)]);
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()),
            Err(VmError::Refused(
                super::refusal::EvaluationRefusal::DivisionByZero {
                    operation: super::refusal::ArithmeticOp::Divide,
                }
            ))
        );
    }

    #[test]
    fn pk_kind_rejects_non_pk_column() {
        let mut vm: Vm<Postgres> = Vm::new();
        // Predicate: col0 == 1
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(1)),
            Instruction::Equal(ComparisonRef::NONE),
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
                comparison: ComparisonRef::NONE,
                escape: Some('\\'),
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

    // Membership term slots

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
                vm.eval_with_terms(&program, &e, RowKind::New, &pg_catalog(), &[truth], None)
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
            vm.eval_with_terms(&program, &e, RowKind::New, &pg_catalog(), truths, None)
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
                Instruction::GreaterThan(ComparisonRef::NONE),
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
                &[Tri::True],
                None
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
                &[Tri::False],
                None
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
                &[Tri::True],
                None
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
            vm.eval_with_terms(
                &program,
                &e,
                RowKind::New,
                &pg_catalog(),
                &[Tri::True],
                None
            ),
            Err(VmError::MissingTermTruth(1)),
            "one truth supplied, slot 1 asked for: the caller is told, not answered"
        );
        assert_eq!(
            vm.eval(&program, &e, RowKind::New, &pg_catalog()),
            Err(VmError::MissingTermTruth(1)),
            "plain eval supplies no truths at all, so any slot is missing"
        );
    }

    /// A pattern whose last character is the escape character escapes
    /// nothing, and PostgreSQL refuses it *only* once its matcher arrives
    /// there with input still to read. The walk reports the same
    /// condition, so the two cases are distinguishable rather than both
    /// collapsed into a no-match.
    #[test]
    fn a_trailing_escape_is_reported_when_the_walk_reaches_it() {
        assert_eq!(
            LikeScratch::default().matches(
                "ab",
                r"a\",
                Some('\\'),
                crate::backend::TextCase::Exact
            ),
            Err(PatternError::TrailingEscape),
            "input remains when the matcher arrives, which is what PostgreSQL refuses"
        );
        assert_eq!(
            LikeScratch::default().matches("a", r"a\", Some('\\'), crate::backend::TextCase::Exact),
            Ok(false),
            "the input ran out first, and PostgreSQL answers false rather than raising"
        );
        assert_eq!(
            LikeScratch::default().matches(
                "axb",
                r"a%\",
                Some('\\'),
                crate::backend::TextCase::Exact
            ),
            Err(PatternError::TrailingEscape),
            "a wildcard ahead of it does not hide the dangling escape"
        );
    }

    /// Without a default escape the same pattern is well formed: the
    /// backslash is an ordinary character to be matched.
    #[test]
    fn a_trailing_backslash_is_ordinary_without_an_escape() {
        assert_eq!(
            LikeScratch::default().matches(r"a\", r"a\", None, crate::backend::TextCase::Exact),
            Ok(true)
        );
        assert_eq!(
            LikeScratch::default().matches("ab", r"a\", None, crate::backend::TextCase::Exact),
            Ok(false)
        );
    }

    /// The escape only escapes; it does not change what the wildcards mean
    /// elsewhere in the pattern.
    #[test]
    fn escaping_one_wildcard_leaves_the_others_alone() {
        assert_eq!(
            LikeScratch::default().matches(
                "a%xb",
                r"a\%%b",
                Some('\\'),
                crate::backend::TextCase::Exact
            ),
            Ok(true)
        );
        assert_eq!(
            LikeScratch::default().matches(
                "axxb",
                r"a\%%b",
                Some('\\'),
                crate::backend::TextCase::Exact
            ),
            Ok(false)
        );
    }

    /// The quadratic walk the matcher replaced, kept as the oracle it must agree with.
    fn walk(
        string: &str,
        pattern: &str,
        escape: Option<char>,
        case: crate::backend::TextCase,
    ) -> Result<bool, PatternError> {
        #[derive(Clone, Copy, PartialEq)]
        enum Atom {
            Any,
            One,
            Char(char),
            Dangling,
        }
        let mut atoms = Vec::new();
        let mut chars = pattern.chars();
        while let Some(ch) = chars.next() {
            atoms.push(if Some(ch) == escape {
                chars.next().map_or(Atom::Dangling, Atom::Char)
            } else {
                match ch {
                    '%' => Atom::Any,
                    '_' => Atom::One,
                    other => Atom::Char(other),
                }
            });
        }
        let n = atoms.len();
        let mut reached = alloc::vec![false; n + 1];
        reached[0] = true;
        for j in 0..n {
            if atoms[j] != Atom::Any {
                break;
            }
            reached[j + 1] = reached[j];
        }
        for sc in string.chars() {
            let mut next = alloc::vec![false; n + 1];
            for j in 0..n {
                if !(reached[j] || (atoms[j] == Atom::Any && next[j])) {
                    continue;
                }
                match atoms[j] {
                    Atom::Any => {
                        next[j] = true;
                        next[j + 1] = true;
                    }
                    Atom::One => next[j + 1] |= reached[j],
                    Atom::Char(ch) => {
                        next[j + 1] |= reached[j] && super::same_character(sc, ch, case);
                    }
                    Atom::Dangling => return Err(PatternError::TrailingEscape),
                }
            }
            reached = next;
        }
        Ok(reached[n])
    }

    fn every_word(alphabet: &[char], longest: usize) -> Vec<alloc::string::String> {
        let mut words = alloc::vec![alloc::string::String::new()];
        let mut last = alloc::vec![alloc::string::String::new()];
        for _ in 0..longest {
            last = last
                .iter()
                .flat_map(|word| alphabet.iter().map(move |ch| alloc::format!("{word}{ch}")))
                .collect();
            words.extend(last.iter().cloned());
        }
        words
    }

    #[test]
    fn the_matcher_agrees_with_the_walk_on_every_short_pattern() {
        let patterns = every_word(&['a', 'B', '%', '_', '\\'], 4);
        let texts = every_word(&['a', 'A', 'b', 'é'], 5);
        let mut like = LikeScratch::default();
        for case in [
            crate::backend::TextCase::Exact,
            crate::backend::TextCase::AsciiNoCase,
        ] {
            for escape in [Some('\\'), None] {
                for pattern in &patterns {
                    for text in &texts {
                        assert_eq!(
                            like.matches(text, pattern, escape, case),
                            walk(text, pattern, escape, case),
                            "{text:?} LIKE {pattern:?} escape {escape:?} {case:?}"
                        );
                    }
                }
            }
        }
    }

    /// Segments longer than one machine word, over texts spelled from the
    /// pattern itself so they match or miss by one character, from a fixed seed.
    #[test]
    fn the_matcher_agrees_with_the_walk_on_long_segments() {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut draw = |bound: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % bound
        };
        let letter = |pick: u64| ['a', 'b', 'A', 'B'][usize::try_from(pick).unwrap()];
        let mut like = LikeScratch::default();
        for _ in 0..4_000 {
            let pattern: alloc::string::String = (0..draw(300))
                .map(|_| match draw(60) {
                    0 => '%',
                    1..=6 => '_',
                    pick => letter(pick % 4),
                })
                .collect();
            let mut text: Vec<char> = Vec::new();
            for ch in pattern.chars() {
                match ch {
                    '%' => text.extend((0..draw(90)).map(|_| letter(draw(4)))),
                    '_' => text.push(letter(draw(4))),
                    literal if draw(2) == 0 => text.push(literal.to_ascii_uppercase()),
                    literal => text.push(literal),
                }
            }
            if !text.is_empty() && draw(2) == 0 {
                let at = usize::try_from(draw(u64::try_from(text.len()).unwrap())).unwrap();
                text[at] = letter(draw(4));
            }
            let text: alloc::string::String = text.into_iter().collect();
            for case in [
                crate::backend::TextCase::Exact,
                crate::backend::TextCase::AsciiNoCase,
            ] {
                assert_eq!(
                    like.matches(&text, &pattern, Some('\\'), case),
                    walk(&text, &pattern, Some('\\'), case),
                    "{text:?} LIKE {pattern:?} {case:?}"
                );
            }
        }
    }

    /// State words one match updates. The walk this matcher replaced paired
    /// every pattern position with every text character, 6,000 by 1,000,000
    /// in the first case below.
    fn words_read(text: &str, pattern: &str) -> (Result<bool, PatternError>, usize) {
        let mut like = LikeScratch::default();
        let matched = like.matches(text, pattern, None, crate::backend::TextCase::Exact);
        (matched, like.finder.words_read)
    }

    #[test]
    fn a_missed_segment_reads_the_text_once() {
        let text = "b".repeat(1_000_000);
        let (matched, read) = words_read(&text, &"%a".repeat(3_000));
        assert_eq!(matched, Ok(false));
        assert!(
            read <= text.len(),
            "{read} words over {} characters",
            text.len()
        );
    }

    #[test]
    fn segments_found_in_turn_read_the_text_between_them_once() {
        let text = "a".repeat(1_000_000);
        let (matched, read) = words_read(&text, &format!("{}%", "%a".repeat(3_000)));
        assert_eq!(matched, Ok(true));
        assert!(
            read <= text.len(),
            "{read} words over {} characters",
            text.len()
        );
    }

    /// A segment that nearly matches at every position costs one word per 64
    /// of its positions per character.
    #[test]
    fn a_long_segment_reads_the_text_once_per_word_it_fills() {
        let text = "a".repeat(1_000_000);
        let segment = format!("{}b", "a".repeat(2_000));
        let (matched, read) = words_read(&text, &format!("%{segment}%"));
        assert_eq!(matched, Ok(false));
        let words = segment.len().div_ceil(64);
        assert!(
            read <= text.len() * words,
            "{read} words over {} characters",
            text.len()
        );
    }
}
