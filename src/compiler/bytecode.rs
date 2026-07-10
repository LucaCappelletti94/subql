//! VM bytecode instruction set for predicate evaluation.
//!
//! [`Instruction`] and [`BytecodeProgram`] are parameterised on the observed
//! [`Backend`] rather than on any specific event type. A compiled program is
//! reusable across every `E: CdcEvent<Backend = B>` — the coupling between
//! bytecode and event lives on the VM's `eval` method, not on this storage
//! shape. See `src/backend.rs` and `docs/refactor-cdc-event-handoff.md`
//! for the wider trait design.
//!
//! # Same-scalar arithmetic
//!
//! Arithmetic instructions ([`Instruction::Add`], `Subtract`, `Multiply`,
//! `Divide`, `Modulo`, `Negate`) are same-scalar only. The VM never performs
//! runtime `Int` <-> `Float` coercion; the compiler emits an explicit cast
//! (once that instruction lands) if a query mixes types. This preserves the
//! trait-level invariant that `Backend::Int` and `Backend::Float` are
//! independent Rust types with independent arithmetic surfaces.

use crate::backend::{Backend, ScalarKind, Value};
use crate::types::ColumnId;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

/// VM instruction for tri-state predicate evaluation.
///
/// Parameterised on the observed [`Backend`] so that `PushLiteral`,
/// `LoadColumn`, and `In` carry backend-typed payloads. Every other variant
/// is backend-agnostic but shares the type parameter for uniform storage.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Instruction<B: Backend> {
    // ========================================================================
    // Stack Operations
    // ========================================================================
    /// Push a literal [`Value`] onto the stack.
    ///
    /// Stack: `[...] -> [..., Value]`.
    PushLiteral(Value<B>),

    /// Read a cell from the current CDC event and push it onto the stack.
    ///
    /// The [`ScalarKind`] tag names which typed accessor on
    /// [`crate::backend::CdcEvent`] to call (`bool_at`, `int_at`, ...). The
    /// VM lifts the returned `Presence<&B::T>` into `Value::Missing`,
    /// `Value::Null`, or `Value::T(v.clone())` respectively.
    ///
    /// Stack: `[...] -> [..., Value]`.
    LoadColumn(ColumnId, ScalarKind),

    // ========================================================================
    // Comparison Operators (pop 2 values, push Tri)
    // ========================================================================
    /// Equal: `a = b`.
    ///
    /// NULL-safe: any `Missing` / `Null` operand yields `Tri::Unknown`. Same
    /// scalar variant compares via [`PartialEq`]; cross-variant yields
    /// `Tri::False`.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    Equal,

    /// Not equal: `a != b`. Complement of [`Equal`](Self::Equal) on defined
    /// operands; still `Tri::Unknown` on `Missing` / `Null`.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    NotEqual,

    /// Less than: `a < b`.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    LessThan,

    /// Less than or equal: `a <= b`.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    LessThanOrEqual,

    /// Greater than: `a > b`.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    GreaterThan,

    /// Greater than or equal: `a >= b`.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    GreaterThanOrEqual,

    // ========================================================================
    // NULL Checks (pop 1 value, push Tri)
    // ========================================================================
    /// IS NULL check. `Missing` and `Null` both satisfy `IS NULL`.
    ///
    /// Stack: `[..., value] -> [..., Tri]`.
    IsNull,

    /// IS NOT NULL check. Any concrete `Value::T(_)` satisfies `IS NOT NULL`.
    ///
    /// Stack: `[..., value] -> [..., Tri]`.
    IsNotNull,

    // ========================================================================
    // Logical Operators (pop 2 Tri, push Tri)
    // ========================================================================
    /// AND with tri-state semantics.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    And,

    /// OR with tri-state semantics.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    Or,

    // ========================================================================
    // Unary Operators (pop 1 Tri, push Tri)
    // ========================================================================
    /// NOT with tri-state semantics.
    ///
    /// Stack: `[..., tri] -> [..., Tri]`.
    Not,

    // ========================================================================
    // Arithmetic Operators (pop 2 values, push value)
    // ========================================================================
    /// Add: `a + b`.
    ///
    /// Same-scalar only: `Int + Int -> Int`, `Float + Float -> Float`,
    /// `Decimal + Decimal -> Decimal`. Any other pair (including
    /// cross-scalar and `Missing` / `Null` operands) yields `Value::Null`.
    ///
    /// Stack: `[..., a, b] -> [..., Value]`.
    Add,

    /// Subtract: `a - b`. Same-scalar rules as [`Add`](Self::Add).
    ///
    /// Stack: `[..., a, b] -> [..., Value]`.
    Subtract,

    /// Multiply: `a * b`. Same-scalar rules as [`Add`](Self::Add).
    ///
    /// Stack: `[..., a, b] -> [..., Value]`.
    Multiply,

    /// Divide: `a / b`. Same-scalar rules as [`Add`](Self::Add). Division by
    /// zero yields `Value::Null`. Integer division is truncated (result is
    /// `Int` when both operands are `Int`); the compiler emits an explicit
    /// cast to `Float` upstream when the query wants float division.
    ///
    /// Stack: `[..., a, b] -> [..., Value]`.
    Divide,

    /// Modulo: `a % b`. `Int % Int -> Int` only (SQL modulo is undefined on
    /// floats). Any other pair, or a zero divisor, yields `Value::Null`.
    ///
    /// Stack: `[..., a, b] -> [..., Value]`.
    Modulo,

    /// Negate: `-a` (unary minus).
    ///
    /// Preserves scalar variant: `Int -> Int`, `Float -> Float`,
    /// `Decimal -> Decimal`. Other variants and `Missing` / `Null` yield
    /// `Value::Null`.
    ///
    /// Stack: `[..., a] -> [..., Value]`.
    Negate,

    // ========================================================================
    // Special Operations
    // ========================================================================
    /// `IN (...)`: membership test against a literal set.
    ///
    /// `Missing` / `Null` on the stack yields `Tri::Unknown`. A `Null` in
    /// the RHS list produces `Tri::Unknown` when no other literal matches
    /// (SQL standard: `x IN (1, NULL)` is `Unknown` when `x != 1`).
    ///
    /// Stack: `[..., value] -> [..., Tri]`.
    In(Vec<Value<B>>),

    /// `BETWEEN a AND b`: closed-range membership. Equivalent to
    /// `value >= lower AND value <= upper`.
    ///
    /// Any `Missing` / `Null` operand yields `Tri::Unknown`.
    ///
    /// Stack: `[..., value, lower, upper] -> [..., Tri]`.
    Between,

    /// `LIKE` pattern matching against a text scalar. `%` matches zero or
    /// more characters, `_` matches exactly one character. No ESCAPE clause
    /// support.
    ///
    /// Non-string operands yield `Tri::Unknown`. `Missing` / `Null` operands
    /// yield `Tri::Unknown`.
    ///
    /// Stack: `[..., string, pattern] -> [..., Tri]`.
    Like {
        /// When `false`, both operands are lowercased before matching.
        case_sensitive: bool,
    },

    // ========================================================================
    // Control Flow (short-circuit evaluation)
    // ========================================================================
    /// Jump forward `offset` instructions if top-of-stack is `Tri::False`.
    /// Used for `AND` short-circuiting.
    ///
    /// The TOS is peeked, not popped, so the `False` remains as the result
    /// of the enclosing `AND`. `offset == 0` is a compiler bug (would loop);
    /// the VM rejects it as [`crate::compiler::VmError::BadJump`].
    ///
    /// Stack: `[..., tri] -> [..., tri]` (no change).
    JumpIfFalse(usize),

    /// Jump forward `offset` instructions if top-of-stack is `Tri::True`.
    /// Used for `OR` short-circuiting.
    ///
    /// Symmetric to [`JumpIfFalse`](Self::JumpIfFalse).
    JumpIfTrue(usize),
}

/// A compiled bytecode program.
///
/// Parameterised on the observed [`Backend`]; reusable across any
/// `E: CdcEvent<Backend = B>`. The compiler builds one program per
/// subscription predicate; the runtime interprets it per incoming event.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct BytecodeProgram<B: Backend> {
    /// Instruction sequence in evaluation order.
    pub instructions: Vec<Instruction<B>>,

    /// Column ids referenced by any [`Instruction::LoadColumn`] in this
    /// program, sorted and deduplicated.
    ///
    /// Used by the dispatch layer to skip predicate evaluation entirely when
    /// an UPDATE event's `changed_columns` is disjoint from this set — a
    /// program whose dependencies did not change cannot change verdict.
    pub dependency_columns: Vec<ColumnId>,
}

impl<B: Backend> BytecodeProgram<B> {
    /// Build a program from an instruction sequence, extracting the
    /// dependency-column set once at construction.
    #[must_use]
    pub fn new(instructions: Vec<Instruction<B>>) -> Self {
        let dependency_columns = Self::extract_dependencies(&instructions);
        Self {
            instructions,
            dependency_columns,
        }
    }

    /// Column ids referenced by any [`Instruction::LoadColumn`] in
    /// `instructions`. Sorted and deduplicated.
    fn extract_dependencies(instructions: &[Instruction<B>]) -> Vec<ColumnId> {
        let mut cols: Vec<ColumnId> = instructions
            .iter()
            .filter_map(|inst| {
                if let Instruction::LoadColumn(col_id, _) = inst {
                    Some(*col_id)
                } else {
                    None
                }
            })
            .collect();
        cols.sort_unstable();
        cols.dedup();
        cols
    }

    /// True when the program has no column dependencies. A constant program
    /// always evaluates to the same result and can be folded at registration
    /// time.
    #[must_use]
    pub const fn is_constant(&self) -> bool {
        self.dependency_columns.is_empty()
    }
}

#[cfg(any())] // Phase 10 test port pending
mod tests {
    use super::*;
    use crate::backend::Postgres;

    #[test]
    fn test_extract_dependencies() {
        // age > 18 AND status = 'active'
        let instructions: Vec<Instruction<Postgres>> = vec![
            Instruction::LoadColumn(5, ScalarKind::Int), // age
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(7, ScalarKind::String), // status
            Instruction::PushLiteral(Value::String("active".into())),
            Instruction::Equal,
            Instruction::And,
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, vec![5, 7]);
        assert!(!program.is_constant());
    }

    #[test]
    fn test_constant_program() {
        // Just a literal true (e.g., WHERE true)
        let instructions: Vec<Instruction<Postgres>> =
            vec![Instruction::PushLiteral(Value::Bool(true))];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, Vec::<ColumnId>::new());
        assert!(program.is_constant());
    }

    #[test]
    fn test_dependency_deduplication() {
        // age > 18 AND age < 65 (age used twice)
        let instructions: Vec<Instruction<Postgres>> = vec![
            Instruction::LoadColumn(5, ScalarKind::Int),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(5, ScalarKind::Int), // Same column again
            Instruction::PushLiteral(Value::Int(65)),
            Instruction::LessThan,
            Instruction::And,
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, vec![5]); // Deduplicated
    }
}

// Manual `Clone` / `Debug` / `PartialEq` impls on `Instruction<B>` and
// `BytecodeProgram<B>` — `#[derive(...)]` would infer `B: Clone` etc.,
// which is not implied by `Backend`. See the matching hand-impls on
// `Value<B>` in `crate::backend`.

impl<B: Backend> Clone for Instruction<B> {
    fn clone(&self) -> Self {
        match self {
            Self::PushLiteral(v) => Self::PushLiteral(v.clone()),
            Self::LoadColumn(col, kind) => Self::LoadColumn(*col, *kind),
            Self::Equal => Self::Equal,
            Self::NotEqual => Self::NotEqual,
            Self::LessThan => Self::LessThan,
            Self::LessThanOrEqual => Self::LessThanOrEqual,
            Self::GreaterThan => Self::GreaterThan,
            Self::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            Self::IsNull => Self::IsNull,
            Self::IsNotNull => Self::IsNotNull,
            Self::And => Self::And,
            Self::Or => Self::Or,
            Self::Not => Self::Not,
            Self::Add => Self::Add,
            Self::Subtract => Self::Subtract,
            Self::Multiply => Self::Multiply,
            Self::Divide => Self::Divide,
            Self::Modulo => Self::Modulo,
            Self::Negate => Self::Negate,
            Self::In(lits) => Self::In(lits.clone()),
            Self::Between => Self::Between,
            Self::Like { case_sensitive } => Self::Like {
                case_sensitive: *case_sensitive,
            },
            Self::JumpIfFalse(offset) => Self::JumpIfFalse(*offset),
            Self::JumpIfTrue(offset) => Self::JumpIfTrue(*offset),
        }
    }
}

impl<B: Backend> core::fmt::Debug for Instruction<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PushLiteral(v) => f.debug_tuple("PushLiteral").field(v).finish(),
            Self::LoadColumn(col, kind) => {
                f.debug_tuple("LoadColumn").field(col).field(kind).finish()
            }
            Self::Equal => f.write_str("Equal"),
            Self::NotEqual => f.write_str("NotEqual"),
            Self::LessThan => f.write_str("LessThan"),
            Self::LessThanOrEqual => f.write_str("LessThanOrEqual"),
            Self::GreaterThan => f.write_str("GreaterThan"),
            Self::GreaterThanOrEqual => f.write_str("GreaterThanOrEqual"),
            Self::IsNull => f.write_str("IsNull"),
            Self::IsNotNull => f.write_str("IsNotNull"),
            Self::And => f.write_str("And"),
            Self::Or => f.write_str("Or"),
            Self::Not => f.write_str("Not"),
            Self::Add => f.write_str("Add"),
            Self::Subtract => f.write_str("Subtract"),
            Self::Multiply => f.write_str("Multiply"),
            Self::Divide => f.write_str("Divide"),
            Self::Modulo => f.write_str("Modulo"),
            Self::Negate => f.write_str("Negate"),
            Self::In(lits) => f.debug_tuple("In").field(lits).finish(),
            Self::Between => f.write_str("Between"),
            Self::Like { case_sensitive } => f
                .debug_struct("Like")
                .field("case_sensitive", case_sensitive)
                .finish(),
            Self::JumpIfFalse(offset) => f.debug_tuple("JumpIfFalse").field(offset).finish(),
            Self::JumpIfTrue(offset) => f.debug_tuple("JumpIfTrue").field(offset).finish(),
        }
    }
}

impl<B: Backend> PartialEq for Instruction<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PushLiteral(a), Self::PushLiteral(b)) => a == b,
            (Self::LoadColumn(ac, ak), Self::LoadColumn(bc, bk)) => ac == bc && ak == bk,
            (Self::Equal, Self::Equal)
            | (Self::NotEqual, Self::NotEqual)
            | (Self::LessThan, Self::LessThan)
            | (Self::LessThanOrEqual, Self::LessThanOrEqual)
            | (Self::GreaterThan, Self::GreaterThan)
            | (Self::GreaterThanOrEqual, Self::GreaterThanOrEqual)
            | (Self::IsNull, Self::IsNull)
            | (Self::IsNotNull, Self::IsNotNull)
            | (Self::And, Self::And)
            | (Self::Or, Self::Or)
            | (Self::Not, Self::Not)
            | (Self::Add, Self::Add)
            | (Self::Subtract, Self::Subtract)
            | (Self::Multiply, Self::Multiply)
            | (Self::Divide, Self::Divide)
            | (Self::Modulo, Self::Modulo)
            | (Self::Negate, Self::Negate)
            | (Self::Between, Self::Between) => true,
            (Self::In(a), Self::In(b)) => a == b,
            (Self::Like { case_sensitive: a }, Self::Like { case_sensitive: b }) => a == b,
            (Self::JumpIfFalse(a), Self::JumpIfFalse(b))
            | (Self::JumpIfTrue(a), Self::JumpIfTrue(b)) => a == b,
            _ => false,
        }
    }
}

impl<B: Backend> Clone for BytecodeProgram<B> {
    fn clone(&self) -> Self {
        Self {
            instructions: self.instructions.clone(),
            dependency_columns: self.dependency_columns.clone(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for BytecodeProgram<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BytecodeProgram")
            .field("instructions", &self.instructions)
            .field("dependency_columns", &self.dependency_columns)
            .finish()
    }
}

impl<B: Backend> PartialEq for BytecodeProgram<B> {
    fn eq(&self, other: &Self) -> bool {
        self.instructions == other.instructions
            && self.dependency_columns == other.dependency_columns
    }
}
