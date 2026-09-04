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

use crate::backend::{Backend, Value};
use crate::types::ColumnId;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

/// VM instruction for tri-state predicate evaluation.
///
/// Parameterised on the observed [`Backend`] so that `PushLiteral` and
/// `In` carry backend-typed payloads. Every other variant
/// is backend-agnostic but shares the type parameter for uniform storage.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Instruction<B: Backend> {
    // Stack Operations
    /// Push a literal [`Value`] onto the stack.
    ///
    /// Stack: `[...] -> [..., Value]`.
    PushLiteral(Value<B>),

    /// Read a cell from the current CDC event and push it onto the stack.
    ///
    /// The VM reads the cell through
    /// [`CdcEvent::value_at`](crate::backend::CdcEvent::value_at), which
    /// decodes it against the catalog and returns an owned [`Value`].
    ///
    /// Stack: `[...] -> [..., Value]`.
    LoadColumn(ColumnId),

    // Comparison Operators (pop 2 values, push Tri)
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

    // NULL Checks (pop 1 value, push Tri)
    /// IS NULL check. `Missing` and `Null` both satisfy `IS NULL`.
    ///
    /// Stack: `[..., value] -> [..., Tri]`.
    IsNull,

    /// IS NOT NULL check. Any concrete `Value::T(_)` satisfies `IS NOT NULL`.
    ///
    /// Stack: `[..., value] -> [..., Tri]`.
    IsNotNull,

    // Logical Operators (pop 2 Tri, push Tri)
    /// AND with tri-state semantics.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    And,

    /// OR with tri-state semantics.
    ///
    /// Stack: `[..., a, b] -> [..., Tri]`.
    Or,

    // Unary Operators (pop 1 Tri, push Tri)
    /// NOT with tri-state semantics.
    ///
    /// Stack: `[..., tri] -> [..., Tri]`.
    Not,

    // Arithmetic Operators (pop 2 values, push value)
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

    // Special Operations
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

    // Control Flow (short-circuit evaluation)
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

    // Membership terms
    /// Push the truth supplied for membership term slot `slot`.
    ///
    /// A membership term is not a row test: it answers which subscribers the
    /// changed row admits, and the same row admits different subscribers. So the
    /// caller supplies one truth per term slot and evaluates the program once
    /// per assignment, which is what [`Vm::eval_with_terms`] takes.
    ///
    /// A slot the caller supplied no truth for is
    /// [`VmError::MissingTermTruth`], never a silent `Tri::Unknown`: the
    /// silent answer would deliver the row to every subscriber sharing the
    /// predicate.
    ///
    /// Stack: `[...] -> [..., Tri]`.
    ///
    /// [`Vm::eval_with_terms`]: crate::compiler::Vm::eval_with_terms
    /// [`VmError::MissingTermTruth`]: crate::compiler::VmError::MissingTermTruth
    TermTruth(u16),
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

    /// The column each membership term slot compares, indexed by slot.
    ///
    /// Empty for a filter carrying no term, which is every filter the
    /// `membership-term` feature does not compile. Dispatch reads the columns at
    /// `term_columns[slot]` off the changed row to learn which subscribers the
    /// row admits through that term, so it travels with the program rather than
    /// beside it: the program is what persistence stores and reloads, and a
    /// reloaded term with no columns would narrow nothing and deliver the row to
    /// every subscriber sharing the predicate.
    pub term_columns: Vec<Vec<ColumnId>>,

    /// The catalog facts each loaded column's comparisons depend on, sorted
    /// by column id.
    ///
    /// Resolved once at registration so no comparison consults the catalog
    /// per row, and carried in the program because the program is what
    /// persistence stores and reloads. A column absent here has no resolved
    /// facts and compares structurally.
    pub column_comparisons: Vec<(ColumnId, crate::backend::ColumnComparisonOf<B>)>,
}

impl<B: Backend> BytecodeProgram<B> {
    /// Build a term-free program from an instruction sequence, extracting the
    /// dependency-column set once at construction.
    #[must_use]
    pub fn new(instructions: Vec<Instruction<B>>) -> Self {
        Self::with_terms(instructions, Vec::new())
    }

    /// Build a program whose slot `i` carries a membership term comparing
    /// `term_columns[i]`, one entry per compared column and in written order.
    ///
    /// A term column is a dependency like any other: an UPDATE touching only
    /// a column a term compares moves which subscribers the row admits, so a
    /// program pruned on the load set alone would miss it.
    #[must_use]
    pub fn with_terms(instructions: Vec<Instruction<B>>, term_columns: Vec<Vec<ColumnId>>) -> Self {
        Self::with_comparisons(instructions, term_columns, Vec::new())
    }

    /// Build a program whose loaded columns carry resolved comparison facts.
    #[must_use]
    pub fn with_comparisons(
        instructions: Vec<Instruction<B>>,
        term_columns: Vec<Vec<ColumnId>>,
        mut column_comparisons: Vec<(ColumnId, crate::backend::ColumnComparisonOf<B>)>,
    ) -> Self {
        column_comparisons.sort_by_key(|(column, _)| *column);
        let dependency_columns = Self::extract_dependencies(&instructions, &term_columns);
        Self {
            instructions,
            dependency_columns,
            term_columns,
            column_comparisons,
        }
    }

    /// The resolved comparison facts for `column`, or `None` when the
    /// compiler resolved none.
    #[must_use]
    pub fn comparison_for(
        &self,
        column: ColumnId,
    ) -> Option<&crate::backend::ColumnComparisonOf<B>> {
        self.column_comparisons
            .binary_search_by_key(&column, |(id, _)| *id)
            .ok()
            .map(|index| &self.column_comparisons[index].1)
    }

    /// Install the comparison facts the compiler resolved for this program's
    /// loaded columns.
    pub fn set_column_comparisons(
        &mut self,
        mut comparisons: Vec<(ColumnId, crate::backend::ColumnComparisonOf<B>)>,
    ) {
        comparisons.sort_by_key(|(column, _)| *column);
        self.column_comparisons = comparisons;
    }

    /// Column ids referenced by any [`Instruction::LoadColumn`] in
    /// `instructions`, plus every column a term compares. Sorted and
    /// deduplicated.
    fn extract_dependencies(
        instructions: &[Instruction<B>],
        term_columns: &[Vec<ColumnId>],
    ) -> Vec<ColumnId> {
        let mut cols: Vec<ColumnId> = instructions
            .iter()
            .filter_map(|inst| {
                if let Instruction::LoadColumn(col_id) = inst {
                    Some(*col_id)
                } else {
                    None
                }
            })
            .chain(term_columns.iter().flatten().copied())
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

// Manual `Clone` / `Debug` / `PartialEq` impls on `Instruction<B>` and
// `BytecodeProgram<B>` — `#[derive(...)]` would infer `B: Clone` etc.,
// which is not implied by `Backend`. See the matching hand-impls on
// `Value<B>` in `crate::backend`.

impl<B: Backend> Clone for Instruction<B> {
    fn clone(&self) -> Self {
        match self {
            Self::PushLiteral(v) => Self::PushLiteral(v.clone()),
            Self::LoadColumn(col) => Self::LoadColumn(*col),
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
            Self::TermTruth(slot) => Self::TermTruth(*slot),
        }
    }
}

impl<B: Backend> core::fmt::Debug for Instruction<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PushLiteral(v) => f.debug_tuple("PushLiteral").field(v).finish(),
            Self::LoadColumn(col) => f.debug_tuple("LoadColumn").field(col).finish(),
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
            Self::TermTruth(slot) => f.debug_tuple("TermTruth").field(slot).finish(),
        }
    }
}

impl<B: Backend> PartialEq for Instruction<B> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PushLiteral(a), Self::PushLiteral(b)) => a == b,
            (Self::LoadColumn(ac), Self::LoadColumn(bc)) => ac == bc,
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
            (Self::TermTruth(a), Self::TermTruth(b)) => a == b,
            _ => false,
        }
    }
}

impl<B: Backend> Clone for BytecodeProgram<B> {
    fn clone(&self) -> Self {
        Self {
            instructions: self.instructions.clone(),
            dependency_columns: self.dependency_columns.clone(),
            term_columns: self.term_columns.clone(),
            column_comparisons: self.column_comparisons.clone(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for BytecodeProgram<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BytecodeProgram")
            .field("instructions", &self.instructions)
            .field("dependency_columns", &self.dependency_columns)
            .field("term_columns", &self.term_columns)
            .field("column_comparisons", &self.column_comparisons)
            .finish()
    }
}

impl<B: Backend> PartialEq for BytecodeProgram<B> {
    fn eq(&self, other: &Self) -> bool {
        self.instructions == other.instructions
            && self.dependency_columns == other.dependency_columns
            && self.term_columns == other.term_columns
            && self.column_comparisons == other.column_comparisons
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Postgres;

    #[test]
    fn test_extract_dependencies() {
        // age > 18 AND status = 'active'
        let instructions: Vec<Instruction<Postgres>> = vec![
            Instruction::LoadColumn(5), // age
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(7), // status
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
            Instruction::LoadColumn(5),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(5), // Same column again
            Instruction::PushLiteral(Value::Int(65)),
            Instruction::LessThan,
            Instruction::And,
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, vec![5]); // Deduplicated
    }

    /// The column a term compares is a dependency even though no
    /// [`Instruction::LoadColumn`] reads it, because an UPDATE touching only
    /// that column moves which subscribers the row admits. A program pruned on
    /// the load set alone would skip the event that matters most.
    #[test]
    fn a_term_column_is_a_dependency_without_a_load() {
        let instructions: Vec<Instruction<Postgres>> = vec![
            Instruction::LoadColumn(5),
            Instruction::PushLiteral(Value::Int(18)),
            Instruction::GreaterThan,
            Instruction::JumpIfFalse(3),
            Instruction::TermTruth(0),
            Instruction::And,
        ];

        let program = BytecodeProgram::with_terms(instructions, vec![vec![9]]);
        assert_eq!(program.dependency_columns, vec![5, 9]);
        assert_eq!(program.term_columns, vec![vec![9]]);
        assert!(!program.is_constant());
    }

    /// A filter whose only test is a term still depends on the column the term
    /// compares, so it is not a constant program that registration could fold.
    #[test]
    fn a_filter_of_one_term_alone_is_not_constant() {
        let program: BytecodeProgram<Postgres> =
            BytecodeProgram::with_terms(vec![Instruction::TermTruth(0)], vec![vec![3]]);
        assert_eq!(program.dependency_columns, vec![3]);
        assert!(!program.is_constant());
    }
}
