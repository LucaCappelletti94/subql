//! VM bytecode instruction set for predicate evaluation

use crate::{Cell, ColumnId};

/// VM instruction for tri-state predicate evaluation
#[derive(Clone, Debug, PartialEq)]
pub enum Instruction {
    // ========================================================================
    // Stack Operations
    // ========================================================================

    /// Push a literal cell value onto stack
    ///
    /// Stack: [...] → [..., cell]
    PushLiteral(Cell),

    /// Load cell from row at column index and push onto stack
    ///
    /// If column out of bounds, pushes Cell::Missing.
    /// Stack: [...] → [..., cell]
    LoadColumn(ColumnId),

    // ========================================================================
    // Comparison Operators (pop 2 cells, push Tri)
    // ========================================================================

    /// Equal: a = b
    ///
    /// NULL-safe: NULL = NULL → Unknown (not True!)
    /// Stack: [..., a, b] → [..., Tri]
    Equal,

    /// Not equal: a != b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    NotEqual,

    /// Less than: a < b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    LessThan,

    /// Less than or equal: a <= b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    LessThanOrEqual,

    /// Greater than: a > b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    GreaterThan,

    /// Greater than or equal: a >= b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    GreaterThanOrEqual,

    // ========================================================================
    // NULL Checks (pop 1 cell, push Tri)
    // ========================================================================

    /// IS NULL check
    ///
    /// Stack: [..., cell] → [..., Tri]
    IsNull,

    /// IS NOT NULL check
    ///
    /// Stack: [..., cell] → [..., Tri]
    IsNotNull,

    // ========================================================================
    // Logical Operators (pop 2 Tri, push Tri)
    // ========================================================================

    /// AND with tri-state semantics
    ///
    /// Stack: [..., a, b] → [..., Tri]
    And,

    /// OR with tri-state semantics
    ///
    /// Stack: [..., a, b] → [..., Tri]
    Or,

    // ========================================================================
    // Unary Operators (pop 1 Tri, push Tri)
    // ========================================================================

    /// NOT with tri-state semantics
    ///
    /// Stack: [..., tri] → [..., Tri]
    Not,

    // ========================================================================
    // Special Operations
    // ========================================================================

    /// IN (...) - checks if top of stack is in literal set
    ///
    /// NULL IN (...) → Unknown
    /// Stack: [..., cell] → [..., Tri]
    In(Vec<Cell>),

    /// BETWEEN a AND b - checks if value is in range [a, b]
    ///
    /// Pops upper, lower, value. Equivalent to: value >= lower AND value <= upper
    /// Stack: [..., value, lower, upper] → [..., Tri]
    Between,

    /// LIKE pattern matching (optional, can defer to Phase 2)
    ///
    /// Stack: [..., string, pattern] → [..., Tri]
    Like { case_sensitive: bool },
}

/// Compiled bytecode program
#[derive(Clone, Debug)]
pub struct BytecodeProgram {
    /// Instruction sequence
    pub instructions: Vec<Instruction>,

    /// Columns referenced by this program (for dependency tracking)
    ///
    /// Sorted, deduplicated list of ColumnIds used in LoadColumn instructions.
    /// Used for UPDATE optimization (skip evaluation if no dependencies changed).
    pub dependency_columns: Vec<ColumnId>,
}

impl BytecodeProgram {
    /// Create new bytecode program with dependency extraction
    #[must_use]
    pub fn new(instructions: Vec<Instruction>) -> Self {
        let dependency_columns = Self::extract_dependencies(&instructions);
        Self {
            instructions,
            dependency_columns,
        }
    }

    /// Extract columns referenced by this program
    fn extract_dependencies(instructions: &[Instruction]) -> Vec<ColumnId> {
        let mut cols: Vec<ColumnId> = instructions
            .iter()
            .filter_map(|inst| {
                if let Instruction::LoadColumn(col_id) = inst {
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

    /// Returns true if this program has no dependencies (always evaluates to same result)
    #[must_use]
    pub fn is_constant(&self) -> bool {
        self.dependency_columns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_dependencies() {
        // age > 18 AND status = 'active'
        let instructions = vec![
            Instruction::LoadColumn(5),  // age
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(7),  // status
            Instruction::PushLiteral(Cell::String("active".into())),
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
        let instructions = vec![
            Instruction::PushLiteral(Cell::Bool(true)),
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, Vec::<ColumnId>::new());
        assert!(program.is_constant());
    }

    #[test]
    fn test_dependency_deduplication() {
        // age > 18 AND age < 65 (age used twice)
        let instructions = vec![
            Instruction::LoadColumn(5),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(5),  // Same column again
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::LessThan,
            Instruction::And,
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, vec![5]); // Deduplicated
    }
}
