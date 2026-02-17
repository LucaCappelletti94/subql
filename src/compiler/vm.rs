//! VM interpreter for bytecode evaluation

use crate::{Cell, RowImage};
use super::{Tri, Instruction, BytecodeProgram};

/// VM evaluation error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmError {
    /// Stack underflow (popped from empty stack)
    StackUnderflow,

    /// Type mismatch (expected cell, got tri, or vice versa)
    TypeMismatch {
        expected: &'static str,
        got: &'static str,
    },

    /// Invalid column index (should never happen if compilation correct)
    InvalidColumnIndex(u16),
}

/// Stack-based VM for predicate evaluation
pub struct Vm {
    /// Value stack (grows during evaluation)
    stack: Vec<StackValue>,
}

/// Value on the VM stack (either Cell or Tri)
#[derive(Clone, Debug, PartialEq)]
enum StackValue {
    /// Cell value (from literals or column loads)
    Cell(Cell),
    /// Tri-state boolean (from comparisons and logical ops)
    Tri(Tri),
}

impl Vm {
    /// Create new VM instance
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Most predicates need < 16 stack slots
            stack: Vec::with_capacity(16),
        }
    }

    /// Evaluate bytecode program against a row
    ///
    /// Returns Tri::True if row matches, False/Unknown if not.
    pub fn eval(&mut self, program: &BytecodeProgram, row: &RowImage)
        -> Result<Tri, VmError>
    {
        self.stack.clear();

        // Execute each instruction
        for instruction in &program.instructions {
            self.execute(instruction, row)?;
        }

        // Final stack should have exactly one Tri value
        match self.stack.pop() {
            Some(StackValue::Tri(result)) => {
                if self.stack.is_empty() {
                    Ok(result)
                } else {
                    // Stack not empty - malformed program, treat as Unknown
                    Ok(Tri::Unknown)
                }
            }
            Some(StackValue::Cell(_)) => {
                // Top of stack is cell, not tri - treat as Unknown
                Ok(Tri::Unknown)
            }
            None => Err(VmError::StackUnderflow),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn execute(&mut self, instruction: &Instruction, row: &RowImage)
        -> Result<(), VmError>
    {
        match instruction {
            Instruction::PushLiteral(cell) => {
                self.stack.push(StackValue::Cell(cell.clone()));
            }

            Instruction::LoadColumn(col_id) => {
                let cell = row.get(*col_id)
                    .cloned()
                    .unwrap_or(Cell::Missing);
                self.stack.push(StackValue::Cell(cell));
            }

            Instruction::Equal => {
                let result = self.compare_cells(cells_equal)?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::NotEqual => {
                let result = self.compare_cells(|a, b| !cells_equal(a, b))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThan => {
                let result = self.compare_ordered(|ord| {
                    matches!(ord, std::cmp::Ordering::Less)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThanOrEqual => {
                let result = self.compare_ordered(|ord| {
                    !matches!(ord, std::cmp::Ordering::Greater)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThan => {
                let result = self.compare_ordered(|ord| {
                    matches!(ord, std::cmp::Ordering::Greater)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThanOrEqual => {
                let result = self.compare_ordered(|ord| {
                    !matches!(ord, std::cmp::Ordering::Less)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::IsNull => {
                let cell = self.pop_cell()?;
                let result = if cell.is_null() {
                    Tri::True
                } else {
                    Tri::False
                };
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::IsNotNull => {
                let cell = self.pop_cell()?;
                let result = if cell.is_null() || cell.is_missing() {
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
                let cell = self.pop_cell()?;

                // NULL IN (...) = Unknown
                if cell.is_null() || cell.is_missing() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // Check if cell matches any literal
                let found = literals.iter().any(|lit| cells_equal(&cell, lit));
                self.stack.push(StackValue::Tri(
                    if found { Tri::True } else { Tri::False }
                ));
            }

            Instruction::Between => {
                let upper = self.pop_cell()?;
                let lower = self.pop_cell()?;
                let value = self.pop_cell()?;

                // Any NULL/Missing → Unknown
                if value.is_null() || value.is_missing() ||
                   lower.is_null() || lower.is_missing() ||
                   upper.is_null() || upper.is_missing() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // value >= lower AND value <= upper
                let ge_lower = compare_ordered_cells(&value, &lower, |ord| {
                    !matches!(ord, std::cmp::Ordering::Less)
                });
                let le_upper = compare_ordered_cells(&value, &upper, |ord| {
                    !matches!(ord, std::cmp::Ordering::Greater)
                });

                let result = ge_lower.and(le_upper);
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::Like { case_sensitive } => {
                let pattern = self.pop_cell()?;
                let string = self.pop_cell()?;

                // NULL in either position → Unknown
                if string.is_null() || string.is_missing() ||
                   pattern.is_null() || pattern.is_missing() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // Extract strings
                let (str_val, pat_val) = if let (Cell::String(s), Cell::String(p)) = (&string, &pattern) {
                    (s.as_ref(), p.as_ref())
                } else {
                    // Type mismatch → Unknown
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                };

                // Simple LIKE implementation (% = wildcard, _ = single char)
                let matched = if *case_sensitive {
                    simple_like(str_val, pat_val)
                } else {
                    simple_like(&str_val.to_lowercase(), &pat_val.to_lowercase())
                };

                self.stack.push(StackValue::Tri(
                    if matched { Tri::True } else { Tri::False }
                ));
            }
        }

        Ok(())
    }

    fn pop_cell(&mut self) -> Result<Cell, VmError> {
        match self.stack.pop() {
            Some(StackValue::Cell(c)) => Ok(c),
            Some(StackValue::Tri(_)) => Err(VmError::TypeMismatch {
                expected: "Cell",
                got: "Tri",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn pop_tri(&mut self) -> Result<Tri, VmError> {
        match self.stack.pop() {
            Some(StackValue::Tri(t)) => Ok(t),
            Some(StackValue::Cell(_)) => Err(VmError::TypeMismatch {
                expected: "Tri",
                got: "Cell",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn compare_cells<F>(&mut self, f: F) -> Result<Tri, VmError>
    where
        F: FnOnce(&Cell, &Cell) -> bool,
    {
        let b = self.pop_cell()?;
        let a = self.pop_cell()?;

        // NULL or Missing → Unknown
        if a.is_null() || a.is_missing() || b.is_null() || b.is_missing() {
            return Ok(Tri::Unknown);
        }

        Ok(if f(&a, &b) { Tri::True } else { Tri::False })
    }

    fn compare_ordered<F>(&mut self, f: F) -> Result<Tri, VmError>
    where
        F: FnOnce(std::cmp::Ordering) -> bool,
    {
        let b = self.pop_cell()?;
        let a = self.pop_cell()?;

        Ok(compare_ordered_cells(&a, &b, f))
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn cells_equal(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Bool(x), Cell::Bool(y)) => x == y,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Float(x), Cell::Float(y)) => (x - y).abs() < f64::EPSILON,
        (Cell::String(x), Cell::String(y)) => x == y,
        // NULL = NULL is Unknown, not True!
        _ => false,
    }
}

#[allow(clippy::many_single_char_names)]
fn compare_ordered_cells<F>(lhs: &Cell, rhs: &Cell, predicate: F) -> Tri
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    // NULL or Missing → Unknown
    if lhs.is_null() || lhs.is_missing() || rhs.is_null() || rhs.is_missing() {
        return Tri::Unknown;
    }

    let ord = match (lhs, rhs) {
        (Cell::Int(x), Cell::Int(y)) => x.cmp(y),
        (Cell::Float(x), Cell::Float(y)) => {
            if x < y {
                std::cmp::Ordering::Less
            } else if x > y {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        }
        (Cell::String(x), Cell::String(y)) => x.cmp(y),
        _ => return Tri::Unknown, // Type mismatch
    };

    if predicate(ord) { Tri::True } else { Tri::False }
}

/// Simple LIKE pattern matching
///
/// Supports % (wildcard) and _ (single char). Does NOT support escaping.
fn simple_like(string: &str, pattern: &str) -> bool {
    // TODO: Implement full LIKE semantics
    // For Phase 1, use simple contains/prefix/suffix
    if pattern == "%" {
        true // Match anything
    } else if pattern.starts_with('%') && pattern.ends_with('%') {
        let middle = &pattern[1..pattern.len() - 1];
        string.contains(middle)
    } else if let Some(stripped) = pattern.strip_prefix('%') {
        string.ends_with(stripped)
    } else if let Some(stripped) = pattern.strip_suffix('%') {
        string.starts_with(stripped)
    } else {
        string == pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_row(cells: Vec<Cell>) -> RowImage {
        RowImage { cells: Arc::from(cells) }
    }

    #[test]
    fn test_simple_comparison() {
        let mut vm = Vm::new();

        // age > 18
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![Cell::Int(25)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(15)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Int(18)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_null_propagation() {
        let mut vm = Vm::new();

        // age > 18 (age is NULL)
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_and_logic() {
        let mut vm = Vm::new();

        // age > 18 AND active = true
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Equal,
            Instruction::And,
        ]);

        let row = make_row(vec![Cell::Int(25), Cell::Bool(true)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(25), Cell::Bool(false)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Int(15), Cell::Bool(true)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_is_null() {
        let mut vm = Vm::new();

        // email IS NULL
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::IsNull,
        ]);

        let row = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::String("test@example.com".into())]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_in_list() {
        let mut vm = Vm::new();

        // status IN ('pending', 'active')
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::In(vec![
                Cell::String("pending".into()),
                Cell::String("active".into()),
            ]),
        ]);

        let row = make_row(vec![Cell::String("pending".into())]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::String("completed".into())]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_between() {
        let mut vm = Vm::new();

        // age BETWEEN 18 AND 65
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::Between,
        ]);

        let row = make_row(vec![Cell::Int(25)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(18)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(65)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(17)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Int(66)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }
}
