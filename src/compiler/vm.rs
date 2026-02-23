//! VM interpreter for bytecode evaluation

use super::{BytecodeProgram, Instruction, Tri};
use crate::{Cell, RowImage};

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
    pub fn eval(&mut self, program: &BytecodeProgram, row: &RowImage) -> Result<Tri, VmError> {
        self.stack.clear();

        let instructions = &program.instructions;
        let len = instructions.len();
        let mut ip = 0;

        // Execute instructions with explicit instruction pointer (supports jumps)
        while ip < len {
            match &instructions[ip] {
                Instruction::JumpIfFalse(offset) => {
                    // Peek at TOS without popping
                    let top = self.peek_tri()?;
                    if top == Tri::False {
                        ip += offset;
                        continue;
                    }
                }
                Instruction::JumpIfTrue(offset) => {
                    // Peek at TOS without popping
                    let top = self.peek_tri()?;
                    if top == Tri::True {
                        ip += offset;
                        continue;
                    }
                }
                other => {
                    self.execute(other, row)?;
                }
            }
            ip += 1;
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
            Some(StackValue::Cell(cell)) => {
                // Top-level predicates may evaluate to a boolean (e.g. WHERE true
                // or WHERE bool_col). Coerce known boolean/null forms to Tri.
                Ok(predicate_tri_from_cell(&cell).unwrap_or(Tri::Unknown))
            }
            None => Err(VmError::StackUnderflow),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn execute(&mut self, instruction: &Instruction, row: &RowImage) -> Result<(), VmError> {
        match instruction {
            Instruction::PushLiteral(cell) => {
                self.stack.push(StackValue::Cell(cell.clone()));
            }

            Instruction::LoadColumn(col_id) => {
                let cell = row.get(*col_id).cloned().unwrap_or(Cell::Missing);
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
                let result = self.compare_ordered(|ord| matches!(ord, std::cmp::Ordering::Less))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThanOrEqual => {
                let result =
                    self.compare_ordered(|ord| !matches!(ord, std::cmp::Ordering::Greater))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThan => {
                let result =
                    self.compare_ordered(|ord| matches!(ord, std::cmp::Ordering::Greater))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThanOrEqual => {
                let result =
                    self.compare_ordered(|ord| !matches!(ord, std::cmp::Ordering::Less))?;
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
                self.stack
                    .push(StackValue::Tri(if found { Tri::True } else { Tri::False }));
            }

            Instruction::Between => {
                let upper = self.pop_cell()?;
                let lower = self.pop_cell()?;
                let value = self.pop_cell()?;

                // Any NULL/Missing → Unknown
                if value.is_null()
                    || value.is_missing()
                    || lower.is_null()
                    || lower.is_missing()
                    || upper.is_null()
                    || upper.is_missing()
                {
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
                if string.is_null()
                    || string.is_missing()
                    || pattern.is_null()
                    || pattern.is_missing()
                {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // Extract strings
                let (str_val, pat_val) =
                    if let (Cell::String(s), Cell::String(p)) = (&string, &pattern) {
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

                self.stack.push(StackValue::Tri(if matched {
                    Tri::True
                } else {
                    Tri::False
                }));
            }

            // ================================================================
            // Arithmetic Operations
            // ================================================================
            Instruction::Add => self.execute_binary_cell_op(arithmetic_add)?,
            Instruction::Subtract => self.execute_binary_cell_op(arithmetic_subtract)?,
            Instruction::Multiply => self.execute_binary_cell_op(arithmetic_multiply)?,
            Instruction::Divide => self.execute_binary_cell_op(arithmetic_divide)?,
            Instruction::Modulo => self.execute_binary_cell_op(arithmetic_modulo)?,

            Instruction::Negate => {
                let a = self.pop_cell()?;
                let result = arithmetic_negate(a);
                self.stack.push(StackValue::Cell(result));
            }

            // Jump instructions are handled in eval() before execute() is called
            Instruction::JumpIfFalse(_) | Instruction::JumpIfTrue(_) => {}
        }

        Ok(())
    }

    fn execute_binary_cell_op(&mut self, op: fn(Cell, Cell) -> Cell) -> Result<(), VmError> {
        let b = self.pop_cell()?;
        let a = self.pop_cell()?;
        self.stack.push(StackValue::Cell(op(a, b)));
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
            Some(StackValue::Cell(cell)) => predicate_tri_from_cell(&cell).map_or_else(
                || {
                    Err(VmError::TypeMismatch {
                        expected: "Tri",
                        got: "Cell",
                    })
                },
                Ok,
            ),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn peek_tri(&self) -> Result<Tri, VmError> {
        match self.stack.last() {
            Some(StackValue::Tri(t)) => Ok(*t),
            Some(StackValue::Cell(cell)) => predicate_tri_from_cell(cell).map_or_else(
                || {
                    Err(VmError::TypeMismatch {
                        expected: "Tri",
                        got: "Cell",
                    })
                },
                Ok,
            ),
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
        (Cell::Float(x), Cell::Float(y)) => x
            .partial_cmp(y)
            .is_some_and(|ord| ord == std::cmp::Ordering::Equal),
        (Cell::String(x), Cell::String(y)) => x == y,
        // NULL = NULL is Unknown, not True!
        _ => false,
    }
}

#[allow(clippy::many_single_char_names, clippy::cast_precision_loss)]
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
            if x.is_nan() || y.is_nan() {
                return Tri::Unknown;
            }
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        // Mixed Int/Float comparisons - coerce to Float
        (Cell::Int(x), Cell::Float(y)) => {
            let x_float = *x as f64;
            if y.is_nan() {
                return Tri::Unknown;
            }
            x_float.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Cell::Float(x), Cell::Int(y)) => {
            let y_float = *y as f64;
            if x.is_nan() {
                return Tri::Unknown;
            }
            x.partial_cmp(&y_float).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Cell::String(x), Cell::String(y)) => x.cmp(y),
        _ => return Tri::Unknown, // Type mismatch
    };

    if predicate(ord) {
        Tri::True
    } else {
        Tri::False
    }
}

const fn predicate_tri_from_cell(cell: &Cell) -> Option<Tri> {
    match cell {
        Cell::Bool(true) => Some(Tri::True),
        Cell::Bool(false) => Some(Tri::False),
        Cell::Null | Cell::Missing => Some(Tri::Unknown),
        _ => None,
    }
}

/// SQL LIKE pattern matching
///
/// Supports `%` (zero or more characters) and `_` (exactly one character).
/// Does NOT support ESCAPE clauses.
fn simple_like(string: &str, pattern: &str) -> bool {
    let s: Vec<char> = string.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let pn = p.len();

    // dp[j] = true means s[0..i] matches p[0..j]
    let mut dp = vec![false; pn + 1];
    dp[0] = true;

    // Initialize: leading '%' can match empty string
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
// Arithmetic Operations
// ============================================================================

/// Binary null propagation: returns `Some(Cell::Null)` if either operand is null/missing.
const fn null_propagate_binary(a: &Cell, b: &Cell) -> Option<Cell> {
    if !a.is_present() || !b.is_present() {
        Some(Cell::Null)
    } else {
        None
    }
}

/// Generic numeric binary operation.
///
/// Handles null propagation and Int/Float type coercion.
/// `int_op` is applied for `(Int, Int)`, `float_op` for any Float-involved pair.
#[allow(clippy::cast_precision_loss)]
fn numeric_binop(
    a: Cell,
    b: Cell,
    int_op: fn(i64, i64) -> i64,
    float_op: fn(f64, f64) -> f64,
) -> Cell {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }

    match (a, b) {
        (Cell::Int(x), Cell::Int(y)) => Cell::Int(int_op(x, y)),
        (Cell::Int(x), Cell::Float(y)) => Cell::Float(float_op(x as f64, y)),
        (Cell::Float(x), Cell::Int(y)) => Cell::Float(float_op(x, y as f64)),
        (Cell::Float(x), Cell::Float(y)) => Cell::Float(float_op(x, y)),
        _ => Cell::Null, // Type mismatch
    }
}

/// Add two cells: a + b
fn arithmetic_add(a: Cell, b: Cell) -> Cell {
    numeric_binop(a, b, i64::saturating_add, std::ops::Add::add)
}

/// Subtract two cells: a - b
fn arithmetic_subtract(a: Cell, b: Cell) -> Cell {
    numeric_binop(a, b, i64::saturating_sub, std::ops::Sub::sub)
}

/// Multiply two cells: a * b
fn arithmetic_multiply(a: Cell, b: Cell) -> Cell {
    numeric_binop(a, b, i64::saturating_mul, std::ops::Mul::mul)
}

/// Divide two cells: a / b
///
/// Always returns Float (SQL semantics). Division by zero → NULL.
#[allow(clippy::needless_pass_by_value, clippy::cast_precision_loss)]
fn arithmetic_divide(a: Cell, b: Cell) -> Cell {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }

    let a_float = match a {
        Cell::Int(x) => x as f64,
        Cell::Float(x) => x,
        _ => return Cell::Null,
    };

    let b_float = match b {
        Cell::Int(y) => y as f64,
        Cell::Float(y) => y,
        _ => return Cell::Null,
    };

    if b_float == 0.0 {
        return Cell::Null;
    }

    Cell::Float(a_float / b_float)
}

/// Modulo two cells: a % b
///
/// Integer operation only (coerces to Int first). Modulo by zero → NULL.
#[allow(clippy::needless_pass_by_value, clippy::cast_possible_truncation)]
fn arithmetic_modulo(a: Cell, b: Cell) -> Cell {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }

    let a_int = match a {
        Cell::Int(x) => x,
        Cell::Float(x) => x as i64,
        _ => return Cell::Null,
    };

    let b_int = match b {
        Cell::Int(y) => y,
        Cell::Float(y) => y as i64,
        _ => return Cell::Null,
    };

    a_int.checked_rem(b_int).map_or(Cell::Null, Cell::Int)
}

/// Negate a cell: -a (unary minus)
#[allow(clippy::needless_pass_by_value)]
fn arithmetic_negate(a: Cell) -> Cell {
    if !a.is_present() {
        return Cell::Null;
    }

    match a {
        Cell::Int(x) => Cell::Int(x.saturating_neg()),
        Cell::Float(x) => Cell::Float(-x),
        _ => Cell::Null,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_row(cells: Vec<Cell>) -> RowImage {
        RowImage {
            cells: Arc::from(cells),
        }
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
        let program = BytecodeProgram::new(vec![Instruction::LoadColumn(0), Instruction::IsNull]);

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

    #[test]
    fn test_arithmetic_add() {
        let mut vm = Vm::new();

        // a + b > 100
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // a
            Instruction::LoadColumn(1), // b
            Instruction::Add,
            Instruction::PushLiteral(Cell::Int(100)),
            Instruction::GreaterThan,
        ]);

        // 60 + 50 = 110 > 100 ✓
        let row = make_row(vec![Cell::Int(60), Cell::Int(50)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // 30 + 40 = 70 > 100 ✗
        let row = make_row(vec![Cell::Int(30), Cell::Int(40)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        // Int + Float = Float
        let row = make_row(vec![Cell::Int(50), Cell::Float(60.5)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // NULL + anything = NULL → Unknown
        let row = make_row(vec![Cell::Null, Cell::Int(50)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_arithmetic_subtract() {
        let mut vm = Vm::new();

        // end_date - start_date > 300
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // end_date
            Instruction::LoadColumn(1), // start_date
            Instruction::Subtract,
            Instruction::PushLiteral(Cell::Int(300)),
            Instruction::GreaterThan,
        ]);

        // 500 - 100 = 400 > 300 ✓
        let row = make_row(vec![Cell::Int(500), Cell::Int(100)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // 400 - 200 = 200 > 300 ✗
        let row = make_row(vec![Cell::Int(400), Cell::Int(200)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_arithmetic_multiply() {
        let mut vm = Vm::new();

        // price * quantity > 1000
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // price
            Instruction::LoadColumn(1), // quantity
            Instruction::Multiply,
            Instruction::PushLiteral(Cell::Int(1000)),
            Instruction::GreaterThan,
        ]);

        // 50 * 30 = 1500 > 1000 ✓
        let row = make_row(vec![Cell::Int(50), Cell::Int(30)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // 10 * 50 = 500 > 1000 ✗
        let row = make_row(vec![Cell::Int(10), Cell::Int(50)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_arithmetic_divide() {
        let mut vm = Vm::new();

        // total / count > 100.0
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // total
            Instruction::LoadColumn(1), // count
            Instruction::Divide,
            Instruction::PushLiteral(Cell::Float(100.0)),
            Instruction::GreaterThan,
        ]);

        // 500 / 4 = 125.0 > 100.0 ✓
        let row = make_row(vec![Cell::Int(500), Cell::Int(4)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // 300 / 5 = 60.0 > 100.0 ✗
        let row = make_row(vec![Cell::Int(300), Cell::Int(5)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        // Division by zero = NULL → Unknown
        let row = make_row(vec![Cell::Int(500), Cell::Int(0)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_arithmetic_modulo() {
        let mut vm = Vm::new();

        // id % 10 = 0 (check if divisible by 10)
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // id
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::Modulo,
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Equal,
        ]);

        // 100 % 10 = 0 ✓
        let row = make_row(vec![Cell::Int(100)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // 105 % 10 = 5 ≠ 0 ✗
        let row = make_row(vec![Cell::Int(105)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        // Modulo by zero = NULL → Unknown
        let row = make_row(vec![Cell::Int(100)]);
        let program_div_zero = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Modulo,
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Equal,
        ]);
        assert_eq!(vm.eval(&program_div_zero, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_arithmetic_negate() {
        let mut vm = Vm::new();

        // elevation BETWEEN -50 AND 50 (using negate for -50)
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // elevation
            Instruction::PushLiteral(Cell::Int(50)),
            Instruction::Negate, // -50
            Instruction::PushLiteral(Cell::Int(50)),
            Instruction::Between,
        ]);

        // elevation = 25 is in [-50, 50] ✓
        let row = make_row(vec![Cell::Int(25)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // elevation = -30 is in [-50, 50] ✓
        let row = make_row(vec![Cell::Int(-30)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        // elevation = 75 is NOT in [-50, 50] ✗
        let row = make_row(vec![Cell::Int(75)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        // -NULL = NULL → Unknown
        let program_null = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::Negate,
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Equal,
        ]);
        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program_null, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_arithmetic_complex_expression() {
        let mut vm = Vm::new();

        // (products_this_year - products_last_year) > 0.5 * products_last_year
        // Simplified: (a - b) > 0.5 * b
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0), // products_this_year
            Instruction::LoadColumn(1), // products_last_year
            Instruction::Subtract,      // a - b
            Instruction::PushLiteral(Cell::Float(0.5)),
            Instruction::LoadColumn(1), // products_last_year
            Instruction::Multiply,      // 0.5 * b
            Instruction::GreaterThan,   // (a - b) > (0.5 * b)
        ]);

        // 150 - 100 = 50 > 0.5 * 100 = 50 ✗ (equal, not greater)
        let row = make_row(vec![Cell::Int(150), Cell::Int(100)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        // 160 - 100 = 60 > 0.5 * 100 = 50 ✓
        let row = make_row(vec![Cell::Int(160), Cell::Int(100)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    // Error path and edge case tests for comprehensive coverage
    #[test]
    fn test_stack_underflow() {
        let mut vm = Vm::new();

        // Empty program
        let program = BytecodeProgram::new(vec![]);
        let row = make_row(vec![]);
        assert!(matches!(
            vm.eval(&program, &row),
            Err(VmError::StackUnderflow)
        ));
    }

    #[test]
    fn test_malformed_stack_leaves_cell_not_tri() {
        let mut vm = Vm::new();

        // Program leaves Cell on stack instead of Tri
        let program = BytecodeProgram::new(vec![Instruction::PushLiteral(Cell::Int(42))]);
        let row = make_row(vec![]);

        // Should return Unknown (not crash)
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_malformed_stack_not_empty() {
        let mut vm = Vm::new();

        // Program leaves extra values on stack
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(2)),
            Instruction::Equal,
            Instruction::PushLiteral(Cell::Int(3)), // Extra value
        ]);
        let row = make_row(vec![]);

        // Should return Unknown (not crash)
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_load_column_out_of_bounds() {
        let mut vm = Vm::new();

        // Load column beyond row bounds
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(999), // Out of bounds
            Instruction::IsNull,
        ]);

        let row = make_row(vec![Cell::Int(1)]);

        // Missing column → IsNull returns False (not NULL, but Missing)
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_arithmetic_type_mismatch() {
        let mut vm = Vm::new();

        // Try to add string + int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::Add,
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);

        // Type mismatch → NULL → Unknown
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_arithmetic_with_missing() {
        let mut vm = Vm::new();

        // Missing + Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Missing),
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::Add,
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);

        // Missing → NULL → Unknown
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_comparison_type_mismatch() {
        let mut vm = Vm::new();

        // Compare string with int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![]);

        // Type mismatch → Unknown
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_like_with_non_string() {
        let mut vm = Vm::new();

        // LIKE with int (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::PushLiteral(Cell::String("%test%".into())),
            Instruction::Like {
                case_sensitive: true,
            },
        ]);

        let row = make_row(vec![]);

        // Type mismatch → Unknown
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_like_pattern_matching() {
        let mut vm = Vm::new();

        // Test LIKE patterns (simple implementation - only prefix/suffix/contains)
        let test_cases = vec![
            ("hello", "%", true),        // Match all
            ("hello", "hello", true),    // Exact match
            ("hello", "hell%", true),    // Prefix
            ("hello", "%ello", true),    // Suffix
            ("hello", "%ell%", true),    // Contains
            ("hello", "goodbye", false), // No match
            ("hello", "hell", false),    // Partial prefix (no %)
        ];

        for (string, pattern, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(Cell::String(string.into())),
                Instruction::PushLiteral(Cell::String(pattern.into())),
                Instruction::Like {
                    case_sensitive: true,
                },
            ]);

            let row = make_row(vec![]);
            let result = vm.eval(&program, &row).unwrap();

            if expected {
                assert_eq!(
                    result,
                    Tri::True,
                    "Pattern '{}' should match '{}'",
                    pattern,
                    string
                );
            } else {
                assert_eq!(
                    result,
                    Tri::False,
                    "Pattern '{}' should not match '{}'",
                    pattern,
                    string
                );
            }
        }
    }

    #[test]
    fn test_like_case_insensitive() {
        let mut vm = Vm::new();

        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("HELLO".into())),
            Instruction::PushLiteral(Cell::String("%hello%".into())),
            Instruction::Like {
                case_sensitive: false,
            },
        ]);

        let row = make_row(vec![]);

        // Case-insensitive LIKE should match
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_between_with_null() {
        let mut vm = Vm::new();

        let test_cases = vec![
            (Cell::Null, Cell::Int(1), Cell::Int(10)),
            (Cell::Int(5), Cell::Null, Cell::Int(10)),
            (Cell::Int(5), Cell::Int(1), Cell::Null),
        ];

        for (value, lower, upper) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(value),
                Instruction::PushLiteral(lower),
                Instruction::PushLiteral(upper),
                Instruction::Between,
            ]);

            let row = make_row(vec![]);

            // Any NULL → Unknown
            assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
        }
    }

    #[test]
    fn test_in_with_null() {
        let mut vm = Vm::new();

        // NULL IN (1, 2, 3)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::In(vec![Cell::Int(1), Cell::Int(2), Cell::Int(3)]),
        ]);

        let row = make_row(vec![]);

        // NULL IN (...) → Unknown
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_in_empty_list() {
        let mut vm = Vm::new();

        // value IN () - empty list
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::In(vec![]),
        ]);

        let row = make_row(vec![]);

        // Nothing matches empty list
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_mixed_type_comparisons_edge_cases() {
        let mut vm = Vm::new();

        // Test Int vs Float edge cases
        let test_cases = vec![
            (Cell::Int(100), Cell::Float(100.0), false), // Equal (not less than)
            (Cell::Int(100), Cell::Float(99.9), false),  // Greater (not less than)
            (Cell::Float(99.9), Cell::Int(100), true),   // Less than
        ];

        for (lhs, rhs, less_than_expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(lhs.clone()),
                Instruction::PushLiteral(rhs.clone()),
                Instruction::LessThan,
            ]);

            let row = make_row(vec![]);
            let result = vm.eval(&program, &row).unwrap();

            if less_than_expected {
                assert_eq!(result, Tri::True, "{:?} < {:?} should be True", lhs, rhs);
            } else {
                assert_eq!(result, Tri::False, "{:?} < {:?} should be False", lhs, rhs);
            }
        }
    }

    #[test]
    fn test_modulo_with_float() {
        let mut vm = Vm::new();

        // Float % Float (gets converted to Int)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(10.7)),
            Instruction::PushLiteral(Cell::Float(3.2)),
            Instruction::Modulo,
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);

        // 10 % 3 = 1
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_negate_with_bool() {
        let mut vm = Vm::new();

        // Negate bool (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Negate,
            Instruction::PushLiteral(Cell::Int(0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);

        // Type mismatch → NULL → Unknown
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    // ========================================================================
    // Phase 1: Additional Error Path Coverage Tests
    // ========================================================================

    #[test]
    fn test_stack_not_empty_after_eval() {
        let mut vm = Vm::new();

        // Malformed program: pushes 2 Tri values but they don't get consumed
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(2)),
            Instruction::Equal, // Pops 2 Cells, pushes 1 Tri
            Instruction::PushLiteral(Cell::Int(3)),
            Instruction::PushLiteral(Cell::Int(4)),
            Instruction::Equal, // Pops 2 Cells, pushes 1 Tri
                                // Now stack has 2 Tri values - malformed!
        ]);

        let row = make_row(vec![]);

        // Stack has 2 values, should return Unknown (line 67)
        let result = vm.eval(&program, &row);
        assert_eq!(result.unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_not_equal_operator() {
        let mut vm = Vm::new();

        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::NotEqual,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_less_than_or_equal() {
        let mut vm = Vm::new();

        let test_cases = vec![
            (Cell::Int(5), Cell::Int(10), Tri::True),   // Less than
            (Cell::Int(10), Cell::Int(10), Tri::True),  // Equal
            (Cell::Int(15), Cell::Int(10), Tri::False), // Greater than
        ];

        for (lhs, rhs, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(lhs),
                Instruction::PushLiteral(rhs),
                Instruction::LessThanOrEqual,
            ]);

            let row = make_row(vec![]);
            assert_eq!(vm.eval(&program, &row).unwrap(), expected);
        }
    }

    #[test]
    fn test_greater_than_or_equal() {
        let mut vm = Vm::new();

        let test_cases = vec![
            (Cell::Int(15), Cell::Int(10), Tri::True), // Greater than
            (Cell::Int(10), Cell::Int(10), Tri::True), // Equal
            (Cell::Int(5), Cell::Int(10), Tri::False), // Less than
        ];

        for (lhs, rhs, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(lhs),
                Instruction::PushLiteral(rhs),
                Instruction::GreaterThanOrEqual,
            ]);

            let row = make_row(vec![]);
            assert_eq!(vm.eval(&program, &row).unwrap(), expected);
        }
    }

    #[test]
    fn test_is_not_null_with_missing() {
        let mut vm = Vm::new();

        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(99), // Missing column
            Instruction::IsNotNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_is_not_null_with_value() {
        let mut vm = Vm::new();

        // Test with actual value (not null, not missing)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::IsNotNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_or_operator() {
        let mut vm = Vm::new();

        // True OR False = True
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Equal, // Push Tri::True
            Instruction::PushLiteral(Cell::Bool(false)),
            Instruction::PushLiteral(Cell::Bool(false)),
            Instruction::Equal, // Push Tri::True (false == false)
            Instruction::Or,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_not_operator() {
        let mut vm = Vm::new();

        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Equal, // Push Tri::True
            Instruction::Not,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_like_with_null_pattern() {
        let mut vm = Vm::new();

        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Null),
            Instruction::Like {
                case_sensitive: true,
            },
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_type_mismatch_pop_cell() {
        let mut vm = Vm::new();

        // Push a Tri, then try to use it in arithmetic (expects Cell)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(2)),
            Instruction::Equal, // Pushes Tri
            Instruction::PushLiteral(Cell::Int(3)),
            Instruction::Add, // Tries to pop Cell but gets Tri
        ]);

        let row = make_row(vec![]);
        let result = vm.eval(&program, &row);
        assert!(matches!(result, Err(VmError::TypeMismatch { .. })));
    }

    #[test]
    fn test_type_mismatch_pop_tri() {
        let mut vm = Vm::new();

        // Push a Cell, then try to use it in logical operation (expects Tri)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(2)),
            Instruction::And, // Tries to pop Tri but gets Cell
        ]);

        let row = make_row(vec![]);
        let result = vm.eval(&program, &row);
        assert!(matches!(result, Err(VmError::TypeMismatch { .. })));
    }

    #[test]
    fn test_default_vm() {
        let vm = Vm::default();
        assert!(vm.stack.is_empty());
    }

    #[test]
    fn test_boolean_literal_predicate_truthiness() {
        let mut vm = Vm::new();
        let row = make_row(vec![]);

        let program_true = BytecodeProgram::new(vec![Instruction::PushLiteral(Cell::Bool(true))]);
        assert_eq!(vm.eval(&program_true, &row).unwrap(), Tri::True);

        let program_false = BytecodeProgram::new(vec![Instruction::PushLiteral(Cell::Bool(false))]);
        assert_eq!(vm.eval(&program_false, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_not_boolean_literal_predicate() {
        let mut vm = Vm::new();
        let row = make_row(vec![]);

        let program_not_true = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Not,
        ]);
        assert_eq!(vm.eval(&program_not_true, &row).unwrap(), Tri::False);

        let program_not_false = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(false)),
            Instruction::Not,
        ]);
        assert_eq!(vm.eval(&program_not_false, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_boolean_column_predicate_truthiness() {
        let mut vm = Vm::new();
        let program = BytecodeProgram::new(vec![Instruction::LoadColumn(0)]);

        let row_true = make_row(vec![Cell::Bool(true)]);
        assert_eq!(vm.eval(&program, &row_true).unwrap(), Tri::True);

        let row_false = make_row(vec![Cell::Bool(false)]);
        assert_eq!(vm.eval(&program, &row_false).unwrap(), Tri::False);

        let row_null = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row_null).unwrap(), Tri::Unknown);

        let row_missing = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row_missing).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_cells_equal_float_strict() {
        let mut vm = Vm::new();

        // Float equality is strict (SQL-style numeric equality for doubles).
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(1.0)),
            Instruction::PushLiteral(Cell::Float(1.0 + f64::EPSILON)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_cells_equal_type_mismatch() {
        let mut vm = Vm::new();

        // Bool == Int should be False (not Unknown)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_compare_ordered_int_float() {
        let mut vm = Vm::new();

        // Int < Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::PushLiteral(Cell::Float(10.5)),
            Instruction::LessThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_compare_ordered_float_int() {
        let mut vm = Vm::new();

        // Float > Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(10.5)),
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_compare_ordered_string() {
        let mut vm = Vm::new();

        // String comparison
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("apple".into())),
            Instruction::PushLiteral(Cell::String("banana".into())),
            Instruction::LessThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_add_mixed_types() {
        let mut vm = Vm::new();

        // Int + Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::PushLiteral(Cell::Float(2.5)),
            Instruction::Add,
            Instruction::PushLiteral(Cell::Float(7.5)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_add_float_int() {
        let mut vm = Vm::new();

        // Float + Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(2.5)),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Add,
            Instruction::PushLiteral(Cell::Float(7.5)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_add_float_float() {
        let mut vm = Vm::new();

        // Float + Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(2.5)),
            Instruction::PushLiteral(Cell::Float(3.5)),
            Instruction::Add,
            Instruction::PushLiteral(Cell::Float(6.0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_subtract_with_null() {
        let mut vm = Vm::new();

        // NULL - Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Subtract,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_subtract_mixed_types() {
        let mut vm = Vm::new();

        // Int - Float, Float - Int, Float - Float
        let test_cases = vec![
            (Cell::Int(10), Cell::Float(2.5), Cell::Float(7.5)),
            (Cell::Float(10.5), Cell::Int(5), Cell::Float(5.5)),
            (Cell::Float(10.5), Cell::Float(2.5), Cell::Float(8.0)),
        ];

        for (a, b, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(a),
                Instruction::PushLiteral(b),
                Instruction::Subtract,
                Instruction::PushLiteral(expected),
                Instruction::Equal,
            ]);

            let row = make_row(vec![]);
            assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
        }
    }

    #[test]
    fn test_arithmetic_multiply_with_null() {
        let mut vm = Vm::new();

        // NULL * Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Multiply,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_multiply_mixed_types() {
        let mut vm = Vm::new();

        let test_cases = vec![
            (Cell::Int(3), Cell::Float(2.5), Cell::Float(7.5)),
            (Cell::Float(2.5), Cell::Int(3), Cell::Float(7.5)),
            (Cell::Float(2.5), Cell::Float(2.0), Cell::Float(5.0)),
        ];

        for (a, b, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(a),
                Instruction::PushLiteral(b),
                Instruction::Multiply,
                Instruction::PushLiteral(expected),
                Instruction::Equal,
            ]);

            let row = make_row(vec![]);
            assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
        }
    }

    #[test]
    fn test_arithmetic_divide_with_null() {
        let mut vm = Vm::new();

        // NULL / Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Divide,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_divide_type_mismatch() {
        let mut vm = Vm::new();

        // String / Int (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Divide,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_modulo_with_null() {
        let mut vm = Vm::new();

        // NULL % Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Modulo,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_modulo_type_mismatch() {
        let mut vm = Vm::new();

        // String % Int (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Modulo,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_negate_with_null() {
        let mut vm = Vm::new();

        // -NULL
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::Negate,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_negate_float() {
        let mut vm = Vm::new();

        // -Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(5.5)),
            Instruction::Negate,
            Instruction::PushLiteral(Cell::Float(-5.5)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - VM Completion
    // ========================================================================

    #[test]
    fn test_stack_underflow_pop_cell() {
        let mut vm = Vm::new();

        // Empty stack, try to pop cell
        let program = BytecodeProgram::new(vec![
            Instruction::Add, // Tries to pop 2 cells from empty stack
        ]);

        let row = make_row(vec![]);
        let result = vm.eval(&program, &row);
        assert!(matches!(result, Err(VmError::StackUnderflow)));
    }

    #[test]
    fn test_stack_underflow_pop_tri() {
        let mut vm = Vm::new();

        // Empty stack, try to pop tri
        let program = BytecodeProgram::new(vec![
            Instruction::And, // Tries to pop 2 Tri from empty stack
        ]);

        let row = make_row(vec![]);
        let result = vm.eval(&program, &row);
        assert!(matches!(result, Err(VmError::StackUnderflow)));
    }

    #[test]
    fn test_float_comparison_equal() {
        let mut vm = Vm::new();

        // Float == Float (exactly equal)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::LessThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_compare_int_float_equal() {
        let mut vm = Vm::new();

        // Int == Float (mixed comparison, equal values)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_compare_float_int_equal() {
        let mut vm = Vm::new();

        // Float == Int (mixed comparison, equal values)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(10.0)),
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::LessThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_arithmetic_divide_int_int() {
        let mut vm = Vm::new();

        // Int / Int (converts to float)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::PushLiteral(Cell::Int(2)),
            Instruction::Divide,
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_divide_float_float() {
        let mut vm = Vm::new();

        // Float / Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(10.0)),
            Instruction::PushLiteral(Cell::Float(2.0)),
            Instruction::Divide,
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_modulo_int_int() {
        let mut vm = Vm::new();

        // Int % Int
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::PushLiteral(Cell::Int(3)),
            Instruction::Modulo,
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_modulo_float_float_coerced() {
        let mut vm = Vm::new();

        // Float % Float (coerced to Int)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(10.0)),
            Instruction::PushLiteral(Cell::Float(3.0)),
            Instruction::Modulo,
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_negate_type_mismatch() {
        let mut vm = Vm::new();

        // Negate string (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::Negate,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    // ========================================================================
    // Phase 3C: Final Push to 95% - VM Completion
    // ========================================================================

    #[test]
    fn test_arithmetic_subtract_float_float() {
        let mut vm = Vm::new();

        // Float - Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(10.5)),
            Instruction::PushLiteral(Cell::Float(3.5)),
            Instruction::Subtract,
            Instruction::PushLiteral(Cell::Float(7.0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_multiply_float_float() {
        let mut vm = Vm::new();

        // Float * Float
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(2.5)),
            Instruction::PushLiteral(Cell::Float(4.0)),
            Instruction::Multiply,
            Instruction::PushLiteral(Cell::Float(10.0)),
            Instruction::Equal,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_divide_non_numeric_denominator() {
        let mut vm = Vm::new();

        // Int / String (type mismatch in denominator)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::PushLiteral(Cell::String("not a number".into())),
            Instruction::Divide,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_modulo_non_numeric_denominator() {
        let mut vm = Vm::new();

        // Int % String (type mismatch in denominator)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(10)),
            Instruction::PushLiteral(Cell::String("not a number".into())),
            Instruction::Modulo,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_subtract_type_mismatch() {
        let mut vm = Vm::new();

        // String - Int (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Subtract,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_arithmetic_multiply_type_mismatch() {
        let mut vm = Vm::new();

        // String * Int (type mismatch)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::String("test".into())),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::Multiply,
            Instruction::IsNull,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    // ========================================================================
    // Full LIKE pattern matching tests
    // ========================================================================

    #[test]
    fn test_like_underscore_wildcard() {
        let mut vm = Vm::new();

        let test_cases = vec![
            ("abc", "a_c", true),
            ("abc", "_bc", true),
            ("abc", "ab_", true),
            ("abc", "___", true),
            ("ab", "___", false),
            ("abcd", "___", false),
            ("abc", "a__", true),
        ];

        for (string, pattern, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(Cell::String(string.into())),
                Instruction::PushLiteral(Cell::String(pattern.into())),
                Instruction::Like {
                    case_sensitive: true,
                },
            ]);

            let row = make_row(vec![]);
            let result = vm.eval(&program, &row).unwrap();

            if expected {
                assert_eq!(
                    result,
                    Tri::True,
                    "'{string}' LIKE '{pattern}' should be True"
                );
            } else {
                assert_eq!(
                    result,
                    Tri::False,
                    "'{string}' LIKE '{pattern}' should be False"
                );
            }
        }
    }

    #[test]
    fn test_like_multiple_percent_wildcards() {
        let mut vm = Vm::new();

        let test_cases = vec![
            ("abcdef", "a%d%f", true),
            ("abcdef", "%b%e%", true),
            ("abcdef", "a%c%f", true),
            ("abcdef", "a%z%f", false),
            ("", "%", true),
            ("", "%%", true),
            ("a", "a%b%c", false),
            ("abc", "a%b%c", true),
        ];

        for (string, pattern, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(Cell::String(string.into())),
                Instruction::PushLiteral(Cell::String(pattern.into())),
                Instruction::Like {
                    case_sensitive: true,
                },
            ]);

            let row = make_row(vec![]);
            let result = vm.eval(&program, &row).unwrap();

            if expected {
                assert_eq!(
                    result,
                    Tri::True,
                    "'{string}' LIKE '{pattern}' should be True"
                );
            } else {
                assert_eq!(
                    result,
                    Tri::False,
                    "'{string}' LIKE '{pattern}' should be False"
                );
            }
        }
    }

    #[test]
    fn test_like_mixed_wildcards() {
        let mut vm = Vm::new();

        let test_cases = vec![
            ("abc", "_%_", true),
            ("ab", "_%_", true),
            ("a", "_%_", false),
            ("abc", "%_c", true),
            ("ac", "%_c", true),
            ("c", "%_c", false),
        ];

        for (string, pattern, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(Cell::String(string.into())),
                Instruction::PushLiteral(Cell::String(pattern.into())),
                Instruction::Like {
                    case_sensitive: true,
                },
            ]);

            let row = make_row(vec![]);
            let result = vm.eval(&program, &row).unwrap();

            if expected {
                assert_eq!(
                    result,
                    Tri::True,
                    "'{string}' LIKE '{pattern}' should be True"
                );
            } else {
                assert_eq!(
                    result,
                    Tri::False,
                    "'{string}' LIKE '{pattern}' should be False"
                );
            }
        }
    }

    #[test]
    fn test_like_edge_cases() {
        let mut vm = Vm::new();

        let test_cases = vec![
            ("", "", true),
            ("", "_", false),
            ("a", "", false),
            ("", "a", false),
        ];

        for (string, pattern, expected) in test_cases {
            let program = BytecodeProgram::new(vec![
                Instruction::PushLiteral(Cell::String(string.into())),
                Instruction::PushLiteral(Cell::String(pattern.into())),
                Instruction::Like {
                    case_sensitive: true,
                },
            ]);

            let row = make_row(vec![]);
            let result = vm.eval(&program, &row).unwrap();

            if expected {
                assert_eq!(
                    result,
                    Tri::True,
                    "'{string}' LIKE '{pattern}' should be True"
                );
            } else {
                assert_eq!(
                    result,
                    Tri::False,
                    "'{string}' LIKE '{pattern}' should be False"
                );
            }
        }
    }

    // ========================================================================
    // NaN float handling tests
    // ========================================================================

    #[test]
    fn test_nan_comparison_returns_unknown() {
        let mut vm = Vm::new();

        // NaN < 5.0 → Unknown
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(f64::NAN)),
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::LessThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);

        // 5.0 > NaN → Unknown
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(5.0)),
            Instruction::PushLiteral(Cell::Float(f64::NAN)),
            Instruction::GreaterThan,
        ]);

        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);

        // NaN == NaN → Unknown (via ordered comparison)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(f64::NAN)),
            Instruction::PushLiteral(Cell::Float(f64::NAN)),
            Instruction::GreaterThanOrEqual,
        ]);

        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_nan_int_comparison_returns_unknown() {
        let mut vm = Vm::new();

        // NaN < Int → Unknown
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Float(f64::NAN)),
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::LessThan,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);

        // Int < NaN → Unknown
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Int(5)),
            Instruction::PushLiteral(Cell::Float(f64::NAN)),
            Instruction::LessThan,
        ]);

        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    // ========================================================================
    // Short-circuit evaluation tests
    // ========================================================================

    #[test]
    fn test_short_circuit_and_false_skips_rhs() {
        let mut vm = Vm::new();

        // False AND (expensive: column load + comparison)
        // JumpIfFalse skips: LoadColumn + PushLiteral + Equal + And = 4 instructions
        // offset = 5 (lands past And since continue skips ip += 1)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(false)),
            Instruction::PushLiteral(Cell::Bool(false)),
            Instruction::Equal, // Tri::True (false == false)
            Instruction::Not,   // Tri::False
            Instruction::JumpIfFalse(5),
            // These should NOT execute:
            Instruction::LoadColumn(999), // would be Missing
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::Equal,
            Instruction::And,
        ]);

        let row = make_row(vec![]);
        // Should be False (short-circuited, never evaluated rhs)
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_short_circuit_or_true_skips_rhs() {
        let mut vm = Vm::new();

        // True OR (expensive rhs)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Equal, // Tri::True
            Instruction::JumpIfTrue(5),
            // These should NOT execute:
            Instruction::LoadColumn(999),
            Instruction::PushLiteral(Cell::Int(42)),
            Instruction::Equal,
            Instruction::Or,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);
    }

    #[test]
    fn test_short_circuit_and_unknown_evaluates_rhs() {
        let mut vm = Vm::new();

        // Unknown AND True = Unknown (rhs must be evaluated)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal, // Tri::Unknown (NULL = 1)
            Instruction::JumpIfFalse(5),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Equal, // Tri::True
            Instruction::And,   // Unknown AND True = Unknown
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_short_circuit_or_unknown_evaluates_rhs() {
        let mut vm = Vm::new();

        // Unknown OR False = Unknown (rhs must be evaluated)
        let program = BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Null),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal, // Tri::Unknown
            Instruction::JumpIfTrue(5),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::PushLiteral(Cell::Bool(false)),
            Instruction::Equal, // Tri::False
            Instruction::Or,    // Unknown OR False = Unknown
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_short_circuit_and_false_in_chain() {
        let mut vm = Vm::new();

        // False AND True AND True → short-circuits at first False
        // The first JumpIfFalse must skip the entire rest of the program
        let program = BytecodeProgram::new(vec![
            // First: push False
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(2)),
            Instruction::Equal, // False (1 != 2)
            // Inner AND
            Instruction::JumpIfFalse(5),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal, // True (1 == 1)
            Instruction::And,
            // Outer AND
            Instruction::JumpIfFalse(5),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::PushLiteral(Cell::Int(1)),
            Instruction::Equal,
            Instruction::And,
        ]);

        let row = make_row(vec![]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }
}
