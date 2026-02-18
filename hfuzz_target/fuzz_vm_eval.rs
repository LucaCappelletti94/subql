use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use honggfuzz::fuzz;
use subql::compiler::{BytecodeProgram, Instruction, Vm};
use subql::{Cell, RowImage};

/// Generate a Cell from fuzzer-controlled bytes.
fn arb_cell(u: &mut Unstructured<'_>) -> arbitrary::Result<Cell> {
    match u.int_in_range(0u8..=5)? {
        0 => Ok(Cell::Null),
        1 => Ok(Cell::Missing),
        2 => Ok(Cell::Bool(bool::arbitrary(u)?)),
        3 => Ok(Cell::Int(i64::arbitrary(u)?)),
        4 => Ok(Cell::Float(f64::arbitrary(u)?)),
        _ => {
            let len = u.int_in_range(0usize..=64)?;
            let bytes: Vec<u8> = (0..len)
                .map(|_| u.arbitrary())
                .collect::<arbitrary::Result<_>>()?;
            Ok(Cell::String(String::from_utf8_lossy(&bytes).into()))
        }
    }
}

/// Generate an Instruction from fuzzer-controlled bytes.
fn arb_instruction(u: &mut Unstructured<'_>) -> arbitrary::Result<Instruction> {
    match u.int_in_range(0u8..=21)? {
        0 => Ok(Instruction::PushLiteral(arb_cell(u)?)),
        1 => Ok(Instruction::LoadColumn(u.int_in_range(0u16..=63)?)),
        2 => Ok(Instruction::Equal),
        3 => Ok(Instruction::NotEqual),
        4 => Ok(Instruction::LessThan),
        5 => Ok(Instruction::LessThanOrEqual),
        6 => Ok(Instruction::GreaterThan),
        7 => Ok(Instruction::GreaterThanOrEqual),
        8 => Ok(Instruction::IsNull),
        9 => Ok(Instruction::IsNotNull),
        10 => Ok(Instruction::And),
        11 => Ok(Instruction::Or),
        12 => Ok(Instruction::Not),
        13 => Ok(Instruction::Add),
        14 => Ok(Instruction::Subtract),
        15 => Ok(Instruction::Multiply),
        16 => Ok(Instruction::Divide),
        17 => Ok(Instruction::Modulo),
        18 => Ok(Instruction::Negate),
        19 => {
            let len = u.int_in_range(0usize..=8)?;
            let list: Vec<Cell> = (0..len)
                .map(|_| arb_cell(u))
                .collect::<arbitrary::Result<_>>()?;
            Ok(Instruction::In(list))
        }
        20 => Ok(Instruction::Between),
        _ => Ok(Instruction::Like {
            case_sensitive: bool::arbitrary(u)?,
        }),
    }
}

fn main() {
    let mut vm = Vm::new();

    loop {
        fuzz!(|data: &[u8]| {
            let mut u = Unstructured::new(data);

            // Generate 1-32 instructions
            let n_instr = match u.int_in_range(1usize..=32) {
                Ok(n) => n,
                Err(_) => return,
            };
            let instructions: Vec<Instruction> = match (0..n_instr)
                .map(|_| arb_instruction(&mut u))
                .collect::<arbitrary::Result<_>>()
            {
                Ok(v) => v,
                Err(_) => return,
            };

            // Generate 0-16 row cells
            let n_cells = match u.int_in_range(0usize..=16) {
                Ok(n) => n,
                Err(_) => return,
            };
            let cells: Vec<Cell> = match (0..n_cells)
                .map(|_| arb_cell(&mut u))
                .collect::<arbitrary::Result<_>>()
            {
                Ok(v) => v,
                Err(_) => return,
            };

            let program = BytecodeProgram::new(instructions);
            let row = RowImage {
                cells: Arc::from(cells),
            };

            // Any Err/Ok is fine; panics are bugs.
            let _ = vm.eval(&program, &row);
        });
    }
}
