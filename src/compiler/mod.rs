//! SQL compilation pipeline: parse → normalize → compile to VM bytecode

pub mod tristate;
pub mod bytecode;
pub mod vm;
pub mod parser;

pub use tristate::Tri;
pub use bytecode::{Instruction, BytecodeProgram};
pub use vm::{Vm, VmError};
pub use parser::parse_and_compile;
