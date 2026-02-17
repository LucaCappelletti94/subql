//! SQL compilation pipeline: parse → normalize → compile to VM bytecode

pub mod tristate;
pub mod bytecode;
pub mod vm;
pub mod parser;
pub mod canonicalize;

pub use tristate::Tri;
pub use bytecode::{Instruction, BytecodeProgram};
pub use vm::{Vm, VmError};
pub use parser::parse_and_compile;
pub use canonicalize::{normalize_sql, hash_sql, PredicateHash};
