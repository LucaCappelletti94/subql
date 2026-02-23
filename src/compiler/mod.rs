//! SQL compilation pipeline: parse → normalize → compile to VM bytecode

pub mod bytecode;
pub mod canonicalize;
pub mod parser;
pub mod prefilter;
pub mod tristate;
pub mod vm;

pub use bytecode::{BytecodeProgram, Instruction};
pub use canonicalize::{hash_sql, normalize_sql, PredicateHash};
pub use parser::{
    parse_and_compile, parse_compile_and_normalize, parse_compile_normalize_and_prefilter,
};
pub use prefilter::{PlannerAtom, PlannerValue, PrefilterPlan};
pub use tristate::Tri;
pub use vm::{Vm, VmError};
