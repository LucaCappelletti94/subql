//! SQL compilation pipeline: parse -> normalize -> compile to VM bytecode

pub mod bytecode;
pub mod canonicalize;
pub mod literals;
pub mod parser;
pub mod prefilter;
pub mod sql_shape;
pub mod tristate;
pub(crate) mod value_cmp;
pub mod vm;

pub use bytecode::{BytecodeProgram, Instruction};
pub use canonicalize::{hash_sql, normalize_sql, PredicateHash};
pub use literals::SqlLiteralParse;
pub use parser::{
    derive_update_follow_select, parse_and_compile, parse_and_resolve_hash,
    parse_compile_and_normalize, parse_compile_normalize_and_prefilter,
    parse_compile_normalize_and_prefilter_with_binds,
};
pub use prefilter::{PlannerAtom, PlannerValue, PrefilterPlan};
pub use sql_shape::{AggSpec, QueryProjection};
pub use tristate::Tri;
pub use vm::{Vm, VmError};
