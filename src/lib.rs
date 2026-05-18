#![doc = include_str!("../README.md")]

// Lint configuration is in [lints] section of Cargo.toml

// Re-export public API
pub use compiler::{AggSpec, QueryProjection};
pub use errors::*;
pub use runtime::{
    agg_delta_for_row, AggKernel, AvgKernel, CountColumnKernel, CountKernel, SubscriptionEngine,
    SumKernel,
};
pub use types::*;
pub use wal::{
    DebeziumParser, MaxwellParser, PgOutputParser, Wal2JsonV1Parser, Wal2JsonV2Parser,
    WalParseError, WalParser,
};

// Re-export the sql-traits types subql consumers most often need to spell out
// at call sites: trait bounds for generic code, the canonical schema
// fingerprint and its envelope error, and the parser-backed default DB impl.
pub use sql_traits::{
    prelude::{ColumnLike, DatabaseLike, TableLike},
    structs::{AlgorithmId, FingerprintError, ParserDB, SchemaFingerprint},
};

// Internal modules
mod errors;
mod table_resolution;
mod types;

pub mod catalog_helpers;
pub mod compiler;
pub mod config;
#[cfg(feature = "dhat-heap")]
pub mod memory_profile_workload;
pub mod persistence;
pub mod runtime;
pub mod wal;

#[cfg(any(feature = "testing", test))]
pub mod test_harnesses;

// Version and metadata
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// SQL to check REPLICA IDENTITY setting for a table.
///
/// Returns `'f'` for FULL. Callers should run this at setup and reject
/// tables where the result is not `'f'`, since view-relative UPDATE dispatch
/// requires a complete old row image.
pub const REPLICA_IDENTITY_CHECK_SQL: &str =
    "SELECT relreplident FROM pg_class WHERE oid = $1::regclass";

#[cfg(test)]
mod tests {
    #[test]
    fn it_compiles() {
        assert_eq!(2 + 2, 4);
    }
}
