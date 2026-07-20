#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../README.md")]

// Lint configuration is in [lints] section of Cargo.toml

#[macro_use]
extern crate alloc;

// Re-export public API
pub use compiler::{AggSpec, QueryProjection};
pub use errors::*;
#[cfg(feature = "pg-sqlite-emu")]
pub use pg_sqlite_emu::{PgSqliteEmuError, PgSqliteEmuSource};
#[cfg(feature = "pg-streaming")]
pub use polling::{PollingPgCdcConfig, PollingPgCdcError, PollingPgCdcSource};
pub use runtime::{
    agg_delta_for_row, AggKernel, AvgKernel, CountColumnKernel, CountKernel, SubscriptionEngine,
    SumKernel,
};
#[cfg(feature = "sqlite-cdc")]
pub use sqlite_cdc::{SqliteCdcError, SqliteCdcSource};
pub use sqlite_cdc::{SqliteChangesetEvent, SqliteChangesetParser};
pub use types::*;
#[cfg(feature = "std")]
pub use wal::CdcSource;
pub use wal::{
    parse_maxwell, parse_wal2json_v1, parse_wal2json_v2, ChangeEvent, ChangeV1, MaxwellMessage,
    MessageV2, WalParseError, WalParser,
};
#[cfg(feature = "pg-streaming")]
pub use wal::{PgStreamingCdcSource, PgStreamingConfig, PgStreamingError};

// Re-export the sql-traits types subql consumers most often need to spell out
// at call sites: trait bounds for generic code, the canonical schema
// fingerprint and its envelope error, and the parser-backed default DB impl.
pub use checkpoint::{Checkpoint, MysqlBinlogPos, NoCheckpoint, OpaqueCheckpoint, PgLsn};
#[cfg(feature = "std")]
pub use clock::StdClock;
pub use clock::{Clock, ClockHandle, ManualClock};
pub use row_set::{row_set_delta, Row, RowSetDelta};
pub use sql_traits::{
    prelude::{ColumnLike, DatabaseLike, TableLike},
    structs::{AlgorithmId, FingerprintError, ParserDB, SchemaFingerprint},
};

// Internal modules
mod errors;
mod table_resolution;
mod types;

pub mod backend;
pub mod catalog_helpers;
pub mod checkpoint;
pub mod clock;
pub mod compiler;
#[cfg(feature = "std")]
pub mod config;
pub mod emit;
pub mod testing;
// Memory profiling harness driven by the `dhat-heap` feature. Not part of
// the shipping API surface.
#[cfg(feature = "dhat-heap")]
pub mod memory_profile_workload;
#[cfg(any(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-mysql",
    feature = "apply-patchset-sqlite"
))]
pub mod patchset;
#[cfg(feature = "std")]
pub mod persistence;
#[cfg(feature = "pg-sqlite-emu")]
pub mod pg_sqlite_emu;
#[cfg(feature = "pg-streaming")]
pub mod polling;
pub mod reexec;
pub mod row_set;
pub mod runtime;
pub mod sqlite_cdc;
pub mod wal;

// Diesel-typed subscription and follow API. Only compiles when the
// `diesel-typed` family of features pulls in `diesel` with the third-party
// backend hooks its `BindDecode` impls need.
#[cfg(feature = "diesel-typed")]
pub mod diesel_api;

// Shared fuzz harness functions. Feature-gated behind `testing`
// (not part of the production build; enabled by the `subql-fuzz`
// workspace and by `cargo test --features testing`).
#[cfg(feature = "testing")]
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
