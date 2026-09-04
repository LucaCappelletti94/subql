#![cfg_attr(not(feature = "std"), no_std)]
#![doc = include_str!("../README.md")]

// Lint configuration is in [lints] section of Cargo.toml

#[macro_use]
extern crate alloc;

// Re-export public API
pub use compiler::vm::refusal::EvaluationRefusal;
pub use compiler::{AggHaving, AggSpec, HavingFunction, HavingOp, HavingSubject, QueryProjection};
pub use errors::*;
#[cfg(feature = "pg-sqlite-emu")]
pub use pg_sqlite_emu::{PgSqliteEmuError, PgSqliteEmuSource};
#[cfg(feature = "pg-streaming")]
pub use polling::{PollingPgCdcConfig, PollingPgCdcError, PollingPgCdcSource};
pub use runtime::aggregate::{
    DEFAULT_MAX_CHANGES_DURING_AGGREGATE_READ, DEFAULT_MAX_GROUPS_PER_AGGREGATE,
};
pub use runtime::SubscriptionEngine;
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
pub mod term;
#[cfg(feature = "membership-term")]
mod term_compile;
pub mod visibility;
pub mod wal;

// Value decoding shared by the diesel-typed query API, the diesel-backed
// re-execution connectors, and the SQLite-backed Postgres emulator. All three
// need the same raw-value readers, so the convention lives in one place rather
// than one per consumer.
#[cfg(any(
    feature = "diesel-typed",
    feature = "executor-diesel",
    feature = "pg-sqlite-emu"
))]
pub mod diesel_decode;

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

/// SQL listing every table whose change stream omits the previous row.
///
/// Run this once at startup and refuse to serve while it returns rows.
/// Postgres emits an update's or a delete's old image in full only under
/// `REPLICA IDENTITY FULL`. Under the default it sends the key alone, and
/// then a row that leaves a subscriber's reach cannot be distinguished
/// from one that was never reachable, so the subscriber silently keeps a
/// row it may no longer see.
///
/// One round trip for the whole database, and each row names a table to
/// fix. Ordinary and partitioned tables in user schemas only, since
/// nothing replicates the catalogs.
///
/// [`TransitionError::IncompletePreviousImage`](crate::visibility::transition::TransitionError::IncompletePreviousImage)
/// is the per-event counterpart, for a table altered after this ran.
///
/// This audit does not cover an unchanged TOASTed column. PostgreSQL omits
/// one from a logical replication message whatever the replica identity
/// is, so `REPLICA IDENTITY FULL` does not restore it and a passing audit
/// does not mean every cell arrives. Such a cell reads as
/// [`Value::Missing`](crate::backend::Value::Missing), and the
/// subscriptions whose predicate read it are reported through
/// [`ConsumerNotifications::unanswered`](crate::ConsumerNotifications::unanswered)
/// rather than silently not notified.
pub const REPLICA_IDENTITY_AUDIT_SQL: &str = "SELECT n.nspname, c.relname, c.relreplident \
     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
     WHERE c.relkind IN ('r', 'p') \
     AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
     AND c.relreplident <> 'f' \
     ORDER BY n.nspname, c.relname";

#[cfg(test)]
mod tests {
    #[test]
    fn it_compiles() {
        assert_eq!(2 + 2, 4);
    }
}
