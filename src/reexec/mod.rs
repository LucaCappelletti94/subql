//! Database-read tiers and connector wrappers.
//!
//! [`SubscriptionEngine`](crate::SubscriptionEngine) registers every query. A
//! query its in-process evaluator can handle returns [`Tier::InProcess`](crate::Tier::InProcess). A
//! query needing SQL execution returns [`Tier::Scalar`](crate::Tier::Scalar), [`Tier::KeyedRows`](crate::Tier::KeyedRows) or
//! [`Tier::WholeRows`](crate::Tier::WholeRows) and emits [`ReExecutionTrigger`] when a read is due.
//! Each database-read payload owns a [`BoundQuery`] whose SQL and typed binds execute together.
//! Downstream Rust code writes database results back through
//! [`Install::install`](crate::Install::install).
//!
//! [`AutoResolvingEngine`] calls a connector for those triggers.
//! `AutoResolvingEngine<..., SyncMode<X>>` has synchronous methods for
//! `X: Connector`. `AutoResolvingEngine<..., AsyncMode<X>>` has asynchronous
//! methods for `X: AsyncConnector`.
//!
//! A shared scalar or row result is unsafe on a table with row-level security,
//! because viewers can see different rows. Such registrations return
//! [`RegisterError::AggregatorOnRlsTable`](crate::RegisterError::AggregatorOnRlsTable)
//! or [`RegisterError::RowCaptureOnRlsTable`](crate::RegisterError::RowCaptureOnRlsTable).
//!
//! ```
//! use sql_traits::structs::ParserDB;
//! use sqlparser::dialect::PostgreSqlDialect;
//! use subql::backend::Postgres;
//! use subql::testing::TestEvent;
//! use subql::{DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};
//!
//! let database = ParserDB::parse::<PostgreSqlDialect>(
//!     "CREATE TABLE t (id INT PRIMARY KEY, amount INT); \
//!      ALTER TABLE t ENABLE ROW LEVEL SECURITY;",
//! )
//! .expect("DDL parses");
//! let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
//!     SubscriptionEngine::new(database, PostgreSqlDialect {});
//!
//! let count = engine.register(SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM t"));
//! assert!(matches!(count, Err(RegisterError::AggregatorOnRlsTable { .. })));
//!
//! let min = engine.register(SubscriptionRequest::new(2u64, "SELECT MIN(amount) FROM t"));
//! assert!(matches!(min, Err(RegisterError::AggregatorOnRlsTable { .. })));
//! ```

mod engine;
pub(crate) mod maintain;
pub(crate) mod plan;

// Connector traits are always available. Concrete diesel implementations are
// gated inside their modules.
mod async_auto;
mod async_connector;
// Under the union of the backend sub-features rather than the bare
// `executor-diesel-async`: every item inside is a pool-backed connector or
// its plumbing, and only the backend features enable `diesel-async/bb8`.
#[cfg(any(
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-async-mysql"
))]
mod async_diesel;
mod auto;
mod connector;
#[cfg(test)]
mod test_fixtures;

pub use async_auto::AsyncMode;
pub use async_connector::AsyncConnector;
#[cfg(any(
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-async-mysql"
))]
pub use async_diesel::DieselAsyncError;
#[cfg(feature = "executor-diesel-async-mysql")]
pub use async_diesel::MysqlAsyncDieselConnector;
#[cfg(feature = "executor-diesel-async-postgres")]
pub use async_diesel::PgAsyncDieselConnector;
pub use auto::{
    AutoResolvingEngine, ResolverMode, SnapshotResult, SyncMode, DEFAULT_MAX_KEYS_PER_READ,
    DEFAULT_PAGE_BYTES,
};
#[cfg(feature = "executor-diesel-mysql")]
pub use connector::MysqlDieselConnector;
#[cfg(feature = "executor-diesel-postgres")]
pub use connector::PgDieselConnector;
pub use connector::{
    BoundQuery, Connector, CursorError, CursorId, ReExecError, ReadQuery, RowPage, ScalarRowError,
    SessionSetup, Snapshot,
};
#[cfg(feature = "executor-diesel")]
pub use connector::{DieselBackend, DieselConnector};
#[cfg(feature = "executor-diesel-postgres-r2d2")]
pub use connector::{PgR2D2DieselConnector, PgR2D2Error};
pub(crate) use engine::ReExecEntry;
pub use engine::{
    Dispatched, ReExecNotifications, ReExecutionRead, ReExecutionTrigger, ReadDelivery,
    ResolvedReads, RowDelta, RowsUpdate, ScalarUpdate,
};
