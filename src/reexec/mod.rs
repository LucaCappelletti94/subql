//! SQL re-execution wrapper.
//!
//! The core [`crate::runtime::engine::SubscriptionEngine`] is a DB-free,
//! in-process CDC event filter: it rejects any query it cannot evaluate
//! against a single row image (JOINs, subqueries, non-delta-composable
//! aggregates such as `MIN`/`MAX`) with [`crate::RegisterError::UnsupportedSql`].
//!
//! This module is a *front-door* wrapper that sits above the engine and handles
//! exactly those rejected queries. Supported queries pass straight through to
//! the engine untouched. Only the rejected ones are captured here.
//!
//! # Two engines
//!
//! Pick the engine that matches how much of the re-execution loop you want
//! subql to drive:
//!
//! - [`ReExecEngine`] is **DB-free**: it classifies and maintains
//!   in-process. When the state machine cannot resolve a result on its own,
//!   it emits a [`ReExecutionTrigger`] for the caller, who runs the query
//!   themselves and calls [`ReExecEngine::install`] with the result. Pick
//!   this when you need explicit control over re-execution (cross-batch
//!   coalescing, retry-with-backoff, auth that does not fit the
//!   [`Connector::AuthContext`] shape).
//!
//! - [`AutoResolvingEngine`] wraps [`ReExecEngine`] with a user-supplied
//!   [`Connector`] impl and converts triggers into [`ScalarUpdate`]s
//!   inline. Pick this when you want subql to drive the loop end to end:
//!   pass a `Connector` over your DB handle and receive `ScalarUpdate`s
//!   directly. The opt-in `DieselConnector` (feature `executor-diesel`)
//!   ships a sync impl for any diesel [`Connection`](diesel::Connection).
//!
//! # v1 scope
//!
//! Only single-table scalar `MIN`/`MAX` is implemented, and it is maintained
//! **incrementally**: inserts and most updates/deletes are folded in memory
//! from the event's row image. A re-execution is needed only when the
//! current extreme is removed or displaced (the next extreme cannot be known
//! without a scan) or when an event's row image is too incomplete to decide.
//!
//! Two further flavors are designed-for and ride the same trigger seam but are
//! not implemented: **single-table row re-execution** (a query whose projection
//! is one base table but whose filter the engine cannot evaluate in-process)
//! and **aggregate re-execution** (multi-table aggregates, HAVING).
//!
//! # RLS reject
//!
//! Under PostgreSQL row-level security, different viewers observe different
//! rows, so a single in-process IVM state cannot be shared across consumers.
//! Aggregator registrations targeting a table with
//! [`TableLike::has_row_level_security`](sql_traits::prelude::TableLike::has_row_level_security)
//! true are rejected at register time with
//! [`RegisterError::AggregatorOnRlsTable`](crate::RegisterError::AggregatorOnRlsTable).
//! This covers both aggregate families: the in-process delta aggregates
//! (`COUNT`, `SUM`, `AVG`, `VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, `STDDEV_SAMP`)
//! the core engine maintains, and the captured `MIN` / `MAX` re-execution
//! family. Row subscriptions stay accepted, since they are filtered per
//! viewer at delivery. Total per-consumer re-execution would lift this
//! restriction (see `MILESTONES.md`).
//!
//! ```
//! use sql_traits::structs::ParserDB;
//! use sqlparser::dialect::PostgreSqlDialect;
//! use subql::backend::Postgres;
//! use subql::reexec::ReExecEngine;
//! use subql::testing::TestEvent;
//! use subql::{DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};
//!
//! let database = ParserDB::parse::<PostgreSqlDialect>(
//!     "CREATE TABLE t (id INT PRIMARY KEY, amount INT); \
//!      ALTER TABLE t ENABLE ROW LEVEL SECURITY;",
//! )?;
//! let mut engine: ReExecEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
//!     ReExecEngine::new(SubscriptionEngine::new(database, PostgreSqlDialect {}));
//!
//! // An in-process delta aggregate is rejected on the RLS table.
//! let count = engine.register(SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM t"));
//! assert!(matches!(count, Err(RegisterError::AggregatorOnRlsTable { .. })));
//!
//! // So is the captured MIN / MAX family.
//! let min = engine.register(SubscriptionRequest::new(2u64, "SELECT MIN(amount) FROM t"));
//! assert!(matches!(min, Err(RegisterError::AggregatorOnRlsTable { .. })));
//!
//! // A row subscription on the same table is still accepted.
//! let rows =
//!     engine.register(SubscriptionRequest::new(3u64, "SELECT * FROM t WHERE amount > 1"))?;
//! # let _ = rows;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Known limitations (v1)
//!
//! - **Ties.** Deleting one of several rows holding the extreme forces a
//!   re-execution even though the value is unchanged (a tie-count refinement
//!   is left as future work. Correctness never depends on it).
//! - **Initial values.** subql cannot compute initial values for stateful
//!   subscriptions (it has no DB on its own). Callers either bootstrap via
//!   [`ReExecEngine::install`] / [`AutoResolvingEngine::install`] or, under
//!   the auto-resolving engine, accept whatever the connector returns on
//!   the first re-execution.
//! - **Async Connector.** The [`AsyncConnector`] trait and
//!   [`AsyncAutoResolvingEngine`] ship, but no concrete async backend
//!   (sqlx, diesel-async) is bundled yet. See `MILESTONES.md`.

mod engine;
mod maintain;
mod plan;

// Connector trait shell (no diesel impls) so `ReExecEngine` and downstream
// crates that reference `ReExecError` still link. The diesel-backed
// impls are gated inside the file.
mod async_auto;
mod async_connector;
#[cfg(feature = "executor-diesel-async")]
mod async_diesel;
mod auto;
mod connector;

pub use async_auto::AsyncAutoResolvingEngine;
pub use async_connector::AsyncConnector;
#[cfg(feature = "executor-diesel-async")]
pub use async_diesel::DieselAsyncError;
#[cfg(feature = "executor-diesel-async-mysql")]
pub use async_diesel::MysqlAsyncDieselConnector;
#[cfg(feature = "executor-diesel-async-postgres")]
pub use async_diesel::PgAsyncDieselConnector;
pub use auto::{AutoResolvingEngine, SnapshotResult};
#[cfg(feature = "executor-diesel-mysql")]
pub use connector::MysqlDieselConnector;
#[cfg(feature = "executor-diesel-postgres")]
pub use connector::PgDieselConnector;
pub use connector::{Connector, ReExecError, Snapshot};
#[cfg(feature = "executor-diesel")]
pub use connector::{DieselBackend, DieselConnector};
#[cfg(feature = "executor-diesel-postgres-r2d2")]
pub use connector::{PgR2D2DieselConnector, PgR2D2Error};
pub use engine::{
    BatchOutcome, ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecUnregisterReport,
    ReExecutionTrigger, Registered, ScalarUpdate,
};
