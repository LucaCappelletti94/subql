//! SQL re-execution wrapper.
//!
//! The core [`crate::runtime::engine::SubscriptionEngine`] is a DB-free,
//! in-process CDC event filter: it rejects any query it cannot evaluate
//! against a single row image (JOINs, subqueries, non-delta-composable
//! aggregates such as `MIN`/`MAX`) with [`crate::RegisterError::UnsupportedSql`].
//!
//! This module is a *front-door* wrapper that sits above the engine and handles
//! exactly those rejected queries. Supported queries pass straight through to
//! the engine untouched; only the rejected ones are captured here.
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
//!   directly. The opt-in [`DieselConnector`] (feature `executor-diesel`)
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
//! Total per-consumer re-execution lifts this restriction in a follow-on.
//!
//! # Known limitations (v1)
//!
//! - **Ties.** Deleting one of several rows holding the extreme forces a
//!   re-execution even though the value is unchanged (a tie-count refinement
//!   is left as future work; correctness never depends on it).
//! - **Initial values.** subql cannot compute initial values for stateful
//!   subscriptions (it has no DB on its own). Callers either bootstrap via
//!   [`ReExecEngine::install`] / [`AutoResolvingEngine::install`] or, under
//!   the auto-resolving engine, accept whatever the connector returns on
//!   the first re-execution.
//! - **Async Connector.** v1 ships a sync [`Connector`] only. An
//!   `AsyncConnector` and `AsyncAutoResolvingEngine` for sqlx/diesel-async
//!   are planned but not implemented.

mod auto;
mod connector;
mod engine;
mod maintain;
mod plan;

pub use auto::AutoResolvingEngine;
#[cfg(feature = "executor-diesel")]
pub use connector::DieselConnector;
pub use connector::{Connector, ReExecError};
pub use engine::{
    ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecUnregisterReport, ReExecutionTrigger,
    Registered, ScalarUpdate,
};
