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
//! It is organized as three layers:
//!
//! 1. **Classification** (`plan`): turn a rejected query into a re-execution
//!    plan (v1: single-table scalar `MIN`/`MAX`).
//! 2. **Maintenance** (`maintain`): a per-query state machine that consumes
//!    CDC events in-process and decides `Unchanged` / `Updated` /
//!    `NeedsReexecution`, NEVER touching the database. WHERE membership is
//!    evaluated with the engine VM.
//! 3. **Execution** (`executor`): the only DB-touching layer; runs SQL over a
//!    caller-supplied [`ConnectionProvider`] when maintenance asks for it.
//!
//! Execution goes through `diesel`. The wrapper is generic over
//! [`diesel::Connection`], so the concrete backend (Postgres, MySQL, SQLite) is
//! the caller's choice; a ready-made `r2d2` provider is gated behind the
//! `reexec-r2d2` feature.
//!
//! # v1 scope
//!
//! Only single-table scalar `MIN`/`MAX` is handled, and it is maintained
//! **incrementally**: inserts and most updates/deletes are folded in memory
//! from the event's row image, and the database is re-queried ONLY when the
//! current extreme is removed or displaced (where the next extreme cannot be
//! known without a scan), or when a row image is too incomplete to decide.
//! Two further re-execution flavors are designed-for (the `maintain` and
//! `executor` seams extend to them) but not implemented: **single-table row
//! re-execution** - a query whose projection is one base table but whose filter
//! joins/subqueries the engine cannot evaluate in-process, re-run to the
//! matching rows of that table and emitted as per-table PK-keyed row deltas -
//! and **aggregate re-execution** (multi-table aggregates, HAVING). Both emit
//! semantic deltas; downstream (connetto) applies authorization and serializes
//! them into `sqlite-diff-rs` patchsets. There is no arbitrary-result-set path:
//! outputs are always per-known-table.
//!
//! # Known limitations (v1)
//!
//! - **Synchronous execution.** `diesel` is sync and so is the engine's
//!   dispatch path, so a re-execution is a blocking call inside dispatch.
//! - **Eventual consistency.** A re-execution reads the live DB, not the CDC
//!   event's log position, so it can race ahead of the event stream. This is
//!   correct-on-convergence for `MIN`/`MAX`.
//! - **Ties.** Deleting one of several rows holding the extreme re-queries even
//!   though the value is unchanged (a tie-count refinement is left as future
//!   work; correctness never depends on it).

mod engine;
mod executor;
mod maintain;
mod plan;

pub use engine::{
    ReExecDispatchError, ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecRegisterError,
    ReExecUnregisterReport, Registered, ScalarUpdate,
};
pub use executor::{ConnectionProvider, ReExecError, ScalarSource};
