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
//! # Boundary: subql is DB-free
//!
//! The wrapper itself **never touches the database**. It does in-process
//! classification (`plan`) and incremental maintenance (`maintain`), and when
//! the maintenance state machine cannot resolve a result it emits a
//! [`ReExecutionTrigger`] for the caller. The caller (the Subscription
//! Materializer in `connetto-rs`, documented in
//! `connetto-rs/docs/architecture/10-subscription-materializer.md`) services
//! the trigger: re-runs the query against the database, applies authorization,
//! builds a `sqlite-diff-rs` patchset, and feeds the recomputed value back to
//! subql via [`ReExecEngine::install`].
//!
//! # v1 scope
//!
//! Only single-table scalar `MIN`/`MAX` is implemented, and it is maintained
//! **incrementally**: inserts and most updates/deletes are folded in memory
//! from the event's row image. A trigger is emitted only when the current
//! extreme is removed or displaced (the next extreme cannot be known without a
//! scan) or when an event's row image is too incomplete to decide.
//!
//! Two further flavors are designed-for and ride the same trigger seam but are
//! not implemented: **single-table row re-execution** (a query whose projection
//! is one base table but whose filter the engine cannot evaluate in-process)
//! and **aggregate re-execution** (multi-table aggregates, HAVING). The
//! materializer services all three identically: receive trigger, re-execute,
//! `install` the result.
//!
//! # Known limitations (v1)
//!
//! - **Ties.** Deleting one of several rows holding the extreme triggers a
//!   re-execution even though the value is unchanged (a tie-count refinement
//!   is left as future work; correctness never depends on it).
//! - **Initial values.** subql cannot compute initial values for stateful
//!   subscriptions (it has no DB). The materializer bootstraps and `install`s.

mod engine;
mod maintain;
mod plan;

pub use engine::{
    ReExecEngine, ReExecNotifications, ReExecQueryId, ReExecUnregisterReport, ReExecutionTrigger,
    Registered, ScalarUpdate,
};
