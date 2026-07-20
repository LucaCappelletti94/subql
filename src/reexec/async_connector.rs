#![allow(clippy::type_complexity)]

//! Async parallel of [`Connector`](super::Connector).
//!
//! Mirrors the sync trait one-for-one, but returns `Send` futures so it
//! works on multi-threaded async runtimes (tokio multi-thread, async-std,
//! smol with a worker pool). Sync and async are first-class peers. Pick
//! the trait that matches the underlying database driver.
//!
//! No concrete async impl ships in this phase. The surface is locked in so
//! a future `executor-sqlx` or `executor-diesel-async` feature can land
//! additively without a trait-shape change.

use super::connector::Snapshot;
use crate::backend::{Backend, ScalarKind, Value};
use crate::Checkpoint;

/// Async [`Connector`](super::Connector). Returned futures are `Send` so
/// the connector can be driven from a multi-threaded runtime.
///
/// The trait body uses the explicit `impl Future + Send` return type
/// rather than `async fn`, because `async fn` in trait does not allow
/// the caller to add `+ Send` to the returned future. Implementors write
/// their method bodies with `async move { ... }` (or any other future-
/// producing expression) whose captured state is `Send`.
///
/// # Auth context
///
/// One [`AuthContext`](Self::AuthContext) value is stored per registered
/// subscription. Use it to thread a session id, a JWT, or a tenant tag
/// through to the database-level identity that runs the query (e.g.
/// `set_config('request.jwt', ...)` for PostgreSQL RLS).
///
/// # Send bounds
///
/// `Self: Send + Sync` and the return futures are `+ Send` so the trait is
/// usable as a generic bound on engines that themselves move across
/// threads. Connector implementations that need `!Send` (single-threaded
/// embedded async, single-tab WASM) should define their own trait variant.
/// The sync [`Connector`](super::Connector) is also available as an
/// alternative.
pub trait AsyncConnector: Send + Sync {
    /// Per-subscription auth state, passed verbatim to each call.
    type AuthContext: Send + Sync;
    /// Connector-specific error returned by the futures below.
    type Error: Send;
    /// Position token the connector tags reads with. See
    /// [`Connector::Checkpoint`](super::Connector::Checkpoint) for the
    /// sync analogue.
    type Checkpoint: Checkpoint;
    /// Subql backend whose [`Value`] shape this connector produces.
    type Backend: Backend;

    /// Run the re-execution SQL and decode a single scalar value with the
    /// expected [`ScalarKind`], optionally reporting the position at
    /// which the read was taken.
    ///
    /// See [`Connector::execute_scalar`](super::Connector::execute_scalar)
    /// for the contract. The async surface is identical other than
    /// returning a future.
    fn execute_scalar(
        &self,
        sql: &str,
        kind: ScalarKind,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>,
    > + Send;

    /// Run `sql` as a row-returning query and decode every row into a
    /// column-ordered `Vec<Value<Self::Backend>>`.
    ///
    /// See [`Connector::execute_rows`](super::Connector::execute_rows) for
    /// the contract. Impls should open a read-only repeatable-read
    /// transaction so rows and checkpoint observe the same snapshot.
    fn execute_rows(
        &self,
        sql: &str,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<
            Snapshot<alloc::vec::Vec<alloc::vec::Vec<Value<Self::Backend>>>, Self::Checkpoint>,
            Self::Error,
        >,
    > + Send;
}
