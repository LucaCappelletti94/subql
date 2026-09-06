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

use super::connector::{ReadQuery, ScalarRowError, Snapshot};
use crate::backend::{Backend, ScalarFamily, Value};
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
    /// expected [`ScalarFamily`](crate::backend::ScalarFamily), optionally reporting the position at
    /// which the read was taken.
    ///
    /// See [`Connector::execute_scalar`](super::Connector::execute_scalar)
    /// for the contract. The async surface is identical other than
    /// returning a future.
    fn execute_scalar(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kind: ScalarFamily,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>,
    > + Send;

    /// Async peer of [`Connector::read_page`](super::Connector::read_page).
    /// See it for the contract, including the budget rule and why this stays
    /// stateless.
    fn read_page(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        max_bytes: usize,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<Snapshot<super::RowPage<Self::Backend>, Self::Checkpoint>, Self::Error>,
    > + Send;

    /// Async peer of
    /// [`Connector::open_cursor`](super::Connector::open_cursor).
    fn open_cursor(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<Output = Result<super::CursorId, super::CursorError<Self::Error>>>
           + Send {
        let _ = (query, auth);
        core::future::ready(Err(super::CursorError::Unsupported))
    }

    /// Async peer of
    /// [`Connector::fetch_cursor`](super::Connector::fetch_cursor), under the
    /// same contract, restated because an implementor reads this trait and not
    /// its sync twin.
    ///
    /// A cursor is serial, so an implementation MUST report a concurrent read
    /// as [`CursorError::Busy`](super::CursorError::Busy) rather than queueing
    /// behind the first. On failure the cursor's server-side state is unknown,
    /// so an implementation MUST drop it, after which this and
    /// [`close_cursor`](Self::close_cursor) behave as they do for a cursor that
    /// was never opened.
    ///
    /// Cleanup cannot be done in a destructor here: ending a transaction needs
    /// I/O and a destructor cannot await. An implementation that holds a
    /// transaction open across calls MUST therefore open it through its
    /// driver's own transaction bookkeeping, so that a connection released by a
    /// cancelled read is recognised as dirty by its pool rather than handed to
    /// the next caller.
    fn fetch_cursor(
        &self,
        cursor: super::CursorId,
        max_bytes: usize,
    ) -> impl core::future::Future<
        Output = Result<
            Snapshot<super::RowPage<Self::Backend>, Self::Checkpoint>,
            super::CursorError<Self::Error>,
        >,
    > + Send {
        let _ = (cursor, max_bytes);
        core::future::ready(Err(super::CursorError::Unsupported))
    }

    /// Async peer of
    /// [`Connector::close_cursor`](super::Connector::close_cursor).
    fn close_cursor(
        &self,
        cursor: super::CursorId,
    ) -> impl core::future::Future<Output = Result<(), super::CursorError<Self::Error>>> + Send
    {
        let _ = cursor;
        core::future::ready(Err(super::CursorError::Unsupported))
    }

    /// Async peer of
    /// [`Connector::execute_scalar_row`](super::Connector::execute_scalar_row).
    /// See it for the contract; the async surface only differs by returning
    /// a future. The default rejects with [`ScalarRowError::Unsupported`] so
    /// external impls keep compiling.
    fn execute_scalar_row(
        &self,
        query: &ReadQuery<'_, Self::Backend>,
        kinds: &[ScalarFamily],
        auth: &Self::AuthContext,
    ) -> impl core::future::Future<
        Output = Result<
            (
                alloc::vec::Vec<Value<Self::Backend>>,
                Option<Self::Checkpoint>,
            ),
            ScalarRowError<Self::Error>,
        >,
    > + Send {
        let _ = (query, kinds, auth);
        async { Err(ScalarRowError::Unsupported) }
    }
}
