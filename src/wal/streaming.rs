//! Push-based CDC source trait.
//!
//! [`CdcSource`] abstracts any transport that delivers replication
//! events to subql as Postgres flushes them (or the equivalent for
//! other backends). Concrete impls (e.g. `PgStreamingCdcSource` behind
//! the `pg-streaming` feature) own the underlying connection, parse
//! the wire bytes via the existing [`crate::wal`] parsers, and surface
//! their parser output through `next_item`, each row change as a
//! [`SourceItem::Event`] and each transaction's end as a
//! [`SourceItem::Commit`]. Every event type impls
//! [`crate::backend::CdcEvent`], so the caller reads typed scalars off
//! it directly. The caller drives the loop. Acks flow back through `ack`.
//!
//! The trait is runtime-agnostic: method signatures use `impl Future +
//! Send` (RPITIT) rather than naming any specific executor. Subql core
//! pulls no async runtime dependency. Concrete impls bring whatever
//! runtime they need behind their own feature flag.

use crate::backend::CdcEvent;

/// One item a [`CdcSource`] yields: a row change, or the commit ending the
/// transaction whose row changes came before it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceItem<E, C> {
    /// A row change.
    Event(E),
    /// The end of a transaction, yielded after its last row change.
    Commit(C),
}

impl<E, C> SourceItem<E, C> {
    /// The row change, or `None` for a commit.
    #[must_use]
    pub fn into_event(self) -> Option<E> {
        match self {
            Self::Event(event) => Some(event),
            Self::Commit(_) => None,
        }
    }

    /// The commit, or `None` for a row change.
    #[must_use]
    pub fn into_commit(self) -> Option<C> {
        match self {
            Self::Event(_) => None,
            Self::Commit(commit) => Some(commit),
        }
    }
}

/// The item a [`CdcSource`] `S` yields.
pub type SourceItemOf<S> = SourceItem<<S as CdcSource>::Event, <S as CdcSource>::Commit>;

/// Push-based source of typed CDC events.
///
/// Implementations transparently handle non-event protocol frames
/// (keepalives, relation metadata, transaction begins) and surface the
/// consumer-visible row changes and transaction ends through
/// [`Self::next_item`]. Progress is reported via [`Self::ack`], which the
/// source forwards to the upstream server (e.g. Postgres
/// `StandbyStatusUpdate`) so that WAL can be recycled.
///
/// # Send bounds
///
/// `Self: Send` and the returned futures are `+ Send` so the trait is
/// usable as a bound on engines that themselves move between async
/// tasks. The pattern matches
/// [`AsyncConnector`](crate::reexec::AsyncConnector). See its
/// documentation for why bare `async fn in trait` is avoided.
///
/// # Lifecycle
///
/// 1. Build the source with backend-specific config.
/// 2. Loop calling [`next_item`](Self::next_item). Each call yields
///    at most one item or returns `Ok(None)` on clean shutdown.
/// 3. Periodically call [`ack`](Self::ack) with the latest applied
///    checkpoint so the upstream server can release retained WAL.
///
/// # Examples
///
/// The canonical consume-and-ack loop is exercised end to end by
/// `tests/it/pg_streaming_e2e.rs` (push, via [`crate::PgStreamingCdcSource`])
/// and `tests/it/polling_smoke.rs` (poll, via
/// [`crate::PollingPgCdcSource`]).
pub trait CdcSource: Send {
    /// The typed CDC event this source surfaces.
    ///
    /// Position (checkpoint) is expressed through the event's own
    /// [`CdcEvent::Checkpoint`]. The Postgres sources yield
    /// [`crate::PgChangeEvent`], whose checkpoint is a [`crate::PgCommitPosition`].
    type Event: CdcEvent + Send + Sync;

    /// The end of a transaction this source surfaces after its events.
    ///
    /// The Postgres sources yield [`crate::PgCommit`], which carries the
    /// position acknowledging it and the WAL address the slot then resumes
    /// from. A source without transaction ends uses
    /// [`core::convert::Infallible`].
    type Commit: Send + Sync;

    /// Source-specific error returned by the futures below.
    type Error: core::error::Error + Send + 'static;

    /// Pull the next item from the source.
    ///
    /// Returns `Ok(None)` when the source has cleanly shut down: the
    /// upstream server closed the connection gracefully or the slot
    /// was dropped. Returns `Err` for transport or protocol failures
    /// the consumer must surface (typically by reconnecting or
    /// failing over).
    ///
    /// Implementations consume keepalive frames, relation metadata
    /// messages, and transaction begins internally without surfacing
    /// them to the consumer.
    fn next_item(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send;

    /// Mark every item with checkpoint `<= upto` as durably applied.
    ///
    /// Implementations forward this to the upstream server (Postgres
    /// `StandbyStatusUpdate`, MySQL slave heartbeat, and so on) on a
    /// best-effort cadence so the server can recycle WAL. Calling
    /// `ack` more often than the source's internal status interval is
    /// safe. Calling it less often is also safe but extends the WAL
    /// retention window.
    fn ack(
        &mut self,
        upto: <Self::Event as CdcEvent>::Checkpoint,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// Sets an `AtomicBool` when the loop task exits, however it exits.
///
/// Both Postgres CDC sources run their transport on a task and report
/// liveness to the owning source through this flag. Declared once because
/// the two loops must agree on the store: if one of them stopped
/// publishing the exit, its source would report a dead task as live.
#[cfg(feature = "pg-streaming")]
pub struct ExitFlagGuard(pub alloc::sync::Arc<core::sync::atomic::AtomicBool>);

#[cfg(feature = "pg-streaming")]
impl Drop for ExitFlagGuard {
    fn drop(&mut self) {
        self.0.store(true, core::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    // `manual_async_fn` would have us write `async fn next_item(...)`
    // but bare `async fn in trait` does not produce `Send` futures,
    // which the trait signature requires. The `impl Future + Send`
    // shape is intentional. Clippy's suggestion is wrong here.
    #![allow(clippy::manual_async_fn)]

    use super::*;
    use crate::backend::Postgres;
    use crate::testing::TestEvent;
    use crate::NoCheckpoint;
    use core::convert::Infallible;
    use core::future::Future;

    /// A handwritten `CdcSource` impl that proves the trait is
    /// implementable and the returned futures are `Send`.
    struct NoopSource;

    impl CdcSource for NoopSource {
        type Event = TestEvent<Postgres>;
        type Commit = Infallible;
        type Error = Infallible;

        fn next_item(
            &mut self,
        ) -> impl Future<Output = Result<Option<SourceItemOf<Self>>, Self::Error>> + Send {
            async { Ok(None) }
        }

        fn ack(
            &mut self,
            _upto: <Self::Event as crate::backend::CdcEvent>::Checkpoint,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            async { Ok(()) }
        }
    }

    fn accept_no_checkpoint_source<S>(_: &S)
    where
        S: CdcSource + Send,
        S::Event: crate::backend::CdcEvent<Checkpoint = NoCheckpoint>,
    {
    }

    fn assert_send<T: Send>() {}

    #[test]
    fn cdc_source_send_bounds_compose() {
        assert_send::<NoopSource>();
        accept_no_checkpoint_source(&NoopSource);
    }
}

#[cfg(all(test, feature = "pg-streaming"))]
mod exit_flag_guard_tests {
    use super::ExitFlagGuard;
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicBool, Ordering};

    /// The two source loops report task liveness only through this flag, so
    /// a guard that forgot to publish on drop would let a source report a
    /// dead task as live.
    #[test]
    fn the_flag_is_set_when_the_guard_drops() {
        let flag = Arc::new(AtomicBool::new(false));
        {
            let _guard = ExitFlagGuard(Arc::clone(&flag));
            assert!(
                !flag.load(Ordering::Relaxed),
                "the flag stays clear while the guard is alive"
            );
        }
        assert!(
            flag.load(Ordering::Relaxed),
            "dropping the guard publishes the exit"
        );
    }
}
