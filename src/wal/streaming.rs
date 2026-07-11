//! Push-based CDC source trait.
//!
//! [`CdcSource`] abstracts any transport that delivers replication
//! events to subql as Postgres flushes them (or the equivalent for
//! other backends). Concrete impls (e.g. `PgStreamingCdcSource` behind
//! the `pg-streaming` feature) own the underlying connection, parse
//! the wire bytes via the existing [`crate::wal`] parsers, and surface
//! typed [`crate::WalEvent`]s through `next_event`. The caller drives
//! the loop. Acks flow back through `ack`.
//!
//! The trait is runtime-agnostic: method signatures use `impl Future +
//! Send` (RPITIT) rather than naming any specific executor. Subql core
//! pulls no async runtime dependency. Concrete impls bring whatever
//! runtime they need behind their own feature flag.


use crate::backend::CdcEvent;

/// Push-based source of typed CDC events.
///
/// Implementations transparently handle non-event protocol frames
/// (keepalives, relation metadata, transaction boundaries) and surface
/// only the consumer-visible [`Self::Event`]s through [`Self::next_event`].
/// Progress is reported via [`Self::ack`], which the source forwards to
/// the upstream server (e.g. Postgres `StandbyStatusUpdate`) so that
/// WAL can be recycled.
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
/// 2. Loop calling [`next_event`](Self::next_event). Each call yields
///    at most one event or returns `Ok(None)` on clean shutdown.
/// 3. Periodically call [`ack`](Self::ack) with the latest applied
///    checkpoint so the upstream server can release retained WAL.
///
/// # Examples
///
/// The canonical consume-and-ack loop is exercised end to end by
/// `tests/pgoutput_e2e.rs` (push, via [`crate::PgStreamingCdcSource`]) and
/// `tests/polling_smoke.rs` (poll, via
/// [`crate::PollingPgCdcSource`]).
pub trait CdcSource: Send {
    /// The typed CDC event this source surfaces.
    ///
    /// Position (checkpoint) is expressed through the event's own
    /// [`CdcEvent::Checkpoint`]. Postgres sources typically pick a
    /// concrete Backend = Postgres event with `Checkpoint = PgLsn`.
    type Event: CdcEvent + Send + Sync;

    /// Source-specific error returned by the futures below.
    type Error: core::error::Error + Send + 'static;

    /// Pull the next event from the source.
    ///
    /// Returns `Ok(None)` when the source has cleanly shut down: the
    /// upstream server closed the connection gracefully or the slot
    /// was dropped. Returns `Err` for transport or protocol failures
    /// the consumer must surface (typically by reconnecting or
    /// failing over).
    ///
    /// Implementations consume keepalive frames, relation metadata
    /// messages, and transaction-boundary protocol frames internally
    /// without surfacing them to the consumer.
    fn next_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Option<Self::Event>, Self::Error>> + Send;

    /// Mark every event with checkpoint `<= upto` as durably applied.
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

#[cfg(test)]
mod tests {
    // `manual_async_fn` would have us write `async fn next_event(...)`
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
        type Error = Infallible;

        fn next_event(
            &mut self,
        ) -> impl Future<Output = Result<Option<Self::Event>, Self::Error>> + Send {
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
