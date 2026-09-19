//! The value a dispatched event hands back before its queued reads have run.

use super::async_auto::AsyncMode;
use super::async_connector::AsyncConnector;
use super::auto::{AutoResolvingEngine, ResolverMode, SyncMode};
use super::connector::{Connector, ReExecError};
use super::engine::{Dispatched, ReadDelivery, ResolvedReads};
use crate::backend::{Backend, CdcEvent};
use crate::compiler::literals::SqlLiteralParse;
use crate::{Checkpoint, IdTypes};
use sql_traits::prelude::DatabaseLike;

/// One applied event, holding its notifications back until its reads run.
///
/// [`apply`](AutoResolvingEngine::apply) folds the event and queues the reads
/// it made necessary rather than running them in place, so a read's answer
/// arrives from the drain and never from the value `apply` returned. Handing
/// the notifications straight back is what let a caller take them, skip the
/// drain, and lose every queued read with nothing said. Here they are
/// reachable through [`resolve`](Self::resolve) and
/// [`resolve_collect`](Self::resolve_collect) and through nothing else. A
/// caller that drains on a schedule of its own asks for the notifications by
/// name instead, through
/// [`apply_leaving_reads_queued`](AutoResolvingEngine::apply_leaving_reads_queued).
///
/// The engine stays mutably borrowed for as long as this value lives, so a
/// second event cannot be dispatched while the first one's reads sit
/// undelivered, and the drain can only ever run against the engine that
/// queued them.
///
/// The queue depth is readable without draining, since it is a count and not
/// a delivery. This is the shape every example below shares, and it compiles.
///
/// ```
/// use subql::backend::CdcEvent;
/// use subql::compiler::SqlLiteralParse;
/// use subql::reexec::{AutoResolvingEngine, ResolverMode};
/// use subql::{DatabaseLike, IdTypes};
///
/// fn queue_depth<E, I, DB, M>(
///     engine: &mut AutoResolvingEngine<E, I, DB, M>,
///     event: &E,
/// ) -> usize
/// where
///     E: CdcEvent,
///     E::Backend: SqlLiteralParse,
///     I: IdTypes,
///     DB: DatabaseLike + 'static,
///     M: ResolverMode<E::Backend>,
/// {
///     engine.apply(event).expect("the event applies").outstanding()
/// }
/// ```
///
/// Reading a delivery off it does not compile, which is the whole point. The
/// one difference from the example above is the last line.
///
/// ```compile_fail
/// use subql::backend::CdcEvent;
/// use subql::compiler::SqlLiteralParse;
/// use subql::reexec::{AutoResolvingEngine, ResolverMode};
/// use subql::{DatabaseLike, IdTypes};
///
/// fn read_without_draining<E, I, DB, M>(
///     engine: &mut AutoResolvingEngine<E, I, DB, M>,
///     event: &E,
/// ) where
///     E: CdcEvent,
///     E::Backend: SqlLiteralParse,
///     I: IdTypes,
///     DB: DatabaseLike + 'static,
///     M: ResolverMode<E::Backend>,
/// {
///     let dispatch = engine.apply(event).expect("the event applies");
///     let _ = dispatch.aggregate_updates;
/// }
/// ```
///
/// Letting it drop unread does not compile either, under the `-D warnings`
/// this crate is built with.
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// use subql::backend::CdcEvent;
/// use subql::compiler::SqlLiteralParse;
/// use subql::reexec::{AutoResolvingEngine, ResolverMode};
/// use subql::{DatabaseLike, IdTypes};
///
/// fn drop_unread<E, I, DB, M>(
///     engine: &mut AutoResolvingEngine<E, I, DB, M>,
///     event: &E,
/// ) where
///     E: CdcEvent,
///     E::Backend: SqlLiteralParse,
///     I: IdTypes,
///     DB: DatabaseLike + 'static,
///     M: ResolverMode<E::Backend>,
/// {
///     engine.apply(event).expect("the event applies");
/// }
/// ```
#[must_use = "the reads this event queued run only when this value is drained"]
pub struct Dispatch<'engine, E, I, DB, M>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike,
    M: ResolverMode<E::Backend>,
{
    engine: &'engine mut AutoResolvingEngine<E, I, DB, M>,
    notifications: Dispatched<I, E::Backend, E::Checkpoint>,
}

impl<'engine, E, I, DB, M> Dispatch<'engine, E, I, DB, M>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike,
    M: ResolverMode<E::Backend>,
{
    pub(super) const fn new(
        engine: &'engine mut AutoResolvingEngine<E, I, DB, M>,
        notifications: Dispatched<I, E::Backend, E::Checkpoint>,
    ) -> Self {
        Self {
            engine,
            notifications,
        }
    }

    /// [`Dispatched::outstanding`], readable before the drain runs.
    #[must_use]
    pub const fn outstanding(&self) -> usize {
        self.notifications.outstanding
    }

    /// [`Dispatched::debounced`], readable before the drain runs.
    #[must_use]
    pub const fn debounced(&self) -> usize {
        self.notifications.debounced
    }
}

impl<E, I, DB, X> Dispatch<'_, E, I, DB, SyncMode<X>>
where
    E: CdcEvent,
    E::Backend: SqlLiteralParse,
    I: IdTypes,
    DB: DatabaseLike + 'static,
    X: Connector<Backend = E::Backend>,
{
    /// Execute every read this event queued, delivering each installed answer
    /// into `sink` as it completes, and hand back both halves.
    ///
    /// The read half carries the failure when a read fails, on the terms
    /// [`AutoResolvingEngine::resolve`] describes, while the in-process half
    /// arrives either way. The event was folded exactly once and nothing will
    /// offer those notifications again, so a failed read cannot be allowed to
    /// take them with it.
    pub fn resolve<S>(self, sink: S) -> Settled<I, E::Backend, E::Checkpoint, X::Error, ()>
    where
        S: FnMut(ReadDelivery<I, E::Backend, E::Checkpoint>),
    {
        let Self {
            engine,
            notifications,
        } = self;
        Settled {
            dispatched: notifications,
            reads: engine.resolve(sink),
        }
    }

    /// Execute every read this event queued, buffering the deliveries by
    /// channel, and hand back both halves.
    ///
    /// The convenience shape over [`resolve`](Self::resolve), with the same
    /// split of fates between the two halves.
    pub fn resolve_collect(self) -> Settled<I, E::Backend, E::Checkpoint, X::Error> {
        let Self {
            engine,
            notifications,
        } = self;
        Settled {
            dispatched: notifications,
            reads: engine.resolve_collect(),
        }
    }
}

/// Drains spelled `+ Send` like `consumers`, since a leaked auto trait does
/// not reach a caller through the trait's associated type.
impl<'engine, E, I, DB, X> Dispatch<'engine, E, I, DB, AsyncMode<X>>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    E::Checkpoint: Send + Sync,
    I: IdTypes,
    I::ConsumerId: Send,
    I::SessionId: Send,
    DB: DatabaseLike + Send + Sync + 'static,
    X: AsyncConnector<Backend = E::Backend>,
{
    /// Async twin of the synchronous `resolve`, with the concurrency the
    /// async engine's drain describes.
    ///
    /// Dropping the returned future loses nothing. The reads it had not run
    /// stay queued, as
    /// [`AutoResolvingEngine::resolve`](crate::reexec::AutoResolvingEngine::resolve)
    /// promises, and the notifications are parked on the engine rather than
    /// carried inside the future, so a timeout or a losing `select!` arm
    /// cannot destroy them. They are claimed with
    /// [`take_undelivered`](crate::reexec::AutoResolvingEngine::take_undelivered).
    ///
    /// The parking happens in the call rather than in the future, because an
    /// `async fn` body waits for its first poll and a future may be dropped
    /// before ever being polled.
    pub fn resolve<S>(
        self,
        sink: S,
    ) -> impl core::future::Future<Output = Settled<I, E::Backend, E::Checkpoint, X::Error, ()>>
           + Send
           + 'engine
    where
        S: FnMut(ReadDelivery<I, E::Backend, E::Checkpoint>) + Send + 'engine,
    {
        let Self {
            engine,
            notifications,
        } = self;
        engine.park_undelivered(notifications);
        async move {
            let reads = engine.resolve(sink).await;
            Settled {
                dispatched: engine.claim_undelivered(),
                reads,
            }
        }
    }

    /// Async twin of the synchronous `resolve_collect`, abandoned as safely
    /// as [`resolve`](Self::resolve) and parking in the call for the same
    /// reason.
    pub fn resolve_collect(
        self,
    ) -> impl core::future::Future<Output = Settled<I, E::Backend, E::Checkpoint, X::Error>>
           + Send
           + 'engine {
        let Self {
            engine,
            notifications,
        } = self;
        engine.park_undelivered(notifications);
        async move {
            let reads = engine.resolve_collect().await;
            Settled {
                dispatched: engine.claim_undelivered(),
                reads,
            }
        }
    }
}

/// Both halves of one settled event, what it answered in process and what its
/// reads answered.
///
/// The halves have independent fates because `apply` committed the event
/// exactly once before any read ran. A read that fails leaves
/// [`reads`](Self::reads) holding that failure, and
/// [`dispatched`](Self::dispatched) still holds the notifications the fold
/// produced, which nothing will offer a second time. Retrying a read is a
/// later drain through [`AutoResolvingEngine::resolve`], never a second
/// `apply` of the same event.
#[must_use = "both halves carry deliveries, and dropping either one drops its subscriptions' updates"]
pub struct Settled<I: IdTypes, B: Backend, C: Checkpoint, XE, R = ResolvedReads<I, B, C>> {
    /// What the event answered in process, with no database round trip.
    pub dispatched: Dispatched<I, B, C>,
    /// What the queued reads answered, or the failure that stopped them.
    pub reads: Result<R, ReExecError<XE>>,
}
