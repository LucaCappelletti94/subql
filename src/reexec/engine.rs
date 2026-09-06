//! Rust types for database-read maintenance and connector-wrapper outputs.
//!
//! The registry stores [`ReExecEntry`] internally. Public output structs carry
//! scalar changes, row pages, keyed row changes and required database reads.

use crate::backend::{Backend, Value};
use crate::{ConsumerNotifications, IdTypes, SubscriptionId, TableId};
use alloc::string::String;
use alloc::vec::Vec;

/// A captured re-execution query: the table it reads (for cleanup
/// routing), who owns it, and the maintenance state machine that tracks
/// its value.
pub struct ReExecEntry<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Session owning the query, if session-scoped (for cleanup).
    pub session: Option<I::SessionId>,
    /// Table the query reads from (routing + cleanup).
    /// Every table whose changes route to this query. One for a scalar
    /// aggregate, one per referenced table for a whole re-read, and removal
    /// walks all of them so a multi-table capture leaves no stale routing.
    pub tables: Vec<TableId>,
    /// In-process maintenance state machine.
    pub runtime: super::maintain::QueryRuntime<B, C>,
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    /// Original query used for replanning and persistence.
    pub source_query: crate::reexec::BoundQuery<B>,
    /// Executable query for the current fixed read tier.
    pub read_query: crate::reexec::BoundQuery<B>,
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    /// Which read serves it, as saved.
    pub tier: crate::ReadTier,
    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    /// Whether database reads run under the individual consumer's identity.
    pub database_reads_per_consumer: bool,
}

/// A re-executed scalar whose value changed, to be delivered to its
/// consumer.
///
/// Emitted when the in-process maintenance state machine produced a new
/// value from the event (no DB round-trip needed). Carries the originating
/// event's [`Checkpoint`](crate::Checkpoint) when known.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarUpdate<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query whose value changed.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// The new scalar value (`Value::Null` if the aggregate is now empty).
    pub value: Value<B>,
    /// Position of the event that produced this update, when known.
    pub checkpoint: Option<C>,
}

/// One page of a re-read captured query, delivered to its consumer.
///
/// A whole-re-read capture holds no answer, so a change does not produce a
/// delta: it produces the answer again, in pages. `generation` says which
/// re-read a page belongs to and `more` says whether the re-read is finished,
/// which together are what let a consumer replace what it had without ever
/// showing a half-replaced answer.
#[derive(Debug, Clone, PartialEq)]
pub struct RowsUpdate<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query these rows answer.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Which re-read this page belongs to, increasing by one per re-read.
    ///
    /// Pages of one re-read share a generation. A page carrying a higher one
    /// starts a new answer, which is the signal to discard the previous.
    /// Pages stream as they are fetched, so a read that fails or is
    /// abandoned leaves a generation with no final page. The retried read
    /// delivers the higher generation that supersedes it.
    pub generation: u64,
    /// Column names as the database reported them, in projection order.
    pub columns: Vec<String>,
    /// This page's rows, in `columns` order.
    pub rows: Vec<Vec<Value<B>>>,
    /// Whether further pages of this same re-read follow.
    pub more: bool,
    /// Position of the event that triggered the re-read, when known.
    pub checkpoint: Option<C>,
}

/// One row of a keyed capture's answer entering, changing, or leaving.
///
/// The keyed tier's delivery: a change costs one small read and produces one of
/// these per row asked about, rather than the whole answer again. `row` absent
/// means the row is no longer in the answer, and applying a removal for a row
/// the caller never held is harmless, which is what lets this carry no state.
#[derive(Debug, Clone, PartialEq)]
pub struct RowDelta<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query this row belongs to.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Primary key values identifying the row, in key-column order.
    pub key: Vec<Value<B>>,
    /// Column names for [`Self::row`], in projection order, shared across
    /// every delta of one read so the schema is allocated once per read.
    /// Empty when the row left the answer, since no row came back to
    /// describe.
    pub columns: alloc::sync::Arc<[String]>,
    /// The row as it now is, or `None` when it is no longer in the answer.
    pub row: Option<Vec<Value<B>>>,
    /// Position of the event that produced this, when known.
    pub checkpoint: Option<C>,
}

/// Database read named by one re-execution trigger.
#[derive(Debug, PartialEq)]
pub enum ReExecutionRead<B: Backend = crate::backend::Postgres> {
    /// Use the fixed read declared by the registration tier.
    Subscription,
    /// Re-read one displaced grouped extreme.
    GroupedScalar {
        /// Opaque group key used for installation and coalescing.
        group: Vec<u8>,
        /// Bound two-column query constrained to this group.
        query: crate::reexec::BoundQuery<B>,
        /// Decode hints for the extreme and source-row count.
        column_kinds: [crate::backend::ScalarFamily; 2],
    },
}

// Manual rather than derived: the derive would demand `B: Clone` although
// every stored field clones without it.
impl<B: Backend> Clone for ReExecutionRead<B> {
    fn clone(&self) -> Self {
        match self {
            Self::Subscription => Self::Subscription,
            Self::GroupedScalar {
                group,
                query,
                column_kinds,
            } => Self::GroupedScalar {
                group: group.clone(),
                query: query.clone(),
                column_kinds: *column_kinds,
            },
        }
    }
}

impl<B: Backend> ReExecutionRead<B> {
    pub(crate) fn group_key(&self) -> Option<&[u8]> {
        match self {
            Self::Subscription => None,
            Self::GroupedScalar { group, .. } => Some(group),
        }
    }
}

/// Signal that a captured query needs to be re-executed.
///
/// Emitted when the in-process maintenance state machine cannot resolve
/// the result for an event. The caller (materializer) must re-execute the
/// query and call [`Install::install`](crate::Install::install) with the recomputed value.
///
/// Triggers are designed to be **idempotent and coalescible**: emitting
/// the same trigger twice is safe (a single re-execution serves any
/// number of pending triggers), and `install` unconditionally overwrites
/// the stored value.
#[derive(Debug, PartialEq)]
pub struct ReExecutionTrigger<
    I: IdTypes,
    C: crate::Checkpoint = crate::NoCheckpoint,
    B: Backend = crate::backend::Postgres,
> {
    /// The captured query needing re-execution.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Fixed subscription read or one group-scoped read.
    pub read: ReExecutionRead<B>,
    /// Position of the event that triggered this re-execution, when known.
    pub checkpoint: Option<C>,
}

// Manual rather than derived: the derive would demand `I: Clone` although
// only `I::ConsumerId` (always `Copy` per `Id`) is stored.
impl<I: IdTypes, C: crate::Checkpoint, B: Backend> Clone for ReExecutionTrigger<I, C, B> {
    fn clone(&self) -> Self {
        Self {
            subscription_id: self.subscription_id,
            consumer_id: self.consumer_id,
            read: self.read.clone(),
            checkpoint: self.checkpoint.clone(),
        }
    }
}

/// Everything one dispatched event produced across seven channels.
///
/// Every channel has to be drained because each tier has one fixed delivery
/// shape, and ignoring one silently drops that tier's subscriptions.
///
/// - [`engine`](Self::engine): rows the in-process engine matched directly.
/// - [`aggregate_updates`](Self::aggregate_updates): aggregate result rows.
/// - [`scalar_updates`](Self::scalar_updates): ungrouped extreme values.
/// - [`rows_updates`](Self::rows_updates): complete row-result pages.
/// - [`row_deltas`](Self::row_deltas): keyed row changes.
/// - [`triggers`](Self::triggers): unresolved database reads.
/// - [`transitions`](Self::transitions): identity-preserving tier changes.
pub struct ReExecNotifications<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// View-relative notifications from the core engine.
    pub engine: ConsumerNotifications<I, C, B>,
    /// Grouped aggregate rows written or removed.
    pub aggregate_updates: Vec<crate::AggregateValueUpdate<I, B>>,
    /// Scalar values that changed in-process (no DB round-trip).
    pub scalar_updates: Vec<ScalarUpdate<I, B, C>>,
    /// Pages of re-read captured queries, in delivery order.
    pub rows_updates: Vec<RowsUpdate<I, B, C>>,
    /// Per-row changes from keyed captures, in delivery order.
    pub row_deltas: Vec<RowDelta<I, B, C>>,
    /// Queries whose maintenance could not resolve in-process. The
    /// materializer must re-execute and call [`Install::install`](crate::Install::install).
    pub triggers: Vec<ReExecutionTrigger<I, C, B>>,
    /// Subscriptions that changed maintenance tier.
    pub transitions: Vec<crate::MaintenanceTransition<B>>,
}

/// Everything one dispatched event produced when the engine owns the
/// connector and does the reads itself.
///
/// Carries no `rows_updates`, `row_deltas` or `triggers`: this engine
/// queues its own reads, and their answers arrive only through
/// [`resolve`](crate::reexec::AutoResolvingEngine::resolve).
pub struct Dispatched<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// View-relative notifications from the core engine.
    pub engine: ConsumerNotifications<I, C, B>,
    /// Grouped aggregate rows written or removed in process.
    pub aggregate_updates: Vec<crate::AggregateValueUpdate<I, B>>,
    /// Scalar values that changed in process, with no database round trip.
    pub scalar_updates: Vec<ScalarUpdate<I, B, C>>,
    /// Subscriptions that changed maintenance tier.
    pub transitions: Vec<crate::MaintenanceTransition<B>>,
    /// Depth of the read queue after this event.
    ///
    /// Not a completeness signal, and not a promise about which channels
    /// `resolve` will deliver: a refused predicate queues no read and is
    /// reported through
    /// [`engine.evaluation_failures`](crate::ConsumerNotifications::evaluation_failures)
    /// instead.
    pub outstanding: usize,
    /// Reads discarded because one for the same subscription and group ran
    /// inside the [debounce
    /// window](crate::reexec::AutoResolvingEngine::with_debounce_per_query).
    ///
    /// Discarded, not deferred: nothing reschedules when the window
    /// expires, so the window rate-limits reads and does not bound
    /// staleness.
    pub debounced: usize,
}

/// One result delivered by `resolve` as its read completes.
///
/// The sink receives each installed answer the moment its read finishes,
/// so retained memory tracks one read rather than the whole drain.
#[derive(Debug, Clone)]
pub enum ReadDelivery<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// A scalar captured query's re-read value.
    Scalar(ScalarUpdate<I, B, C>),
    /// A grouped aggregate row written or removed by a grouped read.
    Aggregate(crate::AggregateValueUpdate<I, B>),
    /// One page of a whole re-read answer.
    Rows(RowsUpdate<I, B, C>),
    /// One row of a keyed capture entering, changing, or leaving.
    Delta(RowDelta<I, B, C>),
    /// A subscription changed maintenance tier during resolution.
    Transition(crate::MaintenanceTransition<B>),
}

/// Every delivery of one `resolve` drain, buffered by channel.
///
/// The convenience shape over the sink primitive, for callers that want the
/// whole drain in hand. **Every channel has to be drained**, for the reason
/// given on [`ReExecNotifications`]: the deliveries have different shapes
/// rather than being alternatives, so ignoring one silently drops every
/// subscription that uses it.
#[derive(Debug, Clone)]
pub struct ResolvedReads<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// Scalar re-read values, in delivery order.
    pub scalar_updates: Vec<ScalarUpdate<I, B, C>>,
    /// Grouped aggregate rows written or removed, in delivery order.
    pub aggregate_updates: Vec<crate::AggregateValueUpdate<I, B>>,
    /// Pages of whole re-read answers, in delivery order.
    pub rows_updates: Vec<RowsUpdate<I, B, C>>,
    /// Per-row keyed changes, in delivery order.
    pub row_deltas: Vec<RowDelta<I, B, C>>,
    /// Tier changes produced during resolution.
    pub transitions: Vec<crate::MaintenanceTransition<B>>,
}

impl<I: IdTypes, B: Backend, C: crate::Checkpoint> Default for ResolvedReads<I, B, C> {
    fn default() -> Self {
        Self {
            scalar_updates: Vec::new(),
            aggregate_updates: Vec::new(),
            rows_updates: Vec::new(),
            row_deltas: Vec::new(),
            transitions: Vec::new(),
        }
    }
}

impl<I: IdTypes, B: Backend, C: crate::Checkpoint> ResolvedReads<I, B, C> {
    /// File one delivery under its channel.
    pub fn push(&mut self, delivery: ReadDelivery<I, B, C>) {
        match delivery {
            ReadDelivery::Scalar(update) => self.scalar_updates.push(update),
            ReadDelivery::Aggregate(update) => self.aggregate_updates.push(update),
            ReadDelivery::Rows(update) => self.rows_updates.push(update),
            ReadDelivery::Delta(delta) => self.row_deltas.push(delta),
            ReadDelivery::Transition(transition) => self.transitions.push(transition),
        }
    }
}
