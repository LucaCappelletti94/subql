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
    /// The statement this answer re-reads, kept because a saved answer is
    /// restored by planning its SQL again rather than by storing a plan.
    pub sql: String,
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
    /// Column names for [`Self::row`], in projection order. Empty when the row
    /// left the answer, since no row came back to describe.
    pub columns: Vec<String>,
    /// The row as it now is, or `None` when it is no longer in the answer.
    pub row: Option<Vec<Value<B>>>,
    /// Position of the event that produced this, when known.
    pub checkpoint: Option<C>,
}

/// Database read named by one re-execution trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReExecutionRead {
    /// Use the fixed read declared by the registration tier.
    Subscription,
    /// Re-read one displaced grouped extreme.
    GroupedScalar {
        /// Opaque group key used for installation and coalescing.
        group: Vec<u8>,
        /// Runnable two-column SQL constrained to this group.
        sql: String,
        /// Decode hints for the extreme and source-row count.
        column_kinds: [crate::backend::BuiltinKind; 2],
    },
}

impl ReExecutionRead {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReExecutionTrigger<I: IdTypes, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// The captured query needing re-execution.
    pub subscription_id: SubscriptionId,
    /// Consumer that registered the query.
    pub consumer_id: I::ConsumerId,
    /// Fixed subscription read or one group-scoped read.
    pub read: ReExecutionRead,
    /// Position of the event that triggered this re-execution, when known.
    pub checkpoint: Option<C>,
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
    pub triggers: Vec<ReExecutionTrigger<I, C>>,
    /// Subscriptions that changed maintenance tier.
    pub transitions: Vec<crate::MaintenanceTransition>,
}

/// Batch result returned by both connector modes.
///
/// Keeps per-event row notifications in input order and coalesces database
/// reads by subscription identity.
/// **Every channel has to be drained**, for the reason given on
/// [`ReExecNotifications`]: the deliveries have different shapes rather than
/// being alternatives, so ignoring one silently drops every subscription that
/// uses it.
pub struct BatchOutcome<I: IdTypes, B: Backend, C: crate::Checkpoint = crate::NoCheckpoint> {
    /// View-relative engine notifications, one entry per input event in
    /// the order they were supplied.
    pub per_event: Vec<ConsumerNotifications<I, C, B>>,
    /// Grouped aggregate rows written or removed during the batch.
    pub aggregate_updates: Vec<crate::AggregateValueUpdate<I, B>>,
    /// Scalar updates produced in-process during the batch.
    pub scalar_updates: Vec<ScalarUpdate<I, B, C>>,
    /// Pages of re-read captured queries produced during the batch.
    pub rows_updates: Vec<RowsUpdate<I, B, C>>,
    /// Per-row changes from keyed captures produced during the batch.
    pub row_deltas: Vec<RowDelta<I, B, C>>,
    /// Re-execution triggers, deduplicated by subscription and group scope.
    pub triggers: Vec<ReExecutionTrigger<I, C>>,
    /// Tier changes produced during the batch.
    pub transitions: Vec<crate::MaintenanceTransition>,
}
