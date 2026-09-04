//! Subscription traits and notification output types.

#[cfg(feature = "std")]
use super::domain_id_types::MergeJobId;
use super::domain_id_types::{ColumnId, TableId};
use super::generic_id_types::{IdTypes, SubscriptionId};
#[cfg(feature = "std")]
use super::subscription_types::MergeReport;
use super::subscription_types::{
    Registered, SubscriptionRequest, Tier, TierKind, UnregisterReport,
};
use crate::backend::{Backend, CdcEvent, Value};
use crate::checkpoint::{Checkpoint, NoCheckpoint};
use alloc::string::ToString;
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::path::PathBuf;

/// Subscription registration operations.
///
/// Parameterised on the observed [`crate::backend::Backend`] so
/// `register` accepts a typed [`SubscriptionRequest`] whose bind values
/// are `Value<B>`.
pub trait SubscriptionRegistration<I: IdTypes, B: Backend>: Send {
    /// Register a new subscription.
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and
    /// binds consumer. Returns error if SQL is unparseable or unsupported.
    fn register(
        &mut self,
        spec: SubscriptionRequest<I, B>,
    ) -> Result<Registered<B>, crate::RegisterError>;

    /// Unregister a subscription by ID.
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate
    /// is removed. Returns true if subscription existed and was removed.
    fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool;
}

/// Event dispatch operations.
///
/// Parameterised on the observed `E: CdcEvent` so `consumers` accepts a
/// backend-typed event and returns notifications carrying `E::Checkpoint`.
/// Each engine layer chooses its own [`Notifications`](Self::Notifications)
/// shape: the base engine yields [`ConsumerNotifications`], while the
/// re-execution wrappers yield their richer `ReExecNotifications`.
pub trait SubscriptionDispatch<I: IdTypes, E: CdcEvent>: Send {
    /// Notifications produced for a dispatched event.
    type Notifications;
    /// Error returned when dispatch fails.
    type Error;

    /// Get interested consumers for a CDC event.
    ///
    /// Returns view-relative notifications: each consumer sees
    /// INSERT / DELETE / UPDATE relative to their own result set.
    fn consumers(&mut self, event: &E) -> Result<Self::Notifications, Self::Error>;
}

/// Async counterpart of [`SubscriptionDispatch`].
///
/// Separate trait because the async engine's `consumers` returns a
/// future. The `+ Send` bound on that future is the point of spelling it
/// out as return-position `impl Future` rather than `async fn` (same
/// idiom as [`crate::reexec::AsyncConnector`]).
pub trait AsyncSubscriptionDispatch<I: IdTypes, E: CdcEvent>: Send {
    /// Notifications produced for a dispatched event.
    type Notifications;
    /// Error returned when dispatch fails.
    type Error;

    /// Get interested consumers for a CDC event.
    fn consumers(
        &mut self,
        event: &E,
    ) -> impl core::future::Future<Output = Result<Self::Notifications, Self::Error>> + Send;
}

/// Session lifecycle operations
pub trait SubscriptionUnregistration<I: IdTypes>: Send {
    /// Unregister all subscriptions for a session
    ///
    /// Removes all session-bound subscriptions, decrements refcounts, prunes predicates.
    /// Durable subscriptions (`SubscriptionScope::Durable`) are NOT affected.
    fn unregister_session(&mut self, session_id: I::SessionId) -> UnregisterReport;

    /// Unregister all subscriptions for a consumer matching a specific SQL query.
    ///
    /// Parses the SQL just enough to compute the predicate hash (no bytecode
    /// compilation), then removes all bindings for `consumer_id` that share that
    /// hash. Returns a [`UnregisterReport`] with the counts of removed bindings,
    /// predicates, and consumer-dictionary entries.
    fn unregister_query(
        &mut self,
        _consumer_id: I::ConsumerId,
        _sql: &str,
    ) -> Result<UnregisterReport, crate::RegisterError> {
        Err(crate::RegisterError::UnsupportedSql(
            "unregister_query not supported".to_string(),
        ))
    }
}

/// Durable shard storage operations
#[cfg(feature = "std")]
pub trait DurableShardStore: Send {
    /// Snapshot a table partition to durable storage.
    fn snapshot_table(&self, table_id: TableId) -> Result<(), crate::StorageError>;
}

/// Current value of an aggregate subscription, as the engine reports it on
/// [`AggregateValueUpdate`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggValue {
    /// `COUNT(*)` or `COUNT(col)`.
    Count(i64),
    /// `SUM(col)`.
    Sum(f64),
    /// A real-valued aggregate (AVG, variance, stddev). `None` when undefined
    /// for the current row count.
    Real(Option<f64>),
}

impl core::fmt::Display for AggValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Count(c) => write!(f, "{c}"),
            Self::Sum(s) => write!(f, "{s}"),
            Self::Real(Some(v)) => write!(f, "{v}"),
            Self::Real(None) => f.write_str("-"),
        }
    }
}

/// A subscription whose answer changed because a relationship moved, rather than
/// because a row it reads changed.
///
/// Carries no row. The rows leaving are ones the consumer already holds, and it
/// can match this against its own copy. The rows entering were never in the
/// change event, so the consumer reads them through the snapshot path it already
/// has, under row-level security, if the application wants them at all.
///
/// Emitted rather than left silent because a subscription is a standing query: if
/// the engine knows the answer changed and says nothing, a row that never changes
/// again never arrives.
// `Clone`, `Debug` and `PartialEq` are hand-implemented for the same reason
// `Value<B>`'s are: a derive would put the bound on the backend marker `B`
// rather than on the scalar types the value names.
pub struct TermNarrowing<B: Backend> {
    /// The subscription whose answer changed.
    pub subscription: SubscriptionId,
    /// The table that subscription reads.
    pub table: TableId,
    /// The columns its membership subquery compares, in the filter's order.
    pub columns: alloc::vec::Vec<ColumnId>,
    /// The value row those columns now match, or stopped matching, pairwise
    /// with [`columns`](Self::columns).
    pub values: alloc::vec::Vec<Value<B>>,
    /// Whether the values entered the subscription's set or left it.
    pub entered: bool,
}

impl<B: Backend> Clone for TermNarrowing<B> {
    fn clone(&self) -> Self {
        Self {
            subscription: self.subscription,
            table: self.table,
            columns: self.columns.clone(),
            values: self.values.clone(),
            entered: self.entered,
        }
    }
}

impl<B: Backend> core::fmt::Debug for TermNarrowing<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TermNarrowing")
            .field("subscription", &self.subscription)
            .field("table", &self.table)
            .field("columns", &self.columns)
            .field("values", &self.values)
            .field("entered", &self.entered)
            .finish()
    }
}

impl<B: Backend> PartialEq for TermNarrowing<B> {
    fn eq(&self, other: &Self) -> bool {
        self.subscription == other.subscription
            && self.table == other.table
            && self.columns == other.columns
            && self.values == other.values
            && self.entered == other.entered
    }
}

/// Per-consumer notification classification from `consumers()`.
///
/// Each consumer sees events **relative to their own result set** (view-relative
/// deltas), not the base-table operation.  A single base-table UPDATE may
/// produce `Inserted` for one consumer, `Deleted` for another, and `Updated`
/// for a third.
///
/// Carries the originating event's [`Checkpoint`] so downstream replay /
/// oplog code can correlate notifications with positions in the source
/// stream. Default `C = NoCheckpoint` preserves the original API for
/// callers that do not care about positions.
///
/// `B` defaults to `Postgres` for the same reason
/// [`SubscriptionRequest`]'s does: it appears only in
/// [`narrowings`](Self::narrowings), which is empty unless a membership subquery
/// is registered.
pub struct ConsumerNotifications<
    I: IdTypes,
    C: Checkpoint = NoCheckpoint,
    B: Backend = crate::backend::Postgres,
> {
    /// Consumers for whom a row appeared in their result set.
    /// (Base INSERT, or base UPDATE where new row matches but old didn't.)
    pub(crate) inserted: Vec<I::ConsumerId>,
    /// Consumers for whom a row disappeared from their result set.
    /// (Base DELETE, or base UPDATE where old row matched but new doesn't.)
    pub(crate) deleted: Vec<I::ConsumerId>,
    /// Consumers for whom a row changed but remained in their result set.
    /// (Base UPDATE where both old and new rows match.)
    pub(crate) updated: Vec<I::ConsumerId>,
    /// Position of the originating event, when known.
    pub(crate) checkpoint: Option<C>,
    /// Subscriptions whose answer changed because a relationship moved rather
    /// than because a row they read changed. Empty for every event on a table no
    /// membership subquery reads through.
    pub(crate) narrowings: Vec<TermNarrowing<B>>,
    /// Subscriptions whose predicate the target engine refuses to evaluate
    /// for this row, with the cause. Reported rather than folded into a
    /// no-match, because `Value::Null` composes through `OR` and would turn
    /// a refusal into a silent wrong answer.
    pub(crate) evaluation_failures: Vec<EvaluationFailure<I>>,
}

/// One subscription's predicate that the target engine refuses to evaluate
/// for one row.
///
/// Per subscription: a row that overflows one predicate's arithmetic leaves
/// every other subscription reading the same event answered.
#[derive(Clone, Copy, Debug)]
pub struct EvaluationFailure<I: IdTypes> {
    /// The subscription that could not be evaluated. One consumer can hold
    /// several, so this and not the consumer is what identifies the failure.
    pub subscription_id: crate::SubscriptionId,
    /// The consumer that subscription belongs to.
    pub consumer_id: I::ConsumerId,
    /// What the engine refuses.
    pub refusal: crate::compiler::vm::arithmetic::ArithmeticFailure,
}

// Hand-implemented for the same reason as `Value<B>`: `#[derive]` would
// require `I: PartialEq`, which `IdTypes` does not imply, while the two
// fields it compares are both comparable on their own.
impl<I: IdTypes> PartialEq for EvaluationFailure<I> {
    fn eq(&self, other: &Self) -> bool {
        self.subscription_id == other.subscription_id
            && self.consumer_id == other.consumer_id
            && self.refusal == other.refusal
    }
}

impl<I: IdTypes> Eq for EvaluationFailure<I> {}

impl<I: IdTypes, C: Checkpoint, B: Backend> ConsumerNotifications<I, C, B> {
    /// No consumer notified, no checkpoint, no narrowing.
    pub(crate) const fn empty() -> Self {
        Self::from_parts(Vec::new(), Vec::new(), Vec::new())
    }

    pub(crate) const fn from_parts(
        inserted: Vec<I::ConsumerId>,
        deleted: Vec<I::ConsumerId>,
        updated: Vec<I::ConsumerId>,
    ) -> Self {
        Self {
            inserted,
            deleted,
            updated,
            checkpoint: None,
            narrowings: Vec::new(),
            evaluation_failures: Vec::new(),
        }
    }

    /// Attach a checkpoint to these notifications.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: Option<C>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Consumers notified as inserted.
    #[must_use]
    pub fn inserted(&self) -> &[I::ConsumerId] {
        &self.inserted
    }

    /// Consumers notified as deleted.
    #[must_use]
    pub fn deleted(&self) -> &[I::ConsumerId] {
        &self.deleted
    }

    /// Consumers notified as updated.
    #[must_use]
    pub fn updated(&self) -> &[I::ConsumerId] {
        &self.updated
    }

    /// Position of the originating event, when the parser provided one.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&C> {
        self.checkpoint.as_ref()
    }

    /// Subscriptions whose answer changed because a relationship moved.
    ///
    /// Empty for every event on a table no membership subquery reads through,
    /// which is every event until one is registered. Carries no rows: see
    /// [`TermNarrowing`].
    #[must_use]
    pub fn narrowings(&self) -> &[TermNarrowing<B>] {
        &self.narrowings
    }

    /// The subscriptions whose predicate could not be evaluated for this
    /// row, and why. Empty for every event whose predicates all answered.
    #[must_use]
    pub fn evaluation_failures(&self) -> &[EvaluationFailure<I>] {
        &self.evaluation_failures
    }

    /// Attach the evaluation failures one event produced.
    #[must_use]
    pub(crate) fn with_evaluation_failures(mut self, failures: Vec<EvaluationFailure<I>>) -> Self {
        self.evaluation_failures = failures;
        self
    }

    /// Attach the narrowings a membership change produced.
    #[must_use]
    pub(crate) fn with_narrowings(mut self, narrowings: Vec<TermNarrowing<B>>) -> Self {
        self.narrowings = narrowings;
        self
    }

    /// Decompose into `(inserted, deleted, updated)`. The checkpoint is
    /// dropped. Use [`checkpoint`](Self::checkpoint) first if needed.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(self) -> (Vec<I::ConsumerId>, Vec<I::ConsumerId>, Vec<I::ConsumerId>) {
        (self.inserted, self.deleted, self.updated)
    }
}

impl<I: IdTypes, C: Checkpoint, B: Backend> core::fmt::Debug for ConsumerNotifications<I, C, B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConsumerNotifications")
            .field("inserted", &self.inserted)
            .field("deleted", &self.deleted)
            .field("updated", &self.updated)
            .field("checkpoint", &self.checkpoint)
            .field("narrowings", &self.narrowings)
            // `I::ConsumerId` carries no `Debug`, so the causes are what
            // this can show; the ids are read through the accessor.
            .field(
                "evaluation_failures",
                &self
                    .evaluation_failures
                    .iter()
                    .map(|failure| failure.refusal)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Identity of one grouped SQL result row.
pub struct GroupIdentity<B: Backend = crate::backend::Postgres> {
    /// Opaque canonical identity used by maintenance, reads, and downstream storage.
    pub key: Vec<u8>,
    /// Typed `GROUP BY` values in statement order.
    pub values: Vec<Value<B>>,
}

impl<B: Backend> Clone for GroupIdentity<B> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            values: self.values.clone(),
        }
    }
}

impl<B: Backend> core::fmt::Debug for GroupIdentity<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GroupIdentity")
            .field("key", &self.key)
            .field("values", &self.values)
            .finish()
    }
}

impl<B: Backend> PartialEq for GroupIdentity<B> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.values == other.values
    }
}

/// One aggregate subscription's grouped SQL result changed.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateValueUpdate<I: IdTypes, B: Backend = crate::backend::Postgres> {
    /// The registration whose value moved.
    pub subscription: SubscriptionId,
    /// The consumer that registration belongs to.
    pub consumer: I::ConsumerId,
    /// Group identity, or `None` for an ungrouped aggregate.
    pub group: Option<GroupIdentity<B>>,
    /// Write or remove the grouped SQL result row.
    pub change: AggregateValueChange<B>,
}

/// Value produced by either aggregate maintenance algorithm.
#[derive(Clone, Debug, PartialEq)]
pub enum AggregateResultValue<B: Backend> {
    /// Numeric value maintained by additive folding.
    Folded(AggValue),
    /// Ordered value maintained by grouped extreme reads.
    Scalar(Value<B>),
}

/// Operation applied to one grouped aggregate result row.
#[derive(Clone, Debug, PartialEq)]
pub enum AggregateValueChange<B: Backend = crate::backend::Postgres> {
    /// Write this aggregate value.
    Set(AggregateResultValue<B>),
    /// Remove the group because it has no source rows left.
    Remove,
}

impl<I: IdTypes, B: Backend> AggregateValueUpdate<I, B> {
    /// Value carried by `Set`, or `None` for `Remove`.
    #[must_use]
    pub const fn result_value(&self) -> Option<&AggregateResultValue<B>> {
        match &self.change {
            AggregateValueChange::Set(value) => Some(value),
            AggregateValueChange::Remove => None,
        }
    }

    /// Additive value carried by `Set`, or `None` for another result kind.
    #[must_use]
    pub const fn folded_value(&self) -> Option<AggValue> {
        match &self.change {
            AggregateValueChange::Set(AggregateResultValue::Folded(value)) => Some(*value),
            AggregateValueChange::Set(AggregateResultValue::Scalar(_))
            | AggregateValueChange::Remove => None,
        }
    }
}

/// Why the registry stopped maintaining one subscription in process.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MaintenanceStopReason {
    /// A new group would exceed the configured per-subscription limit.
    GroupLimit {
        /// Configured maximum live groups.
        limit: usize,
    },
    /// An UPDATE omitted columns from its old row that the aggregate reads.
    MissingOldRow {
        /// Source table whose event was incomplete.
        table_id: TableId,
    },
    /// A runtime value fell outside the selected canonical key domain.
    GroupKeyUnencodable {
        /// Source table carrying the value.
        table_id: TableId,
    },
    /// A keyed row read received a CDC change with no readable primary key.
    KeyedChangeWithoutKey {
        /// Source table whose event lacked the key.
        table_id: TableId,
    },
}

/// One subscription changed maintenance tier without changing identity.
#[derive(Clone, Debug, PartialEq)]
pub struct MaintenanceTransition<B: Backend = crate::backend::Postgres> {
    /// Subscription that changed tier.
    pub subscription_id: SubscriptionId,
    /// Previous tier.
    pub from: TierKind,
    /// Replacement tier, including the bound query downstream Rust code executes.
    pub to: Tier<B>,
    /// Why the previous tier could not continue.
    pub reason: MaintenanceStopReason,
}

/// Aggregate processing result from either an event or seed installation.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateMaintenanceOutput<
    I: IdTypes,
    B: Backend = crate::backend::Postgres,
    C: Checkpoint = NoCheckpoint,
> {
    /// Aggregate result rows written or removed.
    pub updates: Vec<AggregateValueUpdate<I, B>>,
    /// Database reads required after a tier transition.
    pub triggers: Vec<crate::reexec::ReExecutionTrigger<I, C, B>>,
    /// Tier changes caused by this operation.
    pub transitions: Vec<MaintenanceTransition<B>>,
    /// Subscriptions whose filter the engine refuses to evaluate for this
    /// event, with the cause.
    ///
    /// No trigger and no tier change accompanies one: a database read would
    /// raise the same error, so there is nothing for it to answer. The
    /// subscription's fold is left exactly as it was, since the row was
    /// never judged, and the next event is evaluated normally.
    pub evaluation_failures: Vec<(
        crate::SubscriptionId,
        crate::compiler::vm::arithmetic::ArithmeticFailure,
    )>,
}

impl<I: IdTypes, B: Backend, C: Checkpoint> AggregateMaintenanceOutput<I, B, C> {
    pub(crate) const fn empty() -> Self {
        Self {
            updates: Vec::new(),
            triggers: Vec::new(),
            transitions: Vec::new(),
            evaluation_failures: Vec::new(),
        }
    }
}

impl<I: IdTypes, B: Backend, C: Checkpoint> core::ops::Deref
    for AggregateMaintenanceOutput<I, B, C>
{
    type Target = [AggregateValueUpdate<I, B>];

    fn deref(&self) -> &Self::Target {
        &self.updates
    }
}

impl<I: IdTypes, B: Backend, C: Checkpoint> core::ops::DerefMut
    for AggregateMaintenanceOutput<I, B, C>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.updates
    }
}

impl<I: IdTypes, B: Backend, C: Checkpoint> IntoIterator for AggregateMaintenanceOutput<I, B, C> {
    type Item = AggregateValueUpdate<I, B>;
    type IntoIter = alloc::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.updates.into_iter()
    }
}

/// Combined output of [`dispatch`](crate::SubscriptionEngine::dispatch).
///
/// Row matches, in-process aggregate changes, scalar changes and required
/// database reads share this one registry output. A caller that ignores
/// `triggers` leaves those subscriptions without a current result.
pub struct DispatchOutput<
    I: IdTypes,
    C: Checkpoint = NoCheckpoint,
    B: Backend = crate::backend::Postgres,
> {
    notifications: ConsumerNotifications<I, C, B>,
    aggregate_updates: Vec<AggregateValueUpdate<I, B>>,
    scalar_updates: Vec<crate::reexec::ScalarUpdate<I, B, C>>,
    triggers: Vec<crate::reexec::ReExecutionTrigger<I, C, B>>,
    transitions: Vec<MaintenanceTransition<B>>,
}

impl<I: IdTypes, C: Checkpoint, B: Backend> DispatchOutput<I, C, B> {
    pub(crate) const fn from_parts(
        notifications: ConsumerNotifications<I, C, B>,
        aggregate_updates: Vec<AggregateValueUpdate<I, B>>,
        scalar_updates: Vec<crate::reexec::ScalarUpdate<I, B, C>>,
        triggers: Vec<crate::reexec::ReExecutionTrigger<I, C, B>>,
        transitions: Vec<MaintenanceTransition<B>>,
    ) -> Self {
        Self {
            notifications,
            aggregate_updates,
            scalar_updates,
            triggers,
            transitions,
        }
    }

    /// Per-consumer, view-relative row notifications.
    #[must_use]
    pub const fn notifications(&self) -> &ConsumerNotifications<I, C, B> {
        &self.notifications
    }

    /// In-process aggregate values that moved on this event.
    ///
    /// An aggregate missing its starting rows appears in [`Self::triggers`]
    /// instead.
    #[must_use]
    pub fn aggregate_updates(&self) -> &[AggregateValueUpdate<I, B>] {
        &self.aggregate_updates
    }

    /// Scalar values a re-read subscription updated from this event alone.
    #[must_use]
    pub fn scalar_updates(&self) -> &[crate::reexec::ScalarUpdate<I, B, C>] {
        &self.scalar_updates
    }

    /// Database reads required by this event.
    #[must_use]
    pub fn triggers(&self) -> &[crate::reexec::ReExecutionTrigger<I, C, B>] {
        &self.triggers
    }

    /// Subscriptions that changed maintenance tier during this event.
    #[must_use]
    pub fn transitions(&self) -> &[MaintenanceTransition<B>] {
        &self.transitions
    }

    /// Every consumer whose client-visible data moved, sorted and deduplicated.
    #[must_use]
    pub fn notified(&self) -> Vec<I::ConsumerId> {
        let mut ids: Vec<I::ConsumerId> = self
            .notifications
            .inserted()
            .iter()
            .chain(self.notifications.updated())
            .chain(self.notifications.deleted())
            .copied()
            .collect();
        ids.extend(self.aggregate_updates.iter().map(|u| u.consumer));
        ids.extend(self.scalar_updates.iter().map(|u| u.consumer_id));
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// `true` when no client-visible data moved and no database read is due.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.aggregate_updates.is_empty()
            && self.scalar_updates.is_empty()
            && self.triggers.is_empty()
            && self.transitions.is_empty()
            && self.notifications.inserted().is_empty()
            && self.notifications.updated().is_empty()
            && self.notifications.deleted().is_empty()
    }
}

/// Iterator over `inserted` and `updated` consumers: those who should see
/// the current row state.
pub struct ConsumerNotificationsIter<I: IdTypes> {
    inserted: alloc::vec::IntoIter<I::ConsumerId>,
    updated: alloc::vec::IntoIter<I::ConsumerId>,
}

impl<I: IdTypes> Iterator for ConsumerNotificationsIter<I> {
    type Item = I::ConsumerId;

    fn next(&mut self) -> Option<Self::Item> {
        self.inserted.next().or_else(|| self.updated.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.inserted.len() + self.updated.len();
        (remaining, Some(remaining))
    }
}

impl<I: IdTypes, C: Checkpoint, B: Backend> IntoIterator for ConsumerNotifications<I, C, B> {
    type Item = I::ConsumerId;
    type IntoIter = ConsumerNotificationsIter<I>;

    fn into_iter(self) -> Self::IntoIter {
        ConsumerNotificationsIter {
            inserted: self.inserted.into_iter(),
            updated: self.updated.into_iter(),
        }
    }
}

/// Aggregate dispatch: reports the values of aggregate subscriptions that moved.
///
/// Separate from [`SubscriptionDispatch`] because:
/// - UPDATE events require evaluating **both** the old row and the new row.
/// - Returns values, not consumer bitmaps.
/// - Aggregate predicates are **never** included in `consumers()` results.
///
/// # Caller contract
///
/// The engine holds the running value. What the caller still owes it:
/// 1. **Starting numbers.** Run
///    [`Served::aggregate_bootstrap`](crate::Served::aggregate_bootstrap)
///    and hand the decoded row to
///    [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall)
///    together with the stream position the read was taken at. Take that
///    position **before** the read's snapshot opens. Until the numbers land the
///    subscription reports nothing.
/// 2. **Old UPDATE images.** Aggregate UPDATE deltas need both the old and the
///    new row. A source that omits `before`/`old` images gets
///    [`DispatchError::AggregateUpdateRequiresOldRow`](crate::DispatchError::AggregateUpdateRequiresOldRow).
/// 3. **A word after a permission change.** An RLS or ACL change produces no
///    WAL event, so the engine cannot see it. Call
///    [`reset_aggregate_value`](crate::SubscriptionEngine::reset_aggregate_value) and seed
///    again. `TRUNCATE` needs nothing: the table is empty afterwards, so the
///    engine empties the value itself and reports it.
pub trait AggregateDispatch<I: IdTypes, E: CdcEvent>: Send {
    /// Report every aggregate subscription whose value moved on this event.
    ///
    /// One entry per subscription, naming its consumer alongside. A
    /// subscription whose value did not move, and one that has not been seeded,
    /// are both absent.
    fn aggregate_updates(
        &mut self,
        event: &E,
    ) -> Result<AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>, crate::DispatchError>;
}

/// Background merge operations
#[cfg(feature = "std")]
pub trait DurableShardMerge: Send {
    /// Start background merge of shard files for a table.
    fn merge_shards_background(
        &mut self,
        table_id: TableId,
        shard_paths: &[PathBuf],
    ) -> Result<MergeJobId, crate::MergeError>;

    /// Check whether merge has completed and atomically swap if ready.
    fn try_complete_merge(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, crate::MergeError>;
}
