//! Core type definitions for subql

use crate::checkpoint::{Checkpoint, NoCheckpoint};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt::Debug;
use core::hash::Hash;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(feature = "std")]
use std::path::PathBuf;

// ============================================================================
// Generic ID Types
// ============================================================================

/// Marker trait for ID types used in the subscription engine.
///
/// Any type satisfying these bounds can be used as a consumer, session, or
/// subscription identifier.
pub trait Id:
    Copy + Ord + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

/// Blanket implementation: every type meeting the bounds is automatically an `Id`.
impl<T: Copy + Ord + Hash + Debug + Send + Sync + Serialize + DeserializeOwned + 'static> Id for T {}

/// Engine-assigned subscription identifier (always `u64`).
pub type SubscriptionId = u64;

/// Associated types that pin the consumer-facing ID representations.
///
/// `SubscriptionId` is always `u64` and auto-assigned by the engine.
pub trait IdTypes: 'static {
    /// Consumer identifier (globally unique)
    type ConsumerId: Id;
    /// Session identifier (per-connection)
    type SessionId: Id;
}

/// Default ID configuration using `u64` for consumer and session identifiers.
#[derive(Debug)]
pub struct DefaultIds;

impl IdTypes for DefaultIds {
    type ConsumerId = u64;
    type SessionId = u64;
}

/// Lifetime scope of a subscription.
///
/// Wire-compatible with `Option<SessionId>` under postcard's positional
/// encoding: `Durable` = variant 0 = `None`, `Session(id)` = variant 1 =
/// `Some(id)`.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
pub enum SubscriptionScope<I: IdTypes> {
    /// Persists until explicitly unregistered.
    Durable,
    /// Bound to a session. Auto-removed when the session ends.
    Session(I::SessionId),
}

// Manual impls avoid derived bounds that would require `I: Copy/Clone/Debug/...`.
// Only `I::SessionId` (already `Copy + Debug + Hash + ...` via `Id`) is needed.
impl<I: IdTypes> Copy for SubscriptionScope<I> {}
impl<I: IdTypes> Clone for SubscriptionScope<I> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<I: IdTypes> core::fmt::Debug for SubscriptionScope<I> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Durable => write!(f, "Durable"),
            Self::Session(id) => f.debug_tuple("Session").field(id).finish(),
        }
    }
}
impl<I: IdTypes> PartialEq for SubscriptionScope<I> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Durable, Self::Durable) => true,
            (Self::Session(a), Self::Session(b)) => a == b,
            _ => false,
        }
    }
}
impl<I: IdTypes> Eq for SubscriptionScope<I> {}
impl<I: IdTypes> core::hash::Hash for SubscriptionScope<I> {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        if let Self::Session(id) = self {
            id.hash(state);
        }
    }
}

// ============================================================================
// Domain ID Types (non-generic, internal)
// ============================================================================

/// Table identifier (from schema catalog)
pub type TableId = u32;

/// Column identifier (ordinal within table, 0-indexed)
pub type ColumnId = u16;

/// Shard identifier (for persistence)
pub type ShardId = u64;

/// Merge job identifier (for background operations)
pub type MergeJobId = u64;

// ============================================================================
// Event Types
// ============================================================================

/// CDC event kind
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// Row insertion
    Insert,
    /// Row update (old -> new)
    Update,
    /// Row deletion
    Delete,
    /// Table truncate: all rows in the table are removed.
    ///
    /// **Fanout semantics**: TRUNCATE does not carry a row image, so `consumers()`
    /// skips predicate VM evaluation and notifies row subscriptions for the
    /// table. Aggregate subscriptions are handled separately by
    /// `aggregate_deltas()`, which returns `TruncateRequiresReset`.
    Truncate,
}

// ============================================================================
// Subscription Types
// ============================================================================

/// Subscription request provided by caller
///
/// `Eq` is not derived: `binds` holds [`crate::backend::Value<B>`]s,
/// which may carry `f64` and are only `PartialEq`.
#[derive(Clone, Debug, PartialEq)]
pub struct SubscriptionRequest<I: IdTypes, B: crate::backend::Backend = crate::backend::Postgres> {
    /// Consumer who owns this subscription.
    pub(crate) consumer_id: I::ConsumerId,
    /// Lifetime scope: durable or session-bound.
    pub(crate) scope: SubscriptionScope<I>,
    /// SQL SELECT statement with WHERE clause.
    pub(crate) sql: String,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch).
    pub(crate) updated_at_unix_ms: u64,
    /// Resolved bind values for `$N` / `?` placeholders in `sql`, in
    /// placeholder order. Empty for plain literal SQL (the default);
    /// populated by the typed diesel API, which renders parameterised
    /// SQL plus these values.
    ///
    /// Binds are typed to the observed [`crate::backend::Backend`]. When
    /// binds are empty the `B` parameter is inferred from context; the
    /// default `B = Postgres` covers the common Postgres-backed use.
    pub(crate) binds: alloc::vec::Vec<crate::backend::Value<B>>,
    /// Which subscriber this subscription filters for.
    ///
    /// Required by a filter naming a membership term: a membership subquery, or
    /// a comparison of a column to the caller, both written against
    /// `current_setting('app.user_id')` in SQL and resolved per connection by
    /// Postgres. SubQL has neither a connection nor a session, so the
    /// subscription states it. A membership subquery matches changed membership
    /// rows against the identity to move who the filter admits, and a caller
    /// comparison admits exactly the rows naming it. Trusting it is safe because
    /// visibility gates every delivery afterwards: a subscription claiming
    /// another identity receives nothing it is not permitted to see, it only
    /// fails to receive its own rows.
    pub(crate) subscriber: Option<crate::backend::Value<B>>,
    /// The values this subscriber currently matches, grouped by the column each
    /// membership subquery compares.
    ///
    /// Read them from the membership table, which
    /// [`SubscriptionEngine::describe_terms`](crate::SubscriptionEngine::describe_terms)
    /// hands over as a runnable query. Not from the snapshot rows: a value whose
    /// rows do not exist yet is in no snapshot, and no later membership event
    /// supplies it, because the membership never changed, so every row inserted
    /// under that value afterwards is silently never delivered.
    ///
    /// Grouped by column name because a filter may name several subqueries and
    /// the compared column is the one name both sides share. The list is taken on
    /// trust, and only a membership row naming the same pair ever moves it, so it
    /// bounds this subscriber's own results and nobody else's. Neither direction
    /// self-corrects: a missing value admits nobody until a membership row adds
    /// it, and a value the subscriber does not match keeps admitting rows to it,
    /// with no row to withdraw a value whose membership never existed.
    pub(crate) term_values: alloc::vec::Vec<(String, alloc::vec::Vec<crate::backend::Value<B>>)>,
}

impl<I: IdTypes, B: crate::backend::Backend> SubscriptionRequest<I, B> {
    /// Create a new subscription request with default scope (`Durable`)
    /// and timestamp (`0`).
    pub fn new(consumer_id: I::ConsumerId, sql: impl Into<String>) -> Self {
        Self {
            consumer_id,
            scope: SubscriptionScope::Durable,
            sql: sql.into(),
            updated_at_unix_ms: 0,
            binds: alloc::vec::Vec::new(),
            subscriber: None,
            term_values: alloc::vec::Vec::new(),
        }
    }

    /// Set the subscription scope (default: [`SubscriptionScope::Durable`]).
    #[must_use]
    pub const fn scope(mut self, scope: SubscriptionScope<I>) -> Self {
        self.scope = scope;
        self
    }

    /// Set the conflict-resolution timestamp in milliseconds since Unix
    /// epoch (default: `0`).
    #[must_use]
    pub const fn updated_at_unix_ms(mut self, ts: u64) -> Self {
        self.updated_at_unix_ms = ts;
        self
    }

    /// Attach resolved bind values for `$N` / `?` placeholders in the
    /// SQL, in placeholder order (default: none). Used by the typed
    /// diesel API.
    #[must_use]
    pub fn binds(mut self, binds: alloc::vec::Vec<crate::backend::Value<B>>) -> Self {
        self.binds = binds;
        self
    }

    /// State which subscriber this subscription filters for (default: none).
    ///
    /// A filter naming a membership term is refused without it: for a
    /// membership subquery the identity is what a changed membership row is
    /// matched against, and for a caller comparison it is the one value the
    /// comparison admits. Build it at
    /// [`TermDescription::subject_kind`](crate::term::TermDescription::subject_kind):
    /// the lookup keys a string and a UUID under different variants, so an
    /// identity of another kind matches no membership row and admits nobody in
    /// silence. A caller comparison checks instead of trusting, since the
    /// compared column's kind is in the catalog: a mismatched identity is
    /// refused at registration.
    #[must_use]
    pub fn subscriber(mut self, subscriber: crate::backend::Value<B>) -> Self {
        self.subscriber = Some(subscriber);
        self
    }

    /// State the values this subscriber currently matches for `column`, the
    /// column one of the filter's membership subqueries compares (default: none
    /// for every column).
    ///
    /// Both the column and the read that yields the values come from
    /// [`SubscriptionEngine::describe_terms`](crate::SubscriptionEngine::describe_terms),
    /// which reads the membership table. Deriving them from the snapshot rows
    /// instead loses every value whose rows do not exist yet, permanently.
    ///
    /// A value the subscriber does not match is trusted just as readily, and
    /// keeps admitting rows to it until a membership row naming that pair is
    /// deleted. A membership that never existed has none to delete.
    ///
    /// Called once per compared column. Calling it twice for one column adds to
    /// what that column already carries rather than replacing it.
    #[must_use]
    pub fn term_values(
        mut self,
        column: impl Into<String>,
        values: alloc::vec::Vec<crate::backend::Value<B>>,
    ) -> Self {
        self.term_values.push((column.into(), values));
        self
    }
}

/// Eviction policy applied when the registry cap is hit.
///
/// Default is `Reject`: registrations past the cap fail with
/// [`crate::RegisterError::RegistryFull`]. Other variants make room for
/// the incoming subscription by removing an existing one and surface the
/// evicted [`SubscriptionId`]s in [`RegisterResult::evicted`].
///
/// For closure-based policies that pick the victim per-call (e.g. fair
/// share across tenants, custom heuristics that read live activity
/// counters), use
/// [`SubscriptionEngine::with_custom_eviction`](crate::SubscriptionEngine::with_custom_eviction)
/// instead of this enum.
///
/// ```
///
/// use sql_traits::structs::ParserDB;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use subql::backend::{Postgres, Value};
/// use subql::testing::TestEvent;
/// use subql::{DefaultIds, EvictionPolicy, SubscriptionEngine, SubscriptionRequest};
///
/// let database = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
/// )?;
///
/// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
///     SubscriptionEngine::new(database, PostgreSqlDialect {})
///         .with_max_subscriptions(1, EvictionPolicy::EvictOldest);
///
/// let first = engine.register(SubscriptionRequest::new(
///     1u64,
///     "SELECT * FROM orders WHERE amount > 1",
/// ))?;
/// let second = engine.register(SubscriptionRequest::new(
///     2u64,
///     "SELECT * FROM orders WHERE amount > 2",
/// ))?;
///
/// // The cap was already at 1 when `second` was registered, so the
/// // oldest subscription got evicted to make room.
/// assert_eq!(second.evicted, vec![first.subscription_id]);
/// assert_eq!(engine.subscription_count(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EvictionPolicy {
    /// Hard cap: reject the registration when the registry is full.
    ///
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     DefaultIds, EvictionPolicy, RegisterError, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(1, EvictionPolicy::Reject);
    ///
    /// engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    ///
    /// match engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount > 2",
    /// )) {
    ///     Err(RegisterError::RegistryFull { cap }) => assert_eq!(cap, 1),
    ///     other => panic!("expected RegistryFull, got {other:?}"),
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[default]
    Reject,
    /// Evict the oldest subscription (lowest `SubscriptionId`) and proceed.
    /// Reports the evicted id via [`RegisterResult::evicted`].
    ///
    /// See the [enum-level doctest](EvictionPolicy) for a runnable
    /// example.
    EvictOldest,
    /// Evict the subscription with the oldest `last_dispatch_at`.
    /// Subscriptions never matched by an event are considered "least
    /// active" (their `last_dispatch_at` is `None`, treated as -infinity)
    /// and are evicted first. Ties resolve by oldest `SubscriptionId`.
    ///
    /// Requires activity stamping: configuring this policy makes
    /// [`SubscriptionEngine::consumers`](crate::SubscriptionEngine::consumers)
    /// record the subscriptions that contributed to each match.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, ClockHandle, DefaultIds, EvictionPolicy, ManualClock,
    ///     SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    /// let clock = Arc::new(ManualClock::new(0));
    /// let handle: ClockHandle = clock.clone();
    ///
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(2, EvictionPolicy::EvictLeastActive)
    ///         .with_activity_clock(handle);
    ///
    /// let make_event = |id: i64, amount: i64| -> TestEvent<Postgres> {
    ///     TestEvent::<Postgres>::insert(
    ///         orders_id,
    ///         vec![Value::Int(id), Value::Int(amount)],
    ///     )
    ///     .with_pk_columns([0u16])
    /// };
    ///
    /// // Register and stamp `first` early. Predicate matches amount = 5.
    /// let first = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount = 5",
    /// ))?;
    /// clock.advance(Duration::from_micros(10));
    /// engine.consumers(&make_event(1, 5))?;
    ///
    /// // Register `second` and stamp it later. Predicate matches amount = 10
    /// // exclusively, so `first` is not re-stamped.
    /// let _second = engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount = 10",
    /// ))?;
    /// clock.advance(Duration::from_micros(20));
    /// engine.consumers(&make_event(2, 10))?;
    ///
    /// // Third registration hits the cap. `first` was stamped at t=10
    /// // and `_second` at t=30. `first` is the least active and is
    /// // evicted to make room.
    /// let third = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount = 999",
    /// ))?;
    /// assert_eq!(third.evicted, vec![first.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictLeastActive,
    /// Evict the subscription with the lowest `dispatch_count` (the
    /// "coldest" subscription, with the fewest matches over its
    /// lifetime). Ties resolve by oldest `SubscriptionId`.
    ///
    /// Requires activity stamping (see [`EvictLeastActive`](Self::EvictLeastActive)).
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, DefaultIds, EvictionPolicy,
    ///     SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    ///
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(2, EvictionPolicy::EvictColdest);
    ///
    /// let _hot = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount = 5",
    /// ))?;
    /// let cold = engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount = 9999",
    /// ))?;
    /// for i in 0..3 {
    ///     let event = TestEvent::<Postgres>::insert(
    ///         orders_id,
    ///         vec![Value::Int(i), Value::Int(5)],
    ///     )
    ///     .with_pk_columns([0u16]);
    ///     engine.consumers(&event)?;
    /// }
    /// let third = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount = 7",
    /// ))?;
    /// assert_eq!(third.evicted, vec![cold.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictColdest,
    /// Prefer evicting session-scoped subscriptions before durable ones.
    /// Among session subscriptions, the oldest `SubscriptionId` is
    /// evicted first. If no session subscriptions exist, falls back to
    /// [`EvictOldest`](Self::EvictOldest).
    ///
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     DefaultIds, EvictionPolicy, SubscriptionEngine, SubscriptionRequest,
    ///     SubscriptionScope,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(2, EvictionPolicy::EvictBySession);
    ///
    /// let _durable = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    /// let session = engine.register(
    ///     SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE amount > 2")
    ///         .scope(SubscriptionScope::Session(7_777)),
    /// )?;
    ///
    /// let third = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount > 3",
    /// ))?;
    /// assert_eq!(third.evicted, vec![session.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictBySession,
    /// Evict from the consumer currently holding the most live
    /// subscriptions ("the biggest hog"). Among that consumer's
    /// subscriptions, the oldest `SubscriptionId` is evicted first.
    /// Ties between consumers (same count) resolve by lowest consumer id.
    ///
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     DefaultIds, EvictionPolicy, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {})
    ///         .with_max_subscriptions(3, EvictionPolicy::EvictByConsumer);
    ///
    /// let hog_a = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    /// let _hog_b = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 2",
    /// ))?;
    /// let _other = engine.register(SubscriptionRequest::new(
    ///     2u64,
    ///     "SELECT * FROM orders WHERE amount > 3",
    /// ))?;
    /// let fourth = engine.register(SubscriptionRequest::new(
    ///     3u64,
    ///     "SELECT * FROM orders WHERE amount > 4",
    /// ))?;
    /// // Consumer 1 was the biggest hog (2 subs). The oldest of its
    /// // subscriptions is evicted.
    /// assert_eq!(fourth.evicted, vec![hog_a.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    EvictByConsumer,
}

/// Snapshot of a single subscription's metadata.
///
/// Surfaced through [`SubscriptionsView`] to the closure passed to
/// [`SubscriptionEngine::with_custom_eviction`](crate::SubscriptionEngine::with_custom_eviction).
/// Activity-aware policies use `last_dispatch_at` / `dispatch_count` to
/// pick a victim. Scope/consumer-aware policies use `scope` / `consumer_id`.
/// Subscriptions never matched by an event have `last_dispatch_at = None`
/// and `dispatch_count = 0`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SubscriptionMetadata<I: IdTypes> {
    /// Engine-assigned subscription identifier (monotonic. Lower is older).
    pub subscription_id: SubscriptionId,
    /// Consumer who owns this subscription.
    pub consumer_id: I::ConsumerId,
    /// Lifetime scope: durable or session-bound.
    pub scope: SubscriptionScope<I>,
    /// Microsecond timestamp from the engine's activity clock at the most
    /// recent dispatch that matched this subscription, or `None` if it
    /// has never matched. Compare with other entries within the same
    /// view, never with absolute wall-clock time.
    pub last_dispatch_at: Option<u64>,
    /// Number of dispatch events that matched this subscription since
    /// registration. Saturates at `u64::MAX`.
    pub dispatch_count: u64,
}

impl<I: IdTypes> SubscriptionMetadata<I> {
    /// Construct a [`SubscriptionMetadata`] with the canonical fields.
    /// The struct is `#[non_exhaustive]`. Use this constructor (rather
    /// than a struct literal) so callers compile across future field
    /// additions.
    #[must_use]
    pub const fn new(
        subscription_id: SubscriptionId,
        consumer_id: I::ConsumerId,
        scope: SubscriptionScope<I>,
        last_dispatch_at: Option<u64>,
        dispatch_count: u64,
    ) -> Self {
        Self {
            subscription_id,
            consumer_id,
            scope,
            last_dispatch_at,
            dispatch_count,
        }
    }
}

/// Borrowed read-only view of every live subscription, used by custom
/// eviction closures.
///
/// Cheap to construct (borrows the engine's internal HashMap entries).
/// The closure must not retain the borrow past the call. Lifetime is
/// bounded to the single call into the custom eviction function.
///
/// ```
/// use subql::{DefaultIds, SubscriptionMetadata, SubscriptionScope, SubscriptionsView};
///
/// let entries: Vec<SubscriptionMetadata<DefaultIds>> = vec![
///     SubscriptionMetadata::new(1, 10, SubscriptionScope::Durable, Some(100), 5),
///     SubscriptionMetadata::new(2, 10, SubscriptionScope::Durable, None, 0),
/// ];
/// let view = SubscriptionsView::<DefaultIds>::new(&entries);
/// let coldest = view.iter().min_by_key(|m| m.dispatch_count).unwrap();
/// assert_eq!(coldest.subscription_id, 2);
/// ```
pub struct SubscriptionsView<'a, I: IdTypes> {
    entries: &'a [SubscriptionMetadata<I>],
}

impl<'a, I: IdTypes> SubscriptionsView<'a, I> {
    /// Build a view from a slice of metadata. Public so eviction closures
    /// can be unit-tested with hand-rolled views.
    #[must_use]
    pub const fn new(entries: &'a [SubscriptionMetadata<I>]) -> Self {
        Self { entries }
    }

    /// Iterator over the metadata entries.
    pub fn iter(&self) -> core::slice::Iter<'_, SubscriptionMetadata<I>> {
        self.entries.iter()
    }

    /// Number of live subscriptions in the view.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no live subscriptions are present.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Borrow the underlying metadata slice.
    #[must_use]
    pub const fn as_slice(&self) -> &[SubscriptionMetadata<I>] {
        self.entries
    }
}

impl<'a, I: IdTypes> IntoIterator for &'a SubscriptionsView<'_, I> {
    type Item = &'a SubscriptionMetadata<I>;
    type IntoIter = core::slice::Iter<'a, SubscriptionMetadata<I>>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// Runnable component-seed query for an aggregate registration, bundling
/// the SQL with its per-column decode kinds so the two cannot drift apart.
///
/// [`sql`](Self::sql) projects the seed components aliased positionally
/// (`c0`, `c1`, ...) in the order [`AggAccumulator::seed_from_row`]
/// consumes them, and [`kinds`](Self::kinds) gives the decode kind per
/// column `ci`. `COUNT` components are [`ScalarKind::Int`](crate::backend::ScalarKind::Int);
/// `SUM` and `SUM(x*x)` components are
/// [`ScalarKind::Float`](crate::backend::ScalarKind::Float), decoded as
/// double to match the `f64` accumulator (since `SUM` promotes to
/// `bigint`/`numeric`/`DECIMAL` depending on the backend).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateBootstrap {
    /// Runnable single-row seed query with positionally-aliased columns.
    pub sql: String,
    /// Per-column decode kinds, in column order.
    pub kinds: Vec<crate::backend::ScalarKind>,
}

/// Result of successful subscription registration
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterResult {
    /// Engine-assigned subscription identifier
    pub subscription_id: SubscriptionId,
    /// Table this subscription applies to
    pub table_id: TableId,
    /// Normalized/canonicalized SQL
    pub normalized_sql: String,
    /// Hash of the predicate (for deduplication)
    pub predicate_hash: u128,
    /// True if a new predicate was created, false if reused existing
    pub created_new_predicate: bool,
    /// Projection kind for this subscription
    pub projection: crate::compiler::sql_shape::QueryProjection,
    /// Subscriptions evicted to make room for this registration.
    ///
    /// Empty under the default policy ([`EvictionPolicy::Reject`]) and
    /// when the registry is below cap. Populated when an eviction policy
    /// freed space. The caller may use this to notify the affected
    /// clients (e.g. send an "evicted" signal over their transport).
    pub evicted: Vec<SubscriptionId>,
    /// Component-seed query for the aggregate, for bootstrap or reset.
    /// `None` for a row subscription. Run [`AggregateBootstrap::sql`]
    /// (typing each column by [`AggregateBootstrap::kinds`]) and pass the
    /// decoded row to [`AggAccumulator::seed_from_row`].
    pub aggregate_bootstrap: Option<AggregateBootstrap>,
}

impl RegisterResult {
    /// The aggregate spec when the registered query is an aggregate, else
    /// `None` for a row-set (`SELECT *`) query.
    #[must_use]
    pub const fn aggregate_spec(&self) -> Option<&crate::AggSpec> {
        match &self.projection {
            crate::QueryProjection::Aggregate(spec) => Some(spec),
            crate::QueryProjection::Rows => None,
        }
    }
}

/// Durability policy for registration writes when storage is enabled.
#[cfg(feature = "std")]
#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// Registration succeeds even if snapshot/rotation persistence fails.
    BestEffort,
    /// Registration fails (and is rolled back) if persistence fails.
    Required,
}

/// Report from pruning session subscriptions
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnregisterReport {
    /// Number of subscription bindings removed
    pub removed_bindings: usize,
    /// Number of predicates removed (refcount reached 0)
    pub removed_predicates: usize,
    /// Number of consumer dictionary entries removed
    pub removed_consumers: usize,
}

/// Report from background merge operation
#[cfg(feature = "std")]
#[derive(Clone, Debug, PartialEq)]
pub struct MergeReport {
    /// Number of input shards merged
    pub input_shards: usize,
    /// Number of predicates in output shard
    pub output_predicates: usize,
    /// Number of bindings in output shard
    pub output_bindings: usize,
    /// Deduplication ratio (1.0 = no dedup, 2.0 = 50% reduction)
    pub dedup_ratio: f32,
    /// Time spent building merged shard (milliseconds)
    pub build_ms: u64,
}

// ============================================================================
// Trait Definitions
// ============================================================================

/// Subscription registration operations.
///
/// Parameterised on the observed [`crate::backend::Backend`] so
/// `register` accepts a typed [`SubscriptionRequest`] whose bind values
/// are `Value<B>`.
pub trait SubscriptionRegistration<I: IdTypes, B: crate::backend::Backend>: Send {
    /// Register a new subscription.
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and
    /// binds consumer. Returns error if SQL is unparseable or unsupported.
    fn register(
        &mut self,
        spec: SubscriptionRequest<I, B>,
    ) -> Result<RegisterResult, crate::RegisterError>;

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
pub trait SubscriptionDispatch<I: IdTypes, E: crate::backend::CdcEvent>: Send {
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
pub trait AsyncSubscriptionDispatch<I: IdTypes, E: crate::backend::CdcEvent>: Send {
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

/// Typed signed delta from an aggregate subscription.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AggDelta {
    /// COUNT(*) / COUNT(column) delta: always +/-1 per matching (non-NULL) row.
    Count(i64),
    /// SUM(column) delta: signed change in the column sum.
    Sum(f64),
    /// AVG(column) delta: both components needed to update a running average.
    ///
    /// Caller maintains `running_sum` and `running_count` separately:
    /// ```text
    /// running_sum   += sum_delta
    /// running_count += count_delta
    /// avg            = running_sum / running_count  (when running_count > 0)
    /// ```
    Avg { sum_delta: f64, count_delta: i64 },
    /// VAR_POP / VAR_SAMP / STDDEV_POP / STDDEV_SAMP delta. Carries the
    /// three components needed to update a running variance or standard
    /// deviation.
    ///
    /// The kernel is the same for all four functions. The consumer
    /// applies the appropriate derivation to the running tuple:
    /// ```text
    /// running_sum    += sum_delta
    /// running_sum_sq += sum_sq_delta
    /// running_count  += count_delta
    ///
    /// // population variance:
    /// var_pop  = running_sum_sq / N - (running_sum / N)^2
    /// // sample variance (N >= 2):
    /// var_samp = (running_sum_sq - (running_sum)^2 / N) / (N - 1)
    /// stddev_*  = sqrt(var_*)
    /// ```
    /// where `N = running_count`.
    Stats {
        sum_delta: f64,
        sum_sq_delta: f64,
        count_delta: i64,
    },
}

#[derive(Clone, Copy, Debug)]
enum AggKind {
    Count,
    Sum,
    Avg,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
}

/// Current value of an [`AggAccumulator`].
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

/// Running value of an aggregate subscription, folded from [`AggDelta`]s and
/// seeded from the registration's [`AggSpec`](crate::AggSpec).
///
/// ```
/// use subql::{AggAccumulator, AggDelta, AggSpec, AggValue};
///
/// let mut count = AggAccumulator::from_spec(&AggSpec::CountStar);
/// count.apply(&AggDelta::Count(3));
/// count.apply(&AggDelta::Count(-1));
/// assert_eq!(count.value(), AggValue::Count(2));
/// assert_eq!(count.to_string(), "2");
///
/// let mut avg = AggAccumulator::from_spec(&AggSpec::Avg { column: 1 });
/// avg.apply(&AggDelta::Avg { sum_delta: 10.0, count_delta: 4 });
/// assert_eq!(avg.value(), AggValue::Real(Some(2.5)));
/// ```
#[derive(Clone, Debug)]
pub struct AggAccumulator {
    kind: AggKind,
    count: i64,
    sum: f64,
    sum_sq: f64,
}

impl AggAccumulator {
    /// Seed an accumulator for the aggregate described by `spec`.
    #[must_use]
    pub const fn from_spec(spec: &crate::AggSpec) -> Self {
        use crate::AggSpec as S;
        let kind = match spec {
            S::CountStar | S::CountColumn { .. } => AggKind::Count,
            S::Sum { .. } => AggKind::Sum,
            S::Avg { .. } => AggKind::Avg,
            S::VarPop { .. } => AggKind::VarPop,
            S::VarSamp { .. } => AggKind::VarSamp,
            S::StddevPop { .. } => AggKind::StddevPop,
            S::StddevSamp { .. } => AggKind::StddevSamp,
        };
        Self {
            kind,
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    /// Seed an accumulator from a bootstrap component row produced by
    /// [`AggregateBootstrap`](crate::AggregateBootstrap).
    ///
    /// Consumes the components in the documented column order: `[c]` for
    /// COUNT, `[s]` for SUM, `[s, c]` for AVG, and `[s, sq, c]` for the
    /// variance and stddev family. A zero-row result (COUNT `0`, NULL
    /// sum components) seeds the empty-aggregate state, matching the
    /// "set went empty" semantics of the re-execution family.
    #[must_use]
    pub fn seed_from_row<B: crate::backend::Backend>(
        spec: &crate::AggSpec,
        row: &[crate::backend::Value<B>],
    ) -> Self {
        use crate::AggSpec as S;
        let mut acc = Self::from_spec(spec);
        match spec {
            S::CountStar | S::CountColumn { .. } => {
                acc.count = row.first().and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::Sum { .. } => {
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
            }
            S::Avg { .. } => {
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.count = row.get(1).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
            S::VarPop { .. } | S::VarSamp { .. } | S::StddevPop { .. } | S::StddevSamp { .. } => {
                acc.sum = row.first().and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.sum_sq = row.get(1).and_then(|v| Self::seed_f64(v)).unwrap_or(0.0);
                acc.count = row.get(2).and_then(|v| Self::seed_i64(v)).unwrap_or(0);
            }
        }
        acc
    }

    /// Decode a numeric component cell to `f64`. NULL/Missing/non-numeric
    /// cells return `None` (the caller defaults them to `0.0`, safe because
    /// a zero-count accumulator reports the empty state regardless of sum).
    #[allow(clippy::cast_precision_loss)]
    fn seed_f64<B: crate::backend::Backend>(v: &crate::backend::Value<B>) -> Option<f64> {
        use crate::backend::Value;
        use core::any::Any;
        match v {
            // i64 -> f64 loses precision above 2^53; the seed path accepts
            // the same bounded loss the delta path (`probe_column_for_agg`)
            // already does for realistic aggregate magnitudes.
            Value::Int(i) => (i as &dyn Any).downcast_ref::<i64>().map(|x| *x as f64),
            Value::Float(f) => (f as &dyn Any)
                .downcast_ref::<f64>()
                .copied()
                .filter(|x| x.is_finite()),
            // NUMERIC/DECIMAL sums (e.g. Postgres `SUM(int_col)`) arrive as
            // BigDecimal; parse through its decimal string to avoid a
            // num-traits import.
            Value::Decimal(d) => (d as &dyn Any)
                .downcast_ref::<bigdecimal::BigDecimal>()
                .and_then(|x| x.to_string().parse::<f64>().ok()),
            _ => None,
        }
    }

    /// Decode the COUNT component cell to `i64`. COUNT is exact and integer
    /// on every backend, so only `Value::Int` is accepted.
    fn seed_i64<B: crate::backend::Backend>(v: &crate::backend::Value<B>) -> Option<i64> {
        use crate::backend::Value;
        use core::any::Any;
        match v {
            Value::Int(i) => (i as &dyn Any).downcast_ref::<i64>().copied(),
            _ => None,
        }
    }

    /// Fold one delta into the running value.
    pub fn apply(&mut self, delta: &AggDelta) {
        match delta {
            AggDelta::Count(d) => self.count += d,
            AggDelta::Sum(d) => self.sum += d,
            AggDelta::Avg {
                sum_delta,
                count_delta,
            } => {
                self.sum += sum_delta;
                self.count += count_delta;
            }
            AggDelta::Stats {
                sum_delta,
                sum_sq_delta,
                count_delta,
            } => {
                self.sum += sum_delta;
                self.sum_sq += sum_sq_delta;
                self.count += count_delta;
            }
        }
    }

    /// Rows currently contributing to the aggregate.
    #[must_use]
    pub const fn count(&self) -> i64 {
        self.count
    }

    /// Current aggregate value.
    #[must_use]
    pub fn value(&self) -> AggValue {
        match self.kind {
            AggKind::Count => AggValue::Count(self.count),
            AggKind::Sum => AggValue::Sum(self.sum),
            AggKind::Avg => AggValue::Real(self.mean()),
            AggKind::VarPop => AggValue::Real(self.var_pop()),
            AggKind::VarSamp => AggValue::Real(self.var_samp()),
            AggKind::StddevPop => AggValue::Real(self.var_pop().map(f64::sqrt)),
            AggKind::StddevSamp => AggValue::Real(self.var_samp().map(f64::sqrt)),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn mean(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    fn var_pop(&self) -> Option<f64> {
        (self.count > 0).then(|| {
            let n = self.count as f64;
            self.sum_sq / n - (self.sum / n).powi(2)
        })
    }

    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
    fn var_samp(&self) -> Option<f64> {
        (self.count >= 2).then(|| {
            let n = self.count as f64;
            (self.sum_sq - self.sum.powi(2) / n) / (n - 1.0)
        })
    }
}

impl core::fmt::Display for AggAccumulator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.value() {
            AggValue::Count(c) => write!(f, "{c}"),
            AggValue::Sum(s) => write!(f, "{s}"),
            AggValue::Real(Some(v)) => write!(f, "{v}"),
            AggValue::Real(None) => write!(f, "-"),
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
pub struct TermNarrowing<B: crate::backend::Backend> {
    /// The subscription whose answer changed.
    pub subscription: SubscriptionId,
    /// The table that subscription reads.
    pub table: TableId,
    /// The column its membership subquery compares.
    pub column: ColumnId,
    /// The value that column now matches, or stopped matching.
    pub value: crate::backend::Value<B>,
    /// Whether the value entered the subscription's set or left it.
    pub entered: bool,
}

impl<B: crate::backend::Backend> Clone for TermNarrowing<B> {
    fn clone(&self) -> Self {
        Self {
            subscription: self.subscription,
            table: self.table,
            column: self.column,
            value: self.value.clone(),
            entered: self.entered,
        }
    }
}

impl<B: crate::backend::Backend> core::fmt::Debug for TermNarrowing<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TermNarrowing")
            .field("subscription", &self.subscription)
            .field("table", &self.table)
            .field("column", &self.column)
            .field("value", &self.value)
            .field("entered", &self.entered)
            .finish()
    }
}

impl<B: crate::backend::Backend> PartialEq for TermNarrowing<B> {
    fn eq(&self, other: &Self) -> bool {
        self.subscription == other.subscription
            && self.table == other.table
            && self.column == other.column
            && self.value == other.value
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
    B: crate::backend::Backend = crate::backend::Postgres,
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
}

impl<I: IdTypes, C: Checkpoint, B: crate::backend::Backend> ConsumerNotifications<I, C, B> {
    /// Create empty notifications with no checkpoint.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            inserted: Vec::new(),
            deleted: Vec::new(),
            updated: Vec::new(),
            checkpoint: None,
            narrowings: Vec::new(),
        }
    }

    /// Construct notifications from explicit buckets.
    #[must_use]
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

impl<I: IdTypes, C: Checkpoint, B: crate::backend::Backend> core::fmt::Debug
    for ConsumerNotifications<I, C, B>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConsumerNotifications")
            .field("inserted", &self.inserted)
            .field("deleted", &self.deleted)
            .field("updated", &self.updated)
            .field("checkpoint", &self.checkpoint)
            .field("narrowings", &self.narrowings)
            .finish()
    }
}

/// Combined output of [`dispatch`](crate::SubscriptionEngine::dispatch): row-set
/// notifications plus aggregate deltas for one event.
pub struct DispatchOutput<
    I: IdTypes,
    C: Checkpoint = NoCheckpoint,
    B: crate::backend::Backend = crate::backend::Postgres,
> {
    notifications: ConsumerNotifications<I, C, B>,
    aggregate_deltas: Vec<(I::ConsumerId, AggDelta)>,
}

impl<I: IdTypes, C: Checkpoint, B: crate::backend::Backend> DispatchOutput<I, C, B> {
    pub(crate) const fn from_parts(
        notifications: ConsumerNotifications<I, C, B>,
        aggregate_deltas: Vec<(I::ConsumerId, AggDelta)>,
    ) -> Self {
        Self {
            notifications,
            aggregate_deltas,
        }
    }

    /// Per-consumer, view-relative row notifications (insert/delete/update).
    #[must_use]
    pub const fn notifications(&self) -> &ConsumerNotifications<I, C, B> {
        &self.notifications
    }

    /// Signed aggregate deltas, one entry per `(consumer, aggregate kind)`.
    #[must_use]
    pub fn aggregate_deltas(&self) -> &[(I::ConsumerId, AggDelta)] {
        &self.aggregate_deltas
    }

    /// Every consumer affected by this event, from either path, sorted and
    /// deduplicated.
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
        ids.extend(self.aggregate_deltas.iter().map(|(cid, _)| *cid));
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// `true` when no consumer was affected by this event.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.aggregate_deltas.is_empty()
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

impl<I: IdTypes, C: Checkpoint, B: crate::backend::Backend> IntoIterator
    for ConsumerNotifications<I, C, B>
{
    type Item = I::ConsumerId;
    type IntoIter = ConsumerNotificationsIter<I>;

    fn into_iter(self) -> Self::IntoIter {
        ConsumerNotificationsIter {
            inserted: self.inserted.into_iter(),
            updated: self.updated.into_iter(),
        }
    }
}

/// Aggregate dispatch: delivers typed signed deltas for aggregate subscriptions.
///
/// Separate from [`SubscriptionDispatch`] because:
/// - UPDATE events require evaluating **both** the old row and the new row.
/// - Returns signed deltas, not consumer bitmaps.
/// - Aggregate predicates are **never** included in `consumers()` results.
///
/// # Caller contract
///
/// The engine handles only WAL-driven deltas. Callers must:
/// 1. **Bootstrap**: query the DB for the initial aggregate **before** subscribing.
/// 2. **Accumulate**: `running_value += delta` on each call.
/// 3. **Reset on policy change**: RLS/ACL changes produce no WAL events.
///    Re-query the DB and replace the stored value.
/// 4. **Reset on TRUNCATE**: engine returns `Err(TruncateRequiresReset)`.
///    Re-query and replace the stored value.
pub trait AggregateDispatch<I: IdTypes, E: crate::backend::CdcEvent>: Send {
    /// Compute typed signed deltas for all matching aggregate
    /// subscriptions.
    ///
    /// Returns `Vec<(ConsumerId, AggDelta)>` where each entry is the
    /// signed change for that consumer's subscription. Zero-net entries
    /// are omitted. The same consumer may appear multiple times (once
    /// per aggregate kind).
    fn aggregate_deltas(
        &mut self,
        event: &E,
    ) -> Result<Vec<(I::ConsumerId, AggDelta)>, crate::DispatchError>;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn subscription_scope_wire_compatible_with_option() {
        let none_bytes = crate::persistence::codec::serialize(&Option::<u64>::None).unwrap();
        let durable_bytes =
            crate::persistence::codec::serialize(&SubscriptionScope::<DefaultIds>::Durable)
                .unwrap();
        assert_eq!(none_bytes, durable_bytes);

        let some_bytes = crate::persistence::codec::serialize(&Some(42u64)).unwrap();
        let session_bytes =
            crate::persistence::codec::serialize(&SubscriptionScope::<DefaultIds>::Session(42))
                .unwrap();
        assert_eq!(some_bytes, session_bytes);
    }

    #[test]
    fn subscriptions_view_iterates_metadata_entries() {
        let entries: alloc::vec::Vec<SubscriptionMetadata<DefaultIds>> = alloc::vec![
            SubscriptionMetadata::new(1, 10, SubscriptionScope::Durable, None, 0),
            SubscriptionMetadata::new(2, 20, SubscriptionScope::Session(99), Some(1_000_000), 4),
        ];

        let view = SubscriptionsView::<DefaultIds>::new(&entries);
        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());

        let collected: alloc::vec::Vec<u64> = view.iter().map(|m| m.subscription_id).collect();
        assert_eq!(collected, alloc::vec![1, 2]);

        let coldest = view
            .iter()
            .min_by_key(|m| m.dispatch_count)
            .map(|m| m.subscription_id);
        assert_eq!(coldest, Some(1));

        let least_active = view
            .iter()
            .min_by_key(|m| m.last_dispatch_at.unwrap_or(0))
            .map(|m| m.subscription_id);
        assert_eq!(least_active, Some(1));

        let session_count = view
            .iter()
            .filter(|m| matches!(m.scope, SubscriptionScope::Session(_)))
            .count();
        assert_eq!(session_count, 1);
    }
}
