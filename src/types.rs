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
    /// `aggregate_updates()`, which empties each of that table's held values
    /// and reports the ones that moved.
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
    /// The value rows this subscriber currently matches, grouped by the columns
    /// each membership subquery compares.
    ///
    /// Read them from the membership table, which
    /// [`SubscriptionEngine::describe_terms`](crate::SubscriptionEngine::describe_terms)
    /// hands over as a runnable query. Not from the snapshot rows: a value whose
    /// rows do not exist yet is in no snapshot, and no later membership event
    /// supplies it, because the membership never changed, so every row inserted
    /// under that value afterwards is silently never delivered.
    ///
    /// Grouped by column names because a filter may name several subqueries and
    /// the compared columns are the names both sides share. The list is taken on
    /// trust, and only a membership row naming the same values ever moves it, so
    /// it bounds this subscriber's own results and nobody else's. Neither
    /// direction self-corrects: a missing row admits nobody until a membership
    /// row adds it, and a row the subscriber does not match keeps admitting rows
    /// to it, with no row to withdraw values whose membership never existed.
    pub(crate) term_values: StatedTermValues<B>,
}

/// Value rows stated per membership term: the compared column names, then the
/// rows, each following the names' order.
pub type StatedTermValues<B> = alloc::vec::Vec<(
    alloc::vec::Vec<String>,
    alloc::vec::Vec<alloc::vec::Vec<crate::backend::Value<B>>>,
)>;

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
    /// [`MembershipTermDescription::subject_kind`](crate::term::MembershipTermDescription::subject_kind):
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

    /// State the value rows this subscriber currently matches for `columns`,
    /// the columns one of the filter's membership subqueries compares (default:
    /// none for every term).
    ///
    /// One name and one value per row entry for the ordinary single-column
    /// term, several for a term whose `EXISTS` pairs span a composite key. Each
    /// row follows the order of `columns`, which may differ from the filter's
    /// own order: the engine matches by name. With a static diesel schema,
    /// prefer `term_values_for` (feature `diesel-typed`), which takes the
    /// columns themselves and cannot misspell a name.
    ///
    /// Both the columns and the read that yields the rows come from
    /// [`SubscriptionEngine::describe_terms`](crate::SubscriptionEngine::describe_terms),
    /// which reads the membership table. Deriving them from the snapshot rows
    /// instead loses every value whose rows do not exist yet, permanently.
    ///
    /// A row the subscriber does not match is trusted just as readily, and
    /// keeps admitting rows to it until a membership row naming those values is
    /// deleted. A membership that never existed has none to delete.
    ///
    /// Called once per term. Calling it twice for one term's columns adds to
    /// what those columns already carry rather than replacing it.
    #[must_use]
    pub fn term_values<C: Into<String>>(
        mut self,
        columns: alloc::vec::Vec<C>,
        rows: alloc::vec::Vec<alloc::vec::Vec<crate::backend::Value<B>>>,
    ) -> Self {
        self.term_values
            .push((columns.into_iter().map(Into::into).collect(), rows));
        self
    }
    /// Declare that downstream Rust code executes database reads under this
    /// consumer's database identity.
    ///
    /// Required for row re-reads on a table with row-level security. This is a
    /// compile-time request type, not a runtime flag.
    #[must_use]
    pub const fn database_reads_per_consumer(self) -> PerConsumerDatabaseReads<I, B> {
        PerConsumerDatabaseReads(self)
    }
}

/// Input accepted by [`SubscriptionEngine::register`](crate::SubscriptionEngine::register).
///
/// The associated constant states whether downstream Rust code executes every
/// database read under the individual consumer's database identity.
pub trait RegistrationRequest<I: IdTypes, B: crate::backend::Backend> {
    /// `true` only when database reads are isolated per consumer.
    const DATABASE_READS_PER_CONSUMER: bool;

    /// Recover the SQL request consumed by registration.
    fn into_request(self) -> SubscriptionRequest<I, B>;

    /// Lifetime scope before registration consumes the request.
    fn scope(&self) -> SubscriptionScope<I>;
}

impl<I: IdTypes, B: crate::backend::Backend> RegistrationRequest<I, B>
    for SubscriptionRequest<I, B>
{
    const DATABASE_READS_PER_CONSUMER: bool = false;

    fn into_request(self) -> Self {
        self
    }

    fn scope(&self) -> SubscriptionScope<I> {
        self.scope
    }
}

/// A subscription request whose database reads are isolated per consumer.
#[derive(Clone, Debug, PartialEq)]
pub struct PerConsumerDatabaseReads<
    I: IdTypes,
    B: crate::backend::Backend = crate::backend::Postgres,
>(SubscriptionRequest<I, B>);

impl<I: IdTypes, B: crate::backend::Backend> RegistrationRequest<I, B>
    for PerConsumerDatabaseReads<I, B>
{
    const DATABASE_READS_PER_CONSUMER: bool = true;

    fn into_request(self) -> SubscriptionRequest<I, B> {
        self.0
    }

    fn scope(&self) -> SubscriptionScope<I> {
        self.0.scope
    }
}

/// Eviction policy applied when the registry cap is hit.
///
/// Default is `Reject`: registrations past the cap fail with
/// [`crate::RegisterError::RegistryFull`]. Other variants make room for
/// the incoming subscription by removing an existing one and surface the
/// evicted [`SubscriptionId`]s in [`Registered::evicted`].
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
    /// Reports the evicted id via [`Registered::evicted`].
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
/// [`sql`](Self::sql) projects [`group_columns`](Self::group_columns) group
/// values first, then the seed components aliased positionally (`c0`, `c1`,
/// ...) in the order
/// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall)
/// consumes them, and
/// [`kinds`](Self::kinds) gives the decode kind for every column in that same
/// order. `COUNT` components are
/// [`ScalarKind::Int`](crate::backend::ScalarKind::Int); `SUM` and `SUM(x*x)`
/// components are [`ScalarKind::Float`](crate::backend::ScalarKind::Float),
/// decoded as double to match the `f64` accumulator (since `SUM` promotes to
/// `bigint`/`numeric`/`DECIMAL` depending on the backend).
///
/// An ungrouped seed returns exactly one row. A grouped one returns a row per
/// group, and returns none at all over an empty table, where the ungrouped
/// spelling still returns its empty-aggregate row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateBootstrap {
    /// Runnable seed query with positionally-aliased component columns.
    pub sql: String,
    /// Per-column decode kinds, in column order, group columns included.
    pub kinds: Vec<crate::backend::BuiltinKind>,
    /// How many leading columns of each row are group values.
    ///
    /// Zero for an ungrouped aggregate, in which case every column is a
    /// component and the query returns one row.
    pub group_columns: usize,
}

/// What a registration produced: the identity, and the tier that maintains it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registered {
    /// Engine-assigned identity, from the one counter every maintained answer
    /// draws from.
    pub subscription_id: SubscriptionId,
    /// How the answer is maintained, carrying whatever that tier needs.
    pub tier: Tier,
    /// Subscriptions evicted to make room for this registration.
    ///
    /// Empty under the default policy ([`EvictionPolicy::Reject`]) and when the
    /// registry is below cap. Populated when an eviction policy freed space, so
    /// a caller can tell the affected clients.
    pub evicted: Vec<SubscriptionId>,
    /// Why this answer needs a database read, in the compiler's own words.
    ///
    /// `None` for [`Tier::InProcess`], which needs no read. For every other
    /// tier this is what the in-process evaluator said it could not do, which
    /// is the only thing telling a caller why its query costs a read per
    /// change rather than being answered from memory.
    pub not_served_because: Option<String>,
}

impl Registered {
    /// The registry's own record, for an answer it maintains in process.
    ///
    /// `None` for every tier that needs a read, which have no predicate and no
    /// normalized statement to report.
    #[must_use]
    pub const fn served(&self) -> Option<&Served> {
        match &self.tier {
            Tier::InProcess(served) => Some(served),
            Tier::Scalar { .. }
            | Tier::GroupedScalar { .. }
            | Tier::KeyedRows { .. }
            | Tier::WholeRows { .. } => None,
        }
    }

    /// The aggregate this registration maintains, when it maintains one.
    ///
    /// `None` for a row subscription and for every tier that needs a read.
    #[must_use]
    pub const fn aggregate_spec(&self) -> Option<&crate::AggSpec> {
        match &self.tier {
            Tier::InProcess(served) => served.aggregate_spec(),
            Tier::Scalar { .. }
            | Tier::GroupedScalar { .. }
            | Tier::KeyedRows { .. }
            | Tier::WholeRows { .. } => None,
        }
    }
}

/// How a registered answer is maintained, and what its tier hands back.
///
/// Every variant other than [`Self::InProcess`] needs a database read the
/// engine cannot do itself, so it hands the caller the statement to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Maintained from the change stream alone, with no read at all.
    ///
    /// Rows or a fold: [`Served::projection`] says which, and a fold carries
    /// the query that seeds it.
    InProcess(Served),
    /// A scalar extreme, re-read when the extreme itself leaves the answer.
    Scalar {
        /// SQL to run for the initial value and for any later trigger.
        sql: String,
        /// Decode hint for the scalar result.
        column_kind: crate::backend::BuiltinKind,
    },
    /// Grouped extrema seeded together and re-read one displaced group at a time.
    GroupedScalar {
        /// SQL and decode kinds for the initial group map.
        bootstrap: AggregateBootstrap,
    },
    /// One table's rows, re-read for the keys that changed.
    KeyedRows {
        /// The statement, unchanged: this tier promises exactly the rows the
        /// caller asked for.
        sql: String,
        /// The one table this tier reads, and the only one that triggers it.
        table_id: TableId,
    },
    /// The whole answer, re-read when any table it reads changes.
    WholeRows {
        /// The statement, unchanged.
        sql: String,
        /// Tables whose changes trigger a read.
        tables: Vec<TableId>,
    },
}

impl Tier {
    /// This tier's bare name, for reporting and comparison.
    #[must_use]
    pub const fn kind(&self) -> TierKind {
        match self {
            Self::InProcess(_) => TierKind::InProcess,
            Self::Scalar { .. } => TierKind::Scalar,
            Self::GroupedScalar { .. } => TierKind::GroupedScalar,
            Self::KeyedRows { .. } => TierKind::KeyedRows,
            Self::WholeRows { .. } => TierKind::WholeRows,
        }
    }
}

/// Which read serves an answer, without the payload that goes with it.
///
/// [`Tier`] carries a statement and the tables behind it, which a stored record
/// does not need twice: it keeps the caller's SQL and this, and the statement is
/// planned again on load.
///
/// One of three spellings of the same ladder: [`Tier`] is the registration
/// answer with payloads, [`TierKind`] is the bare name a transition reports,
/// and this is the persisted subset (in-process answers are never stored as
/// reads). A new tier must appear in all three, which is what the
/// [`From<ReadTier>`](TierKind#impl-From<ReadTier>-for-TierKind) conversion
/// and [`Tier::kind`] exist to keep honest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReadTier {
    /// A scalar extreme, re-read when the extreme leaves.
    Scalar,
    /// Grouped extrema with reads scoped to one displaced group.
    GroupedScalar,
    /// One table's rows, re-read for the keys that changed.
    KeyedRows,
    /// The whole answer, re-read when any table it reads changes.
    WholeRows,
}

/// What the registry recorded for an answer it maintains itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Served {
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
    /// Component-seed query for the aggregate, for bootstrap or reset.
    /// `None` for a row subscription. Run [`AggregateBootstrap::sql`]
    /// (typing each column by [`AggregateBootstrap::kinds`]) and pass the
    /// decoded row to
    /// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall).
    pub aggregate_bootstrap: Option<AggregateBootstrap>,
}

impl Served {
    /// The aggregate spec when the registered query is an aggregate, grouped
    /// or not, else `None` for a row-set (`SELECT *`) query.
    ///
    /// A grouped registration answers with its aggregate here and its group
    /// columns on the projection, because every caller of this wants to know
    /// which function is maintained rather than how many values of it there
    /// are.
    #[must_use]
    pub const fn aggregate_spec(&self) -> Option<&crate::AggSpec> {
        match &self.projection {
            crate::QueryProjection::Aggregate(spec)
            | crate::QueryProjection::GroupedAggregate { agg: spec, .. } => Some(spec),
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
    /// Number of re-read answers removed, which have no predicate of their own
    pub removed_reads: usize,
}
/// Write a database result back into the registry.
///
/// `T` is a concrete Rust struct. Each supported struct has its own
/// implementation, output and error type.
pub trait Install<T> {
    /// Value produced after the registry accepts `T`.
    type Output;
    /// Why the registry could not accept `T`.
    type Error;

    /// Apply `input` to `subscription_id`.
    fn install(
        &mut self,
        subscription_id: SubscriptionId,
        input: T,
    ) -> Result<Self::Output, Self::Error>;
}

/// Result of a scalar SQL read.
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarInstall<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Scalar returned by SQL.
    pub value: crate::backend::Value<B>,
    /// Position of the change that caused the read, when known.
    pub checkpoint: Option<C>,
}

/// One page in a complete row result.
#[derive(Clone, Debug, PartialEq)]
pub struct InstalledPage<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Column names reported by the database.
    pub columns: Vec<String>,
    /// Rows in column order.
    pub rows: Vec<Vec<crate::backend::Value<B>>>,
    /// Whether another page follows.
    pub more: bool,
    /// Position the read is tied to, when known.
    pub checkpoint: Option<C>,
}

/// Complete row result from one database read.
#[derive(Clone, Debug, PartialEq)]
pub struct WholeRowsInstall<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Read generation used to group pages from one replacement.
    pub generation: u64,
    /// Pages in delivery order.
    pub pages: Vec<InstalledPage<B, C>>,
}

/// One keyed row change returned by a database read.
#[derive(Clone, Debug, PartialEq)]
pub struct InstalledRowDelta<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Primary-key values in key-column order.
    pub key: Vec<crate::backend::Value<B>>,
    /// Current row, or `None` when the row left the result.
    pub row: Option<Vec<crate::backend::Value<B>>>,
    /// Position of the change that caused the read, when known.
    pub checkpoint: Option<C>,
}

/// Keyed row changes from one database read.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyedRowsInstall<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Column names for `row`, in projection order.
    pub columns: Vec<String>,
    /// Changes in delivery order.
    pub deltas: Vec<InstalledRowDelta<B, C>>,
}

/// Starting rows for an in-process aggregate.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateSeedInstall<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Decoded rows returned by the aggregate seed SQL.
    ///
    /// An ungrouped aggregate supplies exactly one row. A grouped aggregate
    /// supplies one row per group.
    pub rows: Vec<Vec<crate::backend::Value<B>>>,
    /// Stream position taken before the read snapshot opened.
    pub read_at: Option<C>,
}

/// Starting rows for a grouped extreme.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupedScalarSeedInstall<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Group values, extreme and source-row count in bootstrap order.
    pub rows: Vec<Vec<crate::backend::Value<B>>>,
    /// Stream position taken before the read snapshot opened.
    pub read_at: Option<C>,
}

/// Result of re-reading one displaced extreme.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupedScalarInstall<B: crate::backend::Backend, C: Checkpoint = NoCheckpoint> {
    /// Opaque key of the group the SQL constrained.
    pub group: Vec<u8>,
    /// Current extreme and source-row count returned by the scoped SQL.
    pub row: Vec<crate::backend::Value<B>>,
    /// Position of the change that caused the read.
    pub checkpoint: Option<C>,
}

/// A result struct does not match the registered tier or identity.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InstallError {
    /// No re-read subscription has this identity.
    #[error("subscription {0} is not a re-read subscription")]
    UnknownSubscription(SubscriptionId),
    /// The concrete input struct is for another tier.
    #[error("subscription {subscription} does not accept {input}")]
    WrongTier {
        /// Subscription that was addressed.
        subscription: SubscriptionId,
        /// Concrete input struct the caller supplied.
        input: &'static str,
    },
}

/// Why aggregate starting rows could not be reconciled with changes during the
/// database read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AggregateInstallError {
    /// No aggregate subscription with this id, or it was unregistered.
    #[error("subscription {0} is not a live aggregate subscription")]
    UnknownAggregate(SubscriptionId),
    /// The subscription already holds a total. Reset it first.
    #[error("subscription {0} is already seeded")]
    AlreadySeeded(SubscriptionId),
    /// Grouped starting rows or pending changes exceed the configured limit.
    #[error("subscription {subscription} exceeds its {limit}-group limit")]
    GroupLimit {
        /// Grouped aggregate subscription.
        subscription: SubscriptionId,
        /// Configured maximum groups.
        limit: usize,
    },
    /// Changes and the read cannot be ordered because a stream position is
    /// missing on one side.
    #[error("subscription {0} cannot order changes against the database read")]
    PositionUnknown(SubscriptionId),
    /// More changes arrived during the read than the registry kept.
    #[error("subscription {subscription} held more than {cap} changes during the read")]
    TooManyChangesDuringRead {
        /// Aggregate subscription.
        subscription: SubscriptionId,
        /// Configured ceiling.
        cap: usize,
    },
    /// This phase accepts exactly one ungrouped aggregate row.
    #[error("ungrouped aggregate {subscription} needs one seed row, got {rows}")]
    RowCount {
        /// Aggregate subscription.
        subscription: SubscriptionId,
        /// Rows supplied.
        rows: usize,
    },
    /// A grouped seed or replacement row has the wrong number of cells.
    #[error(
        "subscription {subscription} received a grouped row of {got} cells \
         where {expected} were selected"
    )]
    GroupedRowArity {
        /// Grouped aggregate subscription.
        subscription: SubscriptionId,
        /// Cells the seed or scoped read selects.
        expected: usize,
        /// Cells the row carried.
        got: usize,
    },
    /// A grouped row's group values do not encode into a group key.
    #[error("subscription {0} received group values that do not encode to a key")]
    GroupKeyUnencodable(SubscriptionId),
    /// A grouped row's source-row count is missing, non-integer, or not
    /// positive.
    #[error("subscription {0} received a grouped row without a positive source-row count")]
    GroupedRowCount(SubscriptionId),
    /// A scoped read delivered a group the registry was not waiting for.
    #[error("subscription {0} received a scoped read it never asked for")]
    UnexpectedGroupRead(SubscriptionId),
    /// Aggregate state could not be rebuilt as a complete row read.
    ///
    /// Raised from the install paths when seed reconciliation cannot plan a
    /// tier change. The dispatch path's twin lives on
    /// [`DispatchError::TierTransition`](crate::DispatchError::TierTransition).
    #[error("subscription {subscription} could not change tier: {message}")]
    TierTransition {
        /// Aggregate subscription.
        subscription: SubscriptionId,
        /// Planner or registry invariant that failed.
        message: String,
    },
    /// Seed SQL returned the same encoded group twice.
    #[error("subscription {0} received the same aggregate group twice")]
    DuplicateGroup(SubscriptionId),
}

/// What reading the re-read answers' file restored, and what it could not.
///
/// Every restored answer comes back not knowing its value, so each one needs a
/// read before it can report anything: this is that list as well as the record
/// of what was dropped.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RestoredReads {
    /// Answers that came back, each needing a read to fill it in.
    pub restored: Vec<RestoredRead>,
    /// Answers that could not come back, with the reason.
    pub dropped: Vec<DroppedRead>,
}

/// One answer that came back from the file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoredRead {
    /// The identity it had before the restart, which it keeps.
    pub subscription_id: SubscriptionId,
    /// The tier planning its SQL chose this time.
    pub tier: Tier,
    /// Whether that tier differs from the one it was saved with.
    pub tier_changed: bool,
}

/// One answer the file held that could not be restored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DroppedRead {
    /// The identity it had. A caller still holding it has nothing behind it.
    pub subscription_id: SubscriptionId,
    /// Its statement, so a caller can register it again if it wants to.
    pub sql: String,
    /// Why it could not come back.
    pub reason: DropReason,
}

/// Why a saved answer could not be restored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// A table it reads has a different shape than when it was saved.
    TableChanged {
        /// The table whose shape moved.
        table_id: TableId,
    },
    /// A table it reads is not in the catalog at all any more.
    TableGone {
        /// The table the statement named.
        table_id: TableId,
    },
    /// Its statement no longer plans: no tier can serve it.
    Unplannable {
        /// What planning said.
        message: String,
    },
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
    ) -> Result<Registered, crate::RegisterError>;

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
pub struct TermNarrowing<B: crate::backend::Backend> {
    /// The subscription whose answer changed.
    pub subscription: SubscriptionId,
    /// The table that subscription reads.
    pub table: TableId,
    /// The columns its membership subquery compares, in the filter's order.
    pub columns: alloc::vec::Vec<ColumnId>,
    /// The value row those columns now match, or stopped matching, pairwise
    /// with [`columns`](Self::columns).
    pub values: alloc::vec::Vec<crate::backend::Value<B>>,
    /// Whether the values entered the subscription's set or left it.
    pub entered: bool,
}

impl<B: crate::backend::Backend> Clone for TermNarrowing<B> {
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

impl<B: crate::backend::Backend> core::fmt::Debug for TermNarrowing<B> {
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

impl<B: crate::backend::Backend> PartialEq for TermNarrowing<B> {
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

/// One aggregate subscription's grouped SQL result changed.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateValueUpdate<I: IdTypes, B: crate::backend::Backend = crate::backend::Postgres> {
    /// The registration whose value moved.
    pub subscription: SubscriptionId,
    /// The consumer that registration belongs to.
    pub consumer: I::ConsumerId,
    /// Opaque encoded group key, or `None` for an ungrouped aggregate.
    pub group: Option<Vec<u8>>,
    /// Write or remove the grouped SQL result row.
    pub change: AggregateValueChange<B>,
}

/// Value produced by either aggregate maintenance algorithm.
#[derive(Clone, Debug, PartialEq)]
pub enum AggregateResultValue<B: crate::backend::Backend> {
    /// Numeric value maintained by additive folding.
    Folded(AggValue),
    /// Ordered value maintained by grouped extreme reads.
    Scalar(crate::backend::Value<B>),
}

/// Operation applied to one grouped aggregate result row.
#[derive(Clone, Debug, PartialEq)]
pub enum AggregateValueChange<B: crate::backend::Backend = crate::backend::Postgres> {
    /// Write this aggregate value.
    Set(AggregateResultValue<B>),
    /// Remove the group because it has no source rows left.
    Remove,
}

impl<I: IdTypes, B: crate::backend::Backend> AggregateValueUpdate<I, B> {
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

/// Tier names without the data carried by [`Tier`].
///
/// The bare-name spelling of the ladder [`Tier`] answers registrations with
/// and [`ReadTier`] persists. A new tier must appear in all three.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TierKind {
    /// Maintained from CDC events.
    InProcess,
    /// One scalar database read.
    Scalar,
    /// Grouped extrema with reads scoped to one displaced group.
    GroupedScalar,
    /// Database reads scoped to changed primary keys.
    KeyedRows,
    /// Complete database result replacement.
    WholeRows,
}

impl From<ReadTier> for TierKind {
    fn from(tier: ReadTier) -> Self {
        match tier {
            ReadTier::Scalar => Self::Scalar,
            ReadTier::GroupedScalar => Self::GroupedScalar,
            ReadTier::KeyedRows => Self::KeyedRows,
            ReadTier::WholeRows => Self::WholeRows,
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
    /// A keyed row read received a CDC change with no readable primary key.
    KeyedChangeWithoutKey {
        /// Source table whose event lacked the key.
        table_id: TableId,
    },
}

/// One subscription changed maintenance tier without changing identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceTransition {
    /// Subscription that changed tier.
    pub subscription_id: SubscriptionId,
    /// Previous tier.
    pub from: TierKind,
    /// Replacement tier, including the SQL downstream Rust code executes.
    pub to: Tier,
    /// Why the previous tier could not continue.
    pub reason: MaintenanceStopReason,
}

/// Aggregate processing result from either an event or seed installation.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateMaintenanceOutput<
    I: IdTypes,
    B: crate::backend::Backend = crate::backend::Postgres,
    C: Checkpoint = NoCheckpoint,
> {
    /// Aggregate result rows written or removed.
    pub updates: Vec<AggregateValueUpdate<I, B>>,
    /// Database reads required after a tier transition.
    pub triggers: Vec<crate::reexec::ReExecutionTrigger<I, C>>,
    /// Tier changes caused by this operation.
    pub transitions: Vec<MaintenanceTransition>,
}

impl<I: IdTypes, B: crate::backend::Backend, C: Checkpoint> AggregateMaintenanceOutput<I, B, C> {
    pub(crate) const fn empty() -> Self {
        Self {
            updates: Vec::new(),
            triggers: Vec::new(),
            transitions: Vec::new(),
        }
    }
}

impl<I: IdTypes, B: crate::backend::Backend, C: Checkpoint> core::ops::Deref
    for AggregateMaintenanceOutput<I, B, C>
{
    type Target = [AggregateValueUpdate<I, B>];

    fn deref(&self) -> &Self::Target {
        &self.updates
    }
}

impl<I: IdTypes, B: crate::backend::Backend, C: Checkpoint> core::ops::DerefMut
    for AggregateMaintenanceOutput<I, B, C>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.updates
    }
}

impl<I: IdTypes, B: crate::backend::Backend, C: Checkpoint> IntoIterator
    for AggregateMaintenanceOutput<I, B, C>
{
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
    B: crate::backend::Backend = crate::backend::Postgres,
> {
    notifications: ConsumerNotifications<I, C, B>,
    aggregate_updates: Vec<AggregateValueUpdate<I, B>>,
    scalar_updates: Vec<crate::reexec::ScalarUpdate<I, B, C>>,
    triggers: Vec<crate::reexec::ReExecutionTrigger<I, C>>,
    transitions: Vec<MaintenanceTransition>,
}

impl<I: IdTypes, C: Checkpoint, B: crate::backend::Backend> DispatchOutput<I, C, B> {
    pub(crate) const fn from_parts(
        notifications: ConsumerNotifications<I, C, B>,
        aggregate_updates: Vec<AggregateValueUpdate<I, B>>,
        scalar_updates: Vec<crate::reexec::ScalarUpdate<I, B, C>>,
        triggers: Vec<crate::reexec::ReExecutionTrigger<I, C>>,
        transitions: Vec<MaintenanceTransition>,
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
    pub fn triggers(&self) -> &[crate::reexec::ReExecutionTrigger<I, C>] {
        &self.triggers
    }

    /// Subscriptions that changed maintenance tier during this event.
    #[must_use]
    pub fn transitions(&self) -> &[MaintenanceTransition] {
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
pub trait AggregateDispatch<I: IdTypes, E: crate::backend::CdcEvent>: Send {
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
