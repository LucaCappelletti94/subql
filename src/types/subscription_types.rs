//! Subscription request, registration, and install types.

use super::domain_id_types::{ColumnId, TableId};
use super::generic_id_types::{IdTypes, SubscriptionId, SubscriptionScope};
use crate::backend::{Backend, BuiltinKind, ScalarKindOf, Value};
use crate::checkpoint::{Checkpoint, NoCheckpoint};
use alloc::string::String;
use alloc::vec::Vec;

/// Subscription request provided by caller
///
/// `Eq` is not derived: `binds` holds [`crate::backend::Value<B>`]s,
/// which may carry `f64` and are only `PartialEq`.
#[derive(Clone, Debug, PartialEq)]
pub struct SubscriptionRequest<I: IdTypes, B: Backend = crate::backend::Postgres> {
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
    pub(crate) binds: alloc::vec::Vec<Value<B>>,
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
    pub(crate) subscriber: Option<Value<B>>,
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
    alloc::vec::Vec<alloc::vec::Vec<Value<B>>>,
)>;

impl<I: IdTypes, B: Backend> SubscriptionRequest<I, B> {
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
    pub fn binds(mut self, binds: alloc::vec::Vec<Value<B>>) -> Self {
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
    pub fn subscriber(mut self, subscriber: Value<B>) -> Self {
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
        rows: alloc::vec::Vec<alloc::vec::Vec<Value<B>>>,
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
pub trait RegistrationRequest<I: IdTypes, B: Backend> {
    /// `true` only when database reads are isolated per consumer.
    const DATABASE_READS_PER_CONSUMER: bool;

    /// Recover the SQL request consumed by registration.
    fn into_request(self) -> SubscriptionRequest<I, B>;
}

impl<I: IdTypes, B: Backend> RegistrationRequest<I, B> for SubscriptionRequest<I, B> {
    const DATABASE_READS_PER_CONSUMER: bool = false;

    fn into_request(self) -> Self {
        self
    }
}

/// A subscription request whose database reads are isolated per consumer.
#[derive(Clone, Debug, PartialEq)]
pub struct PerConsumerDatabaseReads<I: IdTypes, B: Backend = crate::backend::Postgres>(
    SubscriptionRequest<I, B>,
);

impl<I: IdTypes, B: Backend> RegistrationRequest<I, B> for PerConsumerDatabaseReads<I, B> {
    const DATABASE_READS_PER_CONSUMER: bool = true;

    fn into_request(self) -> SubscriptionRequest<I, B> {
        self.0
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
/// [`query`](Self::query) projects [`group_columns`](Self::group_columns) group
/// values first, then the seed components aliased positionally (`c0`, `c1`,
/// ...) in the order
/// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall)
/// consumes them, and
/// [`kinds`](Self::kinds) gives the decode kind for every column in that same
/// order. `COUNT` components are
/// [`BuiltinKind::Int`](crate::backend::BuiltinKind::Int); `SUM` and `SUM(x*x)`
/// components are [`BuiltinKind::Float`](crate::backend::BuiltinKind::Float),
/// decoded as double to match the `f64` accumulator (since `SUM` promotes to
/// `bigint`/`numeric`/`DECIMAL` depending on the backend).
///
/// An ungrouped seed returns exactly one row. A grouped one returns a row per
/// group, and returns none at all over an empty table, where the ungrouped
/// spelling still returns its empty-aggregate row.
#[derive(Debug, PartialEq)]
pub struct AggregateBootstrap<B: Backend = crate::backend::Postgres> {
    /// Runnable seed query with positionally-aliased component columns.
    pub query: crate::reexec::BoundQuery<B>,
    /// Per-column decode kinds, in column order, group columns included.
    pub kinds: Vec<BuiltinKind>,
    /// How many leading columns of each row are group values.
    ///
    /// Zero for an ungrouped aggregate, in which case every column is a
    /// component and the query returns one row.
    pub group_columns: usize,
}

impl<B: Backend> Clone for AggregateBootstrap<B> {
    fn clone(&self) -> Self {
        Self {
            query: self.query.clone(),
            kinds: self.kinds.clone(),
            group_columns: self.group_columns,
        }
    }
}

/// Why an answer is maintained by a database read instead of in process.
///
/// Typed so a caller branches rather than parsing prose: "a shared answer
/// over this table would be one viewer's answer served to another" is a
/// deployment fact, while "this aggregate reads a column the fold cannot
/// carry" is a query fix, and the two used to arrive as the same `String`.
///
/// Generic over the backend because a cause names a column's scalar kind, and
/// a custom backend kind is not a [`BuiltinKind`](crate::backend::BuiltinKind).
///
/// [`Display`](core::fmt::Display) renders the sentence a caller logs today,
/// so migrating is a match arm rather than a message change.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NotServed<B: Backend> {
    /// A shared in-process answer over a row-security table would be one
    /// viewer's answer served to another, so the read happens per consumer.
    RowSecurityNeedsPerConsumerRead {
        /// The table whose row security forces the read.
        table: TableId,
    },
    /// The aggregate reads a column the fold cannot carry.
    UnfoldableAggregate {
        /// The aggregated column.
        column: ColumnId,
        /// The column's declared scalar kind.
        kind: ScalarKindOf<B>,
        /// The aggregate function, as the statement spelled it.
        function: String,
    },
    /// The comparison orders a kind whose order this build cannot
    /// reproduce. `jsonb` is the one such kind today: PostgreSQL's order
    /// over it is not the order of the canonical binary form, and its
    /// string arm follows the database collation.
    OrderNotReproducible {
        /// The compared column.
        column: ColumnId,
        /// The column's declared scalar kind.
        kind: ScalarKindOf<B>,
    },
    /// A form the compiler refused with prose rather than a structured
    /// cause.
    UnsupportedSql(String),
}

/// A scalar kind as a refusal names it: a builtin by its own name, a custom
/// kind by the embedder's `Debug`.
struct ScalarKindName<'a, B: Backend>(&'a ScalarKindOf<B>);

impl<B: Backend> core::fmt::Display for ScalarKindName<'_, B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0.as_builtin() {
            Some(builtin) => write!(f, "{builtin:?}"),
            None => write!(f, "{:?}", self.0),
        }
    }
}

impl<B: Backend> core::fmt::Display for NotServed<B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::RowSecurityNeedsPerConsumerRead { .. } => {
                f.write_str("aggregate on RLS table requires database re-execution")
            }
            Self::UnfoldableAggregate {
                column,
                kind,
                function,
            } => write!(
                f,
                "{function} requires a numeric column (Int, Float, or Decimal), \
                 but column {column} has type {kind}",
                kind = ScalarKindName::<B>(kind)
            ),
            Self::OrderNotReproducible { column, kind } => write!(
                f,
                "column {column} has type {kind}, whose ordered comparison \
                 subql cannot reproduce in process",
                kind = ScalarKindName::<B>(kind)
            ),
            Self::UnsupportedSql(message) => f.write_str(message),
        }
    }
}

/// What a registration produced: the identity, and the tier that maintains it.
#[derive(Clone, Debug, PartialEq)]
pub struct Registered<B: Backend = crate::backend::Postgres> {
    /// Engine-assigned identity, from the one counter every maintained answer
    /// draws from.
    pub subscription_id: SubscriptionId,
    /// How the answer is maintained, carrying whatever that tier needs.
    pub tier: Tier<B>,
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
    pub not_served_because: Option<NotServed<B>>,
}

impl<B: Backend> Registered<B> {
    /// The registry's own record, for an answer it maintains in process.
    ///
    /// `None` for every tier that needs a read, which have no predicate and no
    /// normalized statement to report.
    #[must_use]
    pub const fn served(&self) -> Option<&Served<B>> {
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
/// Every variant other than [`Self::InProcess`] hands the caller an executable
/// bound query because the engine cannot read the database itself.
#[derive(Clone, Debug, PartialEq)]
pub enum Tier<B: Backend = crate::backend::Postgres> {
    /// Maintained from the change stream alone, with no read at all.
    ///
    /// Rows or a fold: [`Served::projection`] says which, and a fold carries
    /// the query that seeds it.
    InProcess(Served<B>),
    /// A scalar extreme, re-read when the extreme itself leaves the answer.
    Scalar {
        /// Bound query for the initial value and later triggers.
        query: crate::reexec::BoundQuery<B>,
        /// Decode hint for the scalar result.
        column_kind: BuiltinKind,
    },
    /// Grouped extrema seeded together and re-read one displaced group at a time.
    GroupedScalar {
        /// Bound seed query and decode kinds for the initial group map.
        bootstrap: AggregateBootstrap<B>,
    },
    /// One table's rows, re-read for the keys that changed.
    KeyedRows {
        /// Bound query, unchanged from the caller's request.
        query: crate::reexec::BoundQuery<B>,
        /// The one table this tier reads, and the only one that triggers it.
        table_id: TableId,
    },
    /// The whole answer, re-read when any table it reads changes.
    WholeRows {
        /// Bound query, unchanged from the caller's request.
        query: crate::reexec::BoundQuery<B>,
        /// Tables whose changes trigger a read.
        tables: Vec<TableId>,
    },
}

impl<B: Backend> Tier<B> {
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
/// [`Tier`] carries a bound query and the tables behind it, while a stored
/// record keeps the source SQL and plans the executable query again on load.
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

/// What the registry recorded for an answer it maintains itself.
#[derive(Clone, Debug, PartialEq)]
pub struct Served<B: Backend = crate::backend::Postgres> {
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
    /// `None` for a row subscription. Run [`AggregateBootstrap::query`]
    /// (typing each column by [`AggregateBootstrap::kinds`]) and pass the
    /// decoded row to
    /// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall).
    pub aggregate_bootstrap: Option<AggregateBootstrap<B>>,
}

impl<B: Backend> Served<B> {
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
pub struct ScalarInstall<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Scalar returned by SQL.
    pub value: Value<B>,
    /// Position of the change that caused the read, when known.
    pub checkpoint: Option<C>,
}

/// One page in a complete row result.
#[derive(Clone, Debug, PartialEq)]
pub struct InstalledPage<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Column names reported by the database.
    pub columns: Vec<String>,
    /// Rows in column order.
    pub rows: Vec<Vec<Value<B>>>,
    /// Whether another page follows.
    pub more: bool,
    /// Position the read is tied to, when known.
    pub checkpoint: Option<C>,
}

/// Complete row result from one database read.
#[derive(Clone, Debug, PartialEq)]
pub struct WholeRowsInstall<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Read generation used to group pages from one replacement.
    pub generation: u64,
    /// Pages in delivery order.
    pub pages: Vec<InstalledPage<B, C>>,
}

/// One keyed row change returned by a database read.
#[derive(Clone, Debug, PartialEq)]
pub struct InstalledRowDelta<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Primary-key values in key-column order.
    pub key: Vec<Value<B>>,
    /// Current row, or `None` when the row left the result.
    pub row: Option<Vec<Value<B>>>,
    /// Position of the change that caused the read, when known.
    pub checkpoint: Option<C>,
}

/// Keyed row changes from one database read.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyedRowsInstall<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Column names for `row`, in projection order.
    pub columns: Vec<String>,
    /// Changes in delivery order.
    pub deltas: Vec<InstalledRowDelta<B, C>>,
}

/// Starting rows for an in-process aggregate.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateSeedInstall<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Decoded rows returned by the aggregate seed SQL.
    ///
    /// An ungrouped aggregate supplies exactly one row. A grouped aggregate
    /// supplies one row per group.
    pub rows: Vec<Vec<Value<B>>>,
    /// Stream position taken before the read snapshot opened.
    pub read_at: Option<C>,
}

/// Starting rows for a grouped extreme.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupedScalarSeedInstall<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Group values, extreme and source-row count in bootstrap order.
    pub rows: Vec<Vec<Value<B>>>,
    /// Stream position taken before the read snapshot opened.
    pub read_at: Option<C>,
}

/// Result of re-reading one displaced extreme.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupedScalarInstall<B: Backend, C: Checkpoint = NoCheckpoint> {
    /// Opaque canonical identity of the constrained group.
    pub group: Vec<u8>,
    /// Current extreme and source-row count returned by the scoped query.
    pub row: Vec<Value<B>>,
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
    /// A grouped row cannot produce the canonical identity selected at registration.
    #[error("subscription {0} received values outside its canonical group-key domain")]
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
#[derive(Clone, Debug, PartialEq)]
pub struct RestoredReads<B: Backend = crate::backend::Postgres> {
    /// Answers that came back, each needing a read to fill it in.
    pub restored: Vec<RestoredRead<B>>,
    /// Answers that could not come back, with the reason.
    pub dropped: Vec<DroppedRead>,
}

impl<B: Backend> Default for RestoredReads<B> {
    fn default() -> Self {
        Self {
            restored: Vec::new(),
            dropped: Vec::new(),
        }
    }
}

/// One answer that came back from the file.
#[derive(Clone, Debug, PartialEq)]
pub struct RestoredRead<B: Backend = crate::backend::Postgres> {
    /// The identity it had before the restart, which it keeps.
    pub subscription_id: SubscriptionId,
    /// The tier planning its SQL chose this time.
    pub tier: Tier<B>,
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
