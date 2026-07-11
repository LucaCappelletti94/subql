//! Subscription engine - main public API

use super::indexes::IndexableAtom;
use super::{
    dispatch::{dispatch_consumers, dispatch_consumers_with_stamps, ConsumerDictionary},
    ids::{ConsumerOrdinal, PredicateId},
    partition::TablePartition,
    predicate::{Predicate, SubscriptionBinding},
};
use crate::backend::{Backend, CdcEvent, ScalarKind};
use crate::compiler::literals::SqlLiteralParse;
use crate::{
    catalog_helpers,
    compiler::{
        canonicalize, parse_and_resolve_hash, parse_compile_normalize_and_prefilter_with_binds,
        sql_shape::{AggSpec, QueryProjection},
        BytecodeProgram, PrefilterPlan, Vm,
    },
    DispatchError, EventKind, IdTypes, RegisterError, RegisterResult, SubscriptionDispatch,
    SubscriptionId, SubscriptionRegistration, SubscriptionRequest, SubscriptionScope,
    SubscriptionUnregistration, TableId, UnregisterReport,
};
#[cfg(feature = "std")]
use crate::{
    persistence::{
        codec,
        merge::MergeManager,
        predicate_data::dedup_predicates_by_hash,
        shard::{
            deserialize_shard, serialize_shard, BindingData, ConsumerDictData, PredicateData,
            ShardPayload,
        },
    },
    DurabilityMode, DurableShardMerge, DurableShardStore, MergeError, MergeJobId, MergeReport,
    StorageError,
};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use sql_traits::prelude::DatabaseLike;
// sqlparser::dialect::Dialect is now reached via `<E::Backend as Backend>::Dialect`.
#[cfg(feature = "std")]
use std::io::Write;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

type BatchEntries<I, B> = Vec<(
    Predicate<B>,
    Vec<IndexableAtom>,
    Vec<SubscriptionBinding<I>>,
)>;

/// Per-subscription activity counters used by activity-aware eviction
/// policies (e.g. `EvictLeastActive`, `EvictColdest`) to decide which
/// subscription to evict when the registry cap is hit.
///
/// Stamped on dispatch in `consumers()` only when an activity-aware
/// eviction policy is configured. Subscriptions never dispatched have
/// `last_dispatch_at = None` and `dispatch_count = 0`.
#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct ActivityStats {
    pub(crate) last_dispatch_at: Option<u64>,
    pub(crate) dispatch_count: u64,
}

/// Closure type for custom eviction policies. Picks the victim id from
/// a borrowed snapshot of every live subscription, or `None` to leave
/// the registry untouched (registration falls through to `Reject`).
type CustomEvictor<I> =
    Arc<dyn Fn(&crate::SubscriptionsView<'_, I>) -> Option<SubscriptionId> + Send + Sync + 'static>;

/// Internal dispatch shape for the configured eviction behavior.
///
/// `BuiltIn` wraps the user-facing [`crate::EvictionPolicy`] enum.
/// `Custom` holds a closure provided through
/// [`SubscriptionEngine::with_custom_eviction`]. Held internally so the
/// public `EvictionPolicy` enum stays plain `Copy` (closures break
/// `Copy`/`Eq`/`Hash`).
enum EvictionStrategy<I: IdTypes> {
    BuiltIn(crate::EvictionPolicy),
    Custom(CustomEvictor<I>),
}

impl<I: IdTypes> Clone for EvictionStrategy<I> {
    fn clone(&self) -> Self {
        match self {
            Self::BuiltIn(p) => Self::BuiltIn(*p),
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

impl<I: IdTypes> Default for EvictionStrategy<I> {
    fn default() -> Self {
        Self::BuiltIn(crate::EvictionPolicy::Reject)
    }
}

impl<I: IdTypes> EvictionStrategy<I> {
    /// Returns true when the configured strategy reads per-subscription
    /// activity stats, i.e. dispatch should stamp `last_dispatch_at`
    /// and `dispatch_count` to keep those counters live.
    const fn needs_activity_tracking(&self) -> bool {
        match self {
            Self::BuiltIn(p) => matches!(
                p,
                crate::EvictionPolicy::EvictLeastActive | crate::EvictionPolicy::EvictColdest
            ),
            // Custom closures see activity stats through `SubscriptionsView`,
            // so we assume they want them tracked.
            Self::Custom(_) => true,
        }
    }
}

struct CompiledSpec<I: IdTypes, B: Backend> {
    spec: SubscriptionRequest<I, B>,
    table_id: TableId,
    bytecode: BytecodeProgram<B>,
    normalized: String,
    prefilter_plan: PrefilterPlan,
    projection: QueryProjection,
    hash: u128,
}

#[cfg(feature = "std")]
enum DurabilityCheckOutcome {
    Ok,
    RequiredFailure { message: String, post_commit: bool },
}

use alloc::sync::Arc;
#[cfg(feature = "std")]
use std::path::{Path, PathBuf};

#[cfg(feature = "std")]
#[derive(Debug)]
enum RebuildPayloadError {
    Codec(String),
    Corrupt(String),
}

#[cfg(test)]
static INJECT_PARENT_DIR_SYNC_FAILURE_DIRS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
#[cfg(test)]
thread_local! {
    // Phase-3 partition-drop injection is per-thread so that the test
    // running the injection cannot taint sibling tests that hit the same
    // `table_id` from a different thread under cargo's parallel runner.
    static INJECT_BATCH_PHASE3_PARTITION_DROP_TABLES: std::cell::RefCell<HashSet<TableId>> =
        std::cell::RefCell::new(HashSet::new());
    static INJECT_COMPILE_HASH_OVERRIDES: std::cell::RefCell<std::collections::HashMap<String, u128>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

#[cfg(test)]
fn injected_parent_dir_sync_failure_dirs() -> &'static Mutex<HashSet<PathBuf>> {
    INJECT_PARENT_DIR_SYNC_FAILURE_DIRS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(test)]
fn with_injected_compile_hash_overrides<R>(
    f: impl FnOnce(&mut std::collections::HashMap<String, u128>) -> R,
) -> R {
    INJECT_COMPILE_HASH_OVERRIDES.with(|cell| {
        let mut map = cell.borrow_mut();
        f(&mut map)
    })
}

#[cfg(test)]
fn injected_compile_hash_override(normalized: &str) -> Option<u128> {
    INJECT_COMPILE_HASH_OVERRIDES.with(|cell| cell.borrow().get(normalized).copied())
}

#[cfg(feature = "std")]
impl core::fmt::Display for RebuildPayloadError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Codec(msg) | Self::Corrupt(msg) => f.write_str(msg),
        }
    }
}

/// Main subscription engine
///
/// Manages subscriptions across all tables with hybrid indexing and
/// predicate deduplication.
pub struct SubscriptionEngine<E: CdcEvent, I: IdTypes, DB: DatabaseLike>
where
    E::Backend: SqlLiteralParse,
{
    /// SQL dialect for parsing (from `E::Backend::Dialect`).
    dialect: <E::Backend as Backend>::Dialect,
    /// Schema database for table / column resolution.
    database: DB,
    /// Table partitions (TableId -> TablePartition).
    partitions: HashMap<TableId, TablePartition<I, E::Backend>>,
    /// Per-table [`ScalarKind`] cache, index-aligned with [`crate::ColumnId`].
    /// Populated when a subscription first targets a table so dispatch
    /// can route event scalar accessors without re-querying the catalog.
    column_kinds: HashMap<TableId, Arc<[ScalarKind]>>,
    /// User dictionaries (TableId -> ConsumerDictionary).
    consumer_dictionaries: HashMap<TableId, ConsumerDictionary<I>>,
    /// Subscription index for O(1) unregister / upsert lookup.
    subscription_to_table: HashMap<SubscriptionId, TableId>,
    /// Monotonic counter for auto-assigning subscription IDs (starts at 1).
    next_subscription_id: u64,
    /// Dedup index: (consumer_id, predicate_hash, scope) -> existing SubscriptionId.
    binding_dedup: HashMap<(I::ConsumerId, u128, SubscriptionScope<I>), SubscriptionId>,
    /// VM for bytecode evaluation, scoped to the observed backend.
    vm: Vm<E::Backend>,
    /// Optional cap on the number of live subscriptions. `None` means no
    /// cap. The registry can grow unbounded.
    max_subscriptions: Option<usize>,
    /// Eviction strategy applied when [`max_subscriptions`](Self::max_subscriptions)
    /// would be exceeded. Default `BuiltIn(EvictionPolicy::Reject)`.
    eviction_strategy: EvictionStrategy<I>,
    /// Monotonic clock used by activity-aware eviction policies to stamp
    /// `last_dispatch_at`. Lazy-initialised to a `StdClock` the first
    /// time an activity-aware policy is configured. Remains `None` for
    /// the default (no cap) and for non-activity policies so dispatch
    /// stays allocation-free.
    activity_clock: Option<crate::ClockHandle>,
    /// Per-subscription activity counters. Populated only when an
    /// activity-aware policy is configured. Empty otherwise so dispatch
    /// pays no cost.
    subscription_activity: HashMap<SubscriptionId, ActivityStats>,
    /// Optional storage path for durability.
    #[cfg(feature = "std")]
    storage_path: Option<PathBuf>,
    /// Shard rotation threshold (bytes).
    rotation_threshold: usize,
    /// Background merge compaction manager.
    #[cfg(feature = "std")]
    merge_manager: MergeManager<I>,
    /// Persistence strictness policy for registration.
    #[cfg(feature = "std")]
    durability_mode: DurabilityMode,
    /// Per-`(session, subscription)` resume cursor.
    ///
    /// Stored as an [`crate::OpaqueCheckpoint`] so the engine does not
    /// have to carry a struct-level checkpoint type parameter. The
    /// materializer serialises its concrete checkpoint type (`PgLsn`,
    /// `MysqlBinlogPos`, ...) to bytes before installing, deserialises
    /// on read. The map's cleanup is hooked into
    /// [`unregister_session`](Self::unregister_session) and the
    /// per-subscription unregister paths so cursors never outlive their
    /// owning subscription.
    resume_cursors: HashMap<(I::SessionId, SubscriptionId), crate::OpaqueCheckpoint>,
    /// Single-row ("pk") follow subscriptions:
    /// `subscription_id -> (table, pk_values)`. Populated by
    /// [`follow_row`](Self::follow_row); when the tracked row is deleted,
    /// the follow self-unregisters (its `WHERE pk = <value>` predicate
    /// can never match again). In-memory only (not persisted): across a
    /// restart a pk-follow loses its auto-close marker and degrades to
    /// an ordinary inert subscription. Stored as `Vec<Value<E::Backend>>`
    /// so the values round-trip through the parser's bind resolver.
    pk_follows: HashMap<SubscriptionId, (TableId, Vec<crate::backend::Value<E::Backend>>)>,
}

/// Look up the partition and consumer dictionary for a table, or return
/// `DispatchError::UnknownTableId` if either is missing.
fn table_context<'a, I: IdTypes, B: Backend>(
    partitions: &'a HashMap<TableId, TablePartition<I, B>>,
    consumer_dicts: &'a HashMap<TableId, ConsumerDictionary<I>>,
    table_id: TableId,
) -> Result<(&'a TablePartition<I, B>, &'a ConsumerDictionary<I>), DispatchError> {
    let partition = partitions
        .get(&table_id)
        .ok_or(DispatchError::UnknownTableId(table_id))?;
    let consumer_dict = consumer_dicts
        .get(&table_id)
        .ok_or(DispatchError::UnknownTableId(table_id))?;
    Ok((partition, consumer_dict))
}

impl<E: CdcEvent, I: IdTypes, DB: DatabaseLike + 'static> SubscriptionEngine<E, I, DB>
where
    E::Backend: SqlLiteralParse,
{
    /// The SQL dialect used for parsing. Exposed for the reexec wrapper,
    /// which re-parses queries the engine rejects.
    pub(crate) const fn dialect(&self) -> &<E::Backend as Backend>::Dialect {
        &self.dialect
    }

    /// The schema catalog the engine resolves tables/columns against.
    ///
    /// Borrow it when you need to read the schema alongside a live engine
    /// (e.g. to resolve a `TableId` for building `WalEvent`s) without keeping
    /// a separate catalog instance.
    pub const fn database(&self) -> &DB {
        &self.database
    }

    fn index_atoms_from_plan(plan: &PrefilterPlan) -> Vec<IndexableAtom> {
        let mut atoms: Vec<IndexableAtom> = plan
            .trigger_atoms
            .iter()
            .map(IndexableAtom::from_planner)
            .collect();

        if plan.scan_required {
            atoms.push(IndexableAtom::Fallback);
        }

        atoms
    }

    fn compile_spec(
        &self,
        spec: SubscriptionRequest<I, E::Backend>,
    ) -> Result<CompiledSpec<I, E::Backend>, RegisterError> {
        let (table_id, bytecode, normalized, prefilter_plan, projection) =
            parse_compile_normalize_and_prefilter_with_binds::<E::Backend, DB>(
                &spec.sql,
                &self.dialect,
                &self.database,
                &spec.binds,
            )?;

        // Disambiguate hash: same WHERE clause with different projection
        // kind must map to distinct predicates.
        let hash_input = crate::compiler::parser::projection_hash_input(&normalized, &projection);
        let hash = canonicalize::hash_sql(&hash_input);
        #[cfg(test)]
        let hash = injected_compile_hash_override(&normalized).unwrap_or(hash);

        Ok(CompiledSpec {
            spec,
            table_id,
            bytecode,
            normalized,
            prefilter_plan,
            projection,
            hash,
        })
    }

    fn make_predicate_from_compiled(
        compiled: &CompiledSpec<I, E::Backend>,
    ) -> (Predicate<E::Backend>, Vec<IndexableAtom>) {
        let atoms = Self::index_atoms_from_plan(&compiled.prefilter_plan);

        // For aggregate subscriptions that read a column (SUM/AVG/COUNT(col)/
        // VAR_*/STDDEV_*), augment dependency_columns with the aggregate
        // column. This ensures UPDATE events that change only the aggregate
        // column (not any WHERE column) are still dispatched to the aggregate
        // pipeline.
        let dependency_columns: Arc<[u16]> = {
            let mut dep_cols = compiled.bytecode.dependency_columns.clone();
            let agg_col = match &compiled.projection {
                QueryProjection::Aggregate(
                    AggSpec::Sum { column }
                    | AggSpec::Avg { column }
                    | AggSpec::CountColumn { column }
                    | AggSpec::VarPop { column }
                    | AggSpec::VarSamp { column }
                    | AggSpec::StddevPop { column }
                    | AggSpec::StddevSamp { column },
                ) => Some(*column),
                _ => None,
            };
            if let Some(column) = agg_col {
                if !dep_cols.contains(&column) {
                    dep_cols.push(column);
                    dep_cols.sort_unstable();
                }
            }
            Arc::from(dep_cols.as_slice())
        };

        let pred = Predicate {
            // Placeholder. Store allocates the authoritative ID.
            id: PredicateId::from_slab_index(0),
            hash: compiled.hash,
            normalized_sql: compiled.normalized.clone().into(),
            bytecode: Arc::new(compiled.bytecode.clone()),
            dependency_columns,
            index_atoms: Arc::from(atoms.as_slice()),
            prefilter_plan: Arc::new(compiled.prefilter_plan.clone()),
            projection: compiled.projection.clone(),
            refcount: 0, // Will be incremented via binding
            updated_at_unix_ms: compiled.spec.updated_at_unix_ms,
        };
        (pred, atoms)
    }

    const fn make_binding(
        spec: &SubscriptionRequest<I, E::Backend>,
        subscription_id: SubscriptionId,
        pred_id: PredicateId,
        consumer_ord: ConsumerOrdinal,
    ) -> SubscriptionBinding<I> {
        SubscriptionBinding {
            subscription_id,
            predicate_id: pred_id,
            consumer_id: spec.consumer_id,
            consumer_ordinal: consumer_ord,
            scope: spec.scope,
            updated_at_unix_ms: spec.updated_at_unix_ms,
        }
    }

    #[cfg(feature = "std")]
    const fn is_post_commit_dirsync_error(err: &StorageError) -> bool {
        matches!(err, StorageError::PostCommitDirSync(_))
    }

    // Not `const fn`: the observability branch calls tracing::warn! which is not const.
    #[cfg(feature = "std")]
    #[allow(clippy::missing_const_for_fn)]
    fn log_best_effort_durability(message: &str) {
        #[cfg(feature = "observability")]
        tracing::warn!("{message}");
        #[cfg(not(feature = "observability"))]
        let _ = message;
    }

    #[cfg(feature = "std")]
    fn enforce_table_durability(&self, table_id: TableId) -> DurabilityCheckOutcome {
        if self.storage_path.is_none() {
            return DurabilityCheckOutcome::Ok;
        }

        let should_rotate = match self.should_rotate(table_id) {
            Ok(v) => v,
            Err(e) => {
                let message = format!("Rotation check failed for table {table_id}: {e}");
                if self.durability_mode == DurabilityMode::BestEffort {
                    Self::log_best_effort_durability(&format!("Best-effort durability: {message}"));
                    return DurabilityCheckOutcome::Ok;
                }
                return DurabilityCheckOutcome::RequiredFailure {
                    message,
                    post_commit: false,
                };
            }
        };
        if !should_rotate {
            return DurabilityCheckOutcome::Ok;
        }

        match self.snapshot_table(table_id) {
            Ok(()) => DurabilityCheckOutcome::Ok,
            Err(snapshot_err) => {
                if self.durability_mode == DurabilityMode::BestEffort {
                    Self::log_best_effort_durability(&format!(
                        "Best-effort durability: snapshot failed for table {table_id}: {snapshot_err}"
                    ));
                    return DurabilityCheckOutcome::Ok;
                }
                DurabilityCheckOutcome::RequiredFailure {
                    message: format!("Snapshot failed for table {table_id}: {snapshot_err}"),
                    post_commit: Self::is_post_commit_dirsync_error(&snapshot_err),
                }
            }
        }
    }

    #[cfg(test)]
    fn should_inject_parent_dir_sync_failure(path: &Path) -> bool {
        let Some(parent) = path.parent() else {
            return false;
        };
        let lock = injected_parent_dir_sync_failure_dirs();
        let guard = match lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.contains(parent)
    }

    #[cfg(test)]
    fn should_inject_batch_phase3_partition_drop(table_id: TableId) -> bool {
        INJECT_BATCH_PHASE3_PARTITION_DROP_TABLES.with(|set| set.borrow().contains(&table_id))
    }

    /// Create new subscription engine.
    ///
    /// # Examples
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{DefaultIds, SubscriptionEngine};
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE orders (id INT);")
    ///     .expect("DDL parses");
    ///
    /// let engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// assert_eq!(engine.subscription_count(), 0);
    /// ```
    #[must_use]
    pub fn new(database: DB, dialect: <E::Backend as Backend>::Dialect) -> Self {
        Self {
            dialect,
            database,
            partitions: HashMap::new(),
            column_kinds: HashMap::new(),
            consumer_dictionaries: HashMap::new(),
            subscription_to_table: HashMap::new(),
            next_subscription_id: 1,
            binding_dedup: HashMap::new(),
            vm: Vm::new(),
            max_subscriptions: None,
            eviction_strategy: EvictionStrategy::default(),
            activity_clock: None,
            subscription_activity: HashMap::new(),
            #[cfg(feature = "std")]
            storage_path: None,
            #[cfg(feature = "std")]
            rotation_threshold: crate::config::DEFAULT_ROTATION_THRESHOLD,
            #[cfg(not(feature = "std"))]
            rotation_threshold: 0,
            #[cfg(feature = "std")]
            merge_manager: MergeManager::new(),
            #[cfg(feature = "std")]
            durability_mode: DurabilityMode::Required,
            resume_cursors: HashMap::new(),
            pk_follows: HashMap::new(),
        }
    }

    /// Populate [`Self::column_kinds`] for `table_id` if not already
    /// cached. The cache is index-aligned with [`crate::ColumnId`] and
    /// records each column's [`ScalarKind`] so dispatch routes typed
    /// scalar accessors without re-querying the catalog. Columns whose
    /// declared type does not map to a supported scalar fall back to
    /// [`ScalarKind::String`], which routes to the string accessor;
    /// events with mismatched wire shapes surface as `Presence::Missing`
    /// and dispatch falls through to the fallback predicate set.
    fn ensure_column_kinds_cached(&mut self, table_id: TableId) {
        if self.column_kinds.contains_key(&table_id) {
            return;
        }
        let arity = catalog_helpers::table_arity(&self.database, table_id).unwrap_or(0);
        let kinds: Vec<ScalarKind> = (0..arity)
            .map(|i| {
                #[allow(clippy::cast_possible_truncation)]
                let col: crate::ColumnId = i as crate::ColumnId;
                catalog_helpers::column_scalar_kind(&self.database, table_id, col)
                    .unwrap_or(ScalarKind::String)
            })
            .collect();
        self.column_kinds.insert(table_id, Arc::from(kinds));
    }

    /// Configure a maximum number of live subscriptions and the built-in
    /// policy applied when a registration would exceed it.
    ///
    /// By default the registry is uncapped. With [`crate::EvictionPolicy::Reject`]
    /// the cap is hard: `register` returns [`RegisterError::RegistryFull`]
    /// once the registry is full. With [`crate::EvictionPolicy::EvictOldest`]
    /// the oldest subscription (lowest [`SubscriptionId`]) is evicted to
    /// make room. Activity-aware policies such as
    /// [`crate::EvictionPolicy::EvictLeastActive`] /
    /// [`crate::EvictionPolicy::EvictColdest`] additionally enable
    /// per-subscription dispatch stamping. Topological policies such as
    /// [`crate::EvictionPolicy::EvictBySession`] /
    /// [`crate::EvictionPolicy::EvictByConsumer`] pick the victim from a
    /// preferred slice of the registry. In every non-`Reject` case the
    /// evicted ids are surfaced in [`RegisterResult::evicted`].
    ///
    /// For closure-based custom policies see
    /// [`Self::with_custom_eviction`].
    ///
    /// Idempotent re-registrations (matching `(consumer_id, predicate, scope)`)
    /// never trigger eviction since they do not allocate a new subscription
    /// slot.
    #[must_use]
    pub fn with_max_subscriptions(mut self, cap: usize, policy: crate::EvictionPolicy) -> Self {
        self.max_subscriptions = Some(cap);
        self.eviction_strategy = EvictionStrategy::BuiltIn(policy);
        self.ensure_activity_clock_for_strategy();
        self
    }

    /// Configure a maximum number of live subscriptions and a custom
    /// closure that picks the eviction victim when the cap is hit.
    ///
    /// `evictor` receives a [`crate::SubscriptionsView`] over every live
    /// subscription (id, consumer, scope, activity counters) and returns
    /// either `Some(id)` to evict that subscription, or `None` to leave
    /// the registry untouched. When the closure returns `None`, the
    /// registration falls through to a `Reject` outcome and `register`
    /// returns [`RegisterError::RegistryFull`].
    ///
    /// Closures see live `dispatch_count` / `last_dispatch_at` values:
    /// configuring this builder turns on activity stamping in
    /// [`Self::consumers`], so the activity stats stay current.
    ///
    /// The closure runs synchronously on the registration path. Keep it
    /// cheap. It must be `Send + Sync + 'static` so the engine remains
    /// `Send + Sync` whenever `I` is.
    ///
    /// # Examples
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest,
    ///     SubscriptionsView,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// // Evict the subscription belonging to the lowest consumer id.
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {}).with_custom_eviction(
    ///         2,
    ///         |view: &SubscriptionsView<'_, DefaultIds>| {
    ///             view.iter()
    ///                 .min_by_key(|m| m.consumer_id)
    ///                 .map(|m| m.subscription_id)
    ///         },
    ///     );
    ///
    /// let low = engine.register(SubscriptionRequest::new(
    ///     1u64,
    ///     "SELECT * FROM orders WHERE amount > 1",
    /// ))?;
    /// let _high = engine.register(SubscriptionRequest::new(
    ///     9u64,
    ///     "SELECT * FROM orders WHERE amount > 2",
    /// ))?;
    /// let third = engine.register(SubscriptionRequest::new(
    ///     5u64,
    ///     "SELECT * FROM orders WHERE amount > 3",
    /// ))?;
    /// assert_eq!(third.evicted, vec![low.subscription_id]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_custom_eviction<F>(mut self, cap: usize, evictor: F) -> Self
    where
        F: Fn(&crate::SubscriptionsView<'_, I>) -> Option<SubscriptionId> + Send + Sync + 'static,
    {
        self.max_subscriptions = Some(cap);
        self.eviction_strategy = EvictionStrategy::Custom(Arc::new(evictor));
        self.ensure_activity_clock_for_strategy();
        self
    }

    fn ensure_activity_clock_for_strategy(&mut self) {
        if self.eviction_strategy.needs_activity_tracking() && self.activity_clock.is_none() {
            #[cfg(feature = "std")]
            {
                self.activity_clock = Some(Arc::new(crate::StdClock::new()));
            }
        }
    }

    /// Replace the activity clock used by activity-aware eviction
    /// policies. Useful in tests to drive `last_dispatch_at` with a
    /// `ManualClock`. Production builds should rely on the default
    /// `StdClock` selected by the builder methods.
    #[must_use]
    pub fn with_activity_clock(mut self, clock: crate::ClockHandle) -> Self {
        self.activity_clock = Some(clock);
        self
    }

    /// Create engine with durable storage
    ///
    /// Loads existing shards from storage directory on startup.
    #[cfg(feature = "std")]
    #[allow(clippy::needless_pass_by_value)]
    pub fn with_storage(
        database: DB,
        dialect: <E::Backend as Backend>::Dialect,
        storage_path: PathBuf,
    ) -> Result<Self, StorageError> {
        let mut engine = Self::new(database, dialect);
        engine.storage_path = Some(storage_path.clone());

        // Create storage directory if it doesn't exist
        std::fs::create_dir_all(&storage_path)
            .map_err(|e| StorageError::Io(format!("Failed to create storage directory: {e}")))?;

        // Load existing shards
        engine.load_all_shards()?;

        Ok(engine)
    }

    /// Register a new subscription.
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and binds consumer.
    /// If storage is enabled and rotation threshold is exceeded, triggers snapshot.
    ///
    /// # Examples
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// let first = engine.register(
    ///     SubscriptionRequest::new(10, "SELECT * FROM orders WHERE amount > 100")
    ///         .updated_at_unix_ms(1_704_067_200_000),
    /// )?;
    ///
    /// let second = engine.register(
    ///     SubscriptionRequest::new(20, "SELECT * FROM orders WHERE amount > 100")
    ///         .updated_at_unix_ms(1_704_067_200_001),
    /// )?;
    ///
    /// assert!(first.created_new_predicate);
    /// assert!(!second.created_new_predicate);
    /// assert_eq!(engine.predicate_count(orders_id), 1);
    /// assert_eq!(engine.subscription_count(), 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    pub fn register(
        &mut self,
        spec: SubscriptionRequest<I, E::Backend>,
    ) -> Result<RegisterResult, RegisterError> {
        // 1. Parse, compile, and canonicalize in one pass.
        let compiled = self.compile_spec(spec)?;
        let table_id = compiled.table_id;
        let hash = compiled.hash;

        // 2. Check dedup index: same (consumer_id, predicate_hash, scope) -> idempotent return.
        let natural_key = (compiled.spec.consumer_id, hash, compiled.spec.scope);
        if let Some(&existing_sub_id) = self.binding_dedup.get(&natural_key) {
            return Ok(RegisterResult {
                subscription_id: existing_sub_id,
                table_id,
                normalized_sql: compiled.normalized,
                predicate_hash: hash,
                created_new_predicate: false,
                projection: compiled.projection,
                evicted: Vec::new(),
            });
        }

        // 2.5. Enforce subscription registry cap. Idempotent re-registrations
        // already short-circuited above and never trip the cap.
        let mut evicted: Vec<SubscriptionId> = Vec::new();
        if let Some(cap) = self.max_subscriptions {
            if self.subscription_to_table.len() >= cap {
                if let Some(victim) = self.pick_eviction_victim() {
                    if self.unregister_subscription_internal(victim).is_some() {
                        evicted.push(victim);
                    }
                } else {
                    return Err(RegisterError::RegistryFull { cap });
                }
            }
        }

        // 3. Auto-assign a new subscription ID.
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        // Populate the per-table `ScalarKind` cache on first sight of this
        // table. Dispatch relies on this cache to route event scalar
        // accessors; without it, indexable predicates that need a column
        // probe (e.g. range predicates on typed columns) are silently
        // skipped in favor of the fallback set only.
        self.ensure_column_kinds_cached(table_id);

        // 4. Get/create table partition and consumer dictionary
        let partition = self
            .partitions
            .entry(table_id)
            .or_insert_with(|| TablePartition::new(table_id));

        let consumer_dict = self.consumer_dictionaries.entry(table_id).or_default();

        // 5. Get consumer ordinal
        let consumer_ord = consumer_dict
            .try_get_or_create(compiled.spec.consumer_id)
            .map_err(|e| RegisterError::Storage(e.to_string()))?;

        // 6. Check if predicate exists (deduplication)
        let snapshot = partition.load_snapshot();
        let (pred_id, created_new) = snapshot
            .predicates
            .find_by_hash_and_sql(hash, &compiled.normalized)
            .map_or_else(
                || {
                    let (pred, atoms) = Self::make_predicate_from_compiled(&compiled);
                    let pred_id = partition.add_predicate(pred, atoms);
                    (pred_id, true)
                },
                |existing| (existing, false),
            );

        // 7. Create binding
        let binding = Self::make_binding(&compiled.spec, subscription_id, pred_id, consumer_ord);

        // Add binding to partition
        partition.add_binding(binding, pred_id);

        // 8. Index subscription for O(1) unregister/upsert lookups.
        self.subscription_to_table.insert(subscription_id, table_id);
        self.binding_dedup.insert(natural_key, subscription_id);

        // 9. Enforce durability policy for this table.
        #[cfg(feature = "std")]
        if let DurabilityCheckOutcome::RequiredFailure {
            message,
            post_commit,
        } = self.enforce_table_durability(table_id)
        {
            if !post_commit {
                let _ = self.unregister_subscription_internal(subscription_id);
            }
            return Err(RegisterError::Storage(message));
        }

        Ok(RegisterResult {
            subscription_id,
            table_id,
            normalized_sql: compiled.normalized,
            predicate_hash: hash,
            created_new_predicate: created_new,
            projection: compiled.projection,
            evicted,
        })
    }

    /// Register a `SELECT` for `consumer_id`, building the
    /// [`SubscriptionRequest`] with the engine's own id types (no turbofish).
    ///
    /// ```
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{DefaultIds, SubscriptionEngine};
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    /// )?;
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// let rows = engine.register_select(1, "SELECT * FROM orders WHERE amount > 100")?;
    /// assert!(rows.aggregate_spec().is_none());
    ///
    /// let agg = engine.register_select(2, "SELECT COUNT(*) FROM orders")?;
    /// assert!(agg.aggregate_spec().is_some());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn register_select(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: impl Into<String>,
    ) -> Result<RegisterResult, RegisterError> {
        self.register(SubscriptionRequest::new(consumer_id, sql))
    }

    /// Register a follow subscription derived from an UPDATE statement.
    ///
    /// The UPDATE's target table and WHERE clause become a standing
    /// `SELECT * FROM t WHERE <where>` subscription: the consumer follows every
    /// row the UPDATE matches, now and going forward (a moving set). SubQL does
    /// not execute the UPDATE, it only reads its shape.
    ///
    /// # Errors
    /// [`RegisterError::FollowUnsupportedStatement`] if `update_sql` is not an
    /// UPDATE, [`RegisterError::UnsupportedUpdateShape`] for an unsupported
    /// UPDATE (joins, FROM, ORDER BY/LIMIT, or no WHERE), plus any error from
    /// registering the derived SELECT.
    pub fn register_follow_update(
        &mut self,
        consumer_id: I::ConsumerId,
        update_sql: impl Into<String>,
    ) -> Result<RegisterResult, RegisterError> {
        self.register_follow_update_with_binds(consumer_id, update_sql, Vec::new())
    }

    /// Like [`Self::register_follow_update`], but with bind values for the
    /// `$N`/`?` placeholders in the UPDATE's WHERE (the typed diesel path
    /// supplies these).
    ///
    /// # Errors
    /// See [`Self::register_follow_update`].
    pub fn register_follow_update_with_binds(
        &mut self,
        consumer_id: I::ConsumerId,
        update_sql: impl Into<String>,
        binds: Vec<crate::backend::Value<E::Backend>>,
    ) -> Result<RegisterResult, RegisterError> {
        let update_sql = update_sql.into();
        let select_sql = crate::compiler::derive_update_follow_select(&update_sql, &self.dialect)?;
        self.register(SubscriptionRequest::new(consumer_id, select_sql).binds(binds))
    }

    /// Follow a specific row by its primary-key value(s).
    ///
    /// Registers `SELECT * FROM <table> WHERE <pk> = <value>` (an `AND` of
    /// equalities for a composite key), so the consumer receives every future
    /// change to that row. This is the building block for following an inserted
    /// row once its (possibly auto-generated) key is known.
    ///
    /// `pk` must supply one value per primary-key column, in declaration order.
    ///
    /// # Errors
    /// [`RegisterError::UnknownTable`] if `table` is unknown,
    /// [`RegisterError::NoPrimaryKey`] if it has no primary key,
    /// [`RegisterError::UnsupportedSql`] on a key-arity mismatch, plus any error
    /// from registering the derived SELECT.
    pub fn follow_row(
        &mut self,
        consumer_id: I::ConsumerId,
        table: &str,
        pk: Vec<crate::backend::Value<E::Backend>>,
    ) -> Result<RegisterResult, RegisterError> {
        let table_id = catalog_helpers::table_id(&self.database, table)
            .ok_or_else(|| RegisterError::UnknownTable(table.to_string()))?;
        let pk_cols = catalog_helpers::primary_key_columns(&self.database, table_id)
            .ok_or_else(|| RegisterError::UnknownTable(table.to_string()))?;
        if pk_cols.is_empty() {
            return Err(RegisterError::NoPrimaryKey { table_id });
        }
        if pk.len() != pk_cols.len() {
            return Err(RegisterError::UnsupportedSql(format!(
                "follow_row: table {table:?} has {} primary-key column(s) but {} value(s) were supplied",
                pk_cols.len(),
                pk.len()
            )));
        }
        let mut clauses = Vec::with_capacity(pk_cols.len());
        for (i, col_id) in pk_cols.iter().enumerate() {
            let name = catalog_helpers::column_name(&self.database, table_id, *col_id).ok_or_else(
                || RegisterError::UnknownColumn {
                    table_id,
                    column: format!("<primary-key ordinal {col_id}>"),
                },
            )?;
            clauses.push(format!("\"{name}\" = ${}", i + 1));
        }
        let sql = format!("SELECT * FROM \"{table}\" WHERE {}", clauses.join(" AND "));
        // Build the tracked key before `pk` is moved into the request binds.
        let key = pk.clone();
        let result = self.register(SubscriptionRequest::new(consumer_id, sql).binds(pk))?;
        // Mark as a single-row follow so it self-closes when its row is deleted.
        self.pk_follows
            .insert(result.subscription_id, (table_id, key));
        Ok(result)
    }

    /// Register multiple subscriptions in a single batch.
    ///
    /// Significantly more efficient than calling `register()` in a loop:
    /// performs a single COW clone and single snapshot swap per table instead
    /// of one per subscription. Ideal for bulk loading at startup.
    ///
    /// Returns results in the same order as the input specs.
    /// In required durability mode, pre-commit snapshot failures are rolled back.
    /// Post-commit directory fsync failures are surfaced but not rolled back.
    ///
    /// **Parity with `register()`**. The returned `Vec` is guaranteed to
    /// be element-for-element equal to what calling `register()` in a
    /// loop on a fresh engine with the same configuration would have
    /// produced, including within-batch idempotent duplicates
    /// (collapsed onto the first occurrence's `SubscriptionId`) and
    /// the `evicted` accounting under an eviction policy. When an
    /// active eviction policy is configured (anything other than
    /// `Reject`), the implementation transparently falls back to a
    /// sequential `register()` loop to maintain parity. The bulk-COW
    /// fast path applies to the no-cap and `Reject`-cap cases.
    ///
    /// # Examples
    /// ```
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// let results = engine.register_batch(vec![
    ///     SubscriptionRequest::new(10, "SELECT * FROM orders WHERE status = 'paid'")
    ///         .updated_at_unix_ms(1_704_067_200_000),
    ///     SubscriptionRequest::new(11, "SELECT * FROM orders WHERE status = 'paid'")
    ///         .updated_at_unix_ms(1_704_067_200_001),
    ///     SubscriptionRequest::new(12, "SELECT * FROM orders WHERE amount > 100")
    ///         .updated_at_unix_ms(1_704_067_200_002),
    /// ]);
    ///
    /// let results: Vec<_> = results
    ///     .into_iter()
    ///     .collect::<Result<Vec<_>, _>>()?;
    ///
    /// assert!(results[0].created_new_predicate);
    /// assert!(!results[1].created_new_predicate);
    /// assert!(results[2].created_new_predicate);
    /// assert_eq!(engine.predicate_count(orders_id), 2);
    /// assert_eq!(engine.subscription_count(), 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[allow(clippy::too_many_lines)]
    pub fn register_batch(
        &mut self,
        specs: Vec<SubscriptionRequest<I, E::Backend>>,
    ) -> Vec<Result<RegisterResult, RegisterError>> {
        // Eviction-aware fallback: when an active-eviction policy is
        // configured, within-batch eviction would have to consider
        // pending-but-not-yet-committed sub_ids that
        // `pick_eviction_victim` does not see. Rather than thread that
        // visibility through every policy's selection logic
        // (`EvictLeastActive` / `EvictColdest` / `EvictBy*` all need
        // activity metadata pending subs do not have), we fall back to
        // a sequential `register()` loop in that case. The bulk-COW
        // optimization still applies when there is no cap or when the
        // policy is `Reject` (the common cases at startup-bootstrap).
        if self.max_subscriptions.is_some() {
            let eviction_active = !matches!(
                self.eviction_strategy,
                EvictionStrategy::BuiltIn(crate::EvictionPolicy::Reject)
            );
            if eviction_active {
                return specs.into_iter().map(|s| self.register(s)).collect();
            }
        }

        // Phase 1: Parse and compile all specs (can fail individually).
        // Idempotent duplicates are short-circuited before binding creation.
        let mut compiled: Vec<Option<CompiledSpec<I, E::Backend>>> =
            Vec::with_capacity(specs.len());
        let mut results: Vec<Result<RegisterResult, RegisterError>> =
            Vec::with_capacity(specs.len());
        // Within-batch natural-key dedup. Maps the natural key of a
        // freshly-compiled, not-yet-committed binding to the result
        // index that owns it. When a later spec in the same batch
        // matches one of these, we defer setting its `subscription_id`
        // until Phase 2 has assigned the primary's id, then copy it
        // over (or mirror the primary's error). Without this, a
        // duplicate `(consumer, predicate_hash, scope)` triple within
        // the same batch would diverge from the sequential
        // `register()` path, which would short-circuit the duplicate
        // onto the existing binding.
        let mut batch_natural_dedup: HashMap<(I::ConsumerId, u128, SubscriptionScope<I>), usize> =
            HashMap::new();
        // Pairs of `(duplicate_index, primary_index)` to reconcile
        // after Phase 2.
        let mut deferred_dup_copies: Vec<(usize, usize)> = Vec::new();

        for spec in specs {
            match self.compile_spec(spec) {
                Ok(compiled_spec) => {
                    // Check dedup index for idempotent re-registration.
                    let natural_key = (
                        compiled_spec.spec.consumer_id,
                        compiled_spec.hash,
                        compiled_spec.spec.scope,
                    );
                    if let Some(&existing_sub_id) = self.binding_dedup.get(&natural_key) {
                        results.push(Ok(RegisterResult {
                            subscription_id: existing_sub_id,
                            table_id: compiled_spec.table_id,
                            normalized_sql: compiled_spec.normalized,
                            predicate_hash: compiled_spec.hash,
                            created_new_predicate: false,
                            projection: compiled_spec.projection,
                            evicted: Vec::new(),
                        }));
                        compiled.push(None); // already handled
                        continue;
                    }
                    // Check within-batch dedup against earlier specs in
                    // this same batch.
                    if let Some(&primary_idx) = batch_natural_dedup.get(&natural_key) {
                        let dup_idx = results.len();
                        deferred_dup_copies.push((dup_idx, primary_idx));
                        results.push(Ok(RegisterResult {
                            subscription_id: 0, // copied from primary after Phase 2
                            table_id: compiled_spec.table_id,
                            normalized_sql: compiled_spec.normalized,
                            predicate_hash: compiled_spec.hash,
                            created_new_predicate: false,
                            projection: compiled_spec.projection,
                            evicted: Vec::new(),
                        }));
                        compiled.push(None);
                        continue;
                    }
                    // First occurrence of this natural key within the
                    // batch. Record so subsequent duplicates can defer
                    // onto it.
                    batch_natural_dedup.insert(natural_key, results.len());
                    results.push(Ok(RegisterResult {
                        subscription_id: 0, // filled in phase 2
                        table_id: compiled_spec.table_id,
                        normalized_sql: String::new(), // filled in phase 2
                        predicate_hash: compiled_spec.hash,
                        created_new_predicate: false, // filled in phase 2
                        projection: compiled_spec.projection.clone(),
                        evicted: Vec::new(),
                    }));
                    compiled.push(Some(compiled_spec));
                }
                Err(e) => {
                    compiled.push(None);
                    results.push(Err(e));
                }
            }
        }

        // Phase 2: Group by table and batch-insert
        let mut table_entries: HashMap<TableId, BatchEntries<I, E::Backend>> = HashMap::new();
        let mut table_result_indices: HashMap<TableId, Vec<usize>> = HashMap::new();
        let mut table_inserted_sub_ids: HashMap<TableId, Vec<SubscriptionId>> = HashMap::new();

        // Track which hashes we've already prepared (dedup within batch)
        let mut batch_hash_to_idx: HashMap<(TableId, u128, String), usize> = HashMap::new();

        // Pending count = subscriptions allocated during this batch that
        // are not yet visible in `self.subscription_to_table` (the new-
        // predicate / same-batch-dup paths defer the insert to phase 3).
        // The existing-predicate path inserts immediately, so it does
        // not contribute here.
        let mut pending_uncommitted: usize = 0;
        // Evictions attributed to the spec that triggered them. Filled
        // when the cap-branch picks a victim.
        let mut evicted_per_spec: HashMap<usize, Vec<SubscriptionId>> = HashMap::new();

        for (i, entry) in compiled.into_iter().enumerate() {
            let Some(c) = entry else { continue };

            // Enforce the registry cap before allocating a sub id. The
            // projected post-batch size is `subscription_to_table.len()
            // + pending_uncommitted`; if that already exceeds the cap,
            // try to evict via the configured strategy.
            if let Some(cap) = self.max_subscriptions {
                while self.subscription_to_table.len() + pending_uncommitted >= cap {
                    let Some(victim) = self.pick_eviction_victim() else {
                        // Strategy declined to evict (Reject or custom
                        // closure returned None). Reject this spec.
                        results[i] = Err(RegisterError::RegistryFull { cap });
                        break;
                    };
                    if self.unregister_subscription_internal(victim).is_some() {
                        evicted_per_spec.entry(i).or_default().push(victim);
                    } else {
                        // Defensive: victim disappeared. Do not loop.
                        results[i] = Err(RegisterError::RegistryFull { cap });
                        break;
                    }
                }
                // If the spec was just rejected, skip the rest of phase 2 for it.
                if results[i].is_err() {
                    continue;
                }
            }

            // Auto-assign subscription ID.
            let subscription_id = self.next_subscription_id;
            self.next_subscription_id += 1;

            let natural_key = (c.spec.consumer_id, c.hash, c.spec.scope);

            // Populate the per-table `ScalarKind` cache on first sight of
            // this table (batch register path). Mirrors the single-register
            // path above.
            self.ensure_column_kinds_cached(c.table_id);

            let partition = self
                .partitions
                .entry(c.table_id)
                .or_insert_with(|| TablePartition::new(c.table_id));
            let consumer_dict = self.consumer_dictionaries.entry(c.table_id).or_default();
            let consumer_ord = match consumer_dict.try_get_or_create(c.spec.consumer_id) {
                Ok(ord) => ord,
                Err(e) => {
                    results[i] = Err(RegisterError::Storage(e.to_string()));
                    continue;
                }
            };
            table_result_indices.entry(c.table_id).or_default().push(i);
            table_inserted_sub_ids
                .entry(c.table_id)
                .or_default()
                .push(subscription_id);

            // Check if predicate already exists in current snapshot
            let snapshot = partition.load_snapshot();
            let existing = snapshot
                .predicates
                .find_by_hash_and_sql(c.hash, &c.normalized);

            let created_new;

            let dedup_key = (c.table_id, c.hash, c.normalized.clone());

            if let Some(pred_id) = existing {
                // Existing-predicate path commits subscription_to_table
                // immediately below. Does NOT add to pending_uncommitted.
                let binding = Self::make_binding(&c.spec, subscription_id, pred_id, consumer_ord);
                partition.add_binding(binding, pred_id);
                self.subscription_to_table
                    .insert(subscription_id, c.table_id);
                created_new = false;
            } else if let Some(&batch_idx) = batch_hash_to_idx.get(&dedup_key) {
                let Some(entries) = table_entries.get_mut(&c.table_id) else {
                    results[i] = Err(RegisterError::Storage(format!(
                        "Batch register failed for table {}: missing batch entries",
                        c.table_id
                    )));
                    continue;
                };
                let binding = Self::make_binding(
                    &c.spec,
                    subscription_id,
                    PredicateId::from_slab_index(0),
                    consumer_ord,
                );
                entries[batch_idx].2.push(binding);
                // Deferred to phase 3.
                pending_uncommitted += 1;
                created_new = false;
            } else {
                let (pred, atoms) = Self::make_predicate_from_compiled(&c);
                let binding = Self::make_binding(
                    &c.spec,
                    subscription_id,
                    PredicateId::from_slab_index(0),
                    consumer_ord,
                );

                let entries = table_entries.entry(c.table_id).or_default();
                let batch_idx = entries.len();
                entries.push((pred, atoms, vec![binding]));
                batch_hash_to_idx.insert(dedup_key, batch_idx);
                // Deferred to phase 3.
                pending_uncommitted += 1;
                created_new = true;
            }

            self.binding_dedup.insert(natural_key, subscription_id);

            // Fill in the result, including any evictions credited to
            // this spec by the cap branch above.
            if let Ok(ref mut result) = results[i] {
                result.subscription_id = subscription_id;
                result.normalized_sql = c.normalized;
                result.created_new_predicate = created_new;
                if let Some(ev) = evicted_per_spec.remove(&i) {
                    result.evicted = ev;
                }
            }
        }

        #[cfg(test)]
        {
            for &table_id in table_result_indices.keys() {
                if Self::should_inject_batch_phase3_partition_drop(table_id) {
                    self.partitions.remove(&table_id);
                }
            }
        }

        // Phase 3: Batch-insert into partitions (single COW + single swap per table)
        let mut phase3_failed_tables = HashSet::new();
        for (table_id, entries) in table_entries {
            let Some(partition) = self.partitions.get_mut(&table_id) else {
                phase3_failed_tables.insert(table_id);
                if let Some(indices) = table_result_indices.get(&table_id) {
                    for &idx in indices {
                        results[idx] = Err(RegisterError::Storage(format!(
                            "Batch register failed for table {table_id}: missing partition during phase3"
                        )));
                    }
                }
                continue;
            };
            partition.add_batch(&entries);
            for (_, _, bindings) in &entries {
                for binding in bindings {
                    self.subscription_to_table
                        .insert(binding.subscription_id, table_id);
                }
            }
        }

        #[cfg(feature = "std")]
        {
            let mut failures: Vec<(TableId, String, bool)> = Vec::new();
            for &table_id in table_result_indices.keys() {
                if phase3_failed_tables.contains(&table_id) {
                    continue;
                }
                if let DurabilityCheckOutcome::RequiredFailure {
                    message,
                    post_commit,
                } = self.enforce_table_durability(table_id)
                {
                    failures.push((table_id, message, post_commit));
                }
            }

            if !failures.is_empty() && self.durability_mode == DurabilityMode::Required {
                for (table_id, message, post_commit) in failures {
                    if !post_commit {
                        if let Some(sub_ids) = table_inserted_sub_ids.get(&table_id) {
                            for &sub_id in sub_ids {
                                let _ = self.unregister_subscription_internal(sub_id);
                            }
                        }
                    }
                    if let Some(indices) = table_result_indices.get(&table_id) {
                        for &idx in indices {
                            results[idx] = Err(RegisterError::Storage(message.clone()));
                        }
                    }
                }
            }
        }
        #[cfg(not(feature = "std"))]
        {
            let _ = phase3_failed_tables;
            let _ = table_inserted_sub_ids;
        }

        // Phase 4: Within-batch duplicate reconciliation. For every
        // duplicate spec that pointed at an earlier primary in this
        // batch, mirror the primary's outcome so the duplicate's
        // `Result` matches what a sequential `register()` loop would
        // return: same `SubscriptionId` on success, same error on
        // failure.
        for (dup_idx, primary_idx) in deferred_dup_copies {
            match &results[primary_idx] {
                Ok(primary_ok) => {
                    let primary_sub_id = primary_ok.subscription_id;
                    let primary_normalized = primary_ok.normalized_sql.clone();
                    if let Ok(dup_ok) = &mut results[dup_idx] {
                        dup_ok.subscription_id = primary_sub_id;
                        if dup_ok.normalized_sql.is_empty() {
                            dup_ok.normalized_sql = primary_normalized;
                        }
                    }
                }
                Err(primary_err) => {
                    let cloned = primary_err.clone();
                    results[dup_idx] = Err(cloned);
                }
            }
        }

        results
    }

    /// Unregister a subscription
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate is removed.
    /// Also drops every per-session resume cursor associated with this
    /// `subscription_id` so cursors never outlive their owning subscription.
    pub fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        let removed = self
            .unregister_subscription_internal(subscription_id)
            .is_some();
        self.resume_cursors
            .retain(|(_, sub_id), _| *sub_id != subscription_id);
        removed
    }

    // ----------------------------------------------------------------
    // Per-`(session, subscription)` resume cursor API. The cursor is the
    // position the materializer last successfully dispatched to the
    // client. On reconnect the materializer compares it against its own
    // oplog watermark to decide catchup vs full re-sync.
    // ----------------------------------------------------------------

    /// Advance the resume cursor for `(session_id, sub_id)` to
    /// `checkpoint`.
    ///
    /// Returns `Ok(None)` when no cursor was previously stored for the
    /// pair (i.e. this is the first advance), and `Ok(Some(previous))`
    /// when an existing cursor was overwritten.
    ///
    /// Returns [`crate::AdvanceCursorError::NonMonotonic`] when `checkpoint`
    /// is strictly less than the stored value (a rewind) and **does
    /// not** mutate the map. A successful dispatch never moves a
    /// client backwards in the CDC stream, so a rewind is always a
    /// caller bug. Use [`force_set_cursor`](Self::force_set_cursor)
    /// for the legitimate snapshot-bootstrap / recovery escape hatch.
    ///
    /// # Errors
    ///
    /// [`crate::AdvanceCursorError::NonMonotonic`] when `checkpoint < previous`.
    pub fn advance_cursor(
        &mut self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
        checkpoint: crate::OpaqueCheckpoint,
    ) -> Result<Option<crate::OpaqueCheckpoint>, crate::AdvanceCursorError> {
        if let Some(previous) = self.resume_cursors.get(&(session_id, sub_id)) {
            if checkpoint < *previous {
                return Err(crate::AdvanceCursorError::NonMonotonic {
                    previous: previous.clone(),
                    attempted: checkpoint,
                });
            }
        }
        Ok(self.resume_cursors.insert((session_id, sub_id), checkpoint))
    }

    /// Set the resume cursor for `(session_id, sub_id)`
    /// unconditionally, bypassing the monotonic check enforced by
    /// [`advance_cursor`](Self::advance_cursor).
    ///
    /// The intended use is snapshot bootstrap (the materializer
    /// installs the snapshot LSN after the initial sync) and
    /// operator-driven recovery from a stuck state. The normal
    /// happy-path dispatch flow should always use
    /// [`advance_cursor`](Self::advance_cursor).
    ///
    /// Returns the previously stored cursor, if any.
    pub fn force_set_cursor(
        &mut self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
        checkpoint: crate::OpaqueCheckpoint,
    ) -> Option<crate::OpaqueCheckpoint> {
        self.resume_cursors.insert((session_id, sub_id), checkpoint)
    }

    /// Look up the current resume cursor for `(session_id, sub_id)`.
    #[must_use]
    pub fn cursor_for(
        &self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
    ) -> Option<&crate::OpaqueCheckpoint> {
        self.resume_cursors.get(&(session_id, sub_id))
    }

    /// Iterator over `(subscription_id, cursor)` for every cursor stored
    /// against `session_id`. The order is unspecified.
    ///
    /// Typical usage on reconnect: the materializer iterates this to
    /// build the catchup plan for every subscription the session was
    /// observing.
    pub fn cursors_for_session(
        &self,
        session_id: I::SessionId,
    ) -> impl Iterator<Item = (SubscriptionId, &crate::OpaqueCheckpoint)> + '_ {
        self.resume_cursors.iter().filter_map(move |((s, sub), c)| {
            if *s == session_id {
                Some((*sub, c))
            } else {
                None
            }
        })
    }

    /// Remove the resume cursor for `(session_id, sub_id)` and return
    /// the previously stored value, if any. Intended for the
    /// materializer's "I am done with this subscription on this
    /// session" path. Routine cleanup happens automatically via
    /// [`unregister_session`](Self::unregister_session) and
    /// [`unregister_subscription`](Self::unregister_subscription).
    pub fn drop_cursor(
        &mut self,
        session_id: I::SessionId,
        sub_id: SubscriptionId,
    ) -> Option<crate::OpaqueCheckpoint> {
        self.resume_cursors.remove(&(session_id, sub_id))
    }

    fn cleanup_consumer_if_unreferenced(&mut self, table_id: TableId, consumer_id: I::ConsumerId) {
        let has_active_bindings = self.partitions.get(&table_id).is_some_and(|partition| {
            let snapshot = partition.load_snapshot();
            snapshot.predicates.is_consumer_referenced(consumer_id)
        });

        if !has_active_bindings {
            if let Some(consumer_dict) = self.consumer_dictionaries.get_mut(&table_id) {
                let _ = consumer_dict.remove(consumer_id);
            }
        }
    }

    /// Collect a [`crate::SubscriptionMetadata`] snapshot for every live
    /// subscription. Allocates one `Vec` per call. Only invoked from the
    /// register cap-branch and the custom-eviction closure path, so it
    /// is off the hot dispatch path.
    fn collect_subscription_metadata(&self) -> Vec<crate::SubscriptionMetadata<I>> {
        let mut out: Vec<crate::SubscriptionMetadata<I>> =
            Vec::with_capacity(self.subscription_to_table.len());
        for (&sub_id, &table_id) in &self.subscription_to_table {
            let Some(partition) = self.partitions.get(&table_id) else {
                continue;
            };
            let snapshot = partition.load_snapshot();
            let Some(binding) = snapshot.predicates.bindings.get(&sub_id) else {
                continue;
            };
            let stats = self
                .subscription_activity
                .get(&sub_id)
                .copied()
                .unwrap_or_default();
            out.push(crate::SubscriptionMetadata {
                subscription_id: sub_id,
                consumer_id: binding.consumer_id,
                scope: binding.scope,
                last_dispatch_at: stats.last_dispatch_at,
                dispatch_count: stats.dispatch_count,
            });
        }
        out
    }

    /// Select a victim subscription id from the registry according to the
    /// configured eviction strategy. Returns `None` when the strategy
    /// declines to evict (e.g. `Reject`, or a custom closure that
    /// returned `None`). Idempotent re-registrations short-circuit before
    /// this is called.
    fn pick_eviction_victim(&self) -> Option<SubscriptionId> {
        match &self.eviction_strategy {
            EvictionStrategy::BuiltIn(crate::EvictionPolicy::Reject) => None,
            EvictionStrategy::BuiltIn(crate::EvictionPolicy::EvictOldest) => {
                self.subscription_to_table.keys().copied().min()
            }
            EvictionStrategy::BuiltIn(crate::EvictionPolicy::EvictLeastActive) => {
                self.collect_subscription_metadata()
                    .into_iter()
                    .min_by(|a, b| {
                        // None (never matched) sorts before Some: such
                        // subscriptions are evicted before any with a
                        // recorded stamp. Ties resolve by oldest id.
                        a.last_dispatch_at
                            .cmp(&b.last_dispatch_at)
                            .then(a.subscription_id.cmp(&b.subscription_id))
                    })
                    .map(|m| m.subscription_id)
            }
            EvictionStrategy::BuiltIn(crate::EvictionPolicy::EvictColdest) => self
                .collect_subscription_metadata()
                .into_iter()
                .min_by(|a, b| {
                    a.dispatch_count
                        .cmp(&b.dispatch_count)
                        .then(a.subscription_id.cmp(&b.subscription_id))
                })
                .map(|m| m.subscription_id),
            EvictionStrategy::BuiltIn(crate::EvictionPolicy::EvictBySession) => {
                let metas = self.collect_subscription_metadata();
                let session_victim = metas
                    .iter()
                    .filter(|m| matches!(m.scope, SubscriptionScope::Session(_)))
                    .min_by_key(|m| m.subscription_id)
                    .map(|m| m.subscription_id);
                session_victim.or_else(|| metas.iter().map(|m| m.subscription_id).min())
            }
            EvictionStrategy::BuiltIn(crate::EvictionPolicy::EvictByConsumer) => {
                let metas = self.collect_subscription_metadata();
                if metas.is_empty() {
                    return None;
                }
                let mut per_consumer: HashMap<I::ConsumerId, usize> = HashMap::new();
                for m in &metas {
                    *per_consumer.entry(m.consumer_id).or_insert(0) += 1;
                }
                // Largest holder wins. Ties resolve by lowest consumer id
                // so the choice is deterministic across HashMap orderings.
                let target_consumer = per_consumer
                    .into_iter()
                    .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
                    .map(|(c, _)| c)?;
                metas
                    .iter()
                    .filter(|m| m.consumer_id == target_consumer)
                    .min_by_key(|m| m.subscription_id)
                    .map(|m| m.subscription_id)
            }
            EvictionStrategy::Custom(evictor) => {
                let metas = self.collect_subscription_metadata();
                let view = crate::SubscriptionsView::new(metas.as_slice());
                evictor(&view)
            }
        }
    }

    /// Internal unregister helper.
    ///
    /// Returns `Some(predicate_removed)` if subscription existed, else `None`.
    fn unregister_subscription_internal(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Option<bool> {
        self.pk_follows.remove(&subscription_id);
        // Capture dedup key from binding before removing it.
        let dedup_key = self.dedup_key_for_subscription(subscription_id);

        // Fast path: direct lookup from subscription index.
        if let Some(table_id) = self.subscription_to_table.get(&subscription_id).copied() {
            let removal = self
                .partitions
                .get_mut(&table_id)
                .and_then(|partition| partition.remove_binding_detail(subscription_id));
            if let Some(removal) = removal {
                self.subscription_to_table.remove(&subscription_id);
                self.subscription_activity.remove(&subscription_id);
                if let Some(key) = dedup_key {
                    self.binding_dedup.remove(&key);
                }
                self.cleanup_consumer_if_unreferenced(table_id, removal.consumer_id);
                return Some(removal.predicate_removed);
            }

            // Stale index entry. Clean it up and fall back to scan.
            self.subscription_to_table.remove(&subscription_id);
        }

        // Fallback scan for pre-index or inconsistent states.
        let mut removed = None;
        for (&table_id, partition) in &mut self.partitions {
            if let Some(removal) = partition.remove_binding_detail(subscription_id) {
                removed = Some((table_id, removal));
                break;
            }
        }

        if let Some((table_id, removal)) = removed {
            self.subscription_to_table.remove(&subscription_id);
            self.subscription_activity.remove(&subscription_id);
            if let Some(key) = dedup_key {
                self.binding_dedup.remove(&key);
            }
            self.cleanup_consumer_if_unreferenced(table_id, removal.consumer_id);
            return Some(removal.predicate_removed);
        }

        None
    }

    /// Look up the dedup natural key for a subscription by finding its binding.
    fn dedup_key_for_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> Option<(I::ConsumerId, u128, SubscriptionScope<I>)> {
        if let Some(&table_id) = self.subscription_to_table.get(&subscription_id) {
            if let Some(partition) = self.partitions.get(&table_id) {
                let snapshot = partition.load_snapshot();
                if let Some(binding) = snapshot.predicates.bindings.get(&subscription_id) {
                    let pred_hash = snapshot
                        .predicates
                        .get_predicate(binding.predicate_id)
                        .map(|p| p.hash)?;
                    return Some((binding.consumer_id, pred_hash, binding.scope));
                }
            }
        }
        for partition in self.partitions.values() {
            let snapshot = partition.load_snapshot();
            if let Some(binding) = snapshot.predicates.bindings.get(&subscription_id) {
                let pred_hash = snapshot
                    .predicates
                    .get_predicate(binding.predicate_id)
                    .map(|p| p.hash)?;
                return Some((binding.consumer_id, pred_hash, binding.scope));
            }
        }
        None
    }

    /// Dispatch event to interested consumers.
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, DefaultIds, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// engine.register(
    ///     SubscriptionRequest::new(42, "SELECT * FROM orders WHERE amount > 100")
    ///         .updated_at_unix_ms(1_704_067_200_000),
    /// )?;
    ///
    /// let event = TestEvent::<Postgres>::insert(
    ///     orders_id,
    ///     vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
    /// )
    /// .with_pk_columns([0u16]);
    ///
    /// let notifs = engine.consumers(&event)?;
    /// assert_eq!(notifs.inserted(), vec![42]);
    /// assert!(notifs.deleted().is_empty());
    /// assert!(notifs.updated().is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn consumers(
        &mut self,
        event: &E,
    ) -> Result<crate::ConsumerNotifications<I, E::Checkpoint>, DispatchError> {
        // A table owns a partition only once a subscription targets it.
        // An event for a table that is in the catalog but has no
        // subscription affects nobody: report empty rather than
        // erroring. Reserve `UnknownTableId` for ids not in the schema
        // at all (genuine drift or a caller bug).
        if !self.partitions.contains_key(&event.table_id()) {
            return if self.table_in_catalog(event.table_id()) {
                Ok(crate::ConsumerNotifications::empty()
                    .with_checkpoint(event.checkpoint().cloned()))
            } else {
                Err(DispatchError::UnknownTableId(event.table_id()))
            };
        }

        // Activity-aware policies stamp matched subscriptions through
        // the `_with_stamps` dispatch variant. Default (no activity
        // tracking) takes the cheaper path that allocates no stamp
        // vector.
        let needs_stamps = self.eviction_strategy.needs_activity_tracking();

        let column_kinds = self
            .column_kinds
            .get(&event.table_id())
            .cloned()
            .unwrap_or_else(|| Arc::from(Vec::<ScalarKind>::new()));

        let (partition, consumer_dict) = table_context(
            &self.partitions,
            &self.consumer_dictionaries,
            event.table_id(),
        )?;

        if needs_stamps {
            let (notifs, stamps) = dispatch_consumers_with_stamps(
                event,
                partition,
                consumer_dict,
                &mut self.vm,
                &column_kinds,
            )?;
            self.stamp_activity(&stamps);
            Ok(notifs)
        } else {
            dispatch_consumers(event, partition, consumer_dict, &mut self.vm, &column_kinds)
        }
    }

    fn stamp_activity(&mut self, stamped_subs: &[SubscriptionId]) {
        if stamped_subs.is_empty() {
            return;
        }
        let now = self.activity_clock.as_ref().map(|c| c.now_micros());
        for &sub_id in stamped_subs {
            let entry = self.subscription_activity.entry(sub_id).or_default();
            entry.dispatch_count = entry.dispatch_count.saturating_add(1);
            if let Some(now_micros) = now {
                entry.last_dispatch_at = Some(now_micros);
            }
        }
    }

    /// Compute typed signed deltas for aggregate subscriptions (COUNT(*), SUM(col), ...).
    ///
    /// Returns `Vec<(ConsumerId, AggDelta)>`` where each entry is the net signed change
    /// for that consumer's aggregate predicate. Zero-net entries are omitted.
    /// The same consumer may appear multiple times (once per aggregate kind).
    ///
    /// # Caller contract
    /// - Bootstrap: query the DB for the initial aggregate **before** subscribing.
    /// - Accumulate: `running_value += delta` on each call.
    /// - UPDATE image requirement: aggregate UPDATE deltas require both
    ///   `old_row` and `new_row`. When CDC omits old images, this API returns
    ///   `DispatchError::AggregateUpdateRequiresOldRow`.
    /// - Reset on policy change: RLS/ACL changes produce no WAL events.
    ///   Re-query the DB and replace the stored value.
    /// - Reset on TRUNCATE: engine returns `Err(TruncateRequiresReset)`.
    ///   Caller must re-query and replace.
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, AggDelta, DefaultIds, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// engine.register(
    ///     SubscriptionRequest::new(99, "SELECT COUNT(*) FROM orders WHERE status = 'paid'")
    ///         .updated_at_unix_ms(1_704_067_200_000),
    /// )?;
    ///
    /// let event = TestEvent::<Postgres>::insert(
    ///     orders_id,
    ///     vec![Value::Int(1), Value::String("paid".into())],
    /// )
    /// .with_pk_columns([0u16]);
    ///
    /// let deltas = engine.aggregate_deltas(&event)?;
    /// assert_eq!(deltas, vec![(99, AggDelta::Count(1))]);
    ///
    /// // Aggregate subscriptions are handled by `aggregate_deltas()`, not `consumers()`.
    /// let notifs = engine.consumers(&event)?;
    /// assert!(notifs.inserted().is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn aggregate_deltas(
        &mut self,
        event: &E,
    ) -> Result<Vec<(I::ConsumerId, crate::AggDelta)>, DispatchError> {
        // See `consumers`: a cataloged table with no subscription
        // contributes no aggregate deltas; only a truly unknown table id
        // errors.
        if !self.partitions.contains_key(&event.table_id()) {
            return if self.table_in_catalog(event.table_id()) {
                Ok(Vec::new())
            } else {
                Err(DispatchError::UnknownTableId(event.table_id()))
            };
        }

        let column_kinds = self
            .column_kinds
            .get(&event.table_id())
            .cloned()
            .unwrap_or_else(|| Arc::from(Vec::<ScalarKind>::new()));

        let (partition, consumer_dict) = table_context(
            &self.partitions,
            &self.consumer_dictionaries,
            event.table_id(),
        )?;

        super::dispatch::compute_agg_deltas(
            event,
            partition,
            consumer_dict,
            &mut self.vm,
            &column_kinds,
        )
    }

    /// Dispatch one event through the row-set and aggregate paths at once.
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, AggDelta, DefaultIds, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    /// )?;
    /// let orders_id = catalog_helpers::table_id(&database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// engine.register(SubscriptionRequest::new(7, "SELECT * FROM orders WHERE amount > 100"))?;
    /// engine.register(SubscriptionRequest::new(9, "SELECT COUNT(*) FROM orders WHERE status = 'paid'"))?;
    ///
    /// let event = TestEvent::<Postgres>::insert(
    ///     orders_id,
    ///     vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
    /// )
    /// .with_pk_columns([0u16]);
    ///
    /// let out = engine.dispatch(&event)?;
    /// assert_eq!(out.notifications().inserted(), vec![7]);
    /// assert_eq!(out.aggregate_deltas(), &[(9, AggDelta::Count(1))]);
    /// assert_eq!(out.notified(), vec![7, 9]); // deduped union of both paths
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn dispatch(
        &mut self,
        event: &E,
    ) -> Result<crate::DispatchOutput<I, E::Checkpoint>, DispatchError> {
        let notifications = self.consumers(event)?;
        let aggregate_deltas = self.aggregate_deltas(event)?;
        let output = crate::DispatchOutput::from_parts(notifications, aggregate_deltas);
        // A single-row (pk) follow self-closes once its row is deleted.
        // Under Phase 5's typed-accessor model, deriving the deleted PK
        // from `event` requires walking `event.pk_columns()` and
        // calling the per-scalar accessor for each with `RowKind::Pk`.
        // Defer that plumbing to a follow-up (pk-follows are best-effort;
        // loss of the auto-close marker degrades gracefully to an
        // ordinary inert subscription per the `pk_follows` doc contract).
        if event.kind() == EventKind::Delete && !self.pk_follows.is_empty() {
            // TODO(phase-5): re-implement close_deleted_pk_follows against
            // the CdcEvent typed-accessor surface.
        }
        Ok(output)
    }

    /// Unregister all subscriptions for a session
    ///
    /// Also drops every resume cursor stored for `session_id` (across
    /// every subscription) so cursors never outlive their owning
    /// session.
    pub fn unregister_session(&mut self, session_id: I::SessionId) -> UnregisterReport {
        self.resume_cursors.retain(|(s, _), _| *s != session_id);

        let mut removed_bindings = 0;
        let mut removed_predicates = 0;
        let mut removed_consumers = 0;

        // Collect subscription IDs to remove
        let mut to_remove = Vec::new();
        let mut removed_consumer_candidates: HashMap<TableId, HashSet<I::ConsumerId>> =
            HashMap::new();

        for (&table_id, partition) in &self.partitions {
            let snapshot = partition.load_snapshot();

            if let Some(sub_ids) = snapshot.predicates.get_session_subscriptions(session_id) {
                removed_bindings += sub_ids.len();
                to_remove.extend_from_slice(sub_ids);
                let consumers = removed_consumer_candidates.entry(table_id).or_default();
                for sub_id in sub_ids {
                    if let Some(binding) = snapshot.predicates.bindings.get(sub_id) {
                        consumers.insert(binding.consumer_id);
                    }
                }
            }
        }

        // Remove subscriptions
        for sub_id in to_remove {
            if self.unregister_subscription_internal(sub_id) == Some(true) {
                removed_predicates += 1;
            }
        }

        for (table_id, consumers) in removed_consumer_candidates {
            let Some(consumer_dict) = self.consumer_dictionaries.get_mut(&table_id) else {
                continue;
            };

            let active_consumers: HashSet<I::ConsumerId> = self
                .partitions
                .get(&table_id)
                .map(|partition| {
                    let snapshot = partition.load_snapshot();
                    snapshot.predicates.active_consumer_ids()
                })
                .unwrap_or_default();

            for consumer_id in consumers {
                if !active_consumers.contains(&consumer_id) {
                    let was_present = consumer_dict.get(consumer_id).is_some();
                    if was_present {
                        let _ = consumer_dict.remove(consumer_id);
                    }
                    removed_consumers += 1;
                }
            }
        }

        UnregisterReport {
            removed_bindings,
            removed_predicates,
            removed_consumers,
        }
    }

    /// Unregister all subscriptions for a consumer matching a specific SQL query.
    ///
    /// Parses the SQL just enough to compute the predicate hash (no bytecode
    /// compilation), then removes all bindings for `consumer_id` that share that
    /// hash.
    pub fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        let (table_id, hash) =
            parse_and_resolve_hash::<E::Backend, DB>(sql, &self.dialect, &self.database)?;

        let empty = UnregisterReport {
            removed_bindings: 0,
            removed_predicates: 0,
            removed_consumers: 0,
        };

        let Some(partition) = self.partitions.get(&table_id) else {
            return Ok(empty);
        };

        // Find the predicate for this hash.
        let snapshot = partition.load_snapshot();
        let Some(pred_id) = snapshot.predicates.find_by_hash(hash) else {
            return Ok(empty);
        };

        // Collect subscription IDs belonging to this consumer on this predicate.
        let to_remove: Vec<SubscriptionId> = snapshot
            .predicates
            .bindings
            .values()
            .filter(|b| b.predicate_id == pred_id && b.consumer_id == consumer_id)
            .map(|b| b.subscription_id)
            .collect();

        if to_remove.is_empty() {
            return Ok(empty);
        }

        let removed_bindings = to_remove.len();
        let mut removed_predicates = 0;

        for sub_id in to_remove {
            if self.unregister_subscription_internal(sub_id) == Some(true) {
                removed_predicates += 1;
            }
        }

        // Check if the consumer still has any bindings in this table. If not, clean up.
        let removed_consumers = if self.partitions.get(&table_id).is_some_and(|partition| {
            let snap = partition.load_snapshot();
            snap.predicates.is_consumer_referenced(consumer_id)
        }) {
            0
        } else {
            if let Some(consumer_dict) = self.consumer_dictionaries.get_mut(&table_id) {
                let _ = consumer_dict.remove(consumer_id);
            }
            1
        };

        Ok(UnregisterReport {
            removed_bindings,
            removed_predicates,
            removed_consumers,
        })
    }

    /// Whether `table_id` resolves in the schema catalog, regardless of
    /// whether any subscription currently targets it. Used to tell a known
    /// table with no subscription (dispatch is a no-op) from a genuinely
    /// unknown table id (dispatch errors).
    fn table_in_catalog(&self, table_id: TableId) -> bool {
        catalog_helpers::table_arity(&self.database, table_id).is_some()
    }

    /// Validate row image arity against the expected arity for its table.
    ///
    /// Under Phase 5's typed-accessor model, arity validation would
    /// require probing every column via its correct scalar accessor and
    /// per-column [`ScalarKind`]. The dispatch layer already reports
    /// `Presence::Missing` for out-of-range columns via the event's
    /// accessors, so this check is retired on the direct dispatch path.
    /// Kept as a stub for API compatibility; callers that still want
    /// arity validation should implement it against `event.int_at(...)`
    /// etc.
    #[allow(dead_code, clippy::unused_self, clippy::needless_pass_by_value)]
    fn validate_row_arity(&self, _table_id: TableId) -> Result<(), DispatchError> {
        Ok(())
    }

    /// Get number of registered predicates for a table
    #[must_use]
    pub fn predicate_count(&self, table_id: TableId) -> usize {
        self.partitions.get(&table_id).map_or(0, |p| {
            let snapshot = p.load_snapshot();
            snapshot.predicates.predicates.len()
        })
    }

    /// Get number of registered subscriptions
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.partitions
            .values()
            .map(|p| {
                let snapshot = p.load_snapshot();
                snapshot.predicates.bindings.len()
            })
            .sum()
    }

    // ========================================================================
    // Persistence Methods
    // ========================================================================

    #[cfg(feature = "std")]
    fn sync_parent_dir(path: &Path) -> Result<(), StorageError> {
        #[cfg(test)]
        if Self::should_inject_parent_dir_sync_failure(path) {
            return Err(StorageError::Io("injected failure".to_string()));
        }

        #[cfg(unix)]
        {
            let parent = path.parent().ok_or_else(|| {
                StorageError::Io(format!("Path has no parent directory: {}", path.display()))
            })?;
            let dir = std::fs::File::open(parent).map_err(|e| {
                StorageError::Io(format!(
                    "Failed to open parent directory '{}': {e}",
                    parent.display()
                ))
            })?;
            dir.sync_all().map_err(|e| {
                StorageError::Io(format!(
                    "Failed to sync parent directory '{}': {e}",
                    parent.display()
                ))
            })?;
        }

        #[cfg(not(unix))]
        let _ = path;

        Ok(())
    }

    #[cfg(feature = "std")]
    fn durable_atomic_replace(
        storage_path: &Path,
        shard_path: &Path,
        tmp_stem: &str,
        bytes: &[u8],
        seed_ms: u64,
    ) -> Result<(), StorageError> {
        const MAX_ATTEMPTS: u32 = 32;
        let pid = std::process::id();

        for attempt in 0..MAX_ATTEMPTS {
            let tmp_name = format!("{tmp_stem}.shard.tmp.{pid}.{seed_ms}.{attempt}");
            let tmp_path = storage_path.join(tmp_name);

            let file = match std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)
            {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(StorageError::Io(format!(
                        "pre_commit: Failed to create temp shard '{}': {e}",
                        tmp_path.display()
                    )));
                }
            };

            let write_result = (|| -> Result<(), StorageError> {
                let mut file = file;
                file.write_all(bytes).map_err(|e| {
                    StorageError::Io(format!(
                        "pre_commit: Failed to write temp shard '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
                file.sync_all().map_err(|e| {
                    StorageError::Io(format!(
                        "pre_commit: Failed to sync temp shard '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
                drop(file);

                std::fs::rename(&tmp_path, shard_path).map_err(|e| {
                    StorageError::Io(format!(
                        "pre_commit: Failed to rename '{}' -> '{}': {e}",
                        tmp_path.display(),
                        shard_path.display()
                    ))
                })?;

                Self::sync_parent_dir(shard_path)
                    .map_err(|e| StorageError::PostCommitDirSync(e.to_string()))?;
                Ok(())
            })();

            if let Err(e) = write_result {
                let _ = std::fs::remove_file(&tmp_path);
                return Err(e);
            }

            return Ok(());
        }

        Err(StorageError::Io(format!(
            "pre_commit: Failed to allocate unique temp shard path for '{}'",
            shard_path.display()
        )))
    }

    /// Snapshot table partition to disk
    ///
    /// Serializes all predicates, bindings, and consumer dictionary to a shard file.
    #[cfg(feature = "std")]
    pub fn snapshot_table(&self, table_id: TableId) -> Result<(), StorageError> {
        let storage_path = self
            .storage_path
            .as_ref()
            .ok_or_else(|| StorageError::Config("No storage path configured".to_string()))?;

        let partition = self
            .partitions
            .get(&table_id)
            .ok_or_else(|| StorageError::Corrupt(format!("Unknown table ID: {table_id}")))?;

        let consumer_dict = self.consumer_dictionaries.get(&table_id).ok_or_else(|| {
            StorageError::Corrupt(format!("No consumer dictionary for table {table_id}"))
        })?;

        // Load snapshot
        let snapshot = partition.load_snapshot();

        // Convert predicates to serializable format
        let mut predicate_data_vec = Vec::new();
        for (_idx, pred) in &snapshot.predicates.predicates {
            let pred_data = PredicateData {
                hash: pred.hash,
                normalized_sql: pred.normalized_sql.to_string(),
                bytecode_instructions: codec::serialize(&*pred.bytecode)
                    .map_err(|e| StorageError::Codec(format!("Bytecode serialize error: {e}")))?,
                prefilter_plan: codec::serialize(&*pred.prefilter_plan)
                    .map_err(|e| StorageError::Codec(format!("Prefilter serialize error: {e}")))?,
                dependency_columns: pred.dependency_columns.to_vec(),
                projection: pred.projection.clone(),
                refcount: pred.refcount,
                updated_at_unix_ms: pred.updated_at_unix_ms,
            };
            predicate_data_vec.push(pred_data);
        }

        // Convert bindings to serializable format
        let mut binding_data_vec = Vec::new();
        for binding in snapshot.predicates.bindings.values() {
            let predicate_hash = snapshot
                .predicates
                .get_predicate(binding.predicate_id)
                .map(|p| p.hash)
                .ok_or_else(|| {
                    StorageError::Corrupt(format!(
                        "SubscriptionBinding {:?} references missing predicate ID {:?}",
                        binding.subscription_id, binding.predicate_id
                    ))
                })?;
            let binding_data = BindingData::<I> {
                subscription_id: binding.subscription_id,
                predicate_hash,
                consumer_id: binding.consumer_id,
                scope: binding.scope,
                updated_at_unix_ms: binding.updated_at_unix_ms,
            };
            binding_data_vec.push(binding_data);
        }

        // Convert consumer dictionary to serializable format
        let consumer_dict_data = ConsumerDictData::<I> {
            ordinal_to_consumer: consumer_dict.ordinal_to_consumer_vec(),
        };

        // Build payload
        let payload: ShardPayload<I> = ShardPayload {
            predicates: predicate_data_vec,
            bindings: binding_data_vec,
            consumer_dict: consumer_dict_data,
            #[allow(clippy::cast_possible_truncation)]
            created_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        // Serialize shard
        let bytes = serialize_shard::<I, DB>(table_id, &payload, &self.database)?;

        // Write to disk atomically (temp file + fsync + rename + parent-dir fsync).
        let shard_path = storage_path.join(format!("table_{table_id}.shard"));
        Self::durable_atomic_replace(
            storage_path,
            &shard_path,
            &format!("table_{table_id}"),
            &bytes,
            payload.created_at_unix_ms,
        )
    }

    #[cfg(feature = "std")]
    fn rebuild_entries_from_payload(
        payload: &ShardPayload<I>,
    ) -> Result<(ConsumerDictionary<I>, BatchEntries<I, E::Backend>), RebuildPayloadError> {
        let mut consumer_dict = ConsumerDictionary::<I>::new();

        for consumer_id in &payload.consumer_dict.ordinal_to_consumer {
            consumer_dict
                .try_get_or_create(*consumer_id)
                .map_err(|e| RebuildPayloadError::Corrupt(e.to_string()))?;
        }

        // Build hash -> predicate map and validate binding references.
        let pred_hash_to_data = dedup_predicates_by_hash(
            payload.predicates.iter().cloned(),
            RebuildPayloadError::Corrupt,
        )?;

        // Build bindings grouped by predicate hash; IDs are assigned during add_batch.
        let mut bindings_by_hash: HashMap<u128, Vec<SubscriptionBinding<I>>> = HashMap::new();
        let mut consumers_with_bindings = HashSet::new();
        for binding_data in &payload.bindings {
            if !pred_hash_to_data.contains_key(&binding_data.predicate_hash) {
                return Err(RebuildPayloadError::Corrupt(format!(
                    "SubscriptionBinding references unknown predicate hash: {:016x}",
                    binding_data.predicate_hash
                )));
            }

            let consumer_ord = consumer_dict
                .try_get_or_create(binding_data.consumer_id)
                .map_err(|e| RebuildPayloadError::Corrupt(e.to_string()))?;
            consumers_with_bindings.insert(binding_data.consumer_id);
            bindings_by_hash
                .entry(binding_data.predicate_hash)
                .or_default()
                .push(SubscriptionBinding {
                    subscription_id: binding_data.subscription_id,
                    predicate_id: PredicateId::from_slab_index(0), // add_batch patches this
                    consumer_id: binding_data.consumer_id,
                    consumer_ordinal: consumer_ord,
                    scope: binding_data.scope,
                    updated_at_unix_ms: binding_data.updated_at_unix_ms,
                });
        }

        for consumer_id in &payload.consumer_dict.ordinal_to_consumer {
            if !consumers_with_bindings.contains(consumer_id) {
                let _ = consumer_dict.remove(*consumer_id);
            }
        }

        // Build batch entries from predicates + grouped bindings.
        let mut chosen_predicates: Vec<PredicateData> = pred_hash_to_data.into_values().collect();
        chosen_predicates.sort_unstable_by_key(|pred_data| pred_data.hash);

        let mut entries: BatchEntries<I, E::Backend> = Vec::new();
        for pred_data in chosen_predicates {
            let Some(bindings) = bindings_by_hash.remove(&pred_data.hash) else {
                continue;
            };

            let bytecode: BytecodeProgram<E::Backend> =
                codec::deserialize(&pred_data.bytecode_instructions).map_err(|e| {
                    RebuildPayloadError::Codec(format!("Bytecode deserialize error: {e}"))
                })?;

            let prefilter_plan: PrefilterPlan = codec::deserialize(&pred_data.prefilter_plan)
                .map_err(|e| {
                    RebuildPayloadError::Codec(format!("Prefilter deserialize error: {e}"))
                })?;
            let atoms = Self::index_atoms_from_plan(&prefilter_plan);
            let pred = Predicate {
                id: PredicateId::from_slab_index(0),
                hash: pred_data.hash,
                normalized_sql: pred_data.normalized_sql.into(),
                bytecode: Arc::new(bytecode.clone()),
                dependency_columns: Arc::from(pred_data.dependency_columns.as_slice()),
                index_atoms: Arc::from(atoms.as_slice()),
                prefilter_plan: Arc::new(prefilter_plan),
                projection: pred_data.projection,
                refcount: 0, // incremented via bindings in add_batch
                updated_at_unix_ms: pred_data.updated_at_unix_ms,
            };
            entries.push((pred, atoms, bindings));
        }

        if !bindings_by_hash.is_empty() {
            return Err(RebuildPayloadError::Corrupt(
                "Orphan bindings remained after reconstruction".to_string(),
            ));
        }

        Ok((consumer_dict, entries))
    }

    #[cfg(feature = "std")]
    fn replace_table_state(
        &mut self,
        table_id: TableId,
        partition: TablePartition<I, E::Backend>,
        consumer_dict: ConsumerDictionary<I>,
        entries: &BatchEntries<I, E::Backend>,
    ) {
        self.partitions.insert(table_id, partition);
        self.consumer_dictionaries.insert(table_id, consumer_dict);
        // Clear old subscription_to_table and binding_dedup entries for this table.
        self.subscription_to_table
            .retain(|_, mapped_table_id| *mapped_table_id != table_id);
        self.binding_dedup
            .retain(|_, sub_id| self.subscription_to_table.contains_key(sub_id));
        // Rebuild from loaded entries.
        for (pred, _, bindings) in entries {
            for binding in bindings {
                self.subscription_to_table
                    .insert(binding.subscription_id, table_id);
                self.binding_dedup.insert(
                    (binding.consumer_id, pred.hash, binding.scope),
                    binding.subscription_id,
                );
                if binding.subscription_id >= self.next_subscription_id {
                    self.next_subscription_id = binding.subscription_id + 1;
                }
            }
        }
    }

    #[cfg(feature = "std")]
    fn rebuild_and_replace_table_state(
        &mut self,
        table_id: TableId,
        payload: &ShardPayload<I>,
    ) -> Result<(), RebuildPayloadError> {
        let (consumer_dict, entries) = Self::rebuild_entries_from_payload(payload)?;
        let mut partition = TablePartition::new(table_id);
        partition.add_batch(&entries);
        self.replace_table_state(table_id, partition, consumer_dict, &entries);
        Ok(())
    }

    /// Load shard from disk into partition
    #[cfg(feature = "std")]
    fn load_shard(&mut self, table_id: TableId, path: &Path) -> Result<(), StorageError> {
        let bytes = std::fs::read(path)
            .map_err(|e| StorageError::Io(format!("Failed to read shard: {e}")))?;

        let (header, payload) = deserialize_shard::<I, DB>(&bytes, &self.database)?;
        if header.table_id != table_id {
            return Err(StorageError::Corrupt(format!(
                "Shard table ID mismatch: filename table_id {table_id}, header table_id {}",
                header.table_id
            )));
        }

        self.rebuild_and_replace_table_state(table_id, &payload)
            .map_err(|e| match e {
                RebuildPayloadError::Codec(msg) => StorageError::Codec(msg),
                RebuildPayloadError::Corrupt(msg) => StorageError::Corrupt(msg),
            })?;

        Ok(())
    }

    #[cfg(feature = "std")]
    fn parse_table_id_from_shard_path(path: &Path) -> Option<Result<TableId, String>> {
        if path.extension().and_then(|s| s.to_str()) != Some("shard") {
            return None;
        }

        let filename = path.file_stem().and_then(|s| s.to_str())?;
        let table_id_str = filename.strip_prefix("table_")?;
        Some(
            table_id_str
                .parse::<TableId>()
                .map_err(|_| "invalid table id".to_string()),
        )
    }

    // Not `const fn`: the observability branch calls tracing::warn! which is not const.
    #[cfg(feature = "std")]
    #[allow(clippy::missing_const_for_fn)]
    fn log_ignored_shard_filename(path: &Path, reason: &str) {
        #[cfg(feature = "observability")]
        tracing::warn!(
            "Ignoring malformed shard filename '{}': {}",
            path.display(),
            reason
        );
        #[cfg(not(feature = "observability"))]
        let _ = (path, reason);
    }

    /// Load all shards from storage directory
    #[cfg(feature = "std")]
    fn load_all_shards(&mut self) -> Result<(), StorageError> {
        let storage_path = self
            .storage_path
            .as_ref()
            .ok_or_else(|| StorageError::Config("No storage path configured".to_string()))?;

        // Read all .shard files (directory must exist, with_storage creates it)
        let entries = std::fs::read_dir(storage_path)
            .map_err(|e| StorageError::Io(format!("Failed to read storage directory: {e}")))?;

        let mut shard_files: Vec<(TableId, PathBuf)> = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|e| StorageError::Io(format!("Failed to read directory entry: {e}")))?;
            let path = entry.path();
            match Self::parse_table_id_from_shard_path(&path) {
                None => {
                    if path.extension().and_then(|s| s.to_str()) == Some("shard") {
                        Self::log_ignored_shard_filename(&path, "expected format table_<id>.shard");
                    }
                }
                Some(Ok(table_id)) => shard_files.push((table_id, path)),
                Some(Err(reason)) => Self::log_ignored_shard_filename(&path, &reason),
            }
        }

        shard_files.sort_by(|(_, left), (_, right)| left.cmp(right));

        let mut seen_tables: HashMap<TableId, PathBuf> = HashMap::new();
        for (table_id, path) in &shard_files {
            if let Some(first_path) = seen_tables.insert(*table_id, path.clone()) {
                return Err(StorageError::Corrupt(format!(
                    "duplicate shard table id {table_id} in '{}' and '{}'",
                    first_path.display(),
                    path.display()
                )));
            }
        }

        for (table_id, path) in shard_files {
            self.load_shard(table_id, &path)?;
        }

        Ok(())
    }

    /// Check if rotation is needed for a table
    #[cfg(feature = "std")]
    fn should_rotate(&self, table_id: TableId) -> Result<bool, StorageError> {
        let partition = self
            .partitions
            .get(&table_id)
            .ok_or_else(|| StorageError::Corrupt(format!("Unknown table ID: {table_id}")))?;

        let snapshot = partition.load_snapshot();

        // Estimate size (rough approximation)
        let estimated_size = snapshot.predicates.predicates.len() * 1024 + // ~1KB per predicate (bytecode + metadata)
            snapshot.predicates.bindings.len() * 128; // ~128B per binding

        Ok(estimated_size > self.rotation_threshold)
    }

    /// Set rotation threshold
    pub const fn set_rotation_threshold(&mut self, threshold: usize) {
        self.rotation_threshold = threshold;
    }

    /// Set durability mode for registration-time persistence.
    #[cfg(feature = "std")]
    pub const fn set_durability_mode(&mut self, mode: DurabilityMode) {
        self.durability_mode = mode;
    }

    /// Get current rotation threshold
    #[must_use]
    pub const fn rotation_threshold(&self) -> usize {
        self.rotation_threshold
    }

    /// Get current durability mode.
    #[cfg(feature = "std")]
    #[must_use]
    pub const fn durability_mode(&self) -> DurabilityMode {
        self.durability_mode
    }

    // ========================================================================
    // Merge Methods
    // ========================================================================

    /// Start background merge of shard files for a table
    ///
    /// Reads the given shard files from the storage directory, spawns a background
    /// merge thread, and returns a job ID. Use `try_complete_merge` to poll for
    /// completion and swap the merged shard in.
    #[cfg(feature = "std")]
    pub fn merge_shards_background(
        &mut self,
        table_id: TableId,
        shard_paths: &[PathBuf],
    ) -> Result<MergeJobId, MergeError> {
        // Read shard bytes from disk
        let mut shard_bytes = Vec::with_capacity(shard_paths.len());
        for path in shard_paths {
            let bytes = std::fs::read(path).map_err(|e| {
                MergeError::Storage(StorageError::Io(format!(
                    "Failed to read shard for merge: {e}"
                )))
            })?;
            shard_bytes.push(bytes);
        }

        // Compute the table's fingerprint envelope here (we have the catalog),
        // and hand the worker that small value instead of the whole catalog.
        let fingerprint = crate::persistence::shard::expected_envelope(&self.database, table_id)
            .map_err(MergeError::Storage)?;
        self.merge_manager
            .merge_shards_background(table_id, shard_bytes, fingerprint)
    }

    /// Poll for merge completion and swap the result into the live partition
    ///
    /// Returns `Some(report)` if the merge finished and was swapped in,
    /// `None` if still running.
    #[cfg(feature = "std")]
    pub fn try_complete_merge(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, MergeError> {
        let Some(mut merged) = self.merge_manager.try_get_result(job_id)? else {
            return Ok(None);
        };

        let had_live_table_state = self.partitions.contains_key(&merged.table_id)
            || self.consumer_dictionaries.contains_key(&merged.table_id);
        if had_live_table_state {
            let live_subscriptions: HashSet<SubscriptionId> = self
                .partitions
                .get(&merged.table_id)
                .map(|partition| {
                    let snapshot = partition.load_snapshot();
                    snapshot.predicates.bindings.keys().copied().collect()
                })
                .unwrap_or_default();

            merged
                .payload
                .bindings
                .retain(|binding| live_subscriptions.contains(&binding.subscription_id));

            let merged_subscriptions: HashSet<SubscriptionId> = merged
                .payload
                .bindings
                .iter()
                .map(|binding| binding.subscription_id)
                .collect();
            let missing_count = live_subscriptions
                .iter()
                .filter(|sub_id| !merged_subscriptions.contains(*sub_id))
                .count();
            if missing_count > 0 {
                return Err(MergeError::BuildFailed(format!(
                    "merge payload missing live subscriptions for table {}: {} missing live subscriptions",
                    merged.table_id, missing_count
                )));
            }
        }

        self.rebuild_and_replace_table_state(merged.table_id, &merged.payload)
            .map_err(|e| MergeError::BuildFailed(e.to_string()))?;

        Ok(Some(merged.stats.into()))
    }

    /// Get number of active merge jobs
    #[cfg(feature = "std")]
    #[must_use]
    pub fn active_merge_jobs(&self) -> usize {
        self.merge_manager.active_jobs()
    }
}

impl<E, I, DB> SubscriptionRegistration<I, E::Backend> for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    fn register(
        &mut self,
        spec: SubscriptionRequest<I, E::Backend>,
    ) -> Result<RegisterResult, RegisterError> {
        Self::register(self, spec)
    }

    fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        Self::unregister_subscription(self, subscription_id)
    }
}

impl<E, I, DB> SubscriptionDispatch<I, E> for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    type Notifications = crate::ConsumerNotifications<I, E::Checkpoint>;
    type Error = DispatchError;

    fn consumers(&mut self, event: &E) -> Result<Self::Notifications, Self::Error> {
        Self::consumers(self, event)
    }
}

impl<E, I, DB> SubscriptionUnregistration<I> for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    fn unregister_session(&mut self, session_id: I::SessionId) -> UnregisterReport {
        Self::unregister_session(self, session_id)
    }

    fn unregister_query(
        &mut self,
        consumer_id: I::ConsumerId,
        sql: &str,
    ) -> Result<UnregisterReport, RegisterError> {
        Self::unregister_query(self, consumer_id, sql)
    }
}

impl<E, I, DB> crate::AggregateDispatch<I, E> for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    fn aggregate_deltas(
        &mut self,
        event: &E,
    ) -> Result<Vec<(I::ConsumerId, crate::AggDelta)>, DispatchError> {
        Self::aggregate_deltas(self, event)
    }
}

#[cfg(feature = "std")]
impl<E, I, DB> DurableShardStore for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    fn snapshot_table(&self, table_id: TableId) -> Result<(), StorageError> {
        Self::snapshot_table(self, table_id)
    }
}

#[cfg(feature = "std")]
impl<E, I, DB> DurableShardMerge for SubscriptionEngine<E, I, DB>
where
    E: CdcEvent + Send + Sync,
    E::Backend: SqlLiteralParse,
    <E::Backend as Backend>::Dialect: Send + Sync,
    I: IdTypes,
    DB: DatabaseLike + Send + Sync + 'static,
{
    fn merge_shards_background(
        &mut self,
        table_id: TableId,
        shard_paths: &[PathBuf],
    ) -> Result<MergeJobId, MergeError> {
        Self::merge_shards_background(self, table_id, shard_paths)
    }

    fn try_complete_merge(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, MergeError> {
        Self::try_complete_merge(self, job_id)
    }
}

// Test body deferred to Phase 10 per docs/refactor-cdc-event-handoff.md.
