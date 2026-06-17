//! Subscription engine - main public API

use super::indexes::IndexableAtom;
use super::{
    dispatch::{
        dispatch_consumers, dispatch_consumers_with_stamps, select_event_row, ConsumerDictionary,
    },
    ids::{ConsumerOrdinal, PredicateId},
    partition::TablePartition,
    predicate::{Predicate, SubscriptionBinding},
};
use crate::{
    catalog_helpers,
    compiler::{
        canonicalize, parse_and_resolve_hash, parse_compile_normalize_and_prefilter,
        sql_shape::{AggSpec, QueryProjection},
        BytecodeProgram, PrefilterPlan, Vm,
    },
    DispatchError, EventKind, IdTypes, RegisterError, RegisterResult, RowImage,
    SubscriptionDispatch, SubscriptionId, SubscriptionRegistration, SubscriptionRequest,
    SubscriptionScope, SubscriptionUnregistration, TableId, UnregisterReport, WalEvent,
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
use sqlparser::dialect::Dialect;
#[cfg(feature = "std")]
use std::io::Write;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

type BatchEntries<I> = Vec<(Predicate, Vec<IndexableAtom>, Vec<SubscriptionBinding<I>>)>;

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
/// `BuiltIn` wraps the user-facing [`crate::EvictionPolicy`] enum;
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

struct CompiledSpec<I: IdTypes> {
    spec: SubscriptionRequest<I>,
    table_id: TableId,
    bytecode: BytecodeProgram,
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
pub struct SubscriptionEngine<D: Dialect, I: IdTypes, DB: DatabaseLike> {
    /// SQL dialect for parsing
    dialect: D,
    /// Schema database for table/column resolution
    database: Arc<DB>,
    /// Table partitions (TableId → TablePartition)
    partitions: HashMap<TableId, TablePartition<I>>,
    /// User dictionaries (TableId → ConsumerDictionary)
    consumer_dictionaries: HashMap<TableId, ConsumerDictionary<I>>,
    /// Subscription index for O(1) unregister/upsert lookup.
    subscription_to_table: HashMap<SubscriptionId, TableId>,
    /// Monotonic counter for auto-assigning subscription IDs (starts at 1).
    next_subscription_id: u64,
    /// Dedup index: (consumer_id, predicate_hash, scope) → existing SubscriptionId.
    binding_dedup: HashMap<(I::ConsumerId, u128, SubscriptionScope<I>), SubscriptionId>,
    /// VM for bytecode evaluation
    vm: Vm,
    /// Optional cap on the number of live subscriptions. `None` means no
    /// cap; the registry can grow unbounded.
    max_subscriptions: Option<usize>,
    /// Eviction strategy applied when [`max_subscriptions`](Self::max_subscriptions)
    /// would be exceeded. Default `BuiltIn(EvictionPolicy::Reject)`.
    eviction_strategy: EvictionStrategy<I>,
    /// Monotonic clock used by activity-aware eviction policies to stamp
    /// `last_dispatch_at`. Lazy-initialised to a `StdClock` the first time
    /// an activity-aware policy is configured; remains `None` for the
    /// default (no cap) and for non-activity policies so dispatch stays
    /// allocation-free.
    activity_clock: Option<crate::ClockHandle>,
    /// Per-subscription activity counters. Populated only when an
    /// activity-aware policy is configured; empty otherwise so dispatch
    /// pays no cost.
    subscription_activity: HashMap<SubscriptionId, ActivityStats>,
    /// Optional storage path for durability
    #[cfg(feature = "std")]
    storage_path: Option<PathBuf>,
    /// Shard rotation threshold (bytes)
    rotation_threshold: usize,
    /// Background merge compaction manager
    #[cfg(feature = "std")]
    merge_manager: MergeManager<I, DB>,
    /// Persistence strictness policy for registration.
    #[cfg(feature = "std")]
    durability_mode: DurabilityMode,
    /// Per-`(session, subscription)` resume cursor.
    ///
    /// Stored as an [`OpaqueCheckpoint`] so the engine does not have to
    /// carry a struct-level `C: Checkpoint` generic; the materializer
    /// serialises its concrete checkpoint type (`PgLsn`,
    /// `MysqlBinlogPos`, ...) to bytes before installing, deserialises
    /// on read. The map's cleanup is hooked into
    /// [`unregister_session`](Self::unregister_session) and the
    /// per-subscription unregister paths so cursors never outlive
    /// their owning subscription.
    ///
    /// See `connetto-rs` Q6.4 for the underlying delegation.
    resume_cursors: HashMap<(I::SessionId, SubscriptionId), crate::OpaqueCheckpoint>,
}

/// Look up the partition and consumer dictionary for a table, or return
/// `DispatchError::UnknownTableId` if either is missing.
fn table_context<'a, I: IdTypes>(
    partitions: &'a HashMap<TableId, TablePartition<I>>,
    consumer_dicts: &'a HashMap<TableId, ConsumerDictionary<I>>,
    table_id: TableId,
) -> Result<(&'a TablePartition<I>, &'a ConsumerDictionary<I>), DispatchError> {
    let partition = partitions
        .get(&table_id)
        .ok_or(DispatchError::UnknownTableId(table_id))?;
    let consumer_dict = consumer_dicts
        .get(&table_id)
        .ok_or(DispatchError::UnknownTableId(table_id))?;
    Ok((partition, consumer_dict))
}

impl<D: Dialect, I: IdTypes, DB: DatabaseLike + 'static> SubscriptionEngine<D, I, DB> {
    /// The SQL dialect used for parsing. Exposed for the reexec wrapper, which
    /// re-parses queries the engine rejects.
    pub(crate) const fn dialect(&self) -> &D {
        &self.dialect
    }

    /// The schema database used for table/column resolution. Exposed for the
    /// reexec wrapper's classification of rejected queries.
    pub(crate) fn database(&self) -> &DB {
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

    fn compile_spec(&self, spec: SubscriptionRequest<I>) -> Result<CompiledSpec<I>, RegisterError> {
        let (table_id, bytecode, normalized, prefilter_plan, projection) =
            parse_compile_normalize_and_prefilter(&spec.sql, &self.dialect, &*self.database)?;

        // Disambiguate hash: same WHERE clause with different projection kind must map to
        // distinct predicates.
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

    fn make_predicate_from_compiled(compiled: &CompiledSpec<I>) -> (Predicate, Vec<IndexableAtom>) {
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
            // Placeholder; store allocates the authoritative ID.
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
        spec: &SubscriptionRequest<I>,
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
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{DefaultIds, SubscriptionEngine};
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE orders (id INT);")
    ///         .expect("DDL parses"),
    /// );
    ///
    /// let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// assert_eq!(engine.subscription_count(), 0);
    /// ```
    #[must_use]
    pub fn new(database: Arc<DB>, dialect: D) -> Self {
        Self {
            dialect,
            database,
            partitions: HashMap::new(),
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
        }
    }

    /// Configure a maximum number of live subscriptions and the built-in
    /// policy applied when a registration would exceed it.
    ///
    /// By default the registry is uncapped. With [`crate::EvictionPolicy::Reject`]
    /// the cap is hard: `register` returns [`RegisterError::RegistryFull`]
    /// once the registry is full. With [`crate::EvictionPolicy::EvictOldest`]
    /// the oldest subscription (lowest [`SubscriptionId`]) is evicted to
    /// make room; activity-aware policies such as
    /// [`crate::EvictionPolicy::EvictLeastActive`] /
    /// [`crate::EvictionPolicy::EvictColdest`] additionally enable
    /// per-subscription dispatch stamping; topological policies such as
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
    /// The closure runs synchronously on the registration path; keep it
    /// cheap. It must be `Send + Sync + 'static` so the engine remains
    /// `Send + Sync` whenever `I` is.
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest,
    ///     SubscriptionsView,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
    ///     )?,
    /// );
    /// // Evict the subscription belonging to the lowest consumer id.
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
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
    /// `ManualClock`; production builds should rely on the default
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
        database: Arc<DB>,
        dialect: D,
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
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    ///     )?,
    /// );
    /// let orders_id = catalog_helpers::table_id(&*database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
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
        spec: SubscriptionRequest<I>,
    ) -> Result<RegisterResult, RegisterError> {
        // 1. Parse, compile, and canonicalize in one pass.
        let compiled = self.compile_spec(spec)?;
        let table_id = compiled.table_id;
        let hash = compiled.hash;

        // 2. Check dedup index: same (consumer_id, predicate_hash, scope) → idempotent return.
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
    /// sequential `register()` loop to maintain parity; the bulk-COW
    /// fast path applies to the no-cap and `Reject`-cap cases.
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    ///     )?,
    /// );
    /// let orders_id = catalog_helpers::table_id(&*database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
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
        specs: Vec<SubscriptionRequest<I>>,
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
        let mut compiled: Vec<Option<CompiledSpec<I>>> = Vec::with_capacity(specs.len());
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
                    // batch; record so subsequent duplicates can defer
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
        let mut table_entries: HashMap<TableId, BatchEntries<I>> = HashMap::new();
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
                        // Defensive: victim disappeared; do not loop.
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
                // immediately below — does NOT add to pending_uncommitted.
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
    // Per-`(session, subscription)` resume cursor API. See
    // `connetto-rs` Q6.4 (`docs/architecture/open-questions.md`) for
    // the underlying contract. The cursor is the position the
    // materializer last successfully dispatched to the client; on
    // reconnect the materializer compares it against its own oplog
    // watermark to decide catchup vs full re-sync.
    // ----------------------------------------------------------------

    /// Advance the resume cursor for `(session_id, sub_id)` to
    /// `checkpoint`.
    ///
    /// Returns `Ok(None)` when no cursor was previously stored for the
    /// pair (i.e. this is the first advance), and `Ok(Some(previous))`
    /// when an existing cursor was overwritten.
    ///
    /// Returns [`AdvanceCursorError::NonMonotonic`] when `checkpoint`
    /// is strictly less than the stored value (a rewind) and **does
    /// not** mutate the map. A successful dispatch never moves a
    /// client backwards in the CDC stream, so a rewind is always a
    /// caller bug; use [`force_set_cursor`](Self::force_set_cursor)
    /// for the legitimate snapshot-bootstrap / recovery escape hatch.
    ///
    /// # Errors
    ///
    /// [`AdvanceCursorError::NonMonotonic`] when `checkpoint < previous`.
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
    /// session" path; routine cleanup happens automatically via
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
    /// subscription. Allocates one `Vec` per call; only invoked from the
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
                // Largest holder wins; ties resolve by lowest consumer id
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

            // Stale index entry; clean it up and fall back to scan.
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
    /// use subql::{
    ///     catalog_helpers, Cell, DefaultIds, RowImage, SubscriptionEngine,
    ///     SubscriptionRequest, WalEvent,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    ///     )?,
    /// );
    /// let orders_id = catalog_helpers::table_id(&*database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// engine.register(
    ///     SubscriptionRequest::new(42, "SELECT * FROM orders WHERE amount > 100")
    ///         .updated_at_unix_ms(1_704_067_200_000),
    /// )?;
    ///
    /// let event = WalEvent::builder(orders_id)
    ///     .insert()
    ///     .new_row(RowImage {
    ///         cells: Arc::from([Cell::Int(1), Cell::Int(250), Cell::String("paid".into())]),
    ///     })
    ///     .build()?;
    ///
    /// let notifs = engine.consumers(&event)?;
    /// assert_eq!(notifs.inserted(), vec![42]);
    /// assert!(notifs.deleted().is_empty());
    /// assert!(notifs.updated().is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn consumers<C: crate::Checkpoint>(
        &mut self,
        event: &WalEvent<C>,
    ) -> Result<crate::ConsumerNotifications<I, C>, DispatchError> {
        // Activity-aware policies stamp matched subscriptions through the
        // `_with_stamps` dispatch variant. Default (no activity tracking)
        // takes the cheaper path that allocates no stamp vector.
        let needs_stamps = self.eviction_strategy.needs_activity_tracking();

        let (partition, consumer_dict) = table_context(
            &self.partitions,
            &self.consumer_dictionaries,
            event.table_id(),
        )?;

        if event.kind() == EventKind::Truncate {
            if needs_stamps {
                let (notifs, stamps) =
                    dispatch_consumers_with_stamps(event, partition, consumer_dict, &mut self.vm)?;
                self.stamp_activity(&stamps);
                return Ok(notifs);
            }
            return dispatch_consumers(event, partition, consumer_dict, &mut self.vm);
        }

        // Validate row image arity against schema database.
        // For UPDATE events, validate both old_row and new_row.
        if let Some(old_row) = event.old_row() {
            self.validate_row_arity(event.table_id(), old_row)?;
        }
        if event.kind() != EventKind::Delete {
            if let Some(new_row) = event.new_row() {
                self.validate_row_arity(event.table_id(), new_row)?;
            }
        }
        if event.kind() == EventKind::Delete {
            let row = select_event_row(event)?;
            self.validate_row_arity(event.table_id(), row)?;
        }

        let (partition, consumer_dict) = table_context(
            &self.partitions,
            &self.consumer_dictionaries,
            event.table_id(),
        )?;

        if needs_stamps {
            let (notifs, stamps) =
                dispatch_consumers_with_stamps(event, partition, consumer_dict, &mut self.vm)?;
            self.stamp_activity(&stamps);
            Ok(notifs)
        } else {
            dispatch_consumers(event, partition, consumer_dict, &mut self.vm)
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

    /// Compute typed signed deltas for aggregate subscriptions (COUNT(*), SUM(col), …).
    ///
    /// Returns `Vec<(ConsumerId, AggDelta)>`` where each entry is the net signed change
    /// for that consumer's aggregate predicate. Zero-net entries are omitted.
    /// The same consumer may appear multiple times (once per aggregate kind).
    ///
    /// # Caller contract
    /// - Bootstrap: query the DB for the initial aggregate **before** subscribing.
    /// - Accumulate: `running_value += delta` on each call.
    /// - UPDATE image requirement: aggregate UPDATE deltas require both
    ///   `old_row` and `new_row`; when CDC omits old images, this API returns
    ///   `DispatchError::AggregateUpdateRequiresOldRow`.
    /// - Reset on policy change: RLS/ACL changes produce no WAL events;
    ///   re-query the DB and replace the stored value.
    /// - Reset on TRUNCATE: engine returns `Err(TruncateRequiresReset)`;
    ///   caller must re-query and replace.
    ///
    /// # Examples
    /// ```
    /// use std::sync::Arc;
    ///
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::{
    ///     catalog_helpers, AggDelta, Cell, DefaultIds, RowImage, SubscriptionEngine,
    ///     SubscriptionRequest, WalEvent,
    /// };
    ///
    /// let database = Arc::new(
    ///     ParserDB::parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
    ///     )?,
    /// );
    /// let orders_id = catalog_helpers::table_id(&*database, "orders").unwrap();
    /// let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// engine.register(
    ///     SubscriptionRequest::new(99, "SELECT COUNT(*) FROM orders WHERE status = 'paid'")
    ///         .updated_at_unix_ms(1_704_067_200_000),
    /// )?;
    ///
    /// let event = WalEvent::builder(orders_id)
    ///     .insert()
    ///     .new_row(RowImage {
    ///         cells: Arc::from([Cell::Int(1), Cell::String("paid".into())]),
    ///     })
    ///     .build()?;
    ///
    /// let deltas = engine.aggregate_deltas(&event)?;
    /// assert_eq!(deltas, vec![(99, AggDelta::Count(1))]);
    ///
    /// // Aggregate subscriptions are handled by `aggregate_deltas()`, not `consumers()`.
    /// let notifs = engine.consumers(&event)?;
    /// assert!(notifs.inserted().is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn aggregate_deltas<C: crate::Checkpoint>(
        &mut self,
        event: &WalEvent<C>,
    ) -> Result<Vec<(I::ConsumerId, crate::AggDelta)>, DispatchError> {
        let (partition, consumer_dict) = table_context(
            &self.partitions,
            &self.consumer_dictionaries,
            event.table_id(),
        )?;

        // Validate all row images consumed by aggregate dispatch.
        if let Some(old_row) = event.old_row() {
            self.validate_row_arity(event.table_id(), old_row)?;
        }
        if let Some(new_row) = event.new_row() {
            self.validate_row_arity(event.table_id(), new_row)?;
        }

        super::dispatch::compute_agg_deltas(event, partition, consumer_dict, &mut self.vm)
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
        let (table_id, hash) = parse_and_resolve_hash(sql, &self.dialect, &*self.database)?;

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

        // Check if the consumer still has any bindings in this table; if not, clean up.
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

    /// Validate that a row image has the expected arity for its table.
    fn validate_row_arity(&self, table_id: TableId, row: &RowImage) -> Result<(), DispatchError> {
        let expected = catalog_helpers::table_arity(&*self.database, table_id)
            .ok_or(DispatchError::UnknownTableArity(table_id))?;
        let got = row.len();
        if got != expected {
            return Err(DispatchError::InvalidRowArity {
                table_id,
                expected,
                got,
            });
        }
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
        let bytes = serialize_shard::<I, DB>(table_id, &payload, &*self.database)?;

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
    ) -> Result<(ConsumerDictionary<I>, BatchEntries<I>), RebuildPayloadError> {
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

        let mut entries: BatchEntries<I> = Vec::new();
        for pred_data in chosen_predicates {
            let Some(bindings) = bindings_by_hash.remove(&pred_data.hash) else {
                continue;
            };

            let bytecode: BytecodeProgram = codec::deserialize(&pred_data.bytecode_instructions)
                .map_err(|e| {
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
        partition: TablePartition<I>,
        consumer_dict: ConsumerDictionary<I>,
        entries: &BatchEntries<I>,
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

        let (header, payload) = deserialize_shard::<I, DB>(&bytes, &*self.database)?;
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

        // Read all .shard files (directory must exist — with_storage creates it)
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

        // Delegate to merge manager
        self.merge_manager.merge_shards_background(
            table_id,
            shard_bytes,
            Arc::clone(&self.database),
        )
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

impl<D: Dialect + Send + Sync, I: IdTypes, DB: DatabaseLike + 'static> SubscriptionRegistration<I>
    for SubscriptionEngine<D, I, DB>
{
    fn register(&mut self, spec: SubscriptionRequest<I>) -> Result<RegisterResult, RegisterError> {
        Self::register(self, spec)
    }

    fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool {
        Self::unregister_subscription(self, subscription_id)
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes, DB: DatabaseLike + 'static> SubscriptionDispatch<I>
    for SubscriptionEngine<D, I, DB>
{
    fn consumers(
        &mut self,
        event: &WalEvent,
    ) -> Result<crate::ConsumerNotifications<I>, DispatchError> {
        Self::consumers(self, event)
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes, DB: DatabaseLike + 'static> SubscriptionUnregistration<I>
    for SubscriptionEngine<D, I, DB>
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

impl<D: Dialect + Send + Sync, I: IdTypes, DB: DatabaseLike + 'static> crate::AggregateDispatch<I>
    for SubscriptionEngine<D, I, DB>
{
    fn aggregate_deltas(
        &mut self,
        event: &WalEvent,
    ) -> Result<Vec<(I::ConsumerId, crate::AggDelta)>, DispatchError> {
        Self::aggregate_deltas(self, event)
    }
}

#[cfg(feature = "std")]
impl<D: Dialect + Send + Sync, I: IdTypes, DB: DatabaseLike + 'static> DurableShardStore
    for SubscriptionEngine<D, I, DB>
{
    fn snapshot_table(&self, table_id: TableId) -> Result<(), StorageError> {
        Self::snapshot_table(self, table_id)
    }
}

#[cfg(feature = "std")]
impl<D: Dialect + Send + Sync, I: IdTypes, DB: DatabaseLike + 'static> DurableShardMerge
    for SubscriptionEngine<D, I, DB>
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::uninlined_format_args,
    clippy::clone_on_ref_ptr,
    clippy::redundant_clone,
    clippy::needless_collect
)]
mod tests {
    use super::*;
    use crate::catalog_helpers;
    use crate::{Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SubscriptionScope};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::path::{Path, PathBuf};

    /// Fixture catalog for engine tests: one table `orders` with three columns
    /// (`id` PK, `amount` INT, `status` TEXT). Padding tables ensure
    /// `orders` is assigned table id `1` and a second known table is at
    /// id `2`, matching tests that serialize shards with header
    /// `table_id = 2` to exercise filename/header mismatch paths.
    fn make_catalog() -> Arc<ParserDB> {
        Arc::new(
            ParserDB::parse::<PostgreSqlDialect>(
                "CREATE TABLE _engine_pad (id INT);\n\
                 CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);\n\
                 CREATE TABLE z_other (id INT PRIMARY KEY);",
            )
            .expect("orders fixture DDL parses"),
        )
    }

    /// Helper: orders table_id for the standard fixture. With the padded
    /// `make_catalog()` this is `1`; using the helper instead of a literal
    /// keeps tests resilient if the padding strategy changes.
    #[allow(dead_code)]
    fn orders_tid(catalog: &ParserDB) -> TableId {
        catalog_helpers::table_id(catalog, "orders").expect("orders id")
    }

    fn build_insert_event(table_id: TableId, pk: PrimaryKey, new_row: RowImage) -> WalEvent {
        WalEvent::builder(table_id)
            .insert()
            .pk(pk)
            .new_row(new_row)
            .build()
            .expect("insert event builder should be valid")
    }

    fn build_delete_event(table_id: TableId, pk: PrimaryKey, old_row: RowImage) -> WalEvent {
        WalEvent::builder(table_id)
            .delete()
            .pk(pk)
            .old_row(old_row)
            .build()
            .expect("delete event builder should be valid")
    }

    #[cfg(unix)]
    fn set_dir_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(mode);
        std::fs::set_permissions(path, perms).unwrap();
    }

    struct ParentDirSyncFailureGuard {
        dir: PathBuf,
    }

    impl ParentDirSyncFailureGuard {
        fn for_dir(dir: &Path) -> Self {
            let dir = dir.to_path_buf();
            let lock = injected_parent_dir_sync_failure_dirs();
            match lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
            .insert(dir.clone());
            Self { dir }
        }
    }

    impl Drop for ParentDirSyncFailureGuard {
        fn drop(&mut self) {
            let lock = injected_parent_dir_sync_failure_dirs();
            let mut guard = match lock.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.remove(&self.dir);
        }
    }

    struct BatchPhase3PartitionDropGuard {
        table_id: TableId,
    }

    impl BatchPhase3PartitionDropGuard {
        fn for_table(table_id: TableId) -> Self {
            INJECT_BATCH_PHASE3_PARTITION_DROP_TABLES.with(|set| set.borrow_mut().insert(table_id));
            Self { table_id }
        }
    }

    impl Drop for BatchPhase3PartitionDropGuard {
        fn drop(&mut self) {
            INJECT_BATCH_PHASE3_PARTITION_DROP_TABLES
                .with(|set| set.borrow_mut().remove(&self.table_id));
        }
    }

    struct CompileHashOverrideGuard {
        normalized_keys: Vec<String>,
    }

    impl CompileHashOverrideGuard {
        fn force(hash_by_normalized: Vec<(String, u128)>) -> Self {
            let mut normalized_keys = Vec::with_capacity(hash_by_normalized.len());
            with_injected_compile_hash_overrides(|overrides| {
                for (normalized, hash) in hash_by_normalized {
                    overrides.insert(normalized.clone(), hash);
                    normalized_keys.push(normalized);
                }
            });

            Self { normalized_keys }
        }
    }

    impl Drop for CompileHashOverrideGuard {
        fn drop(&mut self) {
            with_injected_compile_hash_overrides(|overrides| {
                for normalized in &self.normalized_keys {
                    overrides.remove(normalized);
                }
            });
        }
    }

    static INJECTION_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn injection_test_lock() -> &'static Mutex<()> {
        INJECTION_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn test_engine_creation() {
        let catalog = make_catalog();
        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_register_subscription() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let spec = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let result = engine.register(spec);
        assert!(result.is_ok());

        let reg = result.unwrap();
        assert_eq!(reg.table_id, 1);
        assert!(reg.created_new_predicate);
        assert!(reg.evicted.is_empty(), "no eviction expected without a cap");
    }

    /// T5.1: `EvictionPolicy::Reject` rejects registrations past the cap
    /// with `RegisterError::RegistryFull`.
    #[test]
    fn test_register_cap_reject_returns_registry_full() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::Reject);

        assert!(engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .is_ok());
        assert!(engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .is_ok());

        // Third registration is past the cap.
        match engine.register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM orders WHERE amount > 3",
        )) {
            Err(RegisterError::RegistryFull { cap }) => assert_eq!(cap, 2),
            other => panic!("expected RegistryFull, got {other:?}"),
        }

        // Idempotent re-registration of an existing subscription does NOT
        // count against the cap (no new subscription slot allocated).
        let again = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("idempotent re-register must succeed");
        assert!(again.evicted.is_empty());
    }

    /// T5.2: `EvictionPolicy::EvictOldest` makes room by evicting the
    /// oldest subscription (lowest `SubscriptionId`) and surfaces the
    /// evicted id in `RegisterResult::evicted`.
    #[test]
    fn test_register_cap_evict_oldest_surfaces_evicted_id() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictOldest);

        let first = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("first register");
        let second = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("second register");
        assert!(first.evicted.is_empty());
        assert!(second.evicted.is_empty());
        assert_eq!(engine.subscription_count(), 2);

        // Third registration triggers eviction of `first`.
        let third = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 3",
            ))
            .expect("third register must succeed under EvictOldest");
        assert_eq!(
            third.evicted,
            vec![first.subscription_id],
            "oldest subscription must be evicted"
        );
        assert_eq!(engine.subscription_count(), 2, "registry size stays at cap");
    }

    /// T5.3: `EvictLeastActive` evicts the subscription with the oldest
    /// `last_dispatch_at` (or `None`, treated as -infinity) once the cap
    /// is full. A `ManualClock` makes the test deterministic.
    #[test]
    fn test_register_cap_evict_least_active() {
        use crate::ManualClock;
        use core::time::Duration;

        let catalog = make_catalog();
        let orders_id = orders_tid(&catalog);
        let clock = Arc::new(ManualClock::new(0));
        let handle: crate::ClockHandle = clock.clone();

        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictLeastActive)
                .with_activity_clock(handle);

        let first = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("first register");
        // Match first: dispatch an event whose row beats amount > 1.
        clock.advance(Duration::from_micros(10));
        let event_a = build_insert_event(
            orders_id,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(5), Cell::String("p".into())]),
            },
        );
        engine.consumers(&event_a).expect("dispatch first");

        let second = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 100",
            ))
            .expect("second register");
        clock.advance(Duration::from_micros(20));
        let event_b = build_insert_event(
            orders_id,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(2)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(2), Cell::Int(500), Cell::String("p".into())]),
            },
        );
        engine.consumers(&event_b).expect("dispatch second");

        // Both at the cap. `first` was stamped at t=10, `second` at t=30.
        // Third registration evicts `first` (least active).
        let third = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 999",
            ))
            .expect("third register under EvictLeastActive");
        assert_eq!(
            third.evicted,
            vec![first.subscription_id],
            "the least recently active subscription is evicted"
        );
        // `second` survives.
        assert!(engine
            .subscription_to_table
            .contains_key(&second.subscription_id));
    }

    /// T5.4: `EvictColdest` evicts the subscription with the lowest
    /// `dispatch_count`. Tie-break: oldest `SubscriptionId`.
    #[test]
    fn test_register_cap_evict_coldest() {
        let catalog = make_catalog();
        let orders_id = orders_tid(&catalog);

        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictColdest);

        let hot = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 0",
            ))
            .expect("register hot");
        let cold = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 1000",
            ))
            .expect("register cold");

        // Dispatch five events that match only `hot`.
        for i in 0..5 {
            let event = build_insert_event(
                orders_id,
                PrimaryKey {
                    columns: Arc::from([0u16]),
                    values: Arc::from([Cell::Int(i)]),
                },
                RowImage {
                    cells: Arc::from([Cell::Int(i), Cell::Int(5), Cell::String("p".into())]),
                },
            );
            engine.consumers(&event).expect("dispatch hot");
        }

        let third = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 7",
            ))
            .expect("third register under EvictColdest");
        assert_eq!(
            third.evicted,
            vec![cold.subscription_id],
            "the coldest subscription (lowest dispatch_count) is evicted"
        );
        assert!(engine
            .subscription_to_table
            .contains_key(&hot.subscription_id));
    }

    /// T5.5: `EvictBySession` prefers session-scoped subscriptions over
    /// durable ones.
    #[test]
    fn test_register_cap_evict_by_session_prefers_session_scope() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictBySession);

        let durable = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("register durable");
        let session = engine
            .register(
                SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE amount > 2")
                    .scope(SubscriptionScope::Session(7777)),
            )
            .expect("register session");

        let third = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 3",
            ))
            .expect("third register under EvictBySession");

        assert_eq!(
            third.evicted,
            vec![session.subscription_id],
            "session-scoped subscription must be evicted before durable"
        );
        assert!(engine
            .subscription_to_table
            .contains_key(&durable.subscription_id));
    }

    /// T5.5b: when no session-scoped subscriptions exist, `EvictBySession`
    /// falls back to `EvictOldest` behavior.
    #[test]
    fn test_register_cap_evict_by_session_falls_back_to_oldest_when_all_durable() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictBySession);

        let first = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("first");
        let _second = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("second");

        let third = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 3",
            ))
            .expect("third");
        assert_eq!(
            third.evicted,
            vec![first.subscription_id],
            "with no session subscriptions, the oldest durable is evicted"
        );
    }

    /// T5.6: `EvictByConsumer` evicts from the consumer holding the most
    /// live subscriptions ("biggest hog"). Tie-break: oldest sub id.
    #[test]
    fn test_register_cap_evict_by_consumer_picks_biggest_hog() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(3, crate::EvictionPolicy::EvictByConsumer);

        let hog_a = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("hog A");
        let _hog_b = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("hog B");
        let lone = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 3",
            ))
            .expect("consumer 2 single");

        // Fourth registration from consumer 3 trips the cap. Consumer 1
        // is the biggest hog (2 subs); its oldest sub (hog_a) is evicted.
        let fourth = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 4",
            ))
            .expect("fourth");
        assert_eq!(
            fourth.evicted,
            vec![hog_a.subscription_id],
            "EvictByConsumer must take the oldest sub from the biggest holder"
        );
        assert!(engine
            .subscription_to_table
            .contains_key(&lone.subscription_id));
    }

    /// T5.6b: `EvictByConsumer` with a single consumer falls back to
    /// `EvictOldest`.
    #[test]
    fn test_register_cap_evict_by_consumer_single_consumer_falls_back_to_oldest() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictByConsumer);

        let first = engine
            .register(SubscriptionRequest::new(
                42u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("first");
        let _second = engine
            .register(SubscriptionRequest::new(
                42u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("second");

        let third = engine
            .register(SubscriptionRequest::new(
                42u64,
                "SELECT * FROM orders WHERE amount > 3",
            ))
            .expect("third");
        assert_eq!(
            third.evicted,
            vec![first.subscription_id],
            "with a single consumer, evict the oldest of that consumer's subs"
        );
    }

    /// T5.7: custom closure drives eviction. The closure receives a
    /// `SubscriptionsView` and returns the subscription id to evict.
    #[test]
    fn test_register_cap_custom_closure_picks_victim() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {}).with_custom_eviction(
                2,
                |view: &crate::SubscriptionsView<'_, DefaultIds>| {
                    view.iter()
                        .max_by_key(|m| m.subscription_id)
                        .map(|m| m.subscription_id)
                },
            );

        let _first = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("first");
        let second = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("second");

        // Custom closure picks the highest sub id (== `second`).
        let third = engine
            .register(SubscriptionRequest::new(
                3u64,
                "SELECT * FROM orders WHERE amount > 3",
            ))
            .expect("third");
        assert_eq!(
            third.evicted,
            vec![second.subscription_id],
            "custom closure must pick the eviction target"
        );
    }

    /// T5.9: `register_batch` honors the registry cap. With
    /// `EvictionPolicy::Reject` the over-cap specs in a batch surface
    /// as per-spec `RegistryFull` errors while earlier specs succeed.
    #[test]
    fn test_register_batch_cap_reject_returns_registry_full_per_spec() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::Reject);

        let specs = vec![
            SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE amount > 1"),
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE amount > 2"),
            SubscriptionRequest::new(3u64, "SELECT * FROM orders WHERE amount > 3"),
            SubscriptionRequest::new(4u64, "SELECT * FROM orders WHERE amount > 4"),
        ];
        let results = engine.register_batch(specs);

        assert!(results[0].is_ok(), "first spec fits under cap");
        assert!(results[1].is_ok(), "second spec hits cap exactly");
        assert!(
            matches!(results[2], Err(RegisterError::RegistryFull { cap: 2 })),
            "third spec is past cap, got {:?}",
            results[2],
        );
        assert!(
            matches!(results[3], Err(RegisterError::RegistryFull { cap: 2 })),
            "fourth spec is past cap, got {:?}",
            results[3],
        );
        assert_eq!(engine.subscription_count(), 2);
    }

    /// T5.10: `register_batch` triggers eviction per over-cap spec under
    /// `EvictOldest`, and surfaces evicted ids in each spec's result.
    #[test]
    fn test_register_batch_cap_evict_oldest_evicts_per_spec() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_max_subscriptions(2, crate::EvictionPolicy::EvictOldest);

        let pre1 = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("pre1");
        let pre2 = engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("pre2");
        assert_eq!(engine.subscription_count(), 2);

        let specs = vec![
            SubscriptionRequest::new(3u64, "SELECT * FROM orders WHERE amount > 3"),
            SubscriptionRequest::new(4u64, "SELECT * FROM orders WHERE amount > 4"),
        ];
        let results = engine.register_batch(specs);

        let r0 = results[0].as_ref().expect("first batch entry");
        let r1 = results[1].as_ref().expect("second batch entry");
        assert_eq!(
            r0.evicted,
            vec![pre1.subscription_id],
            "first over-cap spec evicts the original oldest"
        );
        assert_eq!(
            r1.evicted,
            vec![pre2.subscription_id],
            "second over-cap spec evicts the new oldest"
        );
        assert_eq!(engine.subscription_count(), 2);
    }

    /// T5.11: `register_batch` mixes successful evictions with rejected
    /// specs when the custom closure declines some.
    #[test]
    fn test_register_batch_cap_custom_closure_mixed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {}).with_custom_eviction(
                1,
                |view: &crate::SubscriptionsView<'_, DefaultIds>| {
                    // Evict only when the registry holds an even-id sub.
                    view.iter()
                        .find(|m| m.subscription_id % 2 == 0)
                        .map(|m| m.subscription_id)
                },
            );

        engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("pre");
        // First batch entry hits cap with no even-id sub present, closure
        // returns None → RegistryFull. Second entry would also be rejected.
        let results = engine.register_batch(vec![
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE amount > 2"),
            SubscriptionRequest::new(3u64, "SELECT * FROM orders WHERE amount > 3"),
        ]);
        assert!(matches!(
            results[0],
            Err(RegisterError::RegistryFull { cap: 1 })
        ));
        assert!(matches!(
            results[1],
            Err(RegisterError::RegistryFull { cap: 1 })
        ));
    }

    /// T5.8: a custom closure returning `None` makes registration fall
    /// through to `RegistryFull { cap }`. Allows hybrid policies that
    /// sometimes reject instead of evicting.
    #[test]
    fn test_register_cap_custom_closure_none_rejects() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {})
                .with_custom_eviction(2, |_view: &crate::SubscriptionsView<'_, DefaultIds>| None);

        engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("first");
        engine
            .register(SubscriptionRequest::new(
                2u64,
                "SELECT * FROM orders WHERE amount > 2",
            ))
            .expect("second");

        match engine.register(SubscriptionRequest::new(
            3u64,
            "SELECT * FROM orders WHERE amount > 3",
        )) {
            Err(RegisterError::RegistryFull { cap }) => assert_eq!(cap, 2),
            other => {
                panic!("expected RegistryFull when custom closure returns None, got {other:?}")
            }
        }
    }

    #[test]
    fn test_predicate_deduplication() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register same predicate for two consumers
        let spec1 = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let spec2 = SubscriptionRequest {
            consumer_id: 200,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let result1 = engine.register(spec1).unwrap();
        let result2 = engine.register(spec2).unwrap();

        // Same hash = deduplicated
        assert_eq!(result1.predicate_hash, result2.predicate_hash);
        assert!(result1.created_new_predicate);
        assert!(!result2.created_new_predicate);
    }

    #[test]
    fn test_dispatch_simple() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscription: amount > 100
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        // Dispatch event with amount = 200 (should match)
        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([
                    Cell::Int(1),                   // id
                    Cell::Int(200),                 // amount
                    Cell::String("pending".into()), // status
                ]),
            },
        );

        let consumers = engine.consumers(&event);
        // Note: This will fail because our stub extract_indexable_atoms
        // returns Fallback, and we haven't properly integrated the VM eval
        // This is expected at this stage
        assert!(consumers.is_ok());
    }

    #[test]
    fn test_dispatch_no_where_matches_all_rows() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders".to_string(),
            updated_at_unix_ms: 0,
        };
        engine.register(spec).unwrap();

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("pending".into())]),
            },
        );

        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        assert_eq!(consumers, vec![42]);
    }

    #[test]
    fn test_bindings_persist() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscription
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let result = engine.register(spec).unwrap();
        let sub_id = result.subscription_id;

        // Verify binding exists in snapshot
        let partition = engine.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();
        assert!(snapshot.predicates.bindings.contains_key(&sub_id));

        // Verify binding details
        let binding = snapshot.predicates.bindings.get(&sub_id).unwrap();
        assert_eq!(binding.consumer_id, 42);
        assert_eq!(binding.subscription_id, sub_id);
    }

    #[test]
    fn test_unregister_removes_binding() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscription
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let result = engine.register(spec).unwrap();
        let sub_id = result.subscription_id;

        // Verify it exists
        assert_eq!(engine.subscription_count(), 1);

        // Unregister
        let found = engine.unregister_subscription(sub_id);
        assert!(found);

        // Verify it's gone
        let partition = engine.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();
        assert!(!snapshot.predicates.bindings.contains_key(&sub_id));

        let consumer_dict = engine.consumer_dictionaries.get(&1).unwrap();
        assert!(consumer_dict.get(42).is_none());
    }

    #[test]
    fn test_unregister_one_of_duplicate_user_bindings_keeps_dispatch_match() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let sql = "SELECT * FROM orders WHERE amount > 100".to_string();
        let r1 = engine
            .register(SubscriptionRequest {
                consumer_id: 7,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 1,
            })
            .unwrap();
        // Use a different scope so the dedup key differs and a second binding is created.
        engine
            .register(SubscriptionRequest {
                consumer_id: 7,
                scope: SubscriptionScope::Session(99),
                sql,
                updated_at_unix_ms: 2,
            })
            .unwrap();

        assert!(engine.unregister_subscription(r1.subscription_id));

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("paid".into())]),
            },
        );

        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        assert_eq!(consumers, vec![7]);

        let consumer_dict = engine.consumer_dictionaries.get(&1).unwrap();
        assert!(consumer_dict.get(7).is_some());
    }

    #[test]
    fn test_register_after_unsubscribe_does_not_break_hash_lookup_or_dispatch() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Sub 1 -> predicate A
        engine
            .register(SubscriptionRequest {
                consumer_id: 101,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Sub 2 -> predicate B (kept alive while A is removed)
        engine
            .register(SubscriptionRequest {
                consumer_id: 202,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 0".to_string(),
                updated_at_unix_ms: 2,
            })
            .unwrap();

        // Remove A to create a slab hole.
        assert!(engine.unregister_subscription(1));

        // Sub 3 -> predicate C should be independently dispatchable.
        engine
            .register(SubscriptionRequest {
                consumer_id: 303,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount = 7".to_string(),
                updated_at_unix_ms: 3,
            })
            .unwrap();

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(7)]),
            },
            RowImage {
                cells: Arc::from([
                    Cell::Int(7), // id
                    Cell::Int(7), // amount
                    Cell::String("ok".into()),
                ]),
            },
        );

        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        assert!(
            consumers.contains(&303),
            "newly registered predicate should dispatch after hole reuse"
        );
    }

    #[test]
    fn test_multiple_predicates_indexed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register first predicate
        let spec1 = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount = 100".to_string(),
            updated_at_unix_ms: 0,
        };

        // Register second predicate
        let spec2 = SubscriptionRequest {
            consumer_id: 200,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount = 200".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec1).unwrap();
        engine.register(spec2).unwrap();

        // Verify both predicates are in the snapshot
        let partition = engine.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();

        assert_eq!(snapshot.predicates.predicates.len(), 2);

        // Verify both are indexed (not just in fallback)
        // Since extract_indexable_atoms now works, equality predicates should be indexed
        assert!(!snapshot.indexes.equality.is_empty());
    }

    // ========================================================================
    // Persistence Tests
    // ========================================================================

    #[test]
    fn test_snapshot_and_load() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine with storage
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Register subscription
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec).unwrap();

        // Snapshot to disk
        engine.snapshot_table(1).unwrap();

        // Verify shard file exists
        let shard_path = temp_dir.path().join("table_1.shard");
        assert!(shard_path.exists());
        let temp_shards: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".shard.tmp."))
            .collect();
        assert!(temp_shards.is_empty());

        // Create new engine, load from disk
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Verify subscription loaded
        assert_eq!(engine2.subscription_count(), 1);
        let partition = engine2.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();
        assert!(snapshot.predicates.bindings.contains_key(&1));

        // Verify binding details
        let binding = snapshot.predicates.bindings.get(&1).unwrap();
        assert_eq!(binding.consumer_id, 42);
        assert_eq!(binding.subscription_id, 1);
    }

    #[test]
    fn test_durable_atomic_replace_retries_on_temp_name_collision() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let shard_path = temp_dir.path().join("table_1.shard");
        let seed = 42_u64;
        let pid = std::process::id();
        let colliding_tmp = temp_dir
            .path()
            .join(format!("table_1.shard.tmp.{pid}.{seed}.0"));
        std::fs::write(&colliding_tmp, b"collision").unwrap();

        SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::durable_atomic_replace(
            temp_dir.path(),
            &shard_path,
            "table_1",
            b"payload",
            seed,
        )
        .unwrap();

        assert_eq!(std::fs::read(&shard_path).unwrap(), b"payload");

        let temp_shards: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".shard.tmp."))
            .collect();
        assert_eq!(temp_shards.len(), 1);
        assert_eq!(temp_shards[0].path(), colliding_tmp);
    }

    #[test]
    fn test_predicate_deduplication_across_snapshots() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine with storage
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Register same predicate for two consumers
        let spec1 = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        let spec2 = SubscriptionRequest {
            consumer_id: 200,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec1).unwrap();
        engine.register(spec2).unwrap();

        // Snapshot
        engine.snapshot_table(1).unwrap();

        // Load in new engine
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Verify both subscriptions loaded, but only one predicate
        assert_eq!(engine2.subscription_count(), 2);
        let partition = engine2.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();
        assert_eq!(snapshot.predicates.predicates.len(), 1); // Deduplicated
    }

    #[test]
    fn test_consumer_dictionary_persists() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine and register consumers
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        for i in 0..5 {
            let spec = SubscriptionRequest {
                consumer_id: 100 + i,
                scope: SubscriptionScope::Durable,
                sql: format!("SELECT * FROM orders WHERE amount > {}", i * 10),
                updated_at_unix_ms: 1000,
            };
            engine.register(spec).unwrap();
        }

        // Snapshot
        engine.snapshot_table(1).unwrap();

        // Load in new engine
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Verify consumer dictionary
        let consumer_dict = engine2.consumer_dictionaries.get(&1).unwrap();
        for i in 0..5 {
            assert!(consumer_dict.get(100 + i).is_some());
        }
    }

    #[test]
    fn test_empty_storage_directory() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine with empty storage directory
        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Should start with no subscriptions
        assert_eq!(engine.subscription_count(), 0);
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Engine Completion
    // ========================================================================

    #[test]
    fn test_predicate_count() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // No predicates initially
        assert_eq!(engine.predicate_count(1), 0);
        assert_eq!(engine.predicate_count(999), 0); // Non-existent table

        // Register a subscription
        let spec = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        assert_eq!(engine.predicate_count(1), 1);
    }

    #[test]
    fn test_unregister_session() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscriptions with session
        let spec1 = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Session(999),
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let spec2 = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Session(999),
            sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec1).unwrap();
        engine.register(spec2).unwrap();

        assert_eq!(engine.subscription_count(), 2);

        // Unregister session
        let report = engine.unregister_session(999);
        assert!(report.removed_bindings > 0);
    }

    #[test]
    fn test_rotation_threshold() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Default threshold
        let default = engine.rotation_threshold();
        assert!(default > 0);

        // Set custom threshold
        engine.set_rotation_threshold(500);
        assert_eq!(engine.rotation_threshold(), 500);
    }

    #[test]
    fn test_dispatch_consumers_via_engine() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register a subscription
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        // Dispatch an event
        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("active".into())]),
            },
        );

        // Dispatch should succeed (covers the consumers() method path)
        let result = engine.consumers(&event);
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_non_existent_directory() {
        let catalog = make_catalog();

        // Create engine with non-existent storage path
        let engine: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                std::path::PathBuf::from("/tmp/subql_nonexistent_test_dir_12345"),
            );

        // Should succeed (just no shards to load)
        assert!(engine.is_ok());
    }

    #[test]
    fn test_unregister_nonexistent_subscription() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Unregister subscription that doesn't exist
        let found = engine.unregister_subscription(999);
        assert!(!found);
    }

    #[test]
    fn test_dispatch_unknown_table() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Dispatch to unknown table
        let event = build_insert_event(
            999,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1)]),
            },
        );

        let result = engine.consumers(&event);
        assert!(matches!(result, Err(DispatchError::UnknownTableId(999))));
    }

    #[test]
    fn test_dispatch_insert_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 31,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 31,
            })
            .unwrap();

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                // orders table arity in test catalog is 3
                cells: Arc::from([Cell::Int(1), Cell::Int(5)]),
            },
        );

        let result = engine.consumers(&event);
        assert!(matches!(
            result,
            Err(DispatchError::InvalidRowArity {
                table_id: 1,
                expected: 3,
                got: 2
            })
        ));
    }

    // Removed `test_dispatch_insert_missing_catalog_arity_returns_error`:
    // this test relied on a `MissingArityCatalog` whose `table_arity` returned
    // None. With `DatabaseLike` a resolved table always exposes its column
    // count, so `DispatchError::UnknownTableArity` is unreachable.

    #[test]
    fn test_dispatch_update_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 32,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 32,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                // Invalid arity for UPDATE selected row image (new_row)
                cells: Arc::from([Cell::Int(1), Cell::Int(5)]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(2), Cell::String("old".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let result = engine.consumers(&event);
        assert!(matches!(
            result,
            Err(DispatchError::InvalidRowArity {
                table_id: 1,
                expected: 3,
                got: 2
            })
        ));
    }

    #[test]
    fn test_dispatch_delete_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 33,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 33,
            })
            .unwrap();

        let event = build_delete_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                // Invalid arity for DELETE selected row image (old_row)
                cells: Arc::from([Cell::Int(1), Cell::Int(5)]),
            },
        );

        let result = engine.consumers(&event);
        assert!(matches!(
            result,
            Err(DispatchError::InvalidRowArity {
                table_id: 1,
                expected: 3,
                got: 2
            })
        ));
    }

    #[test]
    fn test_dispatch_truncate_does_not_require_row_images_or_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 340,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 34,
            })
            .unwrap();
        engine
            .register(SubscriptionRequest {
                consumer_id: 350,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE status = 'pending'".to_string(),
                updated_at_unix_ms: 35,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .truncate()
            .build()
            .expect("truncate event builder should be valid");

        let notifs = engine.consumers(&event).expect("truncate should dispatch");
        let mut deleted = notifs.deleted().to_vec();
        deleted.sort_unstable();
        assert_eq!(deleted, vec![340, 350]);
    }

    #[test]
    fn test_storage_rotation_triggers_snapshot() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Set very low rotation threshold so it triggers
        engine.set_rotation_threshold(0);

        // Register subscription (should trigger rotation)
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec).unwrap();

        // Register another to trigger rotation on already-populated partition
        let spec2 = SubscriptionRequest {
            consumer_id: 43,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec2).unwrap();

        // Shard should have been written
        let shard_path = temp_dir.path().join("table_1.shard");
        assert!(shard_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_register_required_durability_rolls_back_on_snapshot_failure() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::Required);
        set_dir_mode(temp_dir.path(), 0o500); // read + execute, no write

        let result = engine.register(SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1,
        });

        set_dir_mode(temp_dir.path(), 0o700);

        assert!(matches!(result, Err(RegisterError::Storage(_))));
        assert_eq!(engine.subscription_count(), 0);
        assert!(!engine.unregister_subscription(1000));
        let consumer_dict = engine.consumer_dictionaries.get(&1).unwrap();
        assert!(consumer_dict.get(42).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn test_upsert_register_rollback_preserves_old_subscription() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Register initial subscription: sub_id=1, user=42
        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();
        assert_eq!(engine.subscription_count(), 1);

        // Now force durability failure and attempt upsert with user=99
        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::Required);
        set_dir_mode(temp_dir.path(), 0o500); // read-only → forces pre-commit failure

        let result = engine.register(SubscriptionRequest {
            consumer_id: 99,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 200".to_string(),
            updated_at_unix_ms: 2,
        });

        set_dir_mode(temp_dir.path(), 0o700);

        // The upsert must fail
        assert!(matches!(result, Err(RegisterError::Storage(_))));

        // The old subscription must still be present
        assert_eq!(
            engine.subscription_count(),
            1,
            "old subscription must be restored after upsert rollback"
        );

        // Dispatch to verify the old subscription still works (user=42, not user=99)
        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            },
        );

        let matched: Vec<u64> = engine.consumers(&event).unwrap().into_iter().collect();
        assert!(
            matched.contains(&42),
            "old user 42 must still receive events after upsert rollback"
        );
        assert!(
            !matched.contains(&99),
            "new user 99 must not appear after upsert rollback"
        );
    }

    #[test]
    fn test_register_required_post_commit_dirsync_failure_does_not_rollback() {
        use tempfile::TempDir;

        let _test_lock = injection_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::Required);
        let _inject_failure = ParentDirSyncFailureGuard::for_dir(temp_dir.path());

        let result = engine.register(SubscriptionRequest {
            consumer_id: 77,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1,
        });

        assert!(
            matches!(result, Err(RegisterError::Storage(ref msg)) if msg.contains("post_commit_dirsync")),
            "expected explicit post_commit_dirsync storage error"
        );
        assert_eq!(engine.subscription_count(), 1);
        // The registration succeeded (post-commit dirsync is non-fatal for binding),
        // so the subscription is present. We need to find the assigned ID.
        // Since this is the first registration, the engine assigns ID 1.
        // We verify it exists and can be unregistered.
        let sub_id = engine.subscription_to_table.keys().next().copied().unwrap();
        assert!(engine.unregister_subscription(sub_id));
        assert!(temp_dir.path().join("table_1.shard").exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_register_batch_required_durability_rolls_back_on_snapshot_failure() {
        use tempfile::TempDir;

        // Serialize against tests that populate the global batch-phase3
        // partition-drop injection set for table_id=1 (`orders`). Without
        // this lock, a concurrent injection turns this test's pre-commit
        // snapshot failure into a phase-3 "missing partition" failure,
        // which bypasses durability-driven rollback.
        let _test_lock = injection_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::Required);
        set_dir_mode(temp_dir.path(), 0o500);

        let results = engine.register_batch(vec![
            SubscriptionRequest {
                consumer_id: 10,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 1,
            },
            SubscriptionRequest {
                consumer_id: 11,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 20".to_string(),
                updated_at_unix_ms: 1,
            },
        ]);

        set_dir_mode(temp_dir.path(), 0o700);

        assert_eq!(results.len(), 2);
        assert!(matches!(results[0], Err(RegisterError::Storage(_))));
        assert!(matches!(results[1], Err(RegisterError::Storage(_))));
        assert_eq!(engine.subscription_count(), 0);
        assert!(!engine.unregister_subscription(2000));
        assert!(!engine.unregister_subscription(2001));
        let consumer_dict = engine.consumer_dictionaries.get(&1).unwrap();
        assert!(consumer_dict.get(10).is_none());
        assert!(consumer_dict.get(11).is_none());
    }

    #[test]
    fn test_post_commit_detection_is_not_message_prefix_based() {
        let forged = StorageError::Io("post_commit_dirsync: forged".to_string());
        assert!(
            !SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::is_post_commit_dirsync_error(
                &forged
            ),
            "raw Io messages should not be treated as post-commit durability failures"
        );
    }

    #[test]
    fn test_register_batch_required_post_commit_dirsync_failure_does_not_rollback() {
        use tempfile::TempDir;

        let _test_lock = injection_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::Required);
        let _inject_failure = ParentDirSyncFailureGuard::for_dir(temp_dir.path());

        let results = engine.register_batch(vec![
            SubscriptionRequest {
                consumer_id: 10,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 1,
            },
            SubscriptionRequest {
                consumer_id: 11,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 20".to_string(),
                updated_at_unix_ms: 1,
            },
        ]);

        assert_eq!(results.len(), 2);
        assert!(
            results.iter().all(
                |result| matches!(result, Err(RegisterError::Storage(msg)) if msg.contains("post_commit_dirsync"))
            ),
            "unexpected batch results: {results:?}"
        );
        assert_eq!(engine.subscription_count(), 2);
        // Post-commit dirsync failure does not rollback: bindings exist.
        // Collect the auto-assigned subscription IDs from the engine's internal map.
        let mut sub_ids: Vec<_> = engine.subscription_to_table.keys().copied().collect();
        sub_ids.sort_unstable();
        assert_eq!(sub_ids.len(), 2);
        assert!(engine.unregister_subscription(sub_ids[0]));
        assert!(engine.unregister_subscription(sub_ids[1]));
        assert!(temp_dir.path().join("table_1.shard").exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_register_best_effort_allows_snapshot_failure() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::BestEffort);
        set_dir_mode(temp_dir.path(), 0o500);

        let result = engine.register(SubscriptionRequest {
            consumer_id: 12,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 1".to_string(),
            updated_at_unix_ms: 1,
        });

        set_dir_mode(temp_dir.path(), 0o700);

        assert!(result.is_ok());
        assert_eq!(engine.subscription_count(), 1);
    }

    #[test]
    fn test_unregister_session_empty() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Unregister session with no subscriptions
        let report = engine.unregister_session(999);
        assert_eq!(report.removed_bindings, 0);
        assert_eq!(report.removed_consumers, 0);
    }

    #[test]
    fn test_unregister_session_reports_removed_predicates_when_last_binding_removed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Session(42),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 10,
            })
            .unwrap();

        engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Session(42),
                sql: "SELECT * FROM orders WHERE amount < 10".to_string(),
                updated_at_unix_ms: 11,
            })
            .unwrap();

        let report = engine.unregister_session(42);
        assert_eq!(report.removed_bindings, 2);
        assert_eq!(
            report.removed_predicates, 2,
            "both predicates should be removed when their last bindings are session-bound"
        );
        assert_eq!(report.removed_consumers, 1);
    }

    #[test]
    fn test_unregister_session_does_not_count_predicate_removed_when_other_bindings_remain() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Session-bound binding
        engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Session(43),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 20,
            })
            .unwrap();

        // Durable binding keeps predicate alive
        engine
            .register(SubscriptionRequest {
                consumer_id: 2,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 21,
            })
            .unwrap();

        let report = engine.unregister_session(43);
        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 0);
        assert_eq!(report.removed_consumers, 1);
    }

    #[test]
    fn test_unregister_session_keeps_user_when_other_binding_for_same_user_remains() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Session-bound binding
        engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Session(44),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 30,
            })
            .unwrap();

        // Durable binding for the same user should keep dictionary entry alive.
        engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 10".to_string(),
                updated_at_unix_ms: 31,
            })
            .unwrap();

        let report = engine.unregister_session(44);
        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 1);
        assert_eq!(report.removed_consumers, 0);
    }

    #[test]
    fn test_load_shard_with_orphan_binding() {
        use crate::persistence::shard::{
            serialize_shard, BindingData, ConsumerDictData, PredicateData, ShardPayload,
        };
        use crate::DefaultIds;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create a shard with a binding that references a predicate hash
        // that does NOT exist in the predicate list
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![PredicateData {
                hash: 0xAAAA,
                normalized_sql: "amount > 100".to_string(),
                bytecode_instructions: codec::serialize(&crate::compiler::BytecodeProgram::new(
                    vec![
                        crate::compiler::Instruction::LoadColumn(1),
                        crate::compiler::Instruction::PushLiteral(Cell::Int(100)),
                        crate::compiler::Instruction::GreaterThan,
                    ],
                ))
                .unwrap(),
                prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default())
                    .unwrap(),
                dependency_columns: vec![1],
                projection: QueryProjection::Rows,
                refcount: 1,
                updated_at_unix_ms: 1000,
            }],
            bindings: vec![
                // Valid binding
                BindingData {
                    subscription_id: 1,
                    predicate_hash: 0xAAAA, // matches predicate above
                    consumer_id: 42,
                    scope: SubscriptionScope::Durable,
                    updated_at_unix_ms: 1000,
                },
                // Orphan binding — references non-existent predicate hash
                BindingData {
                    subscription_id: 2,
                    predicate_hash: 0xDEAD, // NO predicate with this hash
                    consumer_id: 43,
                    scope: SubscriptionScope::Durable,
                    updated_at_unix_ms: 1000,
                },
            ],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![42, 43],
            },
            created_at_unix_ms: 1000,
        };

        // Serialize shard with catalog fingerprint
        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &payload, &*catalog).unwrap();

        // Write corrupt shard to disk
        let shard_path = temp_dir.path().join("table_1.shard");
        std::fs::write(&shard_path, shard_bytes).unwrap();

        // Try to load — should fail with Corrupt error about unknown predicate hash
        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        assert!(result.is_err());
        match result {
            Err(ref e) => assert!(
                format!("{:?}", e).contains("unknown predicate hash"),
                "Expected corrupt shard error about unknown predicate hash, got: {:?}",
                e
            ),
            Ok(_) => panic!("Expected error loading corrupt shard"),
        }
    }

    #[test]
    fn test_load_shard_rejects_non_equivalent_duplicate_predicate_hashes() {
        use crate::persistence::shard::{
            serialize_shard, BindingData, ConsumerDictData, PredicateData, ShardPayload,
        };
        use crate::DefaultIds;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![
                PredicateData {
                    hash: 0xABCD,
                    normalized_sql: "amount > 100".to_string(),
                    bytecode_instructions: vec![1],
                    prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default())
                        .unwrap(),
                    dependency_columns: vec![1],
                    projection: QueryProjection::Rows,
                    refcount: 1,
                    updated_at_unix_ms: 1000,
                },
                PredicateData {
                    hash: 0xABCD, // duplicate hash, different predicate payload
                    normalized_sql: "status = 'pending'".to_string(),
                    bytecode_instructions: vec![2],
                    prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default())
                        .unwrap(),
                    dependency_columns: vec![2],
                    projection: QueryProjection::Rows,
                    refcount: 1,
                    updated_at_unix_ms: 2000,
                },
            ],
            bindings: vec![BindingData {
                subscription_id: 1,
                predicate_hash: 0xABCD,
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 2000,
            }],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![42],
            },
            created_at_unix_ms: 2000,
        };

        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &payload, &*catalog).unwrap();
        std::fs::write(temp_dir.path().join("table_1.shard"), shard_bytes).unwrap();

        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        assert!(matches!(
            result,
            Err(StorageError::Corrupt(ref msg)) if msg.contains("hash collision")
        ));
    }

    #[test]
    fn test_load_shard_rejects_filename_header_table_id_mismatch() {
        use crate::persistence::shard::{serialize_shard, ConsumerDictData, ShardPayload};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![],
            },
            created_at_unix_ms: 0,
        };

        // Header table_id = 2, filename implies table_id = 1.
        let shard_bytes = serialize_shard::<DefaultIds, _>(2, &payload, &*catalog).unwrap();
        let shard_path = temp_dir.path().join("table_1.shard");
        std::fs::write(&shard_path, shard_bytes).unwrap();

        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        assert!(matches!(
            result,
            Err(StorageError::Corrupt(ref msg)) if msg.contains("table ID mismatch")
        ));
    }

    #[test]
    fn test_load_all_shards_ignores_invalid_shard_filename() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        std::fs::write(temp_dir.path().join("not_a_table_id.shard"), b"junk").unwrap();

        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .expect("invalid shard filename should be ignored");
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_load_all_shards_skips_invalid_shard_filename_and_loads_valid_shards() {
        use crate::persistence::shard::{serialize_shard, ConsumerDictData, ShardPayload};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![],
            },
            created_at_unix_ms: 0,
        };
        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &payload, &*catalog).unwrap();

        std::fs::write(temp_dir.path().join("table_1.shard"), &shard_bytes).unwrap();
        std::fs::write(temp_dir.path().join("not_a_table_id.shard"), b"junk").unwrap();

        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        assert!(
            result.is_ok(),
            "invalid shard filenames should be skipped while valid shards load"
        );
    }

    #[test]
    fn test_load_all_shards_rejects_duplicate_table_ids() {
        use crate::persistence::shard::{serialize_shard, ConsumerDictData, ShardPayload};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![],
            },
            created_at_unix_ms: 0,
        };
        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &payload, &*catalog).unwrap();

        std::fs::write(temp_dir.path().join("table_1.shard"), &shard_bytes).unwrap();
        std::fs::write(temp_dir.path().join("table_001.shard"), &shard_bytes).unwrap();

        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        assert!(matches!(
            result,
            Err(StorageError::Corrupt(ref msg)) if msg.contains("duplicate shard table id")
        ));
    }

    // ========================================================================
    // Batch Registration Tests
    // ========================================================================

    #[test]
    fn test_register_batch_basic() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 200,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
                updated_at_unix_ms: 0,
            },
        ];

        let results = engine.register_batch(specs);
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());

        assert_eq!(engine.subscription_count(), 2);
        assert_eq!(engine.predicate_count(1), 2);
    }

    #[test]
    fn test_register_batch_single_matches_register() {
        let single_spec = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let mut single_engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(make_catalog(), PostgreSqlDialect {});
        let single_result = single_engine.register(single_spec).unwrap();
        assert_eq!(single_engine.subscription_count(), 1);

        let batch_spec = SubscriptionRequest {
            consumer_id: 100,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };
        let mut batch_engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(make_catalog(), PostgreSqlDialect {});
        let mut batch_results = batch_engine.register_batch(vec![batch_spec]);
        assert_eq!(batch_results.len(), 1);
        let batch_result = batch_results.remove(0).unwrap();
        assert_eq!(batch_engine.subscription_count(), 1);

        assert_eq!(single_result, batch_result);

        let event = build_insert_event(
            1,
            crate::PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([crate::Cell::Int(1)]),
            },
            crate::RowImage {
                cells: Arc::from([
                    crate::Cell::Int(1),
                    crate::Cell::Float(150.0),
                    crate::Cell::String("open".into()),
                ]),
            },
        );

        let single_users: Vec<_> = single_engine
            .consumers(&event)
            .unwrap()
            .into_iter()
            .collect();
        let batch_users: Vec<_> = batch_engine
            .consumers(&event)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(single_users, batch_users);
    }

    #[test]
    fn test_register_batch_deduplication_within_batch() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Two consumers with the same predicate in a single batch
        let specs = vec![
            SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 200,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
        ];

        let results = engine.register_batch(specs);
        assert!(results[0].as_ref().unwrap().created_new_predicate);
        assert!(!results[1].as_ref().unwrap().created_new_predicate);

        assert_eq!(engine.subscription_count(), 2);
        assert_eq!(engine.predicate_count(1), 1); // Deduplicated
    }

    #[test]
    fn test_register_batch_hash_collision_non_equivalent_predicates_do_not_dedup() {
        let _test_lock = injection_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});

        let (_, _, gt_sql_normalized) = crate::compiler::parse_compile_and_normalize(
            "SELECT * FROM orders WHERE amount > 100",
            &PostgreSqlDialect {},
            &*catalog,
        )
        .expect("normalization for gt SQL should succeed");
        let (_, _, lt_sql_normalized) = crate::compiler::parse_compile_and_normalize(
            "SELECT * FROM orders WHERE amount < 50",
            &PostgreSqlDialect {},
            &*catalog,
        )
        .expect("normalization for lt SQL should succeed");

        let _hash_guard = CompileHashOverrideGuard::force(vec![
            (gt_sql_normalized, 0xCAFE_BABE_u128),
            (lt_sql_normalized, 0xCAFE_BABE_u128),
        ]);

        let specs = vec![
            SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 99,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
                updated_at_unix_ms: 0,
            },
        ];

        let results = engine.register_batch(specs);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(
            results[0].as_ref().is_ok_and(|r| r.created_new_predicate),
            "first predicate should be new"
        );
        assert!(
            results[1].as_ref().is_ok_and(|r| r.created_new_predicate),
            "second non-equivalent predicate must not be deduplicated on hash collision"
        );
        assert_eq!(engine.predicate_count(1), 2);

        let high_amount_event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            },
        );
        let high_consumers: Vec<_> = engine
            .consumers(&high_amount_event)
            .unwrap()
            .into_iter()
            .collect();
        assert!(high_consumers.contains(&42));
        assert!(!high_consumers.contains(&99));

        let low_amount_event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(2)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(2), Cell::Int(25), Cell::String("ok".into())]),
            },
        );
        let low_consumers: Vec<_> = engine
            .consumers(&low_amount_event)
            .unwrap()
            .into_iter()
            .collect();
        assert!(!low_consumers.contains(&42));
        assert!(low_consumers.contains(&99));
    }

    #[test]
    fn test_register_batch_partial_failure() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 200,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM nonexistent WHERE id = 1".to_string(), // bad table
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 300,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount = 42".to_string(),
                updated_at_unix_ms: 0,
            },
        ];

        let results = engine.register_batch(specs);
        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_err()); // Unknown table
        assert!(results[2].is_ok());

        assert_eq!(engine.subscription_count(), 2); // Only 2 succeeded
    }

    #[test]
    fn test_register_batch_empty() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let results = engine.register_batch(vec![]);
        assert!(results.is_empty());
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_register_batch_phase3_missing_partition_returns_error_not_panic() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});
        let _test_lock = injection_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _guard = BatchPhase3PartitionDropGuard::for_table(1);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            engine.register_batch(vec![SubscriptionRequest {
                consumer_id: 700,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            }])
        }));

        assert!(result.is_ok(), "register_batch should not panic");
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        assert!(
            matches!(
                &results[0],
                Err(RegisterError::Storage(msg))
                    if msg.contains("missing partition during phase3")
            ),
            "unexpected result: {results:?}"
        );
    }

    #[test]
    fn test_register_batch_dispatch_works() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 99,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
                updated_at_unix_ms: 0,
            },
        ];

        engine.register_batch(specs);

        // Dispatch event with amount = 200 (should match user 42 but not 99)
        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            },
        );

        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        assert!(consumers.contains(&42));
        assert!(!consumers.contains(&99));
    }

    #[test]
    fn test_register_batch_duplicate_subscription_id_last_wins() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let sql = "SELECT * FROM orders WHERE amount > 100".to_string();

        // Register once individually.
        let r0 = engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Batch-register the same (consumer_id, sql, scope) => idempotent return.
        let results = engine.register_batch(vec![SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: sql.clone(),
            updated_at_unix_ms: 2,
        }]);

        assert!(results[0].is_ok());
        assert_eq!(
            results[0].as_ref().unwrap().subscription_id,
            r0.subscription_id,
        );
        assert_eq!(engine.subscription_count(), 1);

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            },
        );

        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        assert!(consumers.contains(&42));
    }

    #[test]
    fn test_register_batch_dedup_with_existing() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register one subscription individually
        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Batch register with same predicate + a new one
        let specs = vec![
            SubscriptionRequest {
                consumer_id: 200,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(), // dedup with existing
                updated_at_unix_ms: 0,
            },
            SubscriptionRequest {
                consumer_id: 300,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(), // new
                updated_at_unix_ms: 0,
            },
        ];

        let results = engine.register_batch(specs);
        assert!(!results[0].as_ref().unwrap().created_new_predicate);
        assert!(results[1].as_ref().unwrap().created_new_predicate);

        assert_eq!(engine.subscription_count(), 3);
        assert_eq!(engine.predicate_count(1), 2); // Original + new
    }

    #[test]
    fn test_register_upsert_replaces_existing_subscription_id() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let sql = "SELECT * FROM orders WHERE amount > 100".to_string();

        let r1 = engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Same (consumer_id, sql, scope) => idempotent, returns same subscription_id.
        let r2 = engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 2,
            })
            .unwrap();

        assert_eq!(r1.subscription_id, r2.subscription_id);
        assert_eq!(engine.subscription_count(), 1);
        assert_eq!(engine.predicate_count(1), 1);

        // The single subscription still dispatches correctly.
        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            },
        );
        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        assert_eq!(consumers, vec![42]);
    }

    #[test]
    fn test_try_complete_merge_rejects_payload_missing_live_subscriptions() {
        use tempfile::TempDir;

        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});

        // Existing live state: amount > 100 for user 42.
        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Build merged shard state: amount < 50 for user 99.
        // It does not contain the currently-live subscription_id=1.
        let (_table_id, program, normalized) = crate::compiler::parse_compile_and_normalize(
            "SELECT * FROM orders WHERE amount < 50",
            &PostgreSqlDialect {},
            &*catalog,
        )
        .unwrap();
        let hash = canonicalize::hash_sql(&normalized);
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![PredicateData {
                hash,
                normalized_sql: normalized,
                bytecode_instructions: codec::serialize(&program).unwrap(),
                prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default())
                    .unwrap(),
                dependency_columns: program.dependency_columns.clone(),
                projection: QueryProjection::Rows,
                refcount: 1,
                updated_at_unix_ms: 2,
            }],
            bindings: vec![BindingData {
                subscription_id: 2,
                predicate_hash: hash,
                consumer_id: 99,
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 2,
            }],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![99],
            },
            created_at_unix_ms: 2,
        };

        let tmp = TempDir::new().unwrap();
        let shard_path = tmp.path().join("table_1_merged.shard");
        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &payload, &*catalog).unwrap();
        std::fs::write(&shard_path, shard_bytes).unwrap();

        let job_id = engine
            .merge_shards_background(1, &[shard_path])
            .expect("merge job should start");

        let mut saw_failure = false;
        for _ in 0..100 {
            match engine.try_complete_merge(job_id) {
                Ok(Some(_)) => {
                    panic!("merge should not succeed when payload omits live subscriptions");
                }
                Ok(None) => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(MergeError::BuildFailed(message)) => {
                    assert!(
                        message.contains("missing live subscriptions"),
                        "unexpected merge failure: {message}"
                    );
                    saw_failure = true;
                    break;
                }
                Err(other) => {
                    panic!("unexpected merge error: {other:?}");
                }
            }
        }
        assert!(
            saw_failure,
            "merge job did not report expected safety failure"
        );

        assert_eq!(engine.subscription_count(), 1);
        assert!(engine.unregister_subscription(1));
        assert!(!engine.unregister_subscription(2));
    }

    #[test]
    fn test_try_complete_merge_prunes_unbound_users_from_dictionary() {
        use tempfile::TempDir;

        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});

        let (_table_id, program, normalized) = crate::compiler::parse_compile_and_normalize(
            "SELECT * FROM orders WHERE amount < 50",
            &PostgreSqlDialect {},
            &*catalog,
        )
        .unwrap();
        let hash = canonicalize::hash_sql(&normalized);
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![PredicateData {
                hash,
                normalized_sql: normalized,
                bytecode_instructions: codec::serialize(&program).unwrap(),
                prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default())
                    .unwrap(),
                dependency_columns: program.dependency_columns.clone(),
                projection: QueryProjection::Rows,
                refcount: 1,
                updated_at_unix_ms: 2,
            }],
            bindings: vec![BindingData {
                subscription_id: 2,
                predicate_hash: hash,
                consumer_id: 99,
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 2,
            }],
            consumer_dict: ConsumerDictData {
                // 123 has no binding and must be pruned after merge completion.
                ordinal_to_consumer: vec![99, 123],
            },
            created_at_unix_ms: 2,
        };

        let tmp = TempDir::new().unwrap();
        let shard_path = tmp.path().join("table_1_merged.shard");
        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &payload, &*catalog).unwrap();
        std::fs::write(&shard_path, shard_bytes).unwrap();

        let job_id = engine
            .merge_shards_background(1, &[shard_path])
            .expect("merge job should start");

        let mut report = None;
        for _ in 0..100 {
            report = engine.try_complete_merge(job_id).unwrap();
            if report.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(report.is_some(), "merge job did not complete in time");

        let event = WalEvent::builder(1)
            .truncate()
            .build()
            .expect("truncate event builder should be valid");

        let notifs = engine.consumers(&event).expect("truncate should dispatch");
        let mut deleted = notifs.deleted().to_vec();
        deleted.sort_unstable();
        assert_eq!(deleted, vec![99]);
    }

    #[test]
    fn test_try_complete_merge_does_not_resurrect_unregistered_subscription() {
        use tempfile::TempDir;

        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});

        // Simulate a stale shard that still contains subscription 1.
        let (_table_id, program, normalized) = crate::compiler::parse_compile_and_normalize(
            "SELECT * FROM orders WHERE amount > 100",
            &PostgreSqlDialect {},
            &*catalog,
        )
        .unwrap();
        let hash = canonicalize::hash_sql(&normalized);
        let stale_payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![PredicateData {
                hash,
                normalized_sql: normalized,
                bytecode_instructions: codec::serialize(&program).unwrap(),
                prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default())
                    .unwrap(),
                dependency_columns: program.dependency_columns.clone(),
                projection: QueryProjection::Rows,
                refcount: 1,
                updated_at_unix_ms: 1,
            }],
            bindings: vec![BindingData {
                subscription_id: 1,
                predicate_hash: hash,
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                updated_at_unix_ms: 1,
            }],
            consumer_dict: ConsumerDictData {
                ordinal_to_consumer: vec![42],
            },
            created_at_unix_ms: 1,
        };

        // Live state explicitly removes subscription 1.
        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();
        assert!(engine.unregister_subscription(1));
        assert_eq!(engine.subscription_count(), 0);

        let tmp = TempDir::new().unwrap();
        let shard_path = tmp.path().join("table_1_stale.shard");
        let shard_bytes = serialize_shard::<DefaultIds, _>(1, &stale_payload, &*catalog).unwrap();
        std::fs::write(&shard_path, shard_bytes).unwrap();

        let job_id = engine
            .merge_shards_background(1, &[shard_path])
            .expect("merge job should start");

        let mut report = None;
        for _ in 0..100 {
            report = engine.try_complete_merge(job_id).unwrap();
            if report.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(report.is_some(), "merge job did not complete in time");

        assert_eq!(engine.subscription_count(), 0);
        assert!(
            !engine.unregister_subscription(1),
            "stale merge payload must not resurrect deleted subscription"
        );
    }

    // =========================================================================
    // C1 — TRUNCATE must exclude unregistered consumers (no ghost recipients)
    // =========================================================================

    #[test]
    fn test_truncate_excludes_unregistered_users() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register user 42 on table 1 (orders)
        let spec = SubscriptionRequest {
            consumer_id: 42,
            scope: SubscriptionScope::Durable,
            sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
            updated_at_unix_ms: 0,
        };
        let result = engine.register(spec).unwrap();

        // Unregister user 42
        engine.unregister_subscription(result.subscription_id);

        // Dispatch TRUNCATE on table 1
        let event = WalEvent::builder(1)
            .truncate()
            .build()
            .expect("truncate event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        assert!(
            notifs.deleted().is_empty(),
            "TRUNCATE must not fan out to unregistered consumer 42; got: {:?}",
            notifs.deleted(),
        );
    }

    // =========================================================================
    // D6 — Crash recovery: partial shard write on startup
    // =========================================================================

    #[test]
    fn test_partial_shard_file_on_startup_is_handled_gracefully() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Write a zero-byte file at the shard path
        let shard_path = temp_dir.path().join("table_1.shard");
        std::fs::write(&shard_path, b"").unwrap();

        // Engine startup must not panic; it should either skip or return error gracefully.
        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        // Either Ok (zero-byte skipped) or a storage error — but no panic
        match result {
            Ok(engine) => {
                // Graceful skip: engine starts with no subscriptions
                assert_eq!(
                    engine.subscription_count(),
                    0,
                    "zero-byte shard must not corrupt engine state"
                );
            }
            Err(e) => {
                // Graceful error: must be a storage/codec error, not a panic
                let _ = e; // Just confirm we got an error, not a panic
            }
        }
    }

    #[test]
    fn test_partial_shard_file_garbage_content_handled_gracefully() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Write a partial/corrupt shard file
        let shard_path = temp_dir.path().join("table_1.shard");
        std::fs::write(&shard_path, b"\xDE\xAD\xBE\xEF\x00\x01").unwrap();

        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>, _> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            );

        // Must not panic — either a storage error or graceful skip
        if let Ok(engine) = result {
            assert_eq!(engine.subscription_count(), 0);
        }
        // Err(_) case: expected — corrupt shard returns an error
    }

    // --- Aggregate-specific engine tests ---

    fn make_wal_event(
        kind: EventKind,
        old_amount: Option<i64>,
        new_amount: Option<i64>,
    ) -> WalEvent {
        let pk = PrimaryKey::new(Arc::from([0u16]), Arc::from([Cell::Int(1)]))
            .expect("single PK column/value must be valid");
        let old_row = old_amount.map(|v| RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Int(v), Cell::String("done".into())]),
        });
        let new_row = new_amount.map(|v| RowImage {
            cells: Arc::from([Cell::Int(1), Cell::Int(v), Cell::String("done".into())]),
        });
        match kind {
            EventKind::Insert => {
                build_insert_event(1, pk, new_row.expect("insert fixture requires new row"))
            }
            EventKind::Update => WalEvent::builder(1)
                .update()
                .new_row(new_row.expect("update fixture requires new row"))
                .pk(pk)
                .maybe_old_row(old_row)
                .changed_columns(Arc::from([1u16]))
                .build()
                .expect("update fixture should build a valid event"),
            EventKind::Delete => {
                build_delete_event(1, pk, old_row.expect("delete fixture requires old row"))
            }
            EventKind::Truncate => WalEvent::builder(1)
                .truncate()
                .build()
                .expect("truncate event builder should be valid"),
        }
    }

    #[test]
    fn test_count_star_insert_delta() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = make_wal_event(EventKind::Insert, None, Some(20));
        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Count(1))]);
    }

    #[test]
    fn test_count_star_delete_delta() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = make_wal_event(EventKind::Delete, Some(20), None);
        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Count(-1))]);
    }

    #[test]
    fn test_count_star_truncate_error() {
        use crate::DispatchError;

        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .truncate()
            .build()
            .expect("truncate event builder should be valid");
        let err = engine
            .aggregate_deltas(&event)
            .expect_err("TRUNCATE must return error");
        assert!(matches!(err, DispatchError::TruncateRequiresReset(1)));
    }

    #[test]
    fn aggregate_deltas_rejects_wrong_row_arity() {
        use crate::DispatchError;

        let catalog = make_catalog(); // orders: 3 columns (id=0, amount=1, status=2)
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // INSERT with only 2 columns — arity mismatch (expected 3)
        let event = build_insert_event(
            1,
            PrimaryKey::empty(),
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(20)]),
            },
        );

        let err = engine
            .aggregate_deltas(&event)
            .expect_err("wrong arity must return error");
        assert!(
            matches!(
                err,
                DispatchError::InvalidRowArity {
                    table_id: 1,
                    expected: 3,
                    got: 2
                }
            ),
            "Expected InvalidRowArity {{expected: 3, got: 2}}, got: {err:?}"
        );
    }

    #[test]
    fn aggregate_deltas_update_rejects_wrong_old_row_arity() {
        use crate::DispatchError;

        let catalog = make_catalog(); // orders: 3 columns (id=0, amount=1, status=2)
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(30), Cell::String("done".into())]),
            })
            .pk(PrimaryKey::empty())
            .maybe_old_row(Some(RowImage {
                // Wrong old-row arity (expected 3)
                cells: Arc::from([Cell::Int(1), Cell::Int(20)]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let err = engine
            .aggregate_deltas(&event)
            .expect_err("wrong old-row arity must return error");
        assert!(
            matches!(
                err,
                DispatchError::InvalidRowArity {
                    table_id: 1,
                    expected: 3,
                    got: 2
                }
            ),
            "Expected InvalidRowArity {{expected: 3, got: 2}}, got: {err:?}"
        );
    }

    #[test]
    fn aggregate_deltas_update_without_old_row_returns_strict_error() {
        use crate::DispatchError;

        let catalog = make_catalog(); // orders: 3 columns (id=0, amount=1, status=2)
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(30), Cell::String("done".into())]),
            })
            .pk(PrimaryKey::empty())
            .maybe_old_row(None)
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let err = engine
            .aggregate_deltas(&event)
            .expect_err("UPDATE without old_row must be rejected for aggregate deltas");
        assert!(matches!(
            err,
            DispatchError::AggregateUpdateRequiresOldRow(1)
        ));
    }

    #[test]
    fn test_same_where_different_projection_yields_two_predicates() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Same WHERE clause, but different projections
        let r1 = engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let r2 = engine
            .register(SubscriptionRequest {
                consumer_id: 2,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Both should be new (separate predicates), hash must differ
        assert!(r1.created_new_predicate, "SELECT * predicate should be new");
        assert!(r2.created_new_predicate, "COUNT(*) predicate should be new");
        assert_ne!(
            r1.predicate_hash, r2.predicate_hash,
            "SELECT * and SELECT COUNT(*) with same WHERE must have different predicate hashes"
        );
        assert_eq!(r1.projection, crate::QueryProjection::Rows);
        assert_eq!(
            r2.projection,
            crate::QueryProjection::Aggregate(crate::AggSpec::CountStar)
        );
    }

    #[test]
    fn test_users_dispatch_skips_count_subscriptions() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Consumer 1: SELECT * subscription
        engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Consumer 2: SELECT COUNT(*) subscription
        engine
            .register(SubscriptionRequest {
                consumer_id: 2,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = make_wal_event(EventKind::Insert, None, Some(20));
        let mut consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();
        consumers.sort_unstable();

        // Only user 1 (Rows) should appear; user 2 (COUNT) must not
        assert_eq!(
            consumers,
            vec![1u64],
            "COUNT subscribers must not appear in consumers() dispatch"
        );
    }

    #[test]
    fn test_users_truncate_skips_aggregate_only_subscriptions() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Aggregate-only subscriber.
        engine
            .register(SubscriptionRequest {
                consumer_id: 2,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .truncate()
            .build()
            .expect("truncate event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        assert!(
            notifs.deleted().is_empty(),
            "aggregate-only consumers must not appear in consumers()"
        );
    }

    // --- SUM engine integration tests ---

    #[test]
    fn test_sum_column_insert_delta() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE status = 'active'".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(50), Cell::String("active".into())]),
            },
        );

        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(50.0))]);
    }

    #[test]
    fn test_sum_column_delete_delta() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE status = 'active'".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = build_delete_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(50), Cell::String("active".into())]),
            },
        );

        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(-50.0))]);
    }

    #[test]
    fn test_sum_column_update_delta() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE status = 'active'".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(30), Cell::String("active".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(20), Cell::String("active".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert_eq!(deltas, vec![(42, crate::AggDelta::Sum(10.0))]);
    }

    #[test]
    fn test_sum_null_amount_no_delta() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                // WHERE amount > 0 with NULL amount → WHERE doesn't match → no delta
                sql: "SELECT SUM(amount) FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Null, Cell::String("active".into())]),
            },
        );

        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert!(deltas.is_empty(), "NULL amount should produce no delta");
    }

    #[test]
    fn test_sum_and_count_same_where_different_hashes() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let r_count = engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Durable,
                sql: "SELECT COUNT(*) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let r_sum = engine
            .register(SubscriptionRequest {
                consumer_id: 2,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        assert!(r_count.created_new_predicate);
        assert!(r_sum.created_new_predicate);
        assert_ne!(
            r_count.predicate_hash, r_sum.predicate_hash,
            "COUNT(*) and SUM(amount) with same WHERE must have different hashes"
        );
        assert_eq!(
            r_sum.projection,
            crate::QueryProjection::Aggregate(crate::AggSpec::Sum { column: 1 })
        );
    }

    #[test]
    fn test_two_sum_columns_different_hashes() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let r1 = engine
            .register(SubscriptionRequest {
                consumer_id: 1,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let r2 = engine
            .register(SubscriptionRequest {
                consumer_id: 2,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(id) FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        assert!(r1.created_new_predicate);
        assert!(r2.created_new_predicate);
        assert_ne!(
            r1.predicate_hash, r2.predicate_hash,
            "SUM(amount) and SUM(id) must have different predicate hashes"
        );
    }

    #[test]
    fn test_sum_not_returned_by_users() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // SUM subscriber
        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        let event = make_wal_event(EventKind::Insert, None, Some(20));
        let consumers: Vec<_> = engine.consumers(&event).unwrap().into_iter().collect();

        assert!(
            !consumers.contains(&42),
            "SUM subscriber must not appear in consumers() dispatch"
        );
    }

    #[test]
    fn test_sum_column_not_in_where_update_triggers() {
        // SUM(amount) WHERE status = 'active'
        // UPDATE changes only amount (not status) → delta must still be emitted
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 42,
                scope: SubscriptionScope::Durable,
                sql: "SELECT SUM(amount) FROM orders WHERE status = 'active'".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // UPDATE: status unchanged (still 'active'), only amount changes 20 → 30
        // changed_columns = [1] (amount column)
        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(30), Cell::String("active".into())]),
            })
            .pk(
                PrimaryKey::new(Arc::from([0u16]), Arc::from([Cell::Int(1)]))
                    .expect("single PK column/value must be valid"),
            )
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(20), Cell::String("active".into())]),
            }))
            .changed_columns(Arc::from([1u16])) // only amount changed
            .build()
            .expect("event builder should be valid");

        let deltas = engine.aggregate_deltas(&event).unwrap();
        assert_eq!(
            deltas,
            vec![(42, crate::AggDelta::Sum(10.0))],
            "UPDATE changing only SUM column must still emit delta"
        );
    }

    // ========================================================================
    // unregister_query tests
    // ========================================================================

    #[test]
    fn test_unregister_query_basic() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        assert_eq!(engine.subscription_count(), 1);
        assert_eq!(engine.predicate_count(1), 1);

        let report = engine
            .unregister_query(100, "SELECT * FROM orders WHERE amount > 100")
            .unwrap();

        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 1);
        assert_eq!(report.removed_consumers, 1);
        assert_eq!(engine.subscription_count(), 0);
        assert_eq!(engine.predicate_count(1), 0);
    }

    #[test]
    fn test_unregister_query_user_scoping() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let sql = "SELECT * FROM orders WHERE amount > 100".to_string();

        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        engine
            .register(SubscriptionRequest {
                consumer_id: 200,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        assert_eq!(engine.subscription_count(), 2);

        // Remove only user 100's subscription
        let report = engine.unregister_query(100, &sql).unwrap();

        assert_eq!(report.removed_bindings, 1);
        assert_eq!(
            report.removed_predicates, 0,
            "predicate still has refcount from user 200"
        );
        assert_eq!(report.removed_consumers, 1);
        assert_eq!(engine.subscription_count(), 1);
        assert_eq!(engine.predicate_count(1), 1);
    }

    #[test]
    fn test_unregister_query_no_match() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Unregister a different query
        let report = engine
            .unregister_query(100, "SELECT * FROM orders WHERE amount > 200")
            .unwrap();

        assert_eq!(report.removed_bindings, 0);
        assert_eq!(report.removed_predicates, 0);
        assert_eq!(report.removed_consumers, 0);
        assert_eq!(engine.subscription_count(), 1);
    }

    #[test]
    fn test_unregister_query_normalization_equivalence() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register with one ordering of AND clauses
        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: "SELECT * FROM orders WHERE amount > 100 AND status = 'paid'".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Unregister with reversed AND clause order — should still match
        let report = engine
            .unregister_query(
                100,
                "SELECT * FROM orders WHERE status = 'paid' AND amount > 100",
            )
            .unwrap();

        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 1);
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_unregister_query_projection_specificity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let where_clause = "WHERE amount > 100";

        // Register SELECT * (row subscription)
        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: format!("SELECT * FROM orders {where_clause}"),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Register SELECT COUNT(*) (aggregate subscription)
        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: format!("SELECT COUNT(*) FROM orders {where_clause}"),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        assert_eq!(engine.subscription_count(), 2);
        assert_eq!(engine.predicate_count(1), 2);

        // Unregister only the row subscription
        let report = engine
            .unregister_query(100, &format!("SELECT * FROM orders {where_clause}"))
            .unwrap();

        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 1);
        // Consumer still has the COUNT(*) subscription, so not removed from dict
        assert_eq!(report.removed_consumers, 0);
        assert_eq!(engine.subscription_count(), 1);
        assert_eq!(engine.predicate_count(1), 1);
    }

    #[test]
    fn test_unregister_query_multiple_subscriptions_same_user() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let sql = "SELECT * FROM orders WHERE amount > 100".to_string();

        // Same user registers same query with different scopes to create distinct subscriptions.
        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Durable,
                sql: sql.clone(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        engine
            .register(SubscriptionRequest {
                consumer_id: 100,
                scope: SubscriptionScope::Session(1),
                sql: sql.clone(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        assert_eq!(engine.subscription_count(), 2);

        let report = engine.unregister_query(100, &sql).unwrap();

        assert_eq!(report.removed_bindings, 2);
        assert_eq!(report.removed_predicates, 1);
        assert_eq!(report.removed_consumers, 1);
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_unregister_query_invalid_sql() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let result = engine.unregister_query(100, "NOT VALID SQL AT ALL");
        assert!(result.is_err());
    }

    // =========================================================================
    // View-relative delta tests for UPDATE events
    // =========================================================================

    /// Cross-subscription transition: row moves from sub A (x=3) to sub B (x=4).
    #[test]
    fn test_update_cross_subscription_transition() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Sub A: amount = 3
        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount = 3",
            ))
            .unwrap();
        // Sub B: amount = 4
        engine
            .register(SubscriptionRequest::new(
                2,
                "SELECT * FROM orders WHERE amount = 4",
            ))
            .unwrap();

        // UPDATE: amount 3 → 4
        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(4), Cell::String("ok".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(3), Cell::String("ok".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        // Sub B enters (new matches, old doesn't) → inserted
        assert_eq!(notifs.inserted(), vec![2]);
        // Sub A leaves (old matches, new doesn't) → deleted
        assert_eq!(notifs.deleted(), vec![1]);
        // No one stayed → updated is empty
        assert!(notifs.updated().is_empty());
    }

    /// Same-subscription update: row changes but stays in the result set.
    #[test]
    fn test_update_same_subscription_stays() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount > 0",
            ))
            .unwrap();

        // UPDATE: amount 3 → 4, both match `amount > 0`
        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(4), Cell::String("ok".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(3), Cell::String("ok".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        assert!(notifs.inserted().is_empty());
        assert!(notifs.deleted().is_empty());
        assert_eq!(notifs.updated(), vec![1]);
    }

    /// Leave-all: row leaves the only matching subscription.
    #[test]
    fn test_update_leaves_all_subscriptions() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount = 3",
            ))
            .unwrap();

        // UPDATE: amount 3 → 4, leaves the subscription
        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(4), Cell::String("ok".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(3), Cell::String("ok".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        assert!(notifs.inserted().is_empty());
        assert_eq!(notifs.deleted(), vec![1]);
        assert!(notifs.updated().is_empty());
    }

    /// Enter-from-nothing: row enters a subscription it didn't match before.
    #[test]
    fn test_update_enters_subscription() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount = 4",
            ))
            .unwrap();

        // UPDATE: amount 3 → 4, enters the subscription
        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(4), Cell::String("ok".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(3), Cell::String("ok".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        assert_eq!(notifs.inserted(), vec![1]);
        assert!(notifs.deleted().is_empty());
        assert!(notifs.updated().is_empty());
    }

    /// INSERT event produces only `inserted`, no `deleted` or `updated`.
    #[test]
    fn test_insert_produces_only_inserted() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount > 0",
            ))
            .unwrap();

        let event = build_insert_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(10), Cell::String("ok".into())]),
            },
        );

        let notifs = engine.consumers(&event).unwrap();
        assert_eq!(notifs.inserted(), vec![1]);
        assert!(notifs.deleted().is_empty());
        assert!(notifs.updated().is_empty());
    }

    /// DELETE event produces only `deleted`, no `inserted` or `updated`.
    #[test]
    fn test_delete_produces_only_deleted() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount > 0",
            ))
            .unwrap();

        let event = build_delete_event(
            1,
            PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(10), Cell::String("ok".into())]),
            },
        );

        let notifs = engine.consumers(&event).unwrap();
        assert!(notifs.inserted().is_empty());
        assert_eq!(notifs.deleted(), vec![1]);
        assert!(notifs.updated().is_empty());
    }

    /// Missing old_row for UPDATE → graceful degradation (single-eval fallback).
    /// All matching consumers go to `updated` since we can't distinguish.
    #[test]
    fn test_update_missing_old_row_falls_back() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount > 0",
            ))
            .unwrap();

        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(10), Cell::String("ok".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(None)
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        // Fallback: all matches go to `updated`
        assert!(notifs.inserted().is_empty());
        assert!(notifs.deleted().is_empty());
        assert_eq!(notifs.updated(), vec![1]);
    }

    /// Partial old_row (Cell::Missing) for UPDATE → graceful degradation.
    #[test]
    fn test_update_partial_old_row_falls_back() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionRequest::new(
                1,
                "SELECT * FROM orders WHERE amount > 0",
            ))
            .unwrap();

        let event = WalEvent::builder(1)
            .update()
            .new_row(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(10), Cell::String("ok".into())]),
            })
            .pk(PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            })
            .maybe_old_row(Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Missing, Cell::String("ok".into())]),
            }))
            .changed_columns(Arc::from([1u16]))
            .build()
            .expect("event builder should be valid");

        let notifs = engine.consumers(&event).unwrap();
        // Fallback: all matches go to `updated`
        assert!(notifs.inserted().is_empty());
        assert!(notifs.deleted().is_empty());
        assert_eq!(notifs.updated(), vec![1]);
    }

    /// into_iter() yields inserted ∪ updated.
    #[test]
    fn test_consumer_notifications_into_iter() {
        let notifs = crate::ConsumerNotifications::<DefaultIds, crate::NoCheckpoint> {
            checkpoint: None,
            inserted: vec![1, 2],
            deleted: vec![3],
            updated: vec![4, 5],
        };
        let all: Vec<u64> = notifs.into_iter().collect();
        assert_eq!(all, vec![1, 2, 4, 5]);
    }

    /// into_iter() size_hint is correct.
    #[test]
    fn test_consumer_notifications_size_hint() {
        let notifs = crate::ConsumerNotifications::<DefaultIds, crate::NoCheckpoint> {
            checkpoint: None,
            inserted: vec![1, 2],
            deleted: vec![3],
            updated: vec![4],
        };
        let iter = notifs.into_iter();
        assert_eq!(iter.size_hint(), (3, Some(3)));
    }

    // ----------------------------------------------------------------
    // Per-`(session, subscription)` resume cursor (Q6.4)
    // ----------------------------------------------------------------

    fn cursor(bytes: &[u8]) -> crate::OpaqueCheckpoint {
        crate::OpaqueCheckpoint(bytes.to_vec())
    }

    fn cursor_engine() -> SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> {
        SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            make_catalog(),
            PostgreSqlDialect {},
        )
    }

    /// First `advance_cursor` on a `(session, sub)` returns `Ok(None)`
    /// and stores the value.
    #[test]
    fn resume_cursor_first_advance_returns_none() {
        let mut e = cursor_engine();
        let result = e.advance_cursor(7u64, 42u64, cursor(b"\x00\x01"));
        assert!(matches!(result, Ok(None)));
        assert_eq!(e.cursor_for(7u64, 42u64), Some(&cursor(b"\x00\x01")));
    }

    /// Monotonic advance returns `Ok(Some(previous))` and updates the
    /// stored value.
    #[test]
    fn resume_cursor_monotonic_advance_returns_previous() {
        let mut e = cursor_engine();
        let _ = e.advance_cursor(7u64, 42u64, cursor(b"\x00\x01")).unwrap();
        let result = e.advance_cursor(7u64, 42u64, cursor(b"\x00\x02"));
        assert!(matches!(result, Ok(Some(_))));
        assert_eq!(result.unwrap(), Some(cursor(b"\x00\x01")));
        assert_eq!(e.cursor_for(7u64, 42u64), Some(&cursor(b"\x00\x02")));
    }

    /// Rewind is rejected with `NonMonotonic` and the stored value is
    /// unchanged.
    #[test]
    fn resume_cursor_rewind_is_rejected_and_does_not_mutate() {
        let mut e = cursor_engine();
        let _ = e.advance_cursor(7u64, 42u64, cursor(b"\x00\x05")).unwrap();
        let result = e.advance_cursor(7u64, 42u64, cursor(b"\x00\x03"));
        assert_eq!(
            result,
            Err(crate::AdvanceCursorError::NonMonotonic {
                previous: cursor(b"\x00\x05"),
                attempted: cursor(b"\x00\x03"),
            })
        );
        assert_eq!(
            e.cursor_for(7u64, 42u64),
            Some(&cursor(b"\x00\x05")),
            "rewind must not mutate the stored cursor"
        );
    }

    /// `force_set_cursor` overwrites unconditionally and returns the
    /// previous value.
    #[test]
    fn resume_cursor_force_set_overwrites_unconditionally() {
        let mut e = cursor_engine();
        let _ = e.advance_cursor(7u64, 42u64, cursor(b"\x00\x09")).unwrap();
        let prev = e.force_set_cursor(7u64, 42u64, cursor(b"\x00\x01"));
        assert_eq!(prev, Some(cursor(b"\x00\x09")));
        assert_eq!(e.cursor_for(7u64, 42u64), Some(&cursor(b"\x00\x01")));
    }

    /// `cursors_for_session` iterates only the requested session.
    #[test]
    fn resume_cursor_cursors_for_session_filters_correctly() {
        let mut e = cursor_engine();
        e.advance_cursor(1u64, 100u64, cursor(b"\x00\x01")).unwrap();
        e.advance_cursor(1u64, 200u64, cursor(b"\x00\x02")).unwrap();
        e.advance_cursor(2u64, 100u64, cursor(b"\x00\x03")).unwrap();
        let mut session_one: Vec<(SubscriptionId, crate::OpaqueCheckpoint)> = e
            .cursors_for_session(1u64)
            .map(|(s, c)| (s, c.clone()))
            .collect();
        session_one.sort_by_key(|(s, _)| *s);
        assert_eq!(
            session_one,
            vec![(100u64, cursor(b"\x00\x01")), (200u64, cursor(b"\x00\x02")),]
        );
    }

    /// `unregister_session` drops every cursor with `session_id == s`
    /// and leaves cursors for other sessions intact.
    #[test]
    fn resume_cursor_unregister_session_drops_session_cursors_only() {
        let mut e = cursor_engine();
        e.advance_cursor(1u64, 100u64, cursor(b"\x00\x01")).unwrap();
        e.advance_cursor(1u64, 200u64, cursor(b"\x00\x02")).unwrap();
        e.advance_cursor(2u64, 100u64, cursor(b"\x00\x03")).unwrap();

        let _ = e.unregister_session(1u64);

        assert_eq!(e.cursor_for(1u64, 100u64), None);
        assert_eq!(e.cursor_for(1u64, 200u64), None);
        assert_eq!(e.cursor_for(2u64, 100u64), Some(&cursor(b"\x00\x03")));
    }

    /// `unregister_subscription` drops every cursor with that
    /// `sub_id`, across every session, and leaves cursors for other
    /// subscriptions intact.
    #[test]
    fn resume_cursor_unregister_subscription_drops_sub_cursors_across_sessions() {
        let mut e = cursor_engine();
        e.advance_cursor(1u64, 100u64, cursor(b"\x00\x01")).unwrap();
        e.advance_cursor(2u64, 100u64, cursor(b"\x00\x02")).unwrap();
        e.advance_cursor(1u64, 200u64, cursor(b"\x00\x03")).unwrap();

        // 100 is not actually a registered subscription, but the cursor
        // cleanup runs unconditionally on the resume_cursors map.
        let _ = e.unregister_subscription(100u64);

        assert_eq!(e.cursor_for(1u64, 100u64), None);
        assert_eq!(e.cursor_for(2u64, 100u64), None);
        assert_eq!(e.cursor_for(1u64, 200u64), Some(&cursor(b"\x00\x03")));
    }

    /// `drop_cursor` returns the removed cursor and clears the entry.
    #[test]
    fn resume_cursor_drop_cursor_returns_and_removes() {
        let mut e = cursor_engine();
        e.advance_cursor(7u64, 42u64, cursor(b"\x00\x01")).unwrap();
        let dropped = e.drop_cursor(7u64, 42u64);
        assert_eq!(dropped, Some(cursor(b"\x00\x01")));
        assert_eq!(e.cursor_for(7u64, 42u64), None);
    }
}
