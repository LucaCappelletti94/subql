#![allow(clippy::type_complexity)]
//! Subscription engine - main public API

mod agg_maintenance;
mod install;

use super::indexes::IndexableAtom;
use super::{
    dispatch::{dispatch_consumers, dispatch_consumers_with_stamps, ConsumerDictionary},
    ids::{ConsumerOrdinal, PredicateId},
    partition::{PartitionTxn, TablePartition},
    predicate::{Predicate, SubscriptionBinding},
};
use crate::backend::{Backend, CdcEvent, RowKind, Value};
use crate::compiler::literals::SqlLiteralParse;
use crate::{
    catalog_helpers,
    compiler::{
        canonicalize, parse_and_resolve_hash, parse_compile_normalize_and_prefilter_with_binds,
        parser::CompiledQuery, sql_shape::QueryProjection, BytecodeProgram, PrefilterPlan, Vm,
    },
    term::{kind_can_key, CompiledTerm, TermDescription, TermKey, TermLookup, TermPlan},
    ColumnId, DispatchError, EventKind, IdTypes, RegisterError, Registered, Served,
    SubscriptionDispatch, SubscriptionId, SubscriptionRegistration, SubscriptionRequest,
    SubscriptionScope, SubscriptionUnregistration, TableId, Tier, UnregisterReport,
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
    DropReason, DroppedRead, DurabilityMode, DurableShardMerge, DurableShardStore, MergeError,
    MergeJobId, MergeReport, RestoredRead, RestoredReads, StorageError,
};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use hashbrown::{HashMap, HashSet};
use sql_traits::prelude::DatabaseLike;
#[cfg(feature = "std")]
use std::io::Write;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
const RLS_AGGREGATE_NEEDS_DATABASE_READ: &str =
    "aggregate on RLS table requires database re-execution";

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
    /// Runnable component-seed bundle (SQL plus per-column decode kinds)
    /// for an aggregate registration, `None` for row subscriptions.
    /// Rendered once at compile time.
    bootstrap: Option<crate::AggregateBootstrap<B>>,
    /// What registration settled about each membership term the filter names,
    /// one per slot, empty for a filter naming none.
    term_plans: Vec<TermPlan>,
    /// The subscriber this subscription filters for, in the form the term lookup
    /// is keyed by. `Some` exactly when the filter names a term.
    term_subscriber: Option<TermKey<B>>,
    /// Per term slot, the value rows the subscription states it matches today.
    term_seeds: Vec<Vec<crate::term::TermRow<B>>>,
}

struct AggregateRegistration<I: IdTypes, B: Backend> {
    consumer: I::ConsumerId,
    scope: SubscriptionScope<I>,
    source_query: crate::reexec::BoundQuery<B>,
    /// Whether the registration declared per-consumer database reads, so a
    /// tier transition builds its replacement read under the same contract.
    /// Not persisted: a restored in-process aggregate starts shared, like
    /// every batch registration.
    database_reads_per_consumer: bool,
}

impl<I: IdTypes, B: Backend> Clone for AggregateRegistration<I, B> {
    fn clone(&self) -> Self {
        Self {
            consumer: self.consumer,
            scope: self.scope,
            source_query: self.source_query.clone(),
            database_reads_per_consumer: self.database_reads_per_consumer,
        }
    }
}

struct RereadRegistration<'a, I: IdTypes, B: Backend> {
    consumer: I::ConsumerId,
    session: Option<I::SessionId>,
    source_query: &'a crate::reexec::BoundQuery<B>,
    database_reads_per_consumer: bool,
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
}

#[cfg(test)]
fn injected_parent_dir_sync_failure_dirs() -> &'static Mutex<HashSet<PathBuf>> {
    INJECT_PARENT_DIR_SYNC_FAILURE_DIRS.get_or_init(|| Mutex::new(HashSet::new()))
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
    /// The caller's own translation settings, which registration needs to ask
    /// whether a membership subquery can be served at all.
    ///
    /// The whole [`Translator`](rls2fga::translator::Translator) rather than the
    /// two values read off it, so the filter and the caller's real read rules are
    /// judged by the same table and the same bar, and so the registry enriched
    /// from the schema is reachable. `None` refuses every term naming the
    /// missing configuration: the session setting that identifies the caller is
    /// deployment-specific, and defaulting it fails silently when wrong.
    #[cfg(feature = "membership-term")]
    translator: Option<rls2fga::translator::Translator>,
    /// Which term slots a change to each table moves, keyed by the table whose
    /// rows carry the membership.
    ///
    /// Cross-table on purpose: the predicate carrying the term lives in the
    /// subscribed table's partition, and the row that moves its subscriber set
    /// arrives on another table entirely.
    ///
    /// In-memory only (not persisted), like
    /// [`pk_follows`](Self::pk_follows): across a restart a durable term
    /// subscription comes back with an empty subscriber set and admits nobody
    /// until the client registers again. That is the freshness-losing direction,
    /// never a row delivered to somebody the filter excludes.
    term_watch: HashMap<TableId, Vec<TermWatch>>,
    /// Running total per aggregate subscription.
    ///
    /// The engine owns these, so an aggregate subscription reports its value
    /// rather than a delta the caller folds. Populated at registration and
    /// dropped with the subscription, and in-memory only: after a restart a
    /// durable aggregate subscription comes back unseeded and reports nothing
    /// until the caller reads its starting numbers again, which is the
    /// silent direction rather than the wrong-number one.
    aggregates:
        HashMap<SubscriptionId, crate::runtime::aggregate::AggregateTotal<I, E::Checkpoint>>,
    /// Running group maps for grouped aggregate subscriptions.
    grouped_aggregates: HashMap<
        SubscriptionId,
        crate::runtime::aggregate::GroupedAggregateTotal<I, E::Backend, E::Checkpoint>,
    >,
    /// Original SQL and registration metadata retained for an in-place
    /// transition from aggregate maintenance to a complete row read.
    aggregate_registrations: HashMap<SubscriptionId, AggregateRegistration<I, E::Backend>>,
    /// Maximum changes held while aggregate starting rows are read.
    max_changes_during_aggregate_read: usize,
    /// Maximum live groups for one grouped aggregate before it changes tier.
    max_groups_per_aggregate: usize,
    /// Answers this engine cannot maintain from the change stream, each with
    /// the tier that re-reads it and the state machine that decides when.
    ///
    /// Keyed by the same identity a served subscription carries, since both are
    /// registrations of the same registry.
    reexec: HashMap<SubscriptionId, crate::reexec::ReExecEntry<I, E::Backend, E::Checkpoint>>,
    /// Table -> the re-read answers a change to it may move.
    table_deps: HashMap<TableId, hashbrown::HashSet<SubscriptionId>>,
    /// Plans for keyed re-reads, kept so a read renders the scoped statement
    /// from the caller's own SQL rather than from a reconstruction.
    keyed_plans: HashMap<SubscriptionId, crate::reexec::plan::KeyedPlan>,
    /// Session -> its re-read answers, for session-scoped cleanup.
    reexec_sessions: HashMap<I::SessionId, Vec<SubscriptionId>>,
}

/// One term whose subscriber set a change to some table moves.
///
/// Keyed from the membership table's side, so it carries where to put the answer
/// back: the subscribed table names the partition holding the predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TermWatch {
    /// The subscribed table, whose partition holds the predicate.
    subscribed: TableId,
    /// The table whose changed rows move this term, which is the key this watch
    /// is indexed under.
    member_table: TableId,
    /// The predicate carrying the term.
    predicate: PredicateId,
    /// The term's slot in that predicate.
    slot: u16,
    /// The columns of the subscribed table the term compares, which is what
    /// the narrowing message names.
    columns: Vec<crate::ColumnId>,
    /// The columns of the changed row carrying the values the term compares
    /// against, pairwise with `columns`.
    member_keys: Vec<crate::ColumnId>,
    /// The column of the changed row naming the subscriber it admits.
    member_subject: crate::ColumnId,
}

/// One term movement read from a membership event, applied under the
/// subscribed partition's transaction.
enum TermAction<B: Backend> {
    /// Move the subscribers claiming `subject` under `values`.
    Move {
        values: crate::term::TermRow<B>,
        subject: TermKey<B>,
        entered: bool,
    },
    /// Withdraw everything the term admits, as a truncate does.
    Clear,
}

/// One subscription's term seeding, held until its predicate id is final.
struct PendingSeed<B: Backend> {
    table: TableId,
    subscription: SubscriptionId,
    subscriber: TermKey<B>,
    plans: Vec<TermPlan>,
    seeds: Vec<Vec<crate::term::TermRow<B>>>,
}

/// The subscriptions bound to `predicate` for each ordinal in `ordinals`.
///
/// Sorted and deduplicated so a narrowing names each subscription once, however
/// many bindings a consumer holds on the predicate.
fn subscriptions_for<I: IdTypes, B: Backend>(
    store: &super::predicate::PredicateStore<I, B>,
    predicate: PredicateId,
    ordinals: &roaring::RoaringBitmap,
) -> Vec<SubscriptionId> {
    let mut ids: Vec<SubscriptionId> = ordinals
        .iter()
        .filter_map(|ordinal| {
            store
                .binding_lookup
                .get(&(predicate, ConsumerOrdinal::new(ordinal)))
        })
        .flatten()
        .copied()
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
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
    /// The SQL dialect used for parsing statements.
    pub(crate) const fn dialect(&self) -> &<E::Backend as Backend>::Dialect {
        &self.dialect
    }

    /// The schema catalog the engine resolves tables/columns against.
    /// Borrow it when you need to read the schema alongside a live engine
    /// (e.g. to resolve a `TableId` for building a typed [`CdcEvent`]
    /// fixture) without keeping a separate catalog instance.
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

    /// Compile `sql` and settle every membership term its filter names, which is
    /// everything registration decides before it needs the seed.
    ///
    /// Shared with [`Self::describe_terms`], so a filter described is a filter
    /// `register` accepts, and one refused is refused there for the same reason.
    fn compile_and_plan_terms(
        &self,
        sql: &str,
        binds: &[Value<E::Backend>],
        database_reads_per_consumer: bool,
    ) -> Result<(CompiledQuery<E::Backend>, Vec<TermPlan>), RegisterError> {
        let compiled = parse_compile_normalize_and_prefilter_with_binds::<E::Backend, DB>(
            sql,
            &self.dialect,
            &self.database,
            binds,
        )?;

        // Registration policy: under RLS, viewers observe different rows,
        // so a single in-process aggregate state cannot be shared across
        // consumers. Reject aggregate registrations on RLS tables here,
        // the sole choke point for both `register` and `register_batch`,
        // so the guarantee holds for every caller and any future entry
        // point. Row subscriptions are filtered per viewer at delivery and
        // stay accepted. MIN/MAX never reach this branch: they fail the
        // compile step above with UnsupportedSql and are captured by the
        // reexec wrapper, which applies its own RLS guard.
        if matches!(
            compiled.projection,
            QueryProjection::Aggregate(_) | QueryProjection::GroupedAggregate { .. }
        ) && catalog_helpers::table_has_rls(&self.database, compiled.table_id)?
        {
            if database_reads_per_consumer {
                return Err(RegisterError::UnsupportedSql(
                    RLS_AGGREGATE_NEEDS_DATABASE_READ.to_string(),
                ));
            }
            return Err(RegisterError::AggregatorOnRlsTable {
                table_id: compiled.table_id,
            });
        }

        let plans =
            self.check_membership_terms(&compiled.terms, compiled.table_id, &compiled.projection)?;
        Ok((compiled, plans))
    }

    /// What each membership subquery in `spec`'s filter needs seeded, without
    /// registering anything.
    ///
    /// A caller comparison appears in no description: it seeds itself from the
    /// subscriber the request states, so there is nothing to read for it.
    ///
    /// [`Self::register`] consumes the seed and an absent one admits nobody, so
    /// the caller's obligation runs before registration, and this is the only
    /// thing that runs the classification `register` runs. Describe the request,
    /// read each [`MembershipTermDescription::seed_sql`](crate::term::MembershipTermDescription::seed_sql) as the caller, state what came
    /// back through [`SubscriptionRequest::subscriber`] and
    /// [`SubscriptionRequest::term_values`], then register that same request.
    ///
    /// Empty for a filter naming no membership subquery, which is every filter
    /// until one is written.
    ///
    /// # Errors
    ///
    /// Whatever `register` answers for the same request short of the seed
    /// itself: [`RegisterError::UnsupportedSql`] for a filter outside SubQL's
    /// shape, [`RegisterError::AggregatorOnRlsTable`] for an aggregate over
    /// row-level security, and [`RegisterError::MembershipTermRefused`] for a
    /// relationship SubQL cannot serve.
    pub fn describe_terms(
        &self,
        spec: &SubscriptionRequest<I, E::Backend>,
    ) -> Result<Vec<TermDescription>, RegisterError> {
        let (compiled, plans) = self.compile_and_plan_terms(&spec.sql, &spec.binds, false)?;
        plans
            .iter()
            .zip(&compiled.terms)
            .map(|(plan, term)| {
                TermDescription::resolve::<E::Backend, _>(
                    plan,
                    term,
                    compiled.table_id,
                    &self.database,
                )
            })
            .collect()
    }

    fn compile_spec(
        &self,
        spec: SubscriptionRequest<I, E::Backend>,
        database_reads_per_consumer: bool,
    ) -> Result<CompiledSpec<I, E::Backend>, RegisterError> {
        let (compiled, term_plans) =
            self.compile_and_plan_terms(&spec.sql, &spec.binds, database_reads_per_consumer)?;
        let CompiledQuery {
            table_id,
            program: bytecode,
            normalized,
            prefilter_plan,
            projection,
            terms,
        } = compiled;

        let (term_subscriber, term_seeds) = self.settle_term_seeds(&terms, table_id, &spec)?;

        // Disambiguate hash: same WHERE clause with different projection
        // kind must map to distinct predicates.
        let hash_input = crate::compiler::parser::projection_hash_input(&normalized, &projection);
        let hash = canonicalize::hash_sql(&hash_input);

        // For an aggregate registration, render the runnable component
        // seed query so callers can bootstrap the accumulator and re-seed
        // after a mandated reset. `None` for row subscriptions.
        let bootstrap = crate::compiler::sql_shape::render_aggregate_bootstrap(
            &spec.sql,
            &spec.binds,
            &projection,
            &self.dialect,
            table_id,
            &self.database,
        );

        Ok(CompiledSpec {
            spec,
            table_id,
            bytecode,
            normalized,
            prefilter_plan,
            projection,
            hash,
            bootstrap,
            term_plans,
            term_subscriber,
            term_seeds,
        })
    }

    /// Settle every membership term the filter names, or refuse the
    /// registration.
    ///
    /// Refusing here rather than serving is the whole point of the term having
    /// two executors: the subquery text serves the snapshot and the compiled
    /// relationship serves the change path, and a filter they answer differently
    /// is silent divergence.
    fn check_membership_terms(
        &self,
        terms: &[CompiledTerm],
        table_id: TableId,
        projection: &QueryProjection,
    ) -> Result<Vec<TermPlan>, RegisterError> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        // One accumulator per aggregate is shared between consumers, and a term
        // gives each consumer a different set of rows to aggregate, so the
        // number one subscriber reads would be another's. Same reason an
        // aggregate on a table with row-level security is refused above.
        if matches!(
            projection,
            QueryProjection::Aggregate(_) | QueryProjection::GroupedAggregate { .. }
        ) {
            return Err(RegisterError::MembershipTermRefused(
                "an aggregate cannot carry a membership term. SubQL keeps one accumulator \
                 per aggregate and shares it between consumers, and a membership term gives \
                 each consumer a different set of rows to aggregate."
                    .to_string(),
            ));
        }

        for term in terms {
            // The lookup is keyed by the compared columns' values, so each
            // column has to hold a kind whose equality is reflexive. A float, a
            // JSON or a JSONB cell could not find what it stored.
            for &column in &term.columns {
                let kind = catalog_helpers::column_scalar_kind::<E::Backend, _>(
                    &self.database,
                    table_id,
                    column,
                )
                .ok_or_else(|| {
                    RegisterError::MembershipTermRefused(format!(
                        "column {column} of table {table_id} has a SQL type SubQL cannot read, \
                         so a membership subquery comparing it has nothing to look up"
                    ))
                })?;
                if !kind_can_key::<E::Backend>(kind) {
                    return Err(RegisterError::MembershipTermRefused(format!(
                        "a membership subquery cannot compare a {kind:?} column. SubQL looks the \
                         column's value up to decide which subscribers a changed row admits, and \
                         a {kind:?} value is not equal to itself, so the lookup could not find \
                         what it stored."
                    )));
                }
            }

            // Two terms comparing one column would make that column's name
            // ambiguous in the values a subscription states for it, and the
            // names are the only thing both sides share.
            if terms.iter().any(|other| {
                other.slot != term.slot
                    && other
                        .columns
                        .iter()
                        .any(|column| term.columns.contains(column))
            }) {
                let column = term.columns[0];
                return Err(RegisterError::MembershipTermRefused(format!(
                    "two membership terms in one filter compare column {column} of table \
                     {table_id}. A subscription states the values it matches per compared column, \
                     so two terms on one column leave SubQL unable to tell which values \
                     belong to which.",
                )));
            }
        }

        self.plan_membership_terms(terms, table_id)
    }

    /// Turn what the subscription stated into the keys the term lookup stores,
    /// or refuse the registration.
    ///
    /// Returns the subscriber's key and, per term slot, the value rows it
    /// states it matches today. Both are empty for a filter naming no term.
    /// This runs before anything is bound, so seeding afterwards cannot fail
    /// halfway and leave a subscription registered against a term it was never
    /// added to.
    #[allow(
        clippy::type_complexity,
        reason = "the return pairs the subscriber key with per-slot seed rows, and a name \
                  would hide which side is which"
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "one pass settles the caller seeds and the stated rows, and splitting it \
                  would let the two halves disagree about the slot layout"
    )]
    fn settle_term_seeds(
        &self,
        terms: &[CompiledTerm],
        table_id: TableId,
        spec: &SubscriptionRequest<I, E::Backend>,
    ) -> Result<
        (
            Option<TermKey<E::Backend>>,
            Vec<Vec<crate::term::TermRow<E::Backend>>>,
        ),
        RegisterError,
    > {
        if terms.is_empty() {
            return Ok((None, Vec::new()));
        }

        let subscriber = spec.subscriber.clone().ok_or_else(|| {
            RegisterError::MembershipTermRefused(
                "a filter naming a membership term has to say which subscriber it filters \
                 for. SubQL matches a changed membership row against that identity to move who \
                 the filter admits, and a caller comparison admits exactly that identity, so \
                 without one the subscription could never deliver."
                    .to_string(),
            )
        })?;
        let TermLookup::Key(subscriber) = TermLookup::of(subscriber) else {
            return Err(RegisterError::MembershipTermRefused(
                "the subscriber this subscription filters for is null, or of a kind SubQL cannot \
                 look up. An identity has to be a value equal to itself for a membership row to \
                 be matched against it."
                    .to_string(),
            ));
        };

        // A caller comparison seeds itself: the one value it admits is the
        // subscriber, and it has to be of the compared column's kind, or the
        // lookup would store a key no row's cell can ever equal and serve the
        // subscription dead.
        let mut seeds: Vec<Vec<crate::term::TermRow<E::Backend>>> =
            alloc::vec![Vec::new(); terms.len()];
        for term in terms.iter().filter(|term| term.compares_the_caller()) {
            let compared = catalog_helpers::column_scalar_kind::<E::Backend, _>(
                &self.database,
                table_id,
                term.columns[0],
            );
            let stated = subscriber.scalar_kind();
            if compared != Some(stated) {
                return Err(RegisterError::MembershipTermRefused(format!(
                    "the subscriber this subscription filters for is of kind {stated:?}, and its \
                     caller comparison reads a column of kind {compared:?}, so no row could ever \
                     name it. Build the identity at the compared column's kind."
                )));
            }
            seeds[usize::from(term.slot)].push(alloc::vec![subscriber.clone()]);
        }

        for (column_names, rows) in &spec.term_values {
            let stated: Vec<crate::ColumnId> = column_names
                .iter()
                .map(|name| {
                    catalog_helpers::column_id(&self.database, table_id, name).ok_or_else(|| {
                        RegisterError::MembershipTermRefused(format!(
                            "this subscription states the values it matches for column {name:?}, \
                             which table {table_id} does not carry."
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            // The one term comparing exactly this column set, since two terms
            // sharing any column were refused above. The caller's order may
            // differ from the filter's, so each stated row is permuted into the
            // term's own order below.
            let term = terms
                .iter()
                .find(|term| {
                    term.columns.len() == stated.len()
                        && stated.iter().all(|column| term.columns.contains(column))
                        && term.columns.iter().all(|column| stated.contains(column))
                })
                .ok_or_else(|| {
                    RegisterError::MembershipTermRefused(format!(
                        "this subscription states the values it matches for columns \
                         {column_names:?}, which no membership subquery in its filter compares \
                         together. The values would be stored where nothing reads them."
                    ))
                })?;
            if term.compares_the_caller() {
                return Err(RegisterError::MembershipTermRefused(format!(
                    "this subscription states values for columns {column_names:?}, which its \
                     filter compares to the caller directly. A caller comparison seeds itself \
                     from the subscriber, and stated values would admit rows the filter's text \
                     never names."
                )));
            }
            let order: Vec<usize> = term
                .columns
                .iter()
                .map(|column| {
                    stated
                        .iter()
                        .position(|candidate| candidate == column)
                        .expect("the sets were matched column for column above")
                })
                .collect();
            let slot = usize::from(term.slot);
            for row in rows {
                if row.len() != order.len() {
                    return Err(RegisterError::MembershipTermRefused(format!(
                        "a value row stated for columns {column_names:?} carries {got} values \
                         where {expected} columns were named, so SubQL cannot say which value \
                         belongs to which column.",
                        got = row.len(),
                        expected = order.len(),
                    )));
                }
                let mut keys = Vec::with_capacity(order.len());
                for &position in &order {
                    let TermLookup::Key(key) = TermLookup::of(row[position].clone()) else {
                        return Err(RegisterError::MembershipTermRefused(format!(
                            "one of the values stated for columns {column_names:?} is null, or \
                             of a kind SubQL cannot look up. A value the subscriber matches has \
                             to be equal to itself, and SQL never admits a row through a null."
                        )));
                    };
                    keys.push(key);
                }
                seeds[slot].push(keys);
            }
        }

        Ok((Some(subscriber), seeds))
    }

    /// Seed one newly bound subscription into its predicate's term lookups, and
    /// report which tables now move them.
    ///
    /// Nothing to do for a filter naming no term, which is the case for every
    /// filter until one is registered.
    fn seed_terms(
        txn: &mut PartitionTxn<'_, I, E::Backend>,
        compiled: &CompiledSpec<I, E::Backend>,
        pred_id: PredicateId,
        ordinal: ConsumerOrdinal,
    ) -> Vec<TermWatch> {
        let Some(subscriber) = compiled.term_subscriber.as_ref() else {
            return Vec::new();
        };
        txn.seed_terms(pred_id, ordinal, subscriber, &compiled.term_seeds);

        compiled
            .term_plans
            .iter()
            .filter_map(|plan| {
                let movement = plan.moved_by.as_ref()?;
                Some(TermWatch {
                    subscribed: compiled.table_id,
                    predicate: pred_id,
                    slot: plan.slot,
                    columns: plan.columns.clone(),
                    member_table: movement.member_table,
                    member_keys: movement.member_keys.clone(),
                    member_subject: movement.member_subject,
                })
            })
            .collect()
    }

    /// Index the tables whose changes move a term, skipping a watch already
    /// registered because another subscription shares the predicate.
    fn watch_terms(&mut self, watches: Vec<TermWatch>) {
        for watch in watches {
            let indexed = self.term_watch.entry(watch.member_table).or_default();
            if !indexed.contains(&watch) {
                indexed.push(watch);
            }
        }
    }

    /// Ask `rls2fga` whether each term's relationship can be served, and resolve
    /// the answer to subql's ids.
    #[cfg(feature = "membership-term")]
    fn plan_membership_terms(
        &self,
        terms: &[CompiledTerm],
        table_id: TableId,
    ) -> Result<Vec<TermPlan>, RegisterError> {
        let translator = self.translator.as_ref().ok_or_else(|| {
            RegisterError::MembershipTermRefused(
                "this engine was built without translation settings, so SubQL cannot tell \
                 whether the relationship a membership subquery names can be served. Pass the \
                 caller's own translator through with_translator."
                    .to_string(),
            )
        })?;
        let table_name =
            catalog_helpers::table_name(&self.database, table_id).ok_or_else(|| {
                RegisterError::MembershipTermRefused(format!(
                    "table {table_id} is not in the catalog under a name, so the relationship a \
                 membership subquery names cannot be classified"
                ))
            })?;

        terms
            .iter()
            .map(|term| {
                crate::term_compile::plan_term::<E::Backend, _>(
                    term,
                    table_id,
                    &table_name,
                    &self.database,
                    translator,
                )
            })
            .collect()
    }

    /// Refuse every term, naming the feature that compiles one.
    ///
    /// The bounded form is recognised in every build so that one build does not
    /// accept a filter another refuses, and this says which of the two the reader
    /// is holding rather than claiming SubQL cannot run a nested `SELECT`.
    #[cfg(not(feature = "membership-term"))]
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    fn plan_membership_terms(
        &self,
        _terms: &[CompiledTerm],
        _table_id: TableId,
    ) -> Result<Vec<TermPlan>, RegisterError> {
        Err(RegisterError::MembershipTermRefused(
            "this build was compiled without the membership-term feature, which is what serves \
             a membership term at all. The filter itself is one SubQL recognises."
                .to_string(),
        ))
    }

    fn make_predicate_from_compiled(
        database: &DB,
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
            let (agg_col, group_cols): (Option<ColumnId>, &[ColumnId]) = match &compiled.projection
            {
                QueryProjection::Aggregate(spec) => (spec.column(), &[]),
                QueryProjection::GroupedAggregate { groups, agg, .. } => (agg.column(), groups),
                QueryProjection::Rows => (None, &[]),
            };
            for &column in group_cols {
                if !dep_cols.contains(&column) {
                    dep_cols.push(column);
                }
            }
            if let Some(column) = agg_col {
                if !dep_cols.contains(&column) {
                    dep_cols.push(column);
                    dep_cols.sort_unstable();
                }
            }
            Arc::from(dep_cols.as_slice())
        };
        let group_key_encoder = match &compiled.projection {
            QueryProjection::GroupedAggregate { groups, .. } => groups
                .iter()
                .map(|column| {
                    catalog_helpers::group_key_column::<E::Backend, _>(
                        database,
                        compiled.table_id,
                        *column,
                    )
                })
                .collect::<Option<Vec<_>>>()
                .and_then(E::Backend::group_key_encoder),
            QueryProjection::Rows | QueryProjection::Aggregate(_) => None,
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
            group_key_encoder,
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
            aggregate_registrations: HashMap::new(),
            #[cfg(feature = "membership-term")]
            translator: None,
            term_watch: HashMap::new(),
            aggregates: HashMap::new(),
            grouped_aggregates: HashMap::new(),
            max_changes_during_aggregate_read:
                super::aggregate::DEFAULT_MAX_CHANGES_DURING_AGGREGATE_READ,
            max_groups_per_aggregate: super::aggregate::DEFAULT_MAX_GROUPS_PER_AGGREGATE,
            reexec: HashMap::new(),
            table_deps: HashMap::new(),
            keyed_plans: HashMap::new(),
            reexec_sessions: HashMap::new(),
        }
    }

    /// Cap how many changes one aggregate subscription holds while its
    /// starting numbers are being read.
    ///
    /// The engine keeps a change per stream position until the numbers land so
    /// it can drop the ones the read already saw rather than counting them
    /// twice. Past this ceiling it can no longer tell them apart, so
    /// [`Install::install`](crate::Install::install) with [`AggregateSeedInstall`](crate::AggregateSeedInstall) refuses with
    /// [`AggregateInstallError::TooManyChangesDuringRead`](crate::AggregateInstallError::TooManyChangesDuringRead)
    /// and the caller reads again. Zero would refuse every seed that raced at
    /// all, so it clamps to one.
    #[must_use]
    pub const fn with_max_changes_during_aggregate_read(mut self, cap: usize) -> Self {
        self.max_changes_during_aggregate_read = if cap == 0 { 1 } else { cap };
        self
    }

    /// Set the maximum live groups for one grouped aggregate subscription.
    ///
    /// A new group beyond this limit changes the subscription to
    /// [`Tier::WholeRows`] under the same identity. Zero clamps to one.
    #[must_use]
    pub const fn with_max_groups_per_aggregate(mut self, limit: usize) -> Self {
        self.max_groups_per_aggregate = if limit == 0 { 1 } else { limit };
        self
    }

    /// Supply the translation settings a membership subquery is judged by.
    ///
    /// Without this, a filter naming a membership subquery is refused: deciding
    /// whether one can be served needs to know which session setting identifies
    /// the caller and how confident a classification has to be, and both are the
    /// caller's to state. Passing the whole
    /// [`Translator`](rls2fga::translator::Translator) rather than those two
    /// values is what keeps the filter and the caller's real read rules judged by
    /// the same table and the same bar.
    #[cfg(feature = "membership-term")]
    #[must_use]
    pub fn with_translator(mut self, translator: rls2fga::translator::Translator) -> Self {
        self.translator = Some(translator);
        self
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
    /// evicted ids are surfaced in [`Registered::evicted`].
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
    ) -> Result<(Self, RestoredReads<E::Backend>), StorageError> {
        let mut engine = Self::new(database, dialect);
        engine.storage_path = Some(storage_path.clone());

        // Create storage directory if it doesn't exist
        std::fs::create_dir_all(&storage_path)
            .map_err(|e| StorageError::Io(format!("Failed to create storage directory: {e}")))?;

        // Load existing shards
        engine.load_all_shards()?;
        // Then the answers that need a read, which are judged one at a time
        // against the tables they name. The report is returned rather than
        // stored because an answer that could not come back is a subscription
        // the caller still holds an id for.
        let reads = engine.load_reads()?;

        Ok((engine, reads))
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
    /// assert!(first.served().expect("in process").created_new_predicate);
    /// assert!(!second.served().expect("in process").created_new_predicate);
    /// assert_eq!(engine.predicate_count(orders_id), 1);
    /// assert_eq!(engine.subscription_count(), 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn register<R>(&mut self, spec: R) -> Result<Registered<E::Backend>, RegisterError>
    where
        R: crate::RegistrationRequest<I, E::Backend>,
    {
        self.register_request(spec.into_request(), R::DATABASE_READS_PER_CONSUMER)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn register_request(
        &mut self,
        spec: SubscriptionRequest<I, E::Backend>,
        database_reads_per_consumer: bool,
    ) -> Result<Registered<E::Backend>, RegisterError> {
        let source_query = crate::reexec::BoundQuery::new(spec.sql.clone(), spec.binds.clone());
        // 1. Parse, compile, and canonicalize in one pass. A statement this
        // engine cannot serve is planned as a re-read rather than refused:
        // needing a database read is not grounds to turn a query away.
        let consumer_id = spec.consumer_id;
        let session = match &spec.scope {
            SubscriptionScope::Session(s) => Some(*s),
            SubscriptionScope::Durable => None,
        };
        let compiled = match self.compile_spec(spec, database_reads_per_consumer) {
            Ok(compiled) => compiled,
            Err(RegisterError::UnsupportedSql(refusal)) => {
                return self.plan_reread(
                    &source_query,
                    refusal,
                    consumer_id,
                    session,
                    database_reads_per_consumer,
                )
            }
            Err(other) => return Err(other),
        };
        let table_id = compiled.table_id;
        let hash = compiled.hash;

        // 2. Check dedup index: same (consumer_id, predicate_hash, scope) -> idempotent return.
        let natural_key = (compiled.spec.consumer_id, hash, compiled.spec.scope);
        if let Some(&existing_sub_id) = self.binding_dedup.get(&natural_key) {
            return Ok(Registered {
                subscription_id: existing_sub_id,
                tier: Tier::InProcess(Served {
                    table_id,
                    normalized_sql: compiled.normalized,
                    predicate_hash: hash,
                    created_new_predicate: false,
                    projection: compiled.projection,
                    aggregate_bootstrap: compiled.bootstrap,
                }),
                evicted: Vec::new(),
                not_served_because: None,
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
        let subscription_id = self.allocate_subscription_id();

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

        // 6 and 7: dedup the predicate, bind, and seed, as one mutation of the
        // partition publishing one snapshot.
        let (created_new, watches) = partition.mutate(|txn| {
            let existing = txn.store().find_by_hash_and_sql(hash, &compiled.normalized);
            let (pred_id, created_new) = existing.map_or_else(
                || {
                    let (pred, atoms) =
                        Self::make_predicate_from_compiled(&self.database, &compiled);
                    (txn.add_predicate(pred, &atoms), true)
                },
                |existing| (existing, false),
            );
            let binding =
                Self::make_binding(&compiled.spec, subscription_id, pred_id, consumer_ord);
            txn.add_binding(binding, pred_id);
            let watches = Self::seed_terms(txn, &compiled, pred_id, consumer_ord);
            (created_new, watches)
        });
        self.watch_terms(watches);

        // 8. Index subscription for O(1) unregister/upsert lookups.
        self.subscription_to_table.insert(subscription_id, table_id);
        self.binding_dedup.insert(natural_key, subscription_id);
        self.open_aggregate_total(
            subscription_id,
            &compiled.spec,
            compiled.table_id,
            &compiled.projection,
            database_reads_per_consumer,
        );
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

        Ok(Registered {
            subscription_id,
            tier: Tier::InProcess(Served {
                table_id,
                normalized_sql: compiled.normalized,
                predicate_hash: hash,
                created_new_predicate: created_new,
                projection: compiled.projection,
                aggregate_bootstrap: compiled.bootstrap,
            }),
            evicted,
            not_served_because: None,
        })
    }

    /// Take the next identity from the one counter every maintained answer
    /// draws from, whether this engine serves it or a captured query needs a
    /// read to.
    pub(crate) const fn allocate_subscription_id(&mut self) -> SubscriptionId {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;
        id
    }

    /// Plan a re-read tier for a statement this engine cannot serve itself.
    ///
    /// Needing a read never refuses a registration: the tier reports which
    /// read, and whoever holds a database connection services it. `refusal` is
    /// the compiler's own message, returned when no tier can serve it either.
    fn plan_reread(
        &mut self,
        source_query: &crate::reexec::BoundQuery<E::Backend>,
        refusal: String,
        consumer_id: I::ConsumerId,
        session: Option<I::SessionId>,
        database_reads_per_consumer: bool,
    ) -> Result<Registered<E::Backend>, RegisterError> {
        if database_reads_per_consumer && refusal == RLS_AGGREGATE_NEEDS_DATABASE_READ {
            return self.plan_whole_reread(source_query, refusal, consumer_id, session);
        }
        let planned = crate::reexec::plan::build_plan::<E::Backend, DB>(
            source_query,
            self.dialect(),
            &self.database,
        );
        let registration = RereadRegistration {
            consumer: consumer_id,
            session,
            source_query,
            database_reads_per_consumer,
        };
        match planned {
            Ok(planned) => {
                self.registration_rls_refusal(&planned, database_reads_per_consumer)?;
                let subscription_id = self.allocate_subscription_id();
                let mut registered = match planned {
                    crate::reexec::plan::QueryPlan::GroupedPartial(plan) => {
                        self.capture_grouped_scalar(subscription_id, *plan, &registration)
                    }
                    crate::reexec::plan::QueryPlan::Partial(plan) => {
                        self.capture_scalar(subscription_id, plan, &registration)
                    }
                    crate::reexec::plan::QueryPlan::Keyed(plan) => {
                        self.capture_keyed(subscription_id, *plan, &registration)
                    }
                    crate::reexec::plan::QueryPlan::Total(plan) => {
                        self.capture_whole(subscription_id, plan, &registration)
                    }
                };
                registered.not_served_because = Some(refusal);
                self.persist_reads_after_change(subscription_id)?;
                Ok(registered)
            }
            // No tier can serve it either, so the compiler's refusal stands.
            Err(_) => Err(RegisterError::UnsupportedSql(refusal)),
        }
    }

    /// The registration-time row-security refusal for one planned tier, or
    /// `Ok(())` when it may register.
    ///
    /// A shared answer is unsafe when the read's table filters rows per
    /// viewer, unless the subscription reads per consumer, which is exactly
    /// the mode that stays safe under row-level security.
    fn registration_rls_refusal(
        &self,
        planned: &crate::reexec::plan::QueryPlan<E::Backend>,
        database_reads_per_consumer: bool,
    ) -> Result<(), RegisterError> {
        if database_reads_per_consumer {
            return Ok(());
        }
        match planned {
            crate::reexec::plan::QueryPlan::GroupedPartial(plan) => {
                if crate::catalog_helpers::table_has_rls(&self.database, plan.table_id)? {
                    return Err(RegisterError::AggregatorOnRlsTable {
                        table_id: plan.table_id,
                    });
                }
            }
            crate::reexec::plan::QueryPlan::Partial(plan) => {
                if crate::catalog_helpers::table_has_rls(&self.database, plan.table_id)? {
                    return Err(RegisterError::AggregatorOnRlsTable {
                        table_id: plan.table_id,
                    });
                }
            }
            crate::reexec::plan::QueryPlan::Keyed(plan) => {
                if crate::catalog_helpers::table_has_rls(&self.database, plan.table)? {
                    return Err(RegisterError::RowCaptureOnRlsTable {
                        table_id: plan.table,
                    });
                }
            }
            crate::reexec::plan::QueryPlan::Total(plan) => {
                for table in plan.tables.iter().copied() {
                    if crate::catalog_helpers::table_has_rls(&self.database, table)? {
                        return Err(RegisterError::RowCaptureOnRlsTable { table_id: table });
                    }
                }
            }
        }
        Ok(())
    }
    fn plan_whole_reread(
        &mut self,
        source_query: &crate::reexec::BoundQuery<E::Backend>,
        refusal: String,
        consumer_id: I::ConsumerId,
        session: Option<I::SessionId>,
    ) -> Result<Registered<E::Backend>, RegisterError> {
        let plan = crate::reexec::plan::build_whole_rows_plan::<E::Backend, DB>(
            source_query,
            self.dialect(),
            &self.database,
        )?;
        let subscription_id = self.allocate_subscription_id();
        let registration = RereadRegistration {
            consumer: consumer_id,
            session,
            source_query,
            database_reads_per_consumer: true,
        };
        let mut registered = self.capture_whole(subscription_id, plan, &registration);
        registered.not_served_because = Some(refusal);
        self.persist_reads_after_change(subscription_id)?;
        Ok(registered)
    }

    /// Route `subscription_id` from every table in `tables`, and from its
    /// session when it has one.
    fn index_reread(
        &mut self,
        subscription_id: SubscriptionId,
        tables: &[TableId],
        session: Option<I::SessionId>,
    ) {
        for table in tables {
            self.table_deps
                .entry(*table)
                .or_default()
                .insert(subscription_id);
        }
        if let Some(s) = session {
            self.reexec_sessions
                .entry(s)
                .or_default()
                .push(subscription_id);
        }
    }

    /// Register grouped extrema with reads scoped to one displaced group.
    fn capture_grouped_scalar(
        &mut self,
        subscription_id: SubscriptionId,
        plan: crate::reexec::plan::GroupedMinMaxPlan<E::Backend>,
        registration: &RereadRegistration<'_, I, E::Backend>,
    ) -> Registered<E::Backend> {
        let table_id = plan.table_id;
        let bootstrap = plan.bootstrap.clone();
        let read_query = bootstrap.query.clone();
        let runtime = crate::reexec::maintain::QueryRuntime::Grouped(alloc::boxed::Box::new(
            crate::reexec::maintain::GroupedMinMaxQuery::new(
                plan,
                registration.database_reads_per_consumer,
            ),
        ));
        self.index_reread(subscription_id, &[table_id], registration.session);
        self.reexec.insert(
            subscription_id,
            crate::reexec::ReExecEntry {
                consumer_id: registration.consumer,
                session: registration.session,
                tables: alloc::vec![table_id],
                runtime,
                source_query: registration.source_query.clone(),
                read_query,
                tier: crate::ReadTier::GroupedScalar,
                database_reads_per_consumer: registration.database_reads_per_consumer,
            },
        );
        Registered {
            subscription_id,
            tier: Tier::GroupedScalar { bootstrap },
            evicted: Vec::new(),
            not_served_because: None,
        }
    }

    /// Register a scalar extreme, re-read when the extreme itself leaves.
    fn capture_scalar(
        &mut self,
        subscription_id: SubscriptionId,
        plan: crate::reexec::plan::MinMaxPlan<E::Backend>,
        registration: &RereadRegistration<'_, I, E::Backend>,
    ) -> Registered<E::Backend> {
        let crate::reexec::plan::MinMaxPlan {
            table_id,
            kind,
            agg_column,
            agg_kind,
            dependency_columns,
            where_program,
            read_query,
        } = plan;
        let tier_query = read_query.clone();

        // The answer is unknown until someone reads it, which is not the same
        // as an empty one: an unknown extreme asks rather than guessing.
        let runtime = crate::reexec::maintain::QueryRuntime::Partial(
            crate::reexec::maintain::MinMaxQuery::new(
                kind,
                agg_column,
                where_program,
                dependency_columns,
                registration.database_reads_per_consumer,
            ),
        );
        self.index_reread(subscription_id, &[table_id], registration.session);
        self.reexec.insert(
            subscription_id,
            crate::reexec::ReExecEntry {
                consumer_id: registration.consumer,
                session: registration.session,
                tables: alloc::vec![table_id],
                runtime,
                source_query: registration.source_query.clone(),
                read_query,
                tier: crate::ReadTier::Scalar,
                database_reads_per_consumer: registration.database_reads_per_consumer,
            },
        );

        Registered {
            subscription_id,
            tier: Tier::Scalar {
                query: tier_query,
                column_kind: agg_kind,
            },
            evicted: Vec::new(),
            not_served_because: None,
        }
    }

    /// Register a whole re-read: every table it reads triggers a fresh read.
    fn capture_whole(
        &mut self,
        subscription_id: SubscriptionId,
        plan: crate::reexec::plan::TotalPlan<E::Backend>,
        registration: &RereadRegistration<'_, I, E::Backend>,
    ) -> Registered<E::Backend> {
        let crate::reexec::plan::TotalPlan {
            tables,
            dependency_columns,
            read_query,
        } = plan;
        let tier_query = read_query.clone();

        self.index_reread(subscription_id, &tables, registration.session);
        self.reexec.insert(
            subscription_id,
            crate::reexec::ReExecEntry {
                consumer_id: registration.consumer,
                session: registration.session,
                tables: tables.clone(),
                runtime: crate::reexec::maintain::QueryRuntime::Total(
                    crate::reexec::maintain::TotalQuery::new(dependency_columns),
                ),
                source_query: registration.source_query.clone(),
                read_query,
                tier: crate::ReadTier::WholeRows,
                database_reads_per_consumer: registration.database_reads_per_consumer,
            },
        );

        Registered {
            subscription_id,
            tier: Tier::WholeRows {
                query: tier_query,
                tables,
            },
            evicted: Vec::new(),
            not_served_because: None,
        }
    }

    /// Register a keyed re-read: one table, and a change asks about the rows
    /// that moved rather than the whole answer.
    fn capture_keyed(
        &mut self,
        subscription_id: SubscriptionId,
        plan: crate::reexec::plan::KeyedPlan,
        registration: &RereadRegistration<'_, I, E::Backend>,
    ) -> Registered<E::Backend> {
        let table = plan.table;
        self.index_reread(subscription_id, &[table], registration.session);
        let read_query = crate::reexec::BoundQuery::new(
            alloc::format!("{}", plan.statement),
            registration.source_query.binds().to_vec(),
        );
        let tier_query = read_query.clone();
        let dependency_columns = plan.dependency_columns.clone();
        self.keyed_plans.insert(subscription_id, plan);
        self.reexec.insert(
            subscription_id,
            crate::reexec::ReExecEntry {
                consumer_id: registration.consumer,
                session: registration.session,
                tables: alloc::vec![table],
                runtime: crate::reexec::maintain::QueryRuntime::Keyed(
                    crate::reexec::maintain::KeyedQuery::new(dependency_columns),
                ),
                source_query: registration.source_query.clone(),
                read_query,
                tier: crate::ReadTier::KeyedRows,
                database_reads_per_consumer: registration.database_reads_per_consumer,
            },
        );

        Registered {
            subscription_id,
            tier: Tier::KeyedRows {
                query: tier_query,
                table_id: table,
            },
            evicted: Vec::new(),
            not_served_because: None,
        }
    }

    fn transition_reread_to_whole(
        &mut self,
        subscription_id: SubscriptionId,
        from: crate::TierKind,
        reason: crate::MaintenanceStopReason,
        checkpoint: Option<&E::Checkpoint>,
    ) -> Result<
        (
            crate::MaintenanceTransition<E::Backend>,
            crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        ),
        DispatchError,
    > {
        let (consumer, session, source_query, database_reads_per_consumer) = {
            let entry =
                self.reexec
                    .get(&subscription_id)
                    .ok_or_else(|| DispatchError::TierTransition {
                        subscription: subscription_id,
                        message: "read-tier metadata is missing".to_string(),
                    })?;
            (
                entry.consumer_id,
                entry.session,
                entry.source_query.clone(),
                entry.database_reads_per_consumer,
            )
        };
        let plan = crate::reexec::plan::build_whole_rows_plan::<E::Backend, DB>(
            &source_query,
            &self.dialect,
            &self.database,
        )
        .map_err(|error| DispatchError::TierTransition {
            subscription: subscription_id,
            message: error.to_string(),
        })?;
        self.unregister_reread(subscription_id);
        let registration = RereadRegistration {
            consumer,
            session,
            source_query: &source_query,
            database_reads_per_consumer,
        };
        let registered = self.capture_whole(subscription_id, plan, &registration);
        Ok((
            crate::MaintenanceTransition {
                subscription_id,
                from,
                to: registered.tier,
                reason,
            },
            crate::reexec::ReExecutionTrigger {
                subscription_id,
                consumer_id: consumer,
                read: crate::reexec::ReExecutionRead::Subscription,
                checkpoint: checkpoint.cloned(),
            },
        ))
    }

    fn transition_keyed_to_whole(
        &mut self,
        subscription_id: SubscriptionId,
        table_id: TableId,
        checkpoint: Option<&E::Checkpoint>,
    ) -> Result<
        (
            crate::MaintenanceTransition<E::Backend>,
            crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        ),
        DispatchError,
    > {
        self.transition_reread_to_whole(
            subscription_id,
            crate::TierKind::KeyedRows,
            crate::MaintenanceStopReason::KeyedChangeWithoutKey { table_id },
            checkpoint,
        )
    }

    fn transition_grouped_scalar_to_whole(
        &mut self,
        subscription_id: SubscriptionId,
        reason: crate::MaintenanceStopReason,
        checkpoint: Option<&E::Checkpoint>,
    ) -> Result<
        (
            crate::MaintenanceTransition<E::Backend>,
            crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
        ),
        DispatchError,
    > {
        self.transition_reread_to_whole(
            subscription_id,
            crate::TierKind::GroupedScalar,
            reason,
            checkpoint,
        )
    }

    /// The output a stopped tier hands back in place of updates: the same
    /// identity's transition to a whole re-read and the trigger that primes
    /// it, shared by every install path that can hit a limit.
    fn stopped_output(
        subscription_id: SubscriptionId,
        transitioned: Result<
            (
                crate::MaintenanceTransition<E::Backend>,
                crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>,
            ),
            DispatchError,
        >,
    ) -> Result<
        crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>,
        crate::AggregateInstallError,
    > {
        let (transition, trigger) =
            transitioned.map_err(|error| crate::AggregateInstallError::TierTransition {
                subscription: subscription_id,
                message: error.to_string(),
            })?;
        Ok(crate::AggregateMaintenanceOutput {
            updates: Vec::new(),
            triggers: alloc::vec![trigger],
            transitions: alloc::vec![transition],
        })
    }

    /// Feed `event` to every re-read answer whose table it touches.
    ///
    /// Returns the answers that moved in process, and the reads that have to
    /// happen because nothing in process could decide them.
    #[allow(
        clippy::type_complexity,
        clippy::too_many_lines,
        reason = "one routing pass preserves event order across every read tier"
    )]
    pub(crate) fn maintain(
        &mut self,
        event: &E,
    ) -> Result<
        (
            Vec<crate::AggregateValueUpdate<I, E::Backend>>,
            Vec<crate::reexec::ScalarUpdate<I, E::Backend, E::Checkpoint>>,
            Vec<crate::reexec::ReExecutionTrigger<I, E::Checkpoint, E::Backend>>,
            Vec<crate::MaintenanceTransition<E::Backend>>,
        ),
        DispatchError,
    > {
        use crate::reexec::maintain::{Maintenance, QueryRuntime};

        let table_id = event.table_id(&self.database);
        let subscription_ids: Vec<SubscriptionId> = match self.table_deps.get(&table_id) {
            Some(ids) => ids.iter().copied().collect(),
            None => return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new())),
        };
        let pending_cap = self.max_changes_during_aggregate_read;
        let group_limit = self.max_groups_per_aggregate;
        let Self {
            reexec,
            vm,
            database,
            ..
        } = self;

        let mut aggregate_updates = Vec::new();
        let mut scalar_updates = Vec::new();
        let mut triggers = Vec::new();
        let mut keyless = Vec::new();
        let mut grouped_stops = Vec::new();
        let mut transitions = Vec::new();
        for subscription_id in subscription_ids {
            let Some(entry) = reexec.get_mut(&subscription_id) else {
                continue;
            };
            if event.kind() == EventKind::Update {
                let changed = event.changed_columns(database);
                if !changed.is_empty()
                    && !changed
                        .iter()
                        .any(|column| entry.runtime.dependency_columns().contains(column))
                {
                    continue;
                }
            }

            let consumer_id = entry.consumer_id;
            let checkpoint = event.checkpoint();
            if let QueryRuntime::Grouped(query) = &mut entry.runtime {
                let grouped = query
                    .on_event(event, vm, database, pending_cap, group_limit)
                    .map_err(|error| match error {
                        DispatchError::TierTransition { message, .. } => {
                            DispatchError::TierTransition {
                                subscription: subscription_id,
                                message,
                            }
                        }
                        error => error,
                    })?;
                let reason = if grouped.missing_group {
                    Some(crate::MaintenanceStopReason::MissingOldRow { table_id })
                } else if grouped.group_limit {
                    Some(crate::MaintenanceStopReason::GroupLimit { limit: group_limit })
                } else {
                    None
                };
                if let Some(reason) = reason {
                    grouped_stops.push((subscription_id, reason, checkpoint));
                    continue;
                }
                aggregate_updates.extend(grouped.changes.into_iter().map(|(group, change)| {
                    crate::AggregateValueUpdate {
                        subscription: subscription_id,
                        consumer: consumer_id,
                        group: Some(group),
                        change,
                    }
                }));
                triggers.extend(grouped.reads.into_iter().map(|read| {
                    crate::reexec::ReExecutionTrigger {
                        subscription_id,
                        consumer_id,
                        read: crate::reexec::ReExecutionRead::GroupedScalar {
                            group: read.group,
                            query: read.query,
                            column_kinds: read.column_kinds,
                        },
                        checkpoint: read.checkpoint,
                    }
                }));
                continue;
            }
            match entry.runtime.on_event(event, vm, database) {
                Maintenance::Unchanged => {}
                Maintenance::Updated(value) => {
                    scalar_updates.push(crate::reexec::ScalarUpdate {
                        subscription_id,
                        consumer_id,
                        value,
                        checkpoint: checkpoint.clone(),
                    });
                }
                Maintenance::NeedsReexecution => {
                    if let QueryRuntime::Keyed(query) = &mut entry.runtime {
                        if let Some(table_id) = query.take_keyless_change() {
                            keyless.push((subscription_id, table_id, checkpoint.clone()));
                            continue;
                        }
                    }
                    triggers.push(crate::reexec::ReExecutionTrigger {
                        subscription_id,
                        consumer_id,
                        read: crate::reexec::ReExecutionRead::Subscription,
                        checkpoint: checkpoint.clone(),
                    });
                }
            }
        }
        for (subscription_id, table_id, checkpoint) in keyless {
            let (transition, trigger) =
                self.transition_keyed_to_whole(subscription_id, table_id, checkpoint.as_ref())?;
            transitions.push(transition);
            triggers.push(trigger);
        }
        for (subscription_id, reason, checkpoint) in grouped_stops {
            let (transition, trigger) = self.transition_grouped_scalar_to_whole(
                subscription_id,
                reason,
                checkpoint.as_ref(),
            )?;
            transitions.push(transition);
            triggers.push(trigger);
        }
        Ok((aggregate_updates, scalar_updates, triggers, transitions))
    }

    /// Dispatch one event into the compatibility output used by the connector
    /// wrappers while phase B removes that output type.
    pub(crate) fn reread_notifications(
        &mut self,
        event: &E,
    ) -> Result<crate::reexec::ReExecNotifications<I, E::Backend, E::Checkpoint>, DispatchError>
    {
        let engine = match self.consumers(event) {
            Ok(notifications) => notifications,
            Err(DispatchError::UnknownTableId(_))
                if self.routes_reread(event.table_id(&self.database)) =>
            {
                crate::ConsumerNotifications::empty().with_checkpoint(event.checkpoint())
            }
            Err(error) => return Err(error),
        };
        // Fold the ungrouped aggregate channel too, the way `dispatch` does, so
        // a seeded COUNT/SUM/AVG updates through the wrapper facade rather than
        // being silently absorbed. `aggregate_updates` runs before `maintain`
        // so a demotion it triggers is visible to the reexec registry `maintain`
        // then reads. `unseeded_aggregate_triggers` is deliberately left out:
        // the facade seeds an aggregate through `Install`, never auto-bootstraps.
        let mut aggregate = self.aggregate_updates(event)?;
        let (grouped_updates, scalar_updates, mut triggers, mut transitions) =
            self.maintain(event)?;
        aggregate.updates.extend(grouped_updates);
        triggers.extend(aggregate.triggers);
        triggers.sort_unstable_by(|left, right| {
            (left.subscription_id, left.read.group_key())
                .cmp(&(right.subscription_id, right.read.group_key()))
        });
        triggers.dedup_by(|left, right| {
            left.subscription_id == right.subscription_id
                && left.read.group_key() == right.read.group_key()
        });
        transitions.extend(aggregate.transitions);
        Ok(crate::reexec::ReExecNotifications {
            engine,
            aggregate_updates: aggregate.updates,
            scalar_updates,
            rows_updates: Vec::new(),
            row_deltas: Vec::new(),
            triggers,
            transitions,
        })
    }

    /// Whether a change to `table_id` moves any re-read answer.
    pub(crate) fn routes_reread(&self, table_id: TableId) -> bool {
        self.table_deps.contains_key(&table_id)
    }

    /// How many re-read answers this engine holds.
    #[must_use]
    pub fn reread_count(&self) -> usize {
        self.reexec.len()
    }

    /// The keys a keyed re-read has accumulated, left queued until
    /// [`remove_pending_keys`](Self::remove_pending_keys) says a read
    /// delivered them.
    pub(crate) fn clone_pending_keys(
        &self,
        subscription_id: SubscriptionId,
    ) -> Vec<Vec<Value<E::Backend>>> {
        match self.reexec.get(&subscription_id).map(|e| &e.runtime) {
            Some(crate::reexec::maintain::QueryRuntime::Keyed(q)) => q.pending_snapshot(),
            _ => Vec::new(),
        }
    }

    /// Drop exactly the keys a delivered keyed read asked about, keeping any
    /// recorded since its snapshot.
    pub(crate) fn remove_pending_keys(
        &mut self,
        subscription_id: SubscriptionId,
        delivered: &[Vec<Value<E::Backend>>],
    ) {
        if let Some(crate::reexec::maintain::QueryRuntime::Keyed(q)) = self
            .reexec
            .get_mut(&subscription_id)
            .map(|e| &mut e.runtime)
        {
            q.remove_pending(delivered);
        }
    }

    /// The plan a keyed re-read reads with, for rendering its scoped query.
    pub(crate) fn keyed_plan(
        &self,
        subscription_id: SubscriptionId,
    ) -> Option<&crate::reexec::plan::KeyedPlan> {
        self.keyed_plans.get(&subscription_id)
    }

    /// Returns the executable query for a fixed read tier.
    #[must_use]
    pub fn read_query(
        &self,
        subscription_id: SubscriptionId,
    ) -> Option<&crate::reexec::BoundQuery<E::Backend>> {
        let entry = self.reexec.get(&subscription_id)?;
        matches!(
            entry.tier,
            crate::ReadTier::Scalar | crate::ReadTier::KeyedRows | crate::ReadTier::WholeRows
        )
        .then_some(&entry.read_query)
    }

    /// Drop a re-read answer by id, pruning its routing and session indexes.
    pub fn unregister_reread(&mut self, subscription_id: SubscriptionId) -> bool {
        let Some(entry) = self.reexec.remove(&subscription_id) else {
            return false;
        };
        self.keyed_plans.remove(&subscription_id);
        for table in &entry.tables {
            if let Some(set) = self.table_deps.get_mut(table) {
                set.remove(&subscription_id);
                if set.is_empty() {
                    self.table_deps.remove(table);
                }
            }
        }
        if let Some(session) = entry.session {
            if let Some(ids) = self.reexec_sessions.get_mut(&session) {
                ids.retain(|id| *id != subscription_id);
                if ids.is_empty() {
                    self.reexec_sessions.remove(&session);
                }
            }
        }
        true
    }

    /// Rewrite the re-read file after `subscription_id` was added.
    ///
    /// Under [`DurabilityMode::Required`] a write failure fails the
    /// registration and takes the answer back out, so a caller never holds an
    /// id for something the file does not know about. Under
    /// [`DurabilityMode::BestEffort`] the failure is ignored, which is the same
    /// bargain registration already offers for predicates.
    #[cfg(feature = "std")]
    fn persist_reads_after_change(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> Result<(), RegisterError> {
        if self.storage_path.is_none() {
            return Ok(());
        }
        match self.snapshot_reads() {
            Ok(()) => Ok(()),
            Err(e) => match self.durability_mode {
                DurabilityMode::BestEffort => Ok(()),
                DurabilityMode::Required => {
                    self.unregister_reread(subscription_id);
                    Err(RegisterError::Storage(e.to_string()))
                }
            },
        }
    }

    /// Without the standard library there is no file to write.
    #[cfg(not(feature = "std"))]
    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn persist_reads_after_change(
        &mut self,
        _subscription_id: SubscriptionId,
    ) -> Result<(), RegisterError> {
        Ok(())
    }

    /// Write every re-read answer to its own file.
    ///
    /// Separate from the per-table shards because an answer needing a read has
    /// no predicate and can be woken by several tables, so it is not a property
    /// of one of them. Does nothing when no storage path is configured.
    #[cfg(feature = "std")]
    pub fn snapshot_reads(&self) -> Result<(), StorageError> {
        let Some(storage_path) = self.storage_path.as_ref() else {
            return Ok(());
        };
        let mut entries = Vec::with_capacity(self.reexec.len());
        for (subscription_id, entry) in &self.reexec {
            let mut tables = Vec::with_capacity(entry.tables.len());
            for table_id in &entry.tables {
                tables.push((
                    *table_id,
                    crate::persistence::shard::expected_envelope(&self.database, *table_id)?,
                ));
            }
            entries.push(crate::persistence::reads::ReadEntry::<I, E::Backend> {
                subscription_id: *subscription_id,
                consumer_id: entry.consumer_id,
                scope: entry.session.map_or(SubscriptionScope::Durable, |s| {
                    SubscriptionScope::Session(s)
                }),
                source_query: entry.source_query.clone(),
                tables,
                tier: entry.tier,
                database_reads_per_consumer: entry.database_reads_per_consumer,
            });
        }
        // Sorted so the file's bytes depend on the registry's contents rather
        // than on a hash map's iteration order.
        entries.sort_unstable_by_key(|e| e.subscription_id);

        #[allow(clippy::cast_possible_truncation)]
        let created_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = crate::persistence::reads::ReadsPayload::<I, E::Backend> {
            entries,
            created_at_unix_ms,
        };
        let bytes = crate::persistence::reads::serialize(&payload)?;
        Self::durable_atomic_replace(
            storage_path,
            &storage_path.join("reads.shard"),
            "reads",
            &bytes,
            created_at_unix_ms,
        )
    }

    /// Restore the re-read answers from their file, judging each one against
    /// the tables it names.
    ///
    /// An answer whose table changed shape, whose table is gone, or whose
    /// statement no longer plans is reported rather than revived, since the
    /// alternative is a subscription answering against a schema it no longer
    /// matches. Every restored answer comes back not knowing its value.
    #[cfg(feature = "std")]
    fn load_reads(&mut self) -> Result<RestoredReads<E::Backend>, StorageError> {
        let Some(storage_path) = self.storage_path.as_ref() else {
            return Ok(RestoredReads::default());
        };
        let path = storage_path.join("reads.shard");
        if !path.exists() {
            return Ok(RestoredReads::default());
        }
        let bytes = std::fs::read(&path)
            .map_err(|e| StorageError::Io(format!("Failed to read the reads file: {e}")))?;
        let payload: crate::persistence::reads::ReadsPayload<I, E::Backend> =
            crate::persistence::reads::deserialize(&bytes)?;

        let mut report = RestoredReads::default();
        for entry in payload.entries {
            match self.restore_one_read(&entry) {
                Ok(restored) => report.restored.push(restored),
                Err(reason) => report.dropped.push(DroppedRead {
                    subscription_id: entry.subscription_id,
                    sql: entry.source_query.sql().to_string(),
                    reason,
                }),
            }
        }
        Ok(report)
    }

    /// Judge and restore one saved answer, or say why it cannot come back.
    #[cfg(feature = "std")]
    #[allow(
        clippy::too_many_lines,
        reason = "restore validates and rebuilds every persisted read tier in one transaction"
    )]
    fn restore_one_read(
        &mut self,
        entry: &crate::persistence::reads::ReadEntry<I, E::Backend>,
    ) -> Result<RestoredRead<E::Backend>, DropReason> {
        for (table_id, saved) in &entry.tables {
            let live = crate::persistence::shard::expected_envelope(&self.database, *table_id)
                .map_err(|_| DropReason::TableGone {
                    table_id: *table_id,
                })?;
            if live != *saved {
                return Err(DropReason::TableChanged {
                    table_id: *table_id,
                });
            }
        }

        let session = match entry.scope {
            SubscriptionScope::Session(s) => Some(s),
            SubscriptionScope::Durable => None,
        };
        // Planning again is what decides the tier: the saved one is what it
        // was, and a schema that still fingerprints the same can still plan
        // differently (a new unique index, say).
        let planned = crate::reexec::plan::build_plan::<E::Backend, DB>(
            &entry.source_query,
            &self.dialect,
            &self.database,
        )
        .map_err(|e| DropReason::Unplannable {
            message: format!("{e:?}"),
        })?;
        // Row-level filtering may have been enabled without changing the SQL
        // text. A restored shared answer is unsafe in that case for the same
        // reason a fresh registration is: viewers can see different rows.
        if let Some(table_id) =
            self.restored_rls_table(&planned, entry.database_reads_per_consumer)?
        {
            return Err(DropReason::Unplannable {
                message: format!(
                    "table {table_id} now filters rows per viewer, so one shared answer is unsafe"
                ),
            });
        }
        let registration = RereadRegistration {
            consumer: entry.consumer_id,
            session,
            source_query: &entry.source_query,
            database_reads_per_consumer: entry.database_reads_per_consumer,
        };
        let registered = match planned {
            crate::reexec::plan::QueryPlan::GroupedPartial(plan) => {
                self.capture_grouped_scalar(entry.subscription_id, *plan, &registration)
            }
            crate::reexec::plan::QueryPlan::Partial(plan) => {
                self.capture_scalar(entry.subscription_id, plan, &registration)
            }
            crate::reexec::plan::QueryPlan::Keyed(plan) => {
                self.capture_keyed(entry.subscription_id, *plan, &registration)
            }
            crate::reexec::plan::QueryPlan::Total(plan) => {
                self.capture_whole(entry.subscription_id, plan, &registration)
            }
        };
        // A restored id is taken, so the counter must never hand it out again.
        if entry.subscription_id >= self.next_subscription_id {
            self.next_subscription_id = entry.subscription_id + 1;
        }
        let now = match &registered.tier {
            Tier::Scalar { .. } => crate::ReadTier::Scalar,
            Tier::GroupedScalar { .. } => crate::ReadTier::GroupedScalar,
            Tier::KeyedRows { .. } => crate::ReadTier::KeyedRows,
            Tier::WholeRows { .. } => crate::ReadTier::WholeRows,
            // The planner only ever hands back a read tier here.
            Tier::InProcess(_) => entry.tier,
        };
        Ok(RestoredRead {
            subscription_id: registered.subscription_id,
            tier: registered.tier,
            tier_changed: now != entry.tier,
        })
    }

    /// The row-secured table that makes restoring `planned` unsafe as one
    /// shared answer, or `None` when the restore may proceed.
    #[cfg(feature = "std")]
    fn restored_rls_table(
        &self,
        planned: &crate::reexec::plan::QueryPlan<E::Backend>,
        database_reads_per_consumer: bool,
    ) -> Result<Option<TableId>, DropReason> {
        let catalog_failed = |e: crate::CatalogError| DropReason::Unplannable {
            message: format!("row-security could not be checked: {e}"),
        };
        let found = match planned {
            crate::reexec::plan::QueryPlan::GroupedPartial(plan) => {
                crate::catalog_helpers::table_has_rls(&self.database, plan.table_id)
                    .map_err(catalog_failed)?
                    .then_some(plan.table_id)
            }
            crate::reexec::plan::QueryPlan::Partial(plan) => {
                crate::catalog_helpers::table_has_rls(&self.database, plan.table_id)
                    .map_err(catalog_failed)?
                    .then_some(plan.table_id)
            }
            crate::reexec::plan::QueryPlan::Keyed(plan) => (!database_reads_per_consumer
                && crate::catalog_helpers::table_has_rls(&self.database, plan.table)
                    .map_err(catalog_failed)?)
            .then_some(plan.table),
            crate::reexec::plan::QueryPlan::Total(plan) => {
                let mut found = None;
                if !database_reads_per_consumer {
                    for table_id in plan.tables.iter().copied() {
                        if crate::catalog_helpers::table_has_rls(&self.database, table_id)
                            .map_err(catalog_failed)?
                        {
                            found = Some(table_id);
                            break;
                        }
                    }
                }
                found
            }
        };
        Ok(found)
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
    ) -> Result<Registered<E::Backend>, RegisterError> {
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
    ) -> Result<Registered<E::Backend>, RegisterError> {
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
    ) -> Result<Registered<E::Backend>, RegisterError> {
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
    ) -> Result<Registered<E::Backend>, RegisterError> {
        let table_id = catalog_helpers::table_id(&self.database, table)
            .ok_or_else(|| RegisterError::UnknownTable(table.to_string()))?;
        let pk_cols = catalog_helpers::primary_key_columns(&self.database, table_id)?;
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
        // Placeholder syntax and identifier quoting are dialect facts, and this
        // SQL is parsed straight back by the engine's own dialect. MySQL reads
        // `$` as an identifier character, so `$1` becomes a column reference
        // there, and it delimits identifiers with backticks, so a double-quoted
        // name becomes a string literal. Either mistake parses and then matches
        // no row.
        let dollar_binds = matches!(
            crate::compiler::bind_placeholder(&self.dialect),
            crate::compiler::BindPlaceholder::Numbered
        );
        let mut clauses = Vec::with_capacity(pk_cols.len());
        for (i, col_id) in pk_cols.iter().enumerate() {
            let name = catalog_helpers::column_name(&self.database, table_id, *col_id).ok_or_else(
                || RegisterError::UnknownColumn {
                    table_id,
                    column: format!("<primary-key ordinal {col_id}>"),
                },
            )?;
            let column = crate::compiler::quoted_ident(&self.dialect, &name).to_string();
            clauses.push(if dollar_binds {
                format!("{column} = ${}", i + 1)
            } else {
                format!("{column} = ?")
            });
        }
        let sql = format!(
            "SELECT * FROM {} WHERE {}",
            crate::compiler::quoted_ident(&self.dialect, table),
            clauses.join(" AND ")
        );
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
    /// assert!(results[0].served().expect("in process").created_new_predicate);
    /// assert!(!results[1].served().expect("in process").created_new_predicate);
    /// assert!(results[2].served().expect("in process").created_new_predicate);
    /// assert_eq!(engine.predicate_count(orders_id), 2);
    /// assert_eq!(engine.subscription_count(), 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[allow(clippy::too_many_lines, clippy::cognitive_complexity)]
    pub fn register_batch(
        &mut self,
        specs: Vec<SubscriptionRequest<I, E::Backend>>,
    ) -> Vec<Result<Registered<E::Backend>, RegisterError>> {
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
        let mut results: Vec<Result<Registered<E::Backend>, RegisterError>> =
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
            match self.compile_spec(spec, false) {
                Ok(compiled_spec) => {
                    // Check dedup index for idempotent re-registration.
                    let natural_key = (
                        compiled_spec.spec.consumer_id,
                        compiled_spec.hash,
                        compiled_spec.spec.scope,
                    );
                    if let Some(&existing_sub_id) = self.binding_dedup.get(&natural_key) {
                        results.push(Ok(Registered {
                            subscription_id: existing_sub_id,
                            tier: Tier::InProcess(Served {
                                table_id: compiled_spec.table_id,
                                normalized_sql: compiled_spec.normalized,
                                predicate_hash: compiled_spec.hash,
                                created_new_predicate: false,
                                projection: compiled_spec.projection,
                                aggregate_bootstrap: compiled_spec.bootstrap,
                            }),
                            evicted: Vec::new(),
                            not_served_because: None,
                        }));
                        compiled.push(None); // already handled
                        continue;
                    }
                    // Check within-batch dedup against earlier specs in
                    // this same batch.
                    if let Some(&primary_idx) = batch_natural_dedup.get(&natural_key) {
                        let dup_idx = results.len();
                        deferred_dup_copies.push((dup_idx, primary_idx));
                        results.push(Ok(Registered {
                            subscription_id: 0, // copied from primary after Phase 2
                            tier: Tier::InProcess(Served {
                                table_id: compiled_spec.table_id,
                                normalized_sql: compiled_spec.normalized,
                                predicate_hash: compiled_spec.hash,
                                created_new_predicate: false,
                                projection: compiled_spec.projection,
                                aggregate_bootstrap: compiled_spec.bootstrap,
                            }),
                            evicted: Vec::new(),
                            not_served_because: None,
                        }));
                        compiled.push(None);
                        continue;
                    }
                    // First occurrence of this natural key within the
                    // batch. Record so subsequent duplicates can defer
                    // onto it.
                    batch_natural_dedup.insert(natural_key, results.len());
                    results.push(Ok(Registered {
                        subscription_id: 0, // filled in phase 2
                        tier: Tier::InProcess(Served {
                            table_id: compiled_spec.table_id,
                            normalized_sql: String::new(), // filled in phase 2
                            predicate_hash: compiled_spec.hash,
                            created_new_predicate: false, // filled in phase 2
                            projection: compiled_spec.projection.clone(),
                            aggregate_bootstrap: None, // filled in phase 2
                        }),
                        evicted: Vec::new(),
                        not_served_because: None,
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
        // Term seedings, applied after phase 3: the new-predicate path only
        // assigns its predicate id there, so both paths defer rather than one of
        // them seeding early. Empty unless a filter names a membership subquery.
        let mut pending_seeds: Vec<PendingSeed<E::Backend>> = Vec::new();

        for (i, entry) in compiled.into_iter().enumerate() {
            let Some(mut c) = entry else { continue };
            let pending_terms = c.term_subscriber.take().map(|subscriber| {
                (
                    subscriber,
                    core::mem::take(&mut c.term_plans),
                    core::mem::take(&mut c.term_seeds),
                )
            });

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
            let subscription_id = self.allocate_subscription_id();

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
            if let Some((subscriber, plans, seeds)) = pending_terms {
                pending_seeds.push(PendingSeed {
                    table: c.table_id,
                    subscription: subscription_id,
                    subscriber,
                    plans,
                    seeds,
                });
            }

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
                partition.mutate(|txn| txn.add_binding(binding, pred_id));
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
                let (pred, atoms) = Self::make_predicate_from_compiled(&self.database, &c);
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
            // Batch registration takes plain `SubscriptionRequest`, which
            // states shared database reads.
            self.open_aggregate_total(subscription_id, &c.spec, c.table_id, &c.projection, false);

            // Fill in the result, including any evictions credited to
            // this spec by the cap branch above.
            if let Ok(result) = &mut results[i] {
                result.subscription_id = subscription_id;
                if let Tier::InProcess(served) = &mut result.tier {
                    served.normalized_sql = c.normalized;
                    served.created_new_predicate = created_new;
                    served.aggregate_bootstrap = c.bootstrap;
                }
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
            partition.mutate(|txn| {
                for (predicate, atoms, bindings) in &entries {
                    let pred_id = txn.add_predicate(Predicate::clone(predicate), atoms);
                    for binding in bindings {
                        let mut bound = *binding;
                        bound.predicate_id = pred_id;
                        txn.add_binding(bound, pred_id);
                    }
                }
            });
            for (_, _, bindings) in &entries {
                for binding in bindings {
                    self.subscription_to_table
                        .insert(binding.subscription_id, table_id);
                }
            }
        }

        // Phase 3.5: seed the membership terms, now that every predicate id is
        // final. Each seeding reads its own binding back, so a subscription whose
        // table failed phase 3 simply has none and is skipped.
        let mut watches = Vec::new();
        for pending in pending_seeds {
            let Some(partition) = self.partitions.get_mut(&pending.table) else {
                continue;
            };
            let Some(binding) = partition
                .load_snapshot()
                .predicates
                .bindings
                .get(&pending.subscription)
                .copied()
            else {
                continue;
            };
            partition.mutate(|txn| {
                txn.seed_terms(
                    binding.predicate_id,
                    binding.consumer_ordinal,
                    &pending.subscriber,
                    &pending.seeds,
                );
            });
            watches.extend(pending.plans.iter().filter_map(|plan| {
                let movement = plan.moved_by.as_ref()?;
                Some(TermWatch {
                    subscribed: pending.table,
                    member_table: movement.member_table,
                    predicate: binding.predicate_id,
                    slot: plan.slot,
                    columns: plan.columns.clone(),
                    member_keys: movement.member_keys.clone(),
                    member_subject: movement.member_subject,
                })
            }));
        }
        self.watch_terms(watches);

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
                    let primary_normalized = match &primary_ok.tier {
                        Tier::InProcess(served) => served.normalized_sql.clone(),
                        // The batch path registers nothing else, so there is
                        // no normalized statement to mirror.
                        Tier::Scalar { .. }
                        | Tier::GroupedScalar { .. }
                        | Tier::KeyedRows { .. }
                        | Tier::WholeRows { .. } => String::new(),
                    };
                    if let Ok(dup_ok) = &mut results[dup_idx] {
                        dup_ok.subscription_id = primary_sub_id;
                        if let Tier::InProcess(served) = &mut dup_ok.tier {
                            if served.normalized_sql.is_empty() {
                                served.normalized_sql = primary_normalized;
                            }
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

    // Per-`(session, subscription)` resume cursor API. The cursor is the
    // position the materializer last successfully dispatched to the
    // client. On reconnect the materializer compares it against its own
    // oplog watermark to decide catchup vs full re-sync.

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
        self.aggregates.remove(&subscription_id);
        self.grouped_aggregates.remove(&subscription_id);
        self.aggregate_registrations.remove(&subscription_id);
        // Capture dedup key from binding before removing it.
        let dedup_key = self.dedup_key_for_subscription(subscription_id);

        // Fast path: direct lookup from subscription index.
        if let Some(table_id) = self.subscription_to_table.get(&subscription_id).copied() {
            let removal = self
                .partitions
                .get_mut(&table_id)
                .and_then(|partition| partition.mutate(|txn| txn.remove_binding(subscription_id)));
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
            if let Some(removal) = partition.mutate(|txn| txn.remove_binding(subscription_id)) {
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
    ) -> Result<crate::ConsumerNotifications<I, E::Checkpoint, E::Backend>, DispatchError> {
        let table_id = event.table_id(&self.database);
        let notifs = self.row_consumers(event, table_id)?;
        // After the row dispatch rather than before it. A table can be both the
        // one a subscription reads and the one carrying its memberships, and then
        // both row versions are judged against the set as it stood when the event
        // arrived, which is one consistent world rather than the old row judged
        // against a set that did not exist yet. The narrowing beside it is what
        // says the set has since moved.
        let narrowings = self.move_watched_terms(event, table_id);
        Ok(notifs.with_narrowings(narrowings))
    }

    /// The view-relative notifications for subscriptions reading `table_id`.
    fn row_consumers(
        &mut self,
        event: &E,
        table_id: TableId,
    ) -> Result<crate::ConsumerNotifications<I, E::Checkpoint, E::Backend>, DispatchError> {
        // An event for a table that is in the catalog but has no
        // subscription affects nobody: report empty rather than
        // erroring. Reserve `UnknownTableId` for ids not in the schema
        // at all (genuine drift or a caller bug).
        if !self.partitions.contains_key(&table_id) {
            return if self.table_in_catalog(table_id) {
                Ok(crate::ConsumerNotifications::empty().with_checkpoint(event.checkpoint()))
            } else {
                Err(DispatchError::UnknownTableId(table_id))
            };
        }

        // Activity-aware policies stamp matched subscriptions through
        // the `_with_stamps` dispatch variant. Default (no activity
        // tracking) takes the cheaper path that allocates no stamp
        // vector.
        let needs_stamps = self.eviction_strategy.needs_activity_tracking();

        let arity = catalog_helpers::table_arity(&self.database, table_id)?;

        let (partition, consumer_dict) =
            table_context(&self.partitions, &self.consumer_dictionaries, table_id)?;

        if needs_stamps {
            let (notifs, stamps) = dispatch_consumers_with_stamps(
                event,
                partition,
                consumer_dict,
                &mut self.vm,
                arity,
                &self.database,
            )?;
            self.stamp_activity(&stamps);
            Ok(notifs)
        } else {
            dispatch_consumers(
                event,
                partition,
                consumer_dict,
                &mut self.vm,
                arity,
                &self.database,
            )
        }
    }

    /// Move who each term admits, for a change on a table carrying memberships,
    /// and report every subscription whose answer that changed.
    ///
    /// Reads the two columns `rls2fga` named on the shape that states who a row
    /// admits: one carries the value the term compares, the other names the
    /// subscriber. Neither is guessed from the filter text.
    fn move_watched_terms(
        &mut self,
        event: &E,
        table_id: TableId,
    ) -> Vec<crate::TermNarrowing<E::Backend>> {
        let Some(watches) = self.term_watch.get(&table_id) else {
            return Vec::new();
        };
        // Cloned so the mutation below can take the partition mutably. One small
        // copy per event on a table some subscription reads memberships from,
        // and none at all on any other table.
        let watches = watches.clone();

        // Read every movement first, then apply them grouped by subscribed
        // partition, so one event publishes each partition at most once
        // however many terms it moves.
        let mut actions: Vec<(TermWatch, TermAction<E::Backend>)> = Vec::new();
        for watch in watches {
            match event.kind() {
                EventKind::Insert => {
                    if let Some((values, subject)) =
                        self.read_term_pair(event, &watch, RowKind::New)
                    {
                        actions.push((
                            watch,
                            TermAction::Move {
                                values,
                                subject,
                                entered: true,
                            },
                        ));
                    }
                }
                EventKind::Delete => {
                    if let Some((values, subject)) =
                        self.read_term_pair(event, &watch, RowKind::Old)
                    {
                        actions.push((
                            watch,
                            TermAction::Move {
                                values,
                                subject,
                                entered: false,
                            },
                        ));
                    }
                }
                EventKind::Update => {
                    let before = self.read_term_pair(event, &watch, RowKind::Old);
                    let after = self.read_term_pair(event, &watch, RowKind::New);
                    if before == after {
                        continue;
                    }
                    if let Some((values, subject)) = before {
                        actions.push((
                            watch.clone(),
                            TermAction::Move {
                                values,
                                subject,
                                entered: false,
                            },
                        ));
                    }
                    if let Some((values, subject)) = after {
                        actions.push((
                            watch,
                            TermAction::Move {
                                values,
                                subject,
                                entered: true,
                            },
                        ));
                    }
                }
                // Every membership is gone, so every value the term admitted is
                // withdrawn. Doing nothing here would leave the sets admitting
                // rows through memberships that no longer exist, which is the one
                // error direction the whole design refuses.
                EventKind::Truncate => actions.push((watch, TermAction::Clear)),
            }
        }

        let mut grouped: Vec<(TableId, Vec<(TermWatch, TermAction<E::Backend>)>)> = Vec::new();
        for (watch, action) in actions {
            match grouped
                .iter_mut()
                .find(|(table, _)| *table == watch.subscribed)
            {
                Some((_, list)) => list.push((watch, action)),
                None => grouped.push((watch.subscribed, alloc::vec![(watch, action)])),
            }
        }
        let mut out = Vec::new();
        for (table, list) in grouped {
            let Some(partition) = self.partitions.get_mut(&table) else {
                continue;
            };
            partition.mutate(|txn| {
                for (watch, action) in list {
                    match action {
                        TermAction::Move {
                            values,
                            subject,
                            entered,
                        } => Self::move_term(txn, &watch, values, &subject, entered, &mut out),
                        TermAction::Clear => Self::clear_term(txn, &watch, &mut out),
                    }
                }
            });
        }
        out
    }

    /// The value row and the subscriber one membership row names, or [`None`]
    /// when any half is absent, null, or of a kind that cannot be looked up.
    #[allow(clippy::type_complexity)]
    fn read_term_pair(
        &self,
        event: &E,
        watch: &TermWatch,
        row: RowKind,
    ) -> Option<(crate::term::TermRow<E::Backend>, TermKey<E::Backend>)> {
        let mut values = Vec::with_capacity(watch.member_keys.len());
        for &member_key in &watch.member_keys {
            let value = event.value_at(&self.database, row, member_key).ok()?;
            let TermLookup::Key(value) = TermLookup::of(value) else {
                return None;
            };
            values.push(value);
        }
        let subject = event
            .value_at(&self.database, row, watch.member_subject)
            .ok()?;
        let TermLookup::Key(subject) = TermLookup::of(subject) else {
            return None;
        };
        Some((values, subject))
    }

    /// Add or remove the subscribers claiming `subscriber` from what `values`
    /// admit, and report the subscriptions moved.
    fn move_term(
        txn: &mut PartitionTxn<'_, I, E::Backend>,
        watch: &TermWatch,
        values: crate::term::TermRow<E::Backend>,
        subscriber: &TermKey<E::Backend>,
        entered: bool,
        out: &mut Vec<crate::TermNarrowing<E::Backend>>,
    ) {
        let store = txn.store();
        let Some(members) = store.term_members(watch.predicate, watch.slot) else {
            return;
        };
        let Some(claiming) = members.claimed_by(subscriber) else {
            return;
        };
        // Only this predicate's own subscribers: an ordinal is dense per table
        // and several predicates share the numbering.
        let moved = match store.predicate_consumers.get(&watch.predicate) {
            Some(bitmap) => claiming & bitmap,
            None => return,
        };
        if moved.is_empty() {
            return;
        }

        let subscriptions = subscriptions_for(store, watch.predicate, &moved);
        txn.move_term_members(watch.predicate, watch.slot, values.clone(), &moved, entered);

        let values: Vec<_> = values.into_iter().map(TermKey::into_value).collect();
        out.extend(
            subscriptions
                .into_iter()
                .map(|subscription| crate::TermNarrowing {
                    subscription,
                    table: watch.subscribed,
                    columns: watch.columns.clone(),
                    values: values.clone(),
                    entered,
                }),
        );
    }

    /// Withdraw every value one term admitted, as a truncate of the table
    /// carrying its memberships does.
    fn clear_term(
        txn: &mut PartitionTxn<'_, I, E::Backend>,
        watch: &TermWatch,
        out: &mut Vec<crate::TermNarrowing<E::Backend>>,
    ) {
        let withdrawn = txn.clear_term_admissions(watch.predicate, watch.slot);
        if withdrawn.is_empty() {
            return;
        }

        let store = txn.store();
        for (values, ordinals) in withdrawn {
            let subscriptions = subscriptions_for(store, watch.predicate, &ordinals);
            let values: Vec<_> = values.into_iter().map(TermKey::into_value).collect();
            out.extend(
                subscriptions
                    .into_iter()
                    .map(|subscription| crate::TermNarrowing {
                        subscription,
                        table: watch.subscribed,
                        columns: watch.columns.clone(),
                        values: values.clone(),
                        entered: false,
                    }),
            );
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

    /// Dispatch one event through the row-set and aggregate paths at once.
    ///
    /// ```
    /// use sql_traits::structs::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::{Postgres, Value};
    /// use subql::testing::TestEvent;
    /// use subql::{
    ///     catalog_helpers, AggValue, DefaultIds, Install, SubscriptionEngine,
    ///     SubscriptionRequest,
    /// };
    ///
    /// let database = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    /// )
    /// .expect("the DDL parses");
    /// let orders_id = catalog_helpers::table_id(&database, "orders").expect("orders is cataloged");
    /// let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    ///     SubscriptionEngine::new(database, PostgreSqlDialect {});
    ///
    /// engine
    ///     .register(SubscriptionRequest::new(7, "SELECT * FROM orders WHERE amount > 100"))
    ///     .expect("the row query registers");
    /// let counted = engine
    ///     .register(SubscriptionRequest::new(
    ///         9,
    ///         "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
    ///     ))
    ///     .expect("the aggregate registers");
    /// Install::install(
    ///     &mut engine,
    ///     counted.subscription_id,
    ///     subql::AggregateSeedInstall {
    ///         rows: vec![vec![Value::Int(0)]],
    ///         read_at: None,
    ///     },
    /// )
    /// .expect("the starting numbers land");
    ///
    /// let event = TestEvent::<Postgres>::insert(
    ///     orders_id,
    ///     vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
    /// )
    /// .with_pk_columns([0u16]);
    ///
    /// let out = engine.dispatch(&event).expect("the event dispatches");
    /// assert_eq!(out.notifications().inserted(), vec![7]);
    /// assert_eq!(
    ///     out.aggregate_updates()[0].change,
    ///     subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(
    ///         AggValue::Count(1),
    ///     ))
    /// );
    /// assert_eq!(out.notified(), vec![7, 9]); // deduped union of both paths
    /// ```
    pub fn dispatch(
        &mut self,
        event: &E,
    ) -> Result<crate::DispatchOutput<I, E::Checkpoint, E::Backend>, DispatchError> {
        let notifications = match self.consumers(event) {
            Ok(notifications) => notifications,
            Err(DispatchError::UnknownTableId(_))
                if self.routes_reread(event.table_id(&self.database)) =>
            {
                crate::ConsumerNotifications::empty().with_checkpoint(event.checkpoint())
            }
            Err(error) => return Err(error),
        };
        let mut aggregate = self.aggregate_updates(event)?;
        let (grouped_updates, scalar_updates, mut triggers, mut transitions) =
            self.maintain(event)?;
        aggregate.updates.extend(grouped_updates);
        triggers.extend(aggregate.triggers);
        triggers.extend(self.unseeded_aggregate_triggers(event));
        triggers.sort_unstable_by(|left, right| {
            (left.subscription_id, left.read.group_key())
                .cmp(&(right.subscription_id, right.read.group_key()))
        });
        triggers.dedup_by(|left, right| {
            left.subscription_id == right.subscription_id
                && left.read.group_key() == right.read.group_key()
        });
        transitions.extend(aggregate.transitions);
        let output = crate::DispatchOutput::from_parts(
            notifications,
            aggregate.updates,
            scalar_updates,
            triggers,
            transitions,
        );
        // Any single-row (pk) follow whose row was just deleted
        // self-closes. Its `WHERE pk = <value>` predicate can never
        // match again, so leaving the subscription registered would
        // leak memory and spuriously fire on future rows that reuse
        // the same PK.
        if event.kind() == EventKind::Delete && !self.pk_follows.is_empty() {
            self.close_deleted_pk_follows(event);
        }
        Ok(output)
    }

    /// Auto-unregister every pk-follow whose tracked row matches the
    /// deleted row on `event`.
    ///
    /// Reads the event's PK columns through [`CdcEvent::value_at`] under
    /// [`RowKind::Pk`], collects them into `Vec<Value<E::Backend>>`, and
    /// closes each pk-follow whose stored `(TableId, pk_values)` equals the
    /// event's.
    ///
    /// Silently no-ops when the PK cannot be materialised (unknown column,
    /// [`Value::Missing`], or [`Value::Null`]). pk-follows are
    /// best-effort per the `pk_follows` doc contract; loss of the
    /// auto-close marker degrades gracefully to an ordinary inert
    /// subscription rather than surfacing an error.
    fn close_deleted_pk_follows(&mut self, event: &E) {
        let table_id = event.table_id(&self.database);
        let pk_cols: alloc::vec::Vec<crate::ColumnId> = event.pk_columns(&self.database);

        // Extract the event's PK cells once so we can compare each
        // follow against the same materialised Vec.
        let mut event_pk: alloc::vec::Vec<Value<E::Backend>> =
            alloc::vec::Vec::with_capacity(pk_cols.len());
        for &col in &pk_cols {
            let Some(v) = extract_pk_value(event, &self.database, col) else {
                return;
            };
            event_pk.push(v);
        }

        // Collect first, mutate second: `unregister_subscription_internal`
        // needs `&mut self` while `pk_follows` iteration holds an
        // immutable borrow.
        let to_close: alloc::vec::Vec<SubscriptionId> = self
            .pk_follows
            .iter()
            .filter_map(|(&sub_id, (follow_table, follow_pk))| {
                (*follow_table == table_id && follow_pk == &event_pk).then_some(sub_id)
            })
            .collect();
        for sub_id in to_close {
            self.unregister_subscription_internal(sub_id);
        }
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

        // Re-read answers are this registry's too, so a session takes its own
        // with it rather than leaving them routed to nobody.
        let mut removed_reads = 0;
        for subscription_id in self
            .reexec_sessions
            .get(&session_id)
            .cloned()
            .unwrap_or_default()
        {
            if self.unregister_reread(subscription_id) {
                removed_reads += 1;
            }
        }

        UnregisterReport {
            removed_bindings,
            removed_predicates,
            removed_consumers,
            removed_reads,
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
            removed_reads: 0,
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
            // Unregistering by statement touches predicates only, never the
            // re-read answers, which are dropped by id.
            removed_reads: 0,
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
        catalog_helpers::table_arity(&self.database, table_id).is_ok()
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

    // Persistence Methods

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
        &self,
        table_id: TableId,
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
            let group_key_encoder = match &pred_data.projection {
                QueryProjection::GroupedAggregate { groups, .. } => groups
                    .iter()
                    .map(|column| {
                        catalog_helpers::group_key_column::<E::Backend, _>(
                            &self.database,
                            table_id,
                            *column,
                        )
                    })
                    .collect::<Option<Vec<_>>>()
                    .and_then(E::Backend::group_key_encoder),
                QueryProjection::Rows | QueryProjection::Aggregate(_) => None,
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
                group_key_encoder,
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
        let (consumer_dict, entries) = self.rebuild_entries_from_payload(table_id, payload)?;
        let mut partition = TablePartition::new(table_id);
        partition.mutate(|txn| {
            for (predicate, atoms, bindings) in &entries {
                let pred_id = txn.add_predicate(Predicate::clone(predicate), atoms);
                for binding in bindings {
                    let mut bound = *binding;
                    bound.predicate_id = pred_id;
                    txn.add_binding(bound, pred_id);
                }
            }
        });
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

    // Merge Methods

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
    ) -> Result<Registered<E::Backend>, RegisterError> {
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
    type Notifications = crate::ConsumerNotifications<I, E::Checkpoint, E::Backend>;
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
    fn aggregate_updates(
        &mut self,
        event: &E,
    ) -> Result<crate::AggregateMaintenanceOutput<I, E::Backend, E::Checkpoint>, DispatchError>
    {
        Self::aggregate_updates(self, event)
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

/// Read one primary-key column off a [`CdcEvent`] via [`RowKind::Pk`].
/// Returns `None` when the cell is [`Value::Missing`], [`Value::Null`], or
/// carried a value subql could not decode, so callers upstream can bail
/// out cleanly without materialising a partial key.
fn extract_pk_value<E: CdcEvent, DB: DatabaseLike>(
    event: &E,
    db: &DB,
    col: crate::ColumnId,
) -> Option<Value<E::Backend>> {
    match event.value_at(db, RowKind::Pk, col) {
        Ok(Value::Missing | Value::Null) | Err(_) => None,
        Ok(v) => Some(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Postgres;
    use crate::testing::TestEvent;
    use crate::{DefaultIds, EvictionPolicy, SubscriptionRequest};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

    type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

    fn cap1_evict_oldest() -> Engine {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        SubscriptionEngine::new(db, PostgreSqlDialect {})
            .with_max_subscriptions(1, EvictionPolicy::EvictOldest)
    }

    fn spec(consumer: u64, id: u64) -> SubscriptionRequest<DefaultIds, Postgres> {
        SubscriptionRequest::new(consumer, format!("SELECT * FROM orders WHERE id = {id}"))
    }

    /// What the in-process evaluator said it could not do with `where_clause`.
    ///
    /// The shape is not refused any more: it registers on a tier that re-reads
    /// it, and carries the evaluator's own words for why. What these tests pin
    /// is that it is not served in process, and that the reason says which
    /// shape stopped it.
    fn refusal(where_clause: &str) -> String {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        let sql = format!("SELECT * FROM orders WHERE {where_clause}");
        match engine.register(SubscriptionRequest::new(1, sql)) {
            Ok(Registered {
                tier: Tier::InProcess(served),
                ..
            }) => panic!("{where_clause} should not be served in process, got {served:?}"),
            Ok(registered) => registered
                .not_served_because
                .expect("a tier that needs a read says why"),
            Err(e) => panic!("{where_clause} should register on a read tier, got {e:?}"),
        }
    }

    /// A membership subquery the engine cannot serve is refused as a term
    /// rather than as bad SQL, so the message carries why.
    fn term_refusal(where_clause: &str) -> String {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        let sql = format!("SELECT * FROM orders WHERE {where_clause}");
        match engine.register(SubscriptionRequest::new(1, sql)) {
            Err(RegisterError::MembershipTermRefused(message)) => message,
            other => panic!("{where_clause} should be refused as a term, got {other:?}"),
        }
    }

    /// `NOT IN` stays refused. A membership subquery is served by tracking the
    /// relationship it names, and subtraction names no relationship to track.
    #[test]
    fn a_negated_membership_subquery_is_refused_as_subtraction() {
        let message = refusal("id NOT IN (SELECT id FROM orders WHERE amount = 1)");
        assert!(
            message.contains("NOT IN"),
            "the refusal should name the negation, got {message:?}"
        );
        assert!(
            !message.contains("InSubquery"),
            "the refusal should not print the parsed expression, got {message:?}"
        );
    }

    /// `x NOT IN (SELECT ...)` and `NOT (x IN (SELECT ...))` are the same
    /// filter written two ways, so they are refused the same way. They reach
    /// different arms, which is why it is easy to leave one of them behind.
    #[test]
    fn the_two_spellings_of_a_negated_term_are_refused_alike() {
        let inline = refusal("id NOT IN (SELECT id FROM orders WHERE amount = 1)");
        let outer = refusal("NOT (id IN (SELECT id FROM orders WHERE amount = 1))");
        assert_eq!(
            inline, outer,
            "both spellings negate the same term, so the refusal must not depend on which \
             one was written"
        );
    }

    /// A term names the relationship a column of the subscribed table stands
    /// in, so the tested side has to be a column of that table. An expression
    /// has no column to read off the changed row and no name to group the
    /// subscriber's own starting values under.
    #[test]
    fn a_term_comparing_something_other_than_a_column_is_refused() {
        let message = refusal("1 IN (SELECT id FROM orders WHERE amount = 1)");
        assert!(
            message.contains("column of"),
            "the refusal should say the tested side must be a column, got {message:?}"
        );
    }

    /// Same rule, for a name that is not a column of the subscribed table.
    #[test]
    fn a_term_comparing_an_unknown_column_is_refused() {
        let message = refusal("nope IN (SELECT id FROM orders WHERE amount = 1)");
        assert!(
            message.contains("column of"),
            "the refusal should name the missing column, got {message:?}"
        );
    }

    /// The inner query is bounded by the same rule as the outer statement, so a
    /// join inside it is refused by the join wording rather than by a second
    /// rule that could drift from the first.
    #[test]
    fn a_join_inside_the_membership_subquery_is_refused_by_the_one_shape_rule() {
        let message = refusal("id IN (SELECT o.id FROM orders o JOIN orders p ON o.id = p.id)");
        assert!(
            message.contains("JOINs not supported"),
            "the inner join should be refused by the shared shape rule, got {message:?}"
        );
    }

    /// Same rule, the derived-table clause of it.
    #[test]
    fn a_derived_table_inside_the_membership_subquery_is_refused() {
        let message = refusal("id IN (SELECT id FROM (SELECT id FROM orders) inner_q)");
        assert!(
            message.contains("Subqueries and derived tables not supported"),
            "the inner derived table should be refused by the shared shape rule, got {message:?}"
        );
    }

    /// Same rule, the set-operation clause of it.
    #[test]
    fn a_set_operation_inside_the_membership_subquery_is_refused() {
        let message =
            refusal("id IN (SELECT id FROM orders UNION SELECT id FROM orders WHERE amount = 1)");
        assert!(
            message.contains("Set operations"),
            "the inner set operation should be refused by the shared shape rule, got {message:?}"
        );
    }

    /// One projected column, because the tested value is matched against that
    /// one column and a second has nothing to match.
    #[test]
    fn a_membership_subquery_selecting_two_columns_is_refused() {
        let message = refusal("id IN (SELECT id, amount FROM orders WHERE amount = 1)");
        assert!(
            message.contains("exactly one column"),
            "the refusal should say one column, got {message:?}"
        );
    }

    /// A wildcard projects whatever the table happens to carry, which is not one
    /// named column even when the table has exactly one.
    #[test]
    fn a_membership_subquery_selecting_a_wildcard_is_refused() {
        let message = refusal("id IN (SELECT * FROM orders WHERE amount = 1)");
        assert!(
            message.contains("exactly one column"),
            "the refusal should say one column, got {message:?}"
        );
    }

    /// No nesting: one subscription tracks one relationship, and a subquery
    /// inside the subquery names a second one.
    #[test]
    fn a_subquery_nested_inside_the_membership_subquery_is_refused() {
        let message =
            refusal("id IN (SELECT id FROM orders WHERE amount IN (SELECT amount FROM orders))");
        assert!(
            message.contains("cannot contain another subquery"),
            "the refusal should name the nesting, got {message:?}"
        );
    }

    /// The nesting search reaches inside a list item, not just at it. A
    /// parenthesised subquery in an `IN` list is `Nested(Subquery)`, so a search
    /// that only inspected each item without descending would walk straight
    /// past it and admit a filter carrying two relationships.
    #[test]
    fn a_subquery_wrapped_inside_an_in_list_item_is_still_found() {
        let message = refusal(
            "id IN (SELECT id FROM orders WHERE amount IN (1, ((SELECT amount FROM orders))))",
        );
        assert!(
            message.contains("cannot contain another subquery"),
            "the refusal should name the nesting, got {message:?}"
        );
    }

    /// `EXISTS` is a subquery too, and it nests just as much as `IN` does. It
    /// reaches the nesting search through its own AST variant, so covering only
    /// the `IN` spelling leaves that arm undefended.
    #[test]
    fn an_exists_nested_inside_the_membership_subquery_is_refused() {
        let message = refusal("id IN (SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM orders))");
        assert!(
            message.contains("cannot contain another subquery"),
            "the refusal should name the nesting, got {message:?}"
        );
    }

    /// The bounded form is inside the language. Whatever declines to serve it,
    /// the refusal never claims SubQL cannot run a nested `SELECT`, and never
    /// prints the parsed expression.
    #[test]
    fn a_bounded_membership_subquery_is_not_refused_for_being_a_subquery() {
        let message = term_refusal("status IN (SELECT status FROM orders WHERE amount = 1)");
        assert!(
            !message.contains("cannot run a nested SELECT"),
            "a bounded term is within the language, got {message:?}"
        );
        assert!(
            !message.contains("InSubquery"),
            "the refusal should not print the parsed expression, got {message:?}"
        );
    }

    /// With the term feature off the build simply cannot compile a term, and the
    /// refusal says that rather than blaming the filter.
    #[cfg(not(feature = "membership-term"))]
    #[test]
    fn a_build_without_the_term_feature_names_the_feature_it_lacks() {
        let message = term_refusal("status IN (SELECT status FROM orders WHERE amount = 1)");
        assert!(
            message.contains("membership-term"),
            "the refusal should name the missing feature, got {message:?}"
        );
    }

    /// `EXISTS` is a subquery too, and it reaches the same catch-all. It is
    /// refused, and not by the wording that promises something about `IN`.
    #[test]
    fn an_exists_subquery_is_refused_without_claiming_to_be_about_in() {
        let message = refusal("EXISTS (SELECT 1 FROM orders)");
        assert!(
            !message.contains("IN with"),
            "an EXISTS refusal should not describe IN, got {message:?}"
        );
    }

    /// The literal-list arm still refuses a list item that is not a literal, and
    /// now says so instead of blaming subqueries. Comparing a column against
    /// other columns is a different mistake and deserves a different sentence.
    #[test]
    fn a_non_literal_in_list_item_is_refused_for_what_it_is() {
        let message = refusal("id IN (amount, status)");
        assert!(
            !message.contains("subquer"),
            "a non-literal list item is not a subquery, got {message:?}"
        );
        assert!(
            message.contains("literal"),
            "the refusal should say the list must be literals, got {message:?}"
        );
    }

    /// `register_batch` under `EvictOldest` matches a sequential `register`
    /// loop: each over-cap entry evicts the subscription committed just before
    /// it and succeeds, so a batch never shields its own members. Guards
    /// against reintroducing a "no self-eviction" divergence, which would break
    /// the parity contract fixed in commit 95b435d and enforced by
    /// `tests/it/proptest_register_batch_parity.rs`.
    #[test]
    fn register_batch_evict_oldest_churns_like_sequential() {
        let mut engine = cap1_evict_oldest();
        let pre = engine.register(spec(1, 1)).expect("pre registers");

        let results = engine.register_batch(vec![spec(2, 2), spec(3, 3), spec(4, 4)]);
        let oks: Vec<&Registered> = results
            .iter()
            .map(|r| r.as_ref().expect("entry ok"))
            .collect();

        // Every entry evicts the subscription committed just before it.
        assert_eq!(oks[0].evicted, vec![pre.subscription_id]);
        assert_eq!(oks[1].evicted, vec![oks[0].subscription_id]);
        assert_eq!(oks[2].evicted, vec![oks[1].subscription_id]);
        // Cap 1 leaves only the last-registered subscription alive.
        assert_eq!(engine.subscription_count(), 1);
    }

    /// The batch path does not special-case eviction: the same specs run as a
    /// batch and as a sequential loop produce identical ids, evictions, and
    /// final registry size.
    #[test]
    fn register_batch_equals_sequential_loop_under_cap() {
        let specs = || vec![spec(2, 2), spec(3, 3), spec(4, 4)];

        let mut batch_engine = cap1_evict_oldest();
        let batch = batch_engine.register_batch(specs());

        let mut seq_engine = cap1_evict_oldest();
        let seq: Vec<_> = specs()
            .into_iter()
            .map(|s| seq_engine.register(s))
            .collect();

        for (b, s) in batch.iter().zip(seq.iter()) {
            let b = b.as_ref().expect("batch entry ok");
            let s = s.as_ref().expect("sequential entry ok");
            assert_eq!(b.subscription_id, s.subscription_id);
            assert_eq!(b.evicted, s.evicted);
        }
        assert_eq!(
            batch_engine.subscription_count(),
            seq_engine.subscription_count()
        );
    }

    const RLS_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT); \
         ALTER TABLE orders ENABLE ROW LEVEL SECURITY;";

    /// The eight in-process delta aggregates the core engine accepts.
    const INPROCESS_AGGREGATES: &[&str] = &[
        "SELECT COUNT(*) FROM orders",
        "SELECT COUNT(amount) FROM orders",
        "SELECT SUM(amount) FROM orders",
        "SELECT AVG(amount) FROM orders",
        "SELECT VAR_POP(amount) FROM orders",
        "SELECT VAR_SAMP(amount) FROM orders",
        "SELECT STDDEV_POP(amount) FROM orders",
        "SELECT STDDEV_SAMP(amount) FROM orders",
    ];

    fn rls_engine() -> (Engine, TableId) {
        let db = ParserDB::parse::<PostgreSqlDialect>(RLS_DDL).expect("parse RLS DDL");
        let table_id = crate::catalog_helpers::table_id(&db, "orders").expect("orders exists");
        (SubscriptionEngine::new(db, PostgreSqlDialect {}), table_id)
    }

    /// The core `register` rejects every in-process aggregate on an RLS
    /// table, so a direct core-engine caller is protected without the
    /// reexec wrapper. Guards the `compile_spec` choke point.
    #[test]
    fn register_rejects_aggregators_on_rls_table() {
        for sql in INPROCESS_AGGREGATES {
            let (mut engine, table_id) = rls_engine();
            match engine.register(SubscriptionRequest::new(1u64, *sql)) {
                Err(RegisterError::AggregatorOnRlsTable { table_id: got }) => {
                    assert_eq!(got, table_id, "`{sql}` rejected for the wrong table id");
                }
                other => panic!("`{sql}` on RLS table must be rejected, got {other:?}"),
            }
        }
    }

    /// `register_batch`'s bulk path shares the `compile_spec` choke point,
    /// so it rejects aggregates on RLS tables per element while still
    /// accepting a row subscription in the same batch. Without this the
    /// bulk path would bypass the single-`register` guard.
    #[test]
    fn register_batch_rejects_aggregators_on_rls_table() {
        let (mut engine, table_id) = rls_engine();
        let results = engine.register_batch(vec![
            SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders"),
            SubscriptionRequest::new(2u64, "SELECT * FROM orders WHERE amount > 1"),
            SubscriptionRequest::new(3u64, "SELECT SUM(amount) FROM orders"),
        ]);
        match &results[0] {
            Err(RegisterError::AggregatorOnRlsTable { table_id: got }) => {
                assert_eq!(*got, table_id);
            }
            other => panic!("COUNT(*) must be rejected on RLS, got {other:?}"),
        }
        let row = results[1]
            .as_ref()
            .expect("row subscription accepted on RLS table");
        assert!(row.aggregate_spec().is_none());
        match &results[2] {
            Err(RegisterError::AggregatorOnRlsTable { table_id: got }) => {
                assert_eq!(*got, table_id);
            }
            other => panic!("SUM(amount) must be rejected on RLS, got {other:?}"),
        }
    }

    /// The guard keys on the aggregate projection, not the table: a row
    /// subscription on the RLS table is accepted.
    #[test]
    fn register_allows_row_subscription_on_rls_table() {
        let (mut engine, _) = rls_engine();
        let result = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE amount > 1",
            ))
            .expect("row subscription accepted on RLS table");
        assert!(result.aggregate_spec().is_none());
    }

    /// Without RLS the same aggregates all register as aggregate projections.
    #[test]
    fn register_allows_aggregators_without_rls() {
        for sql in INPROCESS_AGGREGATES {
            let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
            let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
            let result = engine
                .register(SubscriptionRequest::new(1u64, *sql))
                .unwrap_or_else(|e| panic!("`{sql}` without RLS must register, got Err({e:?})"));
            assert!(
                result.aggregate_spec().is_some(),
                "`{sql}` should register as an aggregate"
            );
        }
    }

    /// The guard keys on `has_row_level_security` (Postgres `ENABLE`), not
    /// on `FORCE`. `FORCE ROW LEVEL SECURITY` without `ENABLE` does not
    /// filter rows (policies apply only when RLS is enabled), so every
    /// viewer sees the same rows and a shared in-process aggregate is
    /// safe. Pins that a future change to `table_has_rls` must not
    /// conflate force with enable.
    #[test]
    fn register_allows_aggregators_on_force_only_table() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT); \
             ALTER TABLE orders FORCE ROW LEVEL SECURITY;",
        )
        .expect("parse force-only DDL");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        let result = engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT COUNT(*) FROM orders",
            ))
            .expect("aggregate on force-only (RLS-disabled) table must register");
        assert!(result.aggregate_spec().is_some());
    }
    #[test]
    fn aggregate_demotion_preserves_registration_binds_and_mode() {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        let registered = engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT COUNT(*) FROM orders WHERE amount > $1")
                    .binds(alloc::vec![Value::Int(0)])
                    .database_reads_per_consumer(),
            )
            .expect("aggregate registers");
        assert!(matches!(registered.tier, Tier::InProcess(_)));

        let (transition, _) = engine
            .transition_aggregate_to_whole(
                registered.subscription_id,
                crate::MaintenanceStopReason::GroupLimit { limit: 1 },
                None,
            )
            .expect("aggregate demotes");
        let Tier::WholeRows { query, .. } = transition.to else {
            panic!("expected whole rows")
        };
        assert_eq!(query.binds(), &[Value::Int(0)]);
        let entry = engine
            .reexec
            .get(&registered.subscription_id)
            .expect("whole read is stored");
        assert!(entry.database_reads_per_consumer);
        assert_eq!(entry.source_query.binds(), &[Value::Int(0)]);
    }

    #[test]
    fn keyed_transition_preserves_registration_binds_and_mode() {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        let table = catalog_helpers::table_id(engine.database(), "orders").expect("orders");
        let registered = engine
            .register(
                SubscriptionRequest::new(1u64, "SELECT * FROM orders WHERE lower(status) = $1")
                    .binds(alloc::vec![Value::String("paid".into())])
                    .database_reads_per_consumer(),
            )
            .expect("keyed read registers");
        assert!(matches!(registered.tier, Tier::KeyedRows { .. }));

        let (transition, _) = engine
            .transition_keyed_to_whole(registered.subscription_id, table, None)
            .expect("keyed read transitions");
        let Tier::WholeRows { query, .. } = transition.to else {
            panic!("expected whole rows")
        };
        assert_eq!(query.binds(), &[Value::String("paid".into())]);
        let entry = engine
            .reexec
            .get(&registered.subscription_id)
            .expect("whole read is stored");
        assert!(entry.database_reads_per_consumer);
        assert_eq!(entry.source_query.binds(), &[Value::String("paid".into())]);
    }

    /// One registration performs one logical mutation of its partition, so it
    /// publishes exactly one new snapshot: predicate, binding, and seeds land
    /// together, never as separate publications each paying a store clone.
    #[test]
    fn one_registration_publishes_one_partition_snapshot() {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        engine.register(spec(1, 1)).expect("registers");
        let partition = engine
            .partitions
            .values()
            .next()
            .expect("one registration creates its partition");
        assert_eq!(
            partition.publication_count(),
            1,
            "predicate and binding must publish together, not one snapshot each"
        );
    }

    /// One membership event is one logical mutation of the subscribed
    /// partition, even when it moves both halves of an update, so it publishes
    /// at most one new snapshot.
    #[cfg(feature = "membership-term")]
    #[test]
    fn one_membership_event_publishes_one_partition_snapshot() {
        use crate::backend::Value;
        use rls2fga::translator::TranslatorBuilder;
        use rls2fga::types::ConfidenceLevel;

        let ddl = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
             CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT);";
        let db = ParserDB::parse::<PostgreSqlDialect>(ddl).expect("DDL parses");
        let docs = crate::catalog_helpers::table_id(&db, "docs").expect("docs");
        let members = crate::catalog_helpers::table_id(&db, "project_members").expect("members");
        let translator = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build();
        let mut engine: Engine =
            SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator);
        engine
            .register(
                SubscriptionRequest::new(
                    1u64,
                    "SELECT * FROM docs WHERE project_id IN \
                     (SELECT project_id FROM project_members \
                      WHERE user_id = current_setting('app.user_id', true))",
                )
                .subscriber(Value::String("alice".into()))
                .term_values(
                    alloc::vec!["project_id"],
                    alloc::vec![alloc::vec![Value::Int(7)]],
                ),
            )
            .expect("term subscription registers");

        let before = engine.partitions[&docs].publication_count();
        // An update moves both halves, leaving project 7 and entering 11, which
        // is the strongest case: two term moves in one event.
        engine
            .consumers(&TestEvent::update(
                members,
                alloc::vec![Value::Int(7), Value::String("alice".into())],
                alloc::vec![Value::Int(11), Value::String("alice".into())],
            ))
            .expect("membership event dispatches");
        let after = engine.partitions[&docs].publication_count();
        assert_eq!(
            after - before,
            1,
            "both halves of the move must ride one snapshot publication"
        );
    }
}
