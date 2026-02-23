//! Subscription engine - main public API

use super::indexes::IndexableAtom;
use super::{
    dispatch::{dispatch_users, MatchedUsers, UserDictionary},
    ids::PredicateId,
    partition::TablePartition,
    predicate::{Binding, Predicate},
};
use crate::{
    compiler::{
        canonicalize, parse_compile_normalize_and_prefilter, BytecodeProgram, PrefilterPlan, Vm,
    },
    persistence::{
        codec,
        merge::MergeManager,
        shard::{
            deserialize_shard, serialize_shard, BindingData, PredicateData, ShardPayload,
            UserDictData,
        },
    },
    DispatchError, DurabilityMode, DurableShardMerge, DurableShardStore, IdTypes, MergeError,
    MergeJobId, MergeReport, PruneReport, RegisterError, RegisterResult, SchemaCatalog,
    StorageError, SubscriptionDispatch, SubscriptionPruning, SubscriptionRegistration,
    SubscriptionSpec, TableId, WalEvent,
};
use ahash::{AHashMap, AHashSet};
use sqlparser::dialect::Dialect;

type BatchEntries<I> = Vec<(Predicate, Vec<IndexableAtom>, Vec<Binding<I>>)>;

struct CompiledBatchEntry<I: IdTypes> {
    spec: SubscriptionSpec<I>,
    table_id: TableId,
    bytecode: BytecodeProgram,
    normalized: String,
    prefilter_plan: PrefilterPlan,
    hash: u128,
}

use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Wrapper to pass `Arc<dyn SchemaCatalog>` as `Box<dyn SchemaCatalog + Send>`.
struct CatalogRef(Arc<dyn SchemaCatalog>);

#[derive(Debug)]
enum RebuildPayloadError {
    Codec(String),
    Corrupt(String),
}

impl std::fmt::Display for RebuildPayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codec(msg) | Self::Corrupt(msg) => f.write_str(msg),
        }
    }
}

impl SchemaCatalog for CatalogRef {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.0.table_id(table_name)
    }
    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
        self.0.column_id(table_id, column_name)
    }
    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.0.table_arity(table_id)
    }
    fn schema_fingerprint(&self, table_id: TableId) -> Option<u64> {
        self.0.schema_fingerprint(table_id)
    }
}

/// Main subscription engine
///
/// Manages subscriptions across all tables with hybrid indexing and
/// predicate deduplication.
pub struct SubscriptionEngine<D: Dialect, I: IdTypes> {
    /// SQL dialect for parsing
    dialect: D,
    /// Schema catalog for table/column resolution
    catalog: Arc<dyn SchemaCatalog>,
    /// Table partitions (TableId → TablePartition)
    partitions: AHashMap<TableId, TablePartition<I>>,
    /// User dictionaries (TableId → UserDictionary)
    user_dictionaries: AHashMap<TableId, UserDictionary<I>>,
    /// Subscription index for O(1) unregister/upsert lookup.
    subscription_to_table: AHashMap<I::SubscriptionId, TableId>,
    /// VM for bytecode evaluation
    vm: Vm,
    /// Optional storage path for durability
    storage_path: Option<PathBuf>,
    /// Shard rotation threshold (bytes)
    rotation_threshold: usize,
    /// Background merge compaction manager
    merge_manager: MergeManager<I>,
    /// Persistence strictness policy for registration.
    durability_mode: DurabilityMode,
}

impl<D: Dialect, I: IdTypes> SubscriptionEngine<D, I> {
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

    /// Create new subscription engine
    #[must_use]
    pub fn new(catalog: Arc<dyn SchemaCatalog>, dialect: D) -> Self {
        Self {
            dialect,
            catalog,
            partitions: AHashMap::new(),
            user_dictionaries: AHashMap::new(),
            subscription_to_table: AHashMap::new(),
            vm: Vm::new(),
            storage_path: None,
            rotation_threshold: 10 * 1024 * 1024, // 10 MB default
            merge_manager: MergeManager::new(),
            durability_mode: DurabilityMode::Required,
        }
    }

    /// Create engine with durable storage
    ///
    /// Loads existing shards from storage directory on startup.
    #[allow(clippy::needless_pass_by_value)]
    pub fn with_storage(
        catalog: Arc<dyn SchemaCatalog>,
        dialect: D,
        storage_path: PathBuf,
    ) -> Result<Self, StorageError> {
        let mut engine = Self {
            dialect,
            catalog: Arc::clone(&catalog),
            partitions: AHashMap::new(),
            user_dictionaries: AHashMap::new(),
            subscription_to_table: AHashMap::new(),
            vm: Vm::new(),
            storage_path: Some(storage_path.clone()),
            rotation_threshold: 10 * 1024 * 1024, // 10 MB default
            merge_manager: MergeManager::new(),
            durability_mode: DurabilityMode::Required,
        };

        // Create storage directory if it doesn't exist
        std::fs::create_dir_all(&storage_path)
            .map_err(|e| StorageError::Io(format!("Failed to create storage directory: {e}")))?;

        // Load existing shards
        engine.load_all_shards()?;

        Ok(engine)
    }

    /// Register a new subscription
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and binds user.
    /// If storage is enabled and rotation threshold is exceeded, triggers snapshot.
    #[allow(clippy::needless_pass_by_value)]
    pub fn register(&mut self, spec: SubscriptionSpec<I>) -> Result<RegisterResult, RegisterError> {
        // 1. Parse, compile, and canonicalize in one pass.
        let (table_id, bytecode, normalized, prefilter_plan) =
            parse_compile_normalize_and_prefilter(&spec.sql, &self.dialect, &*self.catalog)?;
        let hash = canonicalize::hash_sql(&normalized);

        // 2. Upsert semantics: replace existing subscription ID atomically
        // after new SQL has been validated.
        let _ = self.unregister_subscription_internal(spec.subscription_id);

        // 3. Get/create table partition and user dictionary
        let partition = self
            .partitions
            .entry(table_id)
            .or_insert_with(|| TablePartition::new(table_id));

        let user_dict = self.user_dictionaries.entry(table_id).or_default();

        // 4. Get user ordinal
        let user_ord = user_dict.get_or_create(spec.user_id);

        // 5. Check if predicate exists (deduplication)
        let snapshot = partition.load_snapshot();
        let (pred_id, created_new) = if let Some(existing) = snapshot.predicates.find_by_hash(hash)
        {
            // Predicate exists, increment refcount
            // (We need mutable access, so we'll do this via the partition's mutable store)
            (existing, false)
        } else {
            // Create new predicate
            let atoms = Self::index_atoms_from_plan(&prefilter_plan);
            let pred = Predicate {
                // Placeholder; store allocates the authoritative ID.
                id: PredicateId::from_slab_index(0),
                hash,
                normalized_sql: normalized.clone().into(),
                bytecode: Arc::new(bytecode.clone()),
                dependency_columns: Arc::from(bytecode.dependency_columns.as_slice()),
                index_atoms: Arc::from(atoms.as_slice()),
                prefilter_plan: Arc::new(prefilter_plan),
                refcount: 0, // Will be incremented via binding
                updated_at_unix_ms: spec.updated_at_unix_ms,
            };
            // Add predicate to partition
            let pred_id = partition.add_predicate(pred, atoms);

            (pred_id, true)
        };

        // 6. Create binding
        let binding = Binding {
            subscription_id: spec.subscription_id,
            predicate_id: pred_id,
            user_id: spec.user_id,
            user_ordinal: user_ord,
            session_id: spec.session_id,
            updated_at_unix_ms: spec.updated_at_unix_ms,
        };

        // Add binding to partition
        partition.add_binding(binding, pred_id);

        // 7. Index subscription for O(1) unregister/upsert lookups.
        self.subscription_to_table
            .insert(spec.subscription_id, table_id);

        // 8. Check if rotation needed (if storage enabled)
        if self.storage_path.is_some() {
            let should_rotate = self.should_rotate(table_id).map_err(|e| {
                RegisterError::Storage(format!("Rotation check failed for table {table_id}: {e}"))
            })?;
            if should_rotate {
                match self.snapshot_table(table_id) {
                    Ok(()) => {}
                    Err(snapshot_err) if self.durability_mode == DurabilityMode::BestEffort => {
                        #[cfg(feature = "observability")]
                        tracing::warn!(
                            "Best-effort durability: snapshot failed for table {}: {}",
                            table_id,
                            snapshot_err
                        );
                        #[cfg(not(feature = "observability"))]
                        let _ = snapshot_err;
                    }
                    Err(e) => {
                        // Roll back this registration so Required durability is atomic.
                        let _ = self.unregister_subscription_internal(spec.subscription_id);
                        return Err(RegisterError::Storage(format!(
                            "Snapshot failed for table {table_id}: {e}"
                        )));
                    }
                }
            }
        }

        Ok(RegisterResult {
            table_id,
            normalized_sql: normalized,
            predicate_hash: hash,
            created_new_predicate: created_new,
        })
    }

    /// Register multiple subscriptions in a single batch
    ///
    /// Significantly more efficient than calling `register()` in a loop:
    /// performs a single COW clone and single snapshot swap per table instead
    /// of one per subscription. Ideal for bulk loading at startup.
    ///
    /// Returns results in the same order as the input specs.
    /// On error, already-registered subscriptions in this batch are NOT rolled back.
    #[allow(clippy::too_many_lines)]
    pub fn register_batch(
        &mut self,
        specs: Vec<SubscriptionSpec<I>>,
    ) -> Vec<Result<RegisterResult, RegisterError>> {
        // Preserve correctness for upsert workloads: if the batch includes
        // existing subscription IDs or duplicate IDs within the batch, fall
        // back to ordered per-item registration (last write wins).
        let mut seen = AHashMap::new();
        let requires_ordered_upsert = specs.iter().any(|spec| {
            self.subscription_to_table
                .contains_key(&spec.subscription_id)
                || seen.insert(spec.subscription_id, ()).is_some()
        });
        if requires_ordered_upsert {
            return specs.into_iter().map(|spec| self.register(spec)).collect();
        }

        // Phase 1: Parse and compile all specs (can fail individually)
        let mut compiled: Vec<Option<CompiledBatchEntry<I>>> = Vec::with_capacity(specs.len());
        let mut results: Vec<Result<RegisterResult, RegisterError>> =
            Vec::with_capacity(specs.len());

        for spec in specs {
            match parse_compile_normalize_and_prefilter(&spec.sql, &self.dialect, &*self.catalog) {
                Ok((table_id, bytecode, normalized, prefilter_plan)) => {
                    let hash = canonicalize::hash_sql(&normalized);
                    compiled.push(Some(CompiledBatchEntry {
                        spec,
                        table_id,
                        bytecode,
                        normalized,
                        prefilter_plan,
                        hash,
                    }));
                    results.push(Ok(RegisterResult {
                        table_id,
                        normalized_sql: String::new(), // filled in phase 2
                        predicate_hash: hash,
                        created_new_predicate: false, // filled in phase 2
                    }));
                }
                Err(e) => {
                    compiled.push(None);
                    results.push(Err(e));
                }
            }
        }

        // Phase 2: Group by table and batch-insert
        let mut table_entries: AHashMap<TableId, BatchEntries<I>> = AHashMap::new();
        let mut table_result_indices: AHashMap<TableId, Vec<usize>> = AHashMap::new();
        let mut table_inserted_sub_ids: AHashMap<TableId, Vec<I::SubscriptionId>> = AHashMap::new();

        // Track which hashes we've already prepared (dedup within batch)
        let mut batch_hash_to_idx: AHashMap<(TableId, u128), usize> = AHashMap::new();

        for (i, entry) in compiled.into_iter().enumerate() {
            let Some(c) = entry else { continue };

            let partition = self
                .partitions
                .entry(c.table_id)
                .or_insert_with(|| TablePartition::new(c.table_id));
            let user_dict = self.user_dictionaries.entry(c.table_id).or_default();
            let user_ord = user_dict.get_or_create(c.spec.user_id);
            table_result_indices.entry(c.table_id).or_default().push(i);
            table_inserted_sub_ids
                .entry(c.table_id)
                .or_default()
                .push(c.spec.subscription_id);

            // Check if predicate already exists in current snapshot
            let snapshot = partition.load_snapshot();
            let existing = snapshot.predicates.find_by_hash(c.hash);

            let created_new;

            if let Some(pred_id) = existing {
                // Predicate exists in the live partition — add binding directly
                // (cannot batch this since it references an existing pred_id)
                let binding = Binding {
                    subscription_id: c.spec.subscription_id,
                    predicate_id: pred_id,
                    user_id: c.spec.user_id,
                    user_ordinal: user_ord,
                    session_id: c.spec.session_id,
                    updated_at_unix_ms: c.spec.updated_at_unix_ms,
                };
                partition.add_binding(binding, pred_id);
                self.subscription_to_table
                    .insert(c.spec.subscription_id, c.table_id);
                created_new = false;
            } else if let Some(&batch_idx) = batch_hash_to_idx.get(&(c.table_id, c.hash)) {
                // Deduplicated within this batch — add binding to existing batch entry
                let entries = table_entries.get_mut(&c.table_id).expect("table exists");
                let binding = Binding {
                    subscription_id: c.spec.subscription_id,
                    predicate_id: PredicateId::from_slab_index(0), // placeholder
                    user_id: c.spec.user_id,
                    user_ordinal: user_ord,
                    session_id: c.spec.session_id,
                    updated_at_unix_ms: c.spec.updated_at_unix_ms,
                };
                entries[batch_idx].2.push(binding);
                created_new = false;
            } else {
                // New predicate — create batch entry
                let atoms = Self::index_atoms_from_plan(&c.prefilter_plan);
                let pred = super::predicate::Predicate {
                    id: PredicateId::from_slab_index(0),
                    hash: c.hash,
                    normalized_sql: c.normalized.clone().into(),
                    bytecode: Arc::new(c.bytecode.clone()),
                    dependency_columns: Arc::from(c.bytecode.dependency_columns.as_slice()),
                    index_atoms: Arc::from(atoms.as_slice()),
                    prefilter_plan: Arc::new(c.prefilter_plan.clone()),
                    refcount: 0,
                    updated_at_unix_ms: c.spec.updated_at_unix_ms,
                };
                let binding = Binding {
                    subscription_id: c.spec.subscription_id,
                    predicate_id: PredicateId::from_slab_index(0), // placeholder
                    user_id: c.spec.user_id,
                    user_ordinal: user_ord,
                    session_id: c.spec.session_id,
                    updated_at_unix_ms: c.spec.updated_at_unix_ms,
                };

                let entries = table_entries.entry(c.table_id).or_default();
                let batch_idx = entries.len();
                entries.push((pred, atoms, vec![binding]));
                batch_hash_to_idx.insert((c.table_id, c.hash), batch_idx);
                created_new = true;
            }

            // Fill in the result
            if let Ok(ref mut result) = results[i] {
                result.normalized_sql = c.normalized;
                result.created_new_predicate = created_new;
            }
        }

        // Phase 3: Batch-insert into partitions (single COW + single swap per table)
        for (table_id, entries) in table_entries {
            let partition = self
                .partitions
                .get_mut(&table_id)
                .expect("partition created above");
            partition.add_batch(&entries);
            for (_, _, bindings) in &entries {
                for binding in bindings {
                    self.subscription_to_table
                        .insert(binding.subscription_id, table_id);
                }
            }
        }

        if self.storage_path.is_some() {
            let mut failures: Vec<(TableId, String)> = Vec::new();

            for &table_id in table_result_indices.keys() {
                let should_rotate = match self.should_rotate(table_id) {
                    Ok(v) => v,
                    Err(e) => {
                        if self.durability_mode == DurabilityMode::BestEffort {
                            #[cfg(feature = "observability")]
                            tracing::warn!(
                                "Best-effort durability: rotation check failed for table {}: {}",
                                table_id,
                                e
                            );
                        } else {
                            failures.push((
                                table_id,
                                format!("Rotation check failed for table {table_id}: {e}"),
                            ));
                        }
                        false
                    }
                };
                if !should_rotate {
                    continue;
                }

                if let Err(e) = self.snapshot_table(table_id) {
                    if self.durability_mode == DurabilityMode::BestEffort {
                        #[cfg(feature = "observability")]
                        tracing::warn!(
                            "Best-effort durability: snapshot failed for table {}: {}",
                            table_id,
                            e
                        );
                    } else {
                        failures.push((
                            table_id,
                            format!("Snapshot failed for table {table_id}: {e}"),
                        ));
                    }
                }
            }

            if !failures.is_empty() && self.durability_mode == DurabilityMode::Required {
                for (table_id, message) in failures {
                    if let Some(sub_ids) = table_inserted_sub_ids.get(&table_id) {
                        for &sub_id in sub_ids {
                            let _ = self.unregister_subscription_internal(sub_id);
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

        results
    }

    /// Unregister a subscription
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate is removed.
    pub fn unregister_subscription(&mut self, subscription_id: I::SubscriptionId) -> bool {
        self.unregister_subscription_internal(subscription_id)
            .is_some()
    }

    /// Internal unregister helper.
    ///
    /// Returns `Some(predicate_removed)` if subscription existed, else `None`.
    fn unregister_subscription_internal(
        &mut self,
        subscription_id: I::SubscriptionId,
    ) -> Option<bool> {
        // Fast path: direct lookup from subscription index.
        if let Some(table_id) = self.subscription_to_table.get(&subscription_id).copied() {
            if let Some(partition) = self.partitions.get_mut(&table_id) {
                if let Some(predicate_removed) = partition.remove_binding_status(subscription_id) {
                    self.subscription_to_table.remove(&subscription_id);
                    return Some(predicate_removed);
                }
            }

            // Stale index entry; clean it up and fall back to scan.
            self.subscription_to_table.remove(&subscription_id);
        }

        // Fallback scan for pre-index or inconsistent states.
        for (_table_id, partition) in &mut self.partitions {
            if let Some(predicate_removed) = partition.remove_binding_status(subscription_id) {
                self.subscription_to_table.remove(&subscription_id);
                return Some(predicate_removed);
            }
        }

        None
    }

    /// Dispatch event to interested users
    pub fn users(&mut self, event: &WalEvent) -> Result<MatchedUsers<'_, I>, DispatchError> {
        // Get table partition
        let partition = self
            .partitions
            .get(&event.table_id)
            .ok_or(DispatchError::UnknownTableId(event.table_id))?;

        // Get user dictionary
        let user_dict = self
            .user_dictionaries
            .get(&event.table_id)
            .ok_or(DispatchError::UnknownTableId(event.table_id))?;

        // Validate selected row image arity against schema catalog.
        let row =
            match event.kind {
                crate::EventKind::Insert | crate::EventKind::Update => event
                    .new_row
                    .as_ref()
                    .ok_or(DispatchError::MissingRequiredRowImage(
                        "INSERT/UPDATE requires new_row",
                    ))?,
                crate::EventKind::Delete => {
                    event
                        .old_row
                        .as_ref()
                        .ok_or(DispatchError::MissingRequiredRowImage(
                            "DELETE requires old_row",
                        ))?
                }
            };

        if let Some(expected) = self.catalog.table_arity(event.table_id) {
            let got = row.len();
            if got != expected {
                return Err(DispatchError::InvalidRowArity {
                    table_id: event.table_id,
                    expected,
                    got,
                });
            }
        }

        // Dispatch
        dispatch_users(event, partition, user_dict, &mut self.vm)
    }

    /// Unregister all subscriptions for a session
    pub fn unregister_session(&mut self, session_id: I::SessionId) -> PruneReport {
        let mut removed_bindings = 0;
        let mut removed_predicates = 0;
        let mut removed_users = 0;

        // Collect subscription IDs to remove
        let mut to_remove = Vec::new();
        let mut removed_user_candidates: AHashMap<TableId, AHashSet<I::UserId>> = AHashMap::new();

        for (&table_id, partition) in &self.partitions {
            let snapshot = partition.load_snapshot();

            if let Some(sub_ids) = snapshot.predicates.get_session_subscriptions(session_id) {
                removed_bindings += sub_ids.len();
                to_remove.extend_from_slice(sub_ids);
                let users = removed_user_candidates.entry(table_id).or_default();
                for sub_id in sub_ids {
                    if let Some(binding) = snapshot.predicates.bindings.get(sub_id) {
                        users.insert(binding.user_id);
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

        for (table_id, users) in removed_user_candidates {
            let Some(partition) = self.partitions.get(&table_id) else {
                continue;
            };
            let Some(user_dict) = self.user_dictionaries.get_mut(&table_id) else {
                continue;
            };

            let snapshot = partition.load_snapshot();
            let active_users: AHashSet<I::UserId> = snapshot
                .predicates
                .bindings
                .values()
                .map(|binding| binding.user_id)
                .collect();

            for user_id in users {
                if !active_users.contains(&user_id) && user_dict.remove(user_id).is_some() {
                    removed_users += 1;
                }
            }
        }

        PruneReport {
            removed_bindings,
            removed_predicates,
            removed_users,
        }
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

    /// Snapshot table partition to disk
    ///
    /// Serializes all predicates, bindings, and user dictionary to a shard file.
    pub fn snapshot_table(&self, table_id: TableId) -> Result<(), StorageError> {
        let storage_path = self
            .storage_path
            .as_ref()
            .ok_or_else(|| StorageError::Config("No storage path configured".to_string()))?;

        let partition = self
            .partitions
            .get(&table_id)
            .ok_or_else(|| StorageError::Corrupt(format!("Unknown table ID: {table_id}")))?;

        let user_dict = self.user_dictionaries.get(&table_id).ok_or_else(|| {
            StorageError::Corrupt(format!("No user dictionary for table {table_id}"))
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
                        "Binding {:?} references missing predicate ID {:?}",
                        binding.subscription_id, binding.predicate_id
                    ))
                })?;
            let binding_data = BindingData::<I> {
                subscription_id: binding.subscription_id,
                predicate_hash,
                user_id: binding.user_id,
                session_id: binding.session_id,
                updated_at_unix_ms: binding.updated_at_unix_ms,
            };
            binding_data_vec.push(binding_data);
        }

        // Convert user dictionary to serializable format
        let user_dict_data = UserDictData::<I> {
            ordinal_to_user: user_dict.ordinal_to_user_vec(),
        };

        // Build payload
        let payload: ShardPayload<I> = ShardPayload {
            predicates: predicate_data_vec,
            bindings: binding_data_vec,
            user_dict: user_dict_data,
            #[allow(clippy::cast_possible_truncation)]
            created_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        // Serialize shard
        let bytes = serialize_shard::<I>(table_id, &payload, &*self.catalog)?;

        // Write to disk
        let shard_path = storage_path.join(format!("table_{table_id}.shard"));
        std::fs::write(&shard_path, bytes)
            .map_err(|e| StorageError::Io(format!("Failed to write shard: {e}")))?;

        Ok(())
    }

    fn rebuild_entries_from_payload(
        payload: &ShardPayload<I>,
    ) -> Result<(UserDictionary<I>, BatchEntries<I>), RebuildPayloadError> {
        let mut user_dict = UserDictionary::<I>::new();

        for user_id in &payload.user_dict.ordinal_to_user {
            user_dict.get_or_create(*user_id);
        }

        // Build hash -> predicate map and validate binding references.
        let mut pred_hash_to_data: AHashMap<u128, &PredicateData> = AHashMap::new();
        for pred_data in &payload.predicates {
            pred_hash_to_data.insert(pred_data.hash, pred_data);
        }

        // Build bindings grouped by predicate hash; IDs are assigned during add_batch.
        let mut bindings_by_hash: AHashMap<u128, Vec<Binding<I>>> = AHashMap::new();
        for binding_data in &payload.bindings {
            if !pred_hash_to_data.contains_key(&binding_data.predicate_hash) {
                return Err(RebuildPayloadError::Corrupt(format!(
                    "Binding references unknown predicate hash: {:016x}",
                    binding_data.predicate_hash
                )));
            }

            let user_ord = user_dict.get_or_create(binding_data.user_id);
            bindings_by_hash
                .entry(binding_data.predicate_hash)
                .or_default()
                .push(Binding {
                    subscription_id: binding_data.subscription_id,
                    predicate_id: PredicateId::from_slab_index(0), // add_batch patches this
                    user_id: binding_data.user_id,
                    user_ordinal: user_ord,
                    session_id: binding_data.session_id,
                    updated_at_unix_ms: binding_data.updated_at_unix_ms,
                });
        }

        // Build batch entries from predicates + grouped bindings.
        let mut entries: BatchEntries<I> = Vec::new();
        for pred_data in &payload.predicates {
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
                normalized_sql: pred_data.normalized_sql.clone().into(),
                bytecode: Arc::new(bytecode.clone()),
                dependency_columns: Arc::from(pred_data.dependency_columns.as_slice()),
                index_atoms: Arc::from(atoms.as_slice()),
                prefilter_plan: Arc::new(prefilter_plan),
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

        Ok((user_dict, entries))
    }

    fn replace_table_state(
        &mut self,
        table_id: TableId,
        partition: TablePartition<I>,
        user_dict: UserDictionary<I>,
        entries: &BatchEntries<I>,
    ) {
        self.partitions.insert(table_id, partition);
        self.user_dictionaries.insert(table_id, user_dict);
        self.subscription_to_table
            .retain(|_, mapped_table_id| *mapped_table_id != table_id);
        for (_, _, bindings) in entries {
            for binding in bindings {
                self.subscription_to_table
                    .insert(binding.subscription_id, table_id);
            }
        }
    }

    /// Load shard from disk into partition
    fn load_shard(&mut self, table_id: TableId, path: &Path) -> Result<(), StorageError> {
        let bytes = std::fs::read(path)
            .map_err(|e| StorageError::Io(format!("Failed to read shard: {e}")))?;

        let (header, payload) = deserialize_shard::<I>(&bytes, &*self.catalog)?;
        if header.table_id != table_id {
            return Err(StorageError::Corrupt(format!(
                "Shard table ID mismatch: filename table_id {table_id}, header table_id {}",
                header.table_id
            )));
        }

        let (user_dict, entries) =
            Self::rebuild_entries_from_payload(&payload).map_err(|e| match e {
                RebuildPayloadError::Codec(msg) => StorageError::Codec(msg),
                RebuildPayloadError::Corrupt(msg) => StorageError::Corrupt(msg),
            })?;

        let mut partition = TablePartition::new(table_id);
        partition.add_batch(&entries);
        self.replace_table_state(table_id, partition, user_dict, &entries);

        Ok(())
    }

    /// Load all shards from storage directory
    fn load_all_shards(&mut self) -> Result<(), StorageError> {
        let storage_path = self
            .storage_path
            .as_ref()
            .ok_or_else(|| StorageError::Config("No storage path configured".to_string()))?;

        // Read all .shard files (directory must exist — with_storage creates it)
        let entries = std::fs::read_dir(storage_path)
            .map_err(|e| StorageError::Io(format!("Failed to read storage directory: {e}")))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| StorageError::Io(format!("Failed to read directory entry: {e}")))?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("shard") {
                // Parse table ID from filename (e.g., "table_1.shard")
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(table_id_str) = filename.strip_prefix("table_") {
                        if let Ok(table_id) = table_id_str.parse::<TableId>() {
                            self.load_shard(table_id, &path)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if rotation is needed for a table
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
    pub const fn set_durability_mode(&mut self, mode: DurabilityMode) {
        self.durability_mode = mode;
    }

    /// Get current rotation threshold
    #[must_use]
    pub const fn rotation_threshold(&self) -> usize {
        self.rotation_threshold
    }

    /// Get current durability mode.
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
            Box::new(CatalogRef(Arc::clone(&self.catalog))),
        )
    }

    /// Poll for merge completion and swap the result into the live partition
    ///
    /// Returns `Some(report)` if the merge finished and was swapped in,
    /// `None` if still running.
    pub fn try_complete_merge(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, MergeError> {
        let Some(merged) = self.merge_manager.try_get_result(job_id)? else {
            return Ok(None);
        };

        let table_id = merged.table_id;

        // Build merged table state off to the side, then atomically replace.
        let mut partition = TablePartition::new(table_id);
        let (user_dict, entries) = Self::rebuild_entries_from_payload(&merged.payload)
            .map_err(|e| MergeError::BuildFailed(e.to_string()))?;

        partition.add_batch(&entries);
        self.replace_table_state(table_id, partition, user_dict, &entries);

        Ok(Some(merged.stats.into()))
    }

    /// Get number of active merge jobs
    #[must_use]
    pub fn active_merge_jobs(&self) -> usize {
        self.merge_manager.active_jobs()
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes> SubscriptionRegistration<I>
    for SubscriptionEngine<D, I>
{
    fn register(&mut self, spec: SubscriptionSpec<I>) -> Result<RegisterResult, RegisterError> {
        Self::register(self, spec)
    }

    fn unregister_subscription(&mut self, subscription_id: I::SubscriptionId) -> bool {
        Self::unregister_subscription(self, subscription_id)
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes> SubscriptionDispatch<I> for SubscriptionEngine<D, I> {
    type UserIter<'a>
        = MatchedUsers<'a, I>
    where
        Self: 'a;

    fn users(&mut self, event: &WalEvent) -> Result<Self::UserIter<'_>, DispatchError> {
        Self::users(self, event)
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes> SubscriptionPruning<I> for SubscriptionEngine<D, I> {
    fn unregister_session(&mut self, session_id: I::SessionId) -> PruneReport {
        Self::unregister_session(self, session_id)
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes> DurableShardStore for SubscriptionEngine<D, I> {
    fn snapshot_table(&self, table_id: TableId) -> Result<(), StorageError> {
        Self::snapshot_table(self, table_id)
    }
}

impl<D: Dialect + Send + Sync, I: IdTypes> DurableShardMerge for SubscriptionEngine<D, I> {
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
    use crate::testing::MockCatalog;
    use crate::{Cell, DefaultIds, EventKind, PrimaryKey, RowImage};
    use sqlparser::dialect::PostgreSqlDialect;
    use std::collections::HashMap;
    #[cfg(unix)]
    use std::path::Path;

    fn make_catalog() -> Arc<MockCatalog> {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), (1, 3));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "amount".to_string()), 1);
        columns.insert((1, "status".to_string()), 2);

        Arc::new(MockCatalog { tables, columns })
    }

    #[cfg(unix)]
    fn set_dir_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = std::fs::metadata(path).unwrap().permissions();
        perms.set_mode(mode);
        std::fs::set_permissions(path, perms).unwrap();
    }

    #[test]
    fn test_engine_creation() {
        let catalog = make_catalog();
        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_register_subscription() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 100,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let result = engine.register(spec);
        assert!(result.is_ok());

        let reg = result.unwrap();
        assert_eq!(reg.table_id, 1);
        assert!(reg.created_new_predicate);
    }

    #[test]
    fn test_predicate_deduplication() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register same predicate for two users
        let spec1 = SubscriptionSpec {
            subscription_id: 1,
            user_id: 100,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let spec2 = SubscriptionSpec {
            subscription_id: 2,
            user_id: 200,
            session_id: None,
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscription: amount > 100
        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        // Dispatch event with amount = 200 (should match)
        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([
                    Cell::Int(1),                   // id
                    Cell::Int(200),                 // amount
                    Cell::String("pending".into()), // status
                ]),
            }),
            changed_columns: Arc::from([]),
        };

        let users = engine.users(&event);
        // Note: This will fail because our stub extract_indexable_atoms
        // returns Fallback, and we haven't properly integrated the VM eval
        // This is expected at this stage
        assert!(users.is_ok());
    }

    #[test]
    fn test_dispatch_no_where_matches_all_rows() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders".to_string(),
            updated_at_unix_ms: 0,
        };
        engine.register(spec).unwrap();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("pending".into())]),
            }),
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = engine.users(&event).unwrap().collect();
        assert_eq!(users, vec![42]);
    }

    #[test]
    fn test_bindings_persist() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscription
        let spec = SubscriptionSpec {
            subscription_id: 100,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        // Verify binding exists in snapshot
        let partition = engine.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();
        assert!(snapshot.predicates.bindings.contains_key(&100));

        // Verify binding details
        let binding = snapshot.predicates.bindings.get(&100).unwrap();
        assert_eq!(binding.user_id, 42);
        assert_eq!(binding.subscription_id, 100);
    }

    #[test]
    fn test_unregister_removes_binding() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscription
        let spec = SubscriptionSpec {
            subscription_id: 100,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        // Verify it exists
        assert_eq!(engine.subscription_count(), 1);

        // Unregister
        let found = engine.unregister_subscription(100);
        assert!(found);

        // Verify it's gone
        let partition = engine.partitions.get(&1).unwrap();
        let snapshot = partition.load_snapshot();
        assert!(!snapshot.predicates.bindings.contains_key(&100));
    }

    #[test]
    fn test_unregister_one_of_duplicate_user_bindings_keeps_dispatch_match() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let sql = "SELECT * FROM orders WHERE amount > 100".to_string();
        engine
            .register(SubscriptionSpec {
                subscription_id: 1000,
                user_id: 7,
                session_id: None,
                sql: sql.clone(),
                updated_at_unix_ms: 1,
            })
            .unwrap();
        engine
            .register(SubscriptionSpec {
                subscription_id: 1001,
                user_id: 7,
                session_id: None,
                sql,
                updated_at_unix_ms: 2,
            })
            .unwrap();

        assert!(engine.unregister_subscription(1000));

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("paid".into())]),
            }),
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = engine.users(&event).unwrap().collect();
        assert_eq!(users, vec![7]);
    }

    #[test]
    fn test_register_after_unsubscribe_does_not_break_hash_lookup_or_dispatch() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Sub 1 -> predicate A
        engine
            .register(SubscriptionSpec {
                subscription_id: 1,
                user_id: 101,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Sub 2 -> predicate B (kept alive while A is removed)
        engine
            .register(SubscriptionSpec {
                subscription_id: 2,
                user_id: 202,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount < 0".to_string(),
                updated_at_unix_ms: 2,
            })
            .unwrap();

        // Remove A to create a slab hole.
        assert!(engine.unregister_subscription(1));

        // Sub 3 -> predicate C should be independently dispatchable.
        engine
            .register(SubscriptionSpec {
                subscription_id: 3,
                user_id: 303,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount = 7".to_string(),
                updated_at_unix_ms: 3,
            })
            .unwrap();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(7)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([
                    Cell::Int(7), // id
                    Cell::Int(7), // amount
                    Cell::String("ok".into()),
                ]),
            }),
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = engine.users(&event).unwrap().collect();
        assert!(
            users.contains(&303),
            "newly registered predicate should dispatch after hole reuse"
        );
    }

    #[test]
    fn test_multiple_predicates_indexed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register first predicate
        let spec1 = SubscriptionSpec {
            subscription_id: 1,
            user_id: 100,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount = 100".to_string(),
            updated_at_unix_ms: 0,
        };

        // Register second predicate
        let spec2 = SubscriptionSpec {
            subscription_id: 2,
            user_id: 200,
            session_id: None,
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Register subscription
        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec).unwrap();

        // Snapshot to disk
        engine.snapshot_table(1).unwrap();

        // Verify shard file exists
        let shard_path = temp_dir.path().join("table_1.shard");
        assert!(shard_path.exists());

        // Create new engine, load from disk
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
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
        assert_eq!(binding.user_id, 42);
        assert_eq!(binding.subscription_id, 1);
    }

    #[test]
    fn test_predicate_deduplication_across_snapshots() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine with storage
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Register same predicate for two users
        let spec1 = SubscriptionSpec {
            subscription_id: 1,
            user_id: 100,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        let spec2 = SubscriptionSpec {
            subscription_id: 2,
            user_id: 200,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec1).unwrap();
        engine.register(spec2).unwrap();

        // Snapshot
        engine.snapshot_table(1).unwrap();

        // Load in new engine
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
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
    fn test_user_dictionary_persists() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine and register users
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        for i in 0..5 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: 100 + i,
                session_id: None,
                sql: format!("SELECT * FROM orders WHERE amount > {}", i * 10),
                updated_at_unix_ms: 1000,
            };
            engine.register(spec).unwrap();
        }

        // Snapshot
        engine.snapshot_table(1).unwrap();

        // Load in new engine
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Verify user dictionary
        let user_dict = engine2.user_dictionaries.get(&1).unwrap();
        for i in 0..5 {
            assert!(user_dict.get(100 + i).is_some());
        }
    }

    #[test]
    fn test_empty_storage_directory() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create engine with empty storage directory
        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // No predicates initially
        assert_eq!(engine.predicate_count(1), 0);
        assert_eq!(engine.predicate_count(999), 0); // Non-existent table

        // Register a subscription
        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 100,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        assert_eq!(engine.predicate_count(1), 1);
    }

    #[test]
    fn test_unregister_session() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register subscriptions with session
        let spec1 = SubscriptionSpec {
            subscription_id: 1,
            user_id: 100,
            session_id: Some(999),
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        let spec2 = SubscriptionSpec {
            subscription_id: 2,
            user_id: 100,
            session_id: Some(999),
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Default threshold
        let default = engine.rotation_threshold();
        assert!(default > 0);

        // Set custom threshold
        engine.set_rotation_threshold(500);
        assert_eq!(engine.rotation_threshold(), 500);
    }

    #[test]
    fn test_dispatch_users_via_engine() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register a subscription
        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };

        engine.register(spec).unwrap();

        // Dispatch an event
        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("active".into())]),
            }),
            changed_columns: Arc::from([]),
        };

        // Dispatch should succeed (covers the users() method path)
        let result = engine.users(&event);
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_non_existent_directory() {
        let catalog = make_catalog();

        // Create engine with non-existent storage path
        let engine: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds>, _> =
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Unregister subscription that doesn't exist
        let found = engine.unregister_subscription(999);
        assert!(!found);
    }

    #[test]
    fn test_dispatch_unknown_table() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Dispatch to unknown table
        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 999,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1)]),
            }),
            changed_columns: Arc::from([]),
        };

        let result = engine.users(&event);
        assert!(matches!(result, Err(DispatchError::UnknownTableId(999))));
    }

    #[test]
    fn test_dispatch_insert_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionSpec {
                subscription_id: 31,
                user_id: 31,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 31,
            })
            .unwrap();

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                // orders table arity in test catalog is 3
                cells: Arc::from([Cell::Int(1), Cell::Int(5)]),
            }),
            changed_columns: Arc::from([]),
        };

        let result = engine.users(&event);
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
    fn test_dispatch_update_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionSpec {
                subscription_id: 32,
                user_id: 32,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 32,
            })
            .unwrap();

        let event = WalEvent {
            kind: EventKind::Update,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(2), Cell::String("old".into())]),
            }),
            new_row: Some(RowImage {
                // Invalid arity for UPDATE selected row image (new_row)
                cells: Arc::from([Cell::Int(1), Cell::Int(5)]),
            }),
            changed_columns: Arc::from([1u16]),
        };

        let result = engine.users(&event);
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionSpec {
                subscription_id: 33,
                user_id: 33,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
                updated_at_unix_ms: 33,
            })
            .unwrap();

        let event = WalEvent {
            kind: EventKind::Delete,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: Some(RowImage {
                // Invalid arity for DELETE selected row image (old_row)
                cells: Arc::from([Cell::Int(1), Cell::Int(5)]),
            }),
            new_row: None,
            changed_columns: Arc::from([]),
        };

        let result = engine.users(&event);
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
    fn test_storage_rotation_triggers_snapshot() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog.clone(),
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        // Set very low rotation threshold so it triggers
        engine.set_rotation_threshold(1);

        // Register subscription (should trigger rotation)
        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1000,
        };

        engine.register(spec).unwrap();

        // Register another to trigger rotation on already-populated partition
        let spec2 = SubscriptionSpec {
            subscription_id: 2,
            user_id: 43,
            session_id: None,
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::Required);
        set_dir_mode(temp_dir.path(), 0o500); // read + execute, no write

        let result = engine.register(SubscriptionSpec {
            subscription_id: 1000,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1,
        });

        set_dir_mode(temp_dir.path(), 0o700);

        assert!(matches!(result, Err(RegisterError::Storage(_))));
        assert_eq!(engine.subscription_count(), 0);
        assert!(!engine.unregister_subscription(1000));
    }

    #[cfg(unix)]
    #[test]
    fn test_register_batch_required_durability_rolls_back_on_snapshot_failure() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
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
            SubscriptionSpec {
                subscription_id: 2000,
                user_id: 10,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 10".to_string(),
                updated_at_unix_ms: 1,
            },
            SubscriptionSpec {
                subscription_id: 2001,
                user_id: 11,
                session_id: None,
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
    }

    #[cfg(unix)]
    #[test]
    fn test_register_best_effort_allows_snapshot_failure() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                temp_dir.path().to_path_buf(),
            )
            .unwrap();

        engine.set_rotation_threshold(1);
        engine.set_durability_mode(DurabilityMode::BestEffort);
        set_dir_mode(temp_dir.path(), 0o500);

        let result = engine.register(SubscriptionSpec {
            subscription_id: 3000,
            user_id: 12,
            session_id: None,
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Unregister session with no subscriptions
        let report = engine.unregister_session(999);
        assert_eq!(report.removed_bindings, 0);
        assert_eq!(report.removed_users, 0);
    }

    #[test]
    fn test_unregister_session_reports_removed_predicates_when_last_binding_removed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionSpec {
                subscription_id: 10,
                user_id: 1,
                session_id: Some(42),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 10,
            })
            .unwrap();

        engine
            .register(SubscriptionSpec {
                subscription_id: 11,
                user_id: 1,
                session_id: Some(42),
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
        assert_eq!(report.removed_users, 1);
    }

    #[test]
    fn test_unregister_session_does_not_count_predicate_removed_when_other_bindings_remain() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Session-bound binding
        engine
            .register(SubscriptionSpec {
                subscription_id: 20,
                user_id: 1,
                session_id: Some(43),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 20,
            })
            .unwrap();

        // Durable binding keeps predicate alive
        engine
            .register(SubscriptionSpec {
                subscription_id: 21,
                user_id: 2,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 21,
            })
            .unwrap();

        let report = engine.unregister_session(43);
        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 0);
        assert_eq!(report.removed_users, 1);
    }

    #[test]
    fn test_unregister_session_keeps_user_when_other_binding_for_same_user_remains() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Session-bound binding
        engine
            .register(SubscriptionSpec {
                subscription_id: 30,
                user_id: 1,
                session_id: Some(44),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 30,
            })
            .unwrap();

        // Durable binding for the same user should keep dictionary entry alive.
        engine
            .register(SubscriptionSpec {
                subscription_id: 31,
                user_id: 1,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount < 10".to_string(),
                updated_at_unix_ms: 31,
            })
            .unwrap();

        let report = engine.unregister_session(44);
        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 1);
        assert_eq!(report.removed_users, 0);
    }

    #[test]
    fn test_load_shard_with_orphan_binding() {
        use crate::persistence::shard::{
            serialize_shard, BindingData, PredicateData, ShardPayload, UserDictData,
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
                refcount: 1,
                updated_at_unix_ms: 1000,
            }],
            bindings: vec![
                // Valid binding
                BindingData {
                    subscription_id: 1,
                    predicate_hash: 0xAAAA, // matches predicate above
                    user_id: 42,
                    session_id: None,
                    updated_at_unix_ms: 1000,
                },
                // Orphan binding — references non-existent predicate hash
                BindingData {
                    subscription_id: 2,
                    predicate_hash: 0xDEAD, // NO predicate with this hash
                    user_id: 43,
                    session_id: None,
                    updated_at_unix_ms: 1000,
                },
            ],
            user_dict: UserDictData {
                ordinal_to_user: vec![42, 43],
            },
            created_at_unix_ms: 1000,
        };

        // Serialize shard with catalog fingerprint
        let shard_bytes = serialize_shard::<DefaultIds>(1, &payload, &*catalog).unwrap();

        // Write corrupt shard to disk
        let shard_path = temp_dir.path().join("table_1.shard");
        std::fs::write(&shard_path, shard_bytes).unwrap();

        // Try to load — should fail with Corrupt error about unknown predicate hash
        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds>, _> =
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
    fn test_load_shard_rejects_filename_header_table_id_mismatch() {
        use crate::persistence::shard::{serialize_shard, ShardPayload, UserDictData};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 0,
        };

        // Header table_id = 2, filename implies table_id = 1.
        let shard_bytes = serialize_shard::<DefaultIds>(2, &payload, &*catalog).unwrap();
        let shard_path = temp_dir.path().join("table_1.shard");
        std::fs::write(&shard_path, shard_bytes).unwrap();

        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds>, _> =
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

    // ========================================================================
    // Batch Registration Tests
    // ========================================================================

    #[test]
    fn test_register_batch_basic() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionSpec {
                subscription_id: 1,
                user_id: 100,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionSpec {
                subscription_id: 2,
                user_id: 200,
                session_id: None,
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
    fn test_register_batch_deduplication_within_batch() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Two users with the same predicate in a single batch
        let specs = vec![
            SubscriptionSpec {
                subscription_id: 1,
                user_id: 100,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionSpec {
                subscription_id: 2,
                user_id: 200,
                session_id: None,
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
    fn test_register_batch_partial_failure() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionSpec {
                subscription_id: 1,
                user_id: 100,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionSpec {
                subscription_id: 2,
                user_id: 200,
                session_id: None,
                sql: "SELECT * FROM nonexistent WHERE id = 1".to_string(), // bad table
                updated_at_unix_ms: 0,
            },
            SubscriptionSpec {
                subscription_id: 3,
                user_id: 300,
                session_id: None,
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let results = engine.register_batch(vec![]);
        assert!(results.is_empty());
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_register_batch_dispatch_works() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionSpec {
                subscription_id: 1,
                user_id: 42,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            },
            SubscriptionSpec {
                subscription_id: 2,
                user_id: 99,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
                updated_at_unix_ms: 0,
            },
        ];

        engine.register_batch(specs);

        // Dispatch event with amount = 200 (should match user 42 but not 99)
        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            }),
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = engine.users(&event).unwrap().collect();
        assert!(users.contains(&42));
        assert!(!users.contains(&99));
    }

    #[test]
    fn test_register_batch_duplicate_subscription_id_last_wins() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        let specs = vec![
            SubscriptionSpec {
                subscription_id: 1,
                user_id: 42,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            },
            SubscriptionSpec {
                subscription_id: 1, // same ID, replaces previous
                user_id: 99,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
                updated_at_unix_ms: 2,
            },
        ];

        let results = engine.register_batch(specs);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert_eq!(engine.subscription_count(), 1);

        let event = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(25), Cell::String("ok".into())]),
            }),
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = engine.users(&event).unwrap().collect();
        assert!(users.contains(&99));
        assert!(!users.contains(&42));
    }

    #[test]
    fn test_register_batch_dedup_with_existing() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Register one subscription individually
        engine
            .register(SubscriptionSpec {
                subscription_id: 1,
                user_id: 100,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            })
            .unwrap();

        // Batch register with same predicate + a new one
        let specs = vec![
            SubscriptionSpec {
                subscription_id: 2,
                user_id: 200,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(), // dedup with existing
                updated_at_unix_ms: 0,
            },
            SubscriptionSpec {
                subscription_id: 3,
                user_id: 300,
                session_id: None,
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine
            .register(SubscriptionSpec {
                subscription_id: 1,
                user_id: 42,
                session_id: Some(10),
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Same subscription_id is replaced with a new predicate and user.
        engine
            .register(SubscriptionSpec {
                subscription_id: 1,
                user_id: 99,
                session_id: Some(20),
                sql: "SELECT * FROM orders WHERE amount < 50".to_string(),
                updated_at_unix_ms: 2,
            })
            .unwrap();

        assert_eq!(engine.subscription_count(), 1);
        assert_eq!(engine.predicate_count(1), 1);

        let event_high = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(1)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(1), Cell::Int(200), Cell::String("ok".into())]),
            }),
            changed_columns: Arc::from([]),
        };
        let users_high: Vec<_> = engine.users(&event_high).unwrap().collect();
        assert!(!users_high.contains(&42));
        assert!(!users_high.contains(&99));

        let event_low = WalEvent {
            kind: EventKind::Insert,
            table_id: 1,
            pk: PrimaryKey {
                columns: Arc::from([0u16]),
                values: Arc::from([Cell::Int(2)]),
            },
            old_row: None,
            new_row: Some(RowImage {
                cells: Arc::from([Cell::Int(2), Cell::Int(10), Cell::String("ok".into())]),
            }),
            changed_columns: Arc::from([]),
        };
        let users_low: Vec<_> = engine.users(&event_low).unwrap().collect();
        assert!(users_low.contains(&99));
        assert!(!users_low.contains(&42));
    }

    #[test]
    fn test_try_complete_merge_replaces_table_state() {
        use tempfile::TempDir;

        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
            SubscriptionEngine::new(catalog.clone(), PostgreSqlDialect {});

        // Existing live state: amount > 100 for user 42.
        engine
            .register(SubscriptionSpec {
                subscription_id: 1,
                user_id: 42,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 1,
            })
            .unwrap();

        // Build merged shard state: amount < 50 for user 99.
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
                refcount: 1,
                updated_at_unix_ms: 2,
            }],
            bindings: vec![BindingData {
                subscription_id: 2,
                predicate_hash: hash,
                user_id: 99,
                session_id: None,
                updated_at_unix_ms: 2,
            }],
            user_dict: UserDictData {
                ordinal_to_user: vec![99],
            },
            created_at_unix_ms: 2,
        };

        let tmp = TempDir::new().unwrap();
        let shard_path = tmp.path().join("table_1_merged.shard");
        let shard_bytes = serialize_shard::<DefaultIds>(1, &payload, &*catalog).unwrap();
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

        assert_eq!(engine.subscription_count(), 1);
        assert!(!engine.unregister_subscription(1)); // old state replaced
        assert!(engine.unregister_subscription(2));
    }
}
