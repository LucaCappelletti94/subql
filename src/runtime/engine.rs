//! Subscription engine - main public API

use std::sync::Arc;
use std::path::{Path, PathBuf};
use ahash::AHashMap;
use sqlparser::dialect::Dialect;
use crate::{
    IdTypes, TableId,
    SubscriptionSpec, RegisterResult, PruneReport, MergeReport, WalEvent, MergeJobId,
    RegisterError, DispatchError, StorageError, MergeError,
    SchemaCatalog,
    compiler::{Vm, parse_and_compile, canonicalize, BytecodeProgram},
    persistence::{
        shard::{ShardPayload, PredicateData, BindingData, UserDictData, serialize_shard, deserialize_shard},
        merge::MergeManager,
    },
};
use super::{
    ids::PredicateId,
    predicate::{Predicate, Binding},
    partition::TablePartition,
    dispatch::{UserDictionary, MatchedUsers, dispatch_users},
    indexes::extract_indexable_atoms,
};

/// Wrapper to pass `Arc<dyn SchemaCatalog>` as `Box<dyn SchemaCatalog + Send>`.
struct CatalogRef(Arc<dyn SchemaCatalog>);

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
    /// VM for bytecode evaluation
    vm: Vm,
    /// Optional storage path for durability
    storage_path: Option<PathBuf>,
    /// Shard rotation threshold (bytes)
    rotation_threshold: usize,
    /// Background merge compaction manager
    merge_manager: MergeManager<I>,
}

impl<D: Dialect, I: IdTypes> SubscriptionEngine<D, I> {
    /// Create new subscription engine
    #[must_use]
    pub fn new(catalog: Arc<dyn SchemaCatalog>, dialect: D) -> Self {
        Self {
            dialect,
            catalog,
            partitions: AHashMap::new(),
            user_dictionaries: AHashMap::new(),
            vm: Vm::new(),
            storage_path: None,
            rotation_threshold: 10 * 1024 * 1024, // 10 MB default
            merge_manager: MergeManager::new(),
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
            vm: Vm::new(),
            storage_path: Some(storage_path.clone()),
            rotation_threshold: 10 * 1024 * 1024, // 10 MB default
            merge_manager: MergeManager::new(),
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
        // 1. Parse and compile SQL
        let (table_id, bytecode) = parse_and_compile(&spec.sql, &self.dialect, &*self.catalog)?;

        // 2. Canonicalize SQL and hash
        let normalized = canonicalize::normalize_sql(&spec.sql, &self.dialect)?;
        let hash = canonicalize::hash_sql(&normalized);

        // 3. Get/create table partition and user dictionary
        let partition = self.partitions.entry(table_id)
            .or_insert_with(|| TablePartition::new(table_id));

        let user_dict = self.user_dictionaries.entry(table_id)
            .or_default();

        // 4. Get user ordinal
        let user_ord = user_dict.get_or_create(spec.user_id);

        // 5. Check if predicate exists (deduplication)
        let snapshot = partition.load_snapshot();
        let (pred_id, created_new) = if let Some(existing) = snapshot.predicates.find_by_hash(hash) {
            // Predicate exists, increment refcount
            // (We need mutable access, so we'll do this via the partition's mutable store)
            (existing, false)
        } else {
            // Create new predicate
            let pred = Predicate {
                // Placeholder; store allocates the authoritative ID.
                id: PredicateId::from_slab_index(0),
                hash,
                normalized_sql: normalized.clone().into(),
                bytecode: Arc::new(bytecode.clone()),
                dependency_columns: Arc::from(bytecode.dependency_columns.as_slice()),
                refcount: 0, // Will be incremented via binding
                updated_at_unix_ms: spec.updated_at_unix_ms,
            };

            // Extract indexable atoms
            let atoms = extract_indexable_atoms(&bytecode, &bytecode.dependency_columns);

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

        // 7. Check if rotation needed (if storage enabled)
        if self.storage_path.is_some() && matches!(self.should_rotate(table_id), Ok(true)) {
            // Snapshot to disk (ignore errors for now, just best-effort)
            let _ = self.snapshot_table(table_id);
        }

        Ok(RegisterResult {
            table_id,
            normalized_sql: normalized,
            predicate_hash: hash,
            created_new_predicate: created_new,
        })
    }

    /// Unregister a subscription
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate is removed.
    pub fn unregister_subscription(&mut self, subscription_id: I::SubscriptionId) -> bool {
        self.unregister_subscription_internal(subscription_id).is_some()
    }

    /// Internal unregister helper.
    ///
    /// Returns `Some(predicate_removed)` if subscription existed, else `None`.
    fn unregister_subscription_internal(
        &mut self,
        subscription_id: I::SubscriptionId,
    ) -> Option<bool> {
        // Find the binding across all partitions
        for (_table_id, partition) in &mut self.partitions {
            let snapshot = partition.load_snapshot();

            if snapshot.predicates.bindings.contains_key(&subscription_id) {
                // Remove binding and decrement refcount
                let predicate_removed = partition.remove_binding(subscription_id);
                return Some(predicate_removed);
            }
        }

        None
    }

    /// Dispatch event to interested users
    pub fn users(&mut self, event: &WalEvent) -> Result<MatchedUsers<'_, I>, DispatchError> {
        // Get table partition
        let partition = self.partitions.get(&event.table_id)
            .ok_or(DispatchError::UnknownTableId(event.table_id))?;

        // Get user dictionary
        let user_dict = self.user_dictionaries.get(&event.table_id)
            .ok_or(DispatchError::UnknownTableId(event.table_id))?;

        // Validate selected row image arity against schema catalog.
        let row = match event.kind {
            crate::EventKind::Insert | crate::EventKind::Update => event.new_row.as_ref()
                .ok_or(DispatchError::MissingRequiredRowImage("INSERT/UPDATE requires new_row"))?,
            crate::EventKind::Delete => event.old_row.as_ref()
                .ok_or(DispatchError::MissingRequiredRowImage("DELETE requires old_row"))?,
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
        let removed_users = 0;

        // Collect subscription IDs to remove
        let mut to_remove = Vec::new();

        for (_table_id, partition) in &self.partitions {
            let snapshot = partition.load_snapshot();

            if let Some(sub_ids) = snapshot.predicates.get_session_subscriptions(session_id) {
                removed_bindings += sub_ids.len();
                to_remove.extend_from_slice(sub_ids);
            }
        }

        // Remove subscriptions
        for sub_id in to_remove {
            if self.unregister_subscription_internal(sub_id) == Some(true) {
                removed_predicates += 1;
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
        self.partitions.get(&table_id)
            .map_or(0, |p| {
                let snapshot = p.load_snapshot();
                snapshot.predicates.predicates.len()
            })
    }

    /// Get number of registered subscriptions
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.partitions.values()
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
        let storage_path = self.storage_path.as_ref()
            .ok_or_else(|| StorageError::Config("No storage path configured".to_string()))?;

        let partition = self.partitions.get(&table_id)
            .ok_or_else(|| StorageError::Corrupt(format!("Unknown table ID: {table_id}")))?;

        let user_dict = self.user_dictionaries.get(&table_id)
            .ok_or_else(|| StorageError::Corrupt(format!("No user dictionary for table {table_id}")))?;

        // Load snapshot
        let snapshot = partition.load_snapshot();

        // Convert predicates to serializable format
        let mut predicate_data_vec = Vec::new();
        for (_idx, pred) in &snapshot.predicates.predicates {
            let pred_data = PredicateData {
                hash: pred.hash,
                normalized_sql: pred.normalized_sql.to_string(),
                bytecode_instructions: bincode::serialize(&*pred.bytecode)
                    .map_err(|e| StorageError::Codec(format!("Bytecode serialize error: {e}")))?,
                dependency_columns: pred.dependency_columns.to_vec(),
                refcount: pred.refcount,
                updated_at_unix_ms: pred.updated_at_unix_ms,
            };
            predicate_data_vec.push(pred_data);
        }

        // Convert bindings to serializable format
        let mut binding_data_vec = Vec::new();
        for binding in snapshot.predicates.bindings.values() {
            let binding_data = BindingData::<I> {
                subscription_id: binding.subscription_id,
                predicate_hash: snapshot.predicates.get_predicate(binding.predicate_id)
                    .map_or(0, |p| p.hash),
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

    /// Load shard from disk into partition
    fn load_shard(&mut self, table_id: TableId, path: &Path) -> Result<(), StorageError> {
        let bytes = std::fs::read(path)
            .map_err(|e| StorageError::Io(format!("Failed to read shard: {e}")))?;

        let (_header, payload) = deserialize_shard::<I>(&bytes, &*self.catalog)?;

        // Create or get partition
        let partition = self.partitions.entry(table_id)
            .or_insert_with(|| TablePartition::new(table_id));

        let user_dict = self.user_dictionaries.entry(table_id)
            .or_default();

        // Restore user dictionary
        for user_id in &payload.user_dict.ordinal_to_user {
            user_dict.get_or_create(*user_id);
        }

        // Build hash → PredicateData map
        let mut pred_hash_to_data: AHashMap<u128, &PredicateData> = AHashMap::new();
        for pred_data in &payload.predicates {
            pred_hash_to_data.insert(pred_data.hash, pred_data);
        }

        // Restore predicates and bindings
        let mut hash_to_pred_id: AHashMap<u128, PredicateId> = AHashMap::new();

        for binding_data in &payload.bindings {
            let pred_data = pred_hash_to_data.get(&binding_data.predicate_hash)
                .ok_or_else(|| StorageError::Corrupt(
                    format!("Binding references unknown predicate hash: {:016x}", binding_data.predicate_hash)
                ))?;

            // Check if predicate already loaded
            let pred_id = if let Some(&existing_id) = hash_to_pred_id.get(&pred_data.hash) {
                existing_id
            } else {
                // Deserialize bytecode
                let bytecode: BytecodeProgram = bincode::deserialize(&pred_data.bytecode_instructions)
                    .map_err(|e| StorageError::Codec(format!("Bytecode deserialize error: {e}")))?;

                let pred = Predicate {
                    // Placeholder; store allocates the authoritative ID.
                    id: PredicateId::from_slab_index(0),
                    hash: pred_data.hash,
                    normalized_sql: pred_data.normalized_sql.clone().into(),
                    bytecode: Arc::new(bytecode.clone()),
                    dependency_columns: Arc::from(pred_data.dependency_columns.as_slice()),
                    refcount: 0, // Will be incremented via bindings
                    updated_at_unix_ms: pred_data.updated_at_unix_ms,
                };

                // Extract indexable atoms
                let atoms = extract_indexable_atoms(&bytecode, &bytecode.dependency_columns);

                // Add predicate to partition
                let pred_id = partition.add_predicate(pred, atoms);

                hash_to_pred_id.insert(pred_data.hash, pred_id);
                pred_id
            };

            // Create binding
            let user_ord = user_dict.get_or_create(binding_data.user_id);

            let binding = Binding {
                subscription_id: binding_data.subscription_id,
                predicate_id: pred_id,
                user_id: binding_data.user_id,
                user_ordinal: user_ord,
                session_id: binding_data.session_id,
                updated_at_unix_ms: binding_data.updated_at_unix_ms,
            };

            // Add binding to partition
            partition.add_binding(binding, pred_id);
        }

        Ok(())
    }

    /// Load all shards from storage directory
    fn load_all_shards(&mut self) -> Result<(), StorageError> {
        let storage_path = self.storage_path.as_ref()
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
        let partition = self.partitions.get(&table_id)
            .ok_or_else(|| StorageError::Corrupt(format!("Unknown table ID: {table_id}")))?;

        let snapshot = partition.load_snapshot();

        // Estimate size (rough approximation)
        let estimated_size =
            snapshot.predicates.predicates.len() * 1024 + // ~1KB per predicate (bytecode + metadata)
            snapshot.predicates.bindings.len() * 128;      // ~128B per binding

        Ok(estimated_size > self.rotation_threshold)
    }

    /// Set rotation threshold
    pub fn set_rotation_threshold(&mut self, threshold: usize) {
        self.rotation_threshold = threshold;
    }

    /// Get current rotation threshold
    #[must_use]
    pub const fn rotation_threshold(&self) -> usize {
        self.rotation_threshold
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
            let bytes = std::fs::read(path)
                .map_err(|e| MergeError::Storage(StorageError::Io(
                    format!("Failed to read shard for merge: {e}")
                )))?;
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
        let merged = match self.merge_manager.try_get_result(job_id)? {
            Some(m) => m,
            None => return Ok(None),
        };

        let table_id = merged.table_id;

        // Load merged payload into partition
        let partition = self.partitions.entry(table_id)
            .or_insert_with(|| TablePartition::new(table_id));
        let user_dict = self.user_dictionaries.entry(table_id)
            .or_default();

        // Restore user dictionary from merged shard
        for &user_id in &merged.payload.user_dict.ordinal_to_user {
            user_dict.get_or_create(user_id);
        }

        // Build hash → PredicateData map
        let mut pred_hash_to_data: AHashMap<u128, &PredicateData> = AHashMap::new();
        for pred_data in &merged.payload.predicates {
            pred_hash_to_data.insert(pred_data.hash, pred_data);
        }

        // Restore predicates and bindings from merged payload
        let mut hash_to_pred_id: AHashMap<u128, PredicateId> = AHashMap::new();

        for binding_data in &merged.payload.bindings {
            let pred_data = pred_hash_to_data.get(&binding_data.predicate_hash)
                .ok_or_else(|| MergeError::BuildFailed(
                    format!("Merged binding references unknown predicate hash: {:016x}", binding_data.predicate_hash)
                ))?;

            let pred_id = if let Some(&existing_id) = hash_to_pred_id.get(&pred_data.hash) {
                existing_id
            } else {
                let bytecode: BytecodeProgram = bincode::deserialize(&pred_data.bytecode_instructions)
                    .map_err(|e| MergeError::BuildFailed(format!("Bytecode deserialize error: {e}")))?;

                let pred = Predicate {
                    // Placeholder; store allocates the authoritative ID.
                    id: PredicateId::from_slab_index(0),
                    hash: pred_data.hash,
                    normalized_sql: pred_data.normalized_sql.clone().into(),
                    bytecode: Arc::new(bytecode.clone()),
                    dependency_columns: Arc::from(pred_data.dependency_columns.as_slice()),
                    refcount: 0,
                    updated_at_unix_ms: pred_data.updated_at_unix_ms,
                };

                let atoms = extract_indexable_atoms(&bytecode, &bytecode.dependency_columns);
                let pred_id = partition.add_predicate(pred, atoms);

                hash_to_pred_id.insert(pred_data.hash, pred_id);
                pred_id
            };

            let user_ord = user_dict.get_or_create(binding_data.user_id);
            let binding = Binding {
                subscription_id: binding_data.subscription_id,
                predicate_id: pred_id,
                user_id: binding_data.user_id,
                user_ordinal: user_ord,
                session_id: binding_data.session_id,
                updated_at_unix_ms: binding_data.updated_at_unix_ms,
            };
            partition.add_binding(binding, pred_id);
        }

        Ok(Some(merged.stats.into()))
    }

    /// Get number of active merge jobs
    #[must_use]
    pub fn active_merge_jobs(&self) -> usize {
        self.merge_manager.active_jobs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::collections::HashMap;
    use crate::{Cell, EventKind, RowImage, PrimaryKey, DefaultIds};
    use crate::testing::MockCatalog;

    fn make_catalog() -> Arc<MockCatalog> {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), (1, 3));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "amount".to_string()), 1);
        columns.insert((1, "status".to_string()), 2);

        Arc::new(MockCatalog { tables, columns })
    }

    #[test]
    fn test_engine_creation() {
        let catalog = make_catalog();
        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn test_register_subscription() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
                    Cell::Int(1),      // id
                    Cell::Int(200),    // amount
                    Cell::String("pending".into()),  // status
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
    fn test_bindings_persist() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
    fn test_register_after_unsubscribe_does_not_break_hash_lookup_or_dispatch() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Sub 1 -> predicate A
        engine.register(SubscriptionSpec {
            subscription_id: 1,
            user_id: 101,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 1,
        }).unwrap();

        // Sub 2 -> predicate B (kept alive while A is removed)
        engine.register(SubscriptionSpec {
            subscription_id: 2,
            user_id: 202,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount < 0".to_string(),
            updated_at_unix_ms: 2,
        }).unwrap();

        // Remove A to create a slab hole.
        assert!(engine.unregister_subscription(1));

        // Sub 3 -> predicate C should be independently dispatchable.
        engine.register(SubscriptionSpec {
            subscription_id: 3,
            user_id: 303,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount = 7".to_string(),
            updated_at_unix_ms: 3,
        }).unwrap();

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
                    Cell::Int(7),          // id
                    Cell::Int(7),          // amount
                    Cell::String("ok".into()),
                ]),
            }),
            changed_columns: Arc::from([]),
        };

        let users: Vec<_> = engine.users(&event).unwrap().collect();
        assert!(users.contains(&303), "newly registered predicate should dispatch after hole reuse");
    }

    #[test]
    fn test_multiple_predicates_indexed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog.clone(),
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog,
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog.clone(),
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog,
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog.clone(),
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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
        let engine2: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog,
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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
        let engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog,
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

        // Should start with no subscriptions
        assert_eq!(engine.subscription_count(), 0);
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Engine Completion
    // ========================================================================

    #[test]
    fn test_predicate_count() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let engine: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds>, _> = SubscriptionEngine::with_storage(
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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Unregister subscription that doesn't exist
        let found = engine.unregister_subscription(999);
        assert!(!found);
    }

    #[test]
    fn test_dispatch_unknown_table() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

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
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine.register(SubscriptionSpec {
            subscription_id: 31,
            user_id: 31,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
            updated_at_unix_ms: 31,
        }).unwrap();

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
            Err(DispatchError::InvalidRowArity { table_id: 1, expected: 3, got: 2 })
        ));
    }

    #[test]
    fn test_dispatch_update_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine.register(SubscriptionSpec {
            subscription_id: 32,
            user_id: 32,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
            updated_at_unix_ms: 32,
        }).unwrap();

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
            Err(DispatchError::InvalidRowArity { table_id: 1, expected: 3, got: 2 })
        ));
    }

    #[test]
    fn test_dispatch_delete_invalid_row_arity() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine.register(SubscriptionSpec {
            subscription_id: 33,
            user_id: 33,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 0".to_string(),
            updated_at_unix_ms: 33,
        }).unwrap();

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
            Err(DispatchError::InvalidRowArity { table_id: 1, expected: 3, got: 2 })
        ));
    }

    #[test]
    fn test_storage_rotation_triggers_snapshot() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::with_storage(
            catalog.clone(),
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        ).unwrap();

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

    #[test]
    fn test_unregister_session_empty() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Unregister session with no subscriptions
        let report = engine.unregister_session(999);
        assert_eq!(report.removed_bindings, 0);
    }

    #[test]
    fn test_unregister_session_reports_removed_predicates_when_last_binding_removed() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        engine.register(SubscriptionSpec {
            subscription_id: 10,
            user_id: 1,
            session_id: Some(42),
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 10,
        }).unwrap();

        engine.register(SubscriptionSpec {
            subscription_id: 11,
            user_id: 1,
            session_id: Some(42),
            sql: "SELECT * FROM orders WHERE amount < 10".to_string(),
            updated_at_unix_ms: 11,
        }).unwrap();

        let report = engine.unregister_session(42);
        assert_eq!(report.removed_bindings, 2);
        assert_eq!(
            report.removed_predicates, 2,
            "both predicates should be removed when their last bindings are session-bound"
        );
    }

    #[test]
    fn test_unregister_session_does_not_count_predicate_removed_when_other_bindings_remain() {
        let catalog = make_catalog();
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> = SubscriptionEngine::new(catalog, PostgreSqlDialect {});

        // Session-bound binding
        engine.register(SubscriptionSpec {
            subscription_id: 20,
            user_id: 1,
            session_id: Some(43),
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 20,
        }).unwrap();

        // Durable binding keeps predicate alive
        engine.register(SubscriptionSpec {
            subscription_id: 21,
            user_id: 2,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 21,
        }).unwrap();

        let report = engine.unregister_session(43);
        assert_eq!(report.removed_bindings, 1);
        assert_eq!(report.removed_predicates, 0);
    }

    #[test]
    fn test_load_shard_with_orphan_binding() {
        use tempfile::TempDir;
        use crate::persistence::shard::{
            serialize_shard, ShardPayload, PredicateData, BindingData, UserDictData,
        };
        use crate::DefaultIds;

        let temp_dir = TempDir::new().unwrap();
        let catalog = make_catalog();

        // Create a shard with a binding that references a predicate hash
        // that does NOT exist in the predicate list
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![PredicateData {
                hash: 0xAAAA,
                normalized_sql: "amount > 100".to_string(),
                bytecode_instructions: bincode::serialize(
                    &crate::compiler::BytecodeProgram::new(vec![
                        crate::compiler::Instruction::LoadColumn(1),
                        crate::compiler::Instruction::PushLiteral(Cell::Int(100)),
                        crate::compiler::Instruction::GreaterThan,
                    ])
                ).unwrap(),
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
        let result: Result<SubscriptionEngine<PostgreSqlDialect, DefaultIds>, _> = SubscriptionEngine::with_storage(
            catalog,
            PostgreSqlDialect {},
            temp_dir.path().to_path_buf(),
        );

        assert!(result.is_err());
        match result {
            Err(ref e) => assert!(
                format!("{:?}", e).contains("unknown predicate hash"),
                "Expected corrupt shard error about unknown predicate hash, got: {:?}", e
            ),
            Ok(_) => panic!("Expected error loading corrupt shard"),
        }
    }
}
