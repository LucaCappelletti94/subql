//! Background merge operations with atomic swap

use super::predicate_data::predicate_data_equivalent;
use super::shard::{deserialize_shard, BindingData, PredicateData, ShardPayload, UserDictData};
use crate::{DefaultIds, IdTypes, MergeError, MergeJobId, MergeReport, SchemaCatalog, TableId};
use ahash::AHashMap;
use std::sync::{
    mpsc::{self, Receiver, Sender, TryRecvError},
    Arc, Mutex,
};
use std::thread;

/// Fixed worker count for background merge tasks.
const MERGE_WORKER_COUNT: usize = 4;

/// Merged shard ready for swap
#[derive(Debug)]
pub struct MergedShard<I: IdTypes> {
    pub table_id: TableId,
    pub payload: ShardPayload<I>,
    pub stats: MergeStats,
}

/// Merge statistics
#[derive(Debug, Clone)]
pub struct MergeStats {
    pub input_shards: usize,
    pub input_predicates: usize,
    pub output_predicates: usize,
    pub input_bindings: usize,
    pub output_bindings: usize,
    pub build_ms: u64,
}

/// Background merge job
struct MergeJob<I: IdTypes> {
    receiver: Receiver<Result<MergedShard<I>, String>>,
}

struct MergeTask<I: IdTypes> {
    table_id: TableId,
    shard_bytes: Vec<Vec<u8>>,
    catalog: Box<dyn SchemaCatalog + Send>,
    result_sender: Sender<Result<MergedShard<I>, String>>,
}

/// Manager for background merge operations
pub struct MergeManager<I: IdTypes = DefaultIds> {
    jobs: AHashMap<MergeJobId, MergeJob<I>>,
    next_job_id: MergeJobId,
    task_sender: Sender<MergeTask<I>>,
}

impl<I: IdTypes> MergeManager<I> {
    fn recv_task(receiver: &Arc<Mutex<Receiver<MergeTask<I>>>>) -> Option<MergeTask<I>> {
        let lock = receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lock.recv().ok()
    }

    /// Create new merge manager
    #[must_use]
    pub fn new() -> Self {
        let (task_sender, task_receiver) = mpsc::channel::<MergeTask<I>>();
        let shared_receiver = Arc::new(Mutex::new(task_receiver));

        for _ in 0..MERGE_WORKER_COUNT {
            let receiver = Arc::clone(&shared_receiver);
            thread::spawn(move || {
                loop {
                    let Some(task) = Self::recv_task(&receiver) else {
                        break;
                    };

                    let start = std::time::Instant::now();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        merge_shards_impl::<I>(
                            task.table_id,
                            &task.shard_bytes,
                            &*task.catalog,
                            start,
                        )
                    }))
                    .unwrap_or_else(|_| {
                        Err("Merge worker panicked while processing task".to_string())
                    });

                    // Receiver may have been dropped if caller discarded the job.
                    if task.result_sender.send(result).is_err() {
                        #[cfg(feature = "observability")]
                        tracing::warn!("Merge result receiver dropped before result was sent");
                    }
                }
            });
        }

        Self {
            jobs: AHashMap::new(),
            next_job_id: 1,
            task_sender,
        }
    }

    /// Start background merge of shards
    ///
    /// Returns job ID immediately. Merge runs in background thread.
    pub fn merge_shards_background(
        &mut self,
        table_id: TableId,
        shard_bytes: Vec<Vec<u8>>,
        catalog: Box<dyn SchemaCatalog + Send>,
    ) -> Result<MergeJobId, MergeError> {
        let job_id = self.next_job_id;
        self.next_job_id += 1;

        let (tx, rx) = mpsc::channel();

        let task = MergeTask {
            table_id,
            shard_bytes,
            catalog,
            result_sender: tx,
        };
        self.task_sender
            .send(task)
            .map_err(|_| MergeError::BuildFailed("Merge worker pool is unavailable".to_string()))?;

        self.jobs.insert(job_id, MergeJob { receiver: rx });

        Ok(job_id)
    }

    /// Check if merge is complete and return result
    ///
    /// Returns Some(result) if merge complete, None if still running.
    pub fn try_get_result(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergedShard<I>>, MergeError> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(MergeError::UnknownJob(job_id))?;

        match job.receiver.try_recv() {
            Ok(Ok(merged)) => {
                // Job complete, clean up
                self.jobs.remove(&job_id);
                Ok(Some(merged))
            }
            Ok(Err(e)) => {
                // Job failed
                self.jobs.remove(&job_id);
                Err(MergeError::BuildFailed(e))
            }
            Err(TryRecvError::Empty) => {
                // Still running
                Ok(None)
            }
            Err(TryRecvError::Disconnected) => {
                // Thread panicked
                self.jobs.remove(&job_id);
                Err(MergeError::BuildFailed("Thread panicked".to_string()))
            }
        }
    }

    /// Get number of active jobs
    #[must_use]
    pub fn active_jobs(&self) -> usize {
        self.jobs.len()
    }
}

impl<I: IdTypes> Default for MergeManager<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Perform merge operation
fn unix_ms_from(now: std::time::SystemTime) -> Result<u64, String> {
    let elapsed = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("System clock is before UNIX epoch: {e}"))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| format!("UNIX timestamp does not fit into u64 milliseconds: {elapsed:?}"))
}

/// Perform merge operation
fn merge_shards_impl<I: IdTypes>(
    table_id: TableId,
    shard_bytes: &[Vec<u8>],
    catalog: &dyn SchemaCatalog,
    start: std::time::Instant,
) -> Result<MergedShard<I>, String> {
    let mut all_predicates = Vec::new();
    let mut all_bindings = Vec::new();
    let mut user_ordinals: Vec<I::UserId> = Vec::new();

    // 1. Load all shards
    for bytes in shard_bytes {
        let (header, payload) = deserialize_shard(bytes, catalog)
            .map_err(|e| format!("Shard deserialize error: {e:?}"))?;

        if header.table_id != table_id {
            return Err(format!(
                "Shard table_id mismatch: expected {table_id}, got {}",
                header.table_id
            ));
        }

        all_predicates.extend(payload.predicates);
        all_bindings.extend(payload.bindings);
        user_ordinals.extend(payload.user_dict.ordinal_to_user);
    }

    // Deduplicate users via sort + dedup (O(n log n) instead of O(n^2))
    user_ordinals.sort_unstable();
    user_ordinals.dedup();

    let input_predicates = all_predicates.len();
    let input_bindings = all_bindings.len();

    // 2. Deduplicate predicates by hash (keep most recent)
    let mut unique_predicates: AHashMap<u128, PredicateData> = AHashMap::new();

    for pred in all_predicates {
        if let Some(existing) = unique_predicates.get_mut(&pred.hash) {
            if !predicate_data_equivalent(existing, &pred) {
                return Err(format!(
                    "predicate hash collision for hash {:016x} with non-equivalent payload",
                    pred.hash
                ));
            }

            // Keep most recent
            if pred.updated_at_unix_ms > existing.updated_at_unix_ms {
                *existing = pred;
            }
        } else {
            unique_predicates.insert(pred.hash, pred);
        }
    }

    // 3. Collect final predicates
    let output_predicates: Vec<PredicateData> = unique_predicates.into_values().collect();

    // 4. Filter bindings (remove duplicates)
    let mut unique_bindings: AHashMap<I::SubscriptionId, BindingData<I>> = AHashMap::new();

    for binding in all_bindings {
        unique_bindings
            .entry(binding.subscription_id)
            .and_modify(|existing| {
                if binding.updated_at_unix_ms > existing.updated_at_unix_ms {
                    *existing = binding;
                }
            })
            .or_insert(binding);
    }

    let output_bindings: Vec<_> = unique_bindings.into_values().collect();

    let created_at_unix_ms = unix_ms_from(std::time::SystemTime::now())?;

    // Capture lengths before moving into payload
    let num_output_predicates = output_predicates.len();
    let num_output_bindings = output_bindings.len();

    // 5. Build merged payload
    let payload: ShardPayload<I> = ShardPayload {
        predicates: output_predicates,
        bindings: output_bindings,
        user_dict: UserDictData {
            ordinal_to_user: user_ordinals,
        },
        created_at_unix_ms,
    };

    #[allow(clippy::cast_possible_truncation)] // build time won't exceed u64::MAX ms
    let build_ms = start.elapsed().as_millis() as u64;

    let stats = MergeStats {
        input_shards: shard_bytes.len(),
        input_predicates,
        output_predicates: num_output_predicates,
        input_bindings,
        output_bindings: num_output_bindings,
        build_ms,
    };

    Ok(MergedShard {
        table_id,
        payload,
        stats,
    })
}

/// Convert merge stats to report
impl From<MergeStats> for MergeReport {
    fn from(stats: MergeStats) -> Self {
        #[allow(clippy::cast_precision_loss)] // approximate ratio is fine for reporting
        let dedup_ratio = if stats.output_predicates > 0 {
            stats.input_predicates as f32 / stats.output_predicates as f32
        } else {
            1.0
        };

        Self {
            input_shards: stats.input_shards,
            output_predicates: stats.output_predicates,
            output_bindings: stats.output_bindings,
            dedup_ratio,
            build_ms: stats.build_ms,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::super::codec;
    use super::super::shard::{serialize_shard, PredicateData, UserDictData};
    use super::*;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    struct MockCatalog {
        fingerprints: HashMap<TableId, u64>,
    }

    impl SchemaCatalog for MockCatalog {
        fn table_id(&self, _table_name: &str) -> Option<TableId> {
            Some(1)
        }

        fn column_id(&self, _table_id: TableId, _column_name: &str) -> Option<u16> {
            Some(0)
        }

        fn table_arity(&self, _table_id: TableId) -> Option<usize> {
            Some(5)
        }

        fn schema_fingerprint(&self, table_id: TableId) -> Option<u64> {
            self.fingerprints.get(&table_id).copied()
        }
    }

    fn make_catalog() -> MockCatalog {
        let mut fingerprints = HashMap::new();
        fingerprints.insert(1, 0x1234_5678_90AB_CDEF);
        MockCatalog { fingerprints }
    }

    #[test]
    fn test_merge_deduplication() {
        let catalog = make_catalog();

        // Create two shards with duplicate predicate
        let pred = PredicateData {
            hash: 0x1234,
            normalized_sql: "age > 18".to_string(),
            bytecode_instructions: vec![],
            prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default()).unwrap(),
            dependency_columns: vec![1],
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let payload1: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![pred.clone()],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let payload2: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![pred],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 2000,
        };

        let shard1 = serialize_shard(1, &payload1, &catalog).unwrap();
        let shard2 = serialize_shard(1, &payload2, &catalog).unwrap();

        // Merge
        let start = std::time::Instant::now();
        let result = merge_shards_impl::<DefaultIds>(1, &[shard1, shard2], &catalog, start);

        assert!(result.is_ok());
        let merged = result.unwrap();

        // Should have deduplicated to 1 predicate
        assert_eq!(merged.payload.predicates.len(), 1);
        assert_eq!(merged.stats.input_predicates, 2);
        assert_eq!(merged.stats.output_predicates, 1);
    }

    #[test]
    fn test_merge_manager() {
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();

        let catalog = Box::new(make_catalog());
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![10, 20],
            },
            created_at_unix_ms: 1000,
        };

        let mock_catalog = make_catalog();
        let shard = serialize_shard(1, &payload, &mock_catalog).unwrap();

        let job_id = manager
            .merge_shards_background(1, vec![shard], catalog)
            .unwrap();

        // Wait a bit for merge to complete
        thread::sleep(std::time::Duration::from_millis(100));

        let result = manager.try_get_result(job_id).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_merge_stats_to_report() {
        let stats = MergeStats {
            input_shards: 3,
            input_predicates: 100,
            output_predicates: 50,
            input_bindings: 200,
            output_bindings: 180,
            build_ms: 250,
        };

        let report: MergeReport = stats.into();
        assert_eq!(report.input_shards, 3);
        assert_eq!(report.output_predicates, 50);
        assert!((report.dedup_ratio - 2.0).abs() < 0.01);
    }

    // ========================================================================
    // Phase 3: Push to 95% Coverage - Merge Completion
    // ========================================================================

    #[test]
    fn test_merge_manager_default() {
        let manager: MergeManager<DefaultIds> = MergeManager::default();
        assert_eq!(manager.active_jobs(), 0);
    }

    #[test]
    fn test_merge_manager_active_jobs() {
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();
        assert_eq!(manager.active_jobs(), 0);

        let catalog = Box::new(make_catalog());
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let mock_catalog = make_catalog();
        let shard = serialize_shard(1, &payload, &mock_catalog).unwrap();

        let _job_id = manager
            .merge_shards_background(1, vec![shard], catalog)
            .unwrap();

        // Should have 1 active job
        assert_eq!(manager.active_jobs(), 1);
    }

    #[test]
    fn test_merge_manager_unknown_job() {
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();

        // Try to get result for non-existent job
        let result = manager.try_get_result(999);
        assert!(matches!(result, Err(MergeError::UnknownJob(999))));
    }

    #[test]
    fn test_merge_manager_still_running() {
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();

        let catalog = Box::new(make_catalog());
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let mock_catalog = make_catalog();
        let shard = serialize_shard(1, &payload, &mock_catalog).unwrap();

        let job_id = manager
            .merge_shards_background(1, vec![shard], catalog)
            .unwrap();

        // Immediately check - might still be running
        let result = manager.try_get_result(job_id);
        // Could be None (still running) or Some (very fast)
        assert!(result.is_ok());
    }

    #[test]
    fn test_merge_predicate_keeps_most_recent() {
        let catalog = make_catalog();

        // Create two shards with same predicate but different timestamps
        let pred_old = PredicateData {
            hash: 0x1234,
            normalized_sql: "age > 18".to_string(),
            bytecode_instructions: vec![7, 8, 9],
            prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default()).unwrap(),
            dependency_columns: vec![1],
            refcount: 1,
            updated_at_unix_ms: 1000, // Older
        };

        let pred_new = PredicateData {
            hash: 0x1234, // Same hash
            normalized_sql: "age > 18".to_string(),
            bytecode_instructions: vec![7, 8, 9],
            prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default()).unwrap(),
            dependency_columns: vec![1],
            refcount: 2,
            updated_at_unix_ms: 2000, // Newer
        };

        let payload1: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![pred_old],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let payload2: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![pred_new],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 2000,
        };

        let shard1 = serialize_shard(1, &payload1, &catalog).unwrap();
        let shard2 = serialize_shard(1, &payload2, &catalog).unwrap();

        let start = std::time::Instant::now();
        let result = merge_shards_impl::<DefaultIds>(1, &[shard1, shard2], &catalog, start);

        assert!(result.is_ok());
        let merged = result.unwrap();

        // Should keep the newer one
        assert_eq!(merged.payload.predicates.len(), 1);
        assert_eq!(merged.payload.predicates[0].updated_at_unix_ms, 2000);
        assert_eq!(merged.payload.predicates[0].normalized_sql, "age > 18");
    }

    #[test]
    fn test_merge_rejects_non_equivalent_predicates_with_same_hash() {
        let catalog = make_catalog();

        let pred_a = PredicateData {
            hash: 0x2222,
            normalized_sql: "age > 18".to_string(),
            bytecode_instructions: vec![1],
            prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default()).unwrap(),
            dependency_columns: vec![1],
            refcount: 1,
            updated_at_unix_ms: 1000,
        };

        let pred_b = PredicateData {
            hash: 0x2222, // same hash, different predicate payload
            normalized_sql: "status = 'active'".to_string(),
            bytecode_instructions: vec![2],
            prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default()).unwrap(),
            dependency_columns: vec![2],
            refcount: 1,
            updated_at_unix_ms: 2000,
        };

        let payload1: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![pred_a],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let payload2: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![pred_b],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 2000,
        };

        let shard1 = serialize_shard(1, &payload1, &catalog).unwrap();
        let shard2 = serialize_shard(1, &payload2, &catalog).unwrap();

        let start = std::time::Instant::now();
        let result = merge_shards_impl::<DefaultIds>(1, &[shard1, shard2], &catalog, start);
        assert!(matches!(result, Err(msg) if msg.contains("hash collision")));
    }

    #[test]
    fn test_shared_predicate_equivalence_helper_detects_mismatch() {
        let left = PredicateData {
            hash: 0x0101,
            normalized_sql: "age > 18".to_string(),
            bytecode_instructions: vec![1, 2, 3],
            prefilter_plan: codec::serialize(&crate::compiler::PrefilterPlan::default()).unwrap(),
            dependency_columns: vec![1],
            refcount: 1,
            updated_at_unix_ms: 10,
        };
        let mut right = left.clone();
        right.bytecode_instructions = vec![9, 9, 9];

        assert!(!crate::persistence::predicate_data::predicate_data_equivalent(
            &left, &right
        ));
    }

    #[test]
    fn test_merge_bindings_keeps_most_recent() {
        let catalog = make_catalog();

        // Create two shards with same binding but different timestamps
        let binding_old: BindingData<DefaultIds> = BindingData {
            subscription_id: 100,
            predicate_hash: 0x1234,
            user_id: 42,
            session_id: Some(1000),
            updated_at_unix_ms: 1000, // Older
        };

        let binding_new: BindingData<DefaultIds> = BindingData {
            subscription_id: 100, // Same sub_id
            predicate_hash: 0x1234,
            user_id: 42,
            session_id: Some(2000),   // Different session
            updated_at_unix_ms: 2000, // Newer
        };

        let payload1: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![binding_old],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let payload2: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![binding_new],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 2000,
        };

        let shard1 = serialize_shard(1, &payload1, &catalog).unwrap();
        let shard2 = serialize_shard(1, &payload2, &catalog).unwrap();

        let start = std::time::Instant::now();
        let result = merge_shards_impl::<DefaultIds>(1, &[shard1, shard2], &catalog, start);

        assert!(result.is_ok());
        let merged = result.unwrap();

        // Should keep the newer one
        assert_eq!(merged.payload.bindings.len(), 1);
        assert_eq!(merged.payload.bindings[0].updated_at_unix_ms, 2000);
        assert_eq!(merged.payload.bindings[0].session_id, Some(2000));
    }

    #[test]
    fn test_merge_stats_zero_output_predicates() {
        let stats = MergeStats {
            input_shards: 2,
            input_predicates: 0,
            output_predicates: 0, // Zero output
            input_bindings: 0,
            output_bindings: 0,
            build_ms: 100,
        };

        let report: MergeReport = stats.into();
        assert_eq!(report.dedup_ratio, 1.0); // Should default to 1.0
    }

    #[test]
    fn test_merge_with_invalid_shard_bytes() {
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();

        let catalog = Box::new(make_catalog());
        // Invalid shard bytes - will fail to deserialize
        let invalid_shard = vec![0u8, 1, 2, 3, 4, 5];

        let job_id = manager
            .merge_shards_background(1, vec![invalid_shard], catalog)
            .unwrap();

        // Wait for merge to fail
        thread::sleep(std::time::Duration::from_millis(100));

        let result = manager.try_get_result(job_id);
        assert!(matches!(result, Err(MergeError::BuildFailed(_))));
    }

    #[test]
    fn test_merge_rejects_shard_with_wrong_table_id() {
        let mut fingerprints = HashMap::new();
        fingerprints.insert(1, 0x1234_5678_90AB_CDEF);
        fingerprints.insert(2, 0xAAAA_BBBB_CCCC_DDDD);
        let catalog = MockCatalog { fingerprints };

        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };

        let shard_wrong_table = serialize_shard(2, &payload, &catalog).unwrap();
        let start = std::time::Instant::now();
        let result = merge_shards_impl::<DefaultIds>(1, &[shard_wrong_table], &catalog, start);

        assert!(matches!(result, Err(msg) if msg.contains("table_id mismatch")));
    }

    #[test]
    fn test_merge_thread_disconnected() {
        // Simulate thread panic by manually creating a job with a disconnected channel
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();

        let (tx, rx) = mpsc::channel::<Result<MergedShard<DefaultIds>, String>>();
        // Drop the sender immediately — simulates thread panic
        drop(tx);

        let job_id = manager.next_job_id;
        manager.next_job_id += 1;
        manager.jobs.insert(job_id, MergeJob { receiver: rx });

        // Now try_get_result should see Disconnected
        let result = manager.try_get_result(job_id);
        assert!(
            matches!(result, Err(MergeError::BuildFailed(ref msg)) if msg == "Thread panicked")
        );
    }

    #[test]
    fn test_worker_panic_does_not_take_down_pool() {
        struct PanicCatalog;
        impl SchemaCatalog for PanicCatalog {
            fn table_id(&self, _table_name: &str) -> Option<TableId> {
                Some(1)
            }

            fn column_id(&self, _table_id: TableId, _column_name: &str) -> Option<u16> {
                Some(0)
            }

            fn table_arity(&self, _table_id: TableId) -> Option<usize> {
                Some(5)
            }

            fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
                panic!("intentional catalog panic");
            }
        }

        let mut manager: MergeManager<DefaultIds> = MergeManager::new();
        let catalog = make_catalog();
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };
        let shard = serialize_shard(1, &payload, &catalog).unwrap();

        for _ in 0..MERGE_WORKER_COUNT {
            manager
                .merge_shards_background(1, vec![shard.clone()], Box::new(PanicCatalog))
                .expect("panic task should be enqueued");
        }

        // Let panic tasks run.
        thread::sleep(Duration::from_millis(200));

        let healthy_job = manager
            .merge_shards_background(1, vec![shard], Box::new(make_catalog()))
            .expect("worker pool should remain available after task panic");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match manager.try_get_result(healthy_job) {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(20));
                }
                Ok(None) => panic!("healthy job did not complete before timeout"),
                Err(e) => panic!("healthy job should not fail after panic recovery: {e}"),
            }
        }
    }

    #[test]
    fn recv_task_recovers_from_poisoned_mutex() {
        let (task_tx, task_rx) = mpsc::channel::<MergeTask<DefaultIds>>();
        let receiver = Arc::new(Mutex::new(task_rx));

        let poison_target = Arc::clone(&receiver);
        let _ = thread::spawn(move || {
            let _guard = poison_target.lock().unwrap();
            panic!("poison receiver mutex");
        })
        .join();

        assert!(
            receiver.lock().is_err(),
            "receiver mutex should be poisoned"
        );

        let (result_sender, _result_receiver) =
            mpsc::channel::<Result<MergedShard<DefaultIds>, String>>();
        task_tx
            .send(MergeTask {
                table_id: 1,
                shard_bytes: Vec::new(),
                catalog: Box::new(make_catalog()),
                result_sender,
            })
            .unwrap();

        let received = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            MergeManager::<DefaultIds>::recv_task(&receiver)
        }));
        assert!(received.is_ok(), "poisoned mutex should not panic");
        assert!(received.unwrap().is_some(), "task should still be received");
    }

    #[test]
    fn unix_ms_from_rejects_pre_epoch_time() {
        let before_epoch = std::time::UNIX_EPOCH
            .checked_sub(Duration::from_millis(1))
            .expect("pre-epoch timestamp should be representable");
        let err = unix_ms_from(before_epoch).expect_err("pre-epoch time should be rejected");
        assert!(err.contains("before UNIX epoch"));
    }

    #[test]
    fn unix_ms_from_accepts_epoch() {
        let ts = unix_ms_from(std::time::UNIX_EPOCH).expect("epoch should convert");
        assert_eq!(ts, 0);
    }

    #[test]
    fn test_merge_still_running() {
        // Create a channel where sender is alive but hasn't sent yet
        let mut manager: MergeManager<DefaultIds> = MergeManager::new();

        let (tx, rx) = mpsc::channel::<Result<MergedShard<DefaultIds>, String>>();

        let job_id = manager.next_job_id;
        manager.next_job_id += 1;
        manager.jobs.insert(job_id, MergeJob { receiver: rx });

        // Before sending anything, check — should be "still running"
        let result = manager.try_get_result(job_id);
        assert!(matches!(result, Ok(None)));

        // Now send result and check again
        let _ = tx.send(Ok(MergedShard::<DefaultIds> {
            table_id: 1,
            payload: ShardPayload {
                predicates: vec![],
                bindings: vec![],
                user_dict: UserDictData {
                    ordinal_to_user: vec![],
                },
                created_at_unix_ms: 1000,
            },
            stats: MergeStats {
                input_shards: 1,
                input_predicates: 0,
                output_predicates: 0,
                input_bindings: 0,
                output_bindings: 0,
                build_ms: 0,
            },
        }));

        let result = manager.try_get_result(job_id);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_merge_uses_bounded_worker_threads() {
        struct SlowCatalog;
        impl SchemaCatalog for SlowCatalog {
            fn table_id(&self, _table_name: &str) -> Option<TableId> {
                Some(1)
            }

            fn column_id(&self, _table_id: TableId, _column_name: &str) -> Option<u16> {
                Some(0)
            }

            fn table_arity(&self, _table_id: TableId) -> Option<usize> {
                Some(1)
            }

            fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
                std::thread::sleep(Duration::from_millis(200));
                Some(0x1234_5678_90AB_CDEF)
            }
        }

        let mut manager: MergeManager<DefaultIds> = MergeManager::new();
        let catalog = make_catalog();
        let payload: ShardPayload<DefaultIds> = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData {
                ordinal_to_user: vec![],
            },
            created_at_unix_ms: 1000,
        };
        let shard = serialize_shard(1, &payload, &catalog).unwrap();

        let job_count = 12;
        let start = Instant::now();
        for _ in 0..job_count {
            manager
                .merge_shards_background(1, vec![shard.clone()], Box::new(SlowCatalog))
                .unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.active_jobs() > 0 && Instant::now() < deadline {
            let ids: Vec<_> = manager.jobs.keys().copied().collect();
            for job_id in ids {
                let _ = manager.try_get_result(job_id);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(manager.active_jobs(), 0);

        let elapsed = start.elapsed();

        // With one thread per queued job, these 12 slow jobs would complete in
        // roughly one 200ms wave. With a bounded worker pool, completion must
        // take meaningfully longer than that.
        assert!(
            elapsed >= Duration::from_millis(350),
            "expected bounded merge workers; elapsed={elapsed:?} for {job_count} jobs"
        );
    }
}
