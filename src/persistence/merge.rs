//! Background merge operations with atomic swap

use super::shard::{deserialize_shard, BindingData, PredicateData, ShardPayload, UserDictData};
use crate::{DefaultIds, IdTypes, MergeError, MergeJobId, MergeReport, SchemaCatalog, TableId};
use ahash::AHashMap;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;

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

/// Manager for background merge operations
pub struct MergeManager<I: IdTypes = DefaultIds> {
    jobs: AHashMap<MergeJobId, MergeJob<I>>,
    next_job_id: MergeJobId,
}

impl<I: IdTypes> MergeManager<I> {
    /// Create new merge manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            jobs: AHashMap::new(),
            next_job_id: 1,
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

        // Spawn background thread
        thread::spawn(move || {
            let start = std::time::Instant::now();

            // Perform merge
            let result = merge_shards_impl::<I>(table_id, &shard_bytes, &*catalog, start);

            // Send result — receiver may have been dropped if caller discarded the job
            if tx.send(result).is_err() {
                #[cfg(feature = "observability")]
                tracing::warn!("Merge result receiver dropped before result was sent");
            }
        });

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
        let (_header, payload) = deserialize_shard(bytes, catalog)
            .map_err(|e| format!("Shard deserialize error: {e:?}"))?;

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
        unique_predicates
            .entry(pred.hash)
            .and_modify(|existing| {
                // Keep most recent
                if pred.updated_at_unix_ms > existing.updated_at_unix_ms {
                    *existing = pred.clone();
                }
            })
            .or_insert(pred);
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

    // SystemTime::now() is always after UNIX_EPOCH on supported platforms;
    // truncation from u128 to u64 is acceptable (won't overflow until year ~584M).
    #[allow(clippy::cast_possible_truncation)]
    let created_at_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64;

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
    use super::super::shard::{serialize_shard, PredicateData, UserDictData};
    use super::*;
    use std::collections::HashMap;

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
            bytecode_instructions: vec![],
            dependency_columns: vec![1],
            refcount: 1,
            updated_at_unix_ms: 1000, // Older
        };

        let pred_new = PredicateData {
            hash: 0x1234, // Same hash
            normalized_sql: "age > 18 (updated)".to_string(),
            bytecode_instructions: vec![1, 2, 3],
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
        assert_eq!(
            merged.payload.predicates[0].normalized_sql,
            "age > 18 (updated)"
        );
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
}
