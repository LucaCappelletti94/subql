//! Background merge operations with atomic swap

use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use ahash::AHashMap;
use crate::{TableId, MergeJobId, MergeError, MergeReport, SchemaCatalog};
use super::shard::{ShardPayload, deserialize_shard, PredicateData};

/// Merge job result
type MergeResult = Result<MergedShard, String>;

/// Merged shard ready for swap
#[derive(Debug)]
pub struct MergedShard {
    pub table_id: TableId,
    pub payload: ShardPayload,
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
struct MergeJob {
    receiver: Receiver<MergeResult>,
}

/// Manager for background merge operations
pub struct MergeManager {
    jobs: AHashMap<MergeJobId, MergeJob>,
    next_job_id: MergeJobId,
}

impl MergeManager {
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
            let result = merge_shards_impl(table_id, shard_bytes, &*catalog, start);

            // Send result
            let _ = tx.send(result);
        });

        self.jobs.insert(job_id, MergeJob {
            receiver: rx,
        });

        Ok(job_id)
    }

    /// Check if merge is complete and return result
    ///
    /// Returns Some(result) if merge complete, None if still running.
    pub fn try_get_result(&mut self, job_id: MergeJobId) -> Result<Option<MergedShard>, MergeError> {
        let job = self.jobs.get_mut(&job_id)
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

impl Default for MergeManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Perform merge operation
fn merge_shards_impl(
    table_id: TableId,
    shard_bytes: Vec<Vec<u8>>,
    catalog: &dyn SchemaCatalog,
    start: std::time::Instant,
) -> MergeResult {
    let mut all_predicates = Vec::new();
    let mut all_bindings = Vec::new();
    let mut user_ordinals = Vec::new();

    // 1. Load all shards
    for bytes in &shard_bytes {
        let (_header, payload) = deserialize_shard(bytes, catalog)
            .map_err(|e| format!("Shard deserialize error: {:?}", e))?;

        all_predicates.extend(payload.predicates);
        all_bindings.extend(payload.bindings);

        // Merge user dictionaries
        for user_id in payload.user_dict.ordinal_to_user {
            if !user_ordinals.contains(&user_id) {
                user_ordinals.push(user_id);
            }
        }
    }

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
    let mut unique_bindings: AHashMap<u64, super::shard::BindingData> = AHashMap::new();

    for binding in all_bindings {
        unique_bindings
            .entry(binding.subscription_id)
            .and_modify(|existing| {
                if binding.updated_at_unix_ms > existing.updated_at_unix_ms {
                    *existing = binding.clone();
                }
            })
            .or_insert(binding);
    }

    let output_bindings: Vec<_> = unique_bindings.into_values().collect();

    // 5. Build merged payload
    let payload = ShardPayload {
        predicates: output_predicates.clone(),
        bindings: output_bindings.clone(),
        user_dict: super::shard::UserDictData {
            ordinal_to_user: user_ordinals,
        },
        created_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };

    let build_ms = start.elapsed().as_millis() as u64;

    let stats = MergeStats {
        input_shards: shard_bytes.len(),
        input_predicates,
        output_predicates: output_predicates.len(),
        input_bindings,
        output_bindings: output_bindings.len(),
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
mod tests {
    use super::*;
    use std::collections::HashMap;
    use super::super::shard::{serialize_shard, PredicateData, BindingData, UserDictData};

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
        fingerprints.insert(1, 0x1234567890ABCDEF);
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

        let payload1 = ShardPayload {
            predicates: vec![pred.clone()],
            bindings: vec![],
            user_dict: UserDictData { ordinal_to_user: vec![] },
            created_at_unix_ms: 1000,
        };

        let payload2 = ShardPayload {
            predicates: vec![pred],
            bindings: vec![],
            user_dict: UserDictData { ordinal_to_user: vec![] },
            created_at_unix_ms: 2000,
        };

        let shard1 = serialize_shard(1, &payload1, &catalog).unwrap();
        let shard2 = serialize_shard(1, &payload2, &catalog).unwrap();

        // Merge
        let start = std::time::Instant::now();
        let result = merge_shards_impl(1, vec![shard1, shard2], &catalog, start);

        assert!(result.is_ok());
        let merged = result.unwrap();

        // Should have deduplicated to 1 predicate
        assert_eq!(merged.payload.predicates.len(), 1);
        assert_eq!(merged.stats.input_predicates, 2);
        assert_eq!(merged.stats.output_predicates, 1);
    }

    #[test]
    fn test_merge_manager() {
        let mut manager = MergeManager::new();

        let catalog = Box::new(make_catalog());
        let payload = ShardPayload {
            predicates: vec![],
            bindings: vec![],
            user_dict: UserDictData { ordinal_to_user: vec![10, 20] },
            created_at_unix_ms: 1000,
        };

        let mock_catalog = make_catalog();
        let shard = serialize_shard(1, &payload, &mock_catalog).unwrap();

        let job_id = manager.merge_shards_background(1, vec![shard], catalog).unwrap();

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
}
