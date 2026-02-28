use ahash::AHashMap;

use super::shard::PredicateData;

/// Compare persistence predicate payloads for semantic equivalence.
///
/// This deliberately ignores mutable metadata (hash/refcount/timestamps) and
/// only compares fields that define predicate behavior.
pub fn predicate_data_equivalent(left: &PredicateData, right: &PredicateData) -> bool {
    left.normalized_sql == right.normalized_sql
        && left.bytecode_instructions == right.bytecode_instructions
        && left.prefilter_plan == right.prefilter_plan
        && left.dependency_columns == right.dependency_columns
        && left.projection == right.projection
}

/// Deduplicate predicates by hash: keep most recent timestamp, error on semantic collision.
///
/// Used by both the merge path and the shard-rebuild path to ensure consistent
/// deduplication semantics.
pub fn dedup_predicates_by_hash<E>(
    predicates: impl IntoIterator<Item = PredicateData>,
    make_error: impl Fn(String) -> E,
) -> Result<AHashMap<u128, PredicateData>, E> {
    let mut unique: AHashMap<u128, PredicateData> = AHashMap::new();
    for pred in predicates {
        if let Some(existing) = unique.get_mut(&pred.hash) {
            if !predicate_data_equivalent(existing, &pred) {
                return Err(make_error(format!(
                    "predicate hash collision for hash {:016x} with non-equivalent payload",
                    pred.hash
                )));
            }
            if pred.updated_at_unix_ms > existing.updated_at_unix_ms {
                *existing = pred;
            }
        } else {
            unique.insert(pred.hash, pred);
        }
    }
    Ok(unique)
}
