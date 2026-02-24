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
}
