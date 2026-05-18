use crate::DefaultIds;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::GenericDialect;

use super::shard::{ConsumerDictData, ShardPayload};

/// DDL fixture used by persistence tests. A leading `_persistence_pad` table
/// keeps `orders` at table id `1`, so existing tests that pass `1` directly
/// (e.g. `serialize_shard(1, ...)`) continue to resolve the right table.
const FIXTURE_DDL: &str = "CREATE TABLE _persistence_pad (id INT);\n\
                            CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

/// Build the standard persistence-test catalog as a [`ParserDB`].
#[must_use]
pub fn make_catalog() -> ParserDB {
    ParserDB::parse::<GenericDialect>(FIXTURE_DDL).expect("fixture DDL parses")
}

/// Build a divergent catalog that produces a different fingerprint for the
/// same logical table id. Used by mismatch tests.
#[must_use]
pub fn make_divergent_catalog() -> ParserDB {
    ParserDB::parse::<GenericDialect>(
        "CREATE TABLE _persistence_pad (id INT);\n\
         CREATE TABLE orders (id INT PRIMARY KEY, total INT, status TEXT);",
    )
    .expect("divergent fixture DDL parses")
}

/// Empty shard payload (no predicates, no bindings, no consumers).
#[must_use]
pub fn empty_shard_payload(created_at_unix_ms: u64) -> ShardPayload<DefaultIds> {
    shard_payload_with_consumers(vec![], created_at_unix_ms)
}

/// Shard payload with specific consumer ordinals (no predicates or bindings).
#[must_use]
pub fn shard_payload_with_consumers(
    consumers: Vec<u64>,
    created_at_unix_ms: u64,
) -> ShardPayload<DefaultIds> {
    ShardPayload {
        predicates: vec![],
        bindings: vec![],
        consumer_dict: ConsumerDictData {
            ordinal_to_consumer: consumers,
        },
        created_at_unix_ms,
    }
}
