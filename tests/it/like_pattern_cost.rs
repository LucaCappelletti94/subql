//! A `LIKE` filter does not cost the length of its text times the length of its
//! pattern. A subscriber writes the pattern and any writer sets the length of
//! the text, so a product of the two lets either one stall every event on the
//! table.

use std::time::{Duration, Instant};

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

fn dispatch_time(pattern: &str, text: &str) -> Duration {
    let db = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE t (id INT PRIMARY KEY, s TEXT);")
        .expect("the fixture DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let sql = format!("SELECT * FROM t WHERE s LIKE '{pattern}'");
    engine
        .register(SubscriptionRequest::new(1u64, sql.as_str()))
        .expect("the filter registers");
    let event =
        TestEvent::<Postgres>::insert(0, vec![Value::Int(1), Value::String(text.to_owned())])
            .with_pk_columns([0u16]);
    let start = Instant::now();
    let notifications = engine.consumers(&event).expect("dispatch");
    let elapsed = start.elapsed();
    assert!(
        notifications.inserted().is_empty(),
        "the pattern matches nothing in the text"
    );
    elapsed
}

/// 3,000 one-character segments over a megabyte, 4.2 s per event in release
/// when every pattern position was walked against every character.
#[test]
fn many_short_segments_over_a_long_text_are_cheap() {
    let elapsed = dispatch_time(&"%a".repeat(3_000), &"b".repeat(1_000_000));
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

/// One segment that nearly matches at every position, the worst case for a
/// plain substring search.
#[test]
fn a_long_segment_that_nearly_matches_everywhere_is_cheap() {
    let pattern = format!("%{}b%", "a".repeat(2_000));
    let elapsed = dispatch_time(&pattern, &"a".repeat(1_000_000));
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}
