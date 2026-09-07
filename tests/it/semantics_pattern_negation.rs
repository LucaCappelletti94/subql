//! `NOT LIKE` and `NOT ILIKE` negate the pattern match rather than the row.
//!
//! Both lower through one procedure, so the negation is asserted per keyword.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// `ILIKE` folds by collation, and only an ASCII-only folding is reproducible
/// in process, so the column names `C`.
const DDL: &str = "CREATE TABLE names (id INT PRIMARY KEY, name TEXT COLLATE \"C\")";

fn notifies(predicate: &str, name: &str) -> bool {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "names").expect("names is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    let row = vec![Value::Int(1), Value::String(name.to_string())];
    !engine
        .consumers(&TestEvent::insert(table, row))
        .expect("dispatch succeeds")
        .inserted()
        .is_empty()
}

#[test]
fn not_like_matches_exactly_the_rows_like_refuses() {
    assert!(
        notifies("SELECT * FROM names WHERE name NOT LIKE 'a%'", "zed"),
        "a name outside the pattern passes the negated match"
    );
    assert!(
        !notifies("SELECT * FROM names WHERE name NOT LIKE 'a%'", "abc"),
        "a name inside the pattern is refused"
    );
    assert!(
        notifies("SELECT * FROM names WHERE name LIKE 'a%'", "abc"),
        "and the unnegated pattern answers the other way"
    );
}

#[test]
fn not_ilike_negates_the_case_insensitive_match() {
    assert!(
        !notifies("SELECT * FROM names WHERE name NOT ILIKE 'a%'", "ABC"),
        "the fold still matches, so the negation refuses the row"
    );
    assert!(
        notifies("SELECT * FROM names WHERE name NOT ILIKE 'a%'", "ZED"),
        "a name outside the folded pattern passes"
    );
    assert!(
        notifies("SELECT * FROM names WHERE name ILIKE 'a%'", "ABC"),
        "and the unnegated fold answers the other way"
    );
}
