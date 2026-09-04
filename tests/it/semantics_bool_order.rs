//! Ordered comparison on a boolean column, measured against each engine.
//!
//! PostgreSQL and MySQL both order booleans: `false` sorts below `true`, so
//! `WHERE flag > false` returns every true row. SubQL served that form in
//! process and answered no-match, because the in-process comparator had no
//! order for the `Bool` variant at all and collapsed it to unknown.
//!
//! SQLite has no boolean type. A column declared `BOOLEAN` is an integer
//! column, subql carries its cells as `i64`, and the comparison is integer
//! comparison, which is why that case is asserted separately rather than
//! folded into the boolean rule.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

type PgEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;
type MySqlEngine = SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB>;
type SqliteEngine = SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB>;

const PG_DDL: &str = "CREATE TABLE flags (id INT PRIMARY KEY, flag BOOLEAN)";
const MYSQL_DDL: &str = "CREATE TABLE flags (id INT PRIMARY KEY, flag BOOLEAN)";
const SQLITE_DDL: &str = "CREATE TABLE flags (id INTEGER PRIMARY KEY, flag BOOLEAN)";

/// Whether the one subscription registered for `predicate` sees a row whose
/// `flag` cell is `cell`.
fn pg_notifies(predicate: &str, cell: Value<Postgres>) -> bool {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "flags").expect("flags is in the catalog");
    let mut engine: PgEngine = SubscriptionEngine::new(db, PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    let notifications = engine
        .consumers(&TestEvent::insert(table, vec![Value::Int(1), cell]))
        .expect("dispatch succeeds");
    !notifications.inserted().is_empty()
}

/// The finding: `flag > false` on a true row is a row in PostgreSQL, and the
/// rest of the order follows from the same rule.
#[test]
fn bool_ordering_matches_the_engine() {
    assert!(
        pg_notifies("SELECT * FROM flags WHERE flag > false", Value::Bool(true)),
        "PostgreSQL sorts false below true, so a true row is above the bound"
    );
    assert!(
        !pg_notifies("SELECT * FROM flags WHERE flag > false", Value::Bool(false)),
        "a false row is not above false"
    );
    assert!(
        pg_notifies("SELECT * FROM flags WHERE flag < true", Value::Bool(false)),
        "the other direction follows the same order"
    );
    assert!(
        pg_notifies("SELECT * FROM flags WHERE flag >= true", Value::Bool(true)),
        "an inclusive bound holds at the value itself"
    );
}

/// MySQL orders its booleans the same way, and answers `1` for the same
/// comparison.
#[test]
fn mysql_bool_ordering_matches_the_engine() {
    let db = ParserDB::parse::<MySqlDialect>(MYSQL_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "flags").expect("flags is in the catalog");
    let mut engine: MySqlEngine = SubscriptionEngine::new(db, MySqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM flags WHERE flag > false",
        ))
        .expect("the predicate registers");
    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Bool(true)],
        ))
        .expect("dispatch succeeds");
    assert!(
        !notifications.inserted().is_empty(),
        "MySQL answers 1 for `true > false`"
    );
}

/// SQLite compares the integers it actually stores. `2` is a legal cell in a
/// column declared `BOOLEAN`, and SQLite reports it above `1`, so the
/// comparison cannot be reduced to truth: that would call the two equal.
#[test]
fn sqlite_bool_ordering_is_integer_comparison() {
    let db = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "flags").expect("flags is in the catalog");
    let notifies = |predicate: &str, cell: Value<SQLite>| {
        let mut engine: SqliteEngine = SubscriptionEngine::new(db.clone(), SQLiteDialect {});
        engine
            .register(SubscriptionRequest::new(1u64, predicate))
            .expect("the predicate registers");
        let notifications = engine
            .consumers(&TestEvent::insert(table, vec![Value::Int(1), cell]))
            .expect("dispatch succeeds");
        !notifications.inserted().is_empty()
    };

    assert!(
        notifies("SELECT * FROM flags WHERE flag > false", Value::Bool(1)),
        "1 is above 0"
    );
    assert!(
        notifies("SELECT * FROM flags WHERE flag > true", Value::Bool(2)),
        "2 is above 1, which a truth comparison would call equal"
    );
    assert!(
        !notifies("SELECT * FROM flags WHERE flag > true", Value::Bool(1)),
        "1 is not above 1"
    );
}
