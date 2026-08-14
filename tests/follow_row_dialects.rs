//! `follow_row` renders its predicate in the engine's own dialect.
//!
//! The generated `SELECT * FROM <table> WHERE <pk> = <bind>` is parsed straight
//! back by that dialect, and two of its tokens are dialect-specific. MySQL reads
//! `$` as an identifier character, so a `$1` bind becomes a column reference
//! there, and it delimits identifiers with backticks, so a double-quoted column
//! name becomes a string literal. The first mistake fails registration, but the
//! second registers a predicate comparing two constants that quietly matches
//! nothing, so each case here follows a row and then dispatches against it.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine};

const DDL: &str = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);";

const CONSUMER: u64 = 1;

#[test]
fn follow_row_matches_its_row_under_postgres() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let users = catalog_helpers::table_id(&db, "users").unwrap();
    let mut engine =
        SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, _>::new(db, PostgreSqlDialect {});

    engine
        .follow_row(CONSUMER, "users", vec![Value::Int(7)])
        .expect("follow row");

    let followed =
        TestEvent::<Postgres>::insert(users, vec![Value::Int(7), Value::String("ann".into())])
            .with_pk_columns([0u16]);
    assert_eq!(
        engine.consumers(&followed).unwrap().inserted(),
        vec![CONSUMER]
    );

    let other =
        TestEvent::<Postgres>::insert(users, vec![Value::Int(8), Value::String("bob".into())])
            .with_pk_columns([0u16]);
    assert!(
        engine.consumers(&other).unwrap().inserted().is_empty(),
        "user id 8 was not followed, only id 7"
    );
}

#[test]
fn follow_row_matches_its_row_under_mysql() {
    let db = ParserDB::parse::<MySqlDialect>(DDL).unwrap();
    let users = catalog_helpers::table_id(&db, "users").unwrap();
    let mut engine =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, _>::new(db, MySqlDialect {});

    engine
        .follow_row(CONSUMER, "users", vec![Value::Int(7)])
        .expect("follow row");

    let followed =
        TestEvent::<MySql>::insert(users, vec![Value::Int(7), Value::String("ann".into())])
            .with_pk_columns([0u16]);
    assert_eq!(
        engine.consumers(&followed).unwrap().inserted(),
        vec![CONSUMER]
    );

    let other = TestEvent::<MySql>::insert(users, vec![Value::Int(8), Value::String("bob".into())])
        .with_pk_columns([0u16]);
    assert!(
        engine.consumers(&other).unwrap().inserted().is_empty(),
        "user id 8 was not followed, only id 7"
    );
}

#[test]
fn follow_row_matches_its_row_under_sqlite() {
    let db = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
    let users = catalog_helpers::table_id(&db, "users").unwrap();
    let mut engine =
        SubscriptionEngine::<TestEvent<SQLite>, DefaultIds, _>::new(db, SQLiteDialect {});

    engine
        .follow_row(CONSUMER, "users", vec![Value::Int(7)])
        .expect("follow row");

    let followed =
        TestEvent::<SQLite>::insert(users, vec![Value::Int(7), Value::String("ann".into())])
            .with_pk_columns([0u16]);
    assert_eq!(
        engine.consumers(&followed).unwrap().inserted(),
        vec![CONSUMER]
    );

    let other =
        TestEvent::<SQLite>::insert(users, vec![Value::Int(8), Value::String("bob".into())])
            .with_pk_columns([0u16]);
    assert!(
        engine.consumers(&other).unwrap().inserted().is_empty(),
        "user id 8 was not followed, only id 7"
    );
}

/// A composite key produces one equality per column, each with its own bind.
#[test]
fn follow_row_matches_a_composite_key_under_mysql() {
    const COMPOSITE_DDL: &str = "CREATE TABLE items (\
        region_id INT, item_id INT, name TEXT, \
        PRIMARY KEY (region_id, item_id));";

    let db = ParserDB::parse::<MySqlDialect>(COMPOSITE_DDL).unwrap();
    let items = catalog_helpers::table_id(&db, "items").unwrap();
    let mut engine =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, _>::new(db, MySqlDialect {});

    engine
        .follow_row(CONSUMER, "items", vec![Value::Int(1), Value::Int(100)])
        .expect("follow row");

    let followed = TestEvent::<MySql>::insert(
        items,
        vec![
            Value::Int(1),
            Value::Int(100),
            Value::String("widget".into()),
        ],
    )
    .with_pk_columns([0u16, 1u16]);
    assert_eq!(
        engine.consumers(&followed).unwrap().inserted(),
        vec![CONSUMER]
    );

    // Same region, different item: the second equality must fail.
    let other = TestEvent::<MySql>::insert(
        items,
        vec![
            Value::Int(1),
            Value::Int(200),
            Value::String("widget".into()),
        ],
    )
    .with_pk_columns([0u16, 1u16]);
    assert!(
        engine.consumers(&other).unwrap().inserted().is_empty(),
        "item 200 was not followed, only item 100 in region 1"
    );
}
