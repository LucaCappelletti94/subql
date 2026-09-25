//! Truth tests, `IS [NOT] TRUE`, `IS [NOT] FALSE` and `IS [NOT] UNKNOWN`.
//!
//! Each maps a condition's three values onto two, so `NULL` answers
//! decisively where the condition alone would not. Measured 2026-09-24 on
//! PostgreSQL 16, MySQL 8.0.46 and SQLite 3.51:
//!
//! ```text
//! condition value         IS TRUE  IS NOT TRUE  IS FALSE  IS NOT FALSE  IS UNKNOWN  IS NOT UNKNOWN
//! true                    true     false        false     true          false       true
//! false                   false    true         true      false         false       true
//! null                    false    true         false     true          true        false
//! ```
//!
//! SQLite has no `IS [NOT] UNKNOWN` at all: it reads `UNKNOWN` as a column
//! name. And the engines disagree on a non-boolean operand: PostgreSQL
//! raises `argument of IS TRUE must be type boolean`, while MySQL and SQLite
//! read `5 IS TRUE` as true. So each engine serves only the tests it
//! accepts, and only over a condition or a boolean column. Agreement with
//! the engines is the differential sweep's job.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n INT, m INT, flag BOOLEAN)";

/// Why `predicate` over `t` is not served in process, `None` when it is.
fn not_served<B, D>(dialect: D, predicate: &str) -> Option<NotServed<B>>
where
    B: Backend<Dialect = D> + subql::compiler::SqlLiteralParse,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, dialect);
    engine
        .register(SubscriptionRequest::new(
            7u64,
            format!("SELECT * FROM t WHERE {predicate}"),
        ))
        .unwrap_or_else(|error| panic!("{predicate} registers on some tier, got {error:?}"))
        .not_served_because
}

const EVERYWHERE: [&str; 6] = [
    "(n = m) IS TRUE",
    "(n = m) IS NOT TRUE",
    "(n = m) IS FALSE",
    "(n = m) IS NOT FALSE",
    "flag IS TRUE",
    "NOT ((n > 1 AND m < 5) IS FALSE)",
];

const UNKNOWN: [&str; 3] = [
    "(n = m) IS UNKNOWN",
    "(n = m) IS NOT UNKNOWN",
    "flag IS NOT UNKNOWN",
];

/// Each engine serves the tests it accepts over a condition or a boolean
/// column, and SQLite routes `IS [NOT] UNKNOWN`, which it does not have.
#[test]
fn each_engine_serves_the_truth_tests_it_accepts() {
    for predicate in EVERYWHERE.iter().chain(&UNKNOWN) {
        assert_eq!(
            not_served::<Postgres, _>(PostgreSqlDialect {}, predicate),
            None,
            "PostgreSQL serves {predicate}"
        );
        assert_eq!(
            not_served::<MySql, _>(MySqlDialect {}, predicate),
            None,
            "MySQL serves {predicate}"
        );
    }
    for predicate in EVERYWHERE {
        assert_eq!(
            not_served::<SQLite, _>(SQLiteDialect {}, predicate),
            None,
            "SQLite serves {predicate}"
        );
    }
    for predicate in UNKNOWN {
        assert!(
            not_served::<SQLite, _>(SQLiteDialect {}, predicate).is_some(),
            "SQLite routes {predicate}"
        );
    }
}

/// SQLite keeps a boolean column as the integer stored in it, and reads any
/// nonzero integer as true where a condition is read, while `= true` compares
/// the integer with `1`. Measured on 3.51 over a stored `5`:
///
/// ```text
/// WHERE flag   flag IS TRUE   flag IS NOT FALSE   NOT flag   flag = true
/// selected     1              1                   0          0
/// ```
#[test]
fn sqlite_reads_a_stored_nonzero_boolean_as_true() {
    for (predicate, selected) in [
        ("flag", true),
        ("flag IS TRUE", true),
        ("flag IS NOT FALSE", true),
        ("n = 1 AND flag", true),
        ("COALESCE(flag, false)", true),
        ("NOT flag", false),
        ("flag = true", false),
    ] {
        let database = ParserDB::parse::<SQLiteDialect>(DDL).unwrap();
        let table = catalog_helpers::table_id::<Postgres, _>(&database, "t").unwrap();
        let mut engine: SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, SQLiteDialect {});
        engine
            .register(SubscriptionRequest::new(
                1u64,
                format!("SELECT * FROM t WHERE {predicate}"),
            ))
            .unwrap();
        // The decoder hands a `BOOLEAN` integer over as the integer it is.
        let row = vec![Value::Int(1), Value::Int(1), Value::Int(3), Value::Bool(5)];
        let inserted = engine.consumers(&TestEvent::insert(table, row)).unwrap();
        assert_eq!(!inserted.inserted().is_empty(), selected, "{predicate}");
    }
}

/// A non-boolean operand is where the engines disagree, so it is left to
/// the engine: PostgreSQL raises and the others read a number's truth.
#[test]
fn a_non_boolean_operand_is_left_to_the_engine() {
    for predicate in ["n IS TRUE", "(n + 1) IS NOT FALSE"] {
        assert!(
            not_served::<Postgres, _>(PostgreSqlDialect {}, predicate).is_some(),
            "PostgreSQL routes {predicate}"
        );
        assert!(
            not_served::<MySql, _>(MySqlDialect {}, predicate).is_some(),
            "MySQL routes {predicate}"
        );
        assert!(
            not_served::<SQLite, _>(SQLiteDialect {}, predicate).is_some(),
            "SQLite routes {predicate}"
        );
    }
}

/// A cell the event did not carry is not `NULL`. A truth test reading it
/// has no answer, although it maps a present `NULL` decisively, and a
/// present `NULL` beside an absent cell it never reads still answers.
#[test]
fn a_missing_cell_leaves_a_truth_test_unanswered() {
    let dispatch = |predicate: &str, row: Vec<Value<Postgres>>| {
        let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let table = catalog_helpers::table_id::<Postgres, _>(&database, "t").unwrap();
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        engine
            .register(SubscriptionRequest::new(
                1u64,
                format!("SELECT * FROM t WHERE {predicate}"),
            ))
            .unwrap();
        let notifications = engine.consumers(&TestEvent::insert(table, row)).unwrap();
        (
            notifications.inserted().to_vec(),
            notifications
                .unanswered()
                .iter()
                .map(|entry| entry.column)
                .collect::<Vec<_>>(),
        )
    };
    let n_missing = || vec![Value::Int(1), Value::Missing, Value::Int(3), Value::Null];
    for predicate in [
        "(n = m) IS NOT TRUE",
        "(n = m) IS UNKNOWN",
        "(n = m) IS FALSE",
        "NOT ((n = m) IS NOT UNKNOWN)",
    ] {
        assert_eq!(
            dispatch(predicate, n_missing()),
            (vec![], vec![1]),
            "{predicate} is unanswered for want of `n`"
        );
    }
    // A decisive conjunct settles the condition although `n` is absent, so
    // the test answers.
    assert_eq!(
        dispatch("(m = 4 AND n = 3) IS FALSE", n_missing()),
        (vec![1], vec![]),
        "`m = 4` is false, so the conjunction is false whatever `n` holds"
    );
    // Conservative on purpose: `m` is a present `NULL`, so the conjunction is
    // false or unknown and never true, yet it is left unanswered because `n` was
    // read absent, and the stack does not say which operand the unknown came from.
    assert_eq!(
        dispatch(
            "(n = 3 AND flag) IS TRUE",
            vec![Value::Int(1), Value::Missing, Value::Int(3), Value::Null]
        ),
        (vec![], vec![1]),
        "an unknown that read an absent cell is unanswered"
    );
    assert_eq!(
        dispatch("flag IS UNKNOWN", n_missing()),
        (vec![1], vec![]),
        "a present `NULL` is unknown, and that is an answer"
    );
    assert_eq!(
        dispatch(
            "flag IS UNKNOWN",
            vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Missing]
        ),
        (vec![], vec![3]),
        "an absent flag might hold anything"
    );
}
