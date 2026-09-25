//! `COALESCE`, the first of its arguments that is not `NULL`.
//!
//! Measured 2026-09-24 on PostgreSQL 16, MySQL 8.0.46 and SQLite 3.51:
//!
//! ```text
//! expression                  pg      mysql   sqlite
//! COALESCE(NULL, NULL, 4) = 4 t       1       1
//! COALESCE(1, 2.5) / 2        0.5     0.5     0
//! COALESCE(NULL, 1) = '1'     t       1       0
//! COALESCE(1, 'a')            error   1       1
//! COALESCE(5)                 5       5       error
//! ```
//!
//! The engines agree once every argument has one type: the column arguments
//! share a declared type and collation, and each literal is written in that
//! family's own form. SQLite gives an expression no affinity, so a quoted
//! literal compared with one stays text, which is why a literal beside a
//! `COALESCE` must be written in its family's form too. Served, a `COALESCE`
//! compares and computes as its column would. Agreement with the engines
//! over every `NULL` pairing is the differential sweep's job.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

const PG_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n INT, m INT, big BIGINT, \
                      price DOUBLE PRECISION, label TEXT COLLATE \"C\", strict TEXT COLLATE \"POSIX\", \
                      loose TEXT, flag BOOLEAN, at DATE, until DATE)";
const MYSQL_DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n INT, m INT, big BIGINT, \
                         price DOUBLE, label VARCHAR(16) COLLATE utf8mb4_bin, \
                         strict VARCHAR(16) COLLATE utf8mb4_0900_bin, loose VARCHAR(16), \
                         flag BOOLEAN, at DATE, until DATE)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, m INTEGER, \
                          big BIGINT, price REAL, label TEXT, strict TEXT COLLATE NOCASE, \
                          loose TEXT, flag BOOLEAN, at DATE, until DATE)";

/// Why `predicate` is not served in process, `None` when it is.
fn not_served<B, D>(ddl: &str, dialect: D, predicate: &str) -> Option<NotServed<B>>
where
    B: Backend<Dialect = D> + subql::compiler::SqlLiteralParse,
    D: sqlparser::dialect::Dialect + Default,
{
    let database = ParserDB::parse::<D>(ddl).unwrap();
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

fn served_everywhere(predicate: &str) {
    assert_eq!(
        not_served::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, predicate),
        None,
        "PostgreSQL serves {predicate}"
    );
    assert_eq!(
        not_served::<MySql, _>(MYSQL_DDL, MySqlDialect {}, predicate),
        None,
        "MySQL serves {predicate}"
    );
    assert_eq!(
        not_served::<SQLite, _>(SQLITE_DDL, SQLiteDialect {}, predicate),
        None,
        "SQLite serves {predicate}"
    );
}

fn routed_everywhere(predicate: &str) {
    assert!(
        not_served::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, predicate).is_some(),
        "PostgreSQL routes {predicate}"
    );
    assert!(
        not_served::<MySql, _>(MYSQL_DDL, MySqlDialect {}, predicate).is_some(),
        "MySQL routes {predicate}"
    );
    assert!(
        not_served::<SQLite, _>(SQLITE_DDL, SQLiteDialect {}, predicate).is_some(),
        "SQLite routes {predicate}"
    );
}

/// One declared type, native literals, any arity, wherever a value is read.
#[test]
fn a_coalesce_of_one_type_is_served() {
    for predicate in [
        "COALESCE(n, 0) > 3",
        "COALESCE(n, m, 0) = 4",
        "COALESCE(n, NULL, m) IS NULL",
        "COALESCE(n, 0) + 1 > 2",
        "COALESCE(n, 0) IN (1, 2)",
        "COALESCE(n, 0) BETWEEN 1 AND 3",
        "COALESCE(price, 0.5) > 1",
        "COALESCE(label, 'x') = 'x'",
        "COALESCE(label, 'x') LIKE 'a%'",
        "COALESCE(flag, false)",
        "NOT COALESCE(flag, true) AND n > 1",
        "COALESCE(n, 0) = m",
    ] {
        served_everywhere(predicate);
    }
}

/// Every other mix is left to the engine, which types it by its own rules.
#[test]
fn a_coalesce_the_engines_type_differently_is_routed() {
    for predicate in [
        // Two families, or a literal of another family.
        "COALESCE(n, label) = 'a'",
        "COALESCE(n, 2.5) > 1",
        "COALESCE(n, '1') = 1",
        // Two widths, two collations, and a family no literal spells alike.
        "COALESCE(n, big) = 1",
        "COALESCE(label, strict) = 'a'",
        "COALESCE(at, until) IS NULL",
        // One argument, an argument that is not a column or a literal, and no
        // column at all.
        "COALESCE(n) = 1",
        "COALESCE(n + 1, 0) = 1",
        "COALESCE(n, n + 1) = 1",
        "COALESCE(NULL, 3) = 3",
        // A literal beside it in another family's form.
        "COALESCE(n, 0) = '1'",
        "COALESCE(n, 0) IN ('1')",
        "COALESCE(n, 0) IS NOT DISTINCT FROM '1'",
    ] {
        routed_everywhere(predicate);
    }
}

/// A served `COALESCE` is classified as its column is, so a comparison the
/// column's collation leaves to the database is left to it here too.
#[test]
fn a_coalesce_carries_its_columns_facts() {
    for (column, coalesced) in [
        ("loose < 'b'", "COALESCE(loose, 'x') < 'b'"),
        ("label < 'b'", "COALESCE(label, 'x') < 'b'"),
        // The column is whichever argument is one, not the first argument.
        ("loose < 'b'", "COALESCE(NULL, loose) < 'b'"),
        ("loose < label", "COALESCE(NULL, loose) < label"),
        ("loose = 'b'", "COALESCE(loose, 'x') = 'b'"),
        ("n = price", "COALESCE(n, 0) = price"),
    ] {
        assert_eq!(
            not_served::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, coalesced).is_some(),
            not_served::<Postgres, _>(PG_DDL, PostgreSqlDialect {}, column).is_some(),
            "{coalesced} is classified as {column} is"
        );
    }
}

/// A cell the event did not carry before the first present argument leaves
/// the value unknown and names the column. After it, it is never read.
#[test]
fn a_missing_cell_before_the_answer_leaves_it_unanswered() {
    let dispatch = |predicate: &str, row: Vec<Value<Postgres>>| {
        let database = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
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
    let row = |n: Value<Postgres>, m: Value<Postgres>| {
        let mut cells: Vec<Value<Postgres>> = (0..11).map(|_| Value::Null).collect();
        cells[0] = Value::Int(1);
        cells[1] = n;
        cells[2] = m;
        cells
    };
    assert_eq!(
        dispatch("COALESCE(n, 0) = 0", row(Value::Missing, Value::Null)),
        (vec![], vec![1]),
        "`n` may hold anything"
    );
    assert_eq!(
        dispatch("COALESCE(n, m) = 3", row(Value::Null, Value::Missing)),
        (vec![], vec![2]),
        "`n` is NULL, so the answer is the absent `m`"
    );
    assert_eq!(
        dispatch("COALESCE(m, n) = 3", row(Value::Missing, Value::Int(3))),
        (vec![1], vec![]),
        "`m` answers, so the absent `n` is never needed"
    );
    assert_eq!(
        dispatch("COALESCE(n, m, 4) = 4", row(Value::Null, Value::Null)),
        (vec![1], vec![]),
        "every column is NULL, so the literal answers"
    );
}

/// A `COALESCE` outside the filter, projected, aggregated or grouped by, is
/// not served here and lands on a read tier with a reason.
#[test]
fn a_coalesce_outside_the_filter_is_routed() {
    for sql in [
        "SELECT COALESCE(n, 0) FROM t",
        "SELECT SUM(COALESCE(n, 0)) FROM t",
        "SELECT COUNT(*) FROM t GROUP BY COALESCE(n, 0)",
    ] {
        let database = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap();
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        let reason = engine
            .register(SubscriptionRequest::new(1u64, sql))
            .unwrap_or_else(|error| panic!("{sql} is routed, not refused: {error:?}"))
            .not_served_because;
        assert!(
            matches!(reason, Some(NotServed::UnsupportedSql(_))),
            "{sql}: {reason:?}"
        );
    }
}
