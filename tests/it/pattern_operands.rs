//! What a pattern match reads as text.
//!
//! MySQL and SQLite match a number or a boolean by its text rendering, and
//! PostgreSQL refuses it. subql matches text only, so a `LIKE` whose operand
//! is anything but a text column, a string or `NULL` is routed to a read,
//! where the engine answers it.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Backend, MySql, Postgres, SQLite};
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

const DDL: &str =
    "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT, r DOUBLE PRECISION, s TEXT, f BOOLEAN)";

/// Whether `predicate` over `t` is served in process.
fn served<B, D>(dialect: D, predicate: &str) -> bool
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
        .is_ok_and(|registered| registered.not_served_because.is_none())
}

fn served_everywhere(predicate: &str) -> [bool; 3] {
    [
        served::<Postgres, _>(PostgreSqlDialect {}, predicate),
        served::<MySql, _>(MySqlDialect {}, predicate),
        served::<SQLite, _>(SQLiteDialect {}, predicate),
    ]
}

/// A number, a boolean or a computed value is not text to subql, on either
/// side of the match.
#[test]
fn a_pattern_match_over_anything_but_text_is_routed() {
    for predicate in [
        "a LIKE '1%'",
        "f LIKE '0'",
        "r LIKE r",
        "'1' LIKE a",
        "(- s) LIKE 'a'",
        "s LIKE (a + 1)",
        "a NOT LIKE '' ESCAPE '!'",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [false; 3],
            "{predicate} is routed on every backend"
        );
    }
}

/// Text columns, strings and `NULL` stay served. MySQL's default text
/// collation is case-insensitive, which routes it there for that reason.
#[test]
fn a_pattern_match_over_text_is_served() {
    for predicate in ["s LIKE 'a%'", "'x' LIKE s", "s NOT LIKE s", "(s) LIKE NULL"] {
        assert_eq!(
            served_everywhere(predicate),
            [true, false, true],
            "{predicate} is served on PostgreSQL and SQLite"
        );
    }
}
