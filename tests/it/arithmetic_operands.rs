//! What arithmetic reads as a number.
//!
//! MySQL and SQLite read text as the number it starts with and a boolean as
//! its integer, and PostgreSQL refuses both. subql computes over numbers
//! only, so arithmetic over anything else is routed to a read, where the
//! engine answers it.
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

/// Text or a boolean under arithmetic is routed on every backend.
#[test]
fn arithmetic_over_anything_but_numbers_is_routed() {
    for predicate in [
        "(- s) IS NULL",
        "(- f) IS NULL",
        "('ab' * 'x') IS NOT NULL",
        "(a - s) > 0",
        "(f + 1) = 1",
        "(a * ('1' + 1)) > 0",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [false; 3],
            "{predicate} is routed on every backend"
        );
    }
}

/// Two numeric kinds under one operator, or a `%` over anything but
/// integers, is routed. Every engine widens `1 * 2.0` to a float and subql
/// computes each kind with itself only, and `%` over a float is an error on
/// PostgreSQL, a truncation on SQLite and a remainder on MySQL.
#[test]
fn arithmetic_across_kinds_is_routed() {
    for predicate in [
        "(a + r) IS NULL",
        "(a * r) > 0",
        "(r % r) = 0",
        "(a % r) IS NULL",
        "(r % 2) = 0",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [false; 3],
            "{predicate} is routed on every backend"
        );
    }
}

/// Arithmetic over numeric columns, numbers and `NULL` stays served.
#[test]
fn arithmetic_over_numbers_is_served() {
    for predicate in [
        "(- a) > 0",
        "(a * 2) > r",
        "(a + NULL) IS NULL",
        "(- (a % 3)) = 0",
    ] {
        assert_eq!(
            served_everywhere(predicate),
            [true; 3],
            "{predicate} is served on every backend"
        );
    }
}
