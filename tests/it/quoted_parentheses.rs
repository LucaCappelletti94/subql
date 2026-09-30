//! A parenthesis, bracket or operator character inside a string literal, a
//! quoted name or a comment is text, not structure. The nesting bounds subql
//! applies before parsing count only the structure, so a filter whose text
//! holds an unmatched `(` is served like any other.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use subql::backend::{MySql, Postgres};
use subql::testing::TestEvent;
use subql::{DefaultIds, RegisterError, Registered, SubscriptionEngine, SubscriptionRequest, Tier};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, status TEXT, \"a(b\" TEXT);";

fn served_on_postgres(sql: &str) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    match engine.register(SubscriptionRequest::new(1u64, sql)) {
        Ok(Registered {
            tier: Tier::InProcess(_),
            ..
        }) => {}
        other => panic!("{sql} should be served in process, got {other:?}"),
    }
}

#[test]
fn an_unmatched_parenthesis_inside_a_literal_is_served() {
    for sql in [
        "SELECT * FROM t WHERE status = 'a('",
        "SELECT * FROM t WHERE status = ')'",
        "SELECT * FROM t WHERE status LIKE '%(%' OR status = ']'",
        "SELECT * FROM t WHERE status = E'\\'('",
    ] {
        served_on_postgres(sql);
    }
}

#[test]
fn an_unmatched_parenthesis_inside_a_quoted_name_or_a_comment_is_served() {
    for sql in [
        "SELECT * FROM t WHERE \"a(b\" = 'x'",
        "SELECT * FROM t WHERE status = 'x' -- (",
        "SELECT * FROM t WHERE /* ( */ status = 'x'",
    ] {
        served_on_postgres(sql);
    }
}

/// MySQL escapes a quote with a backslash, so the `(` after it is still inside
/// the literal.
#[test]
fn an_unmatched_parenthesis_after_a_backslash_escaped_quote_is_served_on_mysql() {
    let sql = "SELECT * FROM t WHERE status = 'a\\'('";
    let db = ParserDB::parse::<MySqlDialect>(
        "CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(8) COLLATE utf8mb4_bin);",
    )
    .unwrap();
    let mut engine: SubscriptionEngine<TestEvent<MySql>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, MySqlDialect {});
    match engine.register(SubscriptionRequest::new(1u64, sql)) {
        Ok(Registered {
            tier: Tier::InProcess(_),
            ..
        }) => {}
        other => panic!("{sql} should be served in process, got {other:?}"),
    }
}

/// A run of dashes inside a literal is not a run of operators.
#[test]
fn a_long_run_of_operator_characters_inside_a_literal_is_served() {
    served_on_postgres(&format!(
        "SELECT * FROM t WHERE status = '{}'",
        "-".repeat(200)
    ));
}

/// A parenthesis that only a comment closes is still open.
#[test]
fn a_parenthesis_closed_only_inside_a_comment_is_refused() {
    let sql = "SELECT * FROM t WHERE (status = 'x' -- )";
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    match engine.register(SubscriptionRequest::new(1u64, sql)) {
        Err(RegisterError::UnsupportedSql(message)) => {
            assert!(message.contains("Unbalanced"), "{message}");
        }
        other => panic!("{sql} should be refused as unbalanced, got {other:?}"),
    }
}
