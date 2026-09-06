//! Ordered comparison on a `jsonb` column is classified as needing a
//! database read, not answered from a guess.
//!
//! PostgreSQL defines a total order over `jsonb`, and it is not the order of
//! the canonical binary form: an empty array sorts below a boolean while a
//! one-element array sorts above it, arrays and objects compare by element
//! and pair count before content, and the string arm follows the database
//! collation. The pinned `postgres-jsonb-canonical` exposes `equivalent`,
//! `encode` and `encode_into` and no ordering comparator, so nothing here
//! can reproduce that order. Serving the form in process therefore answered
//! no-match where the database returns a row, which is the divergence this
//! phase removes: the comparison now becomes a read.
//!
//! Equality is unaffected and stays in process: it goes through the
//! canonical form, which is exactly what `jsonb` equality means.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ColumnId, DefaultIds, NotServed, RegisterError, SubscriptionEngine,
    SubscriptionRequest, TableId,
};

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

const DDL: &str = "CREATE TABLE docs (id INT PRIMARY KEY, doc JSONB)";

fn engine() -> (Engine, TableId, ColumnId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
    let column = catalog_helpers::column_id(&db, table, "doc").expect("doc is in the catalog");
    (
        SubscriptionEngine::new(db, PostgreSqlDialect {}),
        table,
        column,
    )
}

/// The finding: `doc > '{}'` is not served in process, and the cause names
/// the column and its kind rather than a sentence a caller has to parse.
#[test]
fn jsonb_ordering_is_classified_not_served() {
    let (mut engine, _, column) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE doc > '{}'",
        ))
        .expect("a read answers it, so registration succeeds");

    assert!(
        registered.served().is_none(),
        "an order this build cannot reproduce must not be answered in process"
    );
    assert_eq!(
        registered.not_served_because,
        Some(NotServed::OrderNotReproducible {
            column,
            kind: ScalarFamily::Jsonb.into(),
        }),
        "the cause is the reason the caller and the engine both read"
    );
}

/// `BETWEEN` is two ordered comparisons, so it carries the same cause.
#[test]
fn jsonb_between_is_classified_too() {
    let (mut engine, _, column) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE doc BETWEEN '{}' AND '[1]'",
        ))
        .expect("a read answers it, so registration succeeds");

    assert!(
        registered.served().is_none(),
        "BETWEEN is an ordered comparison and cannot be served either"
    );
    assert_eq!(
        registered.not_served_because,
        Some(NotServed::OrderNotReproducible {
            column,
            kind: ScalarFamily::Jsonb.into(),
        }),
    );
}

/// `json` is not `jsonb`. PostgreSQL gives the type no comparison operator
/// at all, in either direction, so such a statement is invalid rather than
/// merely unservable: it is refused outright, and planning a read for it
/// would hand the database a query the database itself rejects.
#[test]
fn json_comparison_is_refused_not_read() {
    let db = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE payloads (id INT PRIMARY KEY, payload JSON)",
    )
    .expect("DDL parses");
    for predicate in [
        "SELECT * FROM payloads WHERE payload > '{}'",
        "SELECT * FROM payloads WHERE payload = '{}'",
    ] {
        let mut engine: Engine = SubscriptionEngine::new(db.clone(), PostgreSqlDialect {});
        let error = engine
            .register(SubscriptionRequest::new(1u64, predicate))
            .expect_err("json has no comparison operator");
        assert!(
            matches!(&error, RegisterError::TypeError(prose) if prose.contains("json")),
            "expected a type refusal naming json, got {error:?}"
        );
    }
}

/// The control: equality is `jsonb` equivalence through the canonical form,
/// which this build does reproduce, so it stays in process and still fires.
#[test]
fn jsonb_equality_still_folds_in_process() {
    let (mut engine, table, _) = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE doc = '{\"a\": 1}'",
        ))
        .expect("equality registers");
    assert!(
        registered.served().is_some(),
        "equality is reproducible and must stay in process"
    );
    assert_eq!(registered.not_served_because, None);

    // Key order and whitespace differ from the literal: equality is
    // equivalence of the canonical form, not of the text.
    let row = vec![Value::Int(1), Value::Jsonb(serde_json::json!({ "a": 1 }))];
    let notifications = engine
        .consumers(&TestEvent::insert(table, row))
        .expect("dispatch succeeds");
    assert_eq!(
        notifications.inserted(),
        &[1],
        "the equal document is a match"
    );
}
