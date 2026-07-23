//! `Value::Bytes` bind through placeholder resolution, end to end.
//!
//! A `Value::Bytes` bound to a `$N` or `?` placeholder compiles and
//! matches exactly as if the caller had written the equivalent `X'...'`
//! hex literal inline, across the WHERE shapes placeholder resolution
//! supports (equality, ordered comparison, `IN` lists, and binds mixed
//! with other scalar types). Runs fully in process against the
//! parser-backed catalog and the `TestEvent` harness, so no database is
//! required.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
use subql::backend::{Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

type PgEngine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn pg_engine() -> (PgEngine, TableId) {
    let db =
        ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE t (id INT PRIMARY KEY, payload BYTEA);")
            .expect("parse DDL");
    let table_id = catalog_helpers::table_id(&db, "t").expect("table t exists");
    (SubscriptionEngine::new(db, PostgreSqlDialect {}), table_id)
}

fn pg_row(table_id: TableId, id: i64, payload: Vec<u8>) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(table_id, vec![Value::Int(id), Value::Bytes(payload)])
        .with_pk_columns([0u16])
}

fn matched(engine: &mut PgEngine, event: &TestEvent<Postgres>) -> Vec<u64> {
    engine
        .consumers(event)
        .expect("dispatch")
        .inserted()
        .to_vec()
}

/// The bound bytes reach the WHERE predicate: a row whose `payload` equals
/// the bind matches, a row with different bytes does not. Registration
/// itself succeeds, which is the regression this feature guards (the same
/// call returns `RegisterError::BindResolution` before the `Value::Bytes`
/// arm exists).
#[test]
fn bytes_bind_registers_and_matches() {
    let (mut engine, table_id) = pg_engine();
    let payload = vec![0xde, 0xad, 0xbe, 0xef];
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM t WHERE payload = $1")
                .binds(vec![Value::Bytes(payload.clone())]),
        )
        .expect("bytes bind registers");

    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 1, payload)),
        vec![1u64]
    );
    assert!(matched(&mut engine, &pg_row(table_id, 2, vec![0x00, 0x11])).is_empty());
}

/// An empty bytes bind (`X''`) matches an empty-payload row and nothing
/// else. Empty is the boundary the encode leg zero-pads to and the decode
/// leg reads back as `Value::Bytes(vec![])`.
#[test]
fn empty_bytes_bind_matches_empty_payload() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM t WHERE payload = $1")
                .binds(vec![Value::Bytes(vec![])]),
        )
        .expect("empty bytes bind registers");

    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 1, vec![])),
        vec![2u64]
    );
    assert!(matched(&mut engine, &pg_row(table_id, 2, vec![0x00])).is_empty());
}

/// Ordered comparison against a bytes bind follows `value_cmp`'s
/// lexicographic ordering: strictly-greater rows match, equal and lesser
/// rows do not, and a longer row that extends the bind's prefix is greater.
#[test]
fn bytes_bind_ordered_comparison_is_lexicographic() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(
            SubscriptionRequest::new(3u64, "SELECT * FROM t WHERE payload > $1")
                .binds(vec![Value::Bytes(vec![0x10, 0x20])]),
        )
        .expect("ordered bytes bind registers");

    // Greater by the second byte.
    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 1, vec![0x10, 0x21])),
        vec![3u64]
    );
    // Equal is not strictly greater.
    assert!(matched(&mut engine, &pg_row(table_id, 2, vec![0x10, 0x20])).is_empty());
    // Lesser by the second byte.
    assert!(matched(&mut engine, &pg_row(table_id, 3, vec![0x10, 0x1f])).is_empty());
    // A proper prefix-extension is lexicographically greater.
    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 4, vec![0x10, 0x20, 0x00])),
        vec![3u64]
    );
}

/// `IN (...)` over two bytes binds matches a row equal to either member and
/// rejects a row equal to neither.
#[test]
fn in_list_bytes_binds_match_any_member() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(
            SubscriptionRequest::new(4u64, "SELECT * FROM t WHERE payload IN ($1, $2)").binds(
                vec![Value::Bytes(vec![0xaa]), Value::Bytes(vec![0xbb, 0xcc])],
            ),
        )
        .expect("in-list bytes binds register");

    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 1, vec![0xaa])),
        vec![4u64]
    );
    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 2, vec![0xbb, 0xcc])),
        vec![4u64]
    );
    assert!(matched(&mut engine, &pg_row(table_id, 3, vec![0xdd])).is_empty());
}

/// A bytes bind in a non-first position resolves against the right index:
/// `id = $1 AND payload = $2` selects candidates via the indexed `id`
/// equality and the VM then applies the (unindexed) bytes equality.
#[test]
fn mixed_binds_resolve_bytes_in_second_position() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(
            SubscriptionRequest::new(5u64, "SELECT * FROM t WHERE id = $1 AND payload = $2")
                .binds(vec![Value::Int(5), Value::Bytes(vec![0x42])]),
        )
        .expect("mixed binds register");

    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 5, vec![0x42])),
        vec![5u64]
    );
    // Right id, wrong payload.
    assert!(matched(&mut engine, &pg_row(table_id, 5, vec![0x43])).is_empty());
    // Right payload, wrong id.
    assert!(matched(&mut engine, &pg_row(table_id, 6, vec![0x42])).is_empty());
}

/// An UPDATE moves a row into and out of a bytes-predicate view: a new
/// payload that matches while the old did not yields an insert, and the
/// reverse transition yields a delete. Exercises the dual-eval path that
/// reads both the old and new row images.
#[test]
fn update_event_moves_row_into_and_out_of_bytes_view() {
    let (mut engine, table_id) = pg_engine();
    let bind = vec![0xde, 0xad];
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM t WHERE payload = $1")
                .binds(vec![Value::Bytes(bind.clone())]),
        )
        .expect("register");

    // old payload != bind, new payload == bind -> row enters the view.
    let into = TestEvent::<Postgres>::update(
        table_id,
        vec![Value::Int(1), Value::Bytes(vec![0x00])],
        vec![Value::Int(1), Value::Bytes(bind.clone())],
    )
    .with_pk_columns([0u16])
    .with_changed_columns([1u16]);
    let entered = engine.consumers(&into).expect("dispatch update-in");
    assert_eq!(entered.inserted(), &[1u64]);
    assert!(entered.deleted().is_empty());
    assert!(entered.updated().is_empty());

    // old payload == bind, new payload != bind -> row leaves the view.
    let out = TestEvent::<Postgres>::update(
        table_id,
        vec![Value::Int(1), Value::Bytes(bind)],
        vec![Value::Int(1), Value::Bytes(vec![0x00])],
    )
    .with_pk_columns([0u16])
    .with_changed_columns([1u16]);
    let left = engine.consumers(&out).expect("dispatch update-out");
    assert_eq!(left.deleted(), &[1u64]);
    assert!(left.inserted().is_empty());
    assert!(left.updated().is_empty());
}

/// An UPDATE where both the old and new payloads satisfy an ordered bytes
/// predicate reports the subscriber under `updated` (the row stayed in the
/// view). Payload must change for the dependency-pruned UPDATE path to
/// re-evaluate the predicate, so both values exceed the bind.
#[test]
fn update_event_both_match_reports_updated() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM t WHERE payload > $1")
                .binds(vec![Value::Bytes(vec![0x20])]),
        )
        .expect("register");

    let ev = TestEvent::<Postgres>::update(
        table_id,
        vec![Value::Int(1), Value::Bytes(vec![0x30])],
        vec![Value::Int(1), Value::Bytes(vec![0x40])],
    )
    .with_pk_columns([0u16])
    .with_changed_columns([1u16]);
    let out = engine.consumers(&ev).expect("dispatch update");
    assert_eq!(out.updated(), &[2u64]);
    assert!(out.inserted().is_empty());
    assert!(out.deleted().is_empty());
}

/// A DELETE matches on the old row image: the subscriber whose bind equals
/// the deleted row's payload is reported under `deleted`, and a delete of a
/// non-matching row notifies nobody.
#[test]
fn delete_event_matches_on_old_row() {
    let (mut engine, table_id) = pg_engine();
    let bind = vec![0xca, 0xfe];
    engine
        .register(
            SubscriptionRequest::new(3u64, "SELECT * FROM t WHERE payload = $1")
                .binds(vec![Value::Bytes(bind.clone())]),
        )
        .expect("register");

    let hit = TestEvent::<Postgres>::delete(table_id, vec![Value::Int(1), Value::Bytes(bind)])
        .with_pk_columns([0u16]);
    assert_eq!(
        engine.consumers(&hit).expect("dispatch hit").deleted(),
        &[3u64],
    );

    let miss =
        TestEvent::<Postgres>::delete(table_id, vec![Value::Int(2), Value::Bytes(vec![0x00])])
            .with_pk_columns([0u16]);
    assert!(engine
        .consumers(&miss)
        .expect("dispatch miss")
        .deleted()
        .is_empty());
}

/// A bare `?` placeholder resolves the same bytes bind identically to `$1`.
/// SQLite uses `?` positional binds and `BLOB` columns, so exercise the
/// positional path there.
#[test]
fn positional_bytes_bind_matches_identically() {
    let db =
        ParserDB::parse::<SQLiteDialect>("CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB);")
            .expect("parse DDL");
    let table_id: TableId = catalog_helpers::table_id(&db, "t").expect("table t exists");
    let mut engine: SubscriptionEngine<TestEvent<SQLite>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, SQLiteDialect {});

    let payload = vec![0x01, 0x02, 0x03];
    engine
        .register(
            SubscriptionRequest::new(7u64, "SELECT * FROM t WHERE payload = ?")
                .binds(vec![Value::Bytes(payload.clone())]),
        )
        .expect("positional bytes bind registers");

    let hit = TestEvent::<SQLite>::insert(table_id, vec![Value::Int(1), Value::Bytes(payload)])
        .with_pk_columns([0u16]);
    assert_eq!(
        engine.consumers(&hit).expect("dispatch hit").inserted(),
        &[7u64],
    );

    let miss = TestEvent::<SQLite>::insert(table_id, vec![Value::Int(2), Value::Bytes(vec![0xff])])
        .with_pk_columns([0u16]);
    assert!(
        engine
            .consumers(&miss)
            .expect("dispatch miss")
            .inserted()
            .is_empty(),
        "different payload does not match through the positional bind",
    );
}

/// The bind path and the inline hex-literal path converge: a subscription
/// written as `payload = X'DEADBEEF'` and one written as `payload = $1`
/// bound to the same bytes both fire on the same row and both stay silent
/// on a non-match. This also guards the prefilter fix behaviorally, since
/// the inline literal was silently unmatchable before bytes comparisons
/// were routed to the scan set.
#[test]
fn inline_hex_literal_and_bind_match_identically() {
    let (mut engine, table_id) = pg_engine();
    let payload = vec![0xde, 0xad, 0xbe, 0xef];
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE payload = X'DEADBEEF'",
        ))
        .expect("inline literal registers");
    engine
        .register(
            SubscriptionRequest::new(2u64, "SELECT * FROM t WHERE payload = $1")
                .binds(vec![Value::Bytes(payload.clone())]),
        )
        .expect("bind registers");

    let mut hit = matched(&mut engine, &pg_row(table_id, 1, payload));
    hit.sort_unstable();
    assert_eq!(hit, vec![1u64, 2u64]);

    assert!(matched(&mut engine, &pg_row(table_id, 2, vec![0x00])).is_empty());
}

/// Inequality against a bytes bind matches the complement: rows whose
/// payload differs from the bind fire, the row equal to it does not.
#[test]
fn not_equal_bytes_bind_matches_complement() {
    let (mut engine, table_id) = pg_engine();
    engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT * FROM t WHERE payload <> $1")
                .binds(vec![Value::Bytes(vec![0xaa])]),
        )
        .expect("register");

    assert_eq!(
        matched(&mut engine, &pg_row(table_id, 1, vec![0xbb])),
        vec![1u64]
    );
    assert!(matched(&mut engine, &pg_row(table_id, 2, vec![0xaa])).is_empty());
}
