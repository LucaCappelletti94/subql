//! An update whose old row the stream does not fully carry.
//!
//! PostgreSQL under its default replica identity sends no old row, and
//! Maxwell sends the old values of the changed columns only. A subscriber of
//! `a = 0` holding the row has to learn when `a` changed, so an old cell the
//! filter reads that the event does not carry, for a column that changed,
//! leaves the subscriber unanswered, whether or not the new row matches. A column the event lists as unchanged
//! holds its new value in the old row too, so it costs nothing.
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, ConsumerNotifications, DefaultIds, SubscriptionEngine, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)";

fn row(id: i64, a: Value<Postgres>, b: Value<Postgres>) -> Vec<Value<Postgres>> {
    vec![Value::Int(id), a, b]
}

/// Register `a = 0` and apply one update of `old` to `new`, `changed` naming
/// the columns the event lists as changed.
fn update(
    old: Vec<Value<Postgres>>,
    new: Vec<Value<Postgres>>,
    changed: &[u16],
) -> ConsumerNotifications<DefaultIds, subql::NoCheckpoint, Postgres> {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id::<Postgres, _>(&database, "t").expect("t resolves");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM t WHERE a = 0",
        ))
        .expect("the filter registers");
    assert!(
        registered.not_served_because.is_none(),
        "the filter is served in process"
    );
    engine
        .consumers(
            &TestEvent::update(table, old, new)
                .with_pk_columns([0u16])
                .with_changed_columns(changed.iter().copied()),
        )
        .expect("dispatch succeeds")
}

fn unanswered(
    notifications: &ConsumerNotifications<DefaultIds, subql::NoCheckpoint, Postgres>,
) -> Vec<u64> {
    notifications
        .unanswered()
        .iter()
        .map(|cell| cell.consumer_id)
        .collect()
}

/// No old row, and `a` changed to a value the filter refuses. The subscriber
/// may hold the row, so it is told the event could not answer it.
#[test]
fn a_changed_column_missing_from_the_old_row_leaves_the_subscriber_unanswered() {
    let missing = || vec![Value::Missing; 3];
    let notifications = update(missing(), row(5, Value::Null, Value::Int(2)), &[1, 2]);
    assert_eq!(unanswered(&notifications), vec![1]);
    assert!(notifications.deleted().is_empty() && notifications.inserted().is_empty());
}

/// No old row, and `a` changed to a value the filter keeps. The subscriber
/// may hold the old version, which it cannot remove without knowing it, so
/// it is told the event could not answer it rather than handed a second row.
#[test]
fn a_changed_column_missing_from_the_old_row_leaves_a_still_matching_subscriber_unanswered() {
    let notifications = update(
        vec![Value::Missing; 3],
        row(5, Value::Int(0), Value::Int(2)),
        &[1, 2],
    );
    assert_eq!(unanswered(&notifications), vec![1]);
    assert_eq!(notifications.inserted(), &[] as &[u64]);
    assert_eq!(notifications.updated(), &[] as &[u64]);
}

/// Maxwell's old row carries `b` alone, and `a` did not change and does not
/// match. The row was never in the answer, so nothing is reported and no read
/// is asked for.
#[test]
fn an_unchanged_column_missing_from_the_old_row_costs_nothing() {
    let notifications = update(
        row(5, Value::Missing, Value::Int(1)),
        row(5, Value::Int(7), Value::Int(2)),
        &[2],
    );
    assert_eq!(unanswered(&notifications), [] as [u64; 0]);
    assert_eq!(notifications.inserted(), &[] as &[u64]);
    assert_eq!(notifications.deleted(), &[] as &[u64]);
    assert_eq!(notifications.updated(), &[] as &[u64]);
}

/// The same update over a row the filter keeps. `a` holds its new value in
/// the old row too, so the row was already in the answer and is updated.
#[test]
fn an_unchanged_matching_row_is_updated_rather_than_inserted() {
    let notifications = update(
        row(5, Value::Missing, Value::Int(1)),
        row(5, Value::Int(0), Value::Int(2)),
        &[2],
    );
    assert_eq!(notifications.updated(), [1]);
    assert_eq!(notifications.inserted(), &[] as &[u64]);
    assert_eq!(unanswered(&notifications), [] as [u64; 0]);
}
