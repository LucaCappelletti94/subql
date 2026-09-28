//! A filter the engine refuses to evaluate for one row, on the tiers that
//! keep an extreme. The refusal cannot say whether the row belongs, so the
//! tier reads the database rather than taking the row as excluded.

#![allow(clippy::unwrap_used)]

use super::fixtures::*;
use super::*;

/// `id / quantity` divides by zero when `quantity` is 0, which PostgreSQL
/// refuses.
const REFUSING_FILTER: &str = "WHERE (id / quantity) > 0";

fn row(id: i64, price: f64, quantity: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(quantity),
        Value::String("paid".into()),
    ]
}

fn grouped_minimum(
    engine: &mut AutoResolvingEngine<
        TestEvent<Postgres>,
        DefaultIds,
        ParserDB,
        SyncMode<MockConnector>,
    >,
) {
    let subscription = engine
        .register(
            SubscriptionRequest::new(
                1u64,
                format!("SELECT status, MIN(price) FROM orders {REFUSING_FILTER} GROUP BY status"),
            ),
            (),
        )
        .expect("grouped minimum registers")
        .subscription_id;
    crate::Install::install(
        &mut engine.inner,
        subscription,
        crate::GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("paid".into()),
                Value::Float(5.0),
                Value::Int(1),
            ]],
            fence: None,
        },
    )
    .expect("group map installs");
}

#[test]
fn a_refused_row_arriving_reads_its_group() {
    let (mut engine, table) = engine_with_values(Vec::new());
    grouped_minimum(&mut engine);
    engine
        .apply_leaving_reads_queued(
            &TestEvent::<Postgres>::insert(table, row(3, 1.0, 0)).with_pk_columns([0u16]),
        )
        .expect("the insert dispatches");
    assert_eq!(engine.pending_read_count(), 1, "the group is read");
}

#[test]
fn a_refused_row_leaving_reads_its_group() {
    let (mut engine, table) = engine_with_values(Vec::new());
    grouped_minimum(&mut engine);
    engine
        .apply_leaving_reads_queued(
            &TestEvent::<Postgres>::delete(table, row(1, 5.0, 0)).with_pk_columns([0u16]),
        )
        .expect("the delete dispatches");
    assert_eq!(engine.pending_read_count(), 1, "the group is read");
}

#[test]
fn a_refused_row_arriving_reads_the_minimum() {
    let (mut engine, table) = engine_with_values(alloc::vec![Value::Float(1.0)]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut engine,
        1u64,
        &format!("SELECT MIN(price) FROM orders {REFUSING_FILTER}"),
        5.0,
    );
    engine
        .apply_leaving_reads_queued(
            &TestEvent::<Postgres>::insert(table, row(3, 1.0, 0)).with_pk_columns([0u16]),
        )
        .expect("the insert dispatches");
    assert_eq!(engine.pending_read_count(), 1, "the minimum is read");
}

#[test]
fn a_refused_row_leaving_reads_the_minimum() {
    let (mut engine, table) = engine_with_values(alloc::vec![Value::Float(7.0)]);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut engine,
        1u64,
        &format!("SELECT MIN(price) FROM orders {REFUSING_FILTER}"),
        5.0,
    );
    engine
        .apply_leaving_reads_queued(
            &TestEvent::<Postgres>::delete(table, row(1, 5.0, 0)).with_pk_columns([0u16]),
        )
        .expect("the delete dispatches");
    assert_eq!(engine.pending_read_count(), 1, "the minimum is read");
}

/// A row the filter answers false for is excluded without a read, so the
/// read above is the refusal's and not every row's.
#[test]
fn a_row_the_filter_excludes_reads_nothing() {
    let (mut engine, table) = engine_with_values(Vec::new());
    grouped_minimum(&mut engine);
    crate::reexec::test_fixtures::bootstrap_scalar_query(
        &mut engine,
        2u64,
        &format!("SELECT MIN(price) FROM orders {REFUSING_FILTER}"),
        5.0,
    );
    engine
        .apply_leaving_reads_queued(
            &TestEvent::<Postgres>::insert(table, row(3, 1.0, -1)).with_pk_columns([0u16]),
        )
        .expect("the insert dispatches");
    engine
        .apply_leaving_reads_queued(
            &TestEvent::<Postgres>::delete(table, row(1, 5.0, -1)).with_pk_columns([0u16]),
        )
        .expect("the delete dispatches");
    assert_eq!(engine.pending_read_count(), 0);
}
