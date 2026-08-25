//! Grouped aggregates keep one running value per encoded group, remove a group
//! when its source-row count reaches zero, and install every seed row at once.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateSeedInstall, AggregateValueChange, DefaultIds, Install,
    PgLsn, SubscriptionEngine, SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        table,
    )
}

fn register(engine: &mut Engine) -> u64 {
    engine
        .register(SubscriptionRequest::new(
            7u64,
            "SELECT region, COUNT(*) FROM orders GROUP BY region",
        ))
        .expect("grouped count registers")
        .subscription_id
}

fn row(id: i64, region: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(region.into()),
        Value::Int(1),
        Value::String("paid".into()),
    ]
}

fn insert(table: TableId, id: i64, region: &str, lsn: u64) -> Event {
    TestEvent::insert(table, row(id, region))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn delete(table: TableId, id: i64, region: &str, lsn: u64) -> Event {
    TestEvent::delete(table, row(id, region))
        .with_pk_columns([0u16])
        .with_checkpoint(PgLsn(lsn))
}

fn move_group(table: TableId, id: i64, from: &str, to: &str, lsn: u64) -> Event {
    TestEvent::update(table, row(id, from), row(id, to))
        .with_pk_columns([0u16])
        .with_changed_columns([1u16])
        .with_checkpoint(PgLsn(lsn))
}

fn seed(
    engine: &mut Engine,
    subscription: u64,
    rows: Vec<Vec<Value<Postgres>>>,
    read_at: Option<PgLsn>,
) -> Vec<subql::AggregateValueUpdate<DefaultIds>> {
    Install::install(engine, subscription, AggregateSeedInstall { rows, read_at })
        .expect("all grouped seed rows install together")
        .updates
}

fn one(
    updates: &[subql::AggregateValueUpdate<DefaultIds>],
) -> &subql::AggregateValueUpdate<DefaultIds> {
    assert_eq!(
        updates.len(),
        1,
        "expected one changed group, got {updates:?}"
    );
    &updates[0]
}

#[test]
fn a_group_that_empties_is_removed_and_comes_back_under_the_same_key() {
    let (mut engine, orders) = engine();
    let subscription = register(&mut engine);
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(1),
            Value::Int(1),
        ]],
        None,
    );
    let north = one(&opening)
        .group
        .clone()
        .expect("grouped update carries a key");
    assert_eq!(
        one(&opening).change,
        AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(1)))
    );

    let removed = engine
        .aggregate_updates(&delete(orders, 1, "north", 10))
        .expect("delete folds");
    assert_eq!(one(&removed).group.as_deref(), Some(north.as_slice()));
    assert_eq!(one(&removed).change, AggregateValueChange::Remove);

    let returned = engine
        .aggregate_updates(&insert(orders, 2, "north", 20))
        .expect("insert folds");
    assert_eq!(one(&returned).group.as_deref(), Some(north.as_slice()));
    assert_eq!(
        one(&returned).change,
        AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(1)))
    );
}

#[test]
fn moving_a_row_between_groups_emits_remove_and_set() {
    let (mut engine, orders) = engine();
    let subscription = register(&mut engine);
    let opening = seed(
        &mut engine,
        subscription,
        vec![
            vec![Value::String("north".into()), Value::Int(1), Value::Int(1)],
            vec![Value::String("south".into()), Value::Int(2), Value::Int(2)],
        ],
        None,
    );
    let north = opening
        .iter()
        .find(|update| {
            update.change
                == AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(
                    1,
                )))
        })
        .and_then(|update| update.group.clone())
        .expect("north key");
    let south = opening
        .iter()
        .find(|update| {
            update.change
                == AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(
                    2,
                )))
        })
        .and_then(|update| update.group.clone())
        .expect("south key");

    let moved = engine
        .aggregate_updates(&move_group(orders, 1, "north", "south", 30))
        .expect("update folds both row images");
    assert_eq!(moved.len(), 2, "one group loses the row and one gains it");
    assert!(moved.iter().any(|update| {
        update.group.as_deref() == Some(north.as_slice())
            && update.change == AggregateValueChange::Remove
    }));
    assert!(moved.iter().any(|update| {
        update.group.as_deref() == Some(south.as_slice())
            && update.change
                == AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(
                    3,
                )))
    }));
}

#[test]
fn empty_and_refill_during_the_seed_read_uses_position_and_row_count_together() {
    let (mut engine, orders) = engine();
    let subscription = register(&mut engine);

    // The read is taken at 25. It already contains the south insert at 20 and
    // sees north empty after its delete at 21. North is refilled at 30, after
    // the read. Replaying the already-read south insert would make its count
    // two, so this checks the position comparison and row-count path together.
    assert!(engine
        .aggregate_updates(&insert(orders, 3, "south", 20))
        .expect("south insert queues")
        .is_empty());
    assert!(engine
        .aggregate_updates(&delete(orders, 1, "north", 21))
        .expect("north delete queues")
        .is_empty());
    assert!(engine
        .aggregate_updates(&insert(orders, 2, "north", 30))
        .expect("north insert queues")
        .is_empty());

    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("south".into()),
            Value::Int(1),
            Value::Int(1),
        ]],
        Some(PgLsn(25)),
    );
    assert_eq!(opening.len(), 2, "south from the read, north after it");
    assert!(
        opening.iter().all(|update| {
            update.change
                == AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(
                    1,
                )))
        }),
        "an already-read insert must not be counted twice: {opening:?}"
    );

    let removed = engine
        .aggregate_updates(&delete(orders, 2, "north", 40))
        .expect("delete folds");
    assert_eq!(one(&removed).change, AggregateValueChange::Remove);
}

#[test]
fn an_all_null_group_uses_source_row_count_not_aggregate_value() {
    let (mut engine, orders) = engine();
    let subscription = engine
        .register(SubscriptionRequest::new(
            8u64,
            "SELECT region, COUNT(amount) FROM orders GROUP BY region",
        ))
        .expect("grouped count-column registers")
        .subscription_id;
    let opening = seed(
        &mut engine,
        subscription,
        vec![vec![
            Value::String("north".into()),
            Value::Int(0),
            Value::Int(1),
        ]],
        None,
    );
    assert_eq!(
        one(&opening).change,
        AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(0))),
        "the group exists even though no aggregate cell contributes"
    );

    let removed = TestEvent::delete(
        orders,
        vec![
            Value::Int(1),
            Value::String("north".into()),
            Value::Null,
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(10));
    let updates = engine.aggregate_updates(&removed).expect("delete folds");
    assert_eq!(one(&updates).change, AggregateValueChange::Remove);
}
