//! subql holds the running total of an aggregate subscription.
//!
//! Pins the contract step 4 of `plans/grouped-aggregate-half.md` introduces.
//! The engine owns the value, reports it per registration rather than per
//! consumer, and refuses a set of starting numbers it cannot line up against
//! the changes it has already folded. The window those refusals exist for is
//! real: the caller learns the seed query from the registration, so its read
//! necessarily happens after the engine started counting, and a change
//! committed inside that window is in both the read and the fold.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateInstallError, AggregateValueUpdate, DefaultIds,
    NumericValue, PgLsn, SubscriptionEngine, SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id(&database, "orders").unwrap();
    (
        SubscriptionEngine::new(database, PostgreSqlDialect {}),
        orders,
    )
}

/// A paid order arriving at stream position `lsn`.
fn paid(orders: TableId, id: i64, amount: i64, lsn: u64) -> Event {
    TestEvent::insert(
        orders,
        vec![
            Value::Int(id),
            Value::Int(amount),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(lsn))
}

/// The same order leaving the table at stream position `lsn`.
fn unpaid_delete(orders: TableId, id: i64, amount: i64, lsn: u64) -> Event {
    TestEvent::delete(
        orders,
        vec![
            Value::Int(id),
            Value::Int(amount),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(lsn))
}

/// Reports as comparable tuples, ordered by subscription so a hash map's
/// iteration order cannot decide whether the test passes.
fn reported(updates: &[AggregateValueUpdate<DefaultIds>]) -> Vec<(u64, u64, AggValue)> {
    let mut out: Vec<(u64, u64, AggValue)> = updates
        .iter()
        .map(|u| {
            let subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(value)) =
                &u.change
            else {
                panic!("ungrouped aggregate cannot remove a group")
            };
            (u.subscription, u.consumer, value.clone())
        })
        .collect();
    out.sort_by_key(|(sub, consumer, _)| (*sub, *consumer));
    out
}

fn installed_value(updates: &[AggregateValueUpdate<DefaultIds>]) -> AggValue {
    assert_eq!(updates.len(), 1, "ungrouped install reports one value");
    assert_eq!(updates[0].group, None);
    updates[0]
        .folded_value()
        .expect("ungrouped install sets a value")
}

fn install_seed(
    engine: &mut Engine,
    subscription: u64,
    row: Vec<Value<Postgres>>,
    read_at: Option<PgLsn>,
) -> Result<AggValue, AggregateInstallError> {
    subql::Install::install(
        engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![row],
            read_at,
        },
    )
    .map(|updates| installed_value(&updates))
}

#[test]
fn an_unseeded_total_reports_nothing_though_the_event_matched() {
    let (mut engine, orders) = engine();
    engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap();

    let updates = engine.aggregate_updates(&paid(orders, 1, 250, 10)).unwrap();
    assert!(
        updates.is_empty(),
        "a total covering only the last few seconds is worse than silence, got {updates:?}",
    );
}

#[test]
fn a_change_already_in_the_starting_numbers_is_not_counted_twice() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    // Inside the read's window: the caller's snapshot sees this row, so its
    // count of 1 already includes it.
    engine.aggregate_updates(&paid(orders, 1, 250, 10)).unwrap();
    // After the read: the snapshot cannot see this one.
    engine.aggregate_updates(&paid(orders, 2, 250, 30)).unwrap();

    let value = install_seed(&mut engine, sub, vec![Value::Int(1)], Some(PgLsn(20))).unwrap();
    assert_eq!(
        value,
        AggValue::Count(2),
        "the row at position 10 is in the seed and must not be added a second time",
    );
}

#[test]
fn a_change_at_the_read_position_belongs_to_the_starting_numbers() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    engine.aggregate_updates(&paid(orders, 1, 250, 20)).unwrap();

    // The position is taken before the read's snapshot opens, so everything at
    // or before it is in the numbers the read returns.
    let value = install_seed(&mut engine, sub, vec![Value::Int(1)], Some(PgLsn(20))).unwrap();
    assert_eq!(value, AggValue::Count(1));
}

#[test]
fn two_counts_under_one_consumer_report_separately_rather_than_merged() {
    let (mut engine, orders) = engine();
    let by_status = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;
    let by_amount = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE amount > 100",
        ))
        .unwrap()
        .subscription_id;
    assert_ne!(by_status, by_amount);

    for sub in [by_status, by_amount] {
        install_seed(&mut engine, sub, vec![Value::Int(0)], Some(PgLsn(5))).unwrap();
    }

    // One row matching both. Merging the two totals under the consumer id
    // would report 2 for each of them.
    let updates = engine.aggregate_updates(&paid(orders, 1, 250, 10)).unwrap();
    assert_eq!(
        reported(&updates),
        vec![
            (by_status, 7, AggValue::Count(1)),
            (by_amount, 7, AggValue::Count(1)),
        ],
    );
}

#[test]
fn a_read_that_raced_is_refused_when_a_change_carries_no_position() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    let positionless = TestEvent::insert(
        orders,
        vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
    )
    .with_pk_columns([0u16]);
    engine.aggregate_updates(&positionless).unwrap();

    assert_eq!(
        install_seed(&mut engine, sub, vec![Value::Int(1)], Some(PgLsn(20))),
        Err(AggregateInstallError::PositionUnknown(sub)),
        "a change with no position cannot be told apart from one the read already saw",
    );
}

#[test]
fn a_read_that_raced_is_refused_when_the_install_names_no_position() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    engine.aggregate_updates(&paid(orders, 1, 250, 10)).unwrap();

    assert_eq!(
        install_seed(&mut engine, sub, vec![Value::Int(1)], None),
        Err(AggregateInstallError::PositionUnknown(sub)),
    );
}

#[test]
fn a_quiet_read_is_installed_without_any_position() {
    let (mut engine, _orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    assert_eq!(
        install_seed(&mut engine, sub, vec![Value::Int(5)], None),
        Ok(AggValue::Count(5)),
        "nothing was folded, so there is nothing to line the numbers up against",
    );
}

#[test]
fn more_changes_during_the_read_than_the_cap_refuse_the_seed() {
    let (database, orders) = {
        let database = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
        let orders = catalog_helpers::table_id(&database, "orders").unwrap();
        (database, orders)
    };
    let mut engine: Engine = SubscriptionEngine::new(database, PostgreSqlDialect {})
        .with_max_changes_during_aggregate_read(2);
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    for (id, lsn) in [(1, 10), (2, 20), (3, 30)] {
        engine
            .aggregate_updates(&paid(orders, id, 250, lsn))
            .unwrap();
    }

    assert_eq!(
        install_seed(&mut engine, sub, vec![Value::Int(0)], Some(PgLsn(5))),
        Err(AggregateInstallError::TooManyChangesDuringRead {
            subscription: sub,
            cap: 2,
        },),
    );
}

#[test]
fn a_truncate_zeroes_a_held_total_and_reports_it() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;
    install_seed(&mut engine, sub, vec![Value::Int(5)], None).unwrap();

    let updates = engine
        .aggregate_updates(&TestEvent::truncate(orders).with_checkpoint(PgLsn(40)))
        .unwrap();
    assert_eq!(
        reported(&updates),
        vec![(sub, 7, AggValue::Count(0))],
        "the table is empty afterwards, so the answer is known without a re-read",
    );
}

#[test]
fn a_truncate_during_the_read_supersedes_the_starting_numbers() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;

    engine.aggregate_updates(&paid(orders, 1, 250, 10)).unwrap();
    engine
        .aggregate_updates(&TestEvent::truncate(orders).with_checkpoint(PgLsn(20)))
        .unwrap();
    engine.aggregate_updates(&paid(orders, 2, 250, 30)).unwrap();

    // The read's snapshot saw the first row and not the truncate.
    let value = install_seed(&mut engine, sub, vec![Value::Int(1)], Some(PgLsn(15))).unwrap();
    assert_eq!(
        value,
        AggValue::Count(1),
        "the truncate wipes the starting numbers it followed, leaving only the row after it",
    );
}

#[test]
fn a_reset_zeroes_the_total_and_reports_nothing_until_the_next_seed() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;
    install_seed(&mut engine, sub, vec![Value::Int(5)], None).unwrap();

    assert!(engine.reset_aggregate_value(sub));

    let updates = engine.aggregate_updates(&paid(orders, 1, 250, 40)).unwrap();
    assert!(
        updates.is_empty(),
        "a reset subscription is unseeded again, so it says nothing",
    );

    let value = install_seed(&mut engine, sub, vec![Value::Int(0)], Some(PgLsn(35))).unwrap();
    assert_eq!(value, AggValue::Count(1));
}

#[test]
fn unregistering_drops_the_total() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT COUNT(*) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;
    install_seed(&mut engine, sub, vec![Value::Int(5)], None).unwrap();

    assert!(engine.unregister_subscription(sub));

    assert!(engine
        .aggregate_updates(&paid(orders, 1, 250, 40))
        .unwrap()
        .is_empty());
    assert_eq!(
        install_seed(&mut engine, sub, vec![Value::Int(0)], None),
        Err(AggregateInstallError::UnknownAggregate(sub)),
    );
}

/// An idempotent write, where the application sends every column back
/// unchanged, reaches the engine as an UPDATE naming the summed column. Both
/// images contribute, so the two halves cancel and the value did not move.
#[test]
fn a_change_that_nets_to_zero_reports_nothing() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;
    install_seed(&mut engine, sub, vec![Value::Int(250)], None).unwrap();

    let row = vec![Value::Int(1), Value::Int(250), Value::String("paid".into())];
    let rewrite = TestEvent::update(orders, row.clone(), row)
        .with_pk_columns([0u16])
        .with_changed_columns([1u16])
        .with_checkpoint(PgLsn(40));

    assert!(engine.aggregate_updates(&rewrite).unwrap().is_empty());
}

#[test]
fn a_seeded_sum_follows_inserts_updates_and_deletes() {
    let (mut engine, orders) = engine();
    let sub = engine
        .register(SubscriptionRequest::new(
            7,
            "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
        ))
        .unwrap()
        .subscription_id;
    // An empty table: a NULL sum over no contributing rows.
    install_seed(&mut engine, sub, vec![Value::Null, Value::Int(0)], None).unwrap();

    assert_eq!(
        reported(&engine.aggregate_updates(&paid(orders, 1, 100, 10)).unwrap()),
        vec![(sub, 7, AggValue::Sum(Some(NumericValue::Integer(100))))],
    );

    let raise = TestEvent::update(
        orders,
        vec![Value::Int(1), Value::Int(100), Value::String("paid".into())],
        vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
    )
    .with_pk_columns([0u16])
    .with_changed_columns([1u16])
    .with_checkpoint(PgLsn(20));
    assert_eq!(
        reported(&engine.aggregate_updates(&raise).unwrap()),
        vec![(sub, 7, AggValue::Sum(Some(NumericValue::Integer(250))))],
    );

    assert_eq!(
        reported(
            &engine
                .aggregate_updates(&unpaid_delete(orders, 1, 250, 30))
                .unwrap()
        ),
        vec![(sub, 7, AggValue::Sum(None))],
        "the only contributing row left, so the sum is NULL again rather than zero",
    );
}
