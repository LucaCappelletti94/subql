//! A `MIN` or `MAX` reads the database exactly when a cell the event omits could change its answer.

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, GroupedScalarSeedInstall, Install, PgLsn, ScalarInstall,
    SubscriptionEngine, SubscriptionRequest, TableId,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, region TEXT, amount INT, status TEXT);";

type Event = TestEvent<Postgres, PgLsn>;
type Engine = SubscriptionEngine<Event, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let orders =
        catalog_helpers::table_id::<Postgres, _>(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        orders,
    )
}

/// An ungrouped extreme over `filter`, already read as 4.
fn scalar(filter: &str) -> (Engine, TableId) {
    let (mut engine, orders) = engine();
    let subscription = engine
        .register(SubscriptionRequest::new(
            7u64,
            format!("SELECT MIN(amount) FROM orders WHERE {filter}"),
        ))
        .expect("the extreme registers")
        .subscription_id;
    Install::install(
        &mut engine,
        subscription,
        ScalarInstall {
            value: Value::Int(4),
            checkpoint: None,
            fence: None,
        },
    )
    .expect("the read installs");
    (engine, orders)
}

/// An extreme grouped by region over `filter`, with north already read as 500.
fn grouped(filter: &str) -> (Engine, TableId) {
    let (mut engine, orders) = engine();
    let subscription = engine
        .register(SubscriptionRequest::new(
            7u64,
            format!("SELECT region, MIN(amount) FROM orders WHERE {filter} GROUP BY region"),
        ))
        .expect("the grouped extreme registers")
        .subscription_id;
    Install::install(
        &mut engine,
        subscription,
        GroupedScalarSeedInstall {
            rows: vec![vec![
                Value::String("north".into()),
                Value::Int(500),
                Value::Int(1),
            ]],
            fence: None,
        },
    )
    .expect("the group seed installs");
    (engine, orders)
}

/// An `orders` row in north.
fn row(id: i64, amount: Value<Postgres>, status: Value<Postgres>) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String("north".into()),
        amount,
        status,
    ]
}

fn paid() -> Value<Postgres> {
    Value::String("paid".into())
}

fn reads(engine: &mut Engine, event: Event) -> usize {
    engine
        .dispatch(&event.with_pk_columns([0u16]).with_checkpoint(PgLsn(10)))
        .expect("dispatch runs")
        .triggers()
        .len()
}

/// An unchanged TOASTed `status` arrives as no cell, and the row may still be paid and now the smallest.
#[test]
fn an_update_omitting_the_filter_cell_reads_the_extreme() {
    let (mut engine, orders) = scalar("status = 'paid'");
    let update = Event::update(
        orders,
        row(11, Value::Int(9), paid()),
        row(11, Value::Int(1), Value::Missing),
    );
    assert_eq!(reads(&mut engine, update), 1);
}

#[test]
fn an_update_omitting_the_aggregated_cell_reads_the_extreme() {
    let (mut engine, orders) = scalar("status = 'paid'");
    let update = Event::update(
        orders,
        row(11, Value::Int(9), paid()),
        row(11, Value::Missing, paid()),
    );
    assert_eq!(reads(&mut engine, update), 1);
}

#[test]
fn an_omitted_cell_another_conjunct_excludes_costs_no_read() {
    let (mut engine, orders) = scalar("status = 'paid' AND id > 100");
    let removed = Event::delete(orders, row(1, Value::Int(4), Value::Missing));
    assert_eq!(reads(&mut engine, removed), 0);
}

#[test]
fn an_omitted_cell_that_may_admit_the_removed_extreme_reads_it() {
    let (mut engine, orders) = scalar("status = 'paid' AND id > 100");
    let removed = Event::delete(orders, row(101, Value::Int(4), Value::Missing));
    assert_eq!(reads(&mut engine, removed), 1);
}

#[test]
fn an_omitted_cell_another_conjunct_excludes_costs_no_group_read() {
    let (mut engine, orders) = grouped("status = 'paid' AND id > 100");
    let inserted = Event::insert(orders, row(1, Value::Int(4), Value::Missing));
    assert_eq!(reads(&mut engine, inserted), 0);
}

#[test]
fn an_omitted_cell_that_may_admit_the_row_reads_its_group() {
    let (mut engine, orders) = grouped("status = 'paid' AND id > 100");
    let inserted = Event::insert(orders, row(101, Value::Int(4), Value::Missing));
    assert_eq!(reads(&mut engine, inserted), 1);
}
