//! `Install<T>` uses concrete Rust input structs, and the registry dispatch
//! output carries every database read an event requires.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarKind, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, AggregateSeedInstall, DefaultIds, Install, InstallError,
    InstalledPage, InstalledRowDelta, KeyedRowsInstall, ScalarInstall, SubscriptionEngine,
    SubscriptionRequest, TableId, Tier, WholeRowsInstall,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE managers (id INT PRIMARY KEY);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let table = catalog_helpers::table_id(&catalog, "orders").expect("orders resolves");
    (
        SubscriptionEngine::new(catalog, PostgreSqlDialect {}),
        table,
    )
}

fn register(engine: &mut Engine, consumer: u64, sql: &str) -> subql::Registered {
    engine
        .register(SubscriptionRequest::new(consumer, sql))
        .unwrap_or_else(|e| panic!("`{sql}` registers: {e:?}"))
}

#[test]
fn scalar_install_returns_the_typed_scalar_update() {
    let (mut engine, _) = engine();
    let registered = register(&mut engine, 7, "SELECT MIN(price) FROM orders");
    assert!(matches!(
        registered.tier,
        Tier::Scalar {
            column_kind: ScalarKind::Float,
            ..
        }
    ));

    let update = Install::install(
        &mut engine,
        registered.subscription_id,
        ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<subql::NoCheckpoint>,
        },
    )
    .expect("the scalar result matches the scalar tier");

    assert_eq!(update.subscription_id, registered.subscription_id);
    assert_eq!(update.consumer_id, 7);
    assert_eq!(update.value, Value::Float(5.0));
}

#[test]
fn whole_rows_install_adds_the_registered_identity_to_each_page() {
    let (mut engine, _) = engine();
    let registered = register(
        &mut engine,
        8,
        "SELECT * FROM orders WHERE lower(status) = 'paid' \
         AND id IN (SELECT id FROM managers)",
    );
    assert!(matches!(registered.tier, Tier::WholeRows { .. }));

    let updates = Install::install(
        &mut engine,
        registered.subscription_id,
        WholeRowsInstall {
            generation: 3,
            pages: vec![InstalledPage {
                columns: vec!["id".into(), "price".into(), "status".into()],
                rows: vec![vec![
                    Value::Int(1),
                    Value::Float(5.0),
                    Value::String("paid".into()),
                ]],
                more: false,
                checkpoint: None::<subql::NoCheckpoint>,
            }],
        },
    )
    .expect("the full row result matches the whole-row tier");

    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, registered.subscription_id);
    assert_eq!(updates[0].consumer_id, 8);
    assert_eq!(updates[0].generation, 3);
}

#[test]
fn row_delta_install_adds_the_registered_identity_and_consumer() {
    let (mut engine, _) = engine();
    let registered = register(
        &mut engine,
        9,
        "SELECT * FROM orders WHERE lower(status) = 'paid'",
    );
    assert!(matches!(registered.tier, Tier::KeyedRows { .. }));

    let deltas = Install::install(
        &mut engine,
        registered.subscription_id,
        KeyedRowsInstall {
            columns: vec!["id".into(), "price".into(), "status".into()],
            deltas: vec![InstalledRowDelta {
                key: vec![Value::Int(1)],
                row: Some(vec![
                    Value::Int(1),
                    Value::Float(5.0),
                    Value::String("paid".into()),
                ]),
                checkpoint: None::<subql::NoCheckpoint>,
            }],
        },
    )
    .expect("the keyed result matches the keyed tier");

    assert_eq!(deltas[0].subscription_id, registered.subscription_id);
    assert_eq!(deltas[0].consumer_id, 9);
    assert_eq!(deltas[0].columns, vec!["id", "price", "status"]);
}

#[test]
fn aggregate_seed_install_keeps_its_own_output_and_error_type() {
    let (mut engine, _) = engine();
    let registered = register(&mut engine, 10, "SELECT COUNT(*) FROM orders");
    assert!(matches!(registered.tier, Tier::InProcess(_)));

    let updates = Install::install(
        &mut engine,
        registered.subscription_id,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(2)]],
            read_at: None,
        },
    )
    .expect("one ungrouped seed row installs");
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].change,
        subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(2),))
    );
}

#[test]
fn a_concrete_input_for_the_wrong_tier_is_rejected() {
    let (mut engine, _) = engine();
    let registered = register(&mut engine, 11, "SELECT MIN(price) FROM orders");

    let wrong = Install::install(
        &mut engine,
        registered.subscription_id,
        WholeRowsInstall::<Postgres> {
            generation: 1,
            pages: Vec::new(),
        },
    );
    assert!(matches!(
        wrong,
        Err(InstallError::WrongTier {
            subscription,
            input: "WholeRowsInstall",
        }) if subscription == registered.subscription_id
    ));
}

fn insert(table: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(
        table,
        vec![
            Value::Int(id),
            Value::Float(price),
            Value::String("paid".into()),
        ],
    )
    .with_pk_columns([0u16])
}

#[test]
fn an_unseeded_aggregate_is_a_registry_trigger() {
    let (mut engine, table) = engine();
    let registered = register(&mut engine, 12, "SELECT COUNT(*) FROM orders");

    let output = engine.dispatch(&insert(table, 1, 5.0)).expect("dispatch");

    assert!(output.aggregate_updates().is_empty());
    assert_eq!(
        output
            .triggers()
            .iter()
            .map(|trigger| trigger.subscription_id)
            .collect::<Vec<_>>(),
        vec![registered.subscription_id]
    );
}

#[test]
fn a_seeded_aggregate_updates_without_a_trigger() {
    let (mut engine, table) = engine();
    let registered = register(&mut engine, 13, "SELECT COUNT(*) FROM orders");
    Install::install(
        &mut engine,
        registered.subscription_id,
        AggregateSeedInstall {
            rows: vec![vec![Value::Int(0)]],
            read_at: None,
        },
    )
    .expect("seed");

    let output = engine.dispatch(&insert(table, 1, 5.0)).expect("dispatch");

    assert!(output.triggers().is_empty());
    assert_eq!(
        output.aggregate_updates()[0].change,
        subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Count(1),))
    );
}

#[test]
fn an_unknown_scalar_is_a_registry_trigger() {
    let (mut engine, table) = engine();
    let registered = register(&mut engine, 14, "SELECT MIN(price) FROM orders");

    let output = engine.dispatch(&insert(table, 1, 5.0)).expect("dispatch");

    assert!(output.scalar_updates().is_empty());
    assert_eq!(
        output.triggers()[0].subscription_id,
        registered.subscription_id
    );
}
