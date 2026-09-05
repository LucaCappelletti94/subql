//! An UPDATE that leaves every WHERE-clause column untouched still changes the
//! row a full-row subscriber sees, so it has to reach that subscriber.
//!
//! Candidate selection used to narrow an UPDATE to the predicates reading a
//! changed column. That answers "can the verdict flip", which is not the
//! question a row subscription asks: it receives the whole row image, so a
//! change to any column is visible to it. These tests pin the notification down
//! for both projections the engine treats as full-row (`SELECT *` and an
//! explicit complete column list), for an update whose changed columns the
//! source could not report, and for the aggregate case that keeps the prune,
//! whose value moves only when a column it reads moves.

#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, SubscriptionEngine, SubscriptionId, SubscriptionRequest,
    TableId,
};

const DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE PRECISION NOT NULL,
    quantity INT NOT NULL,
    status TEXT NOT NULL
);";

/// Column ordinals in `orders`.
const PRICE: u16 = 1;
const QUANTITY: u16 = 2;
const STATUS: u16 = 3;

const CONSUMER: u64 = 1;

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine_with(sql: &str) -> (Engine, TableId) {
    let (engine, orders, _) = engine_and_subscription(sql);
    (engine, orders)
}

fn engine_and_subscription(sql: &str) -> (Engine, TableId, SubscriptionId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap();
    let orders = catalog_helpers::table_id(&db, "orders").unwrap();
    let mut engine = Engine::new(db, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(
            CONSUMER, sql,
        ))
        .unwrap();
    (engine, orders, registered.subscription_id)
}

/// The row image the reproduction uses, varying the two columns under test.
fn row(price: f64, quantity: i64, status: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(7),
        Value::Float(price),
        Value::Int(quantity),
        Value::String(status.into()),
    ]
}

fn update(
    table: TableId,
    old: Vec<Value<Postgres>>,
    new: Vec<Value<Postgres>>,
    changed: impl IntoIterator<Item = u16>,
) -> TestEvent<Postgres> {
    TestEvent::update(table, old, new)
        .with_pk_columns([0u16])
        .with_changed_columns(changed)
}

#[test]
fn update_of_a_projected_non_predicate_column_notifies_the_subscription() {
    let (mut engine, orders) = engine_with("SELECT * FROM orders WHERE quantity > 0");

    // `status` is projected but never filtered on. Both images satisfy
    // `quantity > 0`, so the row stays in the view and its content changed.
    let event = update(orders, row(9.5, 5, "v1"), row(9.5, 5, "v2"), [STATUS]);
    let notifs = engine.consumers(&event).unwrap();

    assert_eq!(notifs.updated(), vec![CONSUMER]);
    assert!(
        notifs.inserted().is_empty(),
        "the row stayed in the view so no new entry was fired"
    );
    assert!(
        notifs.deleted().is_empty(),
        "the row stayed in the view so no exit was fired"
    );
}

#[test]
fn explicit_all_columns_projection_notifies_like_select_star() {
    let (mut engine, orders) =
        engine_with("SELECT id, price, quantity, status FROM orders WHERE quantity > 0");

    let event = update(orders, row(9.5, 5, "v1"), row(9.5, 5, "v2"), [STATUS]);

    assert_eq!(engine.consumers(&event).unwrap().updated(), vec![CONSUMER]);
}

#[test]
fn update_of_a_predicate_column_notifies_the_subscription() {
    let (mut engine, orders) = engine_with("SELECT * FROM orders WHERE quantity > 0");

    let event = update(orders, row(9.5, 5, "v1"), row(9.5, 6, "v1"), [QUANTITY]);

    assert_eq!(engine.consumers(&event).unwrap().updated(), vec![CONSUMER]);
}

#[test]
fn update_crossing_the_predicate_boundary_reports_view_exit_and_entry() {
    let (mut engine, orders) = engine_with("SELECT * FROM orders WHERE quantity > 0");

    let leaving = update(orders, row(9.5, 5, "v1"), row(9.5, 0, "v1"), [QUANTITY]);
    let notifs = engine.consumers(&leaving).unwrap();
    assert_eq!(notifs.deleted(), vec![CONSUMER]);
    assert!(
        notifs.updated().is_empty(),
        "a row that left the view produces no in-view diff"
    );

    let entering = update(orders, row(9.5, 0, "v1"), row(9.5, 5, "v1"), [QUANTITY]);
    let notifs = engine.consumers(&entering).unwrap();
    assert_eq!(notifs.inserted(), vec![CONSUMER]);
    assert!(
        notifs.updated().is_empty(),
        "a newly entered row has no prior in-view content"
    );
}

#[test]
fn update_with_unreported_changed_columns_reports_view_exit() {
    let (mut engine, orders) = engine_with("SELECT * FROM orders WHERE quantity > 0");

    // A source that cannot diff the images reports no changed columns at all,
    // which `pg_walstream::ChangeEvent` does for any old image narrower than
    // the whole row. Nothing about the new row hints that this predicate ever
    // matched, so only re-evaluating both images finds the view exit.
    let event =
        TestEvent::update(orders, row(9.5, 5, "v1"), row(9.5, 0, "v1")).with_pk_columns([0u16]);
    let notifs = engine.consumers(&event).unwrap();

    assert_eq!(notifs.deleted(), vec![CONSUMER]);
    assert!(
        notifs.inserted().is_empty(),
        "a row that exited the view cannot also be a new entry"
    );
    assert!(
        notifs.updated().is_empty(),
        "a row that exited the view produces no in-view diff"
    );
}

#[test]
fn delete_of_a_matching_row_notifies_the_subscription() {
    let (mut engine, orders) = engine_with("SELECT * FROM orders WHERE quantity > 0");

    let event = TestEvent::delete(orders, row(9.5, 5, "v2")).with_pk_columns([0u16]);

    assert_eq!(engine.consumers(&event).unwrap().deleted(), vec![CONSUMER]);
}

#[test]
fn aggregate_subscription_sees_an_update_of_the_column_it_sums() {
    let (mut engine, orders, subscription) =
        engine_and_subscription("SELECT SUM(price) FROM orders WHERE quantity > 0");

    // The engine holds the total, so it needs the starting numbers before it
    // reports anything. Nothing has been folded yet, so the read cannot have
    // raced a change and needs no stream position.
    subql::Install::install(
        &mut engine,
        subscription,
        subql::AggregateSeedInstall {
            rows: vec![vec![Value::Float(9.5), Value::Int(1)]],
            read_at: None,
        },
    )
    .expect("the starting numbers land");

    let event = update(orders, row(9.5, 5, "v1"), row(11.5, 5, "v1"), [PRICE]);

    let updates = engine.aggregate_updates(&event).unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].consumer, CONSUMER);
    assert_eq!(
        updates[0].change,
        subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(AggValue::Sum(Some(
            11.5
        )),))
    );
}

/// The same defect driven through a real change source instead of a hand-built
/// event: SQLite's session extension records the write, the emulator encodes it
/// on the pgoutput wire, and `ChangeEvent` decodes it. This is the path a
/// materializer sees.
#[cfg(feature = "pg-sqlite-emu")]
mod real_source {
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::backend::CdcEvent;
    use subql::{
        ChangeEvent, DefaultIds, EventKind, PgSqliteEmuSource, SubscriptionEngine,
        SubscriptionRequest,
    };

    const PG_DDL: &str = "CREATE TABLE orders (
        id INT PRIMARY KEY,
        price FLOAT,
        quantity INT,
        status TEXT
    );";

    fn drain_one(source: &mut PgSqliteEmuSource) -> ChangeEvent {
        source
            .poll_next_event()
            .expect("poll succeeds")
            .expect("expected an event on the queue")
    }

    #[test]
    fn status_only_update_reaches_the_subscription() {
        let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("build source");
        let mut engine = SubscriptionEngine::<ChangeEvent, DefaultIds, _>::new(
            source.pg_catalog().clone(),
            PostgreSqlDialect {},
        );
        engine
            .register(SubscriptionRequest::new(
                1u64,
                "SELECT * FROM orders WHERE quantity > 0",
            ))
            .expect("subscription registers");

        source
            .execute_sql(
                "INSERT INTO orders (id, price, quantity, status) VALUES (7, 9.5, 5, 'v1')",
            )
            .unwrap();
        // Drain between statements: the session merges ops on one row.
        let insert = drain_one(&mut source);
        assert_eq!(insert.kind(), EventKind::Insert);
        assert_eq!(engine.consumers(&insert).unwrap().inserted(), vec![1u64]);

        source
            .execute_sql("UPDATE orders SET status = 'v2' WHERE id = 7")
            .unwrap();
        let update = drain_one(&mut source);
        assert_eq!(update.kind(), EventKind::Update);

        let notifs = engine.consumers(&update).unwrap();
        assert_eq!(notifs.updated(), vec![1u64]);
        assert!(
            notifs.inserted().is_empty(),
            "the row stayed in the view so no new entry was fired"
        );
        assert!(
            notifs.deleted().is_empty(),
            "the row stayed in the view so no exit was fired"
        );
    }
}
