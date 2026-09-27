//! A Postgres read's fence against the transactions around it, on a real
//! server.
//!
//! Requires Docker. Tests are `#[ignore]`d so default `cargo test` does not
//! spin up containers. Run with:
//!
//! ```sh
//! cargo test --test it read_fence_postgres:: --features executor-diesel-postgres \
//!     -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::connection::{AnsiTransactionManager, TransactionManager};
use diesel::{sql_query, ExpressionMethods, PgConnection, QueryableByName, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::reexec::{Connector, PgDieselConnector, ReadQuery};
use subql::{
    AggValue, AggregateSeedInstall, Checkpoint, DefaultIds, Install, PgCommitPosition, PgLsn, Seen,
    SubscriptionEngine, SubscriptionRequest, Wal2JsonV2Event,
};

diesel::table! {
    orders (id) {
        id -> Int4,
        price -> Nullable<Float8>,
        quantity -> Nullable<Int4>,
        status -> Nullable<Text>,
    }
}

fn insert_order(conn: &mut PgConnection, id: i32) {
    diesel::insert_into(orders::table)
        .values((
            orders::id.eq(id),
            orders::price.eq(7.0),
            orders::quantity.eq(1),
            orders::status.eq("paid"),
        ))
        .execute(conn)
        .expect("insert order");
}

/// The position a read taken now would have reported before it read its own
/// snapshot, for contrast with the fence.
fn position_before_read(conn: &mut PgConnection) -> PgCommitPosition {
    #[derive(QueryableByName)]
    struct Lsn {
        #[diesel(sql_type = diesel::sql_types::Text)]
        lsn: String,
    }
    // A WAL function, which the query DSL does not express.
    let row: Lsn = sql_query("SELECT pg_current_wal_lsn()::text AS lsn")
        .get_result(conn)
        .expect("pg_current_wal_lsn");
    PgCommitPosition::before_commit(PgLsn::parse(&row.lsn).expect("an lsn"))
}

/// A commit that lands after a position read beside the snapshot is still
/// seen by it, one open across the read is not, and a seeded count applied to
/// the decoded stream ends at the table's own count.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn a_read_fence_holds_exactly_what_its_snapshot_saw() {
    common::assert_docker_available();
    let db = common::pg_database();
    let slot = db.slot("read_fence");
    let mut setup = db.connect();
    let mut writer = db.connect();
    let mut open = db.connect();
    let mut reader = db.connect();
    common::pg::setup_orders(&mut setup, &[(1, 5.0), (2, 9.0)], &slot);

    let mut engine = SubscriptionEngine::<Wal2JsonV2Event, DefaultIds, ParserDB>::new(
        common::pg::orders_catalog(),
        PostgreSqlDialect {},
    );
    let sql = "SELECT COUNT(*) FROM orders WHERE status = 'paid'";
    let sub = engine
        .register(SubscriptionRequest::<DefaultIds, Postgres>::new(7u64, sql))
        .unwrap()
        .subscription_id;

    let before = position_before_read(&mut reader);
    insert_order(&mut writer, 3);
    AnsiTransactionManager::begin_transaction(&mut open).unwrap();
    insert_order(&mut open, 4);

    let connector = PgDieselConnector::new(reader);
    let (count, fence) = connector
        .execute_scalar(&ReadQuery::without_binds(sql), ScalarFamily::Int, &())
        .unwrap();
    let fence = fence.expect("a Postgres read reports its fence");
    assert_eq!(count, Value::Int(3), "ids 1, 2 and 3");

    AnsiTransactionManager::commit_transaction(&mut open).unwrap();
    insert_order(&mut writer, 5);

    let events = common::read_wal2json_v2(&common::drain_slot(&mut setup, &slot));
    let positions: Vec<PgCommitPosition> = events
        .iter()
        .map(|event| {
            event
                .position()
                .expect("positioned under include-lsn and include-xids")
        })
        .collect();
    assert_eq!(positions.len(), 3, "ids 3, 4 and 5");
    assert!(
        positions[0] > before,
        "id 3 committed after the position a read would have taken first"
    );
    assert_eq!(positions[0].seen_by(&fence), Seen::Held);
    assert_ne!(
        positions[1].seen_by(&fence),
        Seen::Held,
        "id 4 was open across the read"
    );
    assert_eq!(positions[2].seen_by(&fence), Seen::Beyond);

    Install::install(
        &mut engine,
        sub,
        AggregateSeedInstall {
            rows: vec![vec![count]],
            fence: Some(fence),
        },
    )
    .unwrap();
    for event in &events {
        engine.aggregate_updates(event).unwrap();
    }
    assert_eq!(
        engine.current_aggregate_value(sub),
        Some(AggValue::CountStar(5)),
        "the table holds ids 1 to 5"
    );
}
