//! OID sanity check for the mesh benchmark substrate.
//!
//! Spawns two fresh PG containers with identical schema, INSERTs one
//! row into `orders` on each, reads the first event from a push
//! `CdcSource` per PG that share the same `Arc<ParserDB>`, and asserts
//! that `event.table_id()` matches across the two PGs.
//!
//! Background. subql's `TableId` is `u32`. The catalog used by the
//! engine is built from STATIC DDL text via `parser_db()`. The runtime
//! PG OIDs of tables are assigned by each PG independently and are not
//! the same number as the parser's internal `TableId`. The WAL parser
//! receives pgoutput `RELATION` messages that carry `(pg_oid, schema,
//! relname)` and is responsible for translating runtime OIDs to the
//! parser's `TableId`. If that translation is done by NAME, two PGs
//! with the same DDL produce events that resolve to the same
//! `TableId` and a single shared catalog is enough for the entire
//! mesh substrate. If translation is done by raw OID, we have to add
//! a per-source `oid_remap` to the fan-in adapter.
//!
//! This binary answers that question definitively in ~5 seconds.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example mesh_oid_sanity --features pg-streaming
//! ```

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]

#[path = "cdc_bench_common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use diesel::sql_types::Oid;
use diesel::{sql_query, RunQueryDsl};

use common::{
    assert_docker_available, create_publication_all, force_drop_slot, parser_db, pg_connect,
    pg_port, pg_replication_url, pg_with_wal2json, resolve_table_ids, setup_schema,
};
use subql::{PgStreamingCdcSource, PgStreamingConfig};

#[derive(Debug, diesel::QueryableByName)]
struct OidRow {
    #[diesel(sql_type = Oid)]
    oid: u32,
}

fn pg_relation_oid(conn: &mut diesel::PgConnection, relname: &str) -> u32 {
    let rows: Vec<OidRow> = sql_query(format!(
        "SELECT oid FROM pg_class WHERE relname = '{relname}' AND relkind = 'r'"
    ))
    .load(conn)
    .expect("pg_class lookup");
    rows.first().expect("relation present").oid
}

fn main() {
    assert_docker_available();
    println!("=== mesh OID sanity check ===");
    println!("Spawning two fresh PG containers with identical schema.");

    let container_a = pg_with_wal2json();
    let container_b = pg_with_wal2json();
    let port_a = pg_port(&container_a);
    let port_b = pg_port(&container_b);
    let mut setup_a = pg_connect(port_a);
    let mut setup_b = pg_connect(port_b);
    setup_schema(&mut setup_a);
    setup_schema(&mut setup_b);
    let publication = "mesh_oid_sanity_pub";
    create_publication_all(&mut setup_a, publication);
    create_publication_all(&mut setup_b, publication);

    let pg_oid_a = pg_relation_oid(&mut setup_a, "orders");
    let pg_oid_b = pg_relation_oid(&mut setup_b, "orders");
    println!(
        "PG runtime orders.oid: A={pg_oid_a}  B={pg_oid_b}  match={}",
        pg_oid_a == pg_oid_b
    );

    let catalog = parser_db();
    let static_ids = resolve_table_ids(&catalog);
    println!(
        "Static parser TableIds: users={} orders={} order_items={}",
        static_ids.users, static_ids.orders, static_ids.order_items
    );

    // Pre-create the sentinel users row BEFORE slot creation so the
    // slot's WAL stream does not replay it (which would confuse the
    // first-event check below).
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(&mut setup_a)
    .expect("insert sentinel a");
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(&mut setup_b)
    .expect("insert sentinel b");

    let slot_a = "mesh_oid_sanity_a".to_string();
    let slot_b = "mesh_oid_sanity_b".to_string();
    common::create_pgoutput_slot(&mut setup_a, &slot_a);
    common::create_pgoutput_slot(&mut setup_b, &slot_b);

    let pg_repl_a = pg_replication_url(port_a);
    let pg_repl_b = pg_replication_url(port_b);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let (ev_a, ev_b) = rt.block_on(async move {
        let config_a = PgStreamingConfig::new(pg_repl_a, &slot_a, publication)
            .status_interval(Duration::from_secs(10))
            .buffer_capacity(64);
        let config_b = PgStreamingConfig::new(pg_repl_b, &slot_b, publication)
            .status_interval(Duration::from_secs(10))
            .buffer_capacity(64);
        let mut source_a = PgStreamingCdcSource::connect(config_a, Arc::clone(&catalog))
            .await
            .expect("connect source A");
        let mut source_b = PgStreamingCdcSource::connect(config_b, Arc::clone(&catalog))
            .await
            .expect("connect source B");

        tokio::time::sleep(Duration::from_millis(500)).await;

        sql_query(
            "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
             VALUES (1001, 1, 'paid', 100, NOW())",
        )
        .execute(&mut setup_a)
        .expect("insert orders a");
        sql_query(
            "INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
             VALUES (1001, 1, 'paid', 100, NOW())",
        )
        .execute(&mut setup_b)
        .expect("insert orders b");

        let a = wait_for_orders_event(&mut source_a, &static_ids).await;
        let b = wait_for_orders_event(&mut source_b, &static_ids).await;
        force_drop_slot(&mut setup_a, &slot_a);
        force_drop_slot(&mut setup_b, &slot_b);
        (a, b)
    });

    println!();
    println!("Event from PG_A: table_id = {ev_a}");
    println!("Event from PG_B: table_id = {ev_b}");

    println!();
    if ev_a == ev_b && ev_a == static_ids.orders {
        println!("PASS: both PGs resolved orders to the same parser TableId ({ev_a}).");
        println!("Substrate may share one Arc<ParserDB> across the mesh; no oid_remap needed.");
    } else {
        println!("FAIL: TableIds disagree.");
        println!(
            "  PG_A.event.table_id = {ev_a}, PG_B.event.table_id = {ev_b}, static.orders = {}",
            static_ids.orders
        );
        println!("Substrate must add a per-source oid_remap before the mesh harness is built.");
        std::process::exit(1);
    }

    drop(container_a);
    drop(container_b);
}

async fn wait_for_orders_event(
    source: &mut PgStreamingCdcSource<subql::ParserDB>,
    _ids: &common::TableIds,
) -> u32 {
    use subql::CdcSource;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        let next = source.next_event().await;
        let Ok(Some(ev)) = next else { continue };
        // The only INSERT this source should see is into orders, pk=1001.
        if matches!(ev.pk().values().first(), Some(subql::Cell::Int(1001))) {
            return ev.table_id();
        }
    }
    panic!("timed out waiting for orders pk=1001 event");
}
