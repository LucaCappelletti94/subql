//! Smoke test for [`subql::PollingPgCdcSource`].
//!
//! Quick verification that the polling source connects to a real
//! Postgres, drains an INSERT, and the observability counters update.

#![cfg(feature = "pg-streaming")]
#![allow(clippy::unwrap_used, clippy::print_stdout)]

mod common;

use std::time::Duration;

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{CdcSource, EventKind, PollingPgCdcConfig, PollingPgCdcSource};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn polling_source_drains_insert_and_updates_counters() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");
    let slot = "subql_polling_smoke";
    let publication = "subql_polling_smoke_pub";
    common::create_publication(&mut setup, publication, "orders");
    common::create_pgoutput_slot(&mut setup, slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    // The polling source uses the regular SQL URL, NOT the replication
    // one. Replication mode is push-source-only.
    let config = PollingPgCdcConfig::new(common::pg_url(port), slot, publication)
        .poll_interval(Duration::from_millis(50));

    current_thread_rt().block_on(async move {
        let mut source = PollingPgCdcSource::connect(config, catalog)
            .await
            .expect("connect polling source");

        assert_eq!(source.events_received(), 0);
        assert_eq!(source.polls_issued(), 0);
        assert_eq!(source.empty_polls_observed(), 0);

        sql_query("INSERT INTO orders VALUES (1, 5.0)")
            .execute(&mut dml)
            .expect("insert");

        let event = tokio::time::timeout(Duration::from_secs(2), source.next_event())
            .await
            .expect("next_event timeout")
            .expect("next_event err")
            .expect("source closed");
        assert_eq!(event.kind(), EventKind::Insert);

        // After at least one drain that produced an event, counters
        // must reflect it. Polls may include extra empty drains from
        // before the INSERT landed; that is part of the polling cost.
        assert!(source.events_received() >= 1);
        assert!(source.polls_issued() >= 1);
        let avg_batch = source.average_drain_batch_size();
        assert!(
            avg_batch >= 1.0,
            "non-empty drain must contribute >= 1 event to batch average; got {avg_batch}"
        );

        println!(
            "polling smoke: events_received={} polls_issued={} empty_polls={} avg_batch={:.2}",
            source.events_received(),
            source.polls_issued(),
            source.empty_polls_observed(),
            avg_batch,
        );
    });

    sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
        .execute(&mut setup)
        .expect("drop slot");
}
