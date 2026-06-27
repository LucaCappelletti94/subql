//! Differential equivalence test: push + poll observe the same WAL.
//!
//! Drives a deterministic mixed-DML stream against a real Postgres
//! through TWO slots (one per `CdcSource` impl). Both transports
//! drain concurrently. The test asserts that after canonicalization
//! (LSN stripped; ack-driven divergence removed), both observed the
//! same events in the same commit order.
//!
//! Catches a class of regressions that single-transport tests do not:
//!
//! - **pgoutput parser bugs** that affect certain message shapes.
//!   If the parser misinterprets a tuple-data byte, both transports
//!   show the same wrong result. If the parser's behavior differs
//!   between callers (e.g. state leaks across messages), one
//!   transport diverges and this test catches it.
//! - **`tokio-postgres` `CopyBothDuplex` framing regressions.** The
//!   push source reads events through `CopyBothDuplex`; polling
//!   reads them via `pg_logical_slot_get_binary_changes`. If a
//!   `tokio-postgres` update silently changes CopyBoth framing
//!   semantics, push diverges from poll and this test catches it.
//! - **Materialize-fork drift.** We pin a specific rev of the fork
//!   in `Cargo.toml`. If a future bump changes behavior on the push
//!   side without an equivalent change on the polling side, this
//!   test surfaces the divergence.

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::items_after_statements
)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    CdcSource, Cell, ColumnId, EventKind, PgStreamingCdcSource, PgStreamingConfig,
    PollingPgCdcConfig, PollingPgCdcSource, PrimaryKey, RowImage, TableId, WalEvent,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT);";
const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price DOUBLE PRECISION)";

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

/// Structural canonical form of an event for cross-transport comparison.
/// LSN and any transport-specific metadata are intentionally stripped.
///
/// Comparison is done by [`canonical_eq`] rather than `derive(PartialEq)`
/// because `Cell::Float(f64)` blocks `Eq` and `RowImage` does not derive
/// `PartialEq`. The comparator walks structurally and uses `Cell`'s own
/// `PartialEq` for cell-by-cell equality.
#[derive(Debug, Clone)]
struct CanonicalEvent {
    table_id: TableId,
    kind: EventKind,
    pk: PrimaryKey,
    new_row: Option<RowImage>,
    old_row: Option<RowImage>,
    changed_columns: Vec<ColumnId>,
}

fn rows_equivalent(a: &RowImage, b: &RowImage) -> bool {
    if a.cells.len() != b.cells.len() {
        return false;
    }
    a.cells.iter().zip(b.cells.iter()).all(|(x, y)| x == y)
}

fn pks_equivalent(a: &PrimaryKey, b: &PrimaryKey) -> bool {
    if a.columns().len() != b.columns().len() {
        return false;
    }
    if a.values().len() != b.values().len() {
        return false;
    }
    a.columns()
        .iter()
        .zip(b.columns().iter())
        .all(|(x, y)| x == y)
        && a.values()
            .iter()
            .zip(b.values().iter())
            .all(|(x, y)| x == y)
}

fn canonical_eq(a: &CanonicalEvent, b: &CanonicalEvent) -> bool {
    if a.table_id != b.table_id || a.kind != b.kind {
        return false;
    }
    if !pks_equivalent(&a.pk, &b.pk) {
        return false;
    }
    if a.changed_columns != b.changed_columns {
        return false;
    }
    let new_row_ok = match (&a.new_row, &b.new_row) {
        (None, None) => true,
        (Some(x), Some(y)) => rows_equivalent(x, y),
        _ => false,
    };
    if !new_row_ok {
        return false;
    }
    match (&a.old_row, &b.old_row) {
        (None, None) => true,
        (Some(x), Some(y)) => rows_equivalent(x, y),
        _ => false,
    }
}

fn canonicalize<C>(ev: &WalEvent<C>) -> CanonicalEvent
where
    C: subql::Checkpoint,
{
    CanonicalEvent {
        table_id: ev.table_id(),
        kind: ev.kind(),
        pk: ev.pk().clone(),
        new_row: ev.new_row().cloned(),
        old_row: ev.old_row().cloned(),
        changed_columns: ev.changed_columns().to_vec(),
    }
}

async fn drain_n<S>(source: &mut S, n: usize) -> Vec<WalEvent<S::Checkpoint>>
where
    S: CdcSource,
{
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let ev = tokio::time::timeout(Duration::from_secs(5), source.next_event())
            .await
            .expect("next_event timeout draining")
            .expect("next_event err")
            .expect("source closed before drain target reached");
        out.push(ev);
    }
    out
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn push_and_poll_observe_identical_event_streams() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);

    let mut setup = common::pg_connect(port);
    let mut dml = common::pg_connect(port);
    sql_query(PG_DDL).execute(&mut setup).expect("create table");
    sql_query("ALTER TABLE orders REPLICA IDENTITY FULL")
        .execute(&mut setup)
        .expect("REPLICA IDENTITY FULL");

    // One publication, two slots. Each slot is independent so both
    // transports observe the same WAL stream without stealing events
    // from each other. The polling source's drain auto-advances ITS
    // slot; the push source's slot stays where the push side has acked.
    let publication = "subql_equiv_pub";
    common::create_publication(&mut setup, publication, "orders");
    let push_slot = "subql_equiv_push";
    let poll_slot = "subql_equiv_poll";
    common::create_pgoutput_slot(&mut setup, push_slot);
    common::create_pgoutput_slot(&mut setup, poll_slot);

    let catalog = Arc::new(ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"));
    let push_config =
        PgStreamingConfig::new(common::pg_replication_url(port), push_slot, publication);
    let poll_config = PollingPgCdcConfig::new(common::pg_url(port), poll_slot, publication)
        .poll_interval(Duration::from_millis(50));

    // Deterministic DML stream: 5 inserts, 3 updates, 2 deletes. The
    // exact pattern is recorded so both transports must show identical
    // event sequences after canonicalization.
    const N_INSERTS: i32 = 5;
    const N_UPDATES: i32 = 3;
    const N_DELETES: i32 = 2;
    const N_TOTAL: usize = (N_INSERTS + N_UPDATES + N_DELETES) as usize;

    current_thread_rt().block_on(async move {
        let mut push_source = PgStreamingCdcSource::connect(push_config, Arc::clone(&catalog))
            .await
            .expect("connect push source");
        let mut poll_source = PollingPgCdcSource::connect(poll_config, Arc::clone(&catalog))
            .await
            .expect("connect poll source");

        // Drive deterministic DML. All commits happen BEFORE either
        // source begins draining; this is a correctness test, not a
        // latency test, so ordering of "produce vs drain" doesn't
        // matter as long as both transports observe the same WAL.
        for id in 1..=N_INSERTS {
            sql_query(format!("INSERT INTO orders VALUES ({id}, {}.0)", id * 10))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("insert id={id}: {e}"));
        }
        for id in 1..=N_UPDATES {
            sql_query(format!(
                "UPDATE orders SET price = {}.5 WHERE id = {id}",
                id * 20
            ))
            .execute(&mut dml)
            .unwrap_or_else(|e| panic!("update id={id}: {e}"));
        }
        for id in 1..=N_DELETES {
            sql_query(format!("DELETE FROM orders WHERE id = {id}"))
                .execute(&mut dml)
                .unwrap_or_else(|e| panic!("delete id={id}: {e}"));
        }

        // Drain concurrently into Vecs. tokio::join! polls both
        // sequentially on current-thread runtime, but since each await
        // point yields, both make progress.
        let (push_events, poll_events) = tokio::join!(
            drain_n(&mut push_source, N_TOTAL),
            drain_n(&mut poll_source, N_TOTAL)
        );

        assert_eq!(push_events.len(), N_TOTAL);
        assert_eq!(poll_events.len(), N_TOTAL);

        let push_canon: Vec<CanonicalEvent> = push_events.iter().map(canonicalize).collect();
        let poll_canon: Vec<CanonicalEvent> = poll_events.iter().map(canonicalize).collect();

        // Index-by-index comparison so the failure message points at
        // the first divergent event rather than dumping both vectors.
        for (i, (p, q)) in push_canon.iter().zip(poll_canon.iter()).enumerate() {
            assert!(
                canonical_eq(p, q),
                "push and poll diverged at event {i}:\n  push: {p:?}\n  poll: {q:?}\n\
                 exactly one of: push parser bug / poll harness bug / PG drift"
            );
        }

        // Sanity-check the shape too: expect inserts then updates then deletes.
        let kinds: Vec<EventKind> = push_canon.iter().map(|e| e.kind).collect();
        assert_eq!(
            &kinds[..N_INSERTS as usize],
            &vec![EventKind::Insert; N_INSERTS as usize][..],
            "first {N_INSERTS} events must be INSERTs"
        );
        assert_eq!(
            &kinds[N_INSERTS as usize..(N_INSERTS + N_UPDATES) as usize],
            &vec![EventKind::Update; N_UPDATES as usize][..],
            "next {N_UPDATES} events must be UPDATEs"
        );
        assert_eq!(
            &kinds[(N_INSERTS + N_UPDATES) as usize..],
            &vec![EventKind::Delete; N_DELETES as usize][..],
            "final {N_DELETES} events must be DELETEs"
        );

        // Quick sanity-check on data integrity for one event.
        // First INSERT was id=1, price=10.0.
        if let WalEvent::Insert { new_row, .. } = &push_events[0] {
            assert_eq!(new_row.get(0), Some(&Cell::Int(1)));
            assert_eq!(new_row.get(1), Some(&Cell::Float(10.0)));
        } else {
            panic!("first event must be Insert");
        }

        println!("push and poll observed identical {N_TOTAL} events (canonical equality)");
    });

    for slot in [push_slot, poll_slot] {
        sql_query(format!("SELECT pg_drop_replication_slot('{slot}')"))
            .execute(&mut setup)
            .expect("drop slot");
    }
}
