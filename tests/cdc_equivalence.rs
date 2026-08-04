//! Differential equivalence test: push + poll observe the same WAL.
//!
//! Drives a deterministic mixed-DML stream against a real Postgres
//! through TWO slots (one per `CdcSource` impl). Both transports
//! drain concurrently. The test asserts that after canonicalization
//! (LSN stripped, ack-driven divergence removed), both observed the
//! same events in the same commit order.

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::items_after_statements
)]

mod common;

use std::time::Duration;

use diesel::{sql_query, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, RowKind, Value};
use subql::{
    CdcSource, ColumnId, EventKind, PgStreamingCdcSource, PgStreamingConfig, PollingPgCdcConfig,
    PollingPgCdcSource, TableId,
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
#[derive(Debug, Clone, PartialEq)]
struct CanonicalEvent {
    table_id: TableId,
    kind: EventKind,
    pk_columns: Vec<ColumnId>,
    pk_ints: Vec<Option<i64>>,
    new_row: Option<Vec<CanonicalCell>>,
    old_row: Option<Vec<CanonicalCell>>,
    changed_columns: Vec<ColumnId>,
}

/// Two-column canonical cell for the `orders (id INT, price FLOAT)`
/// schema. `Missing` covers `Presence::Missing`. The float column stores
/// bit-patterns for `Eq`.
#[derive(Debug, Clone, PartialEq)]
enum CanonicalCell {
    Missing,
    Null,
    Int(i64),
    FloatBits(u64),
}

fn canonicalize_cell<E: CdcEvent<Backend = subql::backend::Postgres>>(
    ev: &E,
    db: &ParserDB,
    row: RowKind,
    col: ColumnId,
) -> CanonicalCell {
    match col {
        0 => match ev.value_at(db, row, col).unwrap() {
            Value::Null => CanonicalCell::Null,
            Value::Int(v) => CanonicalCell::Int(v),
            _ => CanonicalCell::Missing,
        },
        1 => match ev.value_at(db, row, col).unwrap() {
            Value::Null => CanonicalCell::Null,
            Value::Float(v) => CanonicalCell::FloatBits(v.to_bits()),
            _ => CanonicalCell::Missing,
        },
        _ => CanonicalCell::Missing,
    }
}
fn row_present<E: CdcEvent<Backend = subql::backend::Postgres>>(
    ev: &E,
    db: &ParserDB,
    row: RowKind,
    cols: &[ColumnId],
) -> Option<Vec<CanonicalCell>> {
    let cells: Vec<CanonicalCell> = cols
        .iter()
        .map(|&c| canonicalize_cell(ev, db, row, c))
        .collect();
    if cells.iter().all(|c| matches!(c, CanonicalCell::Missing)) {
        None
    } else {
        Some(cells)
    }
}

fn canonicalize<E: CdcEvent<Backend = subql::backend::Postgres>>(
    ev: &E,
    db: &ParserDB,
) -> CanonicalEvent {
    let all_cols: Vec<ColumnId> = (0..=1).collect();
    let pk_columns: Vec<ColumnId> = ev.pk_columns(db);
    let pk_ints: Vec<Option<i64>> = pk_columns
        .iter()
        .map(|&c| match ev.value_at(db, RowKind::Pk, c).unwrap() {
            Value::Int(v) => Some(v),
            _ => None,
        })
        .collect();
    let (new_row, old_row) = match ev.kind() {
        EventKind::Insert => (row_present(ev, db, RowKind::New, &all_cols), None),
        EventKind::Update => (
            row_present(ev, db, RowKind::New, &all_cols),
            row_present(ev, db, RowKind::Old, &all_cols),
        ),
        EventKind::Delete => (None, row_present(ev, db, RowKind::Old, &all_cols)),
        EventKind::Truncate => (None, None),
    };
    CanonicalEvent {
        table_id: ev.table_id(db),
        kind: ev.kind(),
        pk_columns,
        pk_ints,
        new_row,
        old_row,
        changed_columns: ev.changed_columns(db),
    }
}

async fn drain_n<S>(source: &mut S, n: usize) -> Vec<S::Event>
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
#[allow(clippy::too_many_lines)]
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

    let publication = "subql_equiv_pub";
    common::create_publication(&mut setup, publication, "orders");
    let push_slot = "subql_equiv_push";
    let poll_slot = "subql_equiv_poll";
    common::create_pgoutput_slot(&mut setup, push_slot);
    common::create_pgoutput_slot(&mut setup, poll_slot);

    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let push_config =
        PgStreamingConfig::new(common::pg_replication_url(port), push_slot, publication);
    let poll_config = PollingPgCdcConfig::new(common::pg_url(port), poll_slot, publication)
        .poll_interval(Duration::from_millis(50));

    const N_INSERTS: i32 = 5;
    const N_UPDATES: i32 = 3;
    const N_DELETES: i32 = 2;
    const N_TOTAL: usize = (N_INSERTS + N_UPDATES + N_DELETES) as usize;

    current_thread_rt().block_on(async move {
        let mut push_source = PgStreamingCdcSource::connect(push_config, catalog)
            .await
            .expect("connect push source");
        let mut poll_source = PollingPgCdcSource::connect(
            poll_config,
            ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL"),
        )
        .await
        .expect("connect poll source");

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

        let (push_events, poll_events) = tokio::join!(
            drain_n(&mut push_source, N_TOTAL),
            drain_n(&mut poll_source, N_TOTAL)
        );

        assert_eq!(push_events.len(), N_TOTAL);
        assert_eq!(poll_events.len(), N_TOTAL);

        let canon_catalog =
            ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL for canonicalize");
        let push_canon: Vec<CanonicalEvent> =
            push_events.iter().map(|e| canonicalize(e, &canon_catalog)).collect();
        let poll_canon: Vec<CanonicalEvent> =
            poll_events.iter().map(|e| canonicalize(e, &canon_catalog)).collect();

        for (i, (p, q)) in push_canon.iter().zip(poll_canon.iter()).enumerate() {
            assert_eq!(
                p, q,
                "push and poll diverged at event {i}: exactly one of push parser bug / poll harness bug / PG drift",
            );
        }

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

        // First INSERT was id=1, price=10.0.
        let first = &push_events[0];
        assert_eq!(first.kind(), EventKind::Insert);
        assert!(matches!(
            first.value_at(&canon_catalog, RowKind::New, 0).unwrap(),
            Value::Int(v) if v == 1
        ));
        assert!(matches!(
            first.value_at(&canon_catalog, RowKind::New, 1).unwrap(),
            Value::Float(v) if (v - 10.0).abs() < f64::EPSILON
        ));

        println!("push and poll observed identical {N_TOTAL} events (canonical equality)");
    });

    for slot in [push_slot, poll_slot] {
        common::drop_slot(&mut setup, slot);
    }
}
