//! Catalog and CDC lookup benchmarks.
//!
//! Covers the paths a wire event actually walks: resolving a column name from
//! its ordinal, deriving an UPDATE's changed columns from two full row images,
//! and dispatching a wal2json event through the engine.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::fmt::Write as _;
use std::hint::black_box;
use subql::backend::CdcEvent;
use subql::wal::{parse_wal2json_v1, parse_wal2json_v2, ChangeV1, MessageV2};
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

const WIDTH: usize = 40;

/// A catalog holding one `wide` table of [`WIDTH`] integer columns.
fn wide_catalog() -> ParserDB {
    let mut ddl = String::from("CREATE TABLE wide (c0 INT PRIMARY KEY");
    for column in 1..WIDTH {
        let _ = write!(ddl, ", c{column} INT");
    }
    ddl.push_str(");");
    ParserDB::parse::<PostgreSqlDialect>(&ddl).expect("bench DDL parses")
}

/// A wal2json v2 UPDATE carrying both full images, with `changed` cells moved.
fn v2_update(changed: usize) -> MessageV2 {
    let cells = |shift: i64| {
        (0..WIDTH)
            .map(|column| {
                let moved = i64::from(column < changed) * shift;
                format!(
                    "{{\"name\":\"c{column}\",\"type\":\"integer\",\"value\":{}}}",
                    i64::try_from(column).unwrap_or(0) + moved
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let json = format!(
        "{{\"action\":\"U\",\"schema\":\"public\",\"table\":\"wide\",\
          \"columns\":[{}],\"identity\":[{}]}}",
        cells(1000),
        cells(0)
    );
    let mut parsed = parse_wal2json_v2(json.as_bytes()).expect("bench JSON parses");
    parsed.remove(0)
}

/// The v1 spelling of [`v2_update`].
fn v1_update(changed: usize) -> ChangeV1 {
    let names = (0..WIDTH)
        .map(|column| format!("\"c{column}\""))
        .collect::<Vec<_>>()
        .join(",");
    let types = ["\"integer\""].repeat(WIDTH).join(",");
    let values = |shift: i64| {
        (0..WIDTH)
            .map(|column| {
                let moved = i64::from(column < changed) * shift;
                (i64::try_from(column).unwrap_or(0) + moved).to_string()
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    let json = format!(
        "{{\"change\":[{{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"wide\",\
          \"columnnames\":[{names}],\"columntypes\":[{types}],\"columnvalues\":[{}],\
          \"oldkeys\":{{\"keynames\":[{names}],\"keytypes\":[{types}],\"keyvalues\":[{}]}}}}]}}",
        values(1000),
        values(0)
    );
    let mut parsed = parse_wal2json_v1(json.as_bytes()).expect("bench JSON parses");
    parsed.remove(0)
}

/// Resolving every column name of a wide table by ordinal.
fn column_name_benchmark(c: &mut Criterion) {
    let db = wide_catalog();
    let table = catalog_helpers::table_id(&db, "wide").expect("wide resolves");
    let mut group = c.benchmark_group("catalog_column_name");
    group.bench_function("wide_table_every_ordinal", |b| {
        b.iter(|| {
            for column in 0..u16::try_from(WIDTH).unwrap_or(0) {
                black_box(catalog_helpers::column_name(&db, table, column));
            }
        });
    });
    group.finish();
}

/// Deriving the changed columns of a full-image UPDATE, per wire format.
fn changed_columns_benchmark(c: &mut Criterion) {
    let db = wide_catalog();
    let mut group = c.benchmark_group("wire_changed_columns");
    for changed in [1usize, WIDTH] {
        let v2 = v2_update(changed);
        group.bench_with_input(BenchmarkId::new("v2", changed), &changed, |b, _| {
            b.iter(|| black_box(v2.changed_columns(&db)));
        });
        let v1 = v1_update(changed);
        group.bench_with_input(BenchmarkId::new("v1", changed), &changed, |b, _| {
            b.iter(|| black_box(v1.changed_columns(&db)));
        });
    }
    group.finish();
}

/// Dispatching a wal2json event through an engine holding `predicates`
/// subscriptions on the wide table.
fn wire_dispatch_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_dispatch");
    for predicates in [1usize, 16] {
        let db = wide_catalog();
        let mut engine: SubscriptionEngine<MessageV2, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, PostgreSqlDialect {});
        for subscription in 0..predicates {
            engine
                .register(SubscriptionRequest::new(
                    u64::try_from(subscription).unwrap_or(0),
                    format!("SELECT * FROM wide WHERE c{} > 5", subscription % WIDTH),
                ))
                .expect("bench subscription registers");
        }
        let event = v2_update(WIDTH / 2);
        group.bench_with_input(
            BenchmarkId::new("v2_update", predicates),
            &predicates,
            |b, _| {
                b.iter(|| black_box(engine.dispatch(&event).expect("bench event dispatches")));
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    column_name_benchmark,
    changed_columns_benchmark,
    wire_dispatch_benchmark
);
criterion_main!(benches);
