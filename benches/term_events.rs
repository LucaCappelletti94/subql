//! What a membership event costs as a table's subscription count grows.
//!
//! A membership row change is the one dispatch path that mutates, so it takes
//! the partition's copy-on-write clone. The `plain` arm holds a single term
//! subscription beside a growing crowd of ordinary row subscriptions, so
//! whatever it costs is state the event never reads.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::hint::black_box;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest, TableId};

const DDL: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
     CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
     CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT, score DOUBLE PRECISION);";

const TERM: &str = "SELECT * FROM docs WHERE project_id IN \
     (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";

const GRANTS: i64 = 5;
const SIZES: [u64; 3] = [500, 1_000, 2_000];

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn engine() -> (Engine, TableId) {
    let database = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("bench DDL parses");
    let members = catalog_helpers::table_id::<Postgres, _>(&database, "project_members")
        .expect("project_members is in the catalog");
    let engine = SubscriptionEngine::new(database, PostgreSqlDialect {}).with_translator(
        rls2fga::translator::TranslatorBuilder::new()
            .with_min_confidence(rls2fga::types::ConfidenceLevel::B)
            .build(),
    );
    (engine, members)
}

fn register_term(engine: &mut Engine, consumer: u64, subjects_each: usize) {
    let subjects: Vec<Value<Postgres>> = (0..subjects_each)
        .map(|nth| Value::String(format!("user{consumer}:{nth}")))
        .collect();
    let rows: Vec<(Value<Postgres>, Vec<Value<Postgres>>)> = (0..GRANTS)
        .map(|grant| {
            (
                subjects[usize::try_from(grant).unwrap() % subjects_each].clone(),
                vec![Value::Int(
                    i64::try_from(consumer).unwrap() * GRANTS + grant,
                )],
            )
        })
        .collect();
    engine
        .register(
            SubscriptionRequest::new(consumer, TERM)
                .subjects(subjects)
                .term_values(vec!["project_id"], rows),
        )
        .expect("the bench term registers");
}

/// One membership row appearing, timed against a partition of `size`
/// subscriptions of the named shape.
fn membership_event_benchmark(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("membership_event");
    for size in SIZES {
        for (shape, subjects_each) in [("one_subject", 1), ("three_subjects", 3)] {
            let (mut engine, members) = engine();
            for consumer in 0..size {
                register_term(&mut engine, consumer, subjects_each);
            }
            // One row, admitted before the loop starts, so every iteration pays
            // the same partition state rather than the one it grew itself.
            group.bench_with_input(BenchmarkId::new(shape, size), &size, |bencher, _| {
                bencher.iter(|| {
                    black_box(
                        engine
                            .consumers(&TestEvent::insert(
                                members,
                                vec![Value::Int(1_000_001), Value::String("user0:0".into())],
                            ))
                            .expect("the bench event dispatches"),
                    );
                });
            });
        }

        let (mut engine, members) = engine();
        register_term(&mut engine, 0, 1);
        for consumer in 1..=size {
            engine
                .register(SubscriptionRequest::new(
                    consumer,
                    format!("SELECT * FROM docs WHERE score > {consumer}"),
                ))
                .expect("the bench row filter registers");
        }
        group.bench_with_input(
            BenchmarkId::new("plain_beside_one_term", size),
            &size,
            |bencher, _| {
                bencher.iter(|| {
                    black_box(
                        engine
                            .consumers(&TestEvent::insert(
                                members,
                                vec![Value::Int(2_000_001), Value::String("user0:0".into())],
                            ))
                            .expect("the bench event dispatches"),
                    );
                });
            },
        );
    }
    group.finish();
}

/// One row of the subscribed table, timed against the same partitions.
///
/// Dispatch reads the term index for every candidate row, so whatever the
/// index costs to look up is paid here rather than on the membership path.
fn row_event_benchmark(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("row_event");
    for size in SIZES {
        for (shape, subjects_each) in [("one_subject", 1), ("three_subjects", 3)] {
            let (mut engine, _) = engine();
            let docs = catalog_helpers::table_id::<Postgres, _>(engine.database(), "docs")
                .expect("docs is in the catalog");
            for consumer in 0..size {
                register_term(&mut engine, consumer, subjects_each);
            }
            group.bench_with_input(BenchmarkId::new(shape, size), &size, |bencher, _| {
                bencher.iter(|| {
                    black_box(
                        engine
                            .consumers(&TestEvent::insert(
                                docs,
                                alloc_row(i64::try_from(size).expect("the size fits") / 2),
                            ))
                            .expect("the bench event dispatches"),
                    );
                });
            });
        }
    }
    group.finish();
}

/// A `docs` row in the project `grant` names.
fn alloc_row(grant: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::Int(grant * GRANTS),
        Value::String("spec".into()),
        Value::Float(1.0),
    ]
}

criterion_group!(benches, membership_event_benchmark, row_event_benchmark);
criterion_main!(benches);
