//! Dispatch performance benchmarks
#![allow(clippy::unwrap_used, clippy::unreadable_literal)]
#![allow(clippy::cast_sign_loss)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sqlparser::dialect::PostgreSqlDialect;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use subql::{
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SchemaCatalog, SubscriptionEngine,
    SubscriptionSpec, TableId, WalEvent,
};

// Mock catalog for benchmarking
struct BenchCatalog {
    tables: HashMap<String, (TableId, usize)>,
    columns: HashMap<(TableId, String), u16>,
}

impl BenchCatalog {
    fn new() -> Self {
        let mut catalog = Self {
            tables: HashMap::new(),
            columns: HashMap::new(),
        };

        // Add "orders" table with many columns
        catalog.tables.insert("orders".to_string(), (1, 10));
        catalog.columns.insert((1, "id".to_string()), 0);
        catalog.columns.insert((1, "user_id".to_string()), 1);
        catalog.columns.insert((1, "amount".to_string()), 2);
        catalog.columns.insert((1, "status".to_string()), 3);
        catalog.columns.insert((1, "priority".to_string()), 4);
        catalog.columns.insert((1, "quantity".to_string()), 5);
        catalog.columns.insert((1, "discount".to_string()), 6);
        catalog.columns.insert((1, "tax".to_string()), 7);
        catalog.columns.insert((1, "shipping".to_string()), 8);
        catalog.columns.insert((1, "created_at".to_string()), 9);

        catalog
    }
}

impl SchemaCatalog for BenchCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.tables.get(table_name).map(|(id, _)| *id)
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
        self.columns
            .get(&(table_id, column_name.to_string()))
            .copied()
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.tables
            .values()
            .find(|(id, _)| *id == table_id)
            .map(|(_, arity)| *arity)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0x1234567890ABCDEF)
    }
}

fn make_test_event() -> WalEvent {
    WalEvent {
        kind: EventKind::Insert,
        table_id: 1,
        pk: PrimaryKey {
            columns: Arc::from([0u16]),
            values: Arc::from([Cell::Int(1)]),
        },
        old_row: None,
        new_row: Some(RowImage {
            cells: Arc::from([
                Cell::Int(1),                  // id
                Cell::Int(42),                 // user_id
                Cell::Int(250),                // amount
                Cell::String("active".into()), // status
                Cell::Int(5),                  // priority
                Cell::Int(10),                 // quantity
                Cell::Int(0),                  // discount
                Cell::Int(25),                 // tax
                Cell::Int(10),                 // shipping
                Cell::Int(1234567890),         // created_at
            ]),
        }),
        changed_columns: Arc::from([]),
    }
}

/// Benchmark: Dispatch scaling with increasing predicate count
fn dispatch_scaling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_scaling");

    for &predicate_count in &[100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(predicate_count),
            &predicate_count,
            |b, &count| {
                let catalog = Arc::new(BenchCatalog::new());
                let mut engine =
                    SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

                // Register N predicates with different conditions
                for i in 0..count {
                    let spec = SubscriptionSpec {
                        subscription_id: i as u64,
                        user_id: (i % 1000) as u64,
                        session_id: None,
                        sql: format!("SELECT * FROM orders WHERE amount > {}", i % 500),
                        updated_at_unix_ms: 0,
                    };
                    engine.register(spec).unwrap();
                }

                let event = make_test_event();

                // Benchmark dispatch
                b.iter(|| {
                    let users = engine.users(black_box(&event)).unwrap();
                    black_box(users);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Index efficiency (equality vs range vs fallback)
fn index_efficiency_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_efficiency");

    // Equality predicates (best case)
    group.bench_function("equality_predicates", |b| {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register 1000 equality predicates
        for i in 0..1000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: format!("SELECT * FROM orders WHERE amount = {}", i * 10),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        let event = make_test_event();

        b.iter(|| {
            let users = engine.users(black_box(&event)).unwrap();
            black_box(users);
        });
    });

    // Range predicates (moderate case)
    group.bench_function("range_predicates", |b| {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register 1000 range predicates
        for i in 0..1000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: format!("SELECT * FROM orders WHERE amount > {}", i * 10),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        let event = make_test_event();

        b.iter(|| {
            let users = engine.users(black_box(&event)).unwrap();
            black_box(users);
        });
    });

    // Complex predicates (fallback case)
    group.bench_function("complex_predicates", |b| {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register 1000 complex predicates
        for i in 0..1000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: format!(
                    "SELECT * FROM orders WHERE amount > {} OR priority = {}",
                    i * 10,
                    i % 10
                ),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        let event = make_test_event();

        b.iter(|| {
            let users = engine.users(black_box(&event)).unwrap();
            black_box(users);
        });
    });

    group.finish();
}

/// Benchmark: Registration performance
fn registration_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("registration");

    // Fresh registration (new predicate)
    group.bench_function("new_predicate", |b| {
        b.iter_batched(
            || {
                let catalog = Arc::new(BenchCatalog::new());
                SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {})
            },
            |mut engine| {
                let spec = SubscriptionSpec {
                    subscription_id: 1,
                    user_id: 42,
                    session_id: None,
                    sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                    updated_at_unix_ms: 0,
                };
                black_box(engine.register(spec).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Reused registration (existing predicate)
    group.bench_function("reused_predicate", |b| {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Pre-register predicate
        let spec = SubscriptionSpec {
            subscription_id: 1,
            user_id: 42,
            session_id: None,
            sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
            updated_at_unix_ms: 0,
        };
        engine.register(spec).unwrap();

        let mut next_id = 2;

        b.iter(|| {
            let spec = SubscriptionSpec {
                subscription_id: next_id,
                user_id: next_id,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            };
            next_id += 1;
            black_box(engine.register(spec).unwrap());
        });
    });

    group.finish();
}

/// Benchmark: Deduplication efficiency
fn deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("deduplication");

    // Many users, same predicate (best dedup)
    group.bench_function("high_dedup_ratio", |b| {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register same predicate for 1000 users
        for i in 0..1000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        let event = make_test_event();

        b.iter(|| {
            let users = engine.users(black_box(&event)).unwrap();
            black_box(users);
        });
    });

    // Every user has unique predicate (no dedup)
    group.bench_function("low_dedup_ratio", |b| {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register unique predicate per user
        for i in 0..1000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: format!("SELECT * FROM orders WHERE amount > {i}"),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        let event = make_test_event();

        b.iter(|| {
            let users = engine.users(black_box(&event)).unwrap();
            black_box(users);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    dispatch_scaling_benchmark,
    index_efficiency_benchmark,
    registration_benchmark,
    deduplication_benchmark,
);
criterion_main!(benches);
