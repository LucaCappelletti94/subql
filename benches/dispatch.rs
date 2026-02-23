//! Dispatch performance benchmarks
#![allow(clippy::unwrap_used, clippy::unreadable_literal)]
#![allow(clippy::cast_sign_loss)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use sqlparser::dialect::PostgreSqlDialect;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use subql::{
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SchemaCatalog, SubscriptionEngine,
    SubscriptionSpec, TableId, WalEvent,
};

const STATUS_BUCKETS: [&str; 7] = [
    "pending",
    "active",
    "paid",
    "shipped",
    "cancelled",
    "fraud_hold",
    "backorder",
];
const EVENT_CORPUS_SIZE: usize = 64;
const EVENT_CORPUS_SALT: u64 = 0xA1B2_C3D4_E5F6_7788;

fn mix_seed(mut value: u64) -> u64 {
    // SplitMix64 finalizer: deterministic pseudo-randomness for stable benches.
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn bounded_i64(seed: u64, modulo: u64) -> i64 {
    i64::try_from(mix_seed(seed) % modulo).unwrap_or(0)
}

fn status_for(seed: u64) -> &'static str {
    match mix_seed(seed) % 7 {
        0 => STATUS_BUCKETS[0],
        1 => STATUS_BUCKETS[1],
        2 => STATUS_BUCKETS[2],
        3 => STATUS_BUCKETS[3],
        4 => STATUS_BUCKETS[4],
        5 => STATUS_BUCKETS[5],
        _ => STATUS_BUCKETS[6],
    }
}

fn equality_tree_sql(seed: u64) -> String {
    let amount = 25 + bounded_i64(seed ^ 0x11, 600);
    let priority = 1 + bounded_i64(seed ^ 0x22, 9);
    let shipping = 5 + bounded_i64(seed ^ 0x33, 20);
    let tax = 2 + bounded_i64(seed ^ 0x44, 30);
    let user_bucket = bounded_i64(seed ^ 0x55, 20_000);
    let status_a = status_for(seed ^ 0x66);
    let status_b = status_for(seed ^ 0x77);

    format!(
        "SELECT * FROM orders WHERE \
         (((amount = {amount} AND status = '{status_a}') \
           OR (user_id = {user_bucket} AND priority = {priority})) \
          AND shipping = {shipping}) \
         OR (status = '{status_b}' AND tax = {tax})"
    )
}

fn range_tree_sql(seed: u64) -> String {
    let min_amount = 50 + bounded_i64(seed ^ 0x111, 1_500);
    let max_amount = min_amount + 40 + bounded_i64(seed ^ 0x222, 500);
    let priority_floor = 1 + bounded_i64(seed ^ 0x333, 9);
    let qty_min = 1 + bounded_i64(seed ^ 0x444, 25);
    let qty_max = qty_min + 3 + bounded_i64(seed ^ 0x555, 20);
    let created_after = 1_700_000_000 + bounded_i64(seed ^ 0x666, 120 * 24 * 3600);
    let status_a = status_for(seed ^ 0x777);
    let status_b = status_for(seed ^ 0x888);

    format!(
        "SELECT * FROM orders WHERE \
         (((amount BETWEEN {min_amount} AND {max_amount}) AND priority >= {priority_floor}) \
           OR (created_at >= {created_after} AND quantity BETWEEN {qty_min} AND {qty_max})) \
         AND (status IN ('{status_a}', '{status_b}') OR discount IS NULL)"
    )
}

fn mixed_tree_sql(seed: u64) -> String {
    let amount_floor = 120 + bounded_i64(seed ^ 0x1010, 2_000);
    let amount_ceiling = amount_floor + 40 + bounded_i64(seed ^ 0x2020, 900);
    let priority_floor = 1 + bounded_i64(seed ^ 0x3030, 9);
    let quantity_floor = 1 + bounded_i64(seed ^ 0x4040, 35);
    let created_from = 1_698_000_000 + bounded_i64(seed ^ 0x5050, 180 * 24 * 3600);
    let created_to = created_from + 8 * 24 * 3600 + bounded_i64(seed ^ 0x6060, 20 * 24 * 3600);
    let discount_cap = bounded_i64(seed ^ 0x7070, 15);
    let modulus = 7 + bounded_i64(seed ^ 0x8080, 17);
    let residue = bounded_i64(seed ^ 0x9090, u64::try_from(modulus).unwrap_or(1));
    let status_a = status_for(seed ^ 0xAAAA);
    let status_b = status_for(seed ^ 0xBBBB);

    format!(
        "SELECT * FROM orders WHERE \
         ((((amount > {amount_floor} AND amount < {amount_ceiling}) \
            AND (status = '{status_a}' OR status = '{status_b}')) \
           AND (priority >= {priority_floor} OR quantity >= {quantity_floor})) \
          OR ((discount IS NULL OR discount <= {discount_cap}) \
              AND created_at BETWEEN {created_from} AND {created_to})) \
         AND (user_id % {modulus} = {residue})"
    )
}

fn fallback_tree_sql(seed: u64) -> String {
    let value_floor = 400 + bounded_i64(seed ^ 0xAAAA_1111, 12_000);
    let shipping_tax_floor = 10 + bounded_i64(seed ^ 0xBBBB_2222, 250);
    let priority_floor = 1 + bounded_i64(seed ^ 0xCCCC_3333, 9);
    let created_from = 1_699_000_000 + bounded_i64(seed ^ 0xDDDD_4444, 210 * 24 * 3600);
    let created_to = created_from + 3 * 24 * 3600 + bounded_i64(seed ^ 0xEEEE_5555, 45 * 24 * 3600);
    let amount_floor = 90 + bounded_i64(seed ^ 0xFFFF_6666, 1_500);
    let status_prefix = match mix_seed(seed ^ 0xABCD) % 4 {
        0 => "act",
        1 => "pend",
        2 => "ship",
        _ => "fraud",
    };

    format!(
        "SELECT * FROM orders WHERE \
         ((((amount * quantity) > {value_floor}) AND status ILIKE '{status_prefix}%') \
           OR ((shipping + tax) > {shipping_tax_floor} \
               AND created_at BETWEEN {created_from} AND {created_to})) \
         AND (priority >= {priority_floor} OR amount > {amount_floor})"
    )
}

fn realistic_tree_sql(seed: u64) -> String {
    match mix_seed(seed) % 10 {
        0..=2 => equality_tree_sql(seed),
        3..=6 => range_tree_sql(seed),
        7..=8 => mixed_tree_sql(seed),
        _ => fallback_tree_sql(seed),
    }
}

fn scaling_equality_sql(seed: u64) -> String {
    let amount = 25 + bounded_i64(seed ^ 0x1111, 1_200);
    let priority_floor = 1 + bounded_i64(seed ^ 0x2222, 9);
    let shipping_floor = 2 + bounded_i64(seed ^ 0x3333, 20);
    let status = status_for(seed ^ 0x4444);
    format!(
        "SELECT * FROM orders WHERE \
         amount = {amount} \
         AND status = '{status}' \
         AND priority >= {priority_floor} \
         AND shipping >= {shipping_floor}"
    )
}

fn scaling_range_sql(seed: u64) -> String {
    let min_amount = 50 + bounded_i64(seed ^ 0x5555, 2_000);
    let max_amount = min_amount + 30 + bounded_i64(seed ^ 0x6666, 600);
    let qty_min = 1 + bounded_i64(seed ^ 0x7777, 20);
    let qty_max = qty_min + 2 + bounded_i64(seed ^ 0x8888, 15);
    let created_after = 1_700_000_000 + bounded_i64(seed ^ 0x9999, 180 * 24 * 3600);
    format!(
        "SELECT * FROM orders WHERE \
         amount BETWEEN {min_amount} AND {max_amount} \
         AND quantity BETWEEN {qty_min} AND {qty_max} \
         AND created_at >= {created_after} \
         AND priority >= 2"
    )
}

fn scaling_mixed_sql(seed: u64) -> String {
    let amount_floor = 80 + bounded_i64(seed ^ 0xAAAA, 2_500);
    let amount_ceiling = amount_floor + 50 + bounded_i64(seed ^ 0xBBBB, 700);
    let priority_floor = 1 + bounded_i64(seed ^ 0xCCCC, 9);
    let status_a = status_for(seed ^ 0xDDDD);
    let status_b = status_for(seed ^ 0xEEEE);
    format!(
        "SELECT * FROM orders WHERE \
         status IN ('{status_a}', '{status_b}') \
         AND amount > {amount_floor} \
         AND amount < {amount_ceiling} \
         AND priority >= {priority_floor} \
         AND discount IS NOT NULL"
    )
}

fn scaling_tree_sql(seed: u64) -> String {
    // Scaling benchmark is meant to model a mostly indexable production mix,
    // with a smaller fallback tail so runtime stays practical.
    match mix_seed(seed) % 100 {
        0..=44 => scaling_equality_sql(seed),
        45..=89 => scaling_range_sql(seed),
        90..=98 => scaling_mixed_sql(seed),
        _ => fallback_tree_sql(seed),
    }
}

fn realistic_workload_seed(subscription_ix: u64) -> u64 {
    // Most subscribers belong to repeatable "hot" cohorts, with a long-tail.
    let hot_cohort = subscription_ix % 2_048;
    let long_tail = mix_seed(subscription_ix ^ 0xA53A_9E37_79B9_7F4A);
    if subscription_ix % 5 == 0 {
        long_tail
    } else {
        hot_cohort
    }
}

fn build_scaling_engine(
    predicate_count: usize,
) -> SubscriptionEngine<PostgreSqlDialect, DefaultIds> {
    let catalog = Arc::new(BenchCatalog::new());
    let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

    for i in 0..predicate_count {
        let i_u64 = u64::try_from(i).unwrap_or(0);
        let spec = SubscriptionSpec {
            subscription_id: i_u64,
            user_id: i_u64 % 5_000,
            session_id: None,
            sql: scaling_tree_sql(realistic_workload_seed(i_u64)),
            updated_at_unix_ms: 0,
        };
        engine.register(spec).unwrap();
    }

    engine
}

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

fn make_test_event(seed: u64) -> WalEvent {
    let id = 1 + bounded_i64(seed ^ 0x1A2A, 500_000);
    let user_id = bounded_i64(seed ^ 0x2B3B, 20_000);
    let amount = 30 + bounded_i64(seed ^ 0x3C4C, 3_500);
    let priority = 1 + bounded_i64(seed ^ 0x4D5D, 9);
    let quantity = 1 + bounded_i64(seed ^ 0x5E6E, 40);
    let discount = if mix_seed(seed ^ 0x6F7F) % 5 == 0 {
        Cell::Null
    } else {
        Cell::Int(bounded_i64(seed ^ 0x7A8A, 18))
    };
    let tax = 2 + bounded_i64(seed ^ 0x8B9B, 40);
    let shipping = 4 + bounded_i64(seed ^ 0x9CAC, 30);
    let created_at = 1_699_500_000 + bounded_i64(seed ^ 0xADBD, 240 * 24 * 3600);
    let status = status_for(seed ^ 0xBECF);

    WalEvent {
        kind: EventKind::Insert,
        table_id: 1,
        pk: PrimaryKey {
            columns: Arc::from([0u16]),
            values: Arc::from([Cell::Int(id)]),
        },
        old_row: None,
        new_row: Some(RowImage {
            cells: Arc::from([
                Cell::Int(id),
                Cell::Int(user_id),
                Cell::Int(amount),
                Cell::String(status.into()),
                Cell::Int(priority),
                Cell::Int(quantity),
                discount,
                Cell::Int(tax),
                Cell::Int(shipping),
                Cell::Int(created_at),
            ]),
        }),
        changed_columns: Arc::from([]),
    }
}

fn make_test_update_event(seed: u64) -> WalEvent {
    let id = 1 + bounded_i64(seed ^ 0x1A2A, 500_000);
    let user_id = bounded_i64(seed ^ 0x2B3B, 20_000);
    let old_amount = 30 + bounded_i64(seed ^ 0x3C4C, 3_500);
    let old_priority = 1 + bounded_i64(seed ^ 0x4D5D, 9);
    let old_quantity = 1 + bounded_i64(seed ^ 0x5E6E, 40);
    let old_discount = if mix_seed(seed ^ 0x6F7F) % 5 == 0 {
        Cell::Null
    } else {
        Cell::Int(bounded_i64(seed ^ 0x7A8A, 18))
    };
    let old_tax = 2 + bounded_i64(seed ^ 0x8B9B, 40);
    let old_shipping = 4 + bounded_i64(seed ^ 0x9CAC, 30);
    let old_created_at = 1_699_500_000 + bounded_i64(seed ^ 0xADBD, 240 * 24 * 3600);
    let old_status = status_for(seed ^ 0xBECF);

    let mut new_amount = old_amount;
    let mut new_priority = old_priority;
    let mut new_quantity = old_quantity;
    let mut new_discount = old_discount.clone();
    let mut new_tax = old_tax;
    let mut new_shipping = old_shipping;
    let mut new_status = old_status;

    let changed_columns: Arc<[u16]> = match mix_seed(seed ^ 0xDEAD_BEEF) % 4 {
        0 => {
            new_amount = old_amount + 1 + bounded_i64(seed ^ 0x1111_2222, 40);
            Arc::from([2u16])
        }
        1 => {
            new_status = status_for(seed ^ 0x3333_4444);
            Arc::from([3u16])
        }
        2 => {
            new_priority = old_priority + 1;
            new_quantity = old_quantity + 2;
            Arc::from([4u16, 5u16])
        }
        _ => {
            new_discount = if old_discount.is_null() {
                Cell::Int(1 + bounded_i64(seed ^ 0x5555_6666, 12))
            } else {
                Cell::Null
            };
            new_tax = old_tax + 1;
            new_shipping = old_shipping + 1;
            Arc::from([6u16, 7u16, 8u16])
        }
    };

    WalEvent {
        kind: EventKind::Update,
        table_id: 1,
        pk: PrimaryKey {
            columns: Arc::from([0u16]),
            values: Arc::from([Cell::Int(id)]),
        },
        old_row: Some(RowImage {
            cells: Arc::from([
                Cell::Int(id),
                Cell::Int(user_id),
                Cell::Int(old_amount),
                Cell::String(old_status.into()),
                Cell::Int(old_priority),
                Cell::Int(old_quantity),
                old_discount,
                Cell::Int(old_tax),
                Cell::Int(old_shipping),
                Cell::Int(old_created_at),
            ]),
        }),
        new_row: Some(RowImage {
            cells: Arc::from([
                Cell::Int(id),
                Cell::Int(user_id),
                Cell::Int(new_amount),
                Cell::String(new_status.into()),
                Cell::Int(new_priority),
                Cell::Int(new_quantity),
                new_discount,
                Cell::Int(new_tax),
                Cell::Int(new_shipping),
                Cell::Int(old_created_at),
            ]),
        }),
        changed_columns,
    }
}

fn make_test_delete_event(seed: u64) -> WalEvent {
    let id = 1 + bounded_i64(seed ^ 0x1A2A, 500_000);
    let user_id = bounded_i64(seed ^ 0x2B3B, 20_000);
    let amount = 30 + bounded_i64(seed ^ 0x3C4C, 3_500);
    let priority = 1 + bounded_i64(seed ^ 0x4D5D, 9);
    let quantity = 1 + bounded_i64(seed ^ 0x5E6E, 40);
    let discount = if mix_seed(seed ^ 0x6F7F) % 5 == 0 {
        Cell::Null
    } else {
        Cell::Int(bounded_i64(seed ^ 0x7A8A, 18))
    };
    let tax = 2 + bounded_i64(seed ^ 0x8B9B, 40);
    let shipping = 4 + bounded_i64(seed ^ 0x9CAC, 30);
    let created_at = 1_699_500_000 + bounded_i64(seed ^ 0xADBD, 240 * 24 * 3600);
    let status = status_for(seed ^ 0xBECF);

    WalEvent {
        kind: EventKind::Delete,
        table_id: 1,
        pk: PrimaryKey {
            columns: Arc::from([0u16]),
            values: Arc::from([Cell::Int(id)]),
        },
        old_row: Some(RowImage {
            cells: Arc::from([
                Cell::Int(id),
                Cell::Int(user_id),
                Cell::Int(amount),
                Cell::String(status.into()),
                Cell::Int(priority),
                Cell::Int(quantity),
                discount,
                Cell::Int(tax),
                Cell::Int(shipping),
                Cell::Int(created_at),
            ]),
        }),
        new_row: None,
        changed_columns: Arc::from([]),
    }
}

fn event_corpus(size: usize, salt: u64, make_event: fn(u64) -> WalEvent) -> Vec<WalEvent> {
    (0..size)
        .map(|i| {
            let i_u64 = u64::try_from(i).unwrap_or(0);
            make_event(mix_seed(i_u64 ^ salt))
        })
        .collect()
}

/// Benchmark: Dispatch scaling with increasing predicate count
fn dispatch_scaling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_scaling");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for &predicate_count in &[100, 1_000, 10_000] {
        let mut engine = build_scaling_engine(predicate_count);
        let events = event_corpus(EVENT_CORPUS_SIZE, EVENT_CORPUS_SALT, make_test_event);
        let mut next_event_idx = 0_usize;

        group.bench_with_input(
            BenchmarkId::from_parameter(predicate_count),
            &predicate_count,
            |b, &_count| {
                // Benchmark dispatch
                b.iter(|| {
                    let event = &events[next_event_idx];
                    next_event_idx = (next_event_idx + 1) % events.len();
                    let matched_users = engine.users(black_box(event)).unwrap().count();
                    black_box(matched_users);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: UPDATE and DELETE dispatch scaling with increasing predicate count
fn dispatch_kind_scaling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_kind_scaling");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for &predicate_count in &[100, 1_000, 10_000] {
        let mut update_engine = build_scaling_engine(predicate_count);
        let update_events = event_corpus(
            EVENT_CORPUS_SIZE,
            EVENT_CORPUS_SALT ^ 0x1111_2222_3333_4444,
            make_test_update_event,
        );
        let mut next_update_event_idx = 0_usize;

        group.bench_with_input(
            BenchmarkId::new("update", predicate_count),
            &predicate_count,
            |b, &_count| {
                b.iter(|| {
                    let event = &update_events[next_update_event_idx];
                    next_update_event_idx = (next_update_event_idx + 1) % update_events.len();
                    let matched_users = update_engine.users(black_box(event)).unwrap().count();
                    black_box(matched_users);
                });
            },
        );

        let mut delete_engine = build_scaling_engine(predicate_count);
        let delete_events = event_corpus(
            EVENT_CORPUS_SIZE,
            EVENT_CORPUS_SALT ^ 0x5555_6666_7777_8888,
            make_test_delete_event,
        );
        let mut next_delete_event_idx = 0_usize;

        group.bench_with_input(
            BenchmarkId::new("delete", predicate_count),
            &predicate_count,
            |b, &_count| {
                b.iter(|| {
                    let event = &delete_events[next_delete_event_idx];
                    next_delete_event_idx = (next_delete_event_idx + 1) % delete_events.len();
                    let matched_users = delete_engine.users(black_box(event)).unwrap().count();
                    black_box(matched_users);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Index efficiency (equality vs range vs fallback)
fn index_efficiency_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_efficiency");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    let mut equality_engine = {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register 1000 equality-dominant trees.
        for i in 0_u64..1_000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: equality_tree_sql(i),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        engine
    };
    let equality_events = event_corpus(EVENT_CORPUS_SIZE, EVENT_CORPUS_SALT, make_test_event);
    let mut equality_event_idx = 0_usize;

    // Equality predicates (best case)
    group.bench_function("equality_predicates", |b| {
        b.iter(|| {
            let event = &equality_events[equality_event_idx];
            equality_event_idx = (equality_event_idx + 1) % equality_events.len();
            let matched_users = equality_engine.users(black_box(event)).unwrap().count();
            black_box(matched_users);
        });
    });

    let mut range_engine = {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register 1000 range-heavy trees.
        for i in 0_u64..1_000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: range_tree_sql(i ^ 0xABCD_0000),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        engine
    };
    let range_events = event_corpus(EVENT_CORPUS_SIZE, EVENT_CORPUS_SALT, make_test_event);
    let mut range_event_idx = 0_usize;

    // Range predicates (moderate case)
    group.bench_function("range_predicates", |b| {
        b.iter(|| {
            let event = &range_events[range_event_idx];
            range_event_idx = (range_event_idx + 1) % range_events.len();
            let matched_users = range_engine.users(black_box(event)).unwrap().count();
            black_box(matched_users);
        });
    });

    let mut complex_engine = {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register 1000 fallback-heavy trees.
        for i in 0_u64..1_000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: fallback_tree_sql(i ^ 0xFFFF_0000),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        engine
    };
    let complex_events = event_corpus(EVENT_CORPUS_SIZE, EVENT_CORPUS_SALT, make_test_event);
    let mut complex_event_idx = 0_usize;

    // Complex predicates (fallback case)
    group.bench_function("complex_predicates", |b| {
        b.iter(|| {
            let event = &complex_events[complex_event_idx];
            complex_event_idx = (complex_event_idx + 1) % complex_events.len();
            let matched_users = complex_engine.users(black_box(event)).unwrap().count();
            black_box(matched_users);
        });
    });

    group.finish();
}

/// Benchmark: Registration performance
fn registration_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("registration");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    // Fresh registration (new predicate)
    group.bench_function("new_predicate", |b| {
        let mut next_seed = 1_u64;
        b.iter_batched(
            || {
                let catalog = Arc::new(BenchCatalog::new());
                let engine =
                    SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});
                let seed = next_seed;
                next_seed = next_seed.wrapping_add(1);
                (engine, seed)
            },
            |(mut engine, seed)| {
                let spec = SubscriptionSpec {
                    subscription_id: 1,
                    user_id: 42,
                    session_id: None,
                    sql: realistic_tree_sql(seed),
                    updated_at_unix_ms: 0,
                };
                black_box(engine.register(spec).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Reused registration (existing predicate)
    group.bench_function("reused_predicate", |b| {
        let mut next_seed = 1_u64;
        b.iter_batched(
            || {
                let catalog = Arc::new(BenchCatalog::new());
                let mut engine =
                    SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

                // Pre-register one predicate so the measured operation always
                // takes the dedup/reuse path against a stable engine state.
                let seed = next_seed;
                next_seed = next_seed.wrapping_add(1);
                let shared_sql = realistic_tree_sql(seed);
                engine
                    .register(SubscriptionSpec {
                        subscription_id: 1,
                        user_id: 42,
                        session_id: None,
                        sql: shared_sql.clone(),
                        updated_at_unix_ms: 0,
                    })
                    .unwrap();

                (engine, shared_sql)
            },
            |(mut engine, shared_sql)| {
                let spec = SubscriptionSpec {
                    subscription_id: 2,
                    user_id: 99,
                    session_id: None,
                    sql: shared_sql,
                    updated_at_unix_ms: 0,
                };
                black_box(engine.register(spec).unwrap());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark: Deduplication efficiency
fn deduplication_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("deduplication");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    let mut high_dedup_engine = {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register same realistic tree for 1000 users.
        let shared_sql = mixed_tree_sql(13_579);
        for i in 0_u64..1_000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: shared_sql.clone(),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        engine
    };
    let high_dedup_events = event_corpus(EVENT_CORPUS_SIZE, EVENT_CORPUS_SALT, make_test_event);
    let mut high_dedup_event_idx = 0_usize;

    // Many users, same predicate (best dedup)
    group.bench_function("high_dedup_ratio", |b| {
        b.iter(|| {
            let event = &high_dedup_events[high_dedup_event_idx];
            high_dedup_event_idx = (high_dedup_event_idx + 1) % high_dedup_events.len();
            let matched_users = high_dedup_engine.users(black_box(event)).unwrap().count();
            black_box(matched_users);
        });
    });

    let mut low_dedup_engine = {
        let catalog = Arc::new(BenchCatalog::new());
        let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

        // Register unique realistic tree per user.
        for i in 0_u64..1_000 {
            let spec = SubscriptionSpec {
                subscription_id: i,
                user_id: i,
                session_id: None,
                sql: realistic_tree_sql(mix_seed(i ^ 0xD1E5_EED0)),
                updated_at_unix_ms: 0,
            };
            engine.register(spec).unwrap();
        }

        engine
    };
    let low_dedup_events = event_corpus(EVENT_CORPUS_SIZE, EVENT_CORPUS_SALT, make_test_event);
    let mut low_dedup_event_idx = 0_usize;

    // Every user has unique predicate (no dedup)
    group.bench_function("low_dedup_ratio", |b| {
        b.iter(|| {
            let event = &low_dedup_events[low_dedup_event_idx];
            low_dedup_event_idx = (low_dedup_event_idx + 1) % low_dedup_events.len();
            let matched_users = low_dedup_engine.users(black_box(event)).unwrap().count();
            black_box(matched_users);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    dispatch_scaling_benchmark,
    dispatch_kind_scaling_benchmark,
    index_efficiency_benchmark,
    registration_benchmark,
    deduplication_benchmark,
);
criterion_main!(benches);
