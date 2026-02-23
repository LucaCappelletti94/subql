//! Memory profiling with dhat
//!
//! Run with: cargo run --release --features dhat-heap --bin memory_profile
#![allow(clippy::unwrap_used, clippy::unreadable_literal)]
#![allow(clippy::print_stdout, clippy::unnecessary_cast)]

use sqlparser::dialect::PostgreSqlDialect;
use std::collections::HashMap;
use std::sync::Arc;
use subql::{
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SchemaCatalog, SubscriptionEngine,
    SubscriptionSpec, TableId, WalEvent,
};

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const STATUS_BUCKETS: [&str; 7] = [
    "pending",
    "active",
    "paid",
    "shipped",
    "cancelled",
    "fraud_hold",
    "backorder",
];

const fn mix_seed(mut value: u64) -> u64 {
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

const fn status_for(seed: u64) -> &'static str {
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

const fn realistic_workload_seed(subscription_ix: u64) -> u64 {
    // Most subscribers belong to repeatable "hot" cohorts, with a long-tail.
    let hot_cohort = subscription_ix % 2_048;
    let long_tail = mix_seed(subscription_ix ^ 0xA53A_9E37_79B9_7F4A);
    if subscription_ix.is_multiple_of(5) {
        long_tail
    } else {
        hot_cohort
    }
}

// Mock catalog
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
    let discount = if mix_seed(seed ^ 0x6F7F).is_multiple_of(5) {
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

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let show_progress = std::env::var_os("SUBQL_MEMORY_BENCH_PROGRESS").is_some();

    println!("SubQL Memory Profiling");
    println!("======================");
    println!();

    let catalog = Arc::new(BenchCatalog::new());
    let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

    println!("Registering 100,000 predicates with realistic tree shapes...");
    if !show_progress {
        println!("Progress logs disabled (set SUBQL_MEMORY_BENCH_PROGRESS=1 to enable)");
    }

    for i in 0_u64..100_000 {
        let spec = SubscriptionSpec {
            subscription_id: i,
            user_id: i % 10_000,
            session_id: None,
            sql: realistic_tree_sql(realistic_workload_seed(i)),
            updated_at_unix_ms: 0,
        };
        engine.register(spec).unwrap();

        if show_progress && (i + 1) % 10_000 == 0 {
            println!("  {} predicates registered", i + 1);
        }
    }

    println!();
    println!("Dispatching 1,000 events from a rotating event corpus...");

    let event_corpus: Vec<WalEvent> = (0_u64..32)
        .map(|seed| make_test_event(seed ^ 0x1234_5678_9ABC_DEF0))
        .collect();
    let event_corpus_len_u64 = u64::try_from(event_corpus.len()).unwrap_or(1);

    for i in 0_u64..1_000 {
        let event_idx_u64 = i % event_corpus_len_u64;
        let event_idx = usize::try_from(event_idx_u64).unwrap_or(0);
        let user_count = engine.users(&event_corpus[event_idx]).unwrap().count();

        if show_progress && (i + 1) % 100 == 0 {
            println!(
                "  {} events dispatched (matched {} users)",
                i + 1,
                user_count
            );
        }
    }

    println!();
    println!("Memory profiling complete!");
    println!();
    println!("Results:");
    println!("  Total subscriptions: {}", engine.subscription_count());
    println!();

    #[cfg(feature = "dhat-heap")]
    println!("Check dhat-heap.json for detailed memory profile");

    #[cfg(not(feature = "dhat-heap"))]
    println!("Run with --features dhat-heap to enable memory profiling");
}
