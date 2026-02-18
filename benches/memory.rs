//! Memory profiling with dhat
//!
//! Run with: cargo run --release --features dhat-heap --bin memory_profile

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

        catalog.tables.insert("orders".to_string(), (1, 5));
        catalog.columns.insert((1, "id".to_string()), 0);
        catalog.columns.insert((1, "user_id".to_string()), 1);
        catalog.columns.insert((1, "amount".to_string()), 2);
        catalog.columns.insert((1, "status".to_string()), 3);
        catalog.columns.insert((1, "created_at".to_string()), 4);

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
                Cell::Int(1),
                Cell::Int(42),
                Cell::Int(250),
                Cell::String("active".into()),
                Cell::Int(1234567890),
            ]),
        }),
        changed_columns: Arc::from([]),
    }
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    println!("SubQL Memory Profiling");
    println!("======================");
    println!();

    let catalog = Arc::new(BenchCatalog::new());
    let mut engine = SubscriptionEngine::<_, DefaultIds>::new(catalog, PostgreSqlDialect {});

    println!("Registering 100,000 predicates...");

    // Register 100k predicates
    for i in 0..100_000 {
        let spec = SubscriptionSpec {
            subscription_id: i,
            user_id: (i % 10_000) as u64,
            session_id: None,
            sql: format!("SELECT * FROM orders WHERE amount > {}", i % 1000),
            updated_at_unix_ms: 0,
        };
        engine.register(spec).unwrap();

        if (i + 1) % 10_000 == 0 {
            println!("  {} predicates registered", i + 1);
        }
    }

    println!();
    println!("Dispatching 1,000 events...");

    let event = make_test_event();

    // Dispatch 1000 events
    for i in 0..1000 {
        let user_count = engine.users(&event).unwrap().count();

        if (i + 1) % 100 == 0 {
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
