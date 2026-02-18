//! SubQL CLI - Command-line interface for subscription management

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use subql::{
    config::{load_config, Config},
    SubscriptionEngine, SubscriptionSpec, SchemaCatalog, TableId,
};
use sqlparser::dialect::{PostgreSqlDialect, MySqlDialect, SQLiteDialect};
use std::collections::HashMap;

#[derive(Parser)]
#[command(name = "subql")]
#[command(version, about = "SQL subscription dispatch engine", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "subql.toml")]
    config: PathBuf,

    /// Command to execute
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the service in daemon mode
    Serve,

    /// Register a subscription
    Register {
        /// SQL WHERE clause (without SELECT/FROM)
        sql: String,

        /// User ID
        #[arg(short, long)]
        user_id: u64,

        /// Subscription ID (auto-generated if not provided)
        #[arg(short, long)]
        subscription_id: Option<u64>,

        /// Table name
        #[arg(short, long)]
        table: String,
    },

    /// List all subscriptions
    List {
        /// Table ID to filter by
        #[arg(short, long)]
        table_id: Option<u32>,
    },

    /// Snapshot table to disk
    Snapshot {
        /// Table ID
        table_id: u32,
    },

    /// Show statistics
    Stats,
}

/// Mock schema catalog for demonstration
struct MockCatalog {
    tables: HashMap<String, (TableId, usize)>,
    columns: HashMap<(TableId, String), u16>,
    fingerprints: HashMap<TableId, u64>,
}

impl MockCatalog {
    fn new() -> Self {
        let mut catalog = Self {
            tables: HashMap::new(),
            columns: HashMap::new(),
            fingerprints: HashMap::new(),
        };

        // Add default "orders" table
        catalog.tables.insert("orders".to_string(), (1, 5));
        catalog.columns.insert((1, "id".to_string()), 0);
        catalog.columns.insert((1, "user_id".to_string()), 1);
        catalog.columns.insert((1, "amount".to_string()), 2);
        catalog.columns.insert((1, "status".to_string()), 3);
        catalog.columns.insert((1, "created_at".to_string()), 4);
        catalog.fingerprints.insert(1, 0x1234567890ABCDEF);

        // Add "users" table
        catalog.tables.insert("users".to_string(), (2, 3));
        catalog.columns.insert((2, "id".to_string()), 0);
        catalog.columns.insert((2, "name".to_string()), 1);
        catalog.columns.insert((2, "email".to_string()), 2);
        catalog.fingerprints.insert(2, 0xABCDEF1234567890);

        catalog
    }
}

impl SchemaCatalog for MockCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.tables.get(table_name).map(|(id, _)| *id)
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
        self.columns.get(&(table_id, column_name.to_string())).copied()
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.tables.values()
            .find(|(id, _)| *id == table_id)
            .map(|(_, arity)| *arity)
    }

    fn schema_fingerprint(&self, table_id: TableId) -> Option<u64> {
        self.fingerprints.get(&table_id).copied()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load configuration
    let config = load_config(&args.config)?;

    match args.command {
        Commands::Serve => serve_mode(config),
        Commands::Register { sql, user_id, subscription_id, table } => {
            register_command(config, sql, user_id, subscription_id, table)
        }
        Commands::List { table_id } => list_command(config, table_id),
        Commands::Snapshot { table_id } => snapshot_command(config, table_id),
        Commands::Stats => stats_command(config),
    }
}

fn serve_mode(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    println!("SubQL Service Starting...");
    println!("Storage path: {}", config.storage_path.display());
    println!("Rotation threshold: {} bytes", config.rotation_threshold);
    println!();

    // Create catalog
    let catalog = Arc::new(MockCatalog::new());

    // Create engine based on dialect
    match config.catalog.dialect.as_str() {
        "postgres" => {
            let _engine = SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                config.storage_path.clone(),
            )?;
            println!("✓ Engine initialized (PostgreSQL dialect)");
        }
        "mysql" => {
            let _engine = SubscriptionEngine::with_storage(
                catalog,
                MySqlDialect {},
                config.storage_path.clone(),
            )?;
            println!("✓ Engine initialized (MySQL dialect)");
        }
        "sqlite" => {
            let _engine = SubscriptionEngine::with_storage(
                catalog,
                SQLiteDialect {},
                config.storage_path.clone(),
            )?;
            println!("✓ Engine initialized (SQLite dialect)");
        }
        _ => {
            eprintln!("Warning: Unknown dialect '{}', defaulting to PostgreSQL", config.catalog.dialect);
            let _engine = SubscriptionEngine::with_storage(
                catalog,
                PostgreSqlDialect {},
                config.storage_path.clone(),
            )?;
            println!("✓ Engine initialized (PostgreSQL dialect - default)");
        }
    }

    println!("✓ Storage directory ready");
    println!();
    println!("Service started. Press Ctrl+C to stop.");
    println!();
    println!("In production, this would:");
    println!("  - Listen for CDC events (Debezium, pg_logical_replication, etc.)");
    println!("  - Dispatch events to interested users");
    println!("  - Periodically snapshot to disk");
    println!("  - Run background merge jobs");

    // Keep running (in production, would handle signals properly)
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn register_command(
    config: Config,
    sql: String,
    user_id: u64,
    subscription_id: Option<u64>,
    table: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create catalog
    let catalog = Arc::new(MockCatalog::new());

    // Use PostgreSQL dialect by default for register command
    let mut engine = SubscriptionEngine::with_storage(
        catalog.clone(),
        PostgreSqlDialect {},
        config.storage_path.clone(),
    )?;

    // Get table ID
    let table_id = catalog.table_id(&table)
        .ok_or_else(|| format!("Unknown table: {}", table))?;

    // Auto-generate subscription ID if not provided
    let sub_id = subscription_id.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    });

    // Build full SQL SELECT statement
    let full_sql = format!("SELECT * FROM {} WHERE {}", table, sql);

    // Create subscription spec
    let spec = SubscriptionSpec {
        subscription_id: sub_id,
        user_id,
        session_id: None,
        sql: full_sql,
        updated_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };

    // Register
    let result = engine.register(spec)?;

    // Always snapshot after registration (for CLI persistence)
    engine.snapshot_table(table_id)?;

    println!("✓ Subscription registered successfully");
    println!();
    println!("  Subscription ID: {}", sub_id);
    println!("  User ID:         {}", user_id);
    println!("  Table ID:        {}", table_id);
    println!("  Table:           {}", table);
    println!("  Predicate Hash:  {:032x}", result.predicate_hash);
    println!("  Normalized SQL:  {}", result.normalized_sql);
    println!("  New Predicate:   {}", if result.created_new_predicate { "Yes (created)" } else { "No (reused existing)" });
    println!();
    println!("✓ Changes saved to disk");

    Ok(())
}

fn list_command(config: Config, table_id: Option<u32>) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(MockCatalog::new());
    let engine = SubscriptionEngine::with_storage(
        catalog,
        PostgreSqlDialect {},
        config.storage_path.clone(),
    )?;

    let total_subs = engine.subscription_count();

    println!("SubQL Subscription List");
    println!("=======================");
    println!();

    if let Some(tid) = table_id {
        let count = engine.predicate_count(tid);
        println!("Table ID: {}", tid);
        println!("Predicates: {}", count);
    } else {
        println!("Total subscriptions: {}", total_subs);
        println!();
        println!("Use --table-id <ID> to filter by table");
    }

    Ok(())
}

fn snapshot_command(config: Config, table_id: u32) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(MockCatalog::new());
    let engine = SubscriptionEngine::with_storage(
        catalog,
        PostgreSqlDialect {},
        config.storage_path.clone(),
    )?;

    println!("Creating snapshot for table {}...", table_id);

    engine.snapshot_table(table_id)?;

    println!("✓ Snapshot saved successfully");
    println!();
    println!("  Table ID: {}", table_id);
    println!("  Storage:  {}", config.storage_path.display());

    Ok(())
}

fn stats_command(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(MockCatalog::new());
    let engine = SubscriptionEngine::with_storage(
        catalog,
        PostgreSqlDialect {},
        config.storage_path.clone(),
    )?;

    println!("SubQL Engine Statistics");
    println!("=======================");
    println!();
    println!("Total subscriptions: {}", engine.subscription_count());
    println!("Storage path:        {}", config.storage_path.display());
    println!("Rotation threshold:  {} bytes ({} MB)",
        engine.rotation_threshold(),
        engine.rotation_threshold() / 1024 / 1024);
    println!();
    println!("Configuration:");
    println!("  Merge threshold:   {} shards", config.merge.shard_threshold);
    println!("  Merge interval:    {} seconds", config.merge.interval_secs);
    println!("  Dialect:           {}", config.catalog.dialect);

    Ok(())
}

