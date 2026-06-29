use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::Rng;

use subql::Cell;

use super::{PresetSchema, PresetTable, Row};

const STATUSES: &[&str] = &["open", "pending", "shipped", "cancelled"];
const TIERS: &[&str] = &["free", "pro", "enterprise"];
const NAMES: &[&str] = &["acme", "globex", "initech", "umbrella", "hooli"];

pub const PRESET: PresetSchema = PresetSchema {
    name: "orders",
    pg_ddl: "CREATE TABLE customers (\n    id INT PRIMARY KEY,\n    name TEXT,\n    tier TEXT\n);\nCREATE TABLE orders (\n    id INT PRIMARY KEY,\n    customer_id INT,\n    amount INT,\n    status TEXT\n);",
    tables: &[
        PresetTable {
            table_name: "customers",
            columns: &["id", "name", "tier"],
            column_types: &["INT PK", "TEXT", "TEXT"],
            starter_queries: &[
                "SELECT * FROM customers WHERE tier = 'enterprise'",
                "SELECT COUNT(*) FROM customers WHERE tier = 'pro'",
            ],
            seed_rows: seed_customers,
            generator: generate_customer,
        },
        PresetTable {
            table_name: "orders",
            columns: &["id", "customer_id", "amount", "status"],
            column_types: &["INT PK", "INT", "INT", "TEXT"],
            starter_queries: &[
                "SELECT * FROM orders WHERE amount > 100",
                "SELECT * FROM orders WHERE status = 'open'",
                "SELECT AVG(amount) FROM orders WHERE status = 'shipped'",
            ],
            seed_rows: seed_orders,
            generator: generate_order,
        },
    ],
};

fn seed_customers() -> Vec<Row> {
    vec![
        customer(1, "acme", "pro"),
        customer(2, "globex", "free"),
        customer(3, "initech", "enterprise"),
        customer(4, "umbrella", "pro"),
    ]
}

fn customer(id: i64, name: &str, tier: &str) -> Row {
    vec![
        Cell::Int(id),
        Cell::String(Arc::from(name)),
        Cell::String(Arc::from(tier)),
    ]
}

fn generate_customer(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(1_000..1_000_000);
    let name = NAMES.choose(rng).copied().unwrap_or("acme");
    let tier = TIERS.choose(rng).copied().unwrap_or("free");
    vec![
        Cell::Int(id),
        Cell::String(Arc::from(name)),
        Cell::String(Arc::from(tier)),
    ]
}

fn seed_orders() -> Vec<Row> {
    vec![
        order(1, 1, Some(50), "open"),
        order(2, 3, Some(250), "shipped"),
        order(3, 1, Some(75), "pending"),
        order(4, 2, None, "open"),
        order(5, 4, Some(420), "cancelled"),
    ]
}

fn order(id: i64, customer_id: i64, amount: Option<i64>, status: &str) -> Row {
    vec![
        Cell::Int(id),
        Cell::Int(customer_id),
        amount.map_or(Cell::Null, Cell::Int),
        Cell::String(Arc::from(status)),
    ]
}

fn generate_order(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(1_000..1_000_000);
    let customer_id: i64 = rng.random_range(1..5);
    let amount: Option<i64> = if rng.random_bool(0.15) {
        None
    } else {
        Some(rng.random_range(1..500))
    };
    let status = STATUSES.choose(rng).copied().unwrap_or("open");
    vec![
        Cell::Int(id),
        Cell::Int(customer_id),
        amount.map_or(Cell::Null, Cell::Int),
        Cell::String(Arc::from(status)),
    ]
}
