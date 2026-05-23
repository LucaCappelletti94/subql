use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::Rng;

use subql::Cell;

use super::{PresetSchema, Row};

const STATUSES: &[&str] = &["open", "pending", "shipped", "cancelled"];

pub const PRESET: PresetSchema = PresetSchema {
    name: "orders",
    table_name: "orders",
    columns: &["id", "amount", "status", "comment"],
    column_types: &["INT PK", "INT", "TEXT", "TEXT"],
    pg_ddl: "CREATE TABLE orders (\n    id INT PRIMARY KEY,\n    amount INT,\n    status TEXT,\n    comment TEXT\n);",
    starter_queries: &[
        "SELECT * FROM orders WHERE amount > 100",
        "SELECT * FROM orders WHERE status = 'open'",
        "SELECT * FROM orders WHERE amount IS NULL OR status IN ('pending', 'shipped')",
    ],
    seed_rows: seed,
    generator: generate,
};

fn seed() -> Vec<Row> {
    vec![
        row(1, Some(50), Some("open"), None),
        row(2, Some(250), Some("shipped"), Some("expedited")),
        row(3, Some(75), Some("pending"), None),
        row(4, None, Some("open"), Some("missing amount")),
        row(5, Some(420), Some("cancelled"), None),
    ]
}

fn row(id: i64, amount: Option<i64>, status: Option<&str>, comment: Option<&str>) -> Row {
    vec![
        Cell::Int(id),
        amount.map_or(Cell::Null, Cell::Int),
        status.map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
        comment.map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
    ]
}

fn generate(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(1_000..1_000_000);
    let amount: Option<i64> = if rng.random_bool(0.15) {
        None
    } else {
        Some(rng.random_range(1..500))
    };
    let status = STATUSES.choose(rng).copied().unwrap_or("open");
    let comment = if rng.random_bool(0.3) {
        Some(format!("auto-{id}"))
    } else {
        None
    };
    vec![
        Cell::Int(id),
        amount.map_or(Cell::Null, Cell::Int),
        Cell::String(Arc::from(status)),
        comment.map_or(Cell::Null, |s| Cell::String(Arc::from(s.as_str()))),
    ]
}
