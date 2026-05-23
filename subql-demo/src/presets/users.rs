use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::Rng;

use subql::Cell;

use super::{PresetSchema, Row};

const FIRST_NAMES: &[&str] = &[
    "alice", "bob", "carol", "dan", "eve", "frank", "grace", "heidi", "ivan", "judy",
];

pub const PRESET: PresetSchema = PresetSchema {
    name: "users",
    table_name: "users",
    columns: &["id", "age", "email", "name"],
    column_types: &["INT PK", "INT", "TEXT", "TEXT"],
    pg_ddl: "CREATE TABLE users (\n    id INT PRIMARY KEY,\n    age INT,\n    email TEXT,\n    name TEXT\n);",
    starter_queries: &[
        "SELECT * FROM users WHERE age > 18 AND age < 65",
        "SELECT * FROM users WHERE email IS NULL",
        "SELECT * FROM users WHERE id IN (1, 2, 3) OR age < 18",
    ],
    seed_rows: seed,
    generator: generate,
};

fn seed() -> Vec<Row> {
    vec![
        row(1, Some(27), Some("alice@example.com"), Some("alice")),
        row(2, Some(64), None, Some("bob")),
        row(3, Some(15), Some("carol@example.com"), Some("carol")),
        row(4, None, Some("dan@example.com"), Some("dan")),
        row(5, Some(82), None, Some("eve")),
    ]
}

fn row(id: i64, age: Option<i64>, email: Option<&str>, name: Option<&str>) -> Row {
    vec![
        Cell::Int(id),
        age.map_or(Cell::Null, Cell::Int),
        email.map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
        name.map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
    ]
}

fn generate(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(100..1_000_000);
    let age: Option<i64> = if rng.random_bool(0.1) {
        None
    } else {
        Some(rng.random_range(5..95))
    };
    let name = FIRST_NAMES.choose(rng).copied().unwrap_or("user");
    let email = if rng.random_bool(0.2) {
        None
    } else {
        Some(format!("{name}{id}@example.com"))
    };
    vec![
        Cell::Int(id),
        age.map_or(Cell::Null, Cell::Int),
        email.map_or(Cell::Null, |s| Cell::String(Arc::from(s.as_str()))),
        Cell::String(Arc::from(name)),
    ]
}
