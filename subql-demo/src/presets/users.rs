use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::Rng;

use subql::Cell;

use super::{PresetSchema, PresetTable, Row};

const FIRST_NAMES: &[&str] = &[
    "alice", "bob", "carol", "dan", "eve", "frank", "grace", "heidi", "ivan", "judy",
];
const DEVICES: &[&str] = &["web", "ios", "android", "desktop"];

pub const PRESET: PresetSchema = PresetSchema {
    name: "users",
    pg_ddl: "CREATE TABLE users (\n    id INT PRIMARY KEY,\n    age INT,\n    email TEXT,\n    name TEXT\n);\nCREATE TABLE sessions (\n    id INT PRIMARY KEY,\n    user_id INT,\n    minutes INT,\n    device TEXT\n);",
    tables: &[
        PresetTable {
            table_name: "users",
            columns: &["id", "age", "email", "name"],
            column_types: &["INT PK", "INT", "TEXT", "TEXT"],
            starter_queries: &[
                "SELECT * FROM users WHERE age > 18 AND age < 65",
                "SELECT * FROM users WHERE email IS NULL",
                "SELECT * FROM users WHERE id IN (1, 2, 3) OR age < 18",
            ],
            seed_rows: seed_users,
            generator: generate_user,
        },
        PresetTable {
            table_name: "sessions",
            columns: &["id", "user_id", "minutes", "device"],
            column_types: &["INT PK", "INT", "INT", "TEXT"],
            starter_queries: &[
                "SELECT * FROM sessions WHERE minutes > 30",
                "SELECT * FROM sessions WHERE device = 'ios'",
                "SELECT SUM(minutes) FROM sessions WHERE device = 'web'",
            ],
            seed_rows: seed_sessions,
            generator: generate_session,
        },
    ],
};

fn seed_users() -> Vec<Row> {
    vec![
        user(1, Some(27), Some("alice@example.com"), "alice"),
        user(2, Some(64), None, "bob"),
        user(3, Some(15), Some("carol@example.com"), "carol"),
        user(4, None, Some("dan@example.com"), "dan"),
        user(5, Some(82), None, "eve"),
    ]
}

fn user(id: i64, age: Option<i64>, email: Option<&str>, name: &str) -> Row {
    vec![
        Cell::Int(id),
        age.map_or(Cell::Null, Cell::Int),
        email.map_or(Cell::Null, |s| Cell::String(Arc::from(s))),
        Cell::String(Arc::from(name)),
    ]
}

fn generate_user(rng: &mut SmallRng) -> Row {
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

fn seed_sessions() -> Vec<Row> {
    vec![
        session(1, 1, 42, "web"),
        session(2, 1, 12, "ios"),
        session(3, 2, 75, "desktop"),
        session(4, 3, 5, "android"),
    ]
}

fn session(id: i64, user_id: i64, minutes: i64, device: &str) -> Row {
    vec![
        Cell::Int(id),
        Cell::Int(user_id),
        Cell::Int(minutes),
        Cell::String(Arc::from(device)),
    ]
}

fn generate_session(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(100..1_000_000);
    let user_id: i64 = rng.random_range(1..6);
    let minutes: i64 = rng.random_range(1..180);
    let device = DEVICES.choose(rng).copied().unwrap_or("web");
    vec![
        Cell::Int(id),
        Cell::Int(user_id),
        Cell::Int(minutes),
        Cell::String(Arc::from(device)),
    ]
}
