use std::sync::Arc;

use rand::Rng;
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;

use subql::Cell;

use super::{PresetSchema, Row};

const LOCATIONS: &[&str] = &["warehouse-A", "warehouse-B", "loading-bay", "cold-room"];

pub const PRESET: PresetSchema = PresetSchema {
    name: "readings",
    table_name: "readings",
    columns: &["sensor_id", "temperature", "humidity", "location"],
    column_types: &["INT PK", "DOUBLE PRECISION", "DOUBLE PRECISION", "TEXT"],
    pg_ddl: "CREATE TABLE readings (\n    sensor_id INT PRIMARY KEY,\n    temperature DOUBLE PRECISION,\n    humidity DOUBLE PRECISION,\n    location TEXT\n);",
    starter_queries: &[
        "SELECT * FROM readings WHERE temperature > 30",
        "SELECT * FROM readings WHERE location = 'warehouse-A' AND humidity < 40",
        "SELECT COUNT(*) FROM readings WHERE location = 'warehouse-A'",
        "SELECT AVG(temperature) FROM readings",
    ],
    seed_rows: seed,
    generator: generate,
};

fn seed() -> Vec<Row> {
    vec![
        row(101, 22.5, 38.0, "warehouse-A"),
        row(102, 31.2, 55.0, "warehouse-B"),
        row(103, 18.7, 45.0, "cold-room"),
        row(104, 27.0, 60.0, "loading-bay"),
        row(105, 35.5, 30.0, "warehouse-A"),
    ]
}

fn row(sensor_id: i64, temperature: f64, humidity: f64, location: &str) -> Row {
    vec![
        Cell::Int(sensor_id),
        Cell::Float(temperature),
        Cell::Float(humidity),
        Cell::String(Arc::from(location)),
    ]
}

fn generate(rng: &mut SmallRng) -> Row {
    let sensor_id: i64 = rng.random_range(100..1_000_000);
    let temperature: f64 = rng.random_range(-5.0..45.0);
    let humidity: f64 = rng.random_range(15.0..90.0);
    let location = LOCATIONS.choose(rng).copied().unwrap_or("warehouse-A");
    vec![
        Cell::Int(sensor_id),
        Cell::Float(temperature),
        Cell::Float(humidity),
        Cell::String(Arc::from(location)),
    ]
}
