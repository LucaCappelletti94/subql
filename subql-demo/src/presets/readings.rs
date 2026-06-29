use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::Rng;

use subql::Cell;

use super::{PresetSchema, PresetTable, Row};

const LOCATIONS: &[&str] = &["warehouse-A", "warehouse-B", "loading-bay", "cold-room"];
const KINDS: &[&str] = &["temp", "humidity", "combo"];

pub const PRESET: PresetSchema = PresetSchema {
    name: "readings",
    pg_ddl: "CREATE TABLE sensors (\n    id INT PRIMARY KEY,\n    location TEXT,\n    kind TEXT\n);\nCREATE TABLE readings (\n    id INT PRIMARY KEY,\n    sensor_id INT,\n    temperature DOUBLE PRECISION,\n    humidity DOUBLE PRECISION\n);",
    tables: &[
        PresetTable {
            table_name: "sensors",
            columns: &["id", "location", "kind"],
            column_types: &["INT PK", "TEXT", "TEXT"],
            starter_queries: &[
                "SELECT * FROM sensors WHERE location = 'warehouse-A'",
                "SELECT COUNT(*) FROM sensors WHERE kind = 'combo'",
            ],
            seed_rows: seed_sensors,
            generator: generate_sensor,
        },
        PresetTable {
            table_name: "readings",
            columns: &["id", "sensor_id", "temperature", "humidity"],
            column_types: &["INT PK", "INT", "DOUBLE PRECISION", "DOUBLE PRECISION"],
            starter_queries: &[
                "SELECT * FROM readings WHERE temperature > 30",
                "SELECT * FROM readings WHERE humidity < 40",
                "SELECT AVG(temperature) FROM readings",
            ],
            seed_rows: seed_readings,
            generator: generate_reading,
        },
    ],
};

fn seed_sensors() -> Vec<Row> {
    vec![
        sensor(1, "warehouse-A", "combo"),
        sensor(2, "warehouse-B", "temp"),
        sensor(3, "cold-room", "combo"),
        sensor(4, "loading-bay", "humidity"),
    ]
}

fn sensor(id: i64, location: &str, kind: &str) -> Row {
    vec![
        Cell::Int(id),
        Cell::String(Arc::from(location)),
        Cell::String(Arc::from(kind)),
    ]
}

fn generate_sensor(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(100..1_000_000);
    let location = LOCATIONS.choose(rng).copied().unwrap_or("warehouse-A");
    let kind = KINDS.choose(rng).copied().unwrap_or("combo");
    vec![
        Cell::Int(id),
        Cell::String(Arc::from(location)),
        Cell::String(Arc::from(kind)),
    ]
}

fn seed_readings() -> Vec<Row> {
    vec![
        reading(1, 1, 22.5, 38.0),
        reading(2, 2, 31.2, 55.0),
        reading(3, 3, 18.7, 45.0),
        reading(4, 1, 27.0, 60.0),
        reading(5, 2, 35.5, 30.0),
    ]
}

fn reading(id: i64, sensor_id: i64, temperature: f64, humidity: f64) -> Row {
    vec![
        Cell::Int(id),
        Cell::Int(sensor_id),
        Cell::Float(temperature),
        Cell::Float(humidity),
    ]
}

fn generate_reading(rng: &mut SmallRng) -> Row {
    let id: i64 = rng.random_range(100..1_000_000);
    let sensor_id: i64 = rng.random_range(1..5);
    let temperature: f64 = rng.random_range(-5.0..45.0);
    let humidity: f64 = rng.random_range(15.0..90.0);
    vec![
        Cell::Int(id),
        Cell::Int(sensor_id),
        Cell::Float(temperature),
        Cell::Float(humidity),
    ]
}
