# subql

[![CI](https://github.com/LucaCappelletti94/subql/actions/workflows/ci.yml/badge.svg)](https://github.com/LucaCappelletti94/subql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LucaCappelletti94/subql/graph/badge.svg)](https://codecov.io/gh/LucaCappelletti94/subql)
[![crates.io](https://img.shields.io/crates/v/subql.svg)](https://crates.io/crates/subql)
[![docs.rs](https://docs.rs/subql/badge.svg)](https://docs.rs/subql)
[![MSRV](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/LucaCappelletti94/subql/refs/heads/main/LICENSE)

SQL subscription dispatch engine for Change Data Capture fanout.

`subql` dispatches CDC row events to consumers based on SQL `WHERE` subscriptions.
It compiles SQL predicates once, deduplicates equivalent predicates across consumers, and
uses hybrid indexes to prune candidates before VM evaluation.

## Features

- SQL-correct tri-state logic (`TRUE`/`FALSE`/`UNKNOWN`)
- Hybrid indexing (equality/range/NULL/fallback)
- Predicate deduplication across subscribers
- Session-bound and durable subscriptions
- Optional shard persistence with background merge
- Pluggable WAL parsers (`PgOutput`, `wal2json` v1/v2, Debezium, Maxwell)
- Table-level `TRUNCATE` CDC event support
- Multiple SQL dialects through `sqlparser`
- Streaming aggregate subscriptions: `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`

## Quick Start

```rust
use std::sync::Arc;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SimpleCatalog,
    SubscriptionEngine, SubscriptionRequest, WalEvent,
};

let catalog = Arc::new(
    SimpleCatalog::new()
        .add_table("orders", 1, 3)
        .add_column(1, "id", 0)
        .add_column(1, "amount", 1)
        .add_column(1, "status", 2),
);
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(
    SubscriptionRequest::new(42, "SELECT * FROM orders WHERE amount > 100")
        .updated_at_unix_ms(1_704_067_200_000),
)?;

let event = WalEvent::builder(1)
    .insert()
    .pk_cell(0, Cell::Int(1))
    .new_row(RowImage {
        cells: Arc::from([Cell::Int(1), Cell::Int(250), Cell::String("paid".into())]),
    })
    .build()?;

let notifs = engine.consumers(&event)?;
assert_eq!(notifs.inserted(), vec![42]);

# Ok::<(), Box<dyn std::error::Error>>(())
```

## Streaming Aggregates

`subql` supports streaming aggregate subscriptions alongside row-match subscriptions.
Instead of `SELECT *`, register a `SELECT COUNT(*)`, `SELECT COUNT(col)`,
`SELECT SUM(col)`, or `SELECT AVG(col)` query. The engine then emits signed
**deltas** via `aggregate_deltas()` — the caller maintains the running total.

Aggregate subscribers never appear in `consumers()` output and vice versa.
For `UPDATE` aggregate deltas, CDC events must include both old and new row
images. If a source omits old images (`before`/`old`), `aggregate_deltas()`
returns an error for update events.

```rust
use std::sync::Arc;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    AggDelta, AggregateDispatch, Cell, ColumnType, DefaultIds, PrimaryKey, RowImage,
    SimpleCatalog, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

let catalog = Arc::new(
    SimpleCatalog::new()
        .add_table("orders", 1, 3)
        .add_column(1, "id", 0)
        .add_column_typed(1, "amount", 1, ColumnType::Int)
        .add_column(1, "status", 2),
);
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

// Live count of active orders for consumer 42.
engine.register(SubscriptionRequest::new(
    42, "SELECT COUNT(*) FROM orders WHERE status = 'active'",
))?;

// Running total of active order amounts for consumer 42.
engine.register(SubscriptionRequest::new(
    42, "SELECT SUM(amount) FROM orders WHERE status = 'active'",
))?;

let event = WalEvent::builder(1)
    .insert()
    .pk_cell(0, Cell::Int(1))
    .new_row(RowImage {
        cells: Arc::from([Cell::Int(1), Cell::Int(250), Cell::String("active".into())]),
    })
    .build()?;

let mut deltas: Vec<(u64, AggDelta)> = engine.aggregate_deltas(&event)?;
// Sort for deterministic comparison (Count before Sum).
deltas.sort_by_key(|(_, d)| match d {
    AggDelta::Count(_) => 0,
    AggDelta::Sum(_) => 1,
    AggDelta::Avg { .. } => 2,
    _ => 3,
});
assert_eq!(deltas, vec![
    (42, AggDelta::Count(1)),
    (42, AggDelta::Sum(250.0)),
]);

# Ok::<(), Box<dyn std::error::Error>>(())
```

### Aggregate variants

| SQL | `AggDelta` variant | Notes |
|-----|--------------------|-------|
| `SELECT COUNT(*) FROM t WHERE …` | `Count(i64)` | ±1 per matching row |
| `SELECT COUNT(col) FROM t WHERE …` | `Count(i64)` | skips `NULL` cells |
| `SELECT SUM(col) FROM t WHERE …` | `Sum(f64)` | skips `NULL`/`NaN`/`Inf` |
| `SELECT AVG(col) FROM t WHERE …` | `Avg { sum_delta, count_delta }` | caller divides to get the new average |

For `AVG`, the caller accumulates `running_sum` and `running_count` separately,
then computes the average as `running_sum / running_count` on demand:

```rust
use std::sync::Arc;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    AggDelta, AggregateDispatch, Cell, ColumnType, DefaultIds, PrimaryKey, RowImage,
    SimpleCatalog, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

let catalog = Arc::new(
    SimpleCatalog::new()
        .add_table("scores", 1, 2)
        .add_column(1, "id", 0)
        .add_column_typed(1, "value", 1, ColumnType::Int),
);
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(SubscriptionRequest::new(
    7, "SELECT AVG(value) FROM scores WHERE id > 0",
))?;

let event = WalEvent::builder(1)
    .insert()
    .pk_cell(0, Cell::Int(1))
    .new_row(RowImage {
        cells: Arc::from([Cell::Int(1), Cell::Int(100)]),
    })
    .build()?;

let deltas = engine.aggregate_deltas(&event)?;
let (_, delta) = &deltas[0];

// Accumulate running statistics across events:
let mut running_sum = 0.0_f64;
let mut running_count = 0_i64;

if let AggDelta::Avg { sum_delta, count_delta } = delta {
    running_sum += sum_delta;
    running_count += count_delta;
    let avg = running_sum / running_count as f64;
    assert_eq!(avg, 100.0);
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

### Type validation

Use `SimpleCatalog::add_column_typed` to register column types. When a column
type is known, the engine rejects `SUM` or `AVG` over non-numeric columns
(`Bool`, `String`) at registration time with a `RegisterError::UnsupportedSql`.

```rust
use std::sync::Arc;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    ColumnType, DefaultIds, SimpleCatalog, SubscriptionEngine, SubscriptionRequest,
};

let catalog = Arc::new(
    SimpleCatalog::new()
        .add_table("products", 1, 3)
        .add_column_typed(1, "price", 0, ColumnType::Float)
        .add_column_typed(1, "name", 1, ColumnType::String) // non-numeric
        .add_column(1, "id", 2),
);
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

// Accepted — price is Float:
engine.register(SubscriptionRequest::new(
    1, "SELECT SUM(price) FROM products WHERE id > 0",
))?;

// Rejected at registration — name is String:
assert!(engine
    .register(SubscriptionRequest::new(
        1, "SELECT SUM(name) FROM products WHERE id > 0",
    ))
    .is_err());

# Ok::<(), Box<dyn std::error::Error>>(())
```

## CI

The workflow in `.github/workflows/ci.yml` runs:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo check --locked` matrix (`default`, `--no-default-features`, `--all-features`)
- `cargo test --locked` matrix (`default`, `--all-features`)
- ignored cross-DB integration test (`cdc_cross_db`)
- docs + doctests with rustdoc warnings denied
- MSRV check (`1.88`)
- bench compile check (`cargo bench --no-run --locked`)
- fuzz target compile check (`fuzz/Cargo.toml`)
- `cargo audit`

All cargo CI commands use `--locked` for deterministic dependency resolution.

## Pre-commit Hook

This repo includes a Rust pre-commit hook at `.githooks/pre-commit` that runs:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --lib --tests --locked`

Install once per clone:

```bash
./scripts/install-git-hooks.sh
```

Skip tests for a single commit:

```bash
SUBQL_PRECOMMIT_SKIP_TESTS=1 git commit -m "..."
```
