# subql

[![CI](https://github.com/LucaCappelletti94/subql/actions/workflows/ci.yml/badge.svg)](https://github.com/LucaCappelletti94/subql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LucaCappelletti94/subql/graph/badge.svg)](https://codecov.io/gh/LucaCappelletti94/subql)
[![crates.io](https://img.shields.io/crates/v/subql.svg)](https://crates.io/crates/subql)
[![docs.rs](https://docs.rs/subql/badge.svg)](https://docs.rs/subql)
[![MSRV](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/LucaCappelletti94/subql/refs/heads/main/LICENSE)

SQL subscription dispatch engine for Change Data Capture fanout.

`subql` dispatches CDC row events to consumers based on SQL `WHERE` subscriptions. Predicates compile once. Equivalent SQL is deduplicated across subscribers, then hybrid indexes (equality, range, `IS NULL`, and a fallback set) prune candidates before the VM evaluates. The VM honors SQL three-valued logic (`TRUE`, `FALSE`, `UNKNOWN`) and routes session-bound and durable subscriptions through one path. With the `std` feature, predicate state persists to durable shards with background merge. WAL input is pluggable: `PgOutput`, `wal2json` v1 and v2, Debezium, and Maxwell parsers feed the same path, including table-level `TRUNCATE`. Streaming aggregates cover `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, and the variance/standard-deviation family (`VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, `STDDEV_SAMP`). Any SQL `sqlparser` accepts (multiple dialects) works as a predicate.

## Quick Start

```rust
use std::sync::Arc;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    catalog_helpers, Cell, DefaultIds, EventKind, PrimaryKey, RowImage,
    SubscriptionEngine, SubscriptionRequest, WalEvent,
};

let catalog = Arc::new(
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    )?,
);
let orders_id = catalog_helpers::table_id(&*catalog, "orders").unwrap();
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(
    SubscriptionRequest::new(42, "SELECT * FROM orders WHERE amount > 100")
        .updated_at_unix_ms(1_704_067_200_000),
)?;

let event = WalEvent::builder(orders_id)
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

Alongside row-match subscriptions, register a `SELECT COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, or variance/stddev (`VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`) query instead of `SELECT *`. The engine emits signed deltas via `aggregate_deltas()` and the caller keeps the running total.

Aggregate subscribers never appear in `consumers()` output, and vice versa. `UPDATE` deltas need both old and new row images. If a source omits old images (`before` / `old`), `aggregate_deltas()` returns an error for update events.

```rust
use std::sync::Arc;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    catalog_helpers, AggDelta, AggregateDispatch, Cell, ColumnType, DefaultIds,
    PrimaryKey, RowImage, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

let catalog = Arc::new(
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    )?,
);
let orders_id = catalog_helpers::table_id(&*catalog, "orders").unwrap();
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

// Live count of active orders for consumer 42.
engine.register(SubscriptionRequest::new(
    42, "SELECT COUNT(*) FROM orders WHERE status = 'active'",
))?;

// Running total of active order amounts for consumer 42.
engine.register(SubscriptionRequest::new(
    42, "SELECT SUM(amount) FROM orders WHERE status = 'active'",
))?;

let event = WalEvent::builder(orders_id)
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
| `SELECT COUNT(*) FROM t WHERE ...` | `Count(i64)` | +/-1 per matching row |
| `SELECT COUNT(col) FROM t WHERE ...` | `Count(i64)` | skips `NULL` cells |
| `SELECT SUM(col) FROM t WHERE ...` | `Sum(f64)` | skips `NULL`/`NaN`/`Inf` |
| `SELECT AVG(col) FROM t WHERE ...` | `Avg { sum_delta, count_delta }` | caller divides to get the new average |
| `SELECT VAR_POP(col) FROM t WHERE ...` (also `VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`) | `Stats { sum_delta, sum_sq_delta, count_delta }` | caller maintains running sum, sum-of-squares, and count |

For `AVG`, the caller accumulates `running_sum` and `running_count` separately, then computes the average as `running_sum / running_count` on demand:

```rust
use std::sync::Arc;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    catalog_helpers, AggDelta, AggregateDispatch, Cell, ColumnType, DefaultIds,
    PrimaryKey, RowImage, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

let catalog = Arc::new(
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE scores (id INT PRIMARY KEY, value INT);",
    )?,
);
let scores_id = catalog_helpers::table_id(&*catalog, "scores").unwrap();
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(SubscriptionRequest::new(
    7, "SELECT AVG(value) FROM scores WHERE id > 0",
))?;

let event = WalEvent::builder(scores_id)
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

Column types come from the SQL DDL parsed into `ParserDB`. When a column's type can be determined (e.g. `INT`, `REAL`, `TEXT`), the engine rejects `SUM` or `AVG` over non-numeric columns (`Bool`, `String`) at registration time with a `RegisterError::UnsupportedSql`.

```rust
use std::sync::Arc;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

let catalog = Arc::new(
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE products (price REAL, name TEXT, id INT PRIMARY KEY);",
    )?,
);
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

// Accepted, price is Float:
engine.register(SubscriptionRequest::new(
    1, "SELECT SUM(price) FROM products WHERE id > 0",
))?;

// Rejected at registration, name is String:
assert!(engine
    .register(SubscriptionRequest::new(
        1, "SELECT SUM(name) FROM products WHERE id > 0",
    ))
    .is_err());

# Ok::<(), Box<dyn std::error::Error>>(())
```
