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
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest,
};

let catalog = ParserDB::parse::<PostgreSqlDialect>(
    "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
)?;
let orders_id = catalog_helpers::table_id(&catalog, "orders").unwrap();
let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(
    SubscriptionRequest::new(42, "SELECT * FROM orders WHERE amount > 100")
        .updated_at_unix_ms(1_704_067_200_000),
)?;

let event = TestEvent::<Postgres>::insert(
    orders_id,
    vec![Value::Int(1), Value::Int(250), Value::String("paid".into())],
)
.with_pk_columns([0u16]);

let notifs = engine.consumers(&event)?;
assert_eq!(notifs.inserted(), vec![42]);

# Ok::<(), Box<dyn std::error::Error>>(())
```

## Streaming Aggregates

Alongside row-match subscriptions, register a `SELECT COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, or variance/stddev (`VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`) query instead of `SELECT *`. The engine emits signed deltas via `aggregate_deltas()` and the caller keeps the running total.

Aggregate subscribers never appear in `consumers()` output, and vice versa. `UPDATE` deltas need both old and new row images. If a source omits old images (`before` / `old`), `aggregate_deltas()` returns an error for update events.

```rust
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggDelta, DefaultIds, SubscriptionEngine, SubscriptionRequest,
};

let catalog = ParserDB::parse::<PostgreSqlDialect>(
    "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
)?;
let orders_id = catalog_helpers::table_id(&catalog, "orders").unwrap();
let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

// Live count of active orders for consumer 42.
engine.register(SubscriptionRequest::new(
    42, "SELECT COUNT(*) FROM orders WHERE status = 'active'",
))?;

// Running total of active order amounts for consumer 42.
engine.register(SubscriptionRequest::new(
    42, "SELECT SUM(amount) FROM orders WHERE status = 'active'",
))?;

let event = TestEvent::<Postgres>::insert(
    orders_id,
    vec![Value::Int(1), Value::Int(250), Value::String("active".into())],
)
.with_pk_columns([0u16]);

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
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggDelta, DefaultIds, SubscriptionEngine, SubscriptionRequest,
};

let catalog = ParserDB::parse::<PostgreSqlDialect>(
    "CREATE TABLE scores (id INT PRIMARY KEY, value INT);",
)?;
let scores_id = catalog_helpers::table_id(&catalog, "scores").unwrap();
let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(SubscriptionRequest::new(
    7, "SELECT AVG(value) FROM scores WHERE id > 0",
))?;

let event = TestEvent::<Postgres>::insert(
    scores_id,
    vec![Value::Int(1), Value::Int(100)],
)
.with_pk_columns([0u16]);

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
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Postgres;
use subql::testing::TestEvent;
use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest};

let catalog = ParserDB::parse::<PostgreSqlDialect>(
    "CREATE TABLE products (price REAL, name TEXT, id INT PRIMARY KEY);",
)?;
let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
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
