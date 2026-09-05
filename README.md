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

Alongside row-match subscriptions, register a `SELECT COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, or variance/stddev (`VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`) query instead of `SELECT *`. The engine keeps the running value and reports it whenever it moves, so the caller stores nothing and folds nothing.

A registration answers with an `aggregate_bootstrap`, a runnable query for the starting numbers. Run it, then pass an `AggregateSeedInstall` to `Install::install` with the decoded row and stream position the read was taken at. Take that position **before** the read's snapshot opens: it is what lets the engine drop the changes the read already saw rather than counting them twice. Until the numbers land the subscription reports nothing, and a read the engine cannot line up against what it folded is refused with a `AggregateInstallError` so the caller can `reset_aggregate` and read again.

Aggregate subscribers never appear in `consumers()` output, and vice versa. `UPDATE` deltas need both old and new row images, so a source that omits old images (`before` / `old`) gets an error for update events. A `TRUNCATE` needs nothing from the caller: the table is empty afterwards, so the engine empties the value itself and reports it.

```rust
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, DefaultIds, SubscriptionEngine, SubscriptionRequest,
};

let catalog = ParserDB::parse::<PostgreSqlDialect>(
    "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
)
.expect("the DDL parses");
let orders_id = catalog_helpers::table_id(&catalog, "orders").expect("orders is cataloged");
let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

// Live count of active orders, and their running total, both for consumer 42.
let counted = engine
    .register(SubscriptionRequest::new(
        42, "SELECT COUNT(*) FROM orders WHERE status = 'active'",
    ))
    .expect("the count registers");
let totalled = engine
    .register(SubscriptionRequest::new(
        42, "SELECT SUM(amount) FROM orders WHERE status = 'active'",
    ))
    .expect("the sum registers");

// Starting numbers over an empty table. Nothing has been folded yet, so this
// read cannot have raced a change and needs no stream position.
subql::Install::install(
    &mut engine,
    counted.subscription_id,
    subql::AggregateSeedInstall {
        // An empty table: a NULL total over no contributing rows, and a
        // contributor count of zero.
        rows: vec![vec![Value::Null, Value::Int(0)]],
        read_at: None,
    },
)
.expect("the count's numbers land");
subql::Install::install(
    &mut engine,
    totalled.subscription_id,
    subql::AggregateSeedInstall {
        rows: vec![vec![Value::Int(0)]],
        read_at: None,
    },
)
.expect("the sum's numbers land");

let event = TestEvent::<Postgres>::insert(
    orders_id,
    vec![Value::Int(1), Value::Int(250), Value::String("active".into())],
)
.with_pk_columns([0u16]);

// One report per subscription, because one consumer's two aggregates are two
// separate values.
let mut updates = engine.aggregate_updates(&event).expect("the event folds");
updates.sort_by_key(|update| update.subscription);
assert_eq!(updates[0].subscription, counted.subscription_id);
assert_eq!(updates[0].consumer, 42);
assert_eq!(
    updates[0].change,
    subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(
        AggValue::Count(1),
    )),
);
assert_eq!(
    updates[1].change,
    subql::AggregateValueChange::Set(subql::AggregateResultValue::Folded(
        // `amount` is an `INT`, whose sum is a `bigint` on PostgreSQL.
        AggValue::Sum(Some(subql::NumericValue::Integer(250))),
    )),
);
```

### Aggregate variants

| SQL | `AggValue` variant | Notes |
|-----|--------------------|-------|
| `SELECT COUNT(*) FROM t WHERE ...` | `Count(i64)` | +/-1 per matching row |
| `SELECT COUNT(col) FROM t WHERE ...` | `Count(i64)` | skips `NULL` cells |
| `SELECT SUM(col) FROM t WHERE ...` | `Sum(Option<NumericValue>)` | exact, in the type the engine sums into: `bigint`, `numeric`/`DECIMAL`, `integer` or double. `None` when no row contributes, which is what every engine answers |
| `SELECT AVG(col) FROM t WHERE ...` | `Avg(Option<NumericValue>)` | the engine's own division of the exact total by the count: `numeric` on PostgreSQL, `DECIMAL` on MySQL, a real on SQLite. `None` when no row contributes |

### Type validation

Column types come from the SQL DDL parsed into `ParserDB`. When a column's type can be determined (e.g. `INT`, `REAL`, `TEXT`), the engine will not fold `SUM` or `AVG` over a non-numeric column (`Bool`, `String`). Such a query still registers, on a tier that re-reads it against the database, and `Registered::not_served_because` carries the reason it is not folded in process.

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

// Registered, but not folded in process: name is String, so the answer comes
// from re-reading rather than from the change stream.
let rows = engine.register(SubscriptionRequest::new(
    1, "SELECT SUM(name) FROM products WHERE id > 0",
))?;
assert!(rows.served().is_none());
assert!(rows.not_served_because.is_some());

# Ok::<(), Box<dyn std::error::Error>>(())
```
