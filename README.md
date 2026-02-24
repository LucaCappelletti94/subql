# subql

[![CI](https://github.com/LucaCappelletti94/subql/actions/workflows/ci.yml/badge.svg)](https://github.com/LucaCappelletti94/subql/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/LucaCappelletti94/subql/graph/badge.svg)](https://codecov.io/gh/LucaCappelletti94/subql)
[![crates.io](https://img.shields.io/crates/v/subql.svg)](https://crates.io/crates/subql)
[![docs.rs](https://docs.rs/subql/badge.svg)](https://docs.rs/subql)
[![MSRV](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)]([LICENSE](https://raw.githubusercontent.com/LucaCappelletti94/subql/refs/heads/main/LICENSE))

SQL subscription dispatch engine for Change Data Capture fanout.

This README is the source for the crate-level documentation in `src/lib.rs`.

`subql` dispatches CDC row events to users based on SQL `WHERE` subscriptions.
It compiles SQL predicates once, deduplicates equivalent predicates across users, and
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

## Quick Start

```rust
use std::collections::HashMap;
use std::sync::Arc;

use sqlparser::dialect::PostgreSqlDialect;
use subql::{
    Cell, DefaultIds, EventKind, PrimaryKey, RowImage, SchemaCatalog, SubscriptionEngine,
    SubscriptionSpec, TableId, WalEvent,
};

struct DemoCatalog {
    tables: HashMap<String, (TableId, usize)>,
    columns: HashMap<(TableId, String), u16>,
}

impl DemoCatalog {
    fn new() -> Self {
        Self {
            tables: HashMap::from([(String::from("orders"), (1, 3))]),
            columns: HashMap::from([
                ((1, String::from("id")), 0),
                ((1, String::from("amount")), 1),
                ((1, String::from("status")), 2),
            ]),
        }
    }
}

impl SchemaCatalog for DemoCatalog {
    fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.tables.get(table_name).map(|(id, _)| *id)
    }

    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<u16> {
        self.columns
            .get(&(table_id, column_name.to_string()))
            .copied()
    }

    fn table_arity(&self, table_id: TableId) -> Option<usize> {
        self.tables
            .values()
            .find(|(id, _)| *id == table_id)
            .map(|(_, arity)| *arity)
    }

    fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
        Some(0xABCD_1234_5678_9ABC)
    }
}

let catalog = Arc::new(DemoCatalog::new());
let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds> =
    SubscriptionEngine::new(catalog, PostgreSqlDialect {});

engine.register(SubscriptionSpec {
    subscription_id: 1,
    user_id: 42,
    session_id: None,
    sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
    updated_at_unix_ms: 1_704_067_200_000,
})?;

let event = WalEvent {
    kind: EventKind::Insert,
    table_id: 1,
    pk: PrimaryKey {
        columns: Arc::from([0u16]),
        values: Arc::from([Cell::Int(1)]),
    },
    old_row: None,
    new_row: Some(RowImage {
        cells: Arc::from([Cell::Int(1), Cell::Int(250), Cell::String("paid".into())]),
    }),
    changed_columns: Arc::from([]),
};

let users: Vec<u64> = engine.users(&event)?.collect();
assert_eq!(users, vec![42]);

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
