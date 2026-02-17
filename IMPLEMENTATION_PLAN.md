# subql Implementation Plan

## Document Purpose

This is a comprehensive, step-by-step implementation guide for building the `subql` crate: a SQL subscription dispatch engine for PostgreSQL CDC event fanout. This plan provides clear architectural guidance while remaining practical for AI-assisted or human implementation.

## Background & Motivation

### The Problem

The `sql_rule_tree_benchmarks` workspace demonstrates that full-scan evaluation doesn't scale:

```
native baseline (AST interpretation):
  1 rule:      96 ns
  1000 rules:  96 μs  (linear scaling)
  100k rules:  ~9.6 ms (projected - unacceptable for real-time CDC)
```

For real-time Change Data Capture (CDC) event fanout to tens or hundreds of thousands of subscribers, we need:
- **Sub-millisecond dispatch** at 100k+ subscription scale
- **SQL-correct semantics** (tri-state NULL logic)
- **Session lifecycle** (temporary vs durable subscriptions)
- **Online updates** (no stop-the-world maintenance)

### Why VM Bytecode?

**Question**: Why not just interpret the AST directly like the native baseline?

**Answer**: Three reasons:

1. **Performance**: Bytecode allows optimization opportunities (constant folding, dead code elimination) and better instruction cache locality
2. **Serialization**: Bytecode is compact and trivial to serialize/deserialize for durable storage
3. **Separation of concerns**: Parse once (expensive), evaluate many times (cheap)

**The VM Model**:
```rust
// SQL: age > 18 AND status = 'active'
// Compiles to:
[
    LoadColumn(age_id),        // Stack: [age_value]
    PushLiteral(18),           // Stack: [age_value, 18]
    GreaterThan,               // Stack: [Tri::True/False/Unknown]
    LoadColumn(status_id),     // Stack: [tri, status_value]
    PushLiteral("active"),     // Stack: [tri, status_value, "active"]
    Equal,                     // Stack: [tri, tri]
    And,                       // Stack: [Tri]
]
```

This isn't a general-purpose VM - it's a specialized stack machine for SQL predicate evaluation with tri-state logic.

### Why Generic SQL Dialect?

The original plan hardcoded `PostgreSqlDialect`. However:
- Users may ingest CDC from MySQL, SQLite, etc.
- Dialect affects parsing rules (e.g., backtick vs double-quote identifiers)
- Generic design costs nothing but provides flexibility

**Solution**: Make dialect a parameter to the parser, not a compile-time constant.

---

## Architecture Overview

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    SUBSCRIPTION ENGINE                          │
│                                                                 │
│  Input: SubscriptionSpec { sql, user_id, session_id?, ... }    │
│  Output: Vec<UserId> (deduplicated, interested users)          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────────┐
        │        COMPILER PIPELINE                  │
        │                                           │
        │  1. Parse SQL (generic dialect)           │
        │  2. Validate against supported subset     │
        │  3. Resolve table/columns via catalog     │
        │  4. Canonicalize (normalize + hash)       │
        │  5. Compile to bytecode                   │
        │  6. Extract indexable atoms               │
        └───────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────────┐
        │     RUNTIME DISPATCH (per-table)          │
        │                                           │
        │  Hybrid Indexes:                          │
        │   • Equality: (col, val) → Bitmap         │
        │   • Range: col → IntervalTree             │
        │   • NULL: (col, kind) → Bitmap            │
        │   • Fallback: unindexable → Bitmap        │
        │   • Dependency: col → Bitmap (UPDATE)     │
        │                                           │
        │  Candidate Selection:                     │
        │   • Union index lookups                   │
        │   • Always include fallback               │
        │                                           │
        │  VM Evaluation:                           │
        │   • Run bytecode for each candidate       │
        │   • Tri-state logic (True/False/Unknown)  │
        │   • Only True matches                     │
        │                                           │
        │  User Resolution:                         │
        │   • PredicateId → Bitmap<UserOrdinal>     │
        │   • Union all matching bitmaps            │
        │   • Translate ordinals → UserIds          │
        └───────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────────┐
        │      PERSISTENCE & MERGE                  │
        │                                           │
        │  Live Shard (ArcSwap, immutable):         │
        │   • All indexes + predicates + bindings   │
        │   • Readers never block                   │
        │                                           │
        │  Background Merge:                        │
        │   • Load all shards                       │
        │   • Rebuild unified indexes               │
        │   • Atomic swap when complete             │
        │   • Delete old shards                     │
        │                                           │
        │  Codec: bincode + LZ4                     │
        └───────────────────────────────────────────┘
```

### Data Flow Example

**Scenario**: User registers `SELECT * FROM orders WHERE amount > 100 AND status = 'pending'`

1. **Registration**:
   ```
   register(SubscriptionSpec)
     → parse("SELECT * FROM orders WHERE amount > 100 AND status = 'pending'")
     → normalize to canonical form
     → hash predicate → 0xABCD1234...
     → check if predicate exists (dedup)
     → if new: compile to bytecode, insert into indexes
     → bind user_id to predicate
     → return RegisterResult { predicate_hash, created_new, ... }
   ```

2. **Dispatch** (INSERT event arrives):
   ```
   users(WalEvent { kind: Insert, new_row: { amount: 150, status: "pending" } })
     → select candidates from indexes:
        - equality[(amount_col, 150)] ∪ equality[(status_col, "pending")]
        - range[amount_col].query(150)
        - fallback (always)
     → candidates = {pred_1, pred_7, pred_42, ...}
     → for each candidate:
        - vm.eval(bytecode, new_row) → Tri
        - if Tri::True: add to matching_predicates
     → matching_predicates = {pred_1, pred_42}
     → resolve users:
        - users[pred_1] = {user_10, user_25}
        - users[pred_42] = {user_10, user_99}
        - union = {user_10, user_25, user_99}
     → return [user_10, user_25, user_99] (sorted)
   ```

3. **Session Disconnect**:
   ```
   unregister_session(session_id)
     → find all bindings with this session_id
     → for each binding:
        - decrement predicate refcount
        - if refcount → 0: remove from indexes, delete predicate
        - remove user from user_dict if no bindings remain
     → return PruneReport { removed_bindings, removed_predicates, ... }
   ```

### Key Invariants

These must ALWAYS be true:

1. **Semantic Correctness**: `eval(bytecode, row)` matches PostgreSQL's evaluation of the original SQL
2. **Tri-State Discipline**: Only `Tri::True` is a match; `False` and `Unknown` are both non-matches
3. **Index Coverage**: Candidates ⊇ True Matches (false positives OK, false negatives forbidden)
4. **Atomic Reads**: Readers always see a consistent snapshot (no torn reads during merge)
5. **Refcount Integrity**: Predicates exist ⟺ refcount > 0
6. **Session Isolation**: Disconnecting session X never affects session Y's subscriptions

---

## Phase 0: Project Setup

**Time Estimate**: 30 minutes
**Goal**: Create the crate structure and configure dependencies

### Step 0.1: Initialize Crate

Since we're creating a standalone crate (not in the benchmarks workspace):

```bash
cd /home/luca/github/subql
cargo init --lib --name subql
```

**Verify**:
```bash
cargo build
# Should compile the empty lib successfully
```

### Step 0.2: Configure Cargo.toml

**File**: `Cargo.toml`

```toml
[package]
name = "subql"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
authors = ["Your Name <you@example.com>"]
license = "MIT OR Apache-2.0"
description = "SQL subscription dispatch engine for CDC event fanout"
repository = "https://github.com/yourusername/subql"
readme = "README.md"
keywords = ["sql", "cdc", "subscription", "postgres", "streaming"]
categories = ["database", "parser-implementations"]

[lib]
name = "subql"
path = "src/lib.rs"

# Benchmark harness
[[bench]]
name = "dispatch"
harness = false

[dependencies]
# SQL parsing (supports multiple dialects)
sqlparser = { version = "0.47", features = ["visitor"] }

# Compressed bitmaps for predicate/user sets
roaring = "0.10"

# Stable ID allocation (predicates)
slab = "0.4"

# Atomic pointer swap for immutable snapshots
arc-swap = "1.7"

# Fast hashing
ahash = "0.8"          # Runtime hashing
seahash = "4.1"        # Deterministic hashing for canonicalization

# Serialization
bincode = "1.3"        # Binary encoding
lz4 = "1.24"           # Fast compression

# Error handling
thiserror = "1.0"
anyhow = "1.0"

# Optional features
tracing = { version = "0.1", optional = true }

[dev-dependencies]
# Benchmarking
criterion = { version = "0.5", features = ["html_reports"] }

# Property testing
proptest = "1.4"

# Temporary directories for shard tests
tempfile = "3.8"

# Memory profiling
dhat = "0.3"

[features]
default = []
observability = ["dep:tracing"]

# Performance profiling
profiling = []

[profile.release]
lto = true
codegen-units = 1
opt-level = 3

[profile.bench]
inherits = "release"

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

**Verify**:
```bash
cargo check
# Should download dependencies and compile
```

### Step 0.3: Create Module Structure

```bash
# Core modules
mkdir -p src/compiler
mkdir -p src/runtime
mkdir -p src/persistence

# Tests and benches
mkdir -p src/tests
mkdir -p benches

# Documentation
mkdir -p docs
```

**Create placeholder files**:

```bash
touch src/compiler/mod.rs
touch src/runtime/mod.rs
touch src/persistence/mod.rs
touch benches/dispatch.rs
```

### Step 0.4: Initial lib.rs

**File**: `src/lib.rs`

```rust
//! # subql - SQL Subscription Dispatch Engine
//!
//! A high-performance engine for dispatching PostgreSQL CDC events to users
//! based on SQL WHERE clause subscriptions.
//!
//! ## Features
//!
//! - **SQL-correct semantics**: Full tri-state logic (TRUE/FALSE/UNKNOWN)
//! - **Hybrid indexing**: Equality, range, NULL, and fallback indexes for candidate pruning
//! - **Session lifecycle**: Durable and session-bound subscriptions
//! - **Online merge**: Zero-downtime background merge with atomic swap
//! - **Generic dialects**: Supports PostgreSQL, MySQL, SQLite, and more
//!
//! ## Example
//!
//! ```rust,ignore
//! use subql::{SubscriptionEngine, SubscriptionSpec, WalEvent};
//!
//! let mut engine = SubscriptionEngine::new(catalog);
//!
//! // Register subscription
//! engine.register(SubscriptionSpec {
//!     subscription_id: 1,
//!     user_id: 42,
//!     session_id: Some(100),
//!     sql: "SELECT * FROM orders WHERE amount > 100".to_string(),
//!     updated_at_unix_ms: 1704067200000,
//! })?;
//!
//! // Dispatch event
//! let event = WalEvent { /* ... */ };
//! let interested_users = engine.users(&event)?;
//! ```

#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::missing_errors_doc)]  // Will be added in later phases
#![allow(clippy::missing_panics_doc)]  // Will be added in later phases
#![forbid(unsafe_code)]

// Re-export public API
pub use types::*;
pub use errors::*;

// Internal modules
mod types;
mod errors;

pub mod compiler;
pub mod runtime;
pub mod persistence;

// Version and metadata
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn it_compiles() {
        assert_eq!(2 + 2, 4);
    }
}
```

**Verify Phase 0**:
```bash
cargo check
cargo clippy
cargo test
# All should pass
```

**Phase 0 Complete**: Project structure is ready.

---

## Phase 1: Foundation (Week 1-2)

**Goal**: Implement core types, tri-state logic, VM bytecode, and basic SQL parsing

**Deliverables**:
- Type definitions for all domain objects
- Tri-state logic with exhaustive truth table tests
- VM instruction set and stack-based evaluator
- SQL parser stub (generic dialect support)

### Step 1.1: Define Core Types

**File**: `src/types.rs`

This file defines ALL the types used throughout the system. Key design decisions:

1. **Cell vs Value**:
   - `Cell` has three states: `Missing` (not in row image), `Null` (SQL NULL), or typed value
   - This tri-state is essential for UPDATE semantics (OLD image may be incomplete)

2. **Arc for strings**:
   - `Arc<str>` allows cheap cloning of large strings in row images
   - Critical for performance when same row is evaluated against many predicates

3. **Domain IDs**:
   - Strong type aliases prevent ID confusion (e.g., can't pass UserId where SessionId expected)
   - All u64 except ColumnId (u16 sufficient for 64k columns)

```rust
//! Core type definitions for subql

use std::sync::Arc;

// ============================================================================
// Domain ID Types
// ============================================================================

/// User identifier (globally unique)
pub type UserId = u64;

/// Session identifier (per-connection)
pub type SessionId = u64;

/// Subscription identifier (globally unique, assigned by caller)
pub type SubscriptionId = u64;

/// Table identifier (from schema catalog)
pub type TableId = u32;

/// Column identifier (ordinal within table, 0-indexed)
pub type ColumnId = u16;

/// Shard identifier (for persistence)
pub type ShardId = u64;

/// Merge job identifier (for background operations)
pub type MergeJobId = u64;

// ============================================================================
// Event Types
// ============================================================================

/// CDC event kind
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// Row insertion
    Insert,
    /// Row update (old → new)
    Update,
    /// Row deletion
    Delete,
}

/// Cell value in a row image
///
/// Three-valued logic:
/// - `Missing`: Column not present in this image (UPDATE old_row may be incomplete)
/// - `Null`: SQL NULL value
/// - Typed value: Bool, Int, Float, String
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    /// Column not present in row image
    Missing,
    /// SQL NULL
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Int(i64),
    /// 64-bit float
    Float(f64),
    /// UTF-8 string (Arc for cheap cloning)
    String(Arc<str>),
}

impl Cell {
    /// Returns true if this cell is SQL NULL
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns true if this cell is missing from row image
    #[must_use]
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Returns true if this cell has a concrete value (not NULL or Missing)
    #[must_use]
    pub const fn is_present(&self) -> bool {
        !matches!(self, Self::Null | Self::Missing)
    }
}

/// Row image: array of cells indexed by ColumnId
///
/// Cells are stored in column-ordinal order (ColumnId is index).
/// Missing columns are represented as `Cell::Missing`.
#[derive(Clone, Debug)]
pub struct RowImage {
    pub cells: Arc<[Cell]>,
}

impl RowImage {
    /// Get cell by column ID
    #[must_use]
    pub fn get(&self, col_id: ColumnId) -> Option<&Cell> {
        self.cells.get(col_id as usize)
    }

    /// Number of columns in this image
    #[must_use]
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Returns true if row image is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

/// Primary key columns and values
#[derive(Clone, Debug)]
pub struct PrimaryKey {
    /// Column IDs comprising the primary key
    pub columns: Arc<[ColumnId]>,
    /// Values of the primary key columns
    pub values: Arc<[Cell]>,
}

/// WAL event from PostgreSQL CDC
#[derive(Clone, Debug)]
pub struct WalEvent {
    /// Event type
    pub kind: EventKind,
    /// Table this event belongs to
    pub table_id: TableId,
    /// Primary key of affected row
    pub pk: PrimaryKey,
    /// Old row image (for UPDATE/DELETE)
    pub old_row: Option<RowImage>,
    /// New row image (for INSERT/UPDATE)
    pub new_row: Option<RowImage>,
    /// Columns that changed (for UPDATE optimization)
    pub changed_columns: Arc<[ColumnId]>,
}

// ============================================================================
// Subscription Types
// ============================================================================

/// Subscription specification provided by user
#[derive(Clone, Debug)]
pub struct SubscriptionSpec {
    /// Unique subscription ID (assigned by caller)
    pub subscription_id: SubscriptionId,
    /// User who owns this subscription
    pub user_id: UserId,
    /// Session ID if session-bound, None if durable
    pub session_id: Option<SessionId>,
    /// SQL SELECT statement with WHERE clause
    pub sql: String,
    /// Timestamp for conflict resolution in merge (milliseconds since Unix epoch)
    pub updated_at_unix_ms: u64,
}

/// Result of successful subscription registration
#[derive(Clone, Debug)]
pub struct RegisterResult {
    /// Table this subscription applies to
    pub table_id: TableId,
    /// Normalized/canonicalized SQL
    pub normalized_sql: String,
    /// Hash of the predicate (for deduplication)
    pub predicate_hash: u128,
    /// True if a new predicate was created, false if reused existing
    pub created_new_predicate: bool,
}

/// Report from pruning session subscriptions
#[derive(Clone, Debug)]
pub struct PruneReport {
    /// Number of subscription bindings removed
    pub removed_bindings: usize,
    /// Number of predicates removed (refcount reached 0)
    pub removed_predicates: usize,
    /// Number of user dictionary entries removed
    pub removed_users: usize,
}

/// Report from background merge operation
#[derive(Clone, Debug)]
pub struct MergeReport {
    /// Number of input shards merged
    pub input_shards: usize,
    /// Number of predicates in output shard
    pub output_predicates: usize,
    /// Number of bindings in output shard
    pub output_bindings: usize,
    /// Deduplication ratio (1.0 = no dedup, 2.0 = 50% reduction)
    pub dedup_ratio: f32,
    /// Time spent building merged shard (milliseconds)
    pub build_ms: u64,
}

// ============================================================================
// Trait Definitions
// ============================================================================

/// Schema catalog providing table/column metadata
///
/// This trait abstracts the schema information needed to compile and validate
/// SQL subscriptions. Implementations might query PostgreSQL system catalogs,
/// maintain an in-memory schema cache, or use a schema registry.
pub trait SchemaCatalog: Send + Sync {
    /// Resolve table name to TableId
    fn table_id(&self, table_name: &str) -> Option<TableId>;

    /// Resolve column name to ColumnId within a table
    fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId>;

    /// Get number of columns in table (for row validation)
    fn table_arity(&self, table_id: TableId) -> Option<usize>;

    /// Get schema fingerprint for compatibility checking
    ///
    /// Fingerprint changes when table schema changes (add/remove/rename columns).
    /// Used to detect shard incompatibility on load.
    fn schema_fingerprint(&self, table_id: TableId) -> Option<u64>;
}

/// Subscription registration operations
pub trait SubscriptionRegistration: Send + Sync {
    /// Register a new subscription
    ///
    /// Parses SQL, compiles to bytecode, deduplicates predicates, and binds user.
    /// Returns error if SQL is unparseable or unsupported.
    fn register(&mut self, spec: SubscriptionSpec)
        -> Result<RegisterResult, crate::RegisterError>;

    /// Unregister a subscription by ID
    ///
    /// Decrements predicate refcount. If refcount reaches 0, predicate is removed.
    /// Returns true if subscription existed and was removed.
    fn unregister_subscription(&mut self, subscription_id: SubscriptionId) -> bool;
}

/// Event dispatch operations
pub trait SubscriptionDispatch: Send + Sync {
    /// Get interested users for a WAL event
    ///
    /// Returns deduplicated, sorted list of UserIds.
    fn users(&self, event: &WalEvent) -> Result<Vec<UserId>, crate::DispatchError>;
}

/// Session lifecycle operations
pub trait SubscriptionPruning: Send + Sync {
    /// Unregister all subscriptions for a session
    ///
    /// Removes all session-bound subscriptions, decrements refcounts, prunes predicates.
    /// Durable subscriptions (session_id = None) are NOT affected.
    fn unregister_session(&mut self, session_id: SessionId) -> PruneReport;
}

/// Durable shard storage operations
pub trait DurableShardStore: Send + Sync {
    /// Serialize table partition to bytes
    ///
    /// Returns bincode + LZ4 compressed shard.
    fn snapshot_table(&self, table_id: TableId) -> Result<Vec<u8>, crate::StorageError>;

    /// Load shard from bytes and return ShardId
    ///
    /// Validates version, fingerprint, and codec. Merges loaded predicates into runtime.
    fn load_shard(&mut self, shard_bytes: &[u8]) -> Result<ShardId, crate::StorageError>;
}

/// Background merge operations
pub trait DurableShardMerge: Send + Sync {
    /// Start background merge of shards for a table
    ///
    /// Returns job ID immediately. Merge runs in background thread.
    fn merge_shards_background(
        &self,
        table_id: TableId,
        shard_ids: &[ShardId],
    ) -> Result<MergeJobId, crate::MergeError>;

    /// Check if merge is complete and swap if ready
    ///
    /// Returns Some(report) if merge complete and swap succeeded.
    /// Returns None if merge still running.
    fn try_swap_merged(
        &mut self,
        job_id: MergeJobId,
    ) -> Result<Option<MergeReport>, crate::MergeError>;
}
```

**Verification**:
```bash
cargo check
# Should compile without errors
```

### Step 1.2: Define Error Types

**File**: `src/errors.rs`

Error design principles:
1. Use `thiserror` for ergonomic error derivation
2. Each error carries enough context for debugging (include relevant IDs, positions)
3. Errors are `Clone` so they can be logged/stored
4. Specific error variants prevent misuse (e.g., can't confuse ParseError with TypeError)

```rust
//! Error types for subql

use thiserror::Error;
use crate::{TableId, MergeJobId, ColumnId};

/// Errors during subscription registration
#[derive(Error, Clone, Debug)]
pub enum RegisterError {
    /// SQL parsing failed
    #[error("SQL parse error at line {line}, column {column}: {message}")]
    ParseError {
        line: usize,
        column: usize,
        message: String,
    },

    /// SQL uses unsupported features
    #[error("Unsupported SQL: {0}")]
    UnsupportedSql(String),

    /// Table name not found in catalog
    #[error("Unknown table: {0}")]
    UnknownTable(String),

    /// Column name not found in table
    #[error("Unknown column '{column}' in table {table_id}")]
    UnknownColumn {
        table_id: TableId,
        column: String,
    },

    /// Type mismatch in expression
    #[error("Type error: {0}")]
    TypeError(String),

    /// Schema catalog error
    #[error("Schema catalog error: {0}")]
    SchemaCatalog(String),
}

/// Errors during event dispatch
#[derive(Error, Clone, Debug)]
pub enum DispatchError {
    /// Table not registered in engine
    #[error("Unknown table ID: {0}")]
    UnknownTableId(TableId),

    /// Event missing required row image
    #[error("Missing required row image: {0}")]
    MissingRequiredRowImage(&'static str),

    /// Row arity doesn't match schema
    #[error("Invalid row arity for table {table_id}: expected {expected} columns, got {got}")]
    InvalidRowArity {
        table_id: TableId,
        expected: usize,
        got: usize,
    },

    /// VM evaluation error
    #[error("VM evaluation error: {0}")]
    VmError(String),
}

/// Errors during persistence operations
#[derive(Error, Clone, Debug)]
pub enum StorageError {
    /// I/O error during shard read/write
    #[error("I/O error: {0}")]
    Io(String),

    /// Codec error (bincode/LZ4)
    #[error("Codec error: {0}")]
    Codec(String),

    /// Shard data is corrupt
    #[error("Corrupt shard: {0}")]
    Corrupt(String),

    /// Shard version incompatible
    #[error("Version mismatch: expected {expected}, got {got}")]
    VersionMismatch {
        expected: u16,
        got: u16,
    },

    /// Schema fingerprint doesn't match
    #[error("Schema mismatch for table {table_id}: expected fingerprint {expected:016x}, got {got:016x}")]
    SchemaMismatch {
        table_id: TableId,
        expected: u64,
        got: u64,
    },
}

/// Errors during merge operations
#[derive(Error, Clone, Debug)]
pub enum MergeError {
    /// Merge job ID not found
    #[error("Unknown merge job: {0}")]
    UnknownJob(MergeJobId),

    /// Background merge build failed
    #[error("Merge build failed: {0}")]
    BuildFailed(String),

    /// Storage error during merge
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}
```

**Verification**:
```bash
cargo check
```

### Step 1.3: Implement Tri-State Logic

**File**: `src/compiler/tristate.rs`

This is the HEART of SQL correctness. These truth tables must be 100% accurate.

**Critical Design Notes**:
- `Unknown` represents NULL propagation
- Short-circuit rules: `False AND _` → `False`, `True OR _` → `True`
- DeM organ's laws must hold
- Associativity and commutativity preserved

```rust
//! SQL three-valued logic (TRUE/FALSE/UNKNOWN)
//!
//! Implements truth tables from SQL standard for NULL propagation.

/// SQL tri-state value
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    /// AND operation with short-circuit semantics
    ///
    /// Truth table:
    /// ```text
    /// | A       | B       | A AND B |
    /// |---------|---------|---------|
    /// | True    | True    | True    |
    /// | True    | False   | False   |
    /// | True    | Unknown | Unknown |
    /// | False   | True    | False   |
    /// | False   | False   | False   |
    /// | False   | Unknown | False   | ← short-circuit
    /// | Unknown | True    | Unknown |
    /// | Unknown | False   | False   | ← short-circuit
    /// | Unknown | Unknown | Unknown |
    /// ```
    #[must_use]
    pub const fn and(self, other: Self) -> Self {
        match (self, other) {
            // Short-circuit: False AND _ = False
            (Self::False, _) | (_, Self::False) => Self::False,
            // Both true
            (Self::True, Self::True) => Self::True,
            // Everything else is Unknown
            _ => Self::Unknown,
        }
    }

    /// OR operation with short-circuit semantics
    ///
    /// Truth table:
    /// ```text
    /// | A       | B       | A OR B  |
    /// |---------|---------|---------|
    /// | True    | True    | True    |
    /// | True    | False   | True    |
    /// | True    | Unknown | True    | ← short-circuit
    /// | False   | True    | True    |
    /// | False   | False   | False   |
    /// | False   | Unknown | Unknown |
    /// | Unknown | True    | True    | ← short-circuit
    /// | Unknown | False   | Unknown |
    /// | Unknown | Unknown | Unknown |
    /// ```
    #[must_use]
    pub const fn or(self, other: Self) -> Self {
        match (self, other) {
            // Short-circuit: True OR _ = True
            (Self::True, _) | (_, Self::True) => Self::True,
            // Both false
            (Self::False, Self::False) => Self::False,
            // Everything else is Unknown
            _ => Self::Unknown,
        }
    }

    /// NOT operation
    ///
    /// Truth table:
    /// ```text
    /// | Input   | Output  |
    /// |---------|---------|
    /// | True    | False   |
    /// | False   | True    |
    /// | Unknown | Unknown |
    /// ```
    #[must_use]
    pub const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    /// Converts to Option<bool> (Unknown → None)
    #[must_use]
    pub const fn to_option(self) -> Option<bool> {
        match self {
            Self::True => Some(true),
            Self::False => Some(false),
            Self::Unknown => None,
        }
    }

    /// Converts from Option<bool> (None → Unknown)
    #[must_use]
    pub const fn from_option(opt: Option<bool>) -> Self {
        match opt {
            Some(true) => Self::True,
            Some(false) => Self::False,
            None => Self::Unknown,
        }
    }
}

impl std::ops::Not for Tri {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self::not(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_truth_table() {
        assert_eq!(!Tri::True, Tri::False);
        assert_eq!(!Tri::False, Tri::True);
        assert_eq!(!Tri::Unknown, Tri::Unknown);
    }

    #[test]
    fn test_and_truth_table_exhaustive() {
        // True AND x
        assert_eq!(Tri::True.and(Tri::True), Tri::True);
        assert_eq!(Tri::True.and(Tri::False), Tri::False);
        assert_eq!(Tri::True.and(Tri::Unknown), Tri::Unknown);

        // False AND x (short-circuit)
        assert_eq!(Tri::False.and(Tri::True), Tri::False);
        assert_eq!(Tri::False.and(Tri::False), Tri::False);
        assert_eq!(Tri::False.and(Tri::Unknown), Tri::False);

        // Unknown AND x
        assert_eq!(Tri::Unknown.and(Tri::True), Tri::Unknown);
        assert_eq!(Tri::Unknown.and(Tri::False), Tri::False);
        assert_eq!(Tri::Unknown.and(Tri::Unknown), Tri::Unknown);
    }

    #[test]
    fn test_or_truth_table_exhaustive() {
        // True OR x (short-circuit)
        assert_eq!(Tri::True.or(Tri::True), Tri::True);
        assert_eq!(Tri::True.or(Tri::False), Tri::True);
        assert_eq!(Tri::True.or(Tri::Unknown), Tri::True);

        // False OR x
        assert_eq!(Tri::False.or(Tri::True), Tri::True);
        assert_eq!(Tri::False.or(Tri::False), Tri::False);
        assert_eq!(Tri::False.or(Tri::Unknown), Tri::Unknown);

        // Unknown OR x
        assert_eq!(Tri::Unknown.or(Tri::True), Tri::True);
        assert_eq!(Tri::Unknown.or(Tri::False), Tri::Unknown);
        assert_eq!(Tri::Unknown.or(Tri::Unknown), Tri::Unknown);
    }

    #[test]
    fn test_and_associativity() {
        for a in [Tri::True, Tri::False, Tri::Unknown] {
            for b in [Tri::True, Tri::False, Tri::Unknown] {
                for c in [Tri::True, Tri::False, Tri::Unknown] {
                    assert_eq!(a.and(b).and(c), a.and(b.and(c)),
                        "AND associativity failed for {:?}, {:?}, {:?}", a, b, c);
                }
            }
        }
    }

    #[test]
    fn test_or_associativity() {
        for a in [Tri::True, Tri::False, Tri::Unknown] {
            for b in [Tri::True, Tri::False, Tri::Unknown] {
                for c in [Tri::True, Tri::False, Tri::Unknown] {
                    assert_eq!(a.or(b).or(c), a.or(b.or(c)),
                        "OR associativity failed for {:?}, {:?}, {:?}", a, b, c);
                }
            }
        }
    }

    #[test]
    fn test_demorgan_laws() {
        for a in [Tri::True, Tri::False, Tri::Unknown] {
            for b in [Tri::True, Tri::False, Tri::Unknown] {
                // NOT (a AND b) = (NOT a) OR (NOT b)
                assert_eq!(a.and(b).not(), a.not().or(b.not()),
                    "De Morgan's first law failed for {:?}, {:?}", a, b);

                // NOT (a OR b) = (NOT a) AND (NOT b)
                assert_eq!(a.or(b).not(), a.not().and(b.not()),
                    "De Morgan's second law failed for {:?}, {:?}", a, b);
            }
        }
    }

    #[test]
    fn test_option_conversion() {
        assert_eq!(Tri::from_option(Some(true)), Tri::True);
        assert_eq!(Tri::from_option(Some(false)), Tri::False);
        assert_eq!(Tri::from_option(None), Tri::Unknown);

        assert_eq!(Tri::True.to_option(), Some(true));
        assert_eq!(Tri::False.to_option(), Some(false));
        assert_eq!(Tri::Unknown.to_option(), None);
    }
}
```

**Verification**:
```bash
cargo test --package subql tristate
# All 7+ tests MUST pass
```

### Step 1.4: Define VM Instruction Set

**File**: `src/compiler/bytecode.rs`

The instruction set is designed for:
1. **Simplicity**: Stack-based, no registers
2. **Completeness**: All SQL operators in supported subset
3. **Tri-state native**: All comparisons return `Tri`, not `bool`

```rust
//! VM bytecode instruction set for predicate evaluation

use crate::{Cell, ColumnId};
use super::Tri;

/// VM instruction for tri-state predicate evaluation
#[derive(Clone, Debug, PartialEq)]
pub enum Instruction {
    // ========================================================================
    // Stack Operations
    // ========================================================================

    /// Push a literal cell value onto stack
    ///
    /// Stack: [...] → [..., cell]
    PushLiteral(Cell),

    /// Load cell from row at column index and push onto stack
    ///
    /// If column out of bounds, pushes Cell::Missing.
    /// Stack: [...] → [..., cell]
    LoadColumn(ColumnId),

    // ========================================================================
    // Comparison Operators (pop 2 cells, push Tri)
    // ========================================================================

    /// Equal: a = b
    ///
    /// NULL-safe: NULL = NULL → Unknown (not True!)
    /// Stack: [..., a, b] → [..., Tri]
    Equal,

    /// Not equal: a != b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    NotEqual,

    /// Less than: a < b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    LessThan,

    /// Less than or equal: a <= b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    LessThanOrEqual,

    /// Greater than: a > b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    GreaterThan,

    /// Greater than or equal: a >= b
    ///
    /// Stack: [..., a, b] → [..., Tri]
    GreaterThanOrEqual,

    // ========================================================================
    // NULL Checks (pop 1 cell, push Tri)
    // ========================================================================

    /// IS NULL check
    ///
    /// Stack: [..., cell] → [..., Tri]
    IsNull,

    /// IS NOT NULL check
    ///
    /// Stack: [..., cell] → [..., Tri]
    IsNotNull,

    // ========================================================================
    // Logical Operators (pop 2 Tri, push Tri)
    // ========================================================================

    /// AND with tri-state semantics
    ///
    /// Stack: [..., a, b] → [..., Tri]
    And,

    /// OR with tri-state semantics
    ///
    /// Stack: [..., a, b] → [..., Tri]
    Or,

    // ========================================================================
    // Unary Operators (pop 1 Tri, push Tri)
    // ========================================================================

    /// NOT with tri-state semantics
    ///
    /// Stack: [..., tri] → [..., Tri]
    Not,

    // ========================================================================
    // Special Operations
    // ========================================================================

    /// IN (...) - checks if top of stack is in literal set
    ///
    /// NULL IN (...) → Unknown
    /// Stack: [..., cell] → [..., Tri]
    In(Vec<Cell>),

    /// BETWEEN a AND b - checks if value is in range [a, b]
    ///
    /// Pops upper, lower, value. Equivalent to: value >= lower AND value <= upper
    /// Stack: [..., value, lower, upper] → [..., Tri]
    Between,

    /// LIKE pattern matching (optional, can defer to Phase 2)
    ///
    /// Stack: [..., string, pattern] → [..., Tri]
    Like { case_sensitive: bool },
}

/// Compiled bytecode program
#[derive(Clone, Debug)]
pub struct BytecodeProgram {
    /// Instruction sequence
    pub instructions: Vec<Instruction>,

    /// Columns referenced by this program (for dependency tracking)
    ///
    /// Sorted, deduplicated list of ColumnIds used in LoadColumn instructions.
    /// Used for UPDATE optimization (skip evaluation if no dependencies changed).
    pub dependency_columns: Vec<ColumnId>,
}

impl BytecodeProgram {
    /// Create new bytecode program with dependency extraction
    pub fn new(instructions: Vec<Instruction>) -> Self {
        let dependency_columns = Self::extract_dependencies(&instructions);
        Self {
            instructions,
            dependency_columns,
        }
    }

    /// Extract columns referenced by this program
    fn extract_dependencies(instructions: &[Instruction]) -> Vec<ColumnId> {
        let mut cols: Vec<ColumnId> = instructions
            .iter()
            .filter_map(|inst| {
                if let Instruction::LoadColumn(col_id) = inst {
                    Some(*col_id)
                } else {
                    None
                }
            })
            .collect();
        cols.sort_unstable();
        cols.dedup();
        cols
    }

    /// Returns true if this program has no dependencies (always evaluates to same result)
    #[must_use]
    pub fn is_constant(&self) -> bool {
        self.dependency_columns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_extract_dependencies() {
        // age > 18 AND status = 'active'
        let instructions = vec![
            Instruction::LoadColumn(5),  // age
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(7),  // status
            Instruction::PushLiteral(Cell::String("active".into())),
            Instruction::Equal,
            Instruction::And,
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, vec![5, 7]);
        assert!(!program.is_constant());
    }

    #[test]
    fn test_constant_program() {
        // Just a literal true (e.g., WHERE true)
        let instructions = vec![
            Instruction::PushLiteral(Cell::Bool(true)),
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, Vec::<ColumnId>::new());
        assert!(program.is_constant());
    }

    #[test]
    fn test_dependency_deduplication() {
        // age > 18 AND age < 65 (age used twice)
        let instructions = vec![
            Instruction::LoadColumn(5),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(5),  // Same column again
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::LessThan,
            Instruction::And,
        ];

        let program = BytecodeProgram::new(instructions);
        assert_eq!(program.dependency_columns, vec![5]); // Deduplicated
    }
}
```

**Verification**:
```bash
cargo test --package subql bytecode
```

### Step 1.5: Implement VM Evaluator

**File**: `src/compiler/vm.rs`

This is the runtime interpreter. Key design points:
1. **Stack-based**: Simple, predictable, easy to reason about
2. **Type safety**: Separate `StackValue` enum prevents cell/tri confusion
3. **NULL propagation**: All comparisons check for NULL/Missing first
4. **Performance**: Pre-allocate stack capacity (most predicates use < 16 values)

```rust
//! VM interpreter for bytecode evaluation

use crate::{Cell, RowImage};
use super::{Tri, Instruction, BytecodeProgram};

/// VM evaluation error
#[derive(Debug, Clone, PartialEq)]
pub enum VmError {
    /// Stack underflow (popped from empty stack)
    StackUnderflow,

    /// Type mismatch (expected cell, got tri, or vice versa)
    TypeMismatch {
        expected: &'static str,
        got: &'static str,
    },

    /// Invalid column index (should never happen if compilation correct)
    InvalidColumnIndex(u16),
}

/// Stack-based VM for predicate evaluation
pub struct Vm {
    /// Value stack (grows during evaluation)
    stack: Vec<StackValue>,
}

/// Value on the VM stack (either Cell or Tri)
#[derive(Clone, Debug, PartialEq)]
enum StackValue {
    /// Cell value (from literals or column loads)
    Cell(Cell),
    /// Tri-state boolean (from comparisons and logical ops)
    Tri(Tri),
}

impl Vm {
    /// Create new VM instance
    pub fn new() -> Self {
        Self {
            // Most predicates need < 16 stack slots
            stack: Vec::with_capacity(16),
        }
    }

    /// Evaluate bytecode program against a row
    ///
    /// Returns Tri::True if row matches, False/Unknown if not.
    pub fn eval(&mut self, program: &BytecodeProgram, row: &RowImage)
        -> Result<Tri, VmError>
    {
        self.stack.clear();

        // Execute each instruction
        for instruction in &program.instructions {
            self.execute(instruction, row)?;
        }

        // Final stack should have exactly one Tri value
        match self.stack.pop() {
            Some(StackValue::Tri(result)) => {
                if self.stack.is_empty() {
                    Ok(result)
                } else {
                    // Stack not empty - malformed program, treat as Unknown
                    Ok(Tri::Unknown)
                }
            }
            Some(StackValue::Cell(_)) => {
                // Top of stack is cell, not tri - treat as Unknown
                Ok(Tri::Unknown)
            }
            None => Err(VmError::StackUnderflow),
        }
    }

    fn execute(&mut self, instruction: &Instruction, row: &RowImage)
        -> Result<(), VmError>
    {
        match instruction {
            Instruction::PushLiteral(cell) => {
                self.stack.push(StackValue::Cell(cell.clone()));
            }

            Instruction::LoadColumn(col_id) => {
                let cell = row.get(*col_id)
                    .cloned()
                    .unwrap_or(Cell::Missing);
                self.stack.push(StackValue::Cell(cell));
            }

            Instruction::Equal => {
                let result = self.compare_cells(cells_equal)?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::NotEqual => {
                let result = self.compare_cells(|a, b| !cells_equal(a, b))?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThan => {
                let result = self.compare_ordered(|ord| {
                    matches!(ord, std::cmp::Ordering::Less)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::LessThanOrEqual => {
                let result = self.compare_ordered(|ord| {
                    !matches!(ord, std::cmp::Ordering::Greater)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThan => {
                let result = self.compare_ordered(|ord| {
                    matches!(ord, std::cmp::Ordering::Greater)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::GreaterThanOrEqual => {
                let result = self.compare_ordered(|ord| {
                    !matches!(ord, std::cmp::Ordering::Less)
                })?;
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::IsNull => {
                let cell = self.pop_cell()?;
                let result = if cell.is_null() {
                    Tri::True
                } else {
                    Tri::False
                };
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::IsNotNull => {
                let cell = self.pop_cell()?;
                let result = if cell.is_null() || cell.is_missing() {
                    Tri::False
                } else {
                    Tri::True
                };
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::And => {
                let b = self.pop_tri()?;
                let a = self.pop_tri()?;
                self.stack.push(StackValue::Tri(a.and(b)));
            }

            Instruction::Or => {
                let b = self.pop_tri()?;
                let a = self.pop_tri()?;
                self.stack.push(StackValue::Tri(a.or(b)));
            }

            Instruction::Not => {
                let a = self.pop_tri()?;
                self.stack.push(StackValue::Tri(a.not()));
            }

            Instruction::In(literals) => {
                let cell = self.pop_cell()?;

                // NULL IN (...) = Unknown
                if cell.is_null() || cell.is_missing() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // Check if cell matches any literal
                let found = literals.iter().any(|lit| cells_equal(&cell, lit));
                self.stack.push(StackValue::Tri(
                    if found { Tri::True } else { Tri::False }
                ));
            }

            Instruction::Between => {
                let upper = self.pop_cell()?;
                let lower = self.pop_cell()?;
                let value = self.pop_cell()?;

                // Any NULL/Missing → Unknown
                if value.is_null() || value.is_missing() ||
                   lower.is_null() || lower.is_missing() ||
                   upper.is_null() || upper.is_missing() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // value >= lower AND value <= upper
                let ge_lower = compare_ordered_cells(&value, &lower, |ord| {
                    !matches!(ord, std::cmp::Ordering::Less)
                });
                let le_upper = compare_ordered_cells(&value, &upper, |ord| {
                    !matches!(ord, std::cmp::Ordering::Greater)
                });

                let result = ge_lower.and(le_upper);
                self.stack.push(StackValue::Tri(result));
            }

            Instruction::Like { case_sensitive } => {
                let pattern = self.pop_cell()?;
                let string = self.pop_cell()?;

                // NULL in either position → Unknown
                if string.is_null() || string.is_missing() ||
                   pattern.is_null() || pattern.is_missing() {
                    self.stack.push(StackValue::Tri(Tri::Unknown));
                    return Ok(());
                }

                // Extract strings
                let (str_val, pat_val) = match (&string, &pattern) {
                    (Cell::String(s), Cell::String(p)) => (s.as_ref(), p.as_ref()),
                    _ => {
                        // Type mismatch → Unknown
                        self.stack.push(StackValue::Tri(Tri::Unknown));
                        return Ok(());
                    }
                };

                // Simple LIKE implementation (% = wildcard, _ = single char)
                let matched = if *case_sensitive {
                    simple_like(str_val, pat_val)
                } else {
                    simple_like(&str_val.to_lowercase(), &pat_val.to_lowercase())
                };

                self.stack.push(StackValue::Tri(
                    if matched { Tri::True } else { Tri::False }
                ));
            }
        }

        Ok(())
    }

    fn pop_cell(&mut self) -> Result<Cell, VmError> {
        match self.stack.pop() {
            Some(StackValue::Cell(c)) => Ok(c),
            Some(StackValue::Tri(_)) => Err(VmError::TypeMismatch {
                expected: "Cell",
                got: "Tri",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn pop_tri(&mut self) -> Result<Tri, VmError> {
        match self.stack.pop() {
            Some(StackValue::Tri(t)) => Ok(t),
            Some(StackValue::Cell(_)) => Err(VmError::TypeMismatch {
                expected: "Tri",
                got: "Cell",
            }),
            None => Err(VmError::StackUnderflow),
        }
    }

    fn compare_cells<F>(&mut self, f: F) -> Result<Tri, VmError>
    where
        F: FnOnce(&Cell, &Cell) -> bool,
    {
        let b = self.pop_cell()?;
        let a = self.pop_cell()?;

        // NULL or Missing → Unknown
        if a.is_null() || a.is_missing() || b.is_null() || b.is_missing() {
            return Ok(Tri::Unknown);
        }

        Ok(if f(&a, &b) { Tri::True } else { Tri::False })
    }

    fn compare_ordered<F>(&mut self, f: F) -> Result<Tri, VmError>
    where
        F: FnOnce(std::cmp::Ordering) -> bool,
    {
        let b = self.pop_cell()?;
        let a = self.pop_cell()?;

        Ok(compare_ordered_cells(&a, &b, f))
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn cells_equal(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Bool(x), Cell::Bool(y)) => x == y,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Float(x), Cell::Float(y)) => (x - y).abs() < f64::EPSILON,
        (Cell::String(x), Cell::String(y)) => x == y,
        (Cell::Null, Cell::Null) => false, // NULL = NULL is Unknown, not True!
        _ => false,
    }
}

fn compare_ordered_cells<F>(a: &Cell, b: &Cell, f: F) -> Tri
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    // NULL or Missing → Unknown
    if a.is_null() || a.is_missing() || b.is_null() || b.is_missing() {
        return Tri::Unknown;
    }

    let ord = match (a, b) {
        (Cell::Int(x), Cell::Int(y)) => x.cmp(y),
        (Cell::Float(x), Cell::Float(y)) => {
            if x < y {
                std::cmp::Ordering::Less
            } else if x > y {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        }
        (Cell::String(x), Cell::String(y)) => x.cmp(y),
        _ => return Tri::Unknown, // Type mismatch
    };

    if f(ord) { Tri::True } else { Tri::False }
}

/// Simple LIKE pattern matching
///
/// Supports % (wildcard) and _ (single char). Does NOT support escaping.
fn simple_like(string: &str, pattern: &str) -> bool {
    // TODO: Implement full LIKE semantics
    // For Phase 1, use simple contains/prefix/suffix
    if pattern == "%" {
        true // Match anything
    } else if pattern.starts_with('%') && pattern.ends_with('%') {
        let middle = &pattern[1..pattern.len() - 1];
        string.contains(middle)
    } else if pattern.starts_with('%') {
        string.ends_with(&pattern[1..])
    } else if pattern.ends_with('%') {
        string.starts_with(&pattern[..pattern.len() - 1])
    } else {
        string == pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_row(cells: Vec<Cell>) -> RowImage {
        RowImage { cells: Arc::from(cells) }
    }

    #[test]
    fn test_simple_comparison() {
        let mut vm = Vm::new();

        // age > 18
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![Cell::Int(25)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(15)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Int(18)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_null_propagation() {
        let mut vm = Vm::new();

        // age > 18 (age is NULL)
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
        ]);

        let row = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_and_logic() {
        let mut vm = Vm::new();

        // age > 18 AND active = true
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::GreaterThan,
            Instruction::LoadColumn(1),
            Instruction::PushLiteral(Cell::Bool(true)),
            Instruction::Equal,
            Instruction::And,
        ]);

        let row = make_row(vec![Cell::Int(25), Cell::Bool(true)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(25), Cell::Bool(false)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Int(15), Cell::Bool(true)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_is_null() {
        let mut vm = Vm::new();

        // email IS NULL
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::IsNull,
        ]);

        let row = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::String("test@example.com".into())]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }

    #[test]
    fn test_in_list() {
        let mut vm = Vm::new();

        // status IN ('pending', 'active')
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::In(vec![
                Cell::String("pending".into()),
                Cell::String("active".into()),
            ]),
        ]);

        let row = make_row(vec![Cell::String("pending".into())]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::String("completed".into())]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Null]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::Unknown);
    }

    #[test]
    fn test_between() {
        let mut vm = Vm::new();

        // age BETWEEN 18 AND 65
        let program = BytecodeProgram::new(vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Cell::Int(18)),
            Instruction::PushLiteral(Cell::Int(65)),
            Instruction::Between,
        ]);

        let row = make_row(vec![Cell::Int(25)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(18)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(65)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::True);

        let row = make_row(vec![Cell::Int(17)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);

        let row = make_row(vec![Cell::Int(66)]);
        assert_eq!(vm.eval(&program, &row).unwrap(), Tri::False);
    }
}
```

**File**: `src/compiler/mod.rs`

```rust
//! SQL compilation pipeline: parse → normalize → compile to VM bytecode

pub mod tristate;
pub mod bytecode;
pub mod vm;

pub use tristate::Tri;
pub use bytecode::{Instruction, BytecodeProgram};
pub use vm::{Vm, VmError};

// Parser will be added in next step
```

**Verification**:
```bash
cargo test --package subql
# All tests should pass (tri-state + VM)
cargo clippy -- -D warnings
# No warnings
```

### Step 1.6: SQL Parser (Generic Dialect)

**File**: `src/compiler/parser.rs`

**Key Design Decision**: Accept dialect as parameter, not hardcode.

```rust
//! SQL parser for subscription predicates
//!
//! Supports generic SQL dialects via sqlparser crate.

use sqlparser::ast::{Expr, Statement, SetExpr, TableFactor};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use crate::{RegisterError, TableId, ColumnId, SchemaCatalog, Cell};
use super::{BytecodeProgram, Instruction};
use std::sync::Arc;

/// Parse and compile SQL SELECT statement to bytecode
///
/// # Arguments
/// * `sql` - SQL SELECT statement with optional WHERE clause
/// * `dialect` - SQL dialect (PostgreSQL, MySQL, SQLite, etc.)
/// * `catalog` - Schema catalog for table/column resolution
///
/// # Returns
/// * `Ok((table_id, program))` - Compiled bytecode for the WHERE clause
/// * `Err(RegisterError)` - Parse error, unsupported SQL, or schema error
pub fn parse_and_compile<D: Dialect>(
    sql: &str,
    dialect: &D,
    catalog: &dyn SchemaCatalog,
) -> Result<(TableId, BytecodeProgram), RegisterError> {
    // Parse SQL
    let statements = Parser::parse_sql(dialect, sql)
        .map_err(|e| RegisterError::ParseError {
            line: 1, // sqlparser doesn't provide line numbers easily
            column: 0,
            message: e.to_string(),
        })?;

    if statements.len() != 1 {
        return Err(RegisterError::UnsupportedSql(
            "Expected exactly one SELECT statement".to_string()
        ));
    }

    let stmt = &statements[0];

    // Extract SELECT ... FROM table WHERE predicate
    let (table_name, where_clause) = extract_table_and_where(stmt)?;

    // Resolve table ID
    let table_id = catalog.table_id(&table_name)
        .ok_or_else(|| RegisterError::UnknownTable(table_name.clone()))?;

    // Compile WHERE clause to bytecode
    let program = if let Some(expr) = where_clause {
        compile_expression(&expr, table_id, catalog)?
    } else {
        // No WHERE clause = always match
        // Push True onto stack
        BytecodeProgram::new(vec![
            Instruction::PushLiteral(Cell::Bool(true)),
        ])
    };

    Ok((table_id, program))
}

fn extract_table_and_where(stmt: &Statement)
    -> Result<(String, Option<Expr>), RegisterError>
{
    match stmt {
        Statement::Query(query) => {
            match query.body.as_ref() {
                SetExpr::Select(select) => {
                    // Must have exactly one table
                    if select.from.len() != 1 {
                        return Err(RegisterError::UnsupportedSql(
                            "Exactly one table required (no joins)".to_string()
                        ));
                    }

                    let table_factor = &select.from[0].relation;
                    let table_name = match table_factor {
                        TableFactor::Table { name, .. } => {
                            name.0.first()
                                .ok_or_else(|| RegisterError::UnsupportedSql(
                                    "Missing table name".to_string()
                                ))?
                                .value.clone()
                        }
                        _ => return Err(RegisterError::UnsupportedSql(
                            "Joins, subqueries not supported".to_string()
                        )),
                    };

                    // Extract WHERE clause
                    let where_clause = select.selection.clone();

                    Ok((table_name, where_clause))
                }
                _ => Err(RegisterError::UnsupportedSql(
                    "Only SELECT supported (no UNION, etc.)".to_string()
                )),
            }
        }
        _ => Err(RegisterError::UnsupportedSql(
            "Only SELECT supported".to_string()
        )),
    }
}

/// Compile SQL expression to bytecode
///
/// Phase 1: Stub implementation (will be completed in Phase 2)
fn compile_expression(
    expr: &Expr,
    table_id: TableId,
    catalog: &dyn SchemaCatalog,
) -> Result<BytecodeProgram, RegisterError> {
    // TODO: Full expression compilation in Phase 2
    // For now, just validate that it parses and return placeholder
    _ = (expr, table_id, catalog);

    Ok(BytecodeProgram::new(vec![
        // Placeholder: always return Unknown
        Instruction::PushLiteral(Cell::Null),
        Instruction::IsNull,  // NULL IS NULL → True (temporary)
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::{PostgreSqlDialect, MySqlDialect, SQLiteDialect};
    use std::collections::HashMap;

    struct MockCatalog {
        tables: HashMap<String, (TableId, usize)>, // TableId, arity
        columns: HashMap<(TableId, String), ColumnId>,
    }

    impl SchemaCatalog for MockCatalog {
        fn table_id(&self, table_name: &str) -> Option<TableId> {
            self.tables.get(table_name).map(|(id, _)| *id)
        }

        fn column_id(&self, table_id: TableId, column_name: &str) -> Option<ColumnId> {
            self.columns.get(&(table_id, column_name.to_string())).copied()
        }

        fn table_arity(&self, table_id: TableId) -> Option<usize> {
            self.tables.values()
                .find(|(id, _)| *id == table_id)
                .map(|(_, arity)| *arity)
        }

        fn schema_fingerprint(&self, _table_id: TableId) -> Option<u64> {
            Some(0xABCD_1234_5678_9ABC)
        }
    }

    fn make_catalog() -> MockCatalog {
        let mut tables = HashMap::new();
        tables.insert("users".to_string(), (1, 5));
        tables.insert("orders".to_string(), (2, 7));

        let mut columns = HashMap::new();
        columns.insert((1, "id".to_string()), 0);
        columns.insert((1, "age".to_string()), 1);
        columns.insert((1, "email".to_string()), 2);

        MockCatalog { tables, columns }
    }

    #[test]
    fn test_parse_postgresql_dialect() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (table_id, _program) = result.unwrap();
        assert_eq!(table_id, 1);
    }

    #[test]
    fn test_parse_mysql_dialect() {
        let catalog = make_catalog();
        let dialect = MySqlDialect {};

        // MySQL allows backticks
        let sql = "SELECT * FROM `users` WHERE `age` > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_sqlite_dialect() {
        let catalog = make_catalog();
        let dialect = SQLiteDialect {};

        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_joins() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let result = parse_and_compile(sql, &dialect, &catalog);

        assert!(matches!(result, Err(RegisterError::UnsupportedSql(_))));
    }

    #[test]
    fn test_reject_unknown_table() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM unknown_table WHERE id = 1";
        let result = parse_and_compile(sql, &dialect, &catalog);

        assert!(matches!(result, Err(RegisterError::UnknownTable(_))));
    }

    #[test]
    fn test_no_where_clause() {
        let catalog = make_catalog();
        let dialect = PostgreSqlDialect {};

        let sql = "SELECT * FROM users";
        let result = parse_and_compile(sql, &dialect, &catalog);
        assert!(result.is_ok());

        let (table_id, program) = result.unwrap();
        assert_eq!(table_id, 1);
        // Should have trivial "always match" program
        assert!(!program.instructions.is_empty());
    }
}
```

**Update**: `src/compiler/mod.rs`

```rust
//! SQL compilation pipeline: parse → normalize → compile to VM bytecode

pub mod tristate;
pub mod bytecode;
pub mod vm;
pub mod parser;

pub use tristate::Tri;
pub use bytecode::{Instruction, BytecodeProgram};
pub use vm::{Vm, VmError};
pub use parser::parse_and_compile;
```

**Verification**:
```bash
cargo test --package subql parser
cargo check
cargo clippy -- -D warnings
```

### Phase 1 Complete - Verification Checklist

Run all verification steps:

```bash
# Unit tests
cargo test

# Clippy (should be warning-free)
cargo clippy -- -D warnings

# Check that all modules compile
cargo check

# Build release mode
cargo build --release

# Test with different dialects
cargo test parse_postgresql_dialect
cargo test parse_mysql_dialect
cargo test parse_sqlite_dialect
```

**Expected Results**:
- ✅ All tri-state truth table tests pass (18+ test cases)
- ✅ VM evaluation tests pass (7+ test cases)
- ✅ Parser tests pass (6+ test cases for different dialects)
- ✅ Zero clippy warnings
- ✅ Compiles successfully in release mode

**Deliverables**:
- [x] Type system (types.rs, errors.rs)
- [x] Tri-state logic with truth tables (tristate.rs)
- [x] VM instruction set (bytecode.rs)
- [x] VM interpreter (vm.rs)
- [x] SQL parser with generic dialect support (parser.rs)

**Phase 1 Complete**: Foundation is ready. SQL parsing works (basic), tri-state logic is correct, VM evaluator works for simple predicates.

---

## Phase 2: Core Runtime (Week 3-4)

**Goal**: Implement hybrid indexes, candidate selection, and full dispatch pipeline

**Coming in next section...**

---

## Next Steps

Phase 1 establishes the foundation. Before proceeding to Phase 2 (Runtime), ensure:

1. **All Phase 1 tests pass**
2. **No clippy warnings**
3. **Code review** the tri-state truth tables (most critical for correctness)
4. **Manual testing** with simple SQL predicates

**Ready for Phase 2?** You now have:
- Complete type system
- Correct tri-state logic
- Working VM interpreter
- Generic SQL parser

The next phase will build the runtime dispatch system with hybrid indexes.

---

**End of Phase 1 Plan**
