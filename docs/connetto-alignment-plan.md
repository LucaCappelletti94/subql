# Connetto alignment plan

Status: design accepted, not yet implemented.

This document captures the cross-walk between subql and connetto-rs and the multi-phase plan that follows from it. The driving principle is "one general solution belongs in subql": responsibilities that every connetto-style consumer would otherwise reimplement absorb into subql, with carefully scoped trait boundaries for the parts that genuinely depend on a specific transport, auth source, or delivery format.

## What is moving

After comparing connetto's architecture chapters against subql's current public surface, the items below are migrating into subql. Each is something every consumer would otherwise duplicate.

1. **Checkpoint as a first-class concept.** Subql currently has no notion of "position in the stream." Connetto carries PG LSNs externally. We make Checkpoint a marker trait, attach it as a generic to `WalEvent`, `ConsumerNotifications`, `ScalarUpdate`, and parsers, and ship concrete impls (`PgLsn`, `MysqlBinlogPos`, `OpaqueCheckpoint`).
2. **Cross-event trigger coalescing.** A WAL batch that displaces the same captured query's extreme N times calls the Connector N times today. After this plan it calls it once, inside a new `consumers_batch` API.
3. **Initial snapshot computation.** The Connector grows `execute_rows`. The auto-resolving engine exposes a `snapshot(query_id)` method that bootstraps a captured query against the database.
4. **A `Connector::Checkpoint` associated type.** Snapshots come back checkpoint-tagged so subql + downstream replay layers can chain WAL events onto the snapshot's anchor.
5. **An `AsyncConnector` plus an `AsyncAutoResolvingEngine`.** The sync API stays for tests, doctests, and sync consumers. The async API is the production surface for async-driver consumers (sqlx, diesel-async, etc.).
6. **A subscription registry with parameterized caps and an `EvictionPolicy`.** Connetto would otherwise count externally; subql owns the data, subql owns the bound.
7. **A `Clock` trait plus rate limiting.** Per-query debounce and a max-concurrent-reexecutions cap. The trait is minimal (single `now()` method) and has a `StdClock` default plus a `ManualClock` for tests.
8. **A `row_set_delta` utility.** Pure function. Inserted / deleted / updated triples from two row sets keyed by PK. Useful for total reexec and for any external snapshot consumer.
9. **A `R2D2DieselConnector` behind `executor-diesel-r2d2`.** Pool-backed sync connector. Send + Sync. Enables concurrent re-execution dispatch and snapshots that hold one connection across pages.
10. **A `pg-cdc` feature with a caller-driven, runtime-agnostic WAL reader.** Long-running reader exposing `next_batch` + `advance(checkpoint)` plus slot management. No tokio dep imposed on subql.

## What stays out

These responsibilities are connetto-specific or otherwise outside subql's competence:

- WebSocket transport, framing, flow control, keepalive, per-session sink, wire message types.
- sqlite-diff-rs patchset bytes (a specific delivery format).
- OpenFGA client and per-row auth round-trips. Subql carries an `AuthContext` and passes it to the Connector; the Connector applies whatever auth policy it implements.
- Mutation write path (validation, conflict detection, conflict resolution policy, rollback).
- Client-side local SQLite store, reconnect loop, optimistic apply.
- File sync (chunker, content channel, hash addressing, browser storage adapters).
- Schema distribution to clients, schema-push protocol.
- OPFS, IndexedDB, Cache API on WASM.
- Oplog table + tombstones + LSN retention window as PG-side artifacts. Subql gives consumers Checkpoint primitives; the oplog table is connetto's design.

## Locked-in design decisions

These were settled during planning and the rationale lives in the conversation log.

- `Checkpoint` is a trait, not a typed enum. Engine pins one `C` per construction. Concrete impls: `PgLsn`, `MysqlBinlogPos`, `OpaqueCheckpoint(Bytes)`. Type aliases keep common-case signatures readable.
- pg-cdc lives as a feature inside subql, not a separate crate, not in connetto.
- The pg-cdc reader is caller-driven and runtime-agnostic. Subql does not pull tokio.
- `consumers_batch` is additive on top of the existing per-event `consumers`. Per-event semantics stay unchanged.
- Connector trait extension (`execute_rows`) is a required method, no default `Unsupported`.
- Sync and async are first-class peers. Neither is "the testing surface."
- `AuthContext` is single per subscription, used by both snapshot and re-execution.
- Async trait uses async-fn-in-trait with `+ Send` bounds.
- Concurrent re-execution in batches uses `FuturesUnordered`, capped at `max_concurrent_reexecutions`.

## Phase 1: Checkpoint trait and propagation

Foundational. Every type that carries an event position picks up a `C: Checkpoint` generic.

Changes:

- New module `src/checkpoint.rs`:
  - `pub trait Checkpoint: Ord + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static {}`
  - Concrete impls: `pub struct PgLsn(pub u64)`, `pub struct MysqlBinlogPos { pub file: u32, pub pos: u32 }`, `pub struct OpaqueCheckpoint(pub Vec<u8>)`. Each derives the needed traits. Marker trait, no required methods.
- `WalEvent` grows `<C: Checkpoint>` and gains `checkpoint: Option<C>`. Option because not every parser source provides one.
- `ConsumerNotifications` grows `<I, C>`. Carries `checkpoint: Option<C>`.
- `ScalarUpdate` grows `<I, C>`. Carries the checkpoint of the event that triggered the re-execution.
- `WalParser` trait grows `type Checkpoint: Checkpoint`. `parse_wal_message` returns `Vec<WalEvent<Self::Checkpoint>>`.
- `Wal2JsonV2Parser` pins `type Checkpoint = PgLsn` and parses the JSON `lsn` field.
- `MaxwellParser` pins `type Checkpoint = MysqlBinlogPos`.
- Other parsers (`PgOutputParser`, `DebeziumParser`, `Wal2JsonV1Parser`) get parallel impls.
- `SubscriptionEngine` grows `<C: Checkpoint>` as its fifth generic. `ReExecEngine` and `AutoResolvingEngine` parallel.
- Type aliases for common cases: `pub type PgSubscriptionEngine<D, I, DB> = SubscriptionEngine<D, I, DB, PgLsn>;` plus the same shape for `PgReExecEngine` and `PgAutoResolvingEngine`.

Files (representative):

- `src/checkpoint.rs` (new)
- `src/types.rs` (`WalEvent`, `ConsumerNotifications`)
- `src/runtime/engine.rs` (`SubscriptionEngine`)
- `src/wal/mod.rs` (`WalParser` trait)
- `src/wal/wal2json.rs`, `src/wal/maxwell.rs`, `src/wal/pgoutput.rs`, `src/wal/debezium.rs` (one impl each)
- `src/reexec/engine.rs`, `src/reexec/auto.rs`
- `src/lib.rs` (re-exports + type aliases)

Testable claims:

- T1.1: `Wal2JsonV2Parser` extracts `Some(PgLsn(_))` from a wal2json v2 message with a present `lsn` field and returns `None` when missing.
- T1.2: After a `consumers()` dispatch on an event with `checkpoint = Some(PgLsn(42))`, every emitted `ConsumerNotifications` and `ScalarUpdate` carries that checkpoint.
- T1.3: `PgLsn(5) < PgLsn(6)`. `MysqlBinlogPos { file: 1, pos: 10 } < MysqlBinlogPos { file: 1, pos: 20 } < MysqlBinlogPos { file: 2, pos: 1 }`.
- T1.4: Existing 900-plus lib tests still pass after type-parameter additions.

Checkpoint commands:

- `cargo test --lib`
- `cargo test --features executor-diesel`
- `cargo test --features executor-diesel --test reexec_postgres -- --ignored`

Risks:

- Wide breaking change to public types. One-time pain.
- Test fixtures all need updating to name `PgLsn` or a stub Checkpoint. Plan for roughly 100 small edits across the test suite.

## Phase 2: Connector trait extension (sync)

Make `execute_rows` a required method. Add `Checkpoint` associated type to Connector. Snapshots are checkpoint-tagged.

Changes:

- `pub trait Connector { type AuthContext; type Error; type Checkpoint: Checkpoint; fn execute_scalar(...) -> Result<(Cell, Option<Self::Checkpoint>), _>; fn execute_rows(&self, sql, schema, &auth) -> Result<Snapshot<Vec<RowImage>, Self::Checkpoint>, _>; }`
- `pub struct Snapshot<T, C: Checkpoint> { pub value: T, pub checkpoint: Option<C> }`.
- `DieselConnector<C: Connection>` extended:
  - Decodes one row for `execute_scalar`. Decodes a row set for `execute_rows`.
  - For PG backend, optionally calls `pg_current_wal_lsn()` inside the same transaction as the scalar or row read. Returns `Snapshot { checkpoint: Some(PgLsn(...)), ... }`. For SQLite and MySQL, returns `checkpoint: None`.
  - The `type Checkpoint` associated type is `PgLsn` for PG, `MysqlBinlogPos` for MySQL, `OpaqueCheckpoint` for SQLite.
- `AutoResolvingEngine::snapshot(query_id) -> Result<SnapshotResult, ReExecError<X::Error>>`:
  - `pub enum SnapshotResult<C: Checkpoint> { Scalar(Cell, Option<C>), Rows(Vec<RowImage>, Option<C>) }`.

Files:

- `src/reexec/connector.rs` (trait + Snapshot + DieselConnector impl)
- `src/reexec/auto.rs` (snapshot method)
- `tests/reexec_diesel.rs` (snapshot smoke test against in-memory SQLite)
- `tests/reexec_postgres.rs` (snapshot + LSN test against real PG)

Testable claims:

- T2.1: `DieselConnector::execute_rows` on PG returns a `Snapshot` with `checkpoint = Some(PgLsn(_))` matching a separate `pg_current_wal_lsn()` query taken right after.
- T2.2: `AutoResolvingEngine::snapshot(query_id)` for a captured MIN query returns `SnapshotResult::Scalar(Cell::Float(_), Some(PgLsn(_)))` and installs the value (subsequent dispatch sees the installed value).
- T2.3: `execute_rows` against an empty table returns `Snapshot { value: vec![], checkpoint: Some(_) }`.

Checkpoint commands:

- `cargo test --features executor-diesel`
- `cargo test --features executor-diesel --test reexec_postgres -- --ignored`

Risks:

- Pulling `pg_current_wal_lsn()` requires a transaction that contains both the read and the LSN query. Plan to wrap with `BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ` for snapshot consistency.
- For backends with no checkpoint concept (in-memory SQLite, which we use for tests), `Checkpoint = OpaqueCheckpoint` with always-empty bytes is awkward. Consider a marker `NoCheckpoint` impl or accept that `Option<C>` is always `None` for such backends. Decide during implementation.

## Phase 3: AsyncConnector and AsyncAutoResolvingEngine

Parallel async trait pair. Locks in the async surface so future async impls do not require a breaking bump.

Changes:

- `pub trait AsyncConnector { type AuthContext; type Error; type Checkpoint: Checkpoint; async fn execute_scalar(...) -> Result<(Cell, Option<Self::Checkpoint>), _>; async fn execute_rows(...) -> Result<Snapshot<...>, _>; }`. Every method's future is `Send` by trait declaration so it works on multi-threaded runtimes.
- `pub struct AsyncAutoResolvingEngine<D, I, DB, C, X: AsyncConnector>` parallel to the sync engine. Same methods (`register`, `install`, `snapshot`, `consumers`, `consumers_batch` once Phase 4 lands, `unregister_*`), all async where the connector is involved.
- No async impl shipped in this plan. The trait surface and engine type are documented as "async-ready, no concrete impl yet, sqlx or diesel-async ships separately."

Files:

- `src/reexec/async_connector.rs` (new)
- `src/reexec/async_auto.rs` (new)
- `src/reexec/mod.rs` (re-exports)

Testable claims:

- T3.1: A test-only `MockAsyncConnector` (in unit tests, not shipped) implements the trait and exercises register / install / snapshot / consumers against the async engine. Behavior matches the sync engine for the same scenario.
- T3.2: Compile-only check: the async engine type compiles with the `MockAsyncConnector` bound and the public API surface stays object-safe where needed.

Checkpoint commands:

- `cargo test --lib`
- `cargo doc --no-deps --features executor-diesel`

Risks:

- async-fn-in-trait plus `+ Send` bounds. Generally fine on 1.75 and up, but error messages can be intimidating. Document common patterns in the trait's rustdoc.
- The "no concrete async impl yet" state is fragile, easy to ship an API nobody exercises. Mitigation: the in-tree `MockAsyncConnector` test covers every public method.

## Phase 4: consumers_batch + BatchOutcome + trigger coalescing

Additive batch API on both sync and async engines.

Changes:

- `pub struct BatchOutcome<I: IdTypes, C: Checkpoint> { pub per_event: Vec<ConsumerNotifications<I, C>>, pub scalar_updates: Vec<ScalarUpdate<I, C>>, pub triggers: Vec<ReExecutionTrigger<I, C>> }`. Last field is always empty under the auto-resolving variant.
- `ReExecEngine::consumers_batch(&[WalEvent<C>]) -> Result<BatchOutcome<I, C>, DispatchError>`. Per-event engine notifications fan out in input order. Triggers deduplicated by `query_id`, keeping the last occurrence's checkpoint.
- `AutoResolvingEngine::consumers_batch(&[WalEvent<C>]) -> Result<BatchOutcome<I, C>, ReExecError<X::Error>>`. After per-event dispatch, deduped triggers feed the Connector serially (sync engine).
- `AsyncAutoResolvingEngine::consumers_batch(...) -> ...`. Deduped triggers feed the Connector via `FuturesUnordered`, capped at `max_concurrent_reexecutions` (default 8, configurable, see Phase 6). Completed futures install + push `ScalarUpdate`. First connector error aborts. Partial state is dropped.

Files:

- `src/reexec/engine.rs`
- `src/reexec/auto.rs`
- `src/reexec/async_auto.rs`

Testable claims:

- T4.1: Given a 5-event batch where 3 events displace the same captured query's extreme, `consumers_batch` calls the Connector exactly once for that query.
- T4.2: Per-event engine notifications fan out in input order across the batch (positional alignment).
- T4.3: A connector error mid-batch returns `Err(ReExecError::Connector)`. The partial per-event notifications are dropped. Caller retries the whole batch.
- T4.4 (async): With `max_concurrent_reexecutions(2)` and 8 unique triggers, no more than 2 futures are in flight at once.

Checkpoint commands:

- `cargo test --lib reexec`
- Integration tests
- Async unit tests

Risks:

- "Coalesced trigger keeps the last occurrence's checkpoint." Correct per the model (the most recent state), but worth documenting so users do not assume the first.
- Concurrent re-exec under async raises Send-ness questions for the Connector and the engine state. Pool-backed Connectors are Send + Sync. Single-conn Connectors do not need concurrency anyway. Document the bound.

## Phase 5: Subscription registry caps + EvictionPolicy

Parameterize subscription limit + eviction. Already documented as subql's responsibility in subql.md. Implementing the doc.

Changes:

- `pub enum EvictionPolicy { Reject, EvictOldest, EvictLeastActive }`.
- `SubscriptionEngine::with_max_subscriptions(self, cap: usize, policy: EvictionPolicy) -> Self`.
- `RegisterError::RegistryFull` for the Reject policy.
- Evicted IDs surfaced: `RegisterResult` and `Registered::ReExec` gain an `evicted: Vec<I::ConsumerId>` field. Most often empty. Populated when eviction freed space.
- `EvictionPolicy::EvictLeastActive` requires per-subscription last-dispatch timestamp tracking. Uses the Clock trait introduced in Phase 6, so phase order matters slightly. Can be merged after Phase 6 or both at once.

Files:

- `src/runtime/engine.rs`
- `src/types.rs`
- `src/errors.rs`

Testable claims:

- T5.1: With `with_max_subscriptions(2, Reject)` and 3 register calls, the third returns `RegistryFull`.
- T5.2: With `with_max_subscriptions(2, EvictOldest)` and 3 register calls, the third succeeds and returns the evicted ID list containing the first subscription.
- T5.3: With `EvictLeastActive`, the subscription with the oldest last-dispatch time is evicted.

Checkpoint commands:

- `cargo test --lib runtime::engine`

Risks:

- `EvictLeastActive` adds bookkeeping on the dispatch hot path. Bench impact should be measured (criterion harness already exists).
- Evicted subscriptions should get a clean-up signal so the caller can notify clients. Decide whether to emit something in `BatchOutcome` or rely on the returned vector.

## Phase 6: Clock trait and rate limiting

Introduce time. Per-query debounce + global concurrent-reexecution cap.

Changes:

- `pub trait Clock { type Instant: Ord + Copy + Add<Duration, Output = Self::Instant> + Sub<Self::Instant, Output = Duration>; fn now(&self) -> Self::Instant; }`. Minimal surface.
- `pub struct StdClock` behind the `std` feature. Impls `Clock` using `std::time::Instant`.
- `pub struct ManualClock(Arc<Mutex<Instant>>)` for tests. Deterministic ticking.
- `AutoResolvingEngine::with_clock(self, clock: impl Clock)`, `with_debounce_per_query(Duration)`, `with_max_concurrent_reexecutions(usize)`.
- Debounce semantics: if the time since this query's last re-execution is less than the debounce window, skip the trigger (return the cached installed value, do not call the Connector). The next trigger after the window proceeds normally.
- Concurrent cap: only meaningful on the async engine. Sync engine is naturally serialized. The async engine respects the cap inside `consumers_batch`.

Files:

- `src/clock.rs` (new)
- `src/reexec/auto.rs`
- `src/reexec/async_auto.rs`

Testable claims:

- T6.1: Using `ManualClock`, an engine with `with_debounce_per_query(Duration::from_millis(100))` skips the second trigger when the manual clock advances only 50ms between two displacing events.
- T6.2: After the manual clock advances 150ms, the next trigger fires.
- T6.3: Async engine with `with_max_concurrent_reexecutions(3)` and 10 unique triggers: at no observed moment is the in-flight count above 3.

Checkpoint commands:

- `cargo test --lib reexec`

Risks:

- Clock trait design: keep it minimal. Resist adding methods we do not need (no `sleep`, no `tick`).
- Clock impls that go backward (wall-clock NTP skew). Document that monotonic time is required.

## Phase 7: row_set_delta utility

Pure function. Cheap. No dependencies.

Changes:

- `pub fn row_set_delta(prev: &[RowImage], next: &[RowImage], pk_cols: &[ColumnId]) -> RowSetDelta`.
- `pub struct RowSetDelta { pub inserted: Vec<RowImage>, pub deleted: Vec<RowImage>, pub updated: Vec<(RowImage, RowImage)> }`.
- Match on PK tuple. Inputs assumed deduped by PK (the caller's snapshot results normally are).

Files:

- `src/row_set.rs` (new)
- `src/lib.rs` (re-export)

Testable claims:

- T7.1: prev = `[{id:1, p:5}, {id:2, p:9}]`, next = `[{id:2, p:9}, {id:3, p:11}]`, pk = `[id]` produces inserted = `[{id:3, p:11}]`, deleted = `[{id:1, p:5}]`, updated = `[]`.
- T7.2: prev = `[{id:1, p:5}]`, next = `[{id:1, p:7}]` produces inserted = `[]`, deleted = `[]`, updated = `[(old, new)]`.
- T7.3: empty inputs return all empty.

Checkpoint commands:

- `cargo test --lib row_set`

Risks: none significant.

## Phase 8: R2D2DieselConnector

Pool-backed sync Connector. Unlocks concurrent re-execution dispatch and snapshots that hold their connection across pages.

Changes:

- New feature `executor-diesel-r2d2 = ["executor-diesel", "dep:r2d2", "diesel/r2d2"]`.
- `pub struct R2D2DieselConnector<M: ManageConnection> { pool: r2d2::Pool<M> }`. `impl Connector` for it. Send + Sync.
- `execute_scalar` calls `pool.get()`, runs the query, releases.
- `execute_rows` opens a `BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ` transaction, reads `pg_current_wal_lsn()` plus the rows, commits.

Files:

- `src/reexec/connector.rs`
- `Cargo.toml`
- `tests/reexec_postgres.rs` (extended to use the pool variant in one test)

Testable claims:

- T8.1: With a 4-connection pool and 4 concurrent re-executions from `AsyncAutoResolvingEngine::consumers_batch`, all 4 succeed in parallel (measured via a counted mock on the pool wrapper).
- T8.2: Snapshot via `R2D2DieselConnector` holds a single connection across the read. The snapshot rows + LSN are read in one transaction.

Checkpoint commands:

- `cargo test --test reexec_postgres --features executor-diesel-r2d2 -- --ignored`

Risks:

- r2d2 pulls a transitive dep. Acceptable behind feature flag.
- Snapshot transaction isolation level must be REPEATABLE READ (or stricter) for paged reads to be consistent. Document.

## Phase 9: pg-cdc feature

Long-running WAL reader. Runtime-agnostic. Caller-driven.

Changes:

- New feature `pg-cdc = ["executor-diesel", "pg_walstream/rustls-tls"]` or similar, reusing the same `pg_walstream` native backend the `pg-streaming` feature already pulls. The runtime stays caller-driven via `tokio`'s minimal feature set.
- `pub struct PgCdcReader { ... }` with methods:
  - `pub async fn next_batch(&mut self) -> Result<Vec<WalEvent<PgLsn>>, ReaderError>`.
  - `pub async fn advance(&mut self, ckpt: PgLsn) -> Result<(), ReaderError>`. Advances the replication slot's confirmed-flushed pointer.
  - `pub fn into_stream(self) -> impl Stream<Item = Result<Vec<WalEvent<PgLsn>>, ReaderError>>` for futures-Stream callers.
- Slot management: `pub async fn ensure_slot(&mut self, name: &str) -> Result<(), ReaderError>`, `drop_slot(name)`.
- Internal reconnect: on slot drop or connection loss, the reader reconnects + recreates the slot (with options) or surfaces an error if the slot must be re-created from an unknown LSN.
- The reader does not spawn anything. Caller decides where to await it.

Files:

- `src/wal/pg_cdc/mod.rs` (new module)
- `src/wal/pg_cdc/reader.rs`
- `src/wal/pg_cdc/slot.rs`
- `Cargo.toml`
- `tests/pg_cdc_integration.rs` (new integration test against testcontainers PG)

Testable claims:

- T9.1: `PgCdcReader::next_batch` returns the wal2json messages for an INSERT + DELETE applied between calls, parsed as `Vec<WalEvent<PgLsn>>` with monotonically increasing checkpoints.
- T9.2: After `advance(ckpt)`, `pg_replication_slots.confirmed_flush_lsn >= ckpt.0`.
- T9.3: Killing the PG connection between two `next_batch` calls: the reader transparently reconnects and resumes from the last `advance`d checkpoint.
- T9.4: Compile only. Subql still compiles with `--no-default-features` and `--target wasm32-unknown-unknown` when `pg-cdc` is off.

Checkpoint commands:

- `cargo test --features pg-cdc --test pg_cdc_integration -- --ignored`

Risks:

- Native WAL streaming via `START_REPLICATION` requires the libpq `replication=database` connection mode. `pg_logical_slot_get_changes` polling is the safe-and-simple alternative. The reader's first cut should poll. Streaming is a follow-on.
- Reconnect semantics. Slot persistence across reconnects is at the PG side, but the libpq session is not. Document the assumption that the slot is durable.

## Phase ordering and parallelism

Sequential critical path:

```
1. Checkpoint trait + propagation
2. Connector::execute_rows (sync)
3. AsyncConnector + AsyncAutoResolvingEngine
4. consumers_batch + coalescing
```

After phase 4 lands, phases 5 through 8 are largely independent of each other:

```
5. Registry caps + EvictionPolicy
6. Clock trait + rate limiting
7. row_set_delta utility
8. R2D2DieselConnector
```

Phase 9 (pg-cdc) sits on top of phase 1 (it produces `WalEvent<PgLsn>`) and benefits from but does not strictly require phases 2 through 8.

Each phase is independently mergeable. Suggested PR shape: one PR per phase, sequenced. Phases 5 through 8 can land in any order or in parallel.

## Cross-cutting verification

- `cargo test --lib` green at every phase.
- `cargo clippy --all-targets --all-features` zero warnings.
- `cargo fmt --check` clean.
- `cargo check --no-default-features` green at every phase. The default no_std + alloc surface stays clean.
- `cargo check --no-default-features --target wasm32-unknown-unknown` green at every phase.
- `cargo tree --no-default-features -e=normal` has no diesel, no r2d2, no tokio (only the always-on `pg_walstream` parser bits stay).
- The existing testcontainers tests (`tests/cdc_cross_db.rs`, `tests/reexec_postgres.rs`) keep passing under their existing `cargo test ... -- --ignored` invocations.

## Out of scope, riding follow-ons

- Total single-table row re-execution.
- Per-group MIN/MAX, partial COUNT(DISTINCT), tie tracking on MIN/MAX.
- AsyncDieselConnector or executor-sqlx. No concrete async impl in this plan. AsyncConnector trait surface is in scope, the only async impl ships under a separate plan once we decide between sqlx and diesel-async.
- pg2sqlite-backed lightweight doctest harness.

## Pitfalls and decisions deferred to implementation

- Checkpoint for SQLite in tests. SQLite has no native WAL position usable as a checkpoint. Either ship `NoCheckpoint` (a unit-like marker impl) or accept that `DieselConnector<SqliteConnection>` sets `type Checkpoint = OpaqueCheckpoint` and always returns `None` for the optional. Decide during phase 2.
- pg-cdc underlying driver. `diesel-postgres` is sync, the reader needs async for `next_batch` to be a future. `pg_walstream`'s `rustls-tls` backend is already on subql's `pg-streaming` feature; the same dep powers the reader path here.
- Async-fn-in-trait Send bounds. Use `+ Send` on the trait's `async fn` returns. If a downstream user needs `!Send`, they implement their own trait variant. Document this in the trait's rustdoc.
- Clock trait minimalism. Resist scope creep. Only `now()` is in scope for v1.
- `EvictLeastActive` activity tracking. A per-subscription `last_dispatch_at: Option<Instant>` field is the simplest path. Avoid more elaborate decay models in v1.
- Trigger coalescing keeps the last checkpoint. Document this explicitly in the BatchOutcome rustdoc.
- Snapshot transaction isolation. Use `BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ` for snapshot in DieselConnector and R2D2DieselConnector. Document.
