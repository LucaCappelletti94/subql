# CdcEvent trait refactor — handoff

**Branch**: `refactor/cdc-event-trait`
**Baseline commit**: `2a68599 Introduce Backend and CdcEvent trait system for typed CDC events`
**Status**: Phases 3-15 landed. Refactor closed; only optional `test_harnesses.rs` fuzz-harness restoration remains open.
**Current HEAD**: `6021e08 Fix memory_profile_workload deterministic-seed test to use TestEvent fields directly`.
**Verified bar**: `cargo +1.88 clippy --lib --all-features -- -D warnings` clean. `RUSTDOCFLAGS=-D warnings cargo +1.88 doc --lib --all-features --no-deps` clean. `cargo +1.88 fmt --check` clean. `cargo +1.88 test --lib --release --all-features` = 376 passing. `cargo +1.88 test --doc --release --all-features` = 30 passing. Non-Docker integration tests all pass. Docker-gated tests compile clean but are `#[ignore]`d. Known open item: `src/test_harnesses.rs` (fuzz harness driving 10 `fuzz/fuzz_targets/` binaries plus 2 criterion benches) still speaks `Cell`/`WalEvent`/`PrimaryKey`/`RowImage` and depends on the retired `PgOutputBridge`. The whole file is gated behind `#[cfg(any())]` in `src/lib.rs` and is intentionally deferred (rewiring `PgOutputBridge`, or replacing the harness with a `SqlitePatchsetParser`-based path, is a prerequisite for the `harness_sqlite_pgoutput_e2e` half).

## Why the refactor exists

subql today funnels every CDC source (wal2json v1/v2, pgoutput, Debezium, Maxwell, in-process `SqliteCdcSource`) through a mandatory intermediate — `WalEvent<C>` with `Cell` / `RowImage` / `PrimaryKey` — that every backend parser is required to convert into. That intermediate duplicates the type systems the source parsers already speak natively, forces a per-cell allocation and untyped scalar match on every hot path, and makes any downstream that wants typed values (like a `sqlite-diff-rs` patchset producer) convert back out of the intermediate. The right shape is a trait that any CDC event type implements directly; the engine becomes generic over it and reads typed scalars through associated types on the observed backend.

## Design contract (locked)

1. **`Backend` trait**: names one SQL database subql observes (Postgres, MySQL, SQLite). Carries the sqlparser `Dialect` as an associated type plus one associated type per SQL scalar (`Bool`, `Int`, `Float`, `String`, `Bytes`, `Uuid`, `Timestamp`, `TimestampTz`, `Date`, `Time`, `Decimal`, `Json`, `Jsonb`). Each backend picks its own Rust type per scalar — e.g. `Postgres::Bool = bool`, `SQLite::Bool = i64` (SQLite has no native BOOL), `Postgres::Uuid = uuid::Uuid`, `SQLite::Uuid = String`.
2. **`CdcEvent` trait**: describes one CDC row event. Carries `type Backend: Backend` and `type Checkpoint: Checkpoint`. Exposes event structure (`kind`, `table_id`, `pk_columns`, `changed_columns`, `checkpoint`) plus one typed scalar accessor per Backend scalar (`bool_at`, `int_at`, ...). Each accessor takes a `RowKind::{Old, New, Pk}` view selector and a `ColumnId` and returns `Presence<&<Self::Backend as Backend>::T>`.
3. **Row-per-event contract**: one CDC event is always about exactly one row identity. `RowKind::Pk` combined with a `col` not in `pk_columns()` returns `Presence::Missing`. Composite PKs are first-class — the caller iterates `pk_columns()` and calls the appropriate scalar accessor per column.
4. **`Presence<T>`**: three-valued enum (`Missing` / `Null` / `Present(T)`). Distinguishing `Missing` (source did not carry the cell) from `Null` (source carried SQL NULL) is required for predicate three-valued logic.
5. **`Value<B: Backend>`**: Backend-typed replacement for the retired `Cell`. Same shape as `Cell` — `Missing`, `Null`, plus one variant per scalar carrying `B::T`. Owns its payload so the VM stack does not need to track the event's lifetime. `LoadColumn` instructions clone the scalar out of the event when they push.
6. **`ScalarKind`**: 13-variant runtime tag naming which Backend scalar an operation is on. Decorates `LoadColumn(ColumnId, ScalarKind)` so the VM knows which typed accessor to call. Mirrors `Value<B>`'s scalar variants — no `Missing`/`Null` in `ScalarKind` because those are states not types.
7. **Generic-parameter discipline**:
   - Storage-shaped types (`Instruction<B>`, `BytecodeProgram<B>`, `Value<B>`, `Vm<B>`) take `B: Backend` only. A compiled bytecode program is Backend-scoped and reusable across any `E: CdcEvent<Backend = B>`.
   - Event-consuming types and methods take `E: CdcEvent`. `SubscriptionEngine<E: CdcEvent, ...>`. When a method needs both (VM eval), it spells the coupling constraint locally: `fn eval<E: CdcEvent<Backend = B>>(&mut self, prog: &BytecodeProgram<B>, event: &E, row: RowKind) -> Result<Tri, VmError>`.
   - Do not push `E` and `B` through the same signature as two independent parameters. Do not put `PhantomData<E>` on storage-shaped types.
8. **Scalar type crate picks**: `chrono` for the time family (`NaiveDateTime`, `DateTime<Utc>`, `NaiveDate`, `NaiveTime`), `bigdecimal::BigDecimal` for arbitrary-precision `Decimal`, `serde_json::Value` for both `Json` and `Jsonb`, `uuid::Uuid` for Postgres UUIDs. All are pinned no-default-features to keep subql's `no_std + alloc` posture.
9. **`Jsonb` distinct from `Json`** at the `Backend` level even though both use `serde_json::Value` as the Rust representation, because Diesel treats them as distinct `sql_types`.
10. **`Interval` deferred** — no MySQL/SQLite equivalent and no urgent use case.
11. **`Backend` lives in `subql::backend`** (not in `sql-traits`). Uphill to `sql-traits` deferred; revisit if a downstream that doesn't want subql wants `Backend`.
12. **Refactor style: one continuous branch, no compat shims.** `Cell` / `RowImage` / `PrimaryKey` / `WalEvent<C>` do not survive as parallel types. Every callsite migrates. No `impl CdcEvent for WalEvent<C>` bridge.

## What landed in the baseline commit

`src/backend.rs` — the whole trait system, standalone, compiles.

- `Presence<T>`, `RowKind`.
- `ScalarKind` (13 variants + serde derives + `Copy` + `Hash`).
- `Value<B: Backend>` (15 variants: `Missing`, `Null`, 13 scalars; `Clone + Debug + PartialEq + Serialize + Deserialize`).
- `ScalarCore` (blanket-implemented supertrait — `Clone + Debug + PartialEq + Serialize + Deserialize + Send + Sync + 'static`).
- `Backend` trait with scalar bounds (`ScalarCore` for all; `PartialOrd` on ordered scalars; arithmetic operator bounds on `Int`, `Float`, `Decimal`; `AsRef<str>` on `String`; `AsRef<[u8]>` on `Bytes`).
- `Postgres`, `MySql`, `SQLite` marker impls.
- `CdcEvent` trait: structural methods + one typed accessor per scalar.
- Cargo deps: `chrono`, `bigdecimal`, `uuid` (moved from dev-dep to regular).
- `src/lib.rs`: `pub mod backend;`.

## Progress so far

`cargo +1.88 test --lib --release` = 399 passing on `refactor/cdc-event-trait` at `02047f3`. Feature configs exercised in this session and known clean: default (`--lib`), `executor-diesel`, `executor-diesel-postgres`, `executor-diesel-mysql`, `executor-diesel-postgres-r2d2`, `diesel-typed`, `diesel-typed-sqlite`, `diesel-typed-mysql`, `pg-streaming`, `sqlite-cdc`, and the combined `std,pg-streaming,executor-diesel,executor-diesel-postgres,executor-diesel-mysql,executor-diesel-postgres-r2d2,diesel-typed,dhat-heap,sqlite-cdc,testing`. `--all-features` also clean.

~600 legacy tests are gated behind `#![cfg(any())]` at file level or gated `mod tests` blocks (7 in the runtime cluster) so the crate compiles cleanly while their `WalEvent::builder` / `Cell::` bodies wait for Phase 10 rewrite.

Currently gated (Phase 10 rewrite pending):

- `src/memory_profile_workload.rs` (gated in `src/lib.rs`; benchmark harness against typed `CdcEvent` fixture).
- `src/reexec/auto.rs`, `src/reexec/async_auto.rs`, `src/reexec/async_connector.rs` (file-level `#![cfg(any())]`). `async_connector.rs` was retargeted to the `Value<B>` shape in Phase 6 so it lines up when it un-gates; `auto.rs` / `async_auto.rs` bodies still speak `Cell` / `WalEvent` / `SubscriptionEngine<D: Dialect, ...>` from before Phase 5, and get a full rewrite when they un-gate.
- 7 `mod tests` blocks throughout the runtime cluster carrying `WalEvent::builder(...)` fixtures.
- Integration tests under `tests/`: `reexec_diesel.rs`, `reexec_postgres.rs`, `reexec_mysql.rs`, `reexec_postgres_r2d2.rs`, `follow_insert_*`, `follow_cdc_sqlite` all break at the module surface (they still spell `SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>` and construct `Cell::Float(5.0)`). They are `#[ignore]`d on the Docker path so they do not block `cargo test`, but they will not compile until Phase 10.

### Commit graph on `refactor/cdc-event-trait`

```
02047f3 Phase 6: expose DieselBackend and connector types; update reexec/mod.rs notes
729ed09 Phase 6: Connector trait produces Value<B>; retire Cell/ColumnType from reexec
aa61929 Phase 6 progress: diesel_api Backend-generic; gate memory_profile_workload
a642ac5 Phase 5 complete: workspace compiles clean; 399 tests pass
c0701e3 Phase 5 progress: Backend-generic reexec/plan + reexec/maintain
057da9f Phase 5 progress: SubscriptionEngine<E: CdcEvent, I, DB>; typed traits
c94b66f Phase 5 progress: Backend-generic dispatch.rs + agg.rs (E: CdcEvent, closure-driven kernels)
1e18a8d Phase 5 progress: Backend-generic predicate/indexes/partition runtime storage
2b8baf6 Phase 4: parser + prefilter emit Instruction<B>, target-typed literals
0812982 sql-traits upstream: JSONB and TIMESTAMPTZ canonical tokens; simplify column_scalar_kind
4faa5ba Phase 4 groundwork: SqlLiteralParse companion + column_scalar_kind helper
149c7bf Phase 3: Instruction<B>, BytecodeProgram<B>, Vm<B>, value_cmp.rs
2a68599 Introduce Backend and CdcEvent trait system for typed CDC events   <- baseline
```

`sql-traits` dependency pinned to `git+https://github.com/earth-metabolome-initiative/sql-traits?branch=main` at revision `fe18e67` (updated during Phase 4). `column_scalar_kind` upstream simplification landed in the same revision.

## Migration plan (status-annotated)

**Phase 3 — DONE (`149c7bf`)**: interpreter core. `src/compiler/bytecode.rs` carries `Instruction<B>` (variants `PushLiteral(Value<B>)`, `LoadColumn(ColumnId, ScalarKind)`, `In(Vec<Value<B>>)` — others unchanged) and `BytecodeProgram<B>`. `src/compiler/vm.rs` holds `Vm<B>` with `Vec<StackValue<B>>` where `StackValue<B> = Value(Value<B>) | Tri(Tri)`, and `Vm::eval<E: CdcEvent<Backend = B>>(&mut self, prog: &BytecodeProgram<B>, event: &E, row: RowKind)`. `src/compiler/cell_cmp.rs` was replaced by `src/compiler/value_cmp.rs` (same-scalar comparison only; no runtime Int/Float coercion). `Clone` / `Debug` / `PartialEq` on `Value<B>`, `Instruction<B>`, `BytecodeProgram<B>`, `StackValue<B>` are hand-implemented so their bounds fall on the payloads, not on `B` itself (deriving would defensively add `B: Clone` / `B: Debug` / `B: PartialEq` and prevent `Vm<B: Backend>` from being usable). 11 smoke tests + a private `TestEvent<B>` scaffold in `vm.rs` back the phase.

**Phase 4 — DONE (`4faa5ba`, `0812982`, `2b8baf6`)**: compiler stages. `SqlLiteralParse: Backend + Sized` (in `src/compiler/literals.rs`) is a companion trait to `Backend`, not an extension of it, so CDC-runtime paths that only read events do not inherit a sqlparser dependency. Implemented for `Postgres`, `MySql`, `SQLite` (13 scalars each). `catalog_helpers::column_scalar_kind` returns the finer-grained `ScalarKind` (replaces the coarse `ColumnType` in typed contexts). `src/compiler/parser.rs` became Backend-generic (`<B: Backend + SqlLiteralParse, DB: DatabaseLike>`); target-typed literal inference is in place; a private `wrap_bare_value_as_tri` helper preserves the "bool literal wrapped as `Tri`" surface. `src/compiler/prefilter.rs` swept to the same shape. `value_to_sql_value` handles the reverse direction for the diesel-typed binds.

**Phase 5 — DONE (`1e18a8d`, `c94b66f`, `057da9f`, `c0701e3`, `a642ac5`)**: runtime. `SubscriptionEngine<E: CdcEvent, I: IdTypes, DB: DatabaseLike>` — the `D: Dialect` slot was dropped (it collapses into `E::Backend::Dialect`). The runtime cluster (`src/runtime/{dispatch, agg, indexes, partition, predicate}.rs`) is rewritten: every `Cell` match is a typed accessor call, every `&WalEvent<C>` is `&E`, every `RowImage` disappeared (readers go through `E`'s scalar accessors). `Predicate<B>`, `PredicateStore<I, B>`, `TablePartition<I, B>` with `ColumnProbe` / `CellPresence`, `IndexableCell::from_value<B>` via `Any::downcast_ref`, `AggCellRead` / closure-driven `AggKernel`, `column_kinds: HashMap<TableId, Arc<[ScalarKind]>>` cache. `SubscriptionRequest<I, B: Backend = Postgres>` uses the default backend for source compatibility. Four registration / dispatch traits are parameterized: `SubscriptionRegistration<I, B>`, `SubscriptionDispatch<I, E>`, `AsyncSubscriptionDispatch<I, E>`, `AggregateDispatch<I, E>`. `src/reexec/{plan, maintain, engine}.rs` are also Phase 5 — `ReExecEngine<E: CdcEvent, I, DB>` where `E::Backend: SqlLiteralParse`. All 6 trait impls on `SubscriptionEngine` require `<E::Backend as Backend>::Dialect: Send + Sync`.

**Phase 6 — DONE (`aa61929`, `729ed09`, `02047f3`)**: reexec + auxiliary Connector cascade + diesel-typed API.
  - `src/diesel_api/mod.rs`: `BindDecode` and `FollowRowDecode` traits both carry `type SubqlBackend: crate::backend::Backend` (`Pg -> Postgres`, `Sqlite -> SQLite`, `Mysql -> MySql`). `render_typed` returns `(String, Vec<Value<D::SubqlBackend>>)`. `SubscriptionEngine::register_select_typed` / `register_follow_update_typed` / `register_follow_insert` take a diesel backend `D: BindDecode<SubqlBackend = E::Backend>` (respectively `FollowRowDecode` for the insert path) so the diesel query's rendered flavor matches the engine event's backend. Postgres emits `Value::Uuid(uuid::Uuid)` for `OID::UUID`; SQLite / MySQL emit `Value::String` (canonical hyphenated form). BLOB / bytea binds are rejected as unsupported.
  - `src/reexec/plan.rs`: `MinMaxPlan.column_type: ColumnType` retired; `agg_kind: ScalarKind` is now the sole decode hint field on the plan.
  - `src/reexec/engine.rs`: `Registered::ReExec { column_type: ColumnType }` becomes `Registered::ReExec { column_kind: ScalarKind }`.
  - `src/reexec/connector.rs`: `Connector` trait gains `type Backend: Backend`. `execute_scalar(&self, sql: &str, kind: ScalarKind, auth: &Self::AuthContext) -> Result<(Value<Self::Backend>, Option<Self::Checkpoint>), Self::Error>`. `execute_rows` returns `Snapshot<Vec<Vec<Value<Self::Backend>>>, Self::Checkpoint>`. New bridge trait `DieselBackend: Backend + Sized` in the same file has `value_from_i64` / `value_from_f64` / `value_from_string` and is impl'd for `Postgres`, `MySql`, `SQLite` (all three carry `Int=i64`, `Float=f64`, `String=String`). Generic `DieselConnector<C: Connection, B: DieselBackend>` gains a `PhantomData<fn() -> B>`; construction is `DieselConnector::<PgConnection, Postgres>::new(conn)` etc. `PgDieselConnector`, `MysqlDieselConnector`, `PgR2D2DieselConnector` hardcode `type Backend = Postgres` / `MySql` / `Postgres`. `load_scalar<C, B: DieselBackend>` routes `ScalarKind::{Int, Float}` through `IntRow`/`FloatRow`, and the ten non-numeric kinds through `TextRow` (decimals as text through this path so precision is not lost through `f64`).
  - `src/reexec/async_connector.rs` (gated): `AsyncConnector` retargeted to the same shape (`type Backend`, `ScalarKind` hint, `Value<Self::Backend>` return).
  - `src/reexec/mod.rs`: re-exports `Connector`, `Snapshot`, `ReExecError`, and the diesel connectors + `DieselBackend` + `PgR2D2Error` behind their feature gates. Phase note refreshed.
  - `src/persistence/*` and `src/polling/mod.rs` already compiled clean under Phase 5 and needed no further work in Phase 6; `src/memory_profile_workload.rs` is gated (Phase 10 rewrite).
  - `src/reexec/auto.rs` and `src/reexec/async_auto.rs` stay file-level gated. Their bodies still spell `Cell` / `WalEvent` / `SubscriptionEngine<D: Dialect, ...>` and would need a broad rewrite to consume Phase 5's `SubscriptionEngine<E: CdcEvent, ...>`. Doing that rewrite blind (without compile feedback under `#![cfg(any())]`) tends to leave subtle bugs — the pragmatic call is to un-gate as a discrete step in the Phase 6 continuation (see "Next up" below) and let the compiler drive.

### Legacy-type reference counts (baseline snapshot, unchanged since baseline)

The table below is the original scope estimate against the baseline `2a68599`. It has NOT been re-tallied since — Phases 3-6 rewrote or gated the top rows already, so per-file numbers no longer reflect current state. Kept for scope orientation only.

| File | `Cell::` | `WalEvent` | `RowImage` | `PrimaryKey` |
|---|---:|---:|---:|---:|
| `src/compiler/vm.rs` | 299 | | | |
| `src/runtime/engine.rs` | 107 | 37 | 61 | 6 |
| `src/wal/pg_type.rs` | 102 | | | |
| `src/runtime/indexes.rs` | 68 | | | |
| `src/runtime/agg.rs` | 56 | | 11 | |
| `src/reexec/maintain.rs` | 56 | 12 | 9 | |
| `src/compiler/prefilter.rs` | 51 | | 16 | |
| `src/runtime/dispatch.rs` | 46 | 39 | 25 | 1 |
| `src/compiler/parser.rs` | 44 | | | |
| `src/types.rs` | 41 | 57 | 31 | 4 |
| `src/diesel_api/mod.rs` | 41 | | | |
| `src/wal/pgoutput.rs` | 35 | | | |
| `src/wal/wal2json.rs` | | | 5 | 2 |
| `src/wal/mod.rs` | | 17 | 13 | 3 |
| `src/wal/streaming.rs` | | 7 | | |
| `src/wal/row_build.rs` | | | 6 | 1 |
| `src/sqlite_cdc/source.rs` | | 8 | | 2 |
| `src/sqlite_cdc/pgoutput_bridge.rs` | | 11 | | |
| `src/reexec/engine.rs` | | 11 | | |
| `src/reexec/auto.rs` | | 10 | 6 | |
| `src/reexec/async_auto.rs` | | 9 | | |
| `src/reexec/connector.rs` | | | 7 | |
| `src/row_set.rs` | | | 15 | |
| `src/test_harnesses.rs` | | 14 | | 6 |

Total original scope: ~40 files, ~35k LOC to touch, ~5-8k LOC net new. Phase-3-through-6 covered the runtime + compiler + reexec + diesel-typed + connector clusters; the wal parser cluster (Phase 7), `sqlite_cdc` (Phase 8), and the `types.rs` cleanup (Phase 9) remain.

## Migration plan (remaining phases)

**Phase 7 (DONE)**: parser `impl CdcEvent`. `src/wal/wal2json.rs`, `src/wal/pgoutput.rs`, `src/wal/debezium.rs`, `src/wal/maxwell.rs` now expose `Wal2JsonV{1,2}Event`, `PgOutputEvent`, `MaxwellEvent`, `DebeziumEvent` as public parser outputs that each `impl CdcEvent` directly. The `WalParser` and `CdcSource` traits have been reshaped with an associated `Event` type. `parse_wal_message` returns `Vec<Self::Event>`. `ack` takes `<Self::Event as CdcEvent>::Checkpoint`. Wire-to-typed decoding routes through a shared `pg_type` scalar-decoder layer with lazy `spin::Once<Value<B>>` per column per row image (pgoutput binary path). Changed-column tracking is derived from wire images per parser (Wal2Json identity/oldkeys covering arity, Debezium `before` covering arity, Maxwell authoritative `old` presence, PgOutput `key_type='O'`), returning `[]` for sparse-old cases as a safe over-notification. 134 legacy WalParser tests are gated behind `#[cfg(any())]` pending Phase 10 rewrite.

**Phase 8 (DONE)**: `SqliteCdc`. Deleted the shadow-log machinery entirely (`src/sqlite_cdc/{catalog,error,mod,pgoutput_bridge,source}.rs`, the `pg2sqlite` dep, and the old feature composition are gone) and rebuilt the module around SQLite session-extension bytes. Current shape has three pieces. `SqlitePatchsetEvent` implements `CdcEvent` with `Backend = SQLite` and `Checkpoint = NoCheckpoint`, eagerly decoding each `Value<SQLite>` from the wire. `SqlitePatchsetParser` is unconditional, implements `WalParser`, and wraps `sqlite_diff_rs::ParsedDiffSet::Patchset` (`sqlite-diff-rs` bumped to 0.1.1 for the UPDATE-patchset parser fix). `SqliteCdcSource` is feature-gated behind `sqlite-cdc`, owns a `SqliteConnection` plus a live `diesel_sqlite_session::Session`, calls `.patchset()` on poll, and recreates the session after each drain because the SQLite session extension accumulates rather than draining. `impl CdcSource for SqliteCdcSource` is wired thanks to the upstream `unsafe impl Send for Session` added in `diesel-sqlite-session`. `next_event` wraps the sync `poll_next_event` in `core::future::ready`. `ack` is a no-op on `NoCheckpoint`. The Cargo.toml gains a `[patch."https://github.com/diesel-rs/diesel"]` entry so `diesel-sqlite-session`'s git dep also routes through the LucaCappelletti94 diesel fork.

**Phase 9 (DONE)**: retire legacy types. Removed `Cell`, `RowImage`, `PrimaryKey`, `PrimaryKeyError`, `WalEvent<C>`, `WalEventBuildError`, `WalEventBuilderStart`, `InsertEventBuilder`, `UpdateEventBuilder`, `DeleteEventBuilder`, `TruncateEventBuilder`, `build_primary_key`, `apply_pk`, and `ColumnType` from `src/types.rs`. All live callsites migrated. `catalog_helpers::column_type` and `column_type_from_token` deleted, its test dropped. `compiler::sql_shape::resolve_numeric_agg_column` now checks `catalog_helpers::column_scalar_kind` and accepts `Int` / `Float` / `Decimal` under the finer-grained `ScalarKind` taxonomy. `compiler::prefilter` was pared down to just the atom planner (`PrefilterPlan { trigger_atoms, scan_required }`, `PlannerAtom`, `PlannerValue`, `analyze_*` helpers). The Cell-based runtime evaluator (`may_match`, `eval_may_true`, `eval_atom`, `eval_equality`, `planner_cells_equal`, `eval_range`, `TriPossibility`) had no live callers and got deleted. The `PrefilterPlan.expr: PrefilterExpr` and `PrefilterPlan.requires_prefilter_eval` fields went with it. `src/row_set.rs` was gated behind `#![cfg(any())]` because its diffing algorithm still speaks `RowImage` and needs a `Value<B>`-shaped rewrite before it can un-gate; consumers are future total-row re-execution / snapshot-refresh paths that are not wired in the current runtime, so gating over deleting preserves the algorithm for Phase 10. `SubscriptionRequest.binds` doc comment updated to point at `Value<B>` rather than `Cell`. 240 lib tests pass. `cargo +1.88 check --lib --all-features` is clean. Gated code that still spells the retired names (`src/reexec/{auto,async_auto,async_connector}.rs`, `src/memory_profile_workload.rs`, `src/test_harnesses.rs`, `src/row_set.rs`, various `#[cfg(any())]` `mod tests` blocks, README + engine.rs doctests) is Phase 10.

**Phase 10 (NEXT ENTRY POINT)**: tests + doctests + fixture harnesses. `src/test_harnesses.rs` un-gates and switches from `WalEvent::builder(...)` to a test-only concrete `CdcEvent` (probably `TestEvent<B>`, an evolution of the private `TestEvent<B>` fixture already sitting in `src/compiler/vm.rs`). Every `mod tests` block gated behind `#[cfg(any())]` in the runtime + reexec + wal + compiler clusters un-gates and rewrites its event constructors. Every doctest in `runtime/engine.rs`, `sqlite_cdc/source.rs`, `wal/*.rs`, `catalog_helpers.rs`, `types.rs` migrates, along with the four code blocks in `README.md` (rendered via `#![doc = include_str!("../README.md")]`). The gated `src/reexec/auto.rs` and `src/reexec/async_auto.rs` bodies un-gate. `src/reexec/async_connector.rs` un-gates (already at the correct shape). `src/row_set.rs` un-gates and rewrites its diff algorithm against `Value<B>`. `src/memory_profile_workload.rs` un-gates (probably feature-gated behind `dhat-heap` since it is a benchmark harness). The Docker integration tests (`tests/reexec_*.rs`, `tests/follow_*.rs`, `tests/proptest_row_set_delta.rs`) also migrate here. Success bar: `cargo +1.88 test --lib --release` and `cargo +1.88 test --doc` both pass, every integration test either compiles or is intentionally `#[ignore]`d with a documented reason.


**Phase 11**: verification. `cargo +1.88 fmt --check`, `cargo +stable clippy --workspace --all-targets --all-features --locked -- -D warnings` (matches subql's CI), `cargo +1.88 test --lib --release`, `cargo +1.88 test --all-features --release`, `RUSTDOCFLAGS=-D warnings cargo +1.88 doc --all-features --no-deps`. Clippy and rustdoc gates have not been run since baseline — first pass under `-D warnings` is likely to surface a small backlog of missing docs on Phase-3-through-6 additions.

## Phase 7 entry-point guide

Start Phase 7 by reading these files in order:

1. `src/backend.rs` (`CdcEvent` trait) — the surface each parser impl must satisfy.
2. `src/wal/wal2json.rs` lines 1-330 — the simplest of the four parsers; a good pilot. `Wal2JsonV2Message` already carries `pub columns: Option<Vec<Wal2JsonV2Column>>` where each `Wal2JsonV2Column` is `{name: String, type_name: String, value: serde_json::Value}`. The scalar accessors decode `value` per `type_name` on demand — no eager conversion needed.
3. `src/wal/pg_type.rs` — the pg-type decoding helpers wal2json and pgoutput already use; the typed scalar accessors on `impl CdcEvent for Wal2JsonV2Message` should route through the SAME per-type decoders so the wire → typed contract is single-sourced.
4. `src/wal/pgoutput.rs` — the binary wire path (Postgres logical replication). Bigger and more state-heavy than wal2json but has strong existing type routing.
5. `src/wal/debezium.rs` and `src/wal/maxwell.rs` — the smaller two, JSON-envelope-shaped like wal2json v1/v2.
6. `src/wal/mod.rs` — the `WalParser` trait everyone implements. Its `parse_wal_message` return type moves from `Result<Vec<WalEvent<Self::Checkpoint>>, WalParseError>` to a Backend-tagged event iterator.

A pragmatic Phase 7 cut order:

1. Start with `Wal2JsonV2Message` — smallest surface, per-message shape (one row per message, no batch iteration).
2. Once wal2json v2 compiles + a smoke test dispatches through the engine, do `Wal2JsonV1Message` (batched — the impl needs to expose a "row-iter view" over the message's `change: Vec<Wal2JsonV1Change>`; each `Change` is one event).
3. `MaxwellMessage` next — MySQL-native, one row per message.
4. `DebeziumEnvelope` — one row per message.
5. `PgOutputMessage` last — binary wire, largest surface.
6. Then update `WalParser` and its consumers in `src/wal/{mod, streaming, pg_streaming}.rs`.

### Design gotchas specific to Phase 7

- **The `type Checkpoint` associated type stays.** Each parser already knows its checkpoint flavor (`PgLsn` for pgoutput / wal2json-v2-with-`include-lsn`, `NoCheckpoint` for wal2json v1 and Maxwell / Debezium in most configs). Keep it as-is.
- **Composite PKs are first-class.** Every impl's `pk_columns()` returns the whole PK column set; the scalar accessors read PK cells through `RowKind::Pk` when the caller asks for them. Do not shortcut a "single-column PK" specialization.
- **`RowKind::Pk` with a non-PK column returns `Presence::Missing`.** Repeated across every impl. If the parser only stores the PK columns' names, checking membership before decoding is the natural implementation.
- **`Presence::Missing` vs `Presence::Null` distinction lives in the wire.** wal2json v2 marks omitted columns by their absence from the `columns` / `identity` array; pgoutput uses a `TupleData::Null` marker (SQL NULL) vs `TupleData::UnchangedToast` (missing). Preserve the distinction; the engine's three-valued logic depends on it.
- **Scalar accessors that observe a value NOT of the expected scalar return `Presence::Missing`, not a panic.** If the caller asks for `bool_at(RowKind::New, col)` but the underlying value is `serde_json::Number(...)`, the accessor treats that as "the requested scalar shape is not carried here" and returns `Missing`. This mirrors the wal-side's own "column type mismatch → skip / re-execute" semantics.
- **Decode on demand, do not eagerly materialize.** wal2json stores JSON on the wire; each accessor call decodes the one JSON value it needs, does not build the full `Vec<Value<B>>` up front. The engine reads only the cells its predicates and aggregates touch, so eager materialization is pure overhead.

### After Phase 7, when un-gating `auto.rs` / `async_auto.rs`

These modules were left in Phase-5-old shape because a blind rewrite under `#![cfg(any())]` without compile feedback is dangerous. The reshape they need:

- Struct-level type parameters go from `<D: Dialect, I: IdTypes, DB: DatabaseLike, X: Connector>` to `<E: CdcEvent, I: IdTypes, DB: DatabaseLike, X: Connector<Backend = E::Backend>>` (plus `E::Backend: SqlLiteralParse` where the inner engine requires it).
- `AutoResolvingEngine::consumers(&mut self, event: &WalEvent<C>)` becomes `AutoResolvingEngine::consumers(&mut self, event: &E)`; the `C: Checkpoint` slot is now `E::Checkpoint`.
- `Registered::ReExec { sql, column_type }` destructuring changes to `Registered::ReExec { sql, column_kind }`; each `execute_scalar(&ctx.sql, ctx.column_type, &ctx.auth)` call becomes `execute_scalar(&ctx.sql, ctx.column_kind, &ctx.auth)`; the returned `Cell` becomes `Value<E::Backend>` (installed via `self.inner.install(query_id, value)` on `ReExecEngine<E, ..>`).
- `SnapshotResult::Scalar(Cell, Option<C>)` becomes `SnapshotResult::Scalar(Value<E::Backend>, Option<E::Checkpoint>)`; the `Rows` variant follows the same pattern.
- `impl SubscriptionDispatch<I> for AutoResolvingEngine<D, I, DB, X>` changes to `impl<E, I, DB, X> SubscriptionDispatch<I, E> for AutoResolvingEngine<E, I, DB, X>` (matches how `ReExecEngine` was rewritten in Phase 5).
- The `mod tests` block at the bottom is Phase-10 test rewrite territory; drop it into `#[cfg(any())]` on un-gate and pick it up with the rest of the tests.

`src/reexec/async_connector.rs` is already at the correct shape (Phase 6 retargeted it), so un-gating it is a one-line change.

## Design gotchas the next session will hit

1. **`E: CdcEvent<Backend = B>` at method boundaries only.** Do not double-parameterize `SubscriptionEngine` on both `E` and `B`. Read `<E::Backend as Backend>::Foo` inside methods when a scalar type is needed.
2. **`Value::Missing` vs `Value::Null` are semantically distinct** — Missing = source didn't carry the cell; Null = source carried SQL NULL. Do not collapse them.
3. **`RowKind::Pk` with non-PK columns returns `Presence::Missing`.** Not `Presence::Null`. Document this at every impl.
4. **Same-scalar comparison only in the VM's MVP.** Cross-scalar (Int vs Float) comparison returns `Tri::Unknown`. When a query needs numeric coercion, the compiler emits an explicit `Cast(FromScalar, ToScalar)` instruction (add this to the bytecode when needed). Do not put runtime coercion into `value_cmp.rs`.
5. **Bytecode `Serialize`/`Deserialize` requires every scalar to be `Serialize`/`Deserialize`.** That's why `ScalarCore` bounds them; do not weaken. The subscription-persistence path in `src/persistence/` and `reexec/` serializes bytecode.
6. **`AsRef<str>` on `Backend::String` is load-bearing for `LIKE`.** Similarly `AsRef<[u8]>` on `Bytes`.
7. **`Bool` has no ordering bound in `Backend`.** SQL bool ordered comparison is not defined. If a query somehow reaches `<`/`>` on a bool, `value_cmp` returns `Tri::Unknown`.
8. **`Json`/`Jsonb` have no ordering bound.** JSON equality is defined (structural); JSON ordering is undefined. Same handling as bool.
9. **`serde_json::Value` for `Postgres::Jsonb` is the Rust-side representation.** Postgres carries JSONB as binary on the wire; the wire decoder in `wal/pgoutput.rs` and `wal/pg_type.rs` produces a `serde_json::Value` from that binary. The `Backend::Jsonb` associated type is the decoded shape, not the wire shape.
10. **The `sqlparser::dialect::Dialect` bound is `Sized`.** Marker types like `PostgreSqlDialect` are unit structs. The engine holds one instance (or constructs one on demand) — check `SubscriptionEngine`'s current handling of `D: Dialect` when rewriting.

## Suggested resume workflow for the next session

1. `cd ~/github/subql && git switch refactor/cdc-event-trait && cargo +1.88 check`. Confirm the baseline compiles.
2. Read `src/backend.rs` in full. This is the design contract in code form; anything not clear should surface here before writing any migration code.
3. Read this document in full.
4. Re-read `docs/refactor-cdc-event-handoff.md` this file will show up at.
5. `cargo +1.88 test --lib --release` on the baseline to confirm the current test suite still passes (987 tests as of the baseline commit).
6. Start Phase 3. Delegate mechanical vm.rs body rewrites to a subagent with the trait design as pinned context.

## What NOT to do without user sign-off

- Do not add a `WalEvent -> CdcEvent` compat impl "just for the transition". The whole point of the refactor is retiring the intermediate.
- Do not push both `E` and `B` as parallel type parameters through `SubscriptionEngine` or its methods. See gotcha 1.
- Do not add runtime Int/Float coercion to `value_cmp.rs`. See gotcha 4.
- Do not touch the `patchset` outbound bridge (that path was reverted earlier in the previous session; it is out of scope until this refactor lands).
- Do not commit half-migrated file clusters that leave the tree broken across many turns without at least a clear working-state marker in the commit message.

## Session context reference

The previous session's design conversation lived in the connetto-rs project chat (from which subql is being refactored as an upstream dependency). Key resolved decisions in that chat:

- Six sign-off questions (`Row` folded, `Presence<T>`, `RowKind::Pk` handling, scalar set + crate picks, `Backend` in subql, one continuous branch) — all locked as documented in the "Design contract" section above.
- The `Backend` associated types live on the SQL database marker, not on the payload type. Concrete markers: `Postgres`, `MySql`, `SQLite`.
- One `CdcEvent` trait implemented directly on parser output types (`Wal2JsonV2Message` etc.), no subql-owned canonical intermediate.
- Storage types use `B: Backend`; event-consuming types use `E: CdcEvent`. Do not merge the two.

There is no external design doc beyond this file. The trait's rustdoc is the authoritative reference for signatures and semantics; this handoff is the authoritative reference for the migration plan.
