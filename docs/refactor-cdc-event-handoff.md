# CdcEvent trait refactor — handoff

**Branch**: `refactor/cdc-event-trait`
**Baseline commit**: `2a68599 Introduce Backend and CdcEvent trait system for typed CDC events`
**Status**: design layer landed and compiling; downstream migration not started.

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

## What is not yet done

The whole downstream. Nothing in the crate uses the new trait system — every file still speaks `Cell` / `RowImage` / `PrimaryKey` / `WalEvent<C>`.

### File-by-file scope estimate

Count of legacy-type references per file (baseline):

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
| `src/wal/debezium.rs` | | | | |
| `src/wal/maxwell.rs` | | | | |
| `src/wal/mod.rs` | | 17 | 13 | 3 |
| `src/wal/streaming.rs` | | 7 | | |
| `src/wal/row_build.rs` | | | 6 | 1 |
| `src/wal/pg_streaming.rs` | | | | |
| `src/sqlite_cdc/source.rs` | | 8 | | 2 |
| `src/sqlite_cdc/pgoutput_bridge.rs` | | 11 | | |
| `src/sqlite_cdc/mod.rs` | | | | |
| `src/reexec/engine.rs` | | 11 | | |
| `src/reexec/auto.rs` | | 10 | 6 | |
| `src/reexec/async_auto.rs` | | 9 | | |
| `src/reexec/connector.rs` | | | 7 | |
| `src/row_set.rs` | | | 15 | |
| `src/test_harnesses.rs` | | 14 | | 6 |
| `src/memory_profile_workload.rs` | | | | |
| `src/polling/mod.rs` | | | | |
| `src/checkpoint.rs` | | | | |
| `src/errors.rs` | | | | |

Plus doctests scattered across `runtime/engine.rs` (~50+ event constructors), `sqlite_cdc/source.rs`, `wal/*.rs`, `catalog_helpers.rs`, `types.rs`.

Total: ~40 files, ~35k LOC to touch, ~5-8k LOC net new.

## Migration plan (in order)

**Phase 3: interpreter core rewrite**. Rewrite `src/compiler/bytecode.rs`, `src/compiler/cell_cmp.rs` → `value_cmp.rs`, `src/compiler/vm.rs` to be Backend-generic. `Instruction<B>` variants: `PushLiteral(Value<B>)`, `LoadColumn(ColumnId, ScalarKind)` (gains `ScalarKind`), `In(Vec<Value<B>>)`; other variants unchanged. `BytecodeProgram<B>`. `Vm<B>` holding `Vec<StackValue<B>>` where `StackValue<B>` is `Value(Value<B>) | Tri(Tri)`. `Vm::eval<E: CdcEvent<Backend = B>>(&mut self, prog: &BytecodeProgram<B>, event: &E, row: RowKind)`. Arithmetic helpers stay same-type-only for MVP (no runtime Int/Float coercion); if a future need surfaces, add explicit `Cast` instructions in the compiler.

**Phase 4: compiler stages**. Rewrite `src/compiler/parser.rs`, `prefilter.rs`, `canonicalize.rs`, `sql_shape.rs`, `literals.rs` to emit `Instruction<B>` and consume `Value<B>` literals. The compiler needs to know the Backend at compile time — `parse_and_compile::<B: Backend>(...)` etc. SQL AST → typed literal now goes through backend-aware routing (a `Value::Bool(true)` literal for a `WHERE bool_col = true` query on Postgres, for example).

**Phase 5: runtime**. `SubscriptionEngine<E: CdcEvent, I: IdTypes, DB: DatabaseLike>` (drop the `D: Dialect` slot — it collapses into `E::Backend::Dialect`). Rewrite `src/runtime/{dispatch, agg, indexes, partition, predicate}.rs` — every `Cell` match becomes a typed accessor call, every `&WalEvent<C>` becomes `&E`, every `RowImage` disappears (readers go through `E`'s scalar accessors).

**Phase 6: reexec + auxiliary**. `src/reexec/*.rs`, `src/persistence/`, `src/polling/`, `src/diesel_api/`, `src/memory_profile_workload.rs` follow the same rewrite.

**Phase 7: parser impls**. `impl CdcEvent for Wal2JsonV2Message`, `impl CdcEvent for PgOutputMessage`, `impl CdcEvent for DebeziumEnvelope`, `impl CdcEvent for MaxwellMessage`, `impl CdcEvent for Wal2JsonV1Message` (row-iter view). Each impl declares `type Backend = Postgres` (or `MySql`) and provides the typed scalar accessors, reading from the parser's already-typed fields (wal2json JSON values, pgoutput binary wire types, Debezium/Maxwell JSON envelopes).

**Phase 8: SqliteCdc**. `src/sqlite_cdc/source.rs` currently produces `WalEvent<C>`. Under the new shape it needs a native event type (probably `struct SqliteCdcEvent { table_id, kind, pk_columns, changed_columns, row_new, row_old }` where `row_*` are `Vec<Value<SQLite>>` decoded from the shadow log) with `impl CdcEvent for SqliteCdcEvent { type Backend = SQLite; }`. Or: drop the SqliteCdc translation entirely and drive an `impl CdcEvent for sqlite_diff_rs::PatchsetOp<'_, ...>` under a `patchset` feature (the previously-attempted outbound path, but coming from the source side rather than the sink side).

**Phase 9: retire legacy types**. Remove `Cell`, `RowImage`, `PrimaryKey`, `WalEvent<C>`, `WalEventBuilderStart`, `WalEventBuildError` from `src/types.rs`. Fix every remaining reference.

**Phase 10: tests + doctests + fixture harnesses**. `src/test_harnesses.rs` currently builds `WalEvent` via a fluent builder. Every doctest in `runtime/engine.rs`, `sqlite_cdc/source.rs`, and elsewhere constructs `WalEvent::builder(...)`. All migrate to constructing typed CDC events (probably via a test-only concrete impl of `CdcEvent`, e.g. `TestEvent<B: Backend>`).

**Phase 11: verification**. `cargo +1.88 fmt --check`, `cargo +stable clippy --workspace --all-targets --all-features --locked -- -D warnings` (matches subql's CI), `cargo +1.88 test --lib --release`, `cargo +1.88 test --all-features --release`, `RUSTDOCFLAGS=-D warnings cargo +1.88 doc --all-features --no-deps`.

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
