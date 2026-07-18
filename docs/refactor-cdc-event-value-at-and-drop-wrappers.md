# Refactor: collapse `CdcEvent` to a single `value_at` method and drop per-parser wrappers

**Status**: draft (internal refactor plan)
**Scope**: subql `src/backend.rs` (`CdcEvent` trait), every `impl CdcEvent for ...`, `src/compiler/vm.rs`, `src/runtime/dispatch.rs`, `src/runtime/engine.rs`, `src/reexec/maintain.rs`, every parser output type under `src/wal/` and `src/sqlite_cdc/`.
**Motivation**: kill the `typed-accessor -> ScalarKind runtime dispatch -> Value<B>` trampoline, delete every per-parser wrapper struct, let `impl CdcEvent for pg_walstream::LogicalReplicationMessage` (and its `wal2json` / `maxwell` peers) work directly.

---

## Executive summary

`CdcEvent` today exposes 13 typed scalar accessors (`bool_at`, `int_at`, `uuid_at`, ...) each returning `Presence<&<B as Backend>::T>`. Every downstream consumer reconstitutes a `Value<B>` from those references via a 15-arm runtime `match kind { ScalarKind::Bool => event.bool_at(...), ... }` dispatch. The runtime `ScalarKind` argument comes from a per-table cache the engine maintains just to feed that match. The compile-time bytecode already knows the type of every column it touches. The runtime `ScalarKind` is a redundant tag.

The whole trampoline exists because the trait couldn't hand back a `Value<B>` directly. That was a design mistake in the original CdcEvent refactor. Fixing it is a one-method addition to the trait and a mechanical deletion of the surrounding scaffolding.

After the refactor:

- `CdcEvent` has one accessor: `value_at(&self, row: RowKind, col: ColumnId) -> Value<Self::Backend>`.
- Every downstream call site becomes `event.value_at(row, col)` and matches on the returned `Value<B>` variant.
- The 13 typed accessors are deleted from the trait and from every impl.
- `ScalarKind` stays in the bytecode compiler (compile-time type checking of predicates against the catalog) but is removed from the runtime data flow.
- `load_column` and its twins in `dispatch.rs` and `reexec/maintain.rs` disappear.
- `SubscriptionEngine::column_kinds` cache and `ensure_column_kinds_cached` disappear.
- Every per-parser event wrapper struct (`PgOutputEvent`, `Wal2JsonV1Event`, `Wal2JsonV2Event`, `MaxwellEvent`, `DebeziumEvent` if still around, `SqliteChangesetEvent`, `TestEvent<B>`) gets replaced by direct `impl CdcEvent for <raw upstream event type>`.
- **`impl CdcEvent<Backend = Postgres> for pg_walstream::LogicalReplicationMessage`** is the end state on the pgoutput path. No wrapper, no wire-text copies, no re-parsing.

---

## What is wrong today

### Trait surface

`src/backend.rs::CdcEvent` currently declares:

```rust
pub trait CdcEvent {
    type Backend: Backend;
    type Checkpoint: ...;

    fn kind(&self) -> EventKind;
    fn table_id(&self) -> TableId;
    fn checkpoint(&self) -> Option<&Self::Checkpoint>;
    fn pk_columns(&self) -> &[ColumnId];
    fn changed_columns(&self) -> &[ColumnId];

    // 13 typed accessors, one per scalar:
    fn bool_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Bool>;
    fn int_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Int>;
    fn float_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Float>;
    fn string_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::String>;
    fn bytes_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Bytes>;
    fn uuid_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Uuid>;
    fn timestamp_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Timestamp>;
    fn timestamp_tz_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::TimestampTz>;
    fn date_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Date>;
    fn time_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Time>;
    fn decimal_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Decimal>;
    fn json_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Json>;
    fn jsonb_at(&self, row: RowKind, col: ColumnId) -> Presence<&<Self::Backend as Backend>::Jsonb>;
}
```

The reference-returning accessors force each impl to own a decode cache. That is why every parser has a wrapper struct (`PgOutputEvent`, `Wal2JsonV{1,2}Event`, `MaxwellEvent`, ...) instead of implementing `CdcEvent` on the raw upstream event type. Those wrappers copy wire bytes out of upstream `Bytes` into owned `String` to survive independently of the source buffer, and they duplicate the field derivation (`table_id`, `pk_columns`, `changed_columns`) that could otherwise live once in the trait defaults or once in a shared decoding path.

### Runtime dispatch

Every consumer glues the 13 accessors back into a `Value<B>` via the same 15-arm match:

`src/compiler/vm.rs` (VM's LoadColumn):

```rust
fn load_column<E>(event: &E, row: RowKind, col: ColumnId, kind: ScalarKind) -> Value<E::Backend>
where E: CdcEvent,
{
    match kind {
        ScalarKind::Bool => lift(event.bool_at(row, col), Value::Bool),
        ScalarKind::Int => lift(event.int_at(row, col), Value::Int),
        // ... 11 more arms ...
    }
}
```

`src/runtime/dispatch.rs` has the same shape. `src/reexec/maintain.rs` has the same shape. `src/runtime/engine.rs::extract_pk_value` has the same shape (13-arm match on `ScalarKind`).

`src/reexec/maintain.rs:122` has a particularly egregious form asking "is this cell missing across every possible type interpretation?":

```rust
event.bool_at(row, col).is_missing()
    && event.int_at(row, col).is_missing()
    && event.float_at(row, col).is_missing()
    && event.string_at(row, col).is_missing()
    // ... 9 more accessor calls in the chain
```

Each of these callers already knows the type family it wants. The compile-time bytecode encoded it. The trampoline exists only because the trait couldn't hand back the right shape directly.

### `ScalarKind` cache

`SubscriptionEngine` maintains `column_kinds: HashMap<TableId, Arc<[ScalarKind]>>` and calls `ensure_column_kinds_cached(table_id)` on every register and every restore. Its sole purpose is to feed the runtime match above with a `ScalarKind` argument. Nothing else consumes the cache. Once the runtime match dies, the cache dies with it.

### Wrapper duplication

Every parser output type has a wrapper struct that owns decoded row images:

- `PgOutputEvent { kind, table_id, pk_columns, changed_columns, checkpoint, new_image, old_image }` where each image copies wire text out of `pg_walstream::ColumnData::Text(Bytes)` into `alloc::string::String`.
- `Wal2JsonV1Event`, `Wal2JsonV2Event`, `MaxwellEvent`, `DebeziumEvent` (if still shipped), `SqliteChangesetEvent`, `TestEvent<B>` all follow the same pattern.

The wrappers exist because the reference-returning accessors need somewhere to store decoded state. If the accessor returns owned `Value<B>` instead, no cache is needed on the event and the wrapper stops being necessary.

---

## Root cause

The typed accessors were the wrong abstraction. The trait design should have followed downstream demand: every consumer wants a `Value<B>` per (row, col). Instead the trait handed out 13 typed views that consumers had to reconstitute back into `Value<B>` at every call site, at the cost of owned decode caches on every event impl.

---

## Proposed design

Replace the 13 accessors with one:

```rust
pub trait CdcEvent {
    type Backend: Backend;
    type Checkpoint: ...;

    fn kind(&self) -> EventKind;
    fn table_id(&self) -> TableId;
    fn checkpoint(&self) -> Option<&Self::Checkpoint>;
    fn pk_columns(&self) -> &[ColumnId];
    fn changed_columns(&self) -> &[ColumnId];

    /// Decode one cell. Every impl owns the decoding logic for its own
    /// wire format (pgoutput OID lookup, wal2json type_name lookup,
    /// maxwell columns_types lookup, etc.). Returns owned `Value<B>`
    /// because the impl decodes fresh on demand and there is no cache to
    /// hand a reference into.
    fn value_at(&self, row: RowKind, col: ColumnId) -> Value<Self::Backend>;
}
```

Semantics:

- `Value::Missing` for cells the wire did not carry (pgoutput's `'u'` unchanged-TOAST, wal2json omitted columns, maxwell missing keys).
- `Value::Null` for cells the wire carried as SQL NULL.
- `Value::<Variant>(v)` for present cells, where the variant matches the column's declared type per the wire format's own type source.
- Wrong-shape access (asking for a column that the impl decodes as one variant but the caller expected another) returns whatever variant the impl decoded. The caller's own downstream match handles the mismatch. There is no "shape mismatch returns Missing" fallback anymore because there is no per-scalar accessor to have a shape at all.

Return by value, not by reference. Every impl decodes fresh per call. Repeated access to the same cell within one opcode is the caller's problem (the bytecode compiler is welcome to hoist).

`Value<B>` already carries `Missing` and `Null` variants, so `Presence` collapses into the same enum. `Presence` goes away.

---

## Migration plan

Nine phases. Each phase compiles and passes tests independently.

### Phase 1: add `value_at` alongside the typed accessors

Grow the trait with the new method. Provide a default implementation that folds the 13 typed accessors back into `Value<B>` (the same `load_column` logic, but living on the trait as a default). Every existing impl inherits the default and works unchanged.

Deprecate the typed accessors via `#[deprecated]` on the trait so the diagnostic points at every caller.

### Phase 2: migrate consumer call sites to `value_at`

Walk every call site the previous scan found:

- `src/compiler/vm.rs::load_column` -> deleted. Callers use `event.value_at(...)` directly.
- `src/runtime/dispatch.rs` -> same.
- `src/runtime/engine.rs::extract_pk_value` -> same.
- `src/reexec/maintain.rs::load_column` twin -> same.
- `src/reexec/maintain.rs:122` chain -> `event.value_at(row, col).is_missing()`.

Tests migrate too: `event.int_at(row, col) == Presence::Present(&7)` becomes `event.value_at(row, col) == Value::Int(7)`.

### Phase 3: delete the typed accessors from the trait

Once no consumer calls them, delete the 13 accessor methods from `CdcEvent`. Every existing impl still has them defined at this point but they are unreachable.

### Phase 4: delete `ScalarKind` from the runtime data flow

- `SubscriptionEngine::column_kinds` field removed.
- `ensure_column_kinds_cached` deleted.
- Every place that took `ScalarKind` as a parameter for a match-then-accessor pattern loses the parameter.

`ScalarKind` may still exist as a compile-time helper for the bytecode compiler (predicate type checking against the catalog before emitting opcodes), but it stops flowing through the runtime.

### Phase 5: delete typed accessor impls from every event wrapper

`PgOutputEvent`, `Wal2JsonV{1,2}Event`, `MaxwellEvent`, `SqliteChangesetEvent`, `TestEvent<B>` drop their 13-accessor impls. Each keeps only `value_at`, `kind`, `table_id`, `pk_columns`, `changed_columns`, `checkpoint`.

At this point every wrapper struct still exists and still holds a decode cache, but the cache is no longer required by the trait. Move to Phase 6 to remove the cache and thin the wrappers.

### Phase 6: thin the wrappers to remove the decode cache

Each wrapper's `value_at` implementation switches from "look up in cache, decode if empty" to "decode fresh." This removes `Once<Value<Postgres>>` cells and the `PgOutRowImage::cache` (and its peers) from every wrapper.

Wire storage becomes zero-copy where possible. `PgOutRowImage::PgOutWireCell::PgOutWirePayload::Text(String)` changes to `Text(bytes::Bytes)` (or holds an `Arc<[u8]>` slice of the source buffer). Wire-text `String` copies disappear.

### Phase 7: delete the wrappers where the raw upstream type suffices

The final step. Each wrapper had two reasons to exist: decode cache (gone after Phase 6) and derived-field storage (`table_id`, `pk_columns`, `changed_columns`, `checkpoint`).

Derived fields can be computed on demand from the raw upstream event plus a schema handle. Two options for how the impl gets the schema:

- **Option A**: `CdcEvent::value_at` grows a `&Schema` parameter. Same for `table_id`, `pk_columns`, `changed_columns`. Big trait signature change.
- **Option B**: define a trait `CdcEventFor<Sch>` that captures `impl for (RawEventType, &Sch)`. Then `SubscriptionEngine::consumers(&event)` becomes `consumers(&(event, &self.database))`. Smaller signature change on the trait, more work at the call site.
- **Option C**: keep a *minimal* per-format adapter struct that holds `(raw_event, resolved_derived_fields)`. Shrinks the wrapper to just derived fields, no decode cache, no wire copies. Simplest to land.

Recommend Option C for the first landing. The wrapper shrinks to ~30 lines each and the `CdcEvent` trait signature stays clean. Option A or B lands in a follow-up if the residual wrapper size still bothers us.

Under Option C, `PgOutputEvent` becomes:

```rust
pub struct PgOutputEvent<'msg> {
    inner: &'msg pg_walstream::LogicalReplicationMessage,
    table_id: TableId,
    pk_columns: Arc<[ColumnId]>,
    changed_columns: Arc<[ColumnId]>,
    checkpoint: Option<PgLsn>,
}
```

Zero-copy against `pg_walstream`'s parsed message. All field derivation done once at construct time. `value_at` decodes fresh on demand using `pg_walstream::ColumnInfo::type_id` from the referenced relation cache.

Under Options A or B, the wrapper disappears entirely and `impl CdcEvent for pg_walstream::LogicalReplicationMessage` is the target.

### Phase 8: same collapse across wal2json and maxwell

Repeat Phases 5, 6, 7 for `Wal2JsonV{1,2}Event` and `MaxwellEvent`. `SqliteChangesetEvent` and `TestEvent<B>` also collapse but they are trivial cases (they own their storage explicitly for tests / SQLite).

### Phase 9: verification

Green bar: `cargo +1.88 fmt --check`, `cargo +1.88 clippy --lib --all-features -- -D warnings`, `cargo +1.88 test --lib --release --all-features`, `cargo +1.88 test --doc --release --all-features`, `RUSTDOCFLAGS="-D warnings" cargo +1.88 doc --lib --all-features --no-deps`, `cargo +1.88 check --all-targets --all-features`. Docker E2E tests (`apply_patchset_pg_e2e`, `apply_patchset_pg_uuid_e2e`, `apply_patchset_mysql_e2e`, `apply_patchset_sqlite_e2e`) all green.

Round-trip differential check: PG -> `pg_walstream` -> `impl CdcEvent for LogicalReplicationMessage` -> subql engine -> patchset via `sqlite_diff_rs::DiffSetBuilder::digest` -> apply against a mirror PG via `PgAdapter`. Both directions same values.

---

## What gets deleted

Rough LoC estimate.

| Item | Location | Approx LoC |
|---|---|---|
| Typed accessors on `CdcEvent` trait | `src/backend.rs` | ~40 |
| Typed accessor impls per event type | 7 event types * 13 methods each | ~450 |
| `load_column` function | `src/compiler/vm.rs` | ~15 |
| `load_column` twin | `src/runtime/dispatch.rs` | ~15 |
| `load_column` twin | `src/reexec/maintain.rs` | ~15 |
| `extract_pk_value` match | `src/runtime/engine.rs` | ~30 |
| `is_missing()` chain | `src/reexec/maintain.rs:122` | ~11 |
| `SubscriptionEngine::column_kinds` field + `ensure_column_kinds_cached` | `src/runtime/engine.rs` | ~50 |
| `Presence` enum (folds into `Value`) | `src/backend.rs` | ~40 |
| `PgOutRowImage::cache` and `Once<Value<Postgres>>` cells | `src/wal/pgoutput.rs` | ~30 |
| Wire-text `String` copies | `src/wal/pgoutput.rs` and peers | ~40 |
| Per-parser wrappers (thinned to Option C shape) | `src/wal/*.rs`, `src/sqlite_cdc/event.rs` | ~200 net delete (going from ~300 to ~100 per wrapper) |
| **Total** | | **~950 lines deleted** |

## What stays

- `CdcEvent` trait (with one accessor plus the small metadata methods).
- `ScalarKind` (compile-time only, for bytecode type checking against the catalog).
- Per-format decoding logic (each impl owns its own).
- `Value<B>` (already exists, gains a natural fold with `Presence`).

---

## Downstream implications

- `sqlite_diff_rs 0.2.0`'s `Digestable` trait is `Value<S, B>`-shaped. Once `CdcEvent::value_at` returns `Value<B>`, connecting the two is a one-hop projection: subql hands the event to `DiffSetBuilder::digest(&event, &schema, &adapter)` and sqlite-diff-rs's `TypeMap` decodes to its own `Value<S, B>`. No re-parse.
- The `PgAdapter` we shipped is unchanged. It sits on the apply side, decodes SQLite session bytes into PG binds. Its type dispatch already reads the catalog directly. Nothing here touches it.
- `pg_sqlite_emu` uses `sqlite_diff_rs::pg_walstream_reverse` in isolation. Nothing here touches it.
- `connetto-rs`'s subscription materializer gets the natural interoperation between subql's event stream and sqlite-diff-rs's patchset construction it wanted from day one.

---

## Testing strategy

Every phase runs the same green bar. The load-bearing tests are the parser typed-scenario tests we restored during Gap 2 of the earlier refactor (`typed_pgoutput_*`, `typed_v{1,2}_*`, `typed_maxwell_*`, `typed_debezium_*`, `pg_type::tests::*`). Every assertion of the form `event.int_at(row, col) == Presence::Present(&7)` becomes `event.value_at(row, col) == Value::Int(7)`. Mechanical rewrite, no semantic change.

Round-trip differential test lands in Phase 9 as the acceptance gate. PG (Docker) generates real pgoutput events, parser produces `LogicalReplicationMessage`, `CdcEvent::value_at` returns `Value<Postgres>`, sqlite-diff-rs digests into a patchset, `PgAdapter` applies back to a mirror PG. Final row identity byte-identical to the source.

---

## Open questions

1. **`Presence` fold into `Value`**: `Value` already has `Missing` and `Null` variants. Removing `Presence` and having `value_at` return `Value<B>` directly is the natural collapse. Any callers of `Presence::is_missing()` become `matches!(value, Value::Missing)`. Cost: one enum instead of two. Worth doing in Phase 3.
2. **Option A vs B vs C in Phase 7**: recommend C first for smallest blast radius. A or B lands as follow-up if we want the wrapper to disappear entirely.
3. **`TestEvent<B>` fate**: it exists for test-harness synthesis. Under the new trait it becomes a small `struct TestEvent<B> { kind, table_id, pk_columns, changed_columns, checkpoint, rows: Vec<Value<B>> }` with a trivial `value_at`. Simpler than today. Keep.
4. **`SqliteChangesetEvent` fate**: its raw upstream is `sqlite_diff_rs::ChangesetOp` which does not carry a schema handle. Either grows a schema reference (Option A/B pattern) or keeps a small Option C wrapper. Recommend Option C.
5. **Migration to `pg_walstream::LogicalReplicationMessage` directly (Option A/B in Phase 7)**: worth pursuing as a follow-up once the C-shape lands and we can profile whether the wrapper matters. If the wrapper reduces to `~30 lines of derived-field storage`, deleting it is symbolic more than practical.

---

## Acceptance

- `CdcEvent` trait has one accessor, `value_at`, returning `Value<Self::Backend>`.
- No runtime code path takes `ScalarKind` as a dispatch key.
- No wrapper event type stores a decode cache.
- Every parser output type is either a raw upstream event (`pg_walstream::LogicalReplicationMessage`, etc.) or a `~30 lines` thin wrapper holding derived fields plus a borrow of the raw event.
- All existing tests, both unit and Docker E2E, green.
- Round-trip differential test PG -> pg_walstream -> subql -> sqlite-diff-rs -> `PgAdapter` -> PG green with byte-identical row identity.
- Net -900 lines of code (rough estimate). Not a scope guarantee, but the direction.
