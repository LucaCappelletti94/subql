# PG-over-SQLite emulator: coverage restoration plan

**Context:** Commit `2baf519` (Phase 8.1) deleted `PgOutputBridge`, `pg2sqlite`, the shadow-log `SqliteCdcSource`, and every test that relied on any of the four. The `pg_sqlite_emu` module has replaced the machinery, but the tests that depended on it are still missing. This document is the checklist.

Do not delete this file until every row in the table below reports **RESTORED** or **UPSTREAMED**.

## What was deleted (map to new API)

Test files come out of commit `2baf519~1`, `pg2sqlite`'s API from its git branch, and the emulator API from `src/pg_sqlite_emu/`.

| Deleted file / doctest | LOC | Old API | New API | Status | Priority |
|---|---:|---|---|---|---:|
| `src/sqlite_cdc/mod.rs` (mod-level doctest) | 1 block | `SqliteCdcSource::with_pg_ddl` + `SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>` + `source.execute` + `source.poll_next_event` | `PgSqliteEmuSource::open_in_memory` + `SubscriptionEngine<PgOutputEvent, DefaultIds, ParserDB>` + `source.execute` + `source.poll_next_event` | RESTORED (mod-level doctest in `src/pg_sqlite_emu/mod.rs`) | 1 |
| `src/sqlite_cdc/source.rs` (struct + `::new` doctests) | 1 block | `SqliteCdcSource::with_pg_ddl(conn, pg_ddl, SqliteCdcConfig::default())` + assertions on `Cell::Int(1)` pk | `PgSqliteEmuSource::new(conn, pg_ddl)` + assertions on `Presence::Present(&1)` typed accessors | RESTORED (struct + method doctests in `src/pg_sqlite_emu/source.rs`) | 1 |
| `src/sqlite_cdc/pgoutput_bridge.rs` (`PgOutputBridge::new` doctest + mod-level bridge doctest) | 2 blocks | `PgOutputBridge::encode_event(&wal_event, &catalog)` -> `Vec<BytesMut>` -> `PgOutputParser::parse_wal_message` -> `engine.consumers(&parsed)` | Same pipeline, but the encoder now lives at `sqlite_diff_rs::pg_walstream_reverse::op_to_message` and the caller drives it directly from a `ChangesetOp`. The full end-to-end pipeline is what `PgSqliteEmuSource` already hides, so this doctest belongs on `sqlite_diff_rs::pg_walstream_reverse::op_to_message` upstream. | UPSTREAMED (owner: sqlite-diff-rs 0.1.2 release) | 3 |
| `tests/sqlite_cdc_smoke.rs` | 76 | `SqliteCdcSource::new(conn, catalog, SqliteCdcConfig::default())` + `source.execute` + `source.next_event().await` + assertions on `Cell::Int(7)`, `Cell::Float(9.5)`, `Cell::String(Arc::from("paid"))` | `PgSqliteEmuSource::open_in_memory(pg_ddl)?` + `source.execute(...)?` + `source.poll_next_event()?` + typed accessors | RESTORED (`tests/pg_sqlite_emu_smoke.rs`) | 1 |
| `tests/sqlite_cdc_dml.rs` | 157 | `SqliteCdcSource::with_pg_ddl` + single-PK and composite-PK DML round trips + `changed_columns` diff assertions | `PgSqliteEmuSource::open_in_memory` + same DML pattern + typed accessors on the pg_output event | RESTORED (`tests/pg_sqlite_emu_dml.rs` including the composite-PK case; also caught the `(None, Some)` fallback bug on unchanged PK columns) | 1 |
| `tests/follow_cdc_sqlite.rs` | 78 | `SqliteCdcSource::new` on a shared temp file + `engine.register_follow_insert` on a sibling connection to the same file + `source.poll_next_event()` + `engine.dispatch` | `PgSqliteEmuSource` on a single in-memory connection + `engine.follow_row` with a typed PK + `source.execute` + drain + dispatch. `register_follow_insert` cannot be used here because its trait bound requires the diesel connection backend to match the engine backend, which is impossible for the Postgres emulator over a SQLite diesel connection; the follow coverage is preserved via `follow_row`. | RESTORED (`tests/follow_pg_sqlite_emu.rs`) | 2 |
| `tests/proptest_sqlite_cdc.rs` | 246 | Proptest: arbitrary `(op, row)` sequences filtered against an in-memory `BTreeMap` model, applied to source, assertions on `EventKind`, `pk().values()`, `new_row().cells`, `changed_columns()` | Same, retargeted to `PgOutputEvent` + `Presence` accessors + `SqliteChangesetEvent`-shaped `changed_columns` semantics. Drain interleaved per DML because SQLite session iterates the changeset in PK order, not DML execution order. | RESTORED (`tests/proptest_pg_sqlite_emu_cdc.rs`; the model also filters no-op UPDATEs since SQLite session collapses them out of the changeset) | 2 |
| `tests/proptest_pgoutput_bridge.rs` | 220 | Proptest: `PgOutputParser::parse(PgOutputBridge::encode(WalEvent)) == WalEvent` for arbitrary Insert / Update / Delete / Truncate over a fixed catalog | Same property, but the "encode" step is now `sqlite_diff_rs::pg_walstream_reverse::op_to_message` + `encode_message`, and the "input" is a `ChangesetOp` synthesised from a proptest strategy. Belongs upstream in `sqlite-diff-rs` as test #12 in `docs/upstream-sqlite-diff-pgoutput-reverse.md`. | UPSTREAMED (owner: sqlite-diff-rs 0.1.2 release) | 3 |
| `tests/proptest_sqlite_dispatch.rs` | 432 | Two proptests: (a) direct `source -> engine.consumers` dispatch matches a reference oracle, (b) round-trip through `PgOutputBridge` + `PgOutputParser` before dispatch matches the same oracle | (a) `PgSqliteEmuSource` already routes every event through the `PgOutputBridge`-equivalent path in the drain loop, so the two proptests collapse into ONE proptest that drives the emulator and asserts against the oracle. **This is the single highest-signal test.** | RESTORED (`tests/proptest_pg_sqlite_emu_dispatch.rs`; also uncovered that SQLite session drops no-op UPDATEs so the model must skip them symmetrically) | 1 |

## Restoration order

Priority 1 (this branch, before land):

1. Add ergonomic helpers to `PgSqliteEmuSource` so doctests stay short. See "Ergonomic surface" below.
2. Write doctests directly in `src/pg_sqlite_emu/mod.rs`, `source.rs::PgSqliteEmuSource`, and `source.rs::PgSqliteEmuSource::new`. These cover the mod-level and struct-level examples deleted from `sqlite_cdc/`.
3. Restore `tests/pg_sqlite_emu_smoke.rs` (renamed from the old `sqlite_cdc_smoke.rs`; different backend, honest new name).
4. Restore `tests/pg_sqlite_emu_dml.rs` (renamed from `sqlite_cdc_dml.rs`, including the composite-PK case which is otherwise uncovered).
5. Restore `tests/proptest_pg_sqlite_emu_dispatch.rs` (single proptest, collapses the old direct-dispatch and pgoutput-round-trip pair since both now travel the same path).

Priority 2 (this branch, if bandwidth allows):

6. Restore `tests/proptest_pg_sqlite_emu_cdc.rs` (arbitrary DML against reference model; catches shape drift the dispatch proptest may miss because it only asserts oracle notifications, not per-event structure).
7. Restore `tests/follow_pg_sqlite_emu.rs` (`register_follow_insert` + shared temp-file emulator).

Priority 3 (owner: `sqlite-diff-rs` 0.1.2 release):

8. Move the `parse(encode(op)) == op` round-trip proptest upstream to `sqlite-diff-rs`. Once landed, delete this row from the checklist.

## Ergonomic surface (the "make doctests short" list)

The old `sqlite_cdc` doctests already required 8-15 lines of setup. Doctests for the emulator should aim for the same or less. The current `pg_sqlite_emu` API is close but forces two rituals every doctest repeats:

- Open a `SqliteConnection` and pass it to `PgSqliteEmuSource::new`. Two lines that can collapse into one.
- Wrap DML in `sql_query(sql).execute(source.connection())`. Two more lines per DML.

These helpers are what will land next (see the TODO markers in the code):

| Method | Rationale | Signature |
|---|---|---|
| `PgSqliteEmuSource::open_in_memory(pg_ddl)` | One-liner constructor for the overwhelmingly common in-memory case. Saves the diesel `establish(":memory:")` incantation on every doctest. | `pub fn open_in_memory(pg_ddl: &str) -> Result<Self, PgSqliteEmuError>` |
| `PgSqliteEmuSource::execute(sql)` | Wraps `diesel::sql_query(sql).execute(&mut self.connection)` so doctests do not need to import `sql_query` and `RunQueryDsl` just to run one DML. Returns the affected row count on success. | `pub fn execute(&mut self, sql: &str) -> Result<usize, PgSqliteEmuError>` |
| `PgSqliteEmuSource::drain()` | Loops `poll_next_event` until the queue empties and returns the collected `Vec<PgOutputEvent>`. Useful in tests that do not care about per-event pacing. | `pub fn drain(&mut self) -> Result<Vec<PgOutputEvent>, PgSqliteEmuError>` |

Non-goals (deliberate):

- **No `PgSqliteEmuSource::build_engine(...)` helper.** The engine crosses a distinct architectural layer; hiding its construction inside the emulator entangles them. Doctests spell out the 3-line engine construction explicitly.
- **No `PgSqliteEmuSource::dispatch_next(engine)` combined poll+dispatch.** Same reason.
- **No new `PgSqliteEmuConfig`.** The old `SqliteCdcConfig` was empty (`#[non_exhaustive]` placeholder). The emulator will re-introduce it if a real knob appears.

## Test scaffolding pattern

Every restored integration test / doctest follows the same skeleton to keep them scannable:

```rust
use diesel::{Connection, SqliteConnection};
use subql::{
    catalog_helpers, DefaultIds, EventKind, PgOutputEvent, PgSqliteEmuSource,
    SubscriptionEngine, SubscriptionRequest,
};
use sqlparser::dialect::PostgreSqlDialect;

const PG_DDL: &str = "...";

// Build source (uses the ergonomic helper).
let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL)?;

// Build engine over the same catalog.
let mut engine: SubscriptionEngine<PgOutputEvent, DefaultIds, _> =
    SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});
engine.register(SubscriptionRequest::new(1, "..."))?;

// Drive DML.
source.execute("INSERT INTO ...")?;

// Drain + dispatch.
let event = source.poll_next_event()?.expect("event pending");
let notifs = engine.consumers(&event)?;
assert_eq!(notifs.inserted(), &[1]);
```

Restored tests may deviate from this pattern only for a stated reason (proptest oracle setup, temp-file plumbing for `follow_insert`, etc.). Any deviation belongs at the top of the file in a `//! Deviation:` block so a reviewer can skim.
