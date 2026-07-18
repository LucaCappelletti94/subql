# Handoff: CDC ecosystem convergence and the full patchset loop

## Purpose of this document

This is a handoff for a fresh session with clean context. It records the end goal, why the current subql code stands in the way, and the concrete cleanup that unblocks it. Read it top to bottom before touching code.

## 1. The end goal: the full bidirectional loop

We want subql to support the complete round trip of a `connetto-rs`-shaped topology, where a Postgres server is the source of truth and SQLite clients hold local replicas.

Outbound (server to client), NOT built yet:

1. Postgres emits generic CDC (pgoutput, or wal2json, or Maxwell on MySQL).
2. subql ingests that CDC.
3. subql emits a `sqlite-diff-rs` patchset directed at the subscribed clients.
4. the client applies that patchset into its local SQLite database (the INSERT/UPDATE/DELETE lands in SQLite).

Inbound (client to server), already built and Docker E2E tested:

5. the client mutates its local SQLite, and the SQLite session extension produces a changeset or patchset.
6. that patchset is uploaded to subql.
7. `SubscriptionEngine::apply_patchset(patchset, conn, adapter)` applies it to Postgres (or MySQL, or SQLite) with native diesel binds.

Step 7 exists (`src/patchset/`, adapters `PgAdapter`, `MysqlAdapter`, `SqliteAdapter`, tests `tests/apply_patchset_{pg,pg_uuid,mysql,sqlite}_e2e.rs`). Steps 2 and 3 (CDC to patchset emission) do NOT exist in subql. Closing the loop means building the outbound half and proving the whole cycle in one integration test.

The single target test that proves the loop: drive pgoutput CDC through subql, emit a `PatchsetFormat` patchset, apply it to a SQLite database with `SqliteAdapter`, capture the resulting SQLite session changeset, apply that through `PgAdapter` back to Postgres, and assert row parity across the cycle.

## 2. Why the loop is blocked: duplicated parsing and bespoke wrappers

subql currently maintains a second, parallel CDC stack that re-implements what the ecosystem crates already provide. This is the core problem and the main cleanup target. The duplication is most evident for pgoutput and wal2json.

### Duplicated parsing (the headline item)

- pgoutput. subql parses replication in `src/wal/pgoutput.rs` (`PgOutputParser`) against the LOW-level `pg_walstream::protocol::LogicalReplicationMessage` frames plus a private relation cache, producing `PgOutputEvent`. Meanwhile `pg_walstream` already exposes a HIGH-level `EventStream` and `EventStreamRef` that yield `ChangeEvent` values whose `EventType` already carries the resolved schema, table name, `RowData` (name to `ColumnValue`), old and new images, `ReplicaIdentity`, and key columns. On top of that, `sqlite-diff-rs` (`src/pg_walstream.rs`, feature `pg-walstream`) already implements `Digestable` on `pg_walstream::EventType` for both `ChangesetFormat` and `PatchsetFormat`. subql reproduces relation caching and tuple assembly that `pg_walstream` performs already.
- wal2json. subql parses wal2json JSON in `src/wal/wal2json.rs` with its own serde structs (`Wal2JsonV1Message`, `Wal2JsonV2Message`, `Wal2JsonV2Column`, and friends), producing `Wal2JsonV1Event` and `Wal2JsonV2Event`. `sqlite-diff-rs` (`src/wal2json.rs`, feature `wal2json`) already parses wal2json into its own message structs and implements `Digestable` for both formats. Two independent wal2json parsers.
- Maxwell. subql parses Maxwell JSON in `src/wal/maxwell.rs` (`MaxwellParser`, serde `MaxwellMessage`), producing `MaxwellEvent`. `sqlite-diff-rs` (`src/maxwell.rs`, feature `maxwell`) already parses Maxwell and implements `Digestable`. Two independent Maxwell parsers.
- SQLite changeset. subql parses in `src/sqlite_cdc/parser.rs` (`SqliteChangesetParser`), but this one already delegates to `sqlite_diff_rs::ParsedDiffSet::parse` and `ChangesetOp`, so it is a thin adapter, not a duplicate parser. It is the shape the others should converge toward.

### Bespoke wrapper event types

subql defines its own per-source event structs and implements `CdcEvent` on them: `PgOutputEvent`, `Wal2JsonV1Event`, `Wal2JsonV2Event`, `MaxwellEvent`, `SqliteChangesetEvent`, and the test-only `TestEvent<B>`. These wrappers hold re-parsed, subql-shaped state. They are the reason subql events cannot be fed into `sqlite-diff-rs`'s `digest` machinery. They must be removed if, and only if, they carry nothing that the ecosystem event plus the catalog cannot provide on demand.

## 3. Why this is now possible (the enabler already landed on this branch)

The wrappers historically existed to cache decoded cell state, because the old `CdcEvent` returned typed values through reference-returning accessors that needed somewhere to point. That reason is gone. This branch already reshaped the cell accessor to

    fn value_at<DB: DatabaseLike>(&self, db: &DB, row: RowKind, col: ColumnId) -> Value<Self::Backend>

which decodes one cell on demand against the catalog and returns it owned. No decode cache is required anymore. So a `CdcEvent` implementation can now be a thin view over a raw ecosystem event, decoding lazily. This is exactly the convergence the branch set out to do. The value_at change is the precedent to follow for the rest of the trait.

## 4. The linchpin: reshape the `CdcEvent` trait

The remaining obstacle is the trait shape itself, in `src/backend.rs` (`pub trait CdcEvent`). Today:

    fn kind(&self) -> EventKind;
    fn table_id(&self) -> TableId;
    fn checkpoint(&self) -> Option<&Self::Checkpoint>;
    fn pk_columns(&self) -> &[ColumnId];
    fn changed_columns(&self) -> &[ColumnId];
    fn value_at<DB: DatabaseLike>(&self, db: &DB, row: RowKind, col: ColumnId) -> Value<Self::Backend>;

Three structural methods force a wrapper to exist:

- `table_id() -> TableId` takes no catalog, but a raw ecosystem event only knows the table NAME. Resolving a name to a subql `TableId` needs the catalog.
- `pk_columns() -> &[ColumnId]` and `changed_columns() -> &[ColumnId]` return borrowed slices, which forces the event to STORE those vectors. A raw ecosystem event does not carry subql `ColumnId` ordinals, and computing them needs the catalog. A borrowed return cannot be computed on demand.
- `checkpoint() -> Option<&Self::Checkpoint>` returns a borrowed subql checkpoint, but `pg_walstream::ChangeEvent` carries a `pg_walstream::Lsn`, a distinct type from subql `PgLsn`. A borrowed return cannot bridge the type.

The fix is the same pattern already applied to `value_at`: thread `db` through the structural methods and return owned values. Sketch:

    fn table_id<DB: DatabaseLike>(&self, db: &DB) -> TableId;        // resolve name to id
    fn pk_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId>; // or a fill-buffer API
    fn changed_columns<DB: DatabaseLike>(&self, db: &DB) -> Vec<ColumnId>;
    fn checkpoint(&self) -> Option<Self::Checkpoint>;                 // owned, or a bridged type

Notes and open questions for whoever picks this up:

- `pk_columns` reflects replica identity from the wire, not only the catalog primary key (`REPLICA IDENTITY USING INDEX` and `FULL` differ), so it genuinely comes from the event plus the catalog, not the catalog alone.
- Returning `Vec<ColumnId>` per call allocates on a hot path (the engine reads these per event). If profiling shows it matters, use a fill-into-buffer method or a `SmallVec`. Decide this deliberately, do not default to `Vec` without a thought.
- This reshape is atomic. Every `CdcEvent` implementation and every consumer must move together. Consumers to update: `src/runtime/engine.rs` (`consumers`), `src/runtime/dispatch.rs`, `src/runtime/indexes.rs`, `src/runtime/partition.rs`, `src/reexec/{engine,maintain,auto,async_auto}.rs`, and `src/compiler/vm.rs`. The test suite is the largest surface (hundreds of call sites, same class of change we did for `value_at` this session).

## 5. What stays subql-specific (do NOT try to delete this)

Two things are legitimately subql's and must not be collapsed into the ecosystem types.

- The positional, catalog-resolved VIEW. The subscription VM reads cells positionally by `ColumnId` and needs `table_id`, `pk_columns`, `changed_columns` as catalog ordinals. Ecosystem events are name-keyed. So a subql-side view over the raw event is the right abstraction. The point of the cleanup is that the view sits directly on the ecosystem event, not that the view disappears.
- The rich per-backend value model. subql `Value<Backend>` carries real typed scalars (`chrono` temporals, `bigdecimal::BigDecimal`, `uuid::Uuid`, native `bool`) because the VM does typed predicate evaluation (`created_at > '...'`, `amount > 100.50`). `sqlite-diff-rs`'s `Value` is SQLite affinity (Integer, Real, Text, Blob, Null), deliberately lossy of those distinctions. These two value models serve different purposes and stay separate. The `digest` path decodes affinity straight from the wire, so there is no forced unification.

## 6. The ecosystem machinery to align with

In `sqlite-diff-rs` 0.5.0 (`main`, patched into subql by git branch). 0.4.0 landed the source-independent semantic key (`WireType`), and 0.5.0 migrated the pg `Digestable` from pg_walstream 0.7 to 0.8. The `digest` and `WireSchema` surface below was verified against the 0.5.0 source.

- `DiffSetBuilder::<F, T, S, B>::digest(event, schema, adapter)` folds one CDC event into a builder, where `F` is `ChangesetFormat` or `PatchsetFormat`. `build()` produces the wire bytes.
- `Digestable` is implemented in-crate for `pg_walstream::EventType`, `wal2json::MessageV2`, `wal2json::ChangeV1`, and `maxwell::Message`, each for both formats.
- `WireSource` names a source (`PgWalstream`, `Wal2Json`, `Maxwell`) and its per-column payload. Type identity is no longer source-native. Every payload carries a `WireType` (`wire_type(payload) -> WireType`) that selects the decoder.
- `WireSchema` (source-independent) resolves a table name to a `Table: NamedColumns + WireColumnTypes`, and `WireColumnTypes::column_type(col_idx) -> WireType`. One semantic catalog drives every source. This is the "pass the schema to it" hook.
- `WireAdapter<Src, S, B>::decode(payload) -> Value<S, B>` decodes one raw column payload. `TypeMap<Src, S, B>` is keyed by `WireType`, and `TypeMap::defaults()` ships one decoder per `WireType`.

On the apply side, already wired in subql: `SubscriptionEngine::apply_patchset` in `src/patchset/mod.rs` with `PgAdapter`, `MysqlAdapter`, `SqliteAdapter`.

## 7. The uphill work (flagged, needs the user)

Completing the loop through `digest` requires changes above subql, which the user explicitly agreed to work on when they arise.

- RESOLVED in `sqlite-diff-rs` 0.4.0. The `WireSchema` was source-native keyed (OID for Postgres) while subql's catalog is `ScalarKind`-based. 0.4.0 added the source-independent `WireType` semantic key, so subql maps `ScalarKind` to `WireType` (`src/emit.rs::scalar_kind_to_wire_type`) and one `WireCatalog` over `ParserDB` drives every source with no OIDs. See `docs/uphill-sqlite-diff-rs-semantic-wire-type.md` for the upstream spec.
- subql sources must expose the raw high-level events that `digest` consumes. `PgStreamingCdcSource` (`src/wal/pg_streaming.rs`) currently uses the low-level `LogicalReplicationParser`. It would move to `pg_walstream`'s high-level `EventStream` yielding `ChangeEvent`. The wal2json and Maxwell paths would parse via `sqlite-diff-rs`'s parsers rather than subql's own. This is the concrete step that removes the duplicated parsing from section 2.
- RESOLVED in `sqlite-diff-rs` 0.5.0. The pg `Digestable` was written against pg_walstream 0.7 `EventType`, while subql's pgoutput sources decode to the 0.8 fork `ChangeEvent`, so the two `EventType` types did not match. 0.5.0 bumped the crate to pg_walstream 0.8, and subql unifies the version with a `[patch.crates-io] pg_walstream = <fork branch>` so the crate and subql share one `EventType`. The pgoutput emit (`src/emit.rs::pgoutput_patchset_builder`, behind the `pgoutput-emit` feature) then digests subql's `ChangeEvent` directly. This also let `pg-sqlite-emu` drop the `pg_walstream_07` alias and encode through pg_walstream 0.8.

## 8. Suggested phasing (each phase is a checkpoint, report and wait)

The repo convention requires finishing each numbered step fully and reporting before the next. Suggested order:

1. DONE. Reshape the `CdcEvent` trait (thread `db`, owned returns), update every wrapper impl, consumer, and test. Behavior unchanged. Green bar.
2. DONE and Docker-verified. `impl CdcEvent for pg_walstream::ChangeEvent`, decoding against the catalog. Rewire `PgStreamingCdcSource`, `PollingPgCdcSource`, and `PgSqliteEmuSource`. Delete `PgOutputParser` and `PgOutputEvent`.
3. DONE and Docker-verified. `impl CdcEvent` on `sqlite_diff_rs::wal2json::{MessageV2, ChangeV1}` and `sqlite_diff_rs::maxwell::Message`. Delete the bespoke wal2json and Maxwell parsers and events.
4. DONE and Docker-verified (via the Phase 5 round trip). Outbound emission in `src/emit.rs`. `wal2json_patchset_builder(db, events)` folds `sqlite_diff_rs::wal2json::{MessageV2, ChangeV1}` events through `DiffSetBuilder::<PatchsetFormat>::digest` over a subql `WireCatalog` and `WireTable` (`WireSchema` plus `WireColumnTypes` over `ParserDB`), and returns a `PatchSet` builder. `wal2json_patchset` serializes it to wire bytes. Keyed on `WireType` via the 0.4.0 semantic key, so no OIDs. The pg_walstream and Maxwell emission paths reuse the same source-agnostic `WireCatalog`.
5. DONE and Docker-verified for both source vehicles. `tests/round_trip_pg_wal2json_e2e.rs` and `tests/round_trip_pg_pgoutput_e2e.rs` run the full cycle in two phases so update and delete are genuine patchset ops, not consolidated inserts. A seed phase inserts three rows, then a mutate phase updates one and deletes another, matched on a `UUID` primary key. Each phase drives CDC (wal2json slot drain, or pgoutput binary slot drain decoded through `PgOutputDecoder`) and emits its own patchset. The seed applies to a fresh SQLite replica, then a tracking session records the mutate (a real `UPDATE` and `DELETE`), and that session patchset is rebuilt (insert, update, and delete ops) and re-applied to a re-seeded Postgres through a dispatch-aware `PgAdapter`, asserting row parity at each step. The shared machinery is `tests/common/dispatch.rs`. The schema exercises adapter dispatch across a UUID primary key plus four column type classes with no SQL cast: `BOOLEAN` (integer affinity to native `Bool`), `UUID` (16-byte blob to native `UUID`, both the key and a non-key column), a `DOMAIN` over text, and an `ENUM`. The domain and enum bind through diesel's own `SqlType`/`ToSql` resolved by OID name. This closes the bidirectional loop from section 1 on both vehicles, for insert, update, and delete.

An alternative that unblocks the loop sooner without the uphill: build a subql-native `CdcEvent` to patchset encoder (read `value_at`, build `sqlite_diff_rs::{Insert, Update, PatchDelete}` via the public builder API, map `Value<Backend>` to affinity `Value`). It does not use `digest` and does not remove the duplicated parsing, so it is a stopgap, not the convergence. Only take it if the loop is urgent and the cleanup can follow.

## 9. Current state of the branch

Branch `refactor/cdc-event-trait`, many uncommitted files, nothing committed. All five phases of section 8 are complete and green. Both the wal2json and pgoutput round trips are Docker-verified for insert, update, and delete across a UUID primary key plus a bool, a non-key uuid, a domain, and an enum, in both directions. The full bidirectional loop from section 1 is closed on both source vehicles.

Trait and value model (earlier on the branch):

- `CdcEvent::value_at` takes the catalog and decodes on demand. `Presence` gone, typed accessors gone, runtime `ScalarKind` dispatch gone.
- The full `CdcEvent` reshape from section 4 is done. `table_id`, `pk_columns`, and `changed_columns` thread `db` and return owned values, and `checkpoint` returns an owned `Option<Self::Checkpoint>`. Owned `Vec<ColumnId>` was chosen over `SmallVec` or a fill-buffer to keep `smallvec` out of the public trait, since PK and changed-column arity is small.
- Debezium removed. Dead decoders removed. The `ScalarKind` operand on `Instruction::LoadColumn` removed. The `ScalarKind` enum stays.

Convergence onto ecosystem event types (Phases 2 through 5):

- pgoutput. `impl CdcEvent for pg_walstream::ChangeEvent` in `src/wal/change_event.rs`, plus `into_engine_events`. `PgStreamingCdcSource`, `PollingPgCdcSource`, and `PgSqliteEmuSource` decode via `pg_walstream::PgOutputDecoder`. `src/wal/pgoutput.rs` deleted.
- wal2json. `impl CdcEvent for sqlite_diff_rs::wal2json::{MessageV2, ChangeV1}` in `src/wal/wal2json.rs`. Free functions `parse_wal2json_v2` and `parse_wal2json_v1` replace the old parser structs. `MessageV2` surfaces its `lsn` as a `PgLsn` checkpoint. `ChangeV1` uses `NoCheckpoint`.
- Maxwell. `impl CdcEvent for sqlite_diff_rs::maxwell::Message` in `src/wal/maxwell.rs`, re-exported as `MaxwellMessage`. Free function `parse_maxwell` peeks the `type` field, drops control frames (`ddl`, `bootstrap-start`, and similar), and normalizes `bootstrap-insert` to `insert`.
- SQLite changeset. Unchanged. `SqliteChangesetParser` still implements `WalParser`, now the sole implementor of that trait. `src/wal/test_support.rs` deleted (its fixtures only served the old parser tests).
- Emission (Phases 4 and 5). `src/emit.rs` holds `WireTable` and `WireCatalog` (subql `WireSchema` over `ParserDB`), `scalar_kind_to_wire_type`, and the per-source entry points: `wal2json_patchset_builder` / `wal2json_patchset` (unconditional), and `pgoutput_patchset_builder` / `pgoutput_patchset` (behind the `pgoutput-emit` feature, which turns on `sqlite-diff-rs/pg-walstream`). Both fold events through `DiffSetBuilder::<PatchsetFormat>::digest` over the same source-agnostic `WireCatalog`. The adapters decode `UUID` as a 16-byte blob (`UuidBlob16Decoder`), matching the SQLite client's storage. Maxwell emission would add one more adapter the same way.

Design decisions carried across the convergence:

- `pk_columns` resolves the catalog primary key for all sources. Replica-identity availability is handled by `value_at` returning `Value::Missing`, not by widening the PK set. The pg_walstream fork's `key_columns_for_relation` returns all columns under `REPLICA IDENTITY FULL`, which broke PK follow-matching, so subql matches on the catalog PK.
- `changed_columns` for the JSON sources is derived only when both images cover every column (`REPLICA IDENTITY FULL`), matching the old behavior, compared by column name.

Dependency wiring (do not fight this):

- `pg_walstream` points at git branch `feat/pgoutput-decoder` (0.8.0) for subql, both as a direct dependency and as a `[patch.crates-io]` so `sqlite-diff-rs`'s `pg_walstream = "0.8"` resolves to the same fork (one `EventType` across the graph). The old `pg_walstream_07` (crates.io 0.7) alias is gone: `pg-sqlite-emu` now encodes through pg_walstream 0.8.
- `sqlite-diff-rs` is 0.5.0 via `[patch.crates-io] sqlite-diff-rs = { git = <fork>, branch = "main" }` (the dep requirement is `"0.5"`), with `default-features = false, features = ["wal2json", "maxwell"]`. `pgoutput-emit` adds `sqlite-diff-rs/pg-walstream`. 0.5.0 carries the `WireType` semantic key (0.4.0) and the pg_walstream 0.8 migration. `MessageV2.lsn: Option<String>` still backs the wal2json v2 checkpoint. Repoint at a crates.io 0.5.0 release once it ships.

Test-harness and detour notes:

- `tests/common/mod.rs::drain_slot` now requests `include-lsn=true` so wal2json v2 output carries the LSN that `MessageV2::checkpoint` surfaces. Without it `reexec_postgres_r2d2` fails on the event-LSN assertion.
- A misconceived Docker test `register_batch_cannot_evict_in_flight_entries` (in `tests/eviction_e2e.rs`) was deleted. It asserted a no-self-eviction batch contract that contradicts the parity-with-sequential-register contract fixed in commit 95b435d and enforced by `tests/proptest_register_batch_parity.rs`. It had never run because it was Docker-gated. Two in-memory unit tests in `src/runtime/engine.rs` (`register_batch_evict_oldest_churns_like_sequential`, `register_batch_equals_sequential_loop_under_cap`) now pin the correct churn and parity behavior on every `cargo test --lib`.

Green bar last run, all passing: `cargo +1.88 fmt --all -- --check`, `cargo +1.88 clippy --all-targets --all-features -- -D warnings`, `cargo +1.88 test --lib --release --all-features` (393 tests, including 7 `emit` tests), `cargo +1.88 test --doc --all-features` (38 tests), `RUSTDOCFLAGS="-D warnings" cargo +1.88 doc --lib --all-features --no-deps`. Docker E2E verified: `round_trip_pg_wal2json_e2e` and `round_trip_pg_pgoutput_e2e` (the Phase 5 full loop, both vehicles, insert plus update plus delete across a UUID primary key and four column type classes), plus the Phase 1 to 3 convergence set `cdc_cross_db`, `cdc_mysql_e2e`, `apply_patchset_mysql_e2e`, `reexec_postgres` (4), `reexec_postgres_r2d2` (1), `eviction_e2e` (4), and Phase 2's `cdc_equivalence`, `pg_streaming_e2e`, `polling_smoke`, `apply_patchset_pg_e2e`. The in-memory `pg-sqlite-emu` tests (`pg_sqlite_emu_smoke`, `pg_sqlite_emu_dml`, `proptest_pg_sqlite_emu_cdc`) pass after the pg_walstream 0.8 encode migration.

## 10. Constraints and gotchas

- Toolchain `cargo +1.88`. Workspace lints set `pedantic` and `nursery` to `warn`, and `-D warnings` promotes them to errors. No `unsafe`.
- Prose in code, comments, docs, and commit messages uses no semicolons, no em or en dashes, and no ASCII dash as punctuation. Plain ASCII only.
- Never commit, push, or open a PR without an explicit per-time instruction. Docker E2E and any heavy task need explicit approval before running. Subagents must not run cargo (shared build lock).
- Green bar is the gate. Run the full checklist from section 9 before declaring any phase done.
- Key files. Trait: `src/backend.rs`. Events: `src/wal/{change_event,wal2json,maxwell}.rs` (impls on ecosystem types), `src/wal/{pg_type,pg_streaming}.rs`, `src/sqlite_cdc/{parser,event,source}.rs`, `src/pg_sqlite_emu/`, `src/testing.rs` (`TestEvent`). Emission: `src/emit.rs`. Round-trip tests: `tests/round_trip_pg_{wal2json,pgoutput}_e2e.rs` and the shared `tests/common/dispatch.rs`. Consumers: `src/runtime/{engine,dispatch,indexes,partition}.rs`, `src/reexec/`, `src/compiler/vm.rs`. Apply and patchset: `src/patchset/`. Catalog: `src/catalog_helpers.rs`.
