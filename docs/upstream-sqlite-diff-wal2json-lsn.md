# Upstream proposal: carry the wal2json LSN on `sqlite-diff-rs::wal2json::MessageV2`

**Status**: proposal (upstream `sqlite-diff-rs` change, needed by subql Phase 3)
**Target crate**: `sqlite-diff-rs` (currently pinned at 0.2.0 in subql, feature `wal2json`)
**subql context**: `docs/handoff-cdc-ecosystem-convergence.md` sections 2, 7, and 8 step 3

## Summary

subql Phase 3 converges the wal2json and Maxwell CDC paths onto `sqlite-diff-rs`'s parsers, deleting subql's bespoke `Wal2JsonV1Event`, `Wal2JsonV2Event`, `MaxwellEvent`, and their serde parsers, and implementing `CdcEvent` directly on `sqlite_diff_rs::wal2json::{MessageV2, ChangeV1}` and `sqlite_diff_rs::maxwell::Message` (the same convergence Phase 2 did for `pg_walstream::ChangeEvent`).

Almost everything fits with no upstream change. The one gap: `sqlite_diff_rs::wal2json::MessageV2` does not carry the wal2json LSN. wal2json emits an `lsn` field per message when the plugin runs with `include-lsn=true`, and subql uses it as the wal2json v2 stream checkpoint for replay and resume. Without it, converging onto `MessageV2` silently drops that checkpoint.

This document asks `sqlite-diff-rs` to add an optional `lsn` field to `MessageV2`. It is exactly what wal2json already emits, it is a single `#[serde(default)]` field, and it changes nothing for existing consumers or for the `Digestable` path.

## Why the LSN matters to subql

subql anchors a CDC stream on a `Checkpoint` so consumers can persist a cursor and resume. For the wal2json v2 path that checkpoint is a `PgLsn` parsed from the message's `lsn`.

- subql's current parser already reads it: `Wal2JsonV2Message` has `#[serde(default)] pub lsn: Option<String>` (`src/wal/wal2json.rs:73`), and the converter surfaces it as the event checkpoint with `let checkpoint = msg.lsn.as_deref().and_then(crate::PgLsn::parse);` (`src/wal/wal2json.rs:351`). `PgLsn::parse` accepts wal2json's `hi/lo` hex form (for example `0/16B2270`).
- It is load-bearing in tests: `tests/reexec_postgres_r2d2.rs:159` asserts `events[0].checkpoint().expect("wal2json event LSN")`, and `src/wal/wal2json.rs` has `typed_v2_checkpoint_from_lsn` asserting `events[0].checkpoint().is_some()` for a message with `"lsn": "0/16B2270"`.

`sqlite_diff_rs::wal2json::MessageV2` (0.2.0) has fields `action`, `schema`, `table`, `columns`, `identity` and no `lsn` (`src/wal2json.rs:71` in the crate). So a subql `impl CdcEvent for MessageV2` would have to return `checkpoint() == None`, regressing wal2json v2 replay and breaking the test above.

The other two message types need nothing here. `wal2json::ChangeV1` has no per-change LSN (v1 groups a transaction, and subql uses `NoCheckpoint` for it). `maxwell::Message` carries a binlog `position` string, but subql uses `NoCheckpoint` for Maxwell today, so it is out of scope for this change (see Related below).

## Proposed change

Add one optional field to `MessageV2`, gated by the existing `wal2json` feature:

```rust
// sqlite-diff-rs, src/wal2json.rs, struct MessageV2
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageV2 {
    pub action: Action,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<Column>>,
    #[serde(default)]
    pub identity: Option<Vec<Column>>,
    /// PostgreSQL LSN in `hi/lo` hex notation, present when wal2json runs
    /// with `include-lsn=true`. `None` otherwise.
    #[serde(default)]
    pub lsn: Option<String>,
}
```

Keep it a raw `Option<String>`, not a parsed LSN type, so the `wal2json` module stays free of any Postgres-specific numeric LSN type and the consumer decides how to interpret it. subql parses it into its own `PgLsn`.

### Why this is safe

- `#[serde(default)]` means existing inputs without an `lsn` field still deserialize, and existing constructors that use `..Default::default()` or struct literals are the only thing that must add the field. Since the struct is deserialized from wal2json JSON in practice, real inputs are unaffected.
- The `Digestable` implementation for `MessageV2` (both `ChangesetFormat` and `PatchsetFormat`) does not read `lsn`, so the wire output is byte-identical. The LSN is transport metadata, not row data.
- No new dependency. The value is already present in the wal2json JSON stream and is dropped today only because the struct does not capture it.

## Design considerations to settle in the PR

- Scope to v2 only. v1 wal2json (`ChangeV1` inside `TransactionV1`) has no per-change LSN, and subql treats v1 as `NoCheckpoint`. Do not add `lsn` to `ChangeV1`.
- Field type. `Option<String>` keeps the module Postgres-numeric-free. If a typed accessor is wanted, a helper like `impl MessageV2 { pub fn lsn_u64(&self) -> Option<u64> }` could parse the `hi/lo` form, but subql does not need it (subql owns `PgLsn::parse`).
- Constructors and tests inside `sqlite-diff-rs`. Any in-crate `MessageV2 { .. }` literals (tests, builders) need the new field. `#[serde(default)]` does not help struct literals, so those sites add `lsn: None`.

## How subql adopts it (reviewer context, not part of the `sqlite-diff-rs` change)

Once `MessageV2.lsn` exists, subql Phase 3 will:

- Enable `sqlite-diff-rs/wal2json` and `sqlite-diff-rs/maxwell` (currently off, since `sqlite-diff-rs` is pulled with `default-features = false`).
- Parse via `sqlite_diff_rs::wal2json::{parse_v2, parse_v1}` and `sqlite_diff_rs::maxwell::parse`, and implement `CdcEvent` on `MessageV2`, `ChangeV1`, and `maxwell::Message`, decoding each cell's `serde_json::Value` against the subql catalog with the existing `json_value_to_pg_value_by_kind` and `json_value_to_mysql_value_by_kind` helpers (the Phase 2 pattern, resolving table and column names to catalog ordinals on demand).
  - `checkpoint()` for `MessageV2` returns `PgLsn::parse(lsn)`. For `ChangeV1` and `maxwell::Message` it returns `None`.
  - `pk_columns` uses the catalog primary key (the same decision reached in Phase 2, since it is the row identity subql matches follows and PK projections against).
- Delete `src/wal/wal2json.rs` and `src/wal/maxwell.rs`'s bespoke event structs, serde message structs, and `WalParser` impls (`Wal2JsonV1Parser`, `Wal2JsonV2Parser`, `MaxwellParser`), plus the corresponding re-exports.

No `pg_walstream` version interaction here: the `wal2json` and `maxwell` modules pull only `serde` and `serde_json`, so they are independent of the `pg_walstream` 0.7 and 0.8 split that Phase 2 introduced.

## Validation for the `sqlite-diff-rs` change

- Add a unit test that `parse_v2` on a message containing `"lsn": "0/16B2270"` populates `MessageV2.lsn == Some("0/16B2270")`, and that a message without the field deserializes with `lsn == None`.
- Confirm the `Digestable` round-trip output for a message is unchanged whether or not `lsn` is present.

## Related, out of scope for this change

`maxwell::Message` already carries `position: Option<String>` (a binlog position such as `master.000006:800911`). subql currently maps Maxwell to `NoCheckpoint`, so no change is needed for Phase 3. If subql later wants a `MysqlBinlogPos` checkpoint for the Maxwell path, `position` is the field it would parse, with no further `sqlite-diff-rs` change required.
