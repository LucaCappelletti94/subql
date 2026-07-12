# Upstream: `sqlite-diff-rs` changeset -> `pgoutput` adapter

**Target crate:** `sqlite-diff-rs` (currently pinned at `0.1.1`, git `github.com/LucaCappelletti94/sqlite-diff-rs`).
**Consumer:** `subql`'s `pg_sqlite_emu` module (this branch), which needs to feed a live `PgOutputParser` from an in-process SQLite session extension. Every other consumer that wants to bridge a SQLite session-tracked database into a Postgres logical replication pipeline benefits from the same code.

The current forward-direction adapter (`sqlite_diff_rs::pg_walstream`) turns high-level `pg_walstream::EventType` events into `sqlite-diff-rs` builder ops. The direction we need is the mirror image: turn each `ChangesetOp` in a `ParsedDiffSet::Changeset` into a `pg_walstream::LogicalReplicationMessage` ready for `pg_walstream::encode_message`. That gives `subql` (and any other caller) a byte stream a real `PgOutputParser` can decode.

## Motivation

`subql` builds a fake Postgres CDC source over SQLite so doctests and fuzz harnesses can exercise the full `pgoutput` decode plus engine dispatch path without a Docker Postgres. The flow is:

1. Translate PG DDL to SQLite DDL via `pg2sqlite`, apply to a `SqliteConnection`.
2. Attach a `diesel_sqlite_session::Session` to the connection.
3. Route caller DML through the connection so the session accumulates changes.
4. On poll, call `Session::changeset()` (**not** `patchset()` — we need the full old-image per row so `UPDATE` and `DELETE` old tuples survive), then convert every `ChangesetOp` to a `LogicalReplicationMessage`, encode with `encode_message`, and hand the bytes to `PgOutputParser` to get typed `PgOutputEvent`s back.

Step 4 is where the upstream adapter belongs. It is a self-contained, semantically pure conversion; no live database, no engine state, no `subql` types.

The initial attempt kept the conversion inline in `subql/src/pg_sqlite_emu/source.rs`, but the shape (op iteration, per-variant tuple construction, per-column `ColumnData` synthesis, PG type OID routing, hex encoding for BYTEA) is exactly what the existing `sqlite_diff_rs::pg_walstream` module does for the forward direction. The two paths belong side by side.

## Scope

### In scope (upstream)

- New module in `sqlite-diff-rs` next to the existing forward adapter. Working name: `pg_walstream_reverse`, exact name to be finalised at implementation time. It exports:
  - `ConversionError` (dedicated, does not share with the forward `pg_walstream::ConversionError` because failure modes differ).
  - `ChangesetToPgOutput` (or free functions, see API sketch below): stateful adapter carrying the per-relation schema needed to build `Relation` and data messages.
  - `RelationSchema` (or similar): the metadata the caller supplies per SQLite table: relation OID, per-column PG type OID, PK flag, PG namespace + name.
  - `wire_value_to_column_data(value: &Value<String, Vec<u8>>, target_oid: Oid) -> ColumnData`: the pure value-shape mapper.

- The following semantic rules, all covered by tests (see below):
  - `ChangesetOp::Insert { values, .. }` -> `LogicalReplicationMessage::Insert { relation_id, tuple: TupleData(values_mapped_to_ColumnData) }`.
  - `ChangesetOp::Delete { old_values, .. }` -> `LogicalReplicationMessage::Delete { relation_id, old_tuple, key_type: 'O' }` (REPLICA IDENTITY FULL semantics).
  - `ChangesetOp::Update { values, .. }` -> `LogicalReplicationMessage::Update { relation_id, old_tuple: Some(...), new_tuple: ..., key_type: Some('O') }` under the same REPLICA IDENTITY FULL contract.
  - `ChangesetOp::Update` cell handling:
    - `(Some(old), Some(new))` -> both tuples carry the mapped `ColumnData`.
    - `(None, None)` -> caller decides. The upstream API accepts a fallback: either a caller-supplied `Vec<Value>` used for both sides, or a `ColumnData::unchanged()` marker. Default when no fallback is supplied: `ColumnData::unchanged()` on **the old side only**, and an `UpstreamError::UnchangedInNewTuple` refusal on the new side (see caller integration below). This asymmetry matches the real `pgoutput` protocol contract that `'u'` is only valid in the old tuple.
    - `(Some(old), None)` and `(None, Some(new))` do not occur in practice on SQLite changesets. If encountered, treat as `(None, None)` on the missing side.
  - Value shape mapping is text mode only (protocol version 1). Wire ->  `ColumnData`:
    - `Value::Null` -> `ColumnData::null()`.
    - `Value::Integer(i)` with target `PG_BOOL` (OID 16) -> `ColumnData::text(b"t")` if `i != 0`, else `ColumnData::text(b"f")`.
    - `Value::Integer(i)` otherwise -> `ColumnData::text(i.to_string().into_bytes())`.
    - `Value::Real(f)` -> `ColumnData::text(format!("{f}").into_bytes())` (Rust `Display` for `f64`; consistent with the forward adapter's parse path).
    - `Value::Text(s)` -> `ColumnData::text(s.as_bytes().to_vec())`.
    - `Value::Blob(b)` with target `PG_BYTEA` (OID 17) -> `ColumnData::text(format!("\\x{HEX}").into_bytes())` where `HEX` is lowercase hex. Other target OIDs on `Blob`: `ColumnData::binary(b.clone())`.
  - Building a `Relation` message from a `RelationSchema`: `namespace`, `relation_name`, `replica_identity = b'f'`, `columns = Vec<ColumnInfo>` in column order. Column flags: bit 0 set when column is PK.
  - Caller helper: `encode_op_to_bytes(op, schema, options) -> Result<Vec<BytesMut>, ConversionError>` that returns 0-2 frames (a `Relation` frame the first time the schema is seen, then the data frame) using an internal `HashSet<Oid>` to track announced schemas. Deferred; only if the shape lands cleanly.

### Out of scope (stays in `subql`)

- PG DDL parsing and `pg2sqlite` translation.
- Session lifecycle, drain, and recreation.
- `PgOutputParser` feedback loop and consumer queue.
- Filling in unchanged-column values via a live SQLite lookup. The upstream API accepts an optional fallback but does not implement the lookup; the emulator queries SQLite with `SELECT json_array(...) FROM t WHERE pk = ?` and passes the result in.
- `CdcSource` trait impl and event queue management.
- The `ScalarKind` -> PG type OID table. That table lives in `subql` because `ScalarKind` is a `subql` concept and the mapping is opinionated. The upstream API only speaks OIDs.

## API sketch

```rust
// sqlite_diff_rs::pg_walstream_reverse   (name subject to review)

use pg_walstream::{ColumnData, ColumnInfo, LogicalReplicationMessage, Oid, TupleData};

/// Errors during changeset -> pgoutput conversion.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    /// The changeset carried a column count that does not match the
    /// caller-supplied schema.
    #[error("arity mismatch: schema has {schema_arity}, op carried {op_arity}")]
    ArityMismatch { schema_arity: usize, op_arity: usize },

    /// A `ChangesetOp::Update` column pair was (None, None) on the new
    /// side and the caller did not supply a fallback row.
    #[error("unchanged column {column_index} in UPDATE new tuple has no fallback")]
    UnchangedInNewTuple { column_index: usize },
}

/// Description of one PG relation the caller wants to encode into.
#[derive(Debug, Clone)]
pub struct RelationSchema<'a> {
    pub relation_oid: Oid,
    pub namespace: &'a str,
    pub relation_name: &'a str,
    pub columns: &'a [ColumnSchema<'a>],
}

/// Per-column metadata.
#[derive(Debug, Clone)]
pub struct ColumnSchema<'a> {
    pub name: &'a str,
    pub pg_type_oid: Oid,
    pub is_pk: bool,
}

/// Optional fallback row for `UPDATE` ops carrying `(None, None)` pairs.
/// `None` means "no fallback available"; conversion will return
/// `ConversionError::UnchangedInNewTuple` on the first such column.
pub type UpdateFallback<'a> = Option<&'a [crate::Value<alloc::string::String, alloc::vec::Vec<u8>>]>;

/// Build the `Relation` message the parser needs before any data frame
/// for `schema.relation_oid` decodes.
pub fn relation_message(schema: &RelationSchema<'_>) -> LogicalReplicationMessage;

/// Encode a single `ChangesetOp` as a `LogicalReplicationMessage`.
///
/// `fallback` is consulted only for `Update` ops and only for columns
/// whose changeset pair is `(None, None)`. Non-Update ops ignore it.
pub fn op_to_message<T, S, B>(
    op: &ChangesetOp<'_, T, S, B>,
    schema: &RelationSchema<'_>,
    fallback: UpdateFallback<'_>,
) -> Result<LogicalReplicationMessage, ConversionError>
where
    S: AsRef<str>,
    B: AsRef<[u8]>;

/// Convenience: run a whole `DiffSet<ChangesetFormat, _, _, _>` through
/// `op_to_message` with a caller-supplied `schema_by_name` lookup.
/// Optional; only added if it does not force the caller to squash
/// their fallback logic into a closure. Deferred to a second pass.
```

Any function that takes a caller-owned fallback should not clone the values — take `&[Value<...>]` and index into it. `Vec` allocation belongs to the caller, not the adapter.

## Semantic contracts (per case)

| ChangesetOp variant | Input on wire | `LogicalReplicationMessage` emitted | Caller obligations |
|---|---|---|---|
| `Insert { values, .. }` | Full row image, one value per column. | `Insert { relation_id, tuple }` where `tuple` has one text-mode `ColumnData` per column, routed by `pg_type_oid`. | None. |
| `Delete { old_values, .. }` | Full old-row image, one value per column. | `Delete { relation_id, old_tuple, key_type: 'O' }`. | None. |
| `Update { values, .. }` all `(Some, Some)` | Full old + new pair per column (includes PK columns unchanged and changed non-PK columns). | `Update { relation_id, old_tuple: Some(...), new_tuple: ..., key_type: Some('O') }`. Both tuples fully populated. | None. |
| `Update { values, .. }` any `(None, None)` with fallback | Old + new pairs for changed columns and PK columns; `(None, None)` for unchanged non-PK columns. | Same message shape as above, with unchanged positions filled from the fallback row on **both** old and new sides. | Supply `fallback` = current row image indexed by column ordinal. |
| `Update { values, .. }` any `(None, None)` without fallback | Same as above. | `ConversionError::UnchangedInNewTuple { column_index }` on the first offending column. | Either supply a fallback or accept the error and skip the op. |

For every case above the `TupleData` follows the value-shape mapping table in the "Scope" section. Callers that want a different mapping (for example: encode `Value::Real` as binary IEEE 754 instead of text) can call `wire_value_to_column_data` directly and assemble their own tuples.

## Test plan

All tests live in `sqlite-diff-rs`'s test suite. The intent is to lock the semantic contracts above with tests that would fail on any drift.

### Unit tests (per variant)

1. `insert_maps_to_pgoutput_insert_with_full_tuple`.
   - Build a `ChangesetOp::Insert` with three columns: an `INT`, a `TEXT`, a `BYTEA`. Wire values: `Integer(7)`, `Text("paid")`, `Blob(vec![0xAB, 0xCD])`.
   - Supply schema with matching OIDs (`INT8`, `TEXT`, `BYTEA`).
   - Call `op_to_message`.
   - Assert message is `Insert { relation_id: SCHEMA_OID, tuple }` with three columns whose `data_type == b't'` and whose `data` bytes equal `b"7"`, `b"paid"`, `b"\\xabcd"` respectively.

2. `insert_bool_column_yields_t_or_f_text`.
   - Columns: `BOOL` mapped to `Value::Integer(1)` and `Value::Integer(0)`.
   - Assert emitted `ColumnData` bytes are `b"t"` and `b"f"` respectively.

3. `delete_maps_to_pgoutput_delete_with_full_old_tuple`.
   - Build a `ChangesetOp::Delete` with three-column old image.
   - Assert message is `Delete { relation_id, old_tuple, key_type: 'O' }` and old tuple bytes match.

4. `update_all_diffed_columns_maps_to_pgoutput_update_with_both_tuples`.
   - Build a `ChangesetOp::Update` where every column pair is `(Some(old), Some(new))` and old != new.
   - Assert `Update { old_tuple: Some(o), new_tuple: n, key_type: Some('O') }` and each side's `ColumnData` matches the source pair value.

5. `update_pk_unchanged_and_one_non_pk_changed_carries_pk_on_both_sides`.
   - PK is `(Some(5), Some(5))`, one non-PK is `(Some("pending"), Some("shipped"))`, one non-PK is `(None, None)`.
   - No fallback.
   - Expect `ConversionError::UnchangedInNewTuple { column_index }` where `column_index` is the unchanged non-PK column.

6. `update_with_fallback_fills_unchanged_columns_on_both_sides`.
   - Same UPDATE as case 5, but supply `fallback = &[Value::Integer(5), Value::Integer(100), Value::Text("pending")]`.
   - Expect message succeeds; unchanged column position carries `Text(b"100")` on both old and new tuples; the changed status column carries the pair values.

7. `update_pk_change_carries_old_pk_in_old_tuple_and_new_pk_in_new_tuple`.
   - PK `(Some(5), Some(6))`, non-PK columns `(None, None)`.
   - Fallback: `&[Value::Integer(5), Value::Integer(100), Value::Text("paid")]` (matches the pre-image state).
   - Assert old tuple carries `b"5"` at PK position and fallback values at non-PK positions; new tuple carries `b"6"` at PK position and fallback values at non-PK positions.

8. `arity_mismatch_op_wider_than_schema_errors`.
   - Insert op with 4 wire values, schema with 3 columns.
   - Assert `ConversionError::ArityMismatch { schema_arity: 3, op_arity: 4 }`.

9. `wire_value_null_maps_to_column_data_null_regardless_of_type_oid`.
   - Call `wire_value_to_column_data(&Value::Null, oid)` for every OID in `{INT8, TEXT, BOOL, BYTEA, TIMESTAMP}`.
   - Assert `data_type == b'n'` and `data` is empty every time.

10. `blob_with_non_bytea_oid_falls_through_as_binary_column_data`.
    - `wire_value_to_column_data(&Value::Blob(vec![1, 2, 3]), OID_TEXT)`.
    - Assert `data_type == b'b'` and `data` is the raw bytes. The forward parser rejects binary payloads for non-bytea columns, but that is a separate contract; the adapter's job is to route by target OID, not to validate.

11. `relation_message_builds_replica_identity_full_and_pk_flags`.
    - `RelationSchema` with three columns, first two are PK.
    - Assert `Relation { relation_id, namespace, relation_name, replica_identity: b'f', columns }` where `columns[0].flags == 1`, `columns[1].flags == 1`, `columns[2].flags == 0`.

### Round-trip property test

12. `roundtrip_encoded_message_parses_back_to_semantic_equal`.
    - For a fixed schema (INT PK, INT, TEXT) and for each variant (Insert, Update-all-diffed, Delete):
      - Build a `ChangesetOp` from a `proptest`-generated row image.
      - `op_to_message` -> `pg_walstream::encode_message` -> `Vec<u8>`.
      - Precede with the schema's `relation_message` -> encode -> feed to a fresh `LogicalReplicationParser` first, then feed the data bytes.
      - Assert the parser's output matches the original op semantically (relation id, column values decode back to the same wire values).

### Integration test (in `subql`, once the upstream lands)

13. `pg_sqlite_emu_dispatches_pkonly_update` (in `subql/src/pg_sqlite_emu/source.rs::tests` or a new integration test file).
    - Build a `PgSqliteEmuSource` on the fixed `orders(id INT PRIMARY KEY, amount INT, status TEXT)` schema.
    - `INSERT` id=1, amount=100, status='pending'; drain.
    - `UPDATE orders SET id = 2 WHERE id = 1`; drain.
    - Assert the drained event is `PgOutputEvent { kind: Update, table_id: <orders>, pk: 2, old: id=1 amount=100 status='pending', new: id=2 amount=100 status='pending' }`.
    - Register a subscription `SELECT * FROM orders WHERE amount = 100` before the update; assert it fires exactly once against the update event.

### Fuzz backlog

The subql fuzz harness (`fuzz_sqlite_pgoutput_e2e`) is the load-bearing consumer. It stays in `subql` and exercises the composite path (DML through diesel -> session -> changeset -> upstream adapter -> encode -> parse -> engine dispatch) on arbitrary inputs. Its crash directory (`fuzz/artifacts/fuzz_sqlite_pgoutput_e2e/`) already contains inputs; those replay under the regression tests in `subql/src/test_harnesses.rs::regression_tests` once the harness rewrite lands.

## Release and pinning

1. Land the module + tests on `sqlite-diff-rs` `main`. Version bump: `0.1.1 -> 0.1.2` (patch under Cargo's semver — the change is purely additive, all existing API is untouched, and the new module surface is gated behind the existing `pg-walstream` feature). Skip the middle-number bump; `0.2.0` would announce a break we do not have.
2. Publish `0.1.2` to `crates.io` (or cut a git tag if `subql` continues to pin by rev).
3. In `subql`:
   - Bump `sqlite-diff-rs = { version = "0.1", default-features = false }` in `Cargo.toml` (already caret-compatible; the new patch resolves automatically).
   - Delete the inline conversion helpers from `src/pg_sqlite_emu/source.rs` (`op_to_pgoutput`, `values_to_tuple`, `update_side_to_tuple`, `wire_to_column_data`, the `HexWriter` adapter, and the local `pg_type_oid_for_kind` if the caller can use it directly).
   - Keep `pg_type_oid_for_kind` in `subql`; it maps subql's `ScalarKind` and does not belong upstream.
   - Rebuild `TableMeta` around a `Vec<ColumnSchema>` produced from the PG catalog, hand off to `sqlite_diff_rs::pg_walstream_reverse::op_to_message` in the drain loop, keep the fallback-row lookup here.
4. Re-run the verification bar in `docs/refactor-cdc-event-handoff.md` before pushing.

## Open questions for review

- Module name. `pg_walstream_reverse` mirrors the forward `pg_walstream`; alternatives: `pg_walstream_encode`, `pgoutput_encode`, `to_pg_walstream`. Prefer the name that lines up with an eventual PR title and does not collide with the crate's `pg_walstream` re-exports.
- Should `op_to_message` return `LogicalReplicationMessage` or already-encoded `BytesMut`? The bytes form is more useful in practice but couples the module to `encode_message`. Returning the message keeps the split honest and lets callers pick their protocol version.
- Whether to include a stateful `ChangesetToPgOutput` struct that remembers which relations it has already announced (so the caller does not have to track `announced: HashSet<Oid>` themselves). Convenient but adds mutable state; leaning towards leaving it to the caller.
- Whether to bundle the fallback lookup into an `UpdateFallback` trait so callers with a live DB do not have to pre-materialise the row. Deferred until a second consumer asks.
