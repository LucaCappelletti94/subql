# Upstream proposal: a connection-independent pgoutput to `ChangeEvent` decoder in `pg_walstream`

**Status**: proposal (upstream `pg_walstream` change, needed by subql Phase 2)
**Target crate**: `pg_walstream` (currently pinned at 0.7.0 in subql via crates.io)
**subql context**: `docs/handoff-cdc-ecosystem-convergence.md` sections 2, 7, and 8 step 2

## Summary

subql wants every Postgres CDC producer to yield the ecosystem type `pg_walstream::ChangeEvent`, so it can implement `CdcEvent` directly on `ChangeEvent` and feed `sqlite_diff_rs::DiffSetBuilder::digest`. This deletes subql's bespoke `PgOutputParser` and `PgOutputEvent`, which today re-implement relation caching and tuple assembly that `pg_walstream` already performs.

The blocker: `pg_walstream` 0.7.0 has no connection-independent, public way to turn pgoutput message bytes into a `ChangeEvent`. The assembly is a private method on the network-only `LogicalReplicationStream`, so the only way to obtain a `ChangeEvent` is to open a live replication connection and drive the high-level `EventStream`. Two of subql's three Postgres producers hold no such connection: the polling source drains a slot over a plain SQL connection, and the SQLite-backed emulator has offline bytes with no connection at all. The unconditional fuzz harness and unit tests are in the same position.

This document asks `pg_walstream` to lift the existing assembly into a small, connection-independent, feature-independent decoder. The change is a pure refactor. It moves logic that already exists, adds no new decode behavior, and lets `LogicalReplicationStream` delegate to the extracted code with identical results.

## Why the assembly is currently unreachable without a connection

The step that turns a parsed replication message into a `ChangeEvent` is `LogicalReplicationStream::convert_to_change_event`, a private method at `src/stream.rs:878`, together with its private helper `relation_metadata` at `src/stream.rs:1486`.

1. It lives on the connection-owning type, inside a network-gated module. `LogicalReplicationStream` is declared in the `stream` module, and that whole module is `#[cfg(any(feature = "libpq", feature = "rustls-tls"))]` (`src/lib.rs:157`, re-exported at `src/lib.rs:220`). The `connection` and `retry` modules are gated the same way. So the assembly compiles only when a network backend is enabled.

2. The only public doors to it own a live socket. The single public path that reaches `convert_to_change_event` is `EventStream::next_event()` and the `futures::Stream` impl. You obtain an `EventStream` only by constructing `LogicalReplicationStream::new(...)`, which opens and holds a `PgReplicationConnection` (`src/stream.rs:36`), then calling `.into_stream(...)`. There is no public, connection-free entry point.

## Why the coupling is incidental, not intrinsic

The conversion performs no socket I/O. Every field it touches is `self.state`, a `ReplicationState` that holds the relation cache plus LSN counters.

- Insert, Update, and Delete read the cached relation via `self.state.get_relation(...)` (and via `relation_metadata`, `src/stream.rs:1487`, which also reads only `self.state`), then call `tuple.into_row_data(relation)` (`src/stream.rs:953, 980, 1010`). Relation messages mutate the cache via `self.state.add_relation(...)`. Truncate reads names from `self.state`. Begin, Commit, the `Stream*` variants, Origin, Type, and the two-phase variants are stateless passthroughs built from the message fields.
- The read and the parse are already separate, connection-free steps. Raw bytes go through `LogicalReplicationParser::parse_wal_message_bytes` (`src/stream.rs:854`), which lives in the unconditional `protocol` module, and `convert_to_change_event` runs afterward on the already-decoded `StreamingReplicationMessage`.
- Every primitive the assembly uses is already public and unconditional: `ReplicationState::{new, add_relation, get_relation}` (`src/protocol.rs:574`), `RelationInfo::new` (`src/protocol.rs:456`), `TupleData::into_row_data` (`src/protocol.rs:307`), `LogicalReplicationParser` (`src/protocol.rs:671`), and `StreamingReplicationMessage` (`src/protocol.rs:513`). The `state` field is even declared `pub` on the stream (`src/stream.rs:38`).
- The maintainers already treat these methods as connection-independent. The test helper `create_test_stream` (`src/stream.rs:3941`) is documented as "for testing without a DB connection. Only safe for testing methods that don't touch `self.connection`." The conversion is exactly such a method.

The assembly is therefore a pure fold over `(relation cache, parsed message, lsn) -> Option<ChangeEvent>`. It is trapped behind a connection only because it is packaged as a private method on `LogicalReplicationStream` inside the network-gated `stream` module.

## Proposed change

Add the conversion to the unconditional `protocol` module as public API, in two layers.

### Layer 1: the pure conversion function

Move the body of `convert_to_change_event` (and `relation_metadata`) into a public free function in `protocol`, taking the relation cache by reference instead of `self`:

```rust
// in src/protocol.rs (unconditional, no_std + alloc)
pub fn message_to_change_event(
    state: &mut ReplicationState,
    message: StreamingReplicationMessage,
    lsn: XLogRecPtr,
) -> Result<Option<ChangeEvent>>;
```

Behavior is identical to today's method: Relation messages update `state` and return `Ok(None)` (or a `Relation` event on a detected schema change), data messages read `state` and return `Ok(Some(event))`, unknown relations log and return `Ok(None)`. The signature takes `StreamingReplicationMessage` (not the bare `LogicalReplicationMessage`) so the streaming transaction context is preserved unchanged.

### Layer 2: an ergonomic decoder wrapper

For consumers that only have bytes and want a one-call API, add a small owning struct that bundles a parser and a relation cache:

```rust
pub struct PgOutputDecoder {
    parser: LogicalReplicationParser,
    state: ReplicationState,
}

impl PgOutputDecoder {
    /// Protocol version matches `START_REPLICATION`'s `proto_version`.
    pub fn with_protocol_version(protocol_version: u32) -> Self;

    /// Decode one pgoutput logical-replication message body.
    /// `body` is the message after any transport framing has been
    /// stripped (no `'w'` XLogData header, no CopyData wrapper), the
    /// same contract as `LogicalReplicationParser::parse_wal_message`.
    pub fn decode_message(
        &mut self,
        body: impl Into<Bytes>,
        lsn: Lsn,
    ) -> Result<Option<ChangeEvent>>;

    /// Read-only access to the accumulated relation cache.
    pub fn state(&self) -> &ReplicationState;
}
```

`decode_message` parses `body` with `self.parser`, then calls `message_to_change_event(&mut self.state, msg, lsn.value())`. Both layers live in `protocol`, so they compile in the base `no_std + alloc` build with no network features. They reuse the existing error type `pg_walstream::Result` (`ReplicationError`, unconditional at `src/error.rs`), and the existing `chrono` helper `postgres_timestamp_to_chrono` used by the `Begin` and `Commit` arms.

### Delegation from `LogicalReplicationStream` (no behavior change)

`LogicalReplicationStream` keeps its `parser` and `state` fields exactly as they are and replaces the body of its private `convert_to_change_event` with a single delegating call:

```rust
fn convert_to_change_event(
    &mut self,
    message: StreamingReplicationMessage,
    lsn: XLogRecPtr,
) -> Result<Option<ChangeEvent>> {
    crate::protocol::message_to_change_event(&mut self.state, message, lsn)
}
```

The stream continues to own its `ReplicationState` for LSN tracking and feedback throttling (`last_received_lsn`, `last_flushed_lsn`, feedback counters), which are untouched by the conversion. This is the least invasive shape: no struct fields move, the hot path and the streaming lifecycle are unchanged, and the extracted function is exercised by the existing `EventStream` tests unchanged.

## Design considerations to settle in the PR

- `ReplicationState` bundles two concerns: the relation cache (`relations`) and the LSN and feedback counters. The conversion needs only the relation cache. Keeping `state: ReplicationState` on both the stream and `PgOutputDecoder` is fine and reuses the existing type. If a cleaner split is wanted later, the relation cache could become its own type that `ReplicationState` embeds, but that is not required for this change and would widen the diff.
- Transport framing stays the caller's responsibility. `decode_message` accepts a single pgoutput message body, matching `LogicalReplicationParser::parse_wal_message`. Callers strip the `'w'` XLogData header (25 bytes) for the replication transport, and the SQL polling path already receives bare message bodies from `pg_logical_slot_get_binary_changes`.
- Protocol version. `PgOutputDecoder::with_protocol_version` mirrors `LogicalReplicationParser::with_protocol_version`. Streaming transaction messages (`StreamStart` and friends) only appear at proto v2+, and the conversion already handles them, so the decoder is correct at any version the parser supports.
- Relation cache lifetime. A pgoutput byte stream is self-describing: a `Relation` message precedes the first data message for each table and repeats on schema change. So a decoder that starts empty and is fed a complete stream builds its own cache, exactly as the network stream does. Consumers that resume mid-stream must ensure a `Relation` message is delivered before the first data message, which Postgres guarantees at slot read start.
- `Truncate` carries multiple tables. `EventType::Truncate(Vec<Arc<str>>)` holds schema-qualified `full_name()` strings for every truncated relation in one message. This is a consumer concern (subql fans out to one CDC event per table), not part of this change.

## What this unblocks in subql (for reviewer context, not part of the pg_walstream change)

With the decoder available unconditionally, subql can:

- Implement `CdcEvent` directly on `pg_walstream::ChangeEvent`, decoding cells by column name against the subql catalog (`catalog_helpers::column_name` and `column_scalar_kind`), with `checkpoint()` bridging `ChangeEvent.lsn` to subql's `PgLsn`.
- Rewire `PgStreamingCdcSource` to yield `ChangeEvent`, either through `PgOutputDecoder` fed by the existing `XLogData` loop (keeps subql's current ack and status behavior, the smaller change) or through the high-level `EventStream` (larger rewrite).
- Rewire `PollingPgCdcSource` to run its SQL-drained message bodies through `PgOutputDecoder`.
- Reconcile `PgSqliteEmuSource`. Once the decoder exists it can keep decoding pgoutput bytes, or it can skip the encode step entirely and build `ChangeEvent`s directly from the SQLite changeset via `ChangeEvent::insert/update/delete` and `RowData::from_pairs`.
- Delete `src/wal/pgoutput.rs` (`PgOutputParser`, `PgOutputEvent`, `PgOutRowImage`, the private relation cache and wire-cell types) and the `PgOutputEvent`/`PgOutputParser` re-exports.

## Validation for the pg_walstream change

- The extracted `message_to_change_event` is covered by the existing `EventStream` and `convert_to_change_event` tests, since the stream now delegates to it. No behavior should change.
- Add a focused unit test for `PgOutputDecoder` that feeds a `Relation` message followed by `Insert`, `Update`, `Delete`, and `Truncate` bodies (built with the crate's own `pgoutput_encode::encode_message`) and asserts the emitted `ChangeEvent`s match the network path for the same bytes.
- Confirm the base build compiles: `cargo build --no-default-features --features alloc` (or the crate's minimal `no_std + alloc` configuration) should include `protocol::PgOutputDecoder` with no network features enabled.

## Open questions

- Naming: `PgOutputDecoder` versus `ChangeEventDecoder` versus `LogicalReplicationDecoder`. The output is a `ChangeEvent`, but the input is specifically pgoutput, so `PgOutputDecoder` reads clearest.
- Should `decode_message` also accept a batch (`decode_all(&mut self, bodies)`), or is one-message-per-call the right primitive. One per call mirrors the internal method exactly and keeps LSN stamping per message, so it is the recommended primitive, with batching left to callers.
- Whether to expose `&mut ReplicationState` access on the decoder for advanced resume scenarios, or keep it read-only via `state()`.
