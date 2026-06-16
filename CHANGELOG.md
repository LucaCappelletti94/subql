# CHANGELOG

All notable changes to subql are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to pre-1.0 semantic versioning (breaking changes are allowed in minor releases until 1.0).

## Unreleased

### Changed

- `AsyncAutoResolvingEngine::with_max_concurrent_reexecutions(usize)` now configures a **persistent** global concurrency cap on re-execution connector calls. Previously the cap applied per `consumers_batch` call only (consumed by `buffer_unordered`) and was ignored by per-event `consumers`. The cap is now honored by both flows and persists across calls via an `async_lock::Semaphore` stored on the engine. `cap = 0` is normalised to 1 to avoid a deadlock. Connetto-rs `Q5.5`.

### Added

- Per-`(session, subscription)` resume cursor on `SubscriptionEngine` for connetto-rs `Q6.4`. Materializer-pushed model: explicit `advance_cursor(session_id, sub_id, checkpoint)` after each successful dispatch. Monotonic by default (a rewind returns `AdvanceCursorError::NonMonotonic` without mutating); `force_set_cursor(...)` is the snapshot-bootstrap / recovery escape hatch. Reads via `cursor_for(...)` and `cursors_for_session(...)`. Explicit removal via `drop_cursor(...)`. Cleanup is automatic on `unregister_session(...)` (drops every cursor for that session) and `unregister_subscription(...)` (drops every cursor for that subscription across every session). Cursors are stored as `OpaqueCheckpoint` so the engine stays free of a struct-level `C: Checkpoint` generic; the materializer serialises its concrete checkpoint type at the boundary. In-memory only; on engine restart every client gets a full re-sync on reconnect, matching `Q5.4`.
- `AdvanceCursorError::NonMonotonic { previous, attempted }` returned by `advance_cursor` on a rewind attempt.
- `AsyncAutoResolvingEngine::inflight() -> usize` reports the number of re-execution permits currently held (concurrent connector calls in flight). Returns 0 when no cap is configured.
- `AsyncAutoResolvingEngine::concurrency_cap() -> Option<usize>` reports the configured cap.
- `async-lock` dependency (runtime-agnostic `Semaphore`, `no_std + alloc`).
- `tracing::trace!` event on every permit acquire when the `observability` feature is enabled.
