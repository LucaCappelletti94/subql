# CHANGELOG

All notable changes to subql are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to pre-1.0 semantic versioning (breaking changes are allowed in minor releases until 1.0).

## Unreleased

### Changed

- `AsyncAutoResolvingEngine::with_max_concurrent_reexecutions(usize)` now configures a **persistent** global concurrency cap on re-execution connector calls. Previously the cap applied per `consumers_batch` call only (consumed by `buffer_unordered`) and was ignored by per-event `consumers`. The cap is now honored by both flows and persists across calls via an `async_lock::Semaphore` stored on the engine. `cap = 0` is normalised to 1 to avoid a deadlock. Connetto-rs `Q5.5`.

### Added

- `AsyncAutoResolvingEngine::inflight() -> usize` reports the number of re-execution permits currently held (concurrent connector calls in flight). Returns 0 when no cap is configured.
- `AsyncAutoResolvingEngine::concurrency_cap() -> Option<usize>` reports the configured cap.
- `async-lock` dependency (runtime-agnostic `Semaphore`, `no_std + alloc`).
- `tracing::trace!` event on every permit acquire when the `observability` feature is enabled.
