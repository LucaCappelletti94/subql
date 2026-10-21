# MILESTONES

Deferred work tracked here, with the decisions and design context that have already been made so the next pass can pick up without re-deriving anything.

---

## Aggregate IVM v2

### Status

Live folding aggregates use in-process scalar and per-group accumulators. Scalar and grouped `MIN` and `MAX` use targeted source reads. [`ReplayDetector`](REPLAY_STATE.md) maintains its admitted single-table results from retained inputs and checkpoints the native fold and candidate state needed for recovery.

### Shape

Live folds seed their accumulators from source reads and apply admitted row deltas, including group creation, removal and existing `HAVING` visibility. An `UPDATE` that changes a group key contributes to the old and new groups.

Replay operators retain complete dependency input bags, native fold accumulators and extrema candidates. Their versioned `CBOR` checkpoints bind this state to the compiled plan and dependent catalog fingerprints.

### Architecturally excluded (do not add)

Captured so they do not drift back in by accident.

- Ordered extrema candidate trees. Live extrema use targeted reads, and replay extrema use retained candidate bags.
- In-process `JOIN` maintenance. Multi-table joins require database re-execution.
- Approximate distinct counting. Its semantics require a separate contract.
- Persistence of live-only accumulators. The replay operator's persistence contract is defined in [REPLAY_STATE.md](REPLAY_STATE.md).

---

## Total single-table row re-execution

### Status

Deferred. The trait shape shipped, the decoding path did not. `Connector::execute_rows` is a required method, `Snapshot<T, C>` carries the checkpoint anchor, and `AutoResolvingEngine::snapshot(query_id)` bootstraps a captured query. But all three production connectors (`DieselConnector`, `PgDieselConnector`, `PgR2D2DieselConnector`) stub `execute_rows` with `unimplemented!()` in `src/reexec/connector.rs`. Only the in-tree test mocks decode rows. v1 resolves scalar `MIN` / `MAX` only.

### Why it stalled

Row-set decoding through diesel's `sql_query` wants the schema at compile time: each column needs its own typed accessor. A general "run this SQL, hand back `Vec<RowImage>`" path needs runtime-typed decoding the current diesel-backed connectors do not expose.

### What is already in place

- `row_set_delta(prev, next, pk_cols) -> RowSetDelta` (shipped, `src/row_set.rs`) turns two row sets into inserted / deleted / updated triples keyed by PK. This is the diff primitive the row path needs.
- `Snapshot<T, C>` + the `snapshot()` bootstrap give the checkpoint-anchored starting state.

### Resume path

Land a connector that decodes arbitrary row sets at runtime (raw libpq row access, or a dynamic decode layer over diesel), then wire a row-set displacement to: re-query rows, `row_set_delta` against the prior snapshot, emit `Inserted` / `Updated` / `Deleted`. Pairs naturally with the materializer's row-level patchset shape that v1 already targets.

---

## Concrete `AsyncConnector` implementation

### Status

Deferred. The `AsyncConnector` trait and `AsyncAutoResolvingEngine` shipped and are exercised by in-tree mocks (`MockAsyncConnector`, plus the throttle-test connectors). No production async connector exists. No `sqlx` or `diesel-async` dependency is in `Cargo.toml`.

### Design context already settled

- async-fn-in-trait with `+ Send` bounds on every method future, so it works on multi-threaded runtimes. A `!Send` consumer implements its own trait variant.
- Concurrent re-execution in `consumers_batch` runs through `FuturesUnordered` capped at `max_concurrent_reexecutions` (already wired on the async engine).
- Open choice: `sqlx` vs `diesel-async` for the first concrete impl. Picking one does not require a breaking bump, the trait surface is frozen.

---

## Other deferred items

Track future deferred work here as it gets pushed past v1.
