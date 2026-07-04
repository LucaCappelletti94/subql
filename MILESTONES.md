# MILESTONES

Deferred work tracked here, with the decisions and design context that have already been made so the next pass can pick up without re-deriving anything.

---

## Aggregate IVM v2

### Status

Deferred from v1. v1 delivers row-level patchsets: each subscription emits `Inserted` / `Updated` / `Deleted` notifications that the materializer assembles into a SQLite patchset. Per-group aggregate IVM (`GROUP BY`, `DISTINCT`, per-group `MIN` / `MAX`, `HAVING`) sits on per-group accumulator state and re-enters the queue once the row path is solid.

### Shape

Opt-in at engine construction. Without opt-in, a `GROUP BY` subscription registers but emits `NeedsReexecution { query_id }` on every relevant event (the same fallback used for unsupported `WHERE` predicates). With opt-in, it takes an in-process per-group accumulator path with a configurable per-subscription group cap that promotes a single over-cap subscription back to re-exec rather than failing the engine. `UPDATE` events that move a row across group keys produce two deltas (delete-from-old plus insert-into-new), driven by the existing dual-eval of `old_row` and `new_row`.

Scalar (no-`GROUP BY`) aggregate IVM for `SUM` / `AVG` / `COUNT` already ships. `MIN` / `MAX` and `COUNT(DISTINCT)` are rejected at registration today (see `src/compiler/sql_shape.rs`): both need per-group accumulator state, and `MIN` / `MAX` additionally needs targeted re-query on extremum removal.

### Architecturally excluded (do not add)

Captured so they do not drift back in by accident.

- Flink / RisingWave-style ordered per-group state for `MIN` / `MAX`. Overkill. Use pg_ivm-style targeted re-query instead.
- In-process IVM for `JOIN`s (Materialize-style differential dataflow). Multi-table joins route to re-exec, always.
- HLL-style approximate `COUNT(DISTINCT)`. Different semantics (append-only, no retractions). Would muddle the deterministic contract subql owes the materializer.
- Accumulator persistence to disk. Currently in-memory, rebuilt via re-execution on restart. A future optimisation, not a v1 concern.

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

### MySQL follow-on-insert: replace raw `LAST_INSERT_ID()` with diesel sugar

MySQL has no `RETURNING`, so the Pg/MariaDB `register_follow_insert` path does not
apply. The MySQL "follow the inserted row" story (`tests/follow_insert_mysql.rs`)
executes the insert, reads the DB-minted auto-increment key with a **raw**
`SELECT LAST_INSERT_ID()` query, then calls `follow_row`. It is raw SQL because
diesel exposes no sugar for it on MySQL (contrast SQLite's
`SqliteConnection::last_insert_rowid()`, `sqlite/connection/mod.rs`). Once diesel
grows a typed `last_insert_id` accessor (planned by the maintainer), swap the raw
`sql_query` in that test (and any docs) for the typed call.
