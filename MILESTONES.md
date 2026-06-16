# MILESTONES

Deferred work tracked here, with the decisions and design context that have already been made so the next pass can pick up without re-deriving anything.

---

## Aggregate IVM v2

### Status

Deferred from v1. v1 focuses on row-level patchset / changeset delivery to the frontend, where each subscription emits `Inserted` / `Updated` / `Deleted` row notifications that the materializer assembles into a SQLite patchset. Aggregate work re-enters the queue once that row path is solid.

### Why

The frontend's primary delivery shape is row-level patchsets and changesets. Aggregates are an extension of that pipeline, layered on top of the per-group accumulator state introduced by `GROUP BY`. Landing the row path cleanly before adding per-group state lets the materializer story stabilise first.

### `GROUP BY` design that landed before deferral

`GROUP BY` is the foundation for the rest of aggregate IVM v2 work (`DISTINCT`, `MIN` / `MAX`, `HAVING` fast-path all sit on top of per-group accumulator state). The following decisions are committed and should not be re-litigated when the work resumes.

**Architecture: opt-in at engine construction.** Without opting in, subscriptions that carry a `GROUP BY` clause register successfully but every CDC event on the involved tables emits `NeedsReexecution { query_id }`. Same code path already used for unsupported `WHERE` predicates. With opt-in, subscriptions take the in-process per-group accumulator path.

Builder API:

```rust
let engine = SubscriptionEngine::builder(catalog, ids)
    .with_max_subscriptions(1_000, EvictionPolicy::EvictOldest)
    .with_group_by_ivm(GroupByConfig {
        max_groups_per_subscription: 10_000,
        on_overflow: OverflowPolicy::PromoteToReexec,
    })
    .build()?;
```

**MVP shape: multiple bare columns.** `GROUP BY tenant_id, status, day` works in-process. Expressions or function calls in the `GROUP BY` clause (`GROUP BY UPPER(status)`, `GROUP BY DATE_TRUNC('day', created_at)`) are detected at registration, marked IVM-incompatible, and routed to re-exec without ambiguity. `GroupKey = SmallVec<[Cell; 2]>` so 1 or 2 column keys stay on the stack.

**Overflow: promote that one subscription to re-exec mid-life.** When a subscription's group count crosses `max_groups_per_subscription`, subql drops that subscription's per-group state, flips it to re-exec, and emits `NeedsReexecution { query_id }` on every relevant event from then on. Other `GROUP BY` subscriptions on the same engine keep their IVM state untouched. Each promotion is logged via `tracing` at warn level with structured fields (`sub_id`, `query_id`, `group_count_at_promote`, `cap`, `reason`) and exposed as a counter such as `subql_group_by_promoted_total{reason="cap_exceeded"}` so operators can alert.

**Group lifecycle.** Group entries are created on the first matching `INSERT`, removed when their witness count returns to zero. `UPDATE` events that change a `GROUP BY` column key produce two deltas (delete-from-old-group + insert-into-new-group), driven by the existing dual-eval of `old_row` and `new_row`.

### Open design points to revisit on resumption

- **Per-event emission shape.** Choice between flat (`Vec<(SubId, GroupKey, AggDelta)>`) and nested (`Vec<(SubId, Vec<(GroupKey, AggDelta)>)>`). Affects how multi-aggregate subscriptions (`SELECT COUNT(*), SUM(amount) FROM ... GROUP BY status`) compose, and how the materializer iterates.
- **`install` API extension for grouped bootstrap.** Today `install(query_id, Cell)` installs a scalar. For `GROUP BY` the materializer's bootstrap query returns `Vec<(GroupKey, Cell)>` per aggregate. New entry point needed, likely `install_grouped(query_id, Vec<(GroupKey, Vec<Cell>)>)`.
- **`NeedsReexecution` group scoping.** Should the trigger optionally carry a `GroupKey` so the materializer can re-execute only the affected group? Required for the `MIN` / `MAX` path below.
- **Multiple aggregates per subscription under `GROUP BY`.** Verify that the existing `QueryProjection` composes with per-group state without duplicating the group map per aggregate.
- **Reject vs promote on `GROUP BY ROLLUP` / `CUBE` / `GROUPING SETS`.** Out of scope for the MVP. Decide between rejecting registration or silent promotion to re-exec.

### Aggregate IVM v2 features that sit on top of `GROUP BY`

These are deferred together with `GROUP BY` since they all require per-group accumulator state. References point to the connetto-rs `open-questions.md` master index.

- **`DISTINCT` aggregates (Q5.1b).** Frequency map per group, RisingWave-style shared dedup state across aggregate calls on the same column. Currently rejected at parse time with `"COUNT(DISTINCT ...) not supported"` in `src/compiler/sql_shape.rs`.
- **`MIN` / `MAX` IVM (Q5.1).** pg_ivm-style targeted group re-query on extremum delete. Subql tracks per-group extremum and emits `NeedsReexecution { query_id, group_key }` when the extremum is removed. The actual SQL re-query stays on the materializer side per the boundary in `10-subscription-materializer.md`.
- **`HAVING` fast-path (Q5.2).** Post-aggregate predicate eval against accumulator state. Reuses the existing predicate VM, routes anything outside the fast path to re-exec.

### Architecturally excluded (do not add)

Captured here so they do not drift back in by accident.

- Flink / RisingWave-style ordered per-group state for `MIN` / `MAX`. Overkill. The connetto-rs doc prescribes pg_ivm targeted re-query instead.
- In-process IVM for `JOIN`s (Materialize-style differential dataflow). Multi-table joins route to re-exec, always. (Q5.3)
- HLL-style approximate `COUNT(DISTINCT)`. Different semantics (append-only, no retractions). Would muddle the deterministic contract subql owes the materializer.
- Accumulator persistence to disk. (Q5.4) Currently in-memory, rebuilt via re-execution on restart. A future optimisation, not a v1 concern.

---

## Other deferred items

Track future deferred work here as it gets pushed past v1.
