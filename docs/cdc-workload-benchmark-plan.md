# Benchmark plan: polling vs. push across realistic workloads

## Context

The one-shot benchmark in `tests/polling_vs_push_benchmark.rs` measured
single-INSERT latency for both transports and produced clean
wire-bound numbers (push 4.6 ms median; poll @ 100 ms = 57.9 ms;
poll @ 1 s = 554 ms). That settled the headline claim. It did *not*
settle the broader question: across realistic CDC workloads, is push
always preferable to polling, or does polling have regimes where it
is acceptable (or better) on metrics other than per-event latency?

This document is the plan for the next round of empirical work.
Three asks from the user, each addressed in its own section below:

1. A multi-phase plan covering distinct schemas and scenarios.
2. Which polling infrastructure belongs in the crate, and whether
   any of it has value as ground truth for catching parser /
   `pg_walstream` stream regressions.
3. Where the sophisticated benchmark code should live, given the
   tests will be too heavy for `cargo test` and produce
   documentation rather than assertions.

## (1) Multi-phase plan

Each phase produces empirical numbers and either confirms or
recharacterizes a claim. Subsequent phases assume prior phases'
numbers held. Phases are listed in increasing complexity / cost so
we get useful data quickly, even if we stop early.

### Phase 1 — Append-only baseline (single table)

**Schema:** one `events(id BIGSERIAL PRIMARY KEY, kind TEXT, payload TEXT, ts TIMESTAMPTZ)` table. Append-only.

**Workloads:**
- **W1.1 — Steady single-row inserts.** 1 INSERT/sec for 30 s. Measures latency in the lowest-stress regime.
- **W1.2 — Sustained high-rate inserts.** 500 INSERTs/sec for 5 s. Measures throughput under load.
- **W1.3 — Idle-then-burst.** 30 s idle, then 1 INSERT. Polling's worst case.

**Polling intervals:** 10 ms, 100 ms, 1 s.

**Goal:** sanity-check the new measurement infrastructure against the prior one-shot benchmark. The numbers should reproduce the prior table; if they don't, the new harness has a bug.

**Expected outcome:** push wins every cell. Push p99 ~ 10 ms regardless of workload; polling p99 ≈ `P + wire_rtt`.

### Phase 2 — Multi-table schema with I/U/D mix

**Schema:** the e-commerce sketch used by the benchmark example:

```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    last_login_at TIMESTAMPTZ
);
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    status TEXT NOT NULL,
    total_cents BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_name TEXT NOT NULL,
    quantity INT NOT NULL,
    price_cents BIGINT NOT NULL
);
```

All three tables get `REPLICA IDENTITY FULL`.

**Workloads:**
- **W2.1 — Mixed I/U/D, steady.** 1 event/sec rotating through INSERT/UPDATE/DELETE on the same row set.
- **W2.2 — Order-and-items transaction.** Per commit: 1 row to `orders` + 3 rows to `order_items`. Tests how each transport handles per-commit batches.
- **W2.3 — UPDATE-heavy.** 100 UPDATEs/sec on the `users` table. Exercises the parser's UPDATE path (which is where most CDC bugs hide).

**Goal:** validate that the polling-vs-push verdict holds when the operation mix isn't pure INSERTs. UPDATE/DELETE produce more WAL bytes per event; if polling amortizes drain RTT better on large WAL batches, this is where it shows up.

**Expected outcome:** push still wins on latency. On throughput, may tie at high rates because both transports become bottlenecked by parse cost rather than transport choice.

### Phase 3 — Bursty + transactional workloads

**Schema:** same as Phase 2.

**Workloads:**
- **W3.1 — Bursty traffic.** 100 events in 100 ms, then 2 s quiet. Repeat 5 times. Tests catch-up behavior.
- **W3.2 — Large transactions.** 1 commit with 1000 rows. Tests whether push delivers events as they arrive in WAL (per-row) or queues until COMMIT, and how polling handles a large drain.
- **W3.3 — Many small transactions.** 1000 commits with 1 row each over 10 s. Tests per-transaction overhead.

**Goal:** find the regimes where the transports diverge from the "wire + P/2" model. Large transactions are particularly interesting: pgoutput delivers events at COMMIT time, not per-row, so the FIRST event of a large transaction has additional latency proportional to commit-time WAL flush size. Both transports inherit this.

**Expected outcome:** push wins on per-event latency; large-transaction first-event latency is similar for both because the bottleneck is upstream of the transport.

### Phase 4 — Adversarial / operational regimes

**Workloads:**
- **W4.1 — Long idle.** 5 min idle, then 1 event. Tests connection survival (push relies on the periodic pump; polling can sleep but must still handle reconnect on a TCP-killed connection).
- **W4.2 — Slow consumer.** Consumer takes 100 ms per event. Tests back-pressure semantics. Push back-pressures via TCP; polling back-pressures by skipping polls.
- **W4.3 — Server WAL retention.** Run a 5-minute workload with low ack frequency. Measure end-of-run `pg_replication_slots.confirmed_flush_lsn` lag.
- **W4.4 — Connection-flap resilience.** Kill the TCP connection mid-workload (network namespace drop). Measure recovery time for each transport.

**Goal:** test the operational dimensions that don't show up in raw latency. Push holds a long-lived TCP connection; polling reconnects each query (or holds connection pool). These have different failure modes.

**Expected outcome:** push has lower latency but higher connection-state sensitivity. Polling has higher latency but degrades more gracefully under network flap. This phase produces the most nuanced findings.

### Phase 5 — Schema-size sweep

**Schemas:**
- **S5.1 — Wide rows.** 50-column table with mixed text / numeric / timestamp / JSONB. Tests parser cost per event.
- **S5.2 — Many tables.** Pagila (15 tables, DVD rental) with realistic write patterns. Tests publication / relation-cache behavior.
- **S5.3 — Append-only audit log + reads.** Append-only audit table + occasional UPDATE-heavy lookup table.

**Goal:** test whether schema complexity changes the verdict. The pgoutput protocol emits Relation messages on first contact with each table — does that one-time cost dominate any short workload?

**Expected outcome:** schema affects absolute latency (more columns = more parse time) but does not change the relative ranking of transports.

### Recommended sequencing

Run Phase 1 first to validate the harness. If numbers reproduce the prior benchmark, proceed to Phase 2. If Phase 2 shows push winning on every cell, Phase 3 is optional (we likely know the verdict). Phase 4 is the most interesting phase from an engineering standpoint because it might surface operational trade-offs that nuance the architectural claim. Phase 5 is the longest tail; do it only if Phase 4 reveals something worth investigating across schemas.

If we want to be conservative on cost: **Phase 1 + Phase 4** (~1 hour total wall-clock) gives us the broadest informational return. The middle phases mostly confirm Phase 1's findings under variations.

## (2) Which polling infrastructure belongs in the crate

The current repo has polling-shaped code in three places. Each warrants a different treatment.

### Inventory

| Location | Code | Status today |
| --- | --- | --- |
| `tests/common/mod.rs` | `drain_slot()` (wal2json), `drain_pgoutput_slot()` (binary) | Test helpers; used by `tests/pgoutput_e2e.rs` and `tests/eviction_e2e.rs` to drain slots in side connections |
| `tests/polling_vs_push_benchmark.rs` | Polling receiver task with `pg_walstream` + ticker loop | Benchmark only; carries a "DO NOT LIFT INTO LIBRARY" banner |
| `src/wal/pg_streaming.rs` | (push code only) | The production CDC intake — pure push, no polling |

### Where polling has legitimate value

The `tests/common/mod.rs` drainers exist for a non-controversial reason: they validate that the parser correctly handles real-PG-emitted bytes. The polling shape (drive DML, then drain via `pg_logical_slot_get_*_changes`) is the *cheapest way to materialize known WAL bytes for parser tests*. Without it, parser tests would either be on synthetic byte fixtures (brittle when PG protocol drifts) or require the streaming source on the test path (couples parser tests to streaming-source bugs).

This is a legitimate use of polling. The `no-polling-cdc` rule was specifically about *CDC intake transport for production*, not *test apparatus*.

### Where polling could earn its keep as ground truth

**Differential equivalence testing.** Both transports consume the same source-side bytes (PG's WAL via pgoutput). For any workload, they should observe the same events in the same order with the same fields. If they diverge, exactly one of the following is true:

1. The push transport is missing or reordering events (subql bug).
2. The polling-side drain is missing or reordering events (test apparatus bug).
3. PG's behavior changed between version / wal2json version / pgoutput proto_version (environment drift).

This is a real, useful regression invariant. Concrete shape:

```rust
// Pseudocode for tests/cdc_equivalence.rs (proposed).
#[test]
#[ignore = "requires Docker"]
fn push_and_poll_observe_identical_event_streams() {
    let container = pg_with_wal2json();
    // Two separate slots, two separate publications, same DML stream.
    let slot_push = setup_slot(...);
    let slot_poll = setup_slot(...);
    let dml = pg_connect(port);

    // Drive a deterministic DML stream against the table.
    spawn_dml_burst(dml, mixed_workload(seed=42));

    // Drain both. Expected: same events, same order, same field values.
    let push_events = drain_via_push(slot_push).await;
    let poll_events = drain_via_polling(slot_poll, interval=10ms).await;

    assert_eq!(
        canonicalize(push_events),
        canonicalize(poll_events),
        "push and poll diverged on the same WAL stream — \
         exactly one of: push parser bug / poll harness bug / PG drift",
    );
}
```

This test catches three classes of regression that no single-transport test catches:
- pgoutput parser bugs that affect only certain message shapes.
- `pg_walstream` native-backend stream framing bugs (events dropped, reordered, duplicated).
- `pg_walstream` git-rev drift (we pin a specific upstream rev; a future bump might subtly change behavior).

The cost is minimal: ~150 LOC of equivalence test using infrastructure we already have (the wal2json drain helper) + the existing push source. **I recommend we add this as a permanent regression test.** It belongs in `tests/`, not in `examples/`, because it's an assertion, not a benchmark.

### Where polling stays out

The polling code currently in `tests/polling_vs_push_benchmark.rs` is benchmark-only: it exists to measure latency, not to validate correctness. It should stay isolated to benchmarks (moving to `examples/` per ask 3) and continue to carry the "DO NOT LIFT" banner. Nothing in the production library surface should reference it.

### Net split

| Code | Lives where | Purpose |
| --- | --- | --- |
| `drain_slot()` / `drain_pgoutput_slot()` in `tests/common/` | Stays | Parser test apparatus |
| Differential equivalence test | NEW in `tests/` | Regression invariant: push and poll observe same stream |
| Polling receiver in current `polling_vs_push_benchmark.rs` | MOVES to `examples/` | Benchmark subject, output only |
| Workload-matrix benchmarks | NEW in `examples/` | Benchmark subjects, output only |
| Any polling code in `src/` | Forbidden | (no-polling-cdc rule) |

## (3) Location: `examples/`, with a shared helper module

The new benchmark suite has properties that make `tests/` the wrong home:

- **Long-running.** A full Phase 4 sweep is ~1 hour of wall-clock. `cargo test --test xxx -- --ignored` is awkward for this. `cargo run --example xxx --release` is the canonical Rust pattern for runnable measurement programs.
- **Output-not-assertions.** The benchmarks produce numbers we paste into docs. There are no fail-the-build assertions to make; the existing one-shot benchmark already covers the load-bearing "push beats poll @ 100ms" claim.
- **Heavy dependencies on PG container + pgoutput parser + `pg_walstream`.** Putting these under `cargo test` slows the default test run.

**Proposed layout:**

```
examples/
  cdc_bench_common/
    mod.rs            # PG container, schema setup, workload primitives,
                      # measurement primitives. Polling implementations
                      # live here (one place, big banner).
  phase1_baseline.rs       # Phase 1 workloads (W1.1, W1.2, W1.3)
  phase2_mixed_iud.rs      # Phase 2 (W2.x)
  phase3_bursty.rs         # Phase 3 (W3.x)
  phase4_adversarial.rs    # Phase 4 (W4.x) — recommended priority
  phase5_schema_sweep.rs   # Phase 5 (S5.x)
```

Each example file is its own cargo target (`cargo run --example phase1_baseline --features pg-streaming`). The `cdc_bench_common/` module is shared via the `#[path = ...]` attribute (standard cargo example pattern):

```rust
// examples/phase1_baseline.rs
#[path = "cdc_bench_common/mod.rs"]
mod common;

fn main() { common::run_phase1(); }
```

**Cargo.toml** gains one new section per example (none today):

```toml
[[example]]
name = "phase1_baseline"
required-features = ["pg-streaming"]
# etc.
```

**Output format:** each example prints a Markdown-friendly table to stdout. The user redirects to `docs/benchmarks/phase1-2026-06-15.md` to capture the run. The first table in each example summarizes findings; subsequent tables break out per-workload detail.

**Why not `benches/`:**
- `cargo bench` defaults to criterion-style microbenchmarks (statistical analysis of a small operation under many iterations). Our workloads are big and slow; criterion's variance machinery doesn't apply.
- Bench harness convention assumes nightly Rust historically; MSRV concerns.
- `benches/` is conventionally for "is this hot loop faster than before"; ours is "does this workload pattern produce different operational behavior."

**Why not a separate crate:**
- One more workspace member to maintain.
- Cargo's example mechanism already provides the isolation we need.
- Pulling subql + `pg_walstream` + diesel via a sibling crate doubles the dependency-resolution surface for nothing.

## Pitfalls and what I'd flag in advance

- **The Phase 4 long-idle test (`W4.1`)** takes ≥5 minutes by definition. Don't bundle it into the default `cargo run --example phase4_adversarial`; gate it behind a sub-command argument or env var.
- **Network-flap test (`W4.4`)** requires either `tc` netem inside the container or a packet-drop sidecar. May require Docker capabilities our default test container doesn't have. Document the limitation; treat as optional.
- **Pagila in Phase 5** has 15 tables and substantial sample data; downloading it inflates first-run time. Cache the Docker image with pagila preloaded if Phase 5 happens.
- **The differential equivalence test (in `tests/`)** must be tolerant of timing-dependent reordering across separate slots. If two slots see commits at slightly different LSNs (they shouldn't on the same TCP connection, but if a worker scheduling difference shows up), we'd need to dedup / re-sort canonically. Plan for this in the test design.
- **Output format drift.** If the Markdown tables in examples are paste-into-docs, we need stable column ordering and stable rounding. Build a small `LatencyStats::to_markdown_row()` helper and use it everywhere.
- **PG version stability.** All numbers are PG 16 specific. If we ever bump to PG 17 or 18, regenerate the benchmark output as part of the version bump.

## Verification plan

Per phase:

1. Run the example. Confirm Docker is reachable.
2. Pipe output to `docs/benchmarks/phase{N}-{date}.md`.
3. Update the synthesis paragraph in the relevant benchmark doc under `docs/benchmarks/` to point at the new tables.
4. If the differential equivalence test (proposed) was added: run `cargo test --test cdc_equivalence --features pg-streaming -- --ignored`.
5. Confirm all existing tests still pass (no regression on the streaming source).

The "I'm confident the verdict holds" criterion across phases: push wins on per-event median latency in every cell of every phase. Push may tie on throughput-only metrics for sustained high-rate workloads; that's fine. The architectural claim is "push delivers events at the wire RTT floor regardless of cadence" — if any phase falsifies that, we have a real engineering finding.

## Open questions for the user

1. **Which phases to run first?** Phase 1 is the cheapest and most likely to reproduce prior numbers. Phase 4 is the most interesting from an operational-honesty standpoint. Are you OK with "Phase 1 first to validate the harness, then Phase 4 for the real findings, then decide about Phase 2/3/5 based on what we see"?
2. **Differential equivalence test in `tests/`** — yes/no? It's ~150 LOC, runs in <30s against Docker, catches a class of regression nothing else catches. Strong recommend.
3. **Pagila vs hand-rolled e-commerce in Phase 2:** the hand-rolled 3-table schema is smaller and more legible in benchmark output. Pagila is recognizable to outside readers. Either works; pick one.
4. **CI integration of any of this:** none of the example benchmarks should run in CI (too slow). The differential equivalence test could run in CI under `--features pg-streaming -- --ignored`. Should it? Probably yes — it's a high-value regression catcher and runs in ~30s.

If you sign off on the structure, I'd start by:
- Creating `examples/cdc_bench_common/mod.rs` with the shared infrastructure.
- Implementing `examples/phase1_baseline.rs` end-to-end and running it to validate the harness reproduces the prior numbers.
- Then asking what comes next.
