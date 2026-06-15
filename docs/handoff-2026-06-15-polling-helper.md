# Handoff: CDC workload benchmark scaffolding (paused mid-plan, 2026-06-15)

This document is the context-handoff for a future session picking up
the polling-helper + workload-benchmark work mid-stream. The previous
session ran out of context at ~97% with 4 of 10 planned steps done.

## Where to start

Read in order:

1. **This document** for the current state.
2. **`docs/cdc-workload-benchmark-plan.md`** for the overall plan
   structure (5 benchmark phases, location decisions, schema choices).
3. **`/home/luca/.claude/plans/start-planning-to-1-purrfect-bubble.md`**
   for the 10-step implementation plan with locked-in design
   decisions.
4. Memory entries (auto-loaded): **`no-polling-cdc`** (policy, just
   updated), **`polling-helper`** (status of the new type),
   **`pg-streaming-status`** (push-side status).

## What landed in the prior session

| Step | Status | Verification |
| --- | --- | --- |
| 1 | DONE | `events_received() -> u64` on `PgStreamingCdcSource`; e2e test `events_received_counter_tracks_pushed_events` passes |
| 2 | DONE | `src/polling/mod.rs` skeleton (folded into Step 3) |
| 3 | DONE | `PollingPgCdcSource` fully implemented; `tests/polling_smoke.rs` passes (1 INSERT, counters tick) |
| 4 | DONE | `tests/cdc_equivalence.rs` passes; TDD-failure-demoed (dropped every 3rd event, watched test catch, restored) |
| 5 | DONE | `examples/cdc_bench_common/mod.rs` shared infrastructure (3-table schema, Phase 1 workloads, `LatencyStats`, generic `spawn_event_receiver`, Markdown helpers) |
| 6 | DONE | `examples/phase1_baseline.rs` end-to-end; all 4 W1.1 cells reproduce prior one-shot numbers within ±14% (push 3.9 vs 4.6 / poll@10 5.3 vs 5.5 / poll@100 50.8 vs 57.9 / poll@1000 519.2 vs 554.7 ms median). Captured run at `docs/benchmarks/phase1-2026-06-15.md`. |
| 7 | DONE | `examples/phase{2..5}_*.rs` scaffolds with `todo!()` workload bodies; `main()` prints planned workloads; all four `[[example]]` entries in `Cargo.toml`. |
| 8 | DONE | `tests/polling_vs_push_benchmark.rs` refactored to use `PollingPgCdcSource`. Push beats poll@100ms/1s assertion still holds (push 3.9 ms median, poll@1000ms 533.8 ms median). |
| 9 | DONE-EARLY | Memory rule rewrite landed during handoff prep (previously planned as Step 9; landed early because the stale rule would have misled this very handoff). `polling-helper.md` and `pg-streaming-status.md` updated 2026-06-15 to reflect the benchmark harness existing and the refactored polling benchmark. |
| 10 | DONE | Full sweep: 942 lib tests + 20 doctests + 8 pg_streaming_e2e + 2 pgoutput_e2e + 5 eviction_e2e + 1 polling_smoke + 1 cdc_equivalence + 1 polling_vs_push_benchmark all green; phase1 example PASS, phase{2..5} scaffolds run; clippy clean, fmt clean, wasm clean. |

## Library state (push + polling, what's actually shipped)

Both transports implement `subql::CdcSource` and are interchangeable
from generic benchmark / equivalence-test code.

### Push: `subql::PgStreamingCdcSource<DB>`

Existing since 2026-06-15 (this work cycle's prior phase). Gained
`events_received() -> u64` counter in Step 1 for symmetry with
polling. Full observability surface:

- `status_updates_sent() -> u64`
- `events_received() -> u64` (NEW)
- `task_exited() -> bool`
- `task_exited_handle() -> Arc<AtomicBool>`

### Poll: `subql::polling::PollingPgCdcSource<DB>`

New 2026-06-15. Same `pg-streaming` feature, no new feature flag (the
user explicitly chose against gating). Drains
`pg_logical_slot_get_binary_changes` via `tokio::time::interval`,
parses through `PgOutputParser`, pushes typed events to bounded mpsc.
Mirrors structural pattern of `streaming_task`: drop guard, oneshot
shutdown, abort backstop, bounded back-pressure.

**`ack` is intentionally a no-op** — `pg_logical_slot_get_binary_changes`
auto-advances `confirmed_flush_lsn` on drain. Document this for any
consumer that cares about at-least-once.

Observability surface (symmetric with push + polling-specific):

- `events_received() -> u64` (symmetric with push)
- `task_exited()` + `task_exited_handle()` (symmetric)
- `polls_issued() -> u64` (polling-only)
- `empty_polls_observed() -> u64` (polling-only; cost-of-polling metric)
- `average_drain_batch_size() -> f64` (polling-only)

### Trait

`subql::CdcSource` — unchanged from prior work. The polling helper
implementing it means the trait now has two impls, which has already
flushed out one issue: `PgOutputParser::Checkpoint = NoCheckpoint`
means polling events get `None` for their checkpoint (no per-event
LSN available on the polling path; the LSN lives in the XLogData
frame header that polling doesn't see). Push events get
`Some(PgLsn)`. The equivalence test's canonicalizer strips the
checkpoint specifically because of this.

## What's next: Step 7 (Phase 2-5 scaffolds)

Steps 5 + 6 landed 2026-06-15 (second session).
`examples/cdc_bench_common/mod.rs` shipped with the 3-table e-commerce
schema, Phase 1 workload primitives (`drive_inserts`,
`drive_idle_then_burst`), `LatencyStats`, generic
`spawn_event_receiver<S: CdcSource<Checkpoint = PgLsn>>`, and Markdown
helpers. `examples/phase1_baseline.rs` reproduces prior numbers within
±14% on every W1.1 cell (see `docs/benchmarks/phase1-2026-06-15.md`).

The remaining steps:

- **Step 7** — Scaffold `examples/phase{2..5}_*.rs` with `todo!()`
  workload bodies. Each file `#[path = "cdc_bench_common/mod.rs"]
  mod common;` and registers its phase's workloads. Add the four
  `[[example]]` blocks back to `Cargo.toml`. The phase 2-5
  implementations land in follow-up work.
- **Step 8** — Refactor `tests/polling_vs_push_benchmark.rs` to use
  `subql::polling::PollingPgCdcSource` instead of inline hand-rolled
  polling. Should shrink from ~430 LOC to ~150 LOC. The "push beats
  poll @ 100ms / 1s in the median" assertion must still hold.
- **Step 10** — Full sweep: `cargo test --lib`, doctests, all e2e
  tests under `--features pg-streaming -- --ignored`, the refactored
  benchmark, the Phase 1 example, clippy, fmt, wasm.

### Observed in Phase 1 that may be worth investigating later

`W1.3` push median is 29.8 ms, much higher than the wire-RTT floor.
The hypothesis: with a 3 s idle between bursts, PG's wal sender goes
quiet; when the next insert lands, the wake-up path adds ~25 ms. Not
a harness bug (it reproduces across runs and only on the push side
after long idle). May warrant a periodic "keep the sender warm"
strategy or, more honestly, an explicit note in the user-facing docs.
Not blocking; flag for Phase 4 if it persists there.

`W1.2` push median 13.9 ms is also higher than W1.1's 3.9 ms — likely
back-pressure on the 1024-deep bounded `mpsc` channel when 500 events
land in ~1 s. Sane behavior, but worth confirming in Phase 4's
slow-consumer test.

## Critical files to know about

| File | Status | Notes |
| --- | --- | --- |
| `src/polling/mod.rs` | NEW, DONE | Library polling helper. Mirror this when adding new observability to push. |
| `src/wal/pg_streaming.rs` | MODIFIED | Added `events_received` counter; signature of `streaming_task` grew one arg. |
| `src/lib.rs` | MODIFIED | Re-exports polling types under `pg-streaming`. |
| `Cargo.toml` | MODIFIED | New `[[test]]` blocks for `polling_smoke` + `cdc_equivalence`. |
| `tests/polling_smoke.rs` | NEW | Smoke test for polling helper. |
| `tests/cdc_equivalence.rs` | NEW | Differential equivalence test. Permanent regression catcher. |
| `tests/pg_streaming_e2e.rs` | MODIFIED | New `events_received_counter_tracks_pushed_events` test. |
| `tests/polling_vs_push_benchmark.rs` | UNTOUCHED | Step 8 will refactor this to use `PollingPgCdcSource` instead of inline hand-rolled polling. |
| `docs/cdc-workload-benchmark-plan.md` | DONE | Full plan; multi-phase scenarios. Reference, not implementation. |
| `docs/pg-streaming-design.md` | UNTOUCHED | Has the one-shot benchmark numbers in § "Empirical polling-vs-push latency". Step 6 should add or update this section after Phase 1 reproduces those numbers. |
| `docs/benchmarks/pg-streaming-latency-2026-06-15.txt` | UNTOUCHED | Raw prior numbers. Step 6 compares against these. |

## Verification state

All passing as of handoff:

- `cargo test --lib` — 942 tests
- `cargo test --doc` — 20 doctests
- `cargo clippy --all-targets --all-features -- -D warnings` — clean
- `cargo fmt --all -- --check` — clean
- `cargo check --no-default-features --target wasm32-unknown-unknown` — clean
- `cargo check --features pg-streaming` — clean
- All seven Docker-backed pg_streaming_e2e tests pass
- `tests/polling_smoke.rs` passes
- `tests/cdc_equivalence.rs` passes

## Watchouts for the next session

- **The no-polling-cdc memory rule was updated 2026-06-15** to reflect
  that `subql::polling` is now intentional. If you re-read it and
  feel pulled toward deleting the polling code, you're reading a
  stale interpretation — the rule itself was changed deliberately.
- **`Cell::Float(f64)` blocks `Eq`** — any cross-event comparison
  needs a manual structural comparator (see
  `tests/cdc_equivalence.rs::canonical_eq` for the pattern).
- **Examples can't import from `tests/common/`** — `examples/cdc_bench_common/`
  duplicates the PG-container-setup helpers. Accept the duplication;
  alternative (workspace sibling crate) is more cargo plumbing than
  it's worth.
- **Phase 4 long-idle workloads** in `cdc-workload-benchmark-plan.md`
  take minutes by definition. Gate them behind a CLI arg / env var
  so the default `cargo run --example phase4_adversarial` finishes
  fast.
- **Materialize fork pin** is in `Cargo.toml` `[patch.crates-io]`,
  pinned to a specific rev. The equivalence test catches drift on
  rev bumps. See [[rust-postgres-pr-778-split-strategy]] for the
  upstream situation.

## Resume command

After reading this doc + the plan file:

```sh
cd /home/luca/github/subql
# Sanity check that the prior state holds:
cargo test --lib && cargo clippy --all-targets --all-features -- -D warnings
# Then start Step 5 by creating examples/cdc_bench_common/mod.rs.
```

The plan file's "Implementation steps" section has the concrete
Step 5 deliverable list. Implement it in roughly the order listed
there; the file should be runnable-enough to support Step 6
immediately after.
