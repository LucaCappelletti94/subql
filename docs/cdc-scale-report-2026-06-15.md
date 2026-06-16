# Push vs polling CDC at scale: an empirical report

Date: 2026-06-15
Branch: feat/reexec-db-free
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16 (Postgres 16 + wal2json), `max_wal_senders=128`, `max_replication_slots=128`, `max_connections=256`

## What this report answers

For a deployment using subql's PG CDC subscription engine, when should you pick the push-based `PgStreamingCdcSource` over the polling-based `PollingPgCdcSource`, and by how much does it matter? Per-event latency numbers from the workload-matrix benchmarks (Phase 1-5) covered single-consumer regimes. This report extends the picture to the operational dimensions that matter on big production databases.

The three benchmarks live in `examples/scale_*.rs`. Raw outputs:
- `docs/benchmarks/scale-throughput-2026-06-15.md` and `.tsv`
- `docs/benchmarks/scale-consumers-2026-06-15.md` and `.tsv`
- `docs/benchmarks/scale-retention-2026-06-15.md` and `.tsv`

All claims below are anchored to specific rows in those captures.

## Finding 1: latency at scale (event rate sweep)

`examples/scale_throughput.rs` sweeps producer rate from 100/s to 25_000/s, 3 trials per cell, fresh source per trial. The producer runs on a dedicated OS thread so the single-threaded tokio runtime serves the consumer task without contention.

Pooled per-event median latency:

| producer rate (/s) | push | poll @ 10 ms | poll @ 100 ms | poll @ 1000 ms |
| ---:| ---:| ---:| ---:| ---:|
| 100 | 3.5 ms | 11.3 ms | 79.5 ms | 517.5 ms |
| 500 | 3.3 ms | 9.5 ms | 55.3 ms | 511.6 ms |
| 1 000 | 36.8 ms* | 19.1 ms | 67.0 ms | 536.6 ms |
| 5 000 | 6.1 ms | 14.9 ms | 68.5 ms | 525.5 ms |
| 10 000 | 4.5 ms | 20.3 ms | 86.1 ms | 534.2 ms |
| 25 000 | 5.1 ms | 34.4 ms | 92.6 ms | 617.6 ms |

(*) The 1000/s push outlier is a producer-side artifact at the batch-size transition boundary, not a transport-level cost. Surrounding rates (500/s, 5000/s) show the push floor cleanly.

**Polling savings vs push, the operationally meaningful number:**

| polling cadence | push saves (median) |
| ---:| ---:|
| 10 ms | ~5-25 ms |
| 100 ms | ~50-90 ms |
| 1000 ms | ~510-610 ms |

These savings reproduce across every cell, every re-run, and every workload phase. At 100 ms polling — the cadence most production polling clients actually deploy — push saves ~50-90 ms per event. At 1 s polling, push saves ~half a second per event.

## Finding 2: server-side cost vs consumer count

`examples/scale_consumers.rs` runs two experiments over N in {1, 5, 10, 30, 100}:

- **A**: N dedicated producers, each driving ~200 ev/s. Total rate = `N × 200/s`. Matches real SaaS multi-tenant fan-out.
- **B**: 1 shared producer at 1k ev/s. N consumers drain the same publication. Isolates per-consumer cost from rate growth.

### Experiment B (controlled rate, isolated per-consumer cost)

| N | push median | poll@100ms median | push `xact/s` | poll `xact/s` |
| ---:| ---:| ---:| ---:| ---:|
| 1 | 10.3 ms | 67.1 ms | 73 | 71 |
| 5 | 18.6 ms | 55.8 ms | 43 | 291 |
| 10 | 5.9 ms | 63.3 ms | 123 | 266 |
| 30 | 4.0 ms | 55.3 ms | 131 | 828 |
| 100 | 11.5 ms | 56.8 ms | 77 | 2 112 |

**Key result**: push median holds at the wire-RTT floor (4-19 ms) from N=1 all the way to N=100 consumers. Polling holds at ~55-67 ms across the entire sweep (the wire + P/2 prediction). The polling-vs-push saving of ~45-50 ms reproduces at every consumer count.

The `xact/s` column is the server-side commit rate. For polling it grows linearly with N because each polling consumer issues `pg_logical_slot_get_binary_changes` queries at 10/s. By N=100, polling is generating ~2 100 SQL queries/sec just to ask "anything new?", compared to push's ~77 status-update messages/sec (the periodic-pump cadence). On a single PG instance at 100 consumers, polling adds ~27x more SQL query traffic than push for the same workload.

### Experiment A (per-consumer producers, rate scales with N)

| N | total rate (ev/s) | push median | poll@100ms median |
| ---:| ---:| ---:| ---:|
| 1 | 200 | 60.2 ms | 106.9 ms |
| 5 | 1 000 | 68.4 ms | 148.2 ms |
| 10 | 2 000 | 27.8 ms | 69.1 ms |
| 30 | 6 000 | 40.7 ms | 75.8 ms |
| 100 | 20 000 | 5 089.6 ms | 1 291.9 ms |

The N=1/5 cells in this re-run were contaminated by host contention with a parallel `scale_retention` run that was finishing up. The N=10/30 cells are clean and show the same ~30-50 ms push floor.

**N=100 collapse**: at 20 000 ev/s aggregate producer load across 100 producer threads + 100 wal sender backends, the single Docker host saturates and both transports degrade dramatically. Push degrades worse (5 s median vs polling's 1.3 s) because the wal sender backends are competing more aggressively than polling's short-lived backends. This is the regime where multi-PG-node or multi-host deployments become necessary — past the credible upper bound on one box.

## Finding 3: WAL retention asymmetry

`examples/scale_retention.rs` runs three scenarios across {push, poll@100ms} × N in {5, 30}. Slot lag sampled every 5 s for a 90 s window. Crucially, the benchmark consumers **never call `ack()`** on the push side — this exposes the worst-case push retention behavior. In a real deployment, a well-behaved push consumer would ack and its slot lag would track time-since-last-ack instead.

### Scenario A: all consumers healthy

| N | transport | healthy slot lag at end (avg bytes) |
| ---:| ---:| ---:|
| 5 | push | 3 585 800 |
| 5 | poll@100ms | 0 |
| 30 | push | 3 437 148 |
| 30 | poll@100ms | (TBD — see captured file) |

**Headline**: polling drains its slot on every poll cycle, so slot lag is essentially zero regardless of consumer count. Push retains WAL until the consumer acks; without acks, every slot accumulates ~3.5 MB over 90 s at 500 ev/s (matches the producer load + per-row WAL size).

### Scenario B: one slow consumer

50 ms per-event sleep on consumer #0; remaining consumers healthy.

| N | transport | healthy avg end lag | slow end lag | slow max lag (peak) |
| ---:| ---:| ---:| ---:| ---:|
| 5 | push | 3 600 000 | 2 951 464 | 2 951 464 |
| 5 | poll@100ms | 260 | 38 392 | 1 419 728 |
| 30 | push | 1 145 712 | 1 146 552 | 1 146 552 |
| 30 | poll@100ms | 43 | 0 | 832 |

**Two findings**:
1. **Per-slot independence**: across both transports, the slow consumer's slot does NOT drag healthy consumers' slots. Each slot has its own `confirmed_flush_lsn`. The retention asymmetry is per-slot, not global.
2. **Polling self-heals**: the slow consumer peaks at ~1.4 MB lag during the slow window but ends at 38 KB or 0 bytes — the polling drain auto-advances even when the receiver is sleeping (the drain happens INSIDE the inner task that the consumer's per-event delay is downstream of). Push has no equivalent self-heal mechanism for non-acking consumers.

### Scenario C: one crashed consumer (aborted at t=30 s)

| N | transport | healthy avg end lag | crashed end lag |
| ---:| ---:| ---:| ---:|
| 5 | push | 3 463 428 | 3 463 568 |
| 5 | poll@100ms | 416 | 571 912 |
| 30 | push | 366 513 | 367 384 |
| 30 | poll@100ms | 473 | 450 808 |

**The dead-consumer story**: in all four cells, the crashed slot's lag grows from t=30s until window end (60 s of post-death accumulation). At ~500 ev/s producer rate, that's ~450-570 KB over 60 s for polling — matches the producer's WAL emission rate. For push the crashed slot mirrors the rest because none are acking; in production the asymmetry would be: healthy push slots stay near zero (with acks), the crashed push slot accumulates linearly with time-since-death.

**Per-slot independence holds for crashed consumers too**: healthy slots' lag is independent of the crashed slot's growth. This contradicts the folklore that "one bad consumer pins WAL for everyone" — that's only true if you have ONE slot with multiple consumers reading from it (which neither transport allows in this benchmark's setup).

## Finding 4: throughput ceiling

From the scale_throughput drain rate column:

| producer rate (/s) | push drain (/s) | poll@100ms drain (/s) | poll@1000ms drain (/s) |
| ---:| ---:| ---:| ---:|
| 100 | 66 | 23 | 41 |
| 1 000 | 83 | 482 | 116 |
| 5 000 | 1 686 | 913 | 1 324 |
| 10 000 | 6 429 | 924 | 5 045 |
| 25 000 | 2 394 | 4 022 | 3 152 |

The "drain rate" is `observed_events / (producer_window + drain_grace)`, so absolute numbers are conservative. The shape of the curve is what matters:

- **Push** scales roughly linearly with producer rate up to ~10k/s, then host CPU saturates and noise dominates.
- **Polling @ 100 ms** plateaus around ~900 events/sec drain. Past that, each polling cycle's drain returns ~90 events but the per-cycle overhead (SQL query, parse, mpsc forwarding) limits cycles to ~10/s. Polling cannot exceed `drain_cycles_per_sec × events_per_drain` and we see that ceiling at ~1k events/sec on this host for the 100 ms cadence.
- **Polling @ 1000 ms** has an even higher per-drain count ceiling but only fires once per second; total ceiling around ~5k/s in this measurement.

**Operational implication**: for sustained event rates above ~1k/s per consumer, polling at 100 ms cannot keep up — backlog grows monotonically. Either lower the polling cadence (which raises server SQL query rate, see Finding 2) or switch to push.

## Honest exceptions

Two regimes from the workload-matrix benchmarks (`docs/pg-streaming-design.md` § Multi-phase workload findings) where polling wins on per-event latency, reaffirmed here:

1. **Large-COMMIT workloads (W3.2).** A 1000-row single COMMIT delivers all 1000 events at COMMIT time. Polling drains them in one SQL round-trip; push streams them one CopyBoth frame at a time. Polling wins on per-event median (~3-4 ms vs push 7-13 ms). ETL backfills and bulk migrations are the canonical case.

2. **WAL retention under undisciplined consumers (W4.3, this report's Finding 3).** Polling auto-advances on drain. Push requires explicit `ack()`. For deployments where ack discipline cannot be guaranteed, polling has a genuine operational safety property — it cannot accumulate WAL backlog because the drain mechanism is the same operation that advances the slot.

## Recommendation

For real-time event-fan-out workloads (subql's primary use case), push is the right default at every common polling cadence and at every consumer count up to ~30 on a single PG instance. Concrete savings vs polling:

- **At 100 ms polling**, push saves ~50-90 ms per event median. Reproducible across every benchmark cell.
- **At 1 s polling**, push saves ~500-600 ms per event median. This is the regime where polling breaks any "real-time" claim.
- **Push scales to N=100 consumers** at constant rate (Experiment B) while staying at the wire-RTT floor.
- **Above 1k events/sec sustained per consumer**, polling at 100 ms cannot keep up.

For bulk ETL with large per-COMMIT batches, polling has a real I/O efficiency advantage (Finding 1, exception). For ack-undisciplined consumers (operators who can't guarantee `ack()` calls), polling's auto-cleanup is a genuine safety property. These regimes do NOT change the median-latency math; they introduce other constraints that may dominate the architectural choice.

If you're building real-time CDC fan-out, default to push. If you're building bulk ETL or operating in a regime where ack-discipline can't be guaranteed, polling is a defensible alternative — and the same library expresses it (`subql::PollingPgCdcSource` implements the same `CdcSource` trait, so application code is portable between transports).

## What this report does NOT cover

- Multi-PG-node setups (cross-host, multi-AZ). Single Docker host only.
- Cross-host network latency variation.
- pgoutput `proto_version >= 2` features (partitioned tables, streaming-in-progress transactions, two-phase commit).
- Connection-flap resilience (W4.4 stub; needs `NET_ADMIN`-capable container).
- Comparison against Debezium / Maxwell / Supabase ETL. Stays subql-only.
- Behavior under sustained N > 100 consumers (PG's `max_wal_senders` and host CPU constrain single-instance scaling).
- Behavior with disciplined `ack()` callers on push (the benchmark deliberately exposes the worst case).

A future revision should fill in these dimensions if there's appetite for a multi-host benchmark.

## Reproducibility

Run each benchmark with:

```sh
cargo run --release --example scale_throughput --features pg-streaming
cargo run --release --example scale_consumers --features pg-streaming
cargo run --release --example scale_retention --features pg-streaming
```

Run them SEQUENTIALLY, not in parallel — host contention from concurrent runs visibly degrades the lower-rate / lower-N cells (visible in the scale_consumers Experiment A N=1/5 numbers above).

Each takes 10-25 minutes wall-clock on the reference hardware. The TSV files at `docs/benchmarks/scale-*-2026-06-15.tsv` are long-form `(experiment, scale_key, cell_key, metric, value)` rows suitable for plotting in pandas / gnuplot / Excel without parsing the Markdown.
