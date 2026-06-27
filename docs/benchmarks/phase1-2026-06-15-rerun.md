Run: cargo run --release --example phase1_baseline --features pg-streaming
Date: 2026-06-15 (re-run after warmup added)
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16 (Postgres 16 + wal2json)
Branch: feat/reexec-db-free

Note: Phase 1 medians show substantial run-to-run variance (~3-5x on at
least one cell per run). The variance affects polling as well as push,
indicating host-wide noise (PG / Docker / kernel) rather than a
transport-specific regression. The architectural-claim ranking
(push < poll@100ms < poll@1000ms) survives every observed run. See
docs/pg-streaming-design.md Aside: Phase 1 run-to-run variance.

Phase 1 — Append-only baseline (single table)
Reproducing prior one-shot push-vs-poll numbers across three workloads.

=== W1.1 — single-row inserts, gap = max(50ms, 1.3 x poll interval) ===
push (PgStreamingCdcSource)              n= 15  min=    3.5ms  median=    4.3ms  mean=    5.0ms  p99=    8.5ms  max=    8.5ms
poll @ 10ms interval                     n= 15  min=    4.4ms  median=    6.4ms  mean=    6.7ms  p99=   11.5ms  max=   11.5ms
poll @ 100ms interval                    n= 15  min=    5.4ms  median=   51.3ms  mean=   52.0ms  p99=   96.8ms  max=   96.8ms
poll @ 1000ms interval                   n= 15  min=   52.7ms  median=  580.3ms  mean=  543.3ms  p99=  974.1ms  max=  974.1ms

=== W1.2 — sustained high-rate inserts at 500/s ===
push (PgStreamingCdcSource)              n=500  min=    3.0ms  median=    3.6ms  mean=    3.8ms  p99=    7.5ms  max=   11.3ms
poll @ 10ms interval                     n=500  min=    3.9ms  median=    9.8ms  mean=    9.4ms  p99=   25.9ms  max=   44.7ms
poll @ 100ms interval                    n=500  min=    4.6ms  median=   56.0ms  mean=   55.8ms  p99=  110.3ms  max=  118.8ms
poll @ 1000ms interval                   n=500  min=   10.2ms  median=  503.8ms  mean=  504.6ms  p99=  995.7ms  max= 1010.0ms

=== W1.3 — idle-then-burst (3 s idle, 1 insert, repeated) ===
push (PgStreamingCdcSource)              n=  5  min=    3.9ms  median=    5.3ms  mean=    5.4ms  p99=    7.7ms  max=    7.7ms
poll @ 10ms interval                     n=  5  min=    5.8ms  median=    7.4ms  mean=    7.6ms  p99=   10.3ms  max=   10.3ms
poll @ 100ms interval                    n=  5  min=    6.5ms  median=   85.8ms  mean=   71.4ms  p99=   95.9ms  max=   95.9ms
poll @ 1000ms interval                   n=  5  min=  763.6ms  median=  774.8ms  mean=  782.7ms  p99=  805.0ms  max=  805.0ms

---

### W1.1 — single-row inserts, gap = max(50ms, 1.3 x poll interval)

| transport | n | min (ms) | median (ms) | mean (ms) | p99 (ms) | max (ms) |
| --- | ---:| ---:| ---:| ---:| ---:| ---:|
| push (PgStreamingCdcSource) | 15 | 3.5 | 4.3 | 5.0 | 8.5 | 8.5 |
| poll @ 10ms interval | 15 | 4.4 | 6.4 | 6.7 | 11.5 | 11.5 |
| poll @ 100ms interval | 15 | 5.4 | 51.3 | 52.0 | 96.8 | 96.8 |
| poll @ 1000ms interval | 15 | 52.7 | 580.3 | 543.3 | 974.1 | 974.1 |

### W1.2 — sustained high-rate inserts at 500/s

| transport | n | min (ms) | median (ms) | mean (ms) | p99 (ms) | max (ms) |
| --- | ---:| ---:| ---:| ---:| ---:| ---:|
| push (PgStreamingCdcSource) | 500 | 3.0 | 3.6 | 3.8 | 7.5 | 11.3 |
| poll @ 10ms interval | 500 | 3.9 | 9.8 | 9.4 | 25.9 | 44.7 |
| poll @ 100ms interval | 500 | 4.6 | 56.0 | 55.8 | 110.3 | 118.8 |
| poll @ 1000ms interval | 500 | 10.2 | 503.8 | 504.6 | 995.7 | 1010.0 |

### W1.3 — idle-then-burst (3 s idle, 1 insert, repeated)

| transport | n | min (ms) | median (ms) | mean (ms) | p99 (ms) | max (ms) |
| --- | ---:| ---:| ---:| ---:| ---:| ---:|
| push (PgStreamingCdcSource) | 5 | 3.9 | 5.3 | 5.4 | 7.7 | 7.7 |
| poll @ 10ms interval | 5 | 5.8 | 7.4 | 7.6 | 10.3 | 10.3 |
| poll @ 100ms interval | 5 | 6.5 | 85.8 | 71.4 | 95.9 | 95.9 |
| poll @ 1000ms interval | 5 | 763.6 | 774.8 | 782.7 | 805.0 | 805.0 |

Phase 1 W1.1 vs prior one-shot:
  W1.1 push: observed    4.3 ms vs prior    4.6 ms (-6%)
  W1.1 poll@10ms: observed    6.4 ms vs prior    5.5 ms (+17%)
  W1.1 poll@100ms: observed   51.3 ms vs prior   57.9 ms (-11%)
  W1.1 poll@1000ms: observed  580.3 ms vs prior  554.7 ms (+5%)
Phase 1 architectural-claim check: PASS
  push beats poll@100ms and poll@1000ms on every workload (median).
