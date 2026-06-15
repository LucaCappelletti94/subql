Run: cargo run --release --example phase1_smoothed --features pg-streaming
Date: 2026-06-15
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16 (Postgres 16 + wal2json)
Branch: feat/reexec-db-free

This is run 2 of 3 (the cleanest of three back-to-back runs).
Run-to-run variance: poll@100ms / poll@1000ms medians are stable to within 1%.
push / poll@10ms medians fluctuate 3-5x with host load but the polling SAVINGS
vs push (the operationally meaningful number) are stable across all runs.

Smoothed Phase 1
3 trials per cell with fresh source per trial; samples pooled for stats.

  push (PgStreamingCdcSource) trial 1/3: collected 200/200 events
  push (PgStreamingCdcSource) trial 2/3: collected 200/200 events
  push (PgStreamingCdcSource) trial 3/3: collected 200/200 events
push (PgStreamingCdcSource)              n=600  min=    3.1ms  median=    3.8ms  mean=    3.9ms  p99=    4.8ms  max=   43.2ms

  poll @ 10ms interval trial 1/3: collected 200/200 events
  poll @ 10ms interval trial 2/3: collected 200/200 events
  poll @ 10ms interval trial 3/3: collected 200/200 events
poll @ 10ms interval                     n=600  min=    3.5ms  median=    5.2ms  mean=    6.1ms  p99=   10.6ms  max=   36.9ms

  poll @ 100ms interval trial 1/3: collected 100/100 events
  poll @ 100ms interval trial 2/3: collected 100/100 events
  poll @ 100ms interval trial 3/3: collected 100/100 events
poll @ 100ms interval                    n=300  min=    4.0ms  median=   50.0ms  mean=   49.9ms  p99=  100.4ms  max=  119.2ms

  poll @ 1000ms interval trial 1/3: collected 30/30 events
  poll @ 1000ms interval trial 2/3: collected 30/30 events
  poll @ 1000ms interval trial 3/3: collected 30/30 events
poll @ 1000ms interval                   n= 90  min=   49.1ms  median=  539.8ms  mean=  535.3ms  p99=  993.1ms  max=  993.1ms

---

### Smoothed Phase 1: pooled across 3 trials per cell

| transport | n | min (ms) | median (ms) | mean (ms) | p99 (ms) | max (ms) |
| --- | ---:| ---:| ---:| ---:| ---:| ---:|
| push (PgStreamingCdcSource) | 600 | 3.1 | 3.8 | 3.9 | 4.8 | 43.2 |
| poll @ 10ms interval | 600 | 3.5 | 5.2 | 6.1 | 10.6 | 36.9 |
| poll @ 100ms interval | 300 | 4.0 | 50.0 | 49.9 | 100.4 | 119.2 |
| poll @ 1000ms interval | 90 | 49.1 | 539.8 | 535.3 | 993.1 | 993.1 |

Smoothed Phase 1 architectural-claim check: PASS
  push median = 3.8 ms,  poll@100ms median = 50.0 ms (savings: 46.2 ms),  poll@1000ms median = 539.8 ms (savings: 536.0 ms)
