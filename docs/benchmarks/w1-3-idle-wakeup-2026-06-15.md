Run: cargo run --release --example w1_3_idle_wakeup --features pg-streaming
Date: 2026-06-15
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16 (Postgres 16 + wal2json)
Branch: feat/reexec-db-free

W1.3 post-idle push wake-up investigation
5 trials per cell, push status_interval x idle duration matrix, polling control included.

push (status@1000ms)          idle=    0ms  n= 5  min=   4.4ms  median=   4.7ms  max=   5.7ms
push (status@1000ms)          idle=  100ms  n= 5  min=   4.7ms  median=   5.7ms  max=   5.8ms
push (status@1000ms)          idle=  500ms  n= 5  min=   4.7ms  median=   5.3ms  max=   5.9ms
push (status@1000ms)          idle= 1000ms  n= 5  min=   4.8ms  median=   6.9ms  max=   8.5ms
push (status@1000ms)          idle= 3000ms  n= 5  min=   4.5ms  median=   4.8ms  max=   6.8ms

push (status@10000ms)         idle=    0ms  n= 5  min=   4.1ms  median=   5.0ms  max=   6.2ms
push (status@10000ms)         idle=  100ms  n= 5  min=   4.7ms  median=   4.8ms  max=   6.4ms
push (status@10000ms)         idle=  500ms  n= 5  min=   4.8ms  median=   5.5ms  max=   6.6ms
push (status@10000ms)         idle= 1000ms  n= 5  min=   4.5ms  median=   5.9ms  max=   6.2ms
push (status@10000ms)         idle= 3000ms  n= 5  min=   4.8ms  median=   5.1ms  max=   6.4ms

poll @ 100ms                  idle=    0ms  n= 5  min=   5.4ms  median=   5.5ms  max= 100.1ms
poll @ 100ms                  idle=  100ms  n= 5  min=   4.5ms  median=   5.2ms  max= 100.1ms
poll @ 100ms                  idle=  500ms  n= 5  min=   4.9ms  median=   5.9ms  max= 100.5ms
poll @ 100ms                  idle= 1000ms  n= 5  min=   6.0ms  median= 100.5ms  max= 101.5ms
poll @ 100ms                  idle= 3000ms  n= 5  min=   6.3ms  median=  51.5ms  max= 101.7ms

---

### Median post-idle latency (ms)

| idle (ms) | push (status@1000ms) | push (status@10000ms) | poll @ 100ms |
| ---:| ---:| ---:| ---:|
| 0 | 4.7 | 5.0 | 5.5 |
| 100 | 5.7 | 4.8 | 5.2 |
| 500 | 5.3 | 5.5 | 5.9 |
| 1000 | 6.9 | 5.9 | 100.5 |
| 3000 | 4.8 | 5.1 | 51.5 |

Headline: at 3s idle,
  push (status@1s)  median = 4.8 ms
  push (status@10s) median = 5.1 ms
  poll @ 100ms     median = 51.5 ms
Verdict: no large gap between push variants; the 30 ms tail observed in Phase 1 W1.3 may have been measurement noise. Re-run Phase 1 to confirm.
