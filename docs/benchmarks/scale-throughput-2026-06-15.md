Run: cargo run --release --example scale_throughput --features pg-streaming
Date: 2026-06-15
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16
Branch: feat/reexec-db-free

Scale benchmark: throughput ceiling vs event rate
3 trials per cell, 10 s producer window, push + poll@{10,100,1000}ms.

  rate=  100/s  push            median=   3.5ms  p99=   9.4ms  drain=    66/s  producer=    99/s
  rate=  100/s  poll@10ms       median=  11.3ms  p99=  51.2ms  drain=    44/s  producer=    67/s
  rate=  100/s  poll@100ms      median=  79.5ms  p99= 278.7ms  drain=    23/s  producer=    36/s
  rate=  100/s  poll@1000ms     median= 517.5ms  p99=1006.1ms  drain=    41/s  producer=    78/s

  rate=  500/s  push            median=   3.3ms  p99=   8.9ms  drain=   177/s  producer=   266/s
  rate=  500/s  poll@10ms       median=   9.5ms  p99=  15.8ms  drain=   196/s  producer=   295/s
  rate=  500/s  poll@100ms      median=  55.3ms  p99= 104.7ms  drain=   194/s  producer=   299/s
  rate=  500/s  poll@1000ms     median= 511.6ms  p99= 998.4ms  drain=   103/s  producer=   196/s

  rate= 1000/s  push            median=  36.8ms  p99= 212.1ms  drain=    83/s  producer=   124/s
  rate= 1000/s  poll@10ms       median=  19.1ms  p99=  54.5ms  drain=   305/s  producer=   461/s
  rate= 1000/s  poll@100ms      median=  67.0ms  p99= 124.0ms  drain=   482/s  producer=   744/s
  rate= 1000/s  poll@1000ms     median= 536.6ms  p99=1016.2ms  drain=   116/s  producer=   221/s

  rate= 5000/s  push            median=   6.1ms  p99=  50.3ms  drain=  1686/s  producer=  2536/s
  rate= 5000/s  poll@10ms       median=  14.9ms  p99=  57.5ms  drain=  3097/s  producer=  4662/s
  rate= 5000/s  poll@100ms      median=  68.5ms  p99= 136.4ms  drain=   913/s  producer=  1407/s
  rate= 5000/s  poll@1000ms     median= 525.5ms  p99=1008.6ms  drain=  1324/s  producer=  2517/s

  rate=10000/s  push            median=   4.5ms  p99=   9.0ms  drain=  6429/s  producer=  9650/s
  rate=10000/s  poll@10ms       median=  20.3ms  p99=  64.5ms  drain=  2632/s  producer=  3962/s
  rate=10000/s  poll@100ms      median=  86.1ms  p99= 274.2ms  drain=   924/s  producer=  1423/s
  rate=10000/s  poll@1000ms     median= 534.2ms  p99=1017.3ms  drain=  5045/s  producer=  9590/s

  rate=25000/s  push            median=   5.1ms  p99=  84.5ms  drain=  2394/s  producer=  3592/s
  rate=25000/s  poll@10ms       median=  34.4ms  p99= 123.1ms  drain=  2582/s  producer=  3890/s
  rate=25000/s  poll@100ms      median=  92.6ms  p99= 188.0ms  drain=  4022/s  producer=  6216/s
  rate=25000/s  poll@1000ms     median= 617.6ms  p99=1110.3ms  drain=  3152/s  producer=  6006/s

---

### Throughput ceiling: median latency by (rate, transport)

| rate (/s) | push median (ms) | push drain (/s) | poll@10ms median (ms) | poll@10ms drain (/s) | poll@100ms median (ms) | poll@100ms drain (/s) | poll@1000ms median (ms) | poll@1000ms drain (/s) |
| ---:| ---:| ---:| ---:| ---:| ---:| ---:| ---:| ---:|
| 100 | 3.5 | 66 | 11.3 | 44 | 79.5 | 23 | 517.5 | 41 |
| 500 | 3.3 | 177 | 9.5 | 196 | 55.3 | 194 | 511.6 | 103 |
| 1000 | 36.8 | 83 | 19.1 | 305 | 67.0 | 482 | 536.6 | 116 |
| 5000 | 6.1 | 1686 | 14.9 | 3097 | 68.5 | 913 | 525.5 | 1324 |
| 10000 | 4.5 | 6429 | 20.3 | 2632 | 86.1 | 924 | 534.2 | 5045 |
| 25000 | 5.1 | 2394 | 34.4 | 2582 | 92.6 | 4022 | 617.6 | 3152 |

Captured TSV: docs/benchmarks/scale-throughput-2026-06-15.tsv
