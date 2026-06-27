Run: cargo run --release --example scale_throughput --features pg-streaming
Date: 2026-06-16 (commons_server, 64-core idle host)
Host: commons_server (Linux 6.8.0, 251 GiB RAM, load <1.5 throughout)
PG container: subql-test/postgres-wal2json:16, max_wal_senders=384, max_replication_slots=384, max_connections=512
Branch: feat/reexec-db-free, working-tree state (not yet committed)

Scale benchmark: throughput ceiling vs event rate
3 trials per cell, 10 s producer window, push + poll@{10,100,1000}ms.

  rate=  100/s  push            median=   3.3ms  p99=   4.6ms  drain=    66/s  producer=    99/s
  rate=  100/s  poll@10ms       median=   9.3ms  p99=  14.8ms  drain=    66/s  producer=    99/s
  rate=  100/s  poll@100ms      median=  55.5ms  p99= 106.9ms  drain=    64/s  producer=    99/s
  rate=  100/s  poll@1000ms     median= 513.1ms  p99=1001.3ms  drain=    52/s  producer=    98/s

  rate=  500/s  push            median=   3.5ms  p99=   5.6ms  drain=   192/s  producer=   289/s
  rate=  500/s  poll@10ms       median=  23.2ms  p99=  31.0ms  drain=   157/s  producer=   237/s
  rate=  500/s  poll@100ms      median=  65.1ms  p99= 117.0ms  drain=   193/s  producer=   297/s
  rate=  500/s  poll@1000ms     median= 528.0ms  p99=1010.6ms  drain=   154/s  producer=   294/s

  rate= 1000/s  push            median=   3.6ms  p99=   4.1ms  drain=   655/s  producer=   984/s
  rate= 1000/s  poll@10ms       median=  21.6ms  p99=  32.9ms  drain=   602/s  producer=   906/s
  rate= 1000/s  poll@100ms      median=  66.1ms  p99= 116.9ms  drain=   625/s  producer=   963/s
  rate= 1000/s  poll@1000ms     median= 523.4ms  p99=1010.2ms  drain=   517/s  producer=   984/s

  rate= 5000/s  push            median=   4.3ms  p99=   4.9ms  drain=  3219/s  producer=  4833/s
  rate= 5000/s  poll@10ms       median=  31.6ms  p99=  53.1ms  drain=  2682/s  producer=  4038/s
  rate= 5000/s  poll@100ms      median=  75.8ms  p99= 131.9ms  drain=  3041/s  producer=  4688/s
  rate= 5000/s  poll@1000ms     median= 542.8ms  p99=1029.2ms  drain=  2567/s  producer=  4879/s

  rate=10000/s  push            median=   5.5ms  p99=   7.3ms  drain=  6419/s  producer=  9635/s
  rate=10000/s  poll@10ms       median=  38.5ms  p99=  64.5ms  drain=  5857/s  producer=  8815/s
  rate=10000/s  poll@100ms      median=  85.7ms  p99= 147.4ms  drain=  6211/s  producer=  9573/s
  rate=10000/s  poll@1000ms     median= 562.4ms  p99=1047.0ms  drain=  5147/s  producer=  9781/s

  rate=25000/s  push            median=   5.2ms  p99=   6.5ms  drain=  7201/s  producer= 10810/s
  rate=25000/s  poll@10ms       median=  25.1ms  p99=  54.9ms  drain=  7337/s  producer= 11043/s
  rate=25000/s  poll@100ms      median=  82.9ms  p99= 149.6ms  drain=  6975/s  producer= 10751/s
  rate=25000/s  poll@1000ms     median= 568.3ms  p99=1055.4ms  drain=  5767/s  producer= 10965/s

---

### Throughput ceiling: median latency by (rate, transport)

| rate (/s) | push median (ms) | push drain (/s) | poll@10ms median (ms) | poll@10ms drain (/s) | poll@100ms median (ms) | poll@100ms drain (/s) | poll@1000ms median (ms) | poll@1000ms drain (/s) |
| ---:| ---:| ---:| ---:| ---:| ---:| ---:| ---:| ---:|
| 100 | 3.3 | 66 | 9.3 | 66 | 55.5 | 64 | 513.1 | 52 |
| 500 | 3.5 | 192 | 23.2 | 157 | 65.1 | 193 | 528.0 | 154 |
| 1000 | 3.6 | 655 | 21.6 | 602 | 66.1 | 625 | 523.4 | 517 |
| 5000 | 4.3 | 3219 | 31.6 | 2682 | 75.8 | 3041 | 542.8 | 2567 |
| 10000 | 5.5 | 6419 | 38.5 | 5857 | 85.7 | 6211 | 562.4 | 5147 |
| 25000 | 5.2 | 7201 | 25.1 | 7337 | 82.9 | 6975 | 568.3 | 5767 |

Captured TSV: docs/benchmarks/scale-throughput-2026-06-15.tsv
