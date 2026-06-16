Run: cargo run --release --example scale_consumers --features pg-streaming
Date: 2026-06-15
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16
Branch: feat/reexec-db-free

Scale benchmark: latency + server load vs consumer count
Experiment A: N producers x 200 ev/s.  Experiment B: 1 producer x 1000 ev/s.
Window: 30 s per cell, push + poll@100ms.

  exp_a_per_consumer_producers         N=  1  push        median=  60.2ms  p99= 135.4ms  active_pid_end=0  xact/s=14  tup/s=13
  exp_a_per_consumer_producers         N=  1  poll@100ms  median= 106.9ms  p99= 591.4ms  active_pid_end=0  xact/s=35  tup/s=15

  exp_a_per_consumer_producers         N=  5  push        median=  68.4ms  p99= 116.4ms  active_pid_end=0  xact/s=81  tup/s=80
  exp_a_per_consumer_producers         N=  5  poll@100ms  median= 148.2ms  p99= 329.9ms  active_pid_end=0  xact/s=141  tup/s=42

  exp_a_per_consumer_producers         N= 10  push        median=  27.8ms  p99=  72.2ms  active_pid_end=0  xact/s=277  tup/s=275
  exp_a_per_consumer_producers         N= 10  poll@100ms  median=  69.1ms  p99= 131.4ms  active_pid_end=0  xact/s=1194  tup/s=993

  exp_a_per_consumer_producers         N= 30  push        median=  40.7ms  p99= 129.4ms  active_pid_end=30  xact/s=558  tup/s=555
  exp_a_per_consumer_producers         N= 30  poll@100ms  median=  75.8ms  p99= 145.8ms  active_pid_end=0  xact/s=2299  tup/s=1711

  exp_a_per_consumer_producers         N=100  push        median=5089.6ms  p99=9277.9ms  active_pid_end=100  xact/s=3343  tup/s=3332
  exp_a_per_consumer_producers         N=100  poll@100ms  median=1291.9ms  p99=5262.4ms  active_pid_end=0  xact/s=10490  tup/s=10396

  exp_b_shared_producer                N=  1  push        median=  10.3ms  p99=  32.0ms  active_pid_end=0  xact/s=73  tup/s=72
  exp_b_shared_producer                N=  1  poll@100ms  median=  67.1ms  p99= 153.5ms  active_pid_end=0  xact/s=71  tup/s=51

  exp_b_shared_producer                N=  5  push        median=  18.6ms  p99=  59.8ms  active_pid_end=0  xact/s=43  tup/s=43
  exp_b_shared_producer                N=  5  poll@100ms  median=  55.8ms  p99= 105.6ms  active_pid_end=0  xact/s=291  tup/s=191

  exp_b_shared_producer                N= 10  push        median=   5.9ms  p99=  19.7ms  active_pid_end=0  xact/s=123  tup/s=122
  exp_b_shared_producer                N= 10  poll@100ms  median=  63.3ms  p99= 119.9ms  active_pid_end=0  xact/s=266  tup/s=67

  exp_b_shared_producer                N= 30  push        median=   4.0ms  p99=  22.0ms  active_pid_end=30  xact/s=131  tup/s=130
  exp_b_shared_producer                N= 30  poll@100ms  median=  55.3ms  p99= 104.5ms  active_pid_end=0  xact/s=828  tup/s=235

  exp_b_shared_producer                N=100  push        median=  11.5ms  p99=  32.7ms  active_pid_end=100  xact/s=77  tup/s=73
  exp_b_shared_producer                N=100  poll@100ms  median=  56.8ms  p99= 266.8ms  active_pid_end=0  xact/s=2112  tup/s=221

Captured TSV: docs/benchmarks/scale-consumers-2026-06-15.tsv
