Run: cargo run --release --example scale_consumers --features pg-streaming
Date: 2026-06-16 (commons_server)
Host: commons_server
PG container: subql-test/postgres-wal2json:16, max_wal_senders=384, max_replication_slots=384, max_connections=512

Scale benchmark: latency + server load vs consumer count
Experiment A: N producers x 200 ev/s.  Experiment B: 1 producer x 1000 ev/s.
Window: 20 s per cell, push + poll@100ms.

  exp_a_per_consumer_producers         N=  1  push        median(mean±std)=   3.3±0.0ms  pooled=   3.3ms  xact/s=153±1
  exp_a_per_consumer_producers         N=  1  poll@100ms  median(mean±std)=  59.0±4.2ms  pooled=  58.8ms  xact/s=172±1

  exp_a_per_consumer_producers         N=  5  push        median(mean±std)=   6.3±0.0ms  pooled=   6.3ms  xact/s=629±7
  exp_a_per_consumer_producers         N=  5  poll@100ms  median(mean±std)=  71.2±3.0ms  pooled=  71.1ms  xact/s=650±53

  exp_a_per_consumer_producers         N= 10  push        median(mean±std)=   6.7±0.2ms  pooled=   6.7ms  xact/s=1243±23
  exp_a_per_consumer_producers         N= 10  poll@100ms  median(mean±std)=  78.0±1.4ms  pooled=  77.9ms  xact/s=1122±54

  exp_a_per_consumer_producers         N= 30  push        median(mean±std)=   8.3±0.4ms  pooled=   8.2ms  xact/s=3729±34
  exp_a_per_consumer_producers         N= 30  poll@100ms  median(mean±std)=  94.8±1.6ms  pooled=  94.9ms  xact/s=3328±150

  exp_a_per_consumer_producers         N=100  push        median(mean±std)=7909.0±138.1ms  pooled=7908.5ms  xact/s=9017±325
  exp_a_per_consumer_producers         N=100  poll@100ms  median(mean±std)=7919.1±281.0ms  pooled=7912.7ms  xact/s=5126±53

  exp_b_shared_producer                N=  1  push        median(mean±std)=   3.4±0.0ms  pooled=   3.4ms  xact/s=229±4
  exp_b_shared_producer                N=  1  poll@100ms  median(mean±std)=  64.8±1.2ms  pooled=  64.8ms  xact/s=247±1

  exp_b_shared_producer                N=  5  push        median(mean±std)=   3.5±0.0ms  pooled=   3.5ms  xact/s=228±3
  exp_b_shared_producer                N=  5  poll@100ms  median(mean±std)=  64.6±2.2ms  pooled=  64.6ms  xact/s=304±9

  exp_b_shared_producer                N= 10  push        median(mean±std)=   3.6±0.0ms  pooled=   3.6ms  xact/s=226±1
  exp_b_shared_producer                N= 10  poll@100ms  median(mean±std)=  67.7±1.9ms  pooled=  67.7ms  xact/s=365±11

  exp_b_shared_producer                N= 30  push        median(mean±std)=   4.0±0.1ms  pooled=   4.0ms  xact/s=219±3
  exp_b_shared_producer                N= 30  poll@100ms  median(mean±std)=  62.3±4.4ms  pooled=  61.9ms  xact/s=741±31

  exp_b_shared_producer                N=100  push        median(mean±std)=   5.4±0.0ms  pooled=   5.4ms  xact/s=177±1
  exp_b_shared_producer                N=100  poll@100ms  median(mean±std)= 128.8±2.7ms  pooled= 128.8ms  xact/s=1281±19

Captured TSV: docs/benchmarks/scale-consumers-2026-06-15.tsv
