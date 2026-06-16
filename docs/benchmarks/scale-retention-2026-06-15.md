Run: cargo run --release --example scale_retention --features pg-streaming
Date: 2026-06-15
Host: AMD Ryzen Threadripper PRO 5975WX, Linux 6.17, Docker 29.5.2
PG container: subql-test/postgres-wal2json:16
Branch: feat/reexec-db-free

Scale benchmark: WAL retention dynamics
90 s window, slot lag sampled every 5 s, producer @ 500 ev/s.

  scenario=all_healthy   N=  5  transport=push      
    healthy: mean_end_lag=   3585800B  max_lag=   3585912B  (n=5)

  scenario=all_healthy   N=  5  transport=poll@100ms
    healthy: mean_end_lag=         0B  max_lag=      2080B  (n=5)

  scenario=all_healthy   N= 30  transport=push      
    healthy: mean_end_lag=   3437148B  max_lag=   3437960B  (n=30)

  scenario=all_healthy   N= 30  transport=poll@100ms
    healthy: mean_end_lag=      1033B  max_lag=      6448B  (n=30)

  scenario=one_slow      N=  5  transport=push      
    healthy: mean_end_lag=   2951324B  max_lag=   2951408B  (n=4)
    slow    : end_lag=   2951464B  max_lag=   2951464B

  scenario=one_slow      N=  5  transport=poll@100ms
    healthy: mean_end_lag=       260B  max_lag=      3464B  (n=4)
    slow    : end_lag=     38392B  max_lag=   1419728B

  scenario=one_slow      N= 30  transport=push      
    healthy: mean_end_lag=   1145712B  max_lag=   1146496B  (n=29)
    slow    : end_lag=   1146552B  max_lag=   1146552B

  scenario=one_slow      N= 30  transport=poll@100ms
    healthy: mean_end_lag=        43B  max_lag=      2136B  (n=29)
    slow    : end_lag=         0B  max_lag=       832B

  scenario=one_crashed   N=  5  transport=push      
    healthy: mean_end_lag=   3463428B  max_lag=   3463512B  (n=4)
    crashed : end_lag=   3463568B  max_lag=   3463568B

  scenario=one_crashed   N=  5  transport=poll@100ms
    healthy: mean_end_lag=       416B  max_lag=      2520B  (n=4)
    crashed : end_lag=    571912B  max_lag=    571912B

  scenario=one_crashed   N= 30  transport=push      
    healthy: mean_end_lag=    366513B  max_lag=    367328B  (n=29)
    crashed : end_lag=    367384B  max_lag=    367384B

  scenario=one_crashed   N= 30  transport=poll@100ms
    healthy: mean_end_lag=       473B  max_lag=      2288B  (n=29)
    crashed : end_lag=    450808B  max_lag=    450808B

Captured TSV: docs/benchmarks/scale-retention-2026-06-15.tsv
