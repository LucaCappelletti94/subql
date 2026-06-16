Run: cargo run --release --example scale_retention --features pg-streaming
Date: 2026-06-16 (commons_server)
Host: commons_server
PG container: subql-test/postgres-wal2json:16

Scale benchmark: WAL retention dynamics
90 s window, slot lag sampled every 5 s, producer @ 500 ev/s.

  scenario=all_healthy   N=  5  transport=push      
    healthy: mean_end_lag=   5851928B  max_lag=   5852040B  (n=5)

  scenario=all_healthy   N=  5  transport=poll@100ms
    healthy: mean_end_lag=        41B  max_lag=      6608B  (n=5)

  scenario=all_healthy   N= 30  transport=push      
    healthy: mean_end_lag=   5413300B  max_lag=   5414112B  (n=30)

  scenario=all_healthy   N= 30  transport=poll@100ms
    healthy: mean_end_lag=       160B  max_lag=      5544B  (n=30)

  scenario=one_slow      N=  5  transport=push      
    healthy: mean_end_lag=   5618028B  max_lag=   5618112B  (n=4)
    slow    : end_lag=   5618168B  max_lag=   5618168B

  scenario=one_slow      N=  5  transport=poll@100ms
    healthy: mean_end_lag=       474B  max_lag=      6888B  (n=4)
    slow    : end_lag=   3401936B  max_lag=   3401936B

  scenario=one_slow      N= 30  transport=push      
    healthy: mean_end_lag=   5445110B  max_lag=   5447280B  (n=29)
    slow    : end_lag=   5447336B  max_lag=   5447336B

  scenario=one_slow      N= 30  transport=poll@100ms
    healthy: mean_end_lag=       310B  max_lag=      5984B  (n=29)
    slow    : end_lag=    880112B  max_lag=   1592848B

  scenario=one_crashed   N=  5  transport=push      
    healthy: mean_end_lag=   5648436B  max_lag=   5648520B  (n=4)
    crashed : end_lag=   5648576B  max_lag=   5648576B

  scenario=one_crashed   N=  5  transport=poll@100ms
    healthy: mean_end_lag=        52B  max_lag=      6816B  (n=4)
    crashed : end_lag=   3399136B  max_lag=   3399136B

  scenario=one_crashed   N= 30  transport=push      
    healthy: mean_end_lag=   5483571B  max_lag=   5484368B  (n=29)
    crashed : end_lag=   5484424B  max_lag=   5484424B

  scenario=one_crashed   N= 30  transport=poll@100ms
    healthy: mean_end_lag=         0B  max_lag=      6400B  (n=29)
    crashed : end_lag=   2630064B  max_lag=   2630064B

Captured TSV: docs/benchmarks/scale-retention-2026-06-15.tsv
