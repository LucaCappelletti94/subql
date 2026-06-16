//! Mesh topology T2: PG-native logical replication tree.
//!
//! One primary + (M-1) PG subscribers connected to it via PostgreSQL's
//! native `CREATE PUBLICATION` / `CREATE SUBSCRIPTION` mechanism. All
//! writes commit on the primary. subql attaches an independent
//! `CdcSource` to every PG in the tree (primary AND subscribers) and
//! measures per-node observation latency for the SAME logical event.
//!
//! The point: quantify the replication-delay tax that the subscribers
//! pay relative to the primary's observation. If a downstream service
//! reads CDC from a read-replica, this latency tax is the cost of
//! that architectural choice.
//!
//! Networking. testcontainers spins each PG up in its own Docker
//! container. Subscribers need to reach the primary on PG's logical-
//! replication port (5432 inside the container), so all containers
//! are placed on the same Docker network with deterministic
//! container names and the subscription connection string uses the
//! INTERNAL container hostname + port 5432.
//!
//! Sweeps `M in {1, 2, 4}`. Skewed load is not applicable because
//! only the primary takes writes; in lieu of that cell we run a
//! `writer_burst` scenario at M=4 to measure how subscriber lag
//! grows under a 5x rate spike.
//!
//! Long-form TSV output: `docs/benchmarks/mesh-logical-rep-2026-06-16.tsv`.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example mesh_logical_rep --features pg-streaming
//! ```

#![cfg(feature = "pg-streaming")]
#![allow(
    clippy::unwrap_used,
    clippy::print_stdout,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::format_push_string,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::duration_suboptimal_units,
    clippy::missing_errors_doc,
    clippy::must_use_candidate
)]

#[path = "cdc_bench_common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::{sql_query, Connection, PgConnection, RunQueryDsl};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use common::{
    assert_docker_available, ms_of, parser_db, resolve_table_ids, EventKey, MultiSourceFanIn,
    TaggedEvent, TsvWriter,
};
use subql::{EventKind, PgStreamingCdcSource, PgStreamingConfig};

const M_VALUES: &[usize] = &[1, 2, 4];
const N_CONSUMERS: usize = 3;
const TRIALS_PER_CELL: usize = 2;
const WINDOW: Duration = Duration::from_secs(20);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const BUFFER_CAPACITY: usize = 4_096;
const WRITER_RATE: u64 = 1_000;
const BURST_RATE: u64 = 5_000;

const PG_IMAGE: &str = "subql-test/postgres-wal2json";
const PG_TAG: &str = "16";

struct LrNode {
    #[allow(dead_code)]
    container: Container<GenericImage>,
    host_port: u16,
    #[allow(dead_code)]
    internal_name: String,
    source_id: u16,
}

impl LrNode {
    fn sql_url(&self) -> String {
        format!(
            "postgres://subql_test:subql_test@127.0.0.1:{}/testdb",
            self.host_port
        )
    }
    fn dml_conn(&self) -> PgConnection {
        PgConnection::establish(&self.sql_url()).expect("PG dml conn")
    }
}

fn rand_suffix() -> String {
    let now = Instant::now().elapsed();
    format!("{}_{}", std::process::id(), now.as_nanos() % 1_000_000_000)
}

fn pg_image_on_network(name: &str, network: &str) -> Container<GenericImage> {
    GenericImage::new(PG_IMAGE, PG_TAG)
        .with_wait_for(WaitFor::message_on_stderr("ready to accept connections"))
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_USER", "subql_test")
        .with_env_var("POSTGRES_PASSWORD", "subql_test")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=384",
            "-c",
            "max_replication_slots=384",
            "-c",
            "max_connections=512",
        ])
        .with_startup_timeout(Duration::from_secs(60))
        .with_network(network.to_string())
        .with_container_name(name.to_string())
        .start()
        .expect("start pg")
}

fn wait_for_replication(sub_conn: &mut PgConnection, sub_idx: usize) {
    use diesel::sql_types::Bool;
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Bool)]
        replicating: bool,
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let row: Result<Row, _> = sql_query(format!(
            "SELECT EXISTS (SELECT 1 FROM pg_subscription_rel \
             WHERE srsubid = (SELECT oid FROM pg_subscription WHERE subname = 'mesh_sub_{sub_idx}') \
             AND srsubstate = 'r')::BOOL AS replicating"
        ))
        .get_result(sub_conn);
        if let Ok(r) = row {
            if r.replicating {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("subscription mesh_sub_{sub_idx} did not reach 'ready' state within 15s");
}

fn spawn_lr_mesh(m: usize, publication: &str) -> Vec<LrNode> {
    let suffix = rand_suffix();
    let network = format!("mesh_lr_net_{suffix}");
    let primary_name = format!("mesh_lr_primary_{suffix}");
    let primary_container = pg_image_on_network(&primary_name, &network);
    let primary_host_port = primary_container
        .get_host_port_ipv4(5432.tcp())
        .expect("primary port");
    let primary_node = LrNode {
        container: primary_container,
        host_port: primary_host_port,
        internal_name: primary_name.clone(),
        source_id: 0,
    };

    let mut conn = primary_node.dml_conn();
    common::setup_schema(&mut conn);
    common::create_publication_all(&mut conn, publication);
    sql_query(
        "INSERT INTO users (id, email, name, last_login_at) \
         VALUES (1, 'sentinel@example.invalid', 'sentinel', NOW()) \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(&mut conn)
    .expect("primary sentinel");

    let mut nodes = vec![primary_node];
    for i in 1..m {
        let sub_name = format!("mesh_lr_sub_{suffix}_{i}");
        let sub_container = pg_image_on_network(&sub_name, &network);
        let sub_host_port = sub_container
            .get_host_port_ipv4(5432.tcp())
            .expect("sub port");
        let mut sub_conn = PgConnection::establish(&format!(
            "postgres://subql_test:subql_test@127.0.0.1:{sub_host_port}/testdb"
        ))
        .expect("sub conn");
        common::setup_schema(&mut sub_conn);
        // Each subscriber also needs its OWN publication so the
        // subql `CdcSource` slots attached to this subscriber can
        // stream replicated INSERTs back out via pgoutput. Without
        // this, the subscriber's slot would have no publication to
        // attach to and would return empty.
        common::create_publication_all(&mut sub_conn, publication);
        // Subscription's initial COPY phase will replicate the
        // sentinel users row from the primary; do not pre-insert
        // here or the COPY would fail on duplicate.
        let sub_query = format!(
            "CREATE SUBSCRIPTION mesh_sub_{i} \
             CONNECTION 'host={primary_name} port=5432 user=subql_test password=subql_test dbname=testdb' \
             PUBLICATION {publication}"
        );
        sql_query(&sub_query)
            .execute(&mut sub_conn)
            .expect("create subscription");
        wait_for_replication(&mut sub_conn, i);
        nodes.push(LrNode {
            container: sub_container,
            host_port: sub_host_port,
            internal_name: sub_name,
            source_id: u16::try_from(i).expect("source_id fits"),
        });
    }
    nodes
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt")
}

fn main() {
    assert_docker_available();
    println!("=== mesh topology T2: PG logical replication tree ===");
    println!(
        "M in {M_VALUES:?}, N={N_CONSUMERS} per node, {TRIALS_PER_CELL} trials, \
         {}s window. Writer rate: {WRITER_RATE} ev/s on primary only.",
        WINDOW.as_secs()
    );
    println!();

    let tsv_path = "docs/benchmarks/mesh-logical-rep-2026-06-16.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV");

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let orders_table_id = table_ids.orders;

    let mut id_counter: i64 = 1_000_000;

    for &m in M_VALUES {
        for rate_label in ["normal", "burst"] {
            if m == 1 && rate_label == "burst" {
                continue;
            }
            let scale_key = format!("M{m}_{rate_label}");
            let cell_key = "primary_writes_push".to_string();
            let mut per_trial_replication_lag_ms: Vec<f64> = Vec::new();
            let mut per_trial_aggregate_drain: Vec<f64> = Vec::new();

            for trial in 0..TRIALS_PER_CELL {
                id_counter += 500_000_000;
                let id_base = id_counter;
                let publication = format!("mesh_lr_pub_m{m}_{rate_label}_t{trial}");
                println!("  M={m:>2}  {rate_label:<6}  trial{trial}  spawning {m} containers...");
                let nodes = spawn_lr_mesh(m, &publication);

                let label_prefix = format!("mesh_lr_m{m}_{rate_label}_t{trial}");
                for node in &nodes {
                    let mut c = node.dml_conn();
                    for ci in 0..N_CONSUMERS {
                        let slot =
                            format!("{label_prefix}_pg{}_n{N_CONSUMERS}_c{ci}", node.source_id);
                        common::create_pgoutput_slot(&mut c, &slot);
                    }
                }

                let writer_rate = match rate_label {
                    "burst" => BURST_RATE,
                    _ => WRITER_RATE,
                };

                let specs: Vec<(u16, String)> = nodes
                    .iter()
                    .map(|n| {
                        (
                            n.source_id,
                            format!(
                                "postgres://subql_test:subql_test@127.0.0.1:{}/testdb",
                                n.host_port
                            ),
                        )
                    })
                    .collect();

                let primary_sql_url = nodes[0].sql_url();

                let rt = current_thread_rt();
                let (per_source_lats, total_drained) = rt.block_on(run_lr_window(
                    specs.clone(),
                    label_prefix.clone(),
                    publication.clone(),
                    Arc::clone(&catalog),
                    primary_sql_url,
                    writer_rate,
                    id_base,
                    orders_table_id,
                ));

                let per_source_median_ms: Vec<f64> = per_source_lats
                    .iter()
                    .map(|v| {
                        if v.is_empty() {
                            f64::NAN
                        } else {
                            let mut s = v.clone();
                            s.sort_unstable();
                            ms_of(s[s.len() / 2])
                        }
                    })
                    .collect();
                let primary_med = per_source_median_ms.first().copied().unwrap_or(f64::NAN);
                let sub_meds: Vec<f64> = per_source_median_ms
                    .iter()
                    .skip(1)
                    .copied()
                    .filter(|x| x.is_finite())
                    .collect();
                let replication_lag_ms = if sub_meds.is_empty() {
                    0.0
                } else {
                    let avg = sub_meds.iter().sum::<f64>() / sub_meds.len() as f64;
                    (avg - primary_med).max(0.0)
                };
                let aggregate_drain = total_drained as f64 / WINDOW.as_secs_f64();

                println!(
                    "    drain={aggregate_drain:>6.0}/s  primary={primary_med:>6.2}ms  \
                     replicas_avg={:>6.2}ms  replication_lag={replication_lag_ms:>6.2}ms",
                    if sub_meds.is_empty() {
                        f64::NAN
                    } else {
                        sub_meds.iter().sum::<f64>() / sub_meds.len() as f64
                    }
                );

                for (idx, med) in per_source_median_ms.iter().enumerate() {
                    tsv.row(
                        "mesh_lr",
                        &scale_key,
                        &cell_key,
                        &format!("trial{trial}_source{idx}_median_ms"),
                        *med,
                    );
                }
                tsv.row(
                    "mesh_lr",
                    &scale_key,
                    &cell_key,
                    &format!("trial{trial}_replication_lag_ms"),
                    replication_lag_ms,
                );
                tsv.row(
                    "mesh_lr",
                    &scale_key,
                    &cell_key,
                    &format!("trial{trial}_aggregate_drain_per_sec"),
                    aggregate_drain,
                );

                drop(per_source_median_ms);
                per_trial_replication_lag_ms.push(replication_lag_ms);
                per_trial_aggregate_drain.push(aggregate_drain);

                drop(rt);
                drop(nodes);
            }

            let avg = |v: &[f64]| {
                if v.is_empty() {
                    0.0
                } else {
                    v.iter().sum::<f64>() / v.len() as f64
                }
            };
            let stddev = |v: &[f64]| {
                if v.len() < 2 {
                    return 0.0;
                }
                let m = avg(v);
                (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (v.len() - 1) as f64).sqrt()
            };
            tsv.row(
                "mesh_lr",
                &scale_key,
                &cell_key,
                "across_trial_replication_lag_mean_ms",
                avg(&per_trial_replication_lag_ms),
            );
            tsv.row(
                "mesh_lr",
                &scale_key,
                &cell_key,
                "across_trial_replication_lag_std_ms",
                stddev(&per_trial_replication_lag_ms),
            );
            tsv.row(
                "mesh_lr",
                &scale_key,
                &cell_key,
                "across_trial_aggregate_drain_mean",
                avg(&per_trial_aggregate_drain),
            );
            tsv.row_int(
                "mesh_lr",
                &scale_key,
                &cell_key,
                "M",
                i64::try_from(m).unwrap_or(0),
            );
        }
        println!();
    }

    println!("Captured TSV: {tsv_path}");
}

#[allow(clippy::similar_names)]
async fn run_lr_window(
    specs: Vec<(u16, String)>,
    label_prefix: String,
    publication: String,
    catalog: Arc<subql::ParserDB>,
    primary_sql_url: String,
    writer_rate: u64,
    id_base: i64,
    orders_table_id: u32,
) -> (Vec<Vec<Duration>>, usize) {
    // Spawn N MultiSourceFanIn instances, each with sources from
    // every node. Same slot footprint as G2 from `mesh_independent`.
    let mut fanins: Vec<MultiSourceFanIn> = Vec::with_capacity(N_CONSUMERS);
    for c in 0..N_CONSUMERS {
        let mut sources: Vec<(u16, PgStreamingCdcSource<subql::ParserDB>)> =
            Vec::with_capacity(specs.len());
        for (source_id, url) in &specs {
            let slot = format!("{label_prefix}_pg{source_id}_n{N_CONSUMERS}_c{c}");
            let config = PgStreamingConfig::new(url.clone(), &slot, &publication)
                .status_interval(STATUS_INTERVAL)
                .buffer_capacity(BUFFER_CAPACITY);
            let src = PgStreamingCdcSource::connect(config, Arc::clone(&catalog))
                .await
                .expect("connect lr source");
            sources.push((*source_id, src));
        }
        fanins.push(MultiSourceFanIn::spawn(sources));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let writer_fut = drive_primary_writer(
        primary_sql_url,
        writer_rate,
        WINDOW,
        id_base,
        orders_table_id,
    );
    let primary_commits: HashMap<EventKey, Instant> = writer_fut.await;
    let deadline = Instant::now() + Duration::from_secs(5);

    let mut drainers: Vec<tokio::task::JoinHandle<Vec<TaggedEvent>>> = Vec::new();
    for fanin in fanins {
        let MultiSourceFanIn { mut rx, tasks } = fanin;
        let d = tokio::spawn(async move {
            let mut out = Vec::new();
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(ev)) => out.push(ev),
                    Ok(None) | Err(_) => break,
                }
            }
            for t in &tasks {
                t.abort();
            }
            out
        });
        drainers.push(d);
    }

    let mut all_observed: Vec<TaggedEvent> = Vec::new();
    for d in drainers {
        if let Ok(events) = d.await {
            all_observed.extend(events);
        }
    }

    let total_drained = all_observed.len();
    let n_sources = specs.len();
    let mut per_source_lats: Vec<Vec<Duration>> = (0..n_sources).map(|_| Vec::new()).collect();
    let mut seen: Vec<std::collections::HashSet<EventKey>> = (0..n_sources)
        .map(|_| std::collections::HashSet::new())
        .collect();
    for ev in &all_observed {
        let key = (ev.table_id, ev.pk_int, ev.kind);
        let idx = ev.source_id as usize;
        if idx >= n_sources {
            continue;
        }
        if !seen[idx].insert(key) {
            continue;
        }
        if let Some(commit_at) = primary_commits.get(&key) {
            per_source_lats[idx].push(ev.observed_at.saturating_duration_since(*commit_at));
        }
    }
    (per_source_lats, total_drained)
}

fn drive_primary_writer(
    pg_url: String,
    rate: u64,
    duration: Duration,
    id_base: i64,
    orders_table_id: u32,
) -> impl std::future::Future<Output = HashMap<EventKey, Instant>> + Send + 'static {
    use diesel::connection::SimpleConnection;
    let (tx, rx) = tokio::sync::oneshot::channel::<HashMap<EventKey, Instant>>();
    std::thread::spawn(move || {
        let mut conn = PgConnection::establish(&pg_url).expect("primary writer conn");
        let batch_size: u64 = if rate >= 5_000 {
            20
        } else if rate >= 1_000 {
            5
        } else {
            1
        };
        let batches_per_sec = rate.div_ceil(batch_size);
        let gap = Duration::from_secs_f64(1.0 / batches_per_sec as f64);
        let end = Instant::now() + duration;
        let mut commits: HashMap<EventKey, Instant> =
            HashMap::with_capacity(((rate as usize) * duration.as_secs() as usize).max(1024));
        let mut id = id_base;
        while Instant::now() < end {
            let batch_start = Instant::now();
            let mut stmts = String::from("BEGIN;");
            let commit_at = Instant::now();
            for _ in 0..batch_size {
                use std::fmt::Write as _;
                let _ = write!(
                    stmts,
                    " INSERT INTO orders (id, user_id, status, total_cents, updated_at) \
                      VALUES ({id}, 1, 'paid', {p}, NOW());",
                    p = id * 100
                );
                commits.insert((orders_table_id, id, EventKind::Insert), commit_at);
                id += 1;
            }
            stmts.push_str(" COMMIT;");
            conn.batch_execute(&stmts)
                .unwrap_or_else(|e| panic!("primary writer batch: {e}"));
            if let Some(remaining) = gap.checked_sub(batch_start.elapsed()) {
                std::thread::sleep(remaining);
            }
        }
        let _ = tx.send(commits);
    });
    async move { rx.await.unwrap_or_default() }
}
