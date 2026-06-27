//! Mesh topology T1: independent peers.
//!
//! M independent PG containers, no PG-to-PG replication. The same
//! schema lives on each, the same publication name covers all three
//! tables on each, and a sentinel `users` row pre-exists on each so
//! the FK-less `orders` INSERT workload can run without referential
//! conflict.
//!
//! Sweeps `M in {1, 2, 4}`, two writer-load shapes (uniform vs
//! skewed-one-hot), and two subscription geometries:
//! - **G1** "one PG per subscription": N independent push consumers
//!   per PG (5 * M total). Each consumer owns its own slot. Events
//!   are observed at the consumer's mpsc and tagged with the
//!   source_id externally.
//! - **G2** "many PGs per subscription": N `MultiSourceFanIn`
//!   instances, each spawning M push sources internally (so the
//!   PG-side slot footprint is the same 5 * M). Each fan-in's
//!   incoming events arrive pre-tagged with the source_id.
//!
//! Both geometries hold the PG-side slot count constant so the only
//! difference is application-side routing. Push transport only; the
//! polling-on-mesh comparison is the deferred follow-up.
//!
//! Captures per-cell:
//! - per-source pooled median latency
//! - aggregate drain (events / sec across all sources / consumers)
//! - cross-source skew (`max_source_median - min_source_median`)
//! - per-source isolation (skewed cell only: quiet vs hot median)
//! - server-side load delta (`xact_commit/s`, slot count, active_pid)
//!
//! Long-form TSV output: `docs/benchmarks/mesh-independent-2026-06-16.tsv`.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example mesh_independent --features pg-streaming
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

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    assert_docker_available, mesh_writer_rt, ms_of, parser_db, pool_mesh_latencies,
    resolve_table_ids, snapshot_mesh_load, spawn_full_event_receiver, spawn_mesh, EventObservation,
    MeshLoadSnapshot, MeshNode, MultiSourceFanIn, TaggedEvent, TsvWriter, WriterProfile,
};
use subql::{PgStreamingCdcSource, PgStreamingConfig};

const M_VALUES: &[usize] = &[1, 2, 4];
const N_CONSUMERS: usize = 5;
const TRIALS_PER_CELL: usize = 2;
const WINDOW: Duration = Duration::from_secs(20);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const BUFFER_CAPACITY: usize = 4_096;

const UNIFORM_RATE: u64 = 1_000;
const HOT_RATE: u64 = 10_000;
const COLD_RATE: u64 = 100;

#[derive(Debug, Clone, Copy)]
enum LoadShape {
    Uniform,
    Skewed,
}

impl LoadShape {
    const fn label(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Skewed => "skewed",
        }
    }

    const fn profile(self) -> WriterProfile {
        match self {
            Self::Uniform => WriterProfile::Uniform {
                rate_per_pg: UNIFORM_RATE,
            },
            Self::Skewed => WriterProfile::Skewed {
                hot_idx: 0,
                hot_rate: HOT_RATE,
                cold_rate: COLD_RATE,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Geometry {
    G1,
    G2,
}

impl Geometry {
    const fn label(self) -> &'static str {
        match self {
            Self::G1 => "G1_one_per_sub",
            Self::G2 => "G2_many_per_sub",
        }
    }

    const fn slot_tag(self) -> &'static str {
        match self {
            Self::G1 => "g1",
            Self::G2 => "g2",
        }
    }
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread tokio runtime")
}

fn main() {
    assert_docker_available();
    println!("=== mesh topology T1: independent peers ===");
    println!(
        "M in {M_VALUES:?}, geometries [G1, G2], load shapes [uniform, skewed], \
         N={N_CONSUMERS} consumers per cell, {TRIALS_PER_CELL} trials, {}s window per trial.",
        WINDOW.as_secs()
    );
    println!();

    let tsv_path = "docs/benchmarks/mesh-independent-2026-06-16.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV");

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let orders_table_id = table_ids.orders;

    let mut id_counter: i64 = 1_000_000;

    for &m in M_VALUES {
        for load in [LoadShape::Uniform, LoadShape::Skewed] {
            for geom in [Geometry::G1, Geometry::G2] {
                let scale_key = format!("M{m}_{}", load.label());
                let cell_key = format!("{}_push", geom.label());
                let mut per_trial_per_source_med_ms: Vec<Vec<f64>> = Vec::new();
                let mut per_trial_aggregate_drain: Vec<f64> = Vec::new();
                let mut per_trial_cross_source_skew_ms: Vec<f64> = Vec::new();
                let mut per_trial_xact_per_sec: Vec<f64> = Vec::new();
                let mut per_trial_max_active_pids: Vec<i64> = Vec::new();
                let mut per_trial_slot_count_total: Vec<i64> = Vec::new();

                for trial in 0..TRIALS_PER_CELL {
                    id_counter += 500_000_000;
                    let id_base = id_counter;
                    let publication = format!("mesh_indep_pub_t{trial}");
                    let mut nodes = spawn_mesh(m, &publication);

                    let label_prefix = format!(
                        "mesh_indep_m{m}_{}_{}_t{trial}",
                        load.label(),
                        geom.slot_tag()
                    );
                    pre_create_slots(&mut nodes, &label_prefix, geom);

                    let before = snapshot_mesh_load(&mut nodes);

                    let rt = current_thread_rt();
                    let (per_source_lats, total_drained) = rt.block_on(run_window(
                        &nodes,
                        &label_prefix,
                        &publication,
                        Arc::clone(&catalog),
                        geom,
                        load,
                        id_base,
                        orders_table_id,
                    ));

                    let after = snapshot_mesh_load(&mut nodes);

                    let metrics = compute_metrics(&per_source_lats, total_drained, &before, &after);

                    println!(
                        "  M={m:>2}  {:<8}  {:<16}  trial{trial}  drain={:>6.0}/s  med_med={:>7.2}ms  skew={:>6.2}ms",
                        load.label(),
                        geom.label(),
                        metrics.aggregate_drain_per_sec,
                        metrics.median_of_source_medians_ms,
                        metrics.cross_source_skew_ms,
                    );

                    for (idx, med) in metrics.per_source_median_ms.iter().enumerate() {
                        tsv.row(
                            "mesh_indep",
                            &scale_key,
                            &cell_key,
                            &format!("trial{trial}_source{idx}_median_ms"),
                            *med,
                        );
                    }
                    tsv.row(
                        "mesh_indep",
                        &scale_key,
                        &cell_key,
                        &format!("trial{trial}_aggregate_drain_per_sec"),
                        metrics.aggregate_drain_per_sec,
                    );
                    tsv.row(
                        "mesh_indep",
                        &scale_key,
                        &cell_key,
                        &format!("trial{trial}_cross_source_skew_ms"),
                        metrics.cross_source_skew_ms,
                    );
                    tsv.row(
                        "mesh_indep",
                        &scale_key,
                        &cell_key,
                        &format!("trial{trial}_total_xact_per_sec"),
                        metrics.total_xact_per_sec,
                    );
                    tsv.row_int(
                        "mesh_indep",
                        &scale_key,
                        &cell_key,
                        &format!("trial{trial}_max_active_pids"),
                        metrics.max_active_pids,
                    );
                    tsv.row_int(
                        "mesh_indep",
                        &scale_key,
                        &cell_key,
                        &format!("trial{trial}_total_slot_count"),
                        metrics.total_slot_count,
                    );

                    per_trial_per_source_med_ms.push(metrics.per_source_median_ms);
                    per_trial_aggregate_drain.push(metrics.aggregate_drain_per_sec);
                    per_trial_cross_source_skew_ms.push(metrics.cross_source_skew_ms);
                    per_trial_xact_per_sec.push(metrics.total_xact_per_sec);
                    per_trial_max_active_pids.push(metrics.max_active_pids);
                    per_trial_slot_count_total.push(metrics.total_slot_count);

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
                let avg_i = |v: &[i64]| {
                    if v.is_empty() {
                        0.0
                    } else {
                        v.iter().map(|x| *x as f64).sum::<f64>() / v.len() as f64
                    }
                };

                let agg_mean = avg(&per_trial_aggregate_drain);
                let skew_mean = avg(&per_trial_cross_source_skew_ms);
                let xact_mean = avg(&per_trial_xact_per_sec);
                tsv.row(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "across_trial_aggregate_drain_mean",
                    agg_mean,
                );
                tsv.row(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "across_trial_cross_source_skew_mean_ms",
                    skew_mean,
                );
                tsv.row(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "across_trial_cross_source_skew_std_ms",
                    stddev(&per_trial_cross_source_skew_ms),
                );
                tsv.row(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "across_trial_total_xact_per_sec_mean",
                    xact_mean,
                );
                tsv.row(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "across_trial_max_active_pids_mean",
                    avg_i(&per_trial_max_active_pids),
                );
                tsv.row(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "across_trial_total_slot_count_mean",
                    avg_i(&per_trial_slot_count_total),
                );
                tsv.row_int(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "M",
                    i64::try_from(m).unwrap_or(0),
                );
                tsv.row_int(
                    "mesh_indep",
                    &scale_key,
                    &cell_key,
                    "consumers_per_cell",
                    i64::try_from(N_CONSUMERS).unwrap_or(0),
                );

                if !per_trial_per_source_med_ms.is_empty() {
                    let max_src = per_trial_per_source_med_ms[0].len();
                    for source_idx in 0..max_src {
                        let vals: Vec<f64> = per_trial_per_source_med_ms
                            .iter()
                            .filter_map(|v| v.get(source_idx).copied())
                            .collect();
                        tsv.row(
                            "mesh_indep",
                            &scale_key,
                            &cell_key,
                            &format!("across_trial_source{source_idx}_median_mean_ms"),
                            avg(&vals),
                        );
                        tsv.row(
                            "mesh_indep",
                            &scale_key,
                            &cell_key,
                            &format!("across_trial_source{source_idx}_median_std_ms"),
                            stddev(&vals),
                        );
                    }
                }
            }
            println!();
        }
    }

    println!("Captured TSV: {tsv_path}");
}

#[derive(Debug, Clone)]
struct CellMetrics {
    per_source_median_ms: Vec<f64>,
    median_of_source_medians_ms: f64,
    cross_source_skew_ms: f64,
    aggregate_drain_per_sec: f64,
    total_xact_per_sec: f64,
    max_active_pids: i64,
    total_slot_count: i64,
}

fn compute_metrics(
    per_source_lats: &[Vec<Duration>],
    total_drained: usize,
    before: &MeshLoadSnapshot,
    after: &MeshLoadSnapshot,
) -> CellMetrics {
    let per_source_median_ms: Vec<f64> = per_source_lats
        .iter()
        .map(|v| {
            if v.is_empty() {
                f64::NAN
            } else {
                let mut sorted = v.clone();
                sorted.sort_unstable();
                ms_of(sorted[sorted.len() / 2])
            }
        })
        .collect();
    let valid: Vec<f64> = per_source_median_ms
        .iter()
        .filter(|x| x.is_finite())
        .copied()
        .collect();
    let median_of_source_medians_ms = if valid.is_empty() {
        f64::NAN
    } else {
        let mut s = valid.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        s[s.len() / 2]
    };
    let cross_source_skew_ms = if valid.len() < 2 {
        0.0
    } else {
        let max = valid.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let min = valid.iter().copied().fold(f64::INFINITY, f64::min);
        max - min
    };
    let window_secs = after
        .per_node
        .first()
        .and_then(|a| {
            before.per_node.first().map(|b| {
                a.taken_at
                    .saturating_duration_since(b.taken_at)
                    .as_secs_f64()
            })
        })
        .unwrap_or_else(|| WINDOW.as_secs_f64())
        .max(1e-6);
    let aggregate_drain_per_sec = total_drained as f64 / window_secs;
    let total_xact_per_sec: f64 = before
        .per_node
        .iter()
        .zip(&after.per_node)
        .map(|(b, a)| (a.xact_commit - b.xact_commit) as f64 / window_secs)
        .sum();
    let max_active_pids = after
        .per_node
        .iter()
        .map(|s| s.active_pid_count)
        .max()
        .unwrap_or(0);
    let total_slot_count = after.per_node.iter().map(|s| s.slot_count).sum();
    CellMetrics {
        per_source_median_ms,
        median_of_source_medians_ms,
        cross_source_skew_ms,
        aggregate_drain_per_sec,
        total_xact_per_sec,
        max_active_pids,
        total_slot_count,
    }
}

fn pre_create_slots(nodes: &mut [MeshNode], label_prefix: &str, _geom: Geometry) {
    // Slot count per PG = N for G1 (each consumer owns one slot on
    // its PG) and N for G2 (each of N MultiSourceFanIns spawns one
    // source per PG). Same per-PG footprint either way; geometry is
    // already encoded in `label_prefix`.
    for node in nodes {
        for c in 0..N_CONSUMERS {
            let slot = format!("{label_prefix}_pg{}_n{N_CONSUMERS}_c{c}", node.source_id);
            common::create_pgoutput_slot(&mut node.dml_conn, &slot);
        }
    }
}

struct NodeSpec {
    source_id: u16,
    repl_url: String,
}

fn node_specs(nodes: &[MeshNode]) -> Vec<NodeSpec> {
    nodes
        .iter()
        .map(|n| NodeSpec {
            source_id: n.source_id,
            repl_url: n.repl_url.clone(),
        })
        .collect()
}

fn run_window(
    nodes: &[MeshNode],
    label_prefix: &str,
    publication: &str,
    catalog: Arc<subql::ParserDB>,
    geom: Geometry,
    load: LoadShape,
    id_base: i64,
    orders_table_id: u32,
) -> impl std::future::Future<Output = (Vec<Vec<Duration>>, usize)> + Send + 'static {
    let specs = node_specs(nodes);
    let writer_fut = mesh_writer_rt(nodes, load.profile(), WINDOW, id_base, orders_table_id);
    let label = label_prefix.to_string();
    let pub_ = publication.to_string();
    async move {
        match geom {
            Geometry::G1 => {
                run_g1_window(specs, label, pub_, catalog, writer_fut, orders_table_id).await
            }
            Geometry::G2 => {
                run_g2_window(specs, label, pub_, catalog, writer_fut, orders_table_id).await
            }
        }
    }
}

async fn run_g1_window<F>(
    specs: Vec<NodeSpec>,
    label_prefix: String,
    publication: String,
    catalog: Arc<subql::ParserDB>,
    writer_fut: F,
    _orders_table_id: u32,
) -> (Vec<Vec<Duration>>, usize)
where
    F: std::future::Future<Output = Vec<common::CommitMap>> + Send + 'static,
{
    use common::CommitMap;

    // Spawn N independent push consumers per node. Each consumer's
    // events are observed via spawn_full_event_receiver and tagged
    // externally with the node's source_id.
    let mut per_pg_handles: Vec<
        Vec<(
            tokio::sync::mpsc::UnboundedReceiver<EventObservation>,
            tokio::task::JoinHandle<()>,
        )>,
    > = Vec::with_capacity(specs.len());
    for spec in &specs {
        let mut handles = Vec::with_capacity(N_CONSUMERS);
        for c in 0..N_CONSUMERS {
            let slot = format!("{label_prefix}_pg{}_n{N_CONSUMERS}_c{c}", spec.source_id);
            let config = PgStreamingConfig::new(spec.repl_url.clone(), &slot, &publication)
                .status_interval(STATUS_INTERVAL)
                .buffer_capacity(BUFFER_CAPACITY);
            let source = PgStreamingCdcSource::connect(config, Arc::clone(&catalog))
                .await
                .expect("connect G1 source");
            let (rx, task) = spawn_full_event_receiver(source);
            handles.push((rx, task));
        }
        per_pg_handles.push(handles);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let commits: Vec<CommitMap> = writer_fut.await;
    let deadline = Instant::now() + Duration::from_secs(5);

    // Drain each consumer concurrently and tag events with the
    // node's source_id at drain time.
    let mut drainers: Vec<tokio::task::JoinHandle<(usize, Vec<TaggedEvent>)>> = Vec::new();
    for (pg_idx, handles) in per_pg_handles.into_iter().enumerate() {
        for (mut rx, task) in handles {
            let source_id = u16::try_from(pg_idx).unwrap();
            let d = tokio::spawn(async move {
                let mut out = Vec::new();
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, rx.recv()).await {
                        Ok(Some(ob)) => out.push(TaggedEvent {
                            source_id,
                            table_id: ob.table_id,
                            pk_int: ob.pk_int,
                            kind: ob.kind,
                            observed_at: ob.observed_at,
                        }),
                        Ok(None) | Err(_) => break,
                    }
                }
                task.abort();
                (pg_idx, out)
            });
            drainers.push(d);
        }
    }

    let mut all_observed: Vec<TaggedEvent> = Vec::new();
    for d in drainers {
        if let Ok((_pg_idx, events)) = d.await {
            all_observed.extend(events);
        }
    }

    let total_drained = all_observed.len();
    let per_source_lats = pool_mesh_latencies(&commits, &all_observed);
    (per_source_lats, total_drained)
}

async fn run_g2_window<F>(
    specs: Vec<NodeSpec>,
    label_prefix: String,
    publication: String,
    catalog: Arc<subql::ParserDB>,
    writer_fut: F,
    _orders_table_id: u32,
) -> (Vec<Vec<Duration>>, usize)
where
    F: std::future::Future<Output = Vec<common::CommitMap>> + Send + 'static,
{
    use common::CommitMap;

    // For each of N consumers, spawn one PgStreamingCdcSource per PG
    // (M sources) and combine them with MultiSourceFanIn. So PG-side
    // we have M * N slots, same as G1.
    let mut fanins: Vec<MultiSourceFanIn> = Vec::with_capacity(N_CONSUMERS);
    for c in 0..N_CONSUMERS {
        let mut sources: Vec<(u16, PgStreamingCdcSource<subql::ParserDB>)> =
            Vec::with_capacity(specs.len());
        for spec in &specs {
            let slot = format!("{label_prefix}_pg{}_n{N_CONSUMERS}_c{c}", spec.source_id);
            let config = PgStreamingConfig::new(spec.repl_url.clone(), &slot, &publication)
                .status_interval(STATUS_INTERVAL)
                .buffer_capacity(BUFFER_CAPACITY);
            let src = PgStreamingCdcSource::connect(config, Arc::clone(&catalog))
                .await
                .expect("connect G2 source");
            sources.push((spec.source_id, src));
        }
        fanins.push(MultiSourceFanIn::spawn(sources));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let commits: Vec<CommitMap> = writer_fut.await;
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
    let per_source_lats = pool_mesh_latencies(&commits, &all_observed);
    (per_source_lats, total_drained)
}
