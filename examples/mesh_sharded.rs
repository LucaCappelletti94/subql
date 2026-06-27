//! Mesh topology T3: sharded workload across M PGs.
//!
//! M independent PG containers, each holding a disjoint key range
//! (each writer reserves `100M` IDs above the previous via the
//! substrate's `mesh_writer_rt`). The G2 geometry is the only one
//! that makes sense here: one subscription's predicate must match
//! events on any shard, so the engine ingests M `CdcSource`s in
//! parallel via [`MultiSourceFanIn`].
//!
//! What T3 measures that T1 G2 does not. T1 G2 reports per-source
//! median latency and the cross-source skew of those medians. T3
//! adds **shard-fairness**: the ratio `per_source_drain_rate /
//! per_source_write_rate` for each shard. A ratio close to 1.0
//! across all sources means the fan-in is interleaving fairly; a
//! ratio significantly below 1.0 on one or more sources means the
//! engine drained another shard before getting to that one. This
//! is the architectural soundness check for the "one engine,
//! many sources" geometry.
//!
//! Sweeps `M in {1, 2, 4}`, uniform load only (skewed is what T1
//! already measured for noisy-neighbour effects). 5 consumers, 2
//! trials, 20 s window per cell.
//!
//! Long-form TSV output: `docs/benchmarks/mesh-sharded-2026-06-16.tsv`.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example mesh_sharded --features pg-streaming
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
    resolve_table_ids, spawn_mesh, MeshNode, MultiSourceFanIn, TaggedEvent, TsvWriter,
    WriterProfile,
};
use subql::{PgStreamingCdcSource, PgStreamingConfig};

const M_VALUES: &[usize] = &[1, 2, 4];
const N_CONSUMERS: usize = 5;
const TRIALS_PER_CELL: usize = 2;
const WINDOW: Duration = Duration::from_secs(20);
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const BUFFER_CAPACITY: usize = 4_096;
const PER_SHARD_RATE: u64 = 1_000;

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

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt")
}

fn main() {
    assert_docker_available();
    println!("=== mesh topology T3: sharded workload (G2 only) ===");
    println!(
        "M in {M_VALUES:?}, N={N_CONSUMERS} consumers, {TRIALS_PER_CELL} trials, \
         {}s window. Per-shard rate: {PER_SHARD_RATE} ev/s.",
        WINDOW.as_secs()
    );
    println!();

    let tsv_path = "docs/benchmarks/mesh-sharded-2026-06-16.tsv";
    let mut tsv = TsvWriter::open(tsv_path).expect("open TSV");

    let catalog = parser_db();
    let table_ids = resolve_table_ids(&catalog);
    let orders_table_id = table_ids.orders;

    let mut id_counter: i64 = 1_000_000;

    for &m in M_VALUES {
        let scale_key = format!("M{m}_sharded_uniform");
        let cell_key = "G2_push".to_string();
        let mut per_trial_min_fairness: Vec<f64> = Vec::new();
        let mut per_trial_max_skew_ms: Vec<f64> = Vec::new();
        let mut per_trial_aggregate_drain: Vec<f64> = Vec::new();

        for trial in 0..TRIALS_PER_CELL {
            id_counter += 500_000_000;
            let id_base = id_counter;
            let publication = format!("mesh_sharded_pub_t{trial}");
            let mut nodes = spawn_mesh(m, &publication);

            let label_prefix = format!("mesh_sharded_m{m}_t{trial}");
            for node in &mut nodes {
                for c in 0..N_CONSUMERS {
                    let slot = format!("{label_prefix}_pg{}_n{N_CONSUMERS}_c{c}", node.source_id);
                    common::create_pgoutput_slot(&mut node.dml_conn, &slot);
                }
            }

            let specs = node_specs(&nodes);
            let writer_fut = mesh_writer_rt(
                &nodes,
                WriterProfile::Uniform {
                    rate_per_pg: PER_SHARD_RATE,
                },
                WINDOW,
                id_base,
                orders_table_id,
            );

            let rt = current_thread_rt();
            let (per_source_lats, per_source_drained, total_drained) = rt.block_on(run_window(
                specs,
                label_prefix.clone(),
                publication.clone(),
                Arc::clone(&catalog),
                writer_fut,
            ));

            // Fairness: per-source drain rate over per-source write
            // rate. Per-source write rate = PER_SHARD_RATE; per-source
            // drain rate = drained_count / window. We deliberately
            // include the full window (no drain-grace inflation) so
            // a fairness <1 means the consumer truly did not keep up
            // with that shard.
            let per_source_fairness: Vec<f64> = per_source_drained
                .iter()
                .map(|c| (*c as f64 / WINDOW.as_secs_f64()) / PER_SHARD_RATE as f64)
                .collect();
            let min_fairness = per_source_fairness
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
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
            let valid: Vec<f64> = per_source_median_ms
                .iter()
                .copied()
                .filter(|x| x.is_finite())
                .collect();
            let max_skew_ms = if valid.len() < 2 {
                0.0
            } else {
                valid.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                    - valid.iter().copied().fold(f64::INFINITY, f64::min)
            };
            let aggregate_drain = total_drained as f64 / WINDOW.as_secs_f64();

            println!(
                "  M={m:>2}  trial{trial}  drain={aggregate_drain:>6.0}/s  \
                 min_fairness={min_fairness:>5.2}  skew={max_skew_ms:>6.2}ms",
            );

            for (idx, fairness) in per_source_fairness.iter().enumerate() {
                tsv.row(
                    "mesh_sharded",
                    &scale_key,
                    &cell_key,
                    &format!("trial{trial}_source{idx}_fairness"),
                    *fairness,
                );
            }
            for (idx, med) in per_source_median_ms.iter().enumerate() {
                tsv.row(
                    "mesh_sharded",
                    &scale_key,
                    &cell_key,
                    &format!("trial{trial}_source{idx}_median_ms"),
                    *med,
                );
            }
            tsv.row(
                "mesh_sharded",
                &scale_key,
                &cell_key,
                &format!("trial{trial}_min_fairness"),
                min_fairness,
            );
            tsv.row(
                "mesh_sharded",
                &scale_key,
                &cell_key,
                &format!("trial{trial}_cross_source_skew_ms"),
                max_skew_ms,
            );
            tsv.row(
                "mesh_sharded",
                &scale_key,
                &cell_key,
                &format!("trial{trial}_aggregate_drain_per_sec"),
                aggregate_drain,
            );

            per_trial_min_fairness.push(min_fairness);
            per_trial_max_skew_ms.push(max_skew_ms);
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
            "mesh_sharded",
            &scale_key,
            &cell_key,
            "across_trial_min_fairness_mean",
            avg(&per_trial_min_fairness),
        );
        tsv.row(
            "mesh_sharded",
            &scale_key,
            &cell_key,
            "across_trial_min_fairness_std",
            stddev(&per_trial_min_fairness),
        );
        tsv.row(
            "mesh_sharded",
            &scale_key,
            &cell_key,
            "across_trial_cross_source_skew_mean_ms",
            avg(&per_trial_max_skew_ms),
        );
        tsv.row(
            "mesh_sharded",
            &scale_key,
            &cell_key,
            "across_trial_aggregate_drain_mean",
            avg(&per_trial_aggregate_drain),
        );
        tsv.row_int(
            "mesh_sharded",
            &scale_key,
            &cell_key,
            "M",
            i64::try_from(m).unwrap_or(0),
        );
        println!();
    }

    println!("Captured TSV: {tsv_path}");
}

async fn run_window<F>(
    specs: Vec<NodeSpec>,
    label_prefix: String,
    publication: String,
    catalog: Arc<subql::ParserDB>,
    writer_fut: F,
) -> (Vec<Vec<Duration>>, Vec<usize>, usize)
where
    F: std::future::Future<Output = Vec<common::CommitMap>> + Send + 'static,
{
    let n_sources = specs.len();
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
                .expect("connect sharded source");
            sources.push((spec.source_id, src));
        }
        fanins.push(MultiSourceFanIn::spawn(sources));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let commits = writer_fut.await;
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
    let mut per_source_drained: Vec<usize> = vec![0; n_sources];
    for ev in &all_observed {
        let idx = ev.source_id as usize;
        if idx < n_sources {
            per_source_drained[idx] += 1;
        }
    }
    (per_source_lats, per_source_drained, total_drained)
}
