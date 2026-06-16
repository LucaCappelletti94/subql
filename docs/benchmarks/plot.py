#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "matplotlib>=3.8",
#     "pandas>=2.2",
# ]
# ///
"""Generate plots from the scale-benchmark TSV captures.

Reads:
    docs/benchmarks/scale-throughput-2026-06-15.tsv
    docs/benchmarks/scale-consumers-2026-06-15.tsv
    docs/benchmarks/scale-retention-2026-06-15.tsv

Writes:
    docs/benchmarks/plots/scale-throughput-latency.png
    docs/benchmarks/plots/scale-throughput-ceiling.png
    docs/benchmarks/plots/scale-consumers-latency.png
    docs/benchmarks/plots/scale-consumers-server-load.png
    docs/benchmarks/plots/scale-retention-timeseries.png

Run with: ./docs/benchmarks/plot.py    (uv shebang takes care of the venv)
Or:       uv run docs/benchmarks/plot.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

BENCH_DIR = Path(__file__).resolve().parent
PLOTS_DIR = BENCH_DIR / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

THROUGHPUT_TSV = BENCH_DIR / "scale-throughput-2026-06-15.tsv"
CONSUMERS_TSV = BENCH_DIR / "scale-consumers-2026-06-15.tsv"
RETENTION_TSV = BENCH_DIR / "scale-retention-2026-06-15.tsv"

# Consistent color/marker scheme across plots.
TRANSPORT_STYLE = {
    "push": {"color": "#1f77b4", "marker": "o", "label": "push"},
    "poll@10ms": {"color": "#2ca02c", "marker": "s", "label": "poll @ 10 ms"},
    "poll@100ms": {"color": "#ff7f0e", "marker": "^", "label": "poll @ 100 ms"},
    "poll@1000ms": {"color": "#d62728", "marker": "v", "label": "poll @ 1000 ms"},
}


def load_tsv(path: Path) -> pd.DataFrame:
    """Read one of the scale-benchmark TSVs into a long-form DataFrame."""
    return pd.read_csv(path, sep="\t")


def pivot_cell_metric(df: pd.DataFrame) -> pd.DataFrame:
    """Reshape long form into wide form keyed on (scale_key, cell_key)."""
    return df.pivot_table(
        index=["scale_key", "cell_key"],
        columns="metric",
        values="value",
        aggfunc="first",
    ).reset_index()


def plot_throughput_latency() -> Path:
    df = pivot_cell_metric(load_tsv(THROUGHPUT_TSV))
    df["rate"] = df["scale_key"].str.removeprefix("rate=").astype(int)
    df = df.sort_values("rate")
    fig, ax = plt.subplots(figsize=(8, 5))
    for transport, style in TRANSPORT_STYLE.items():
        sub = df[df["cell_key"] == transport]
        if sub.empty:
            continue
        ax.plot(
            sub["rate"],
            sub["median_ms"],
            marker=style["marker"],
            color=style["color"],
            label=style["label"],
            linewidth=2,
            markersize=8,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Producer rate (events/sec)")
    ax.set_ylabel("Per-event median latency (ms)")
    ax.set_title("Latency vs event rate — push wins by ~P/2 at every polling cadence")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(loc="upper right")
    fig.tight_layout()
    out = PLOTS_DIR / "scale-throughput-latency.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def plot_throughput_ceiling() -> Path:
    df = pivot_cell_metric(load_tsv(THROUGHPUT_TSV))
    df["rate"] = df["scale_key"].str.removeprefix("rate=").astype(int)
    df = df.sort_values("rate")
    fig, ax = plt.subplots(figsize=(8, 5))
    # Reference: y=x (perfect keep-up).
    rates = sorted(df["rate"].unique())
    ax.plot(
        rates,
        rates,
        color="gray",
        linestyle="--",
        alpha=0.5,
        label="ideal (drain = producer)",
    )
    for transport, style in TRANSPORT_STYLE.items():
        sub = df[df["cell_key"] == transport]
        if sub.empty:
            continue
        ax.plot(
            sub["rate"],
            sub["drain_rate"],
            marker=style["marker"],
            color=style["color"],
            label=style["label"],
            linewidth=2,
            markersize=8,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Producer rate (events/sec)")
    ax.set_ylabel("Sustained drain rate (events/sec)")
    ax.set_title("Throughput ceiling — polling plateaus, push scales with producer")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(loc="lower right")
    fig.tight_layout()
    out = PLOTS_DIR / "scale-throughput-ceiling.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def _consumers_pivot(experiment: str) -> pd.DataFrame:
    """Pivot the scale_consumers TSV restricted to one experiment label."""
    df = pivot_cell_metric(load_tsv(CONSUMERS_TSV))
    df = df[df["scale_key"].str.startswith(experiment + "_n")].copy()
    df["N"] = df["scale_key"].str.removeprefix(experiment + "_n").astype(int)
    return df.sort_values(["N", "cell_key"])


def plot_consumers_latency() -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    labels = {
        "exp_a_per_consumer_producers": (
            "Experiment A: per-consumer producers (rate = N x 200/s)"
        ),
        "exp_b_shared_producer": (
            "Experiment B: single shared producer at 1000/s"
        ),
    }
    style_push = TRANSPORT_STYLE["push"]
    style_poll = TRANSPORT_STYLE["poll@100ms"]
    for ax, (experiment, title) in zip(axes, labels.items(), strict=True):
        df = _consumers_pivot(experiment)
        push = df[df["cell_key"] == "push"]
        poll = df[df["cell_key"] == "poll@100ms"]
        ax.plot(
            push["N"],
            push["median_ms"],
            marker=style_push["marker"],
            color=style_push["color"],
            linewidth=2,
            markersize=8,
            label=style_push["label"],
        )
        ax.plot(
            poll["N"],
            poll["median_ms"],
            marker=style_poll["marker"],
            color=style_poll["color"],
            linewidth=2,
            markersize=8,
            label="poll @ 100 ms",
        )
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("Consumer count N")
        ax.set_title(title)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(loc="upper left")
    axes[0].set_ylabel("Pooled per-event median latency (ms)")
    fig.suptitle(
        "Per-consumer latency vs consumer count "
        "(push stays at wire-RTT floor under controlled rate)"
    )
    fig.tight_layout()
    out = PLOTS_DIR / "scale-consumers-latency.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def plot_consumers_server_load() -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)
    style_push = TRANSPORT_STYLE["push"]
    style_poll = TRANSPORT_STYLE["poll@100ms"]
    labels = {
        "exp_a_per_consumer_producers": (
            "Experiment A: rate scales with N"
        ),
        "exp_b_shared_producer": (
            "Experiment B: shared producer at 1000/s"
        ),
    }
    for ax, (experiment, title) in zip(axes, labels.items(), strict=True):
        df = _consumers_pivot(experiment)
        push = df[df["cell_key"] == "push"]
        poll = df[df["cell_key"] == "poll@100ms"]
        ax.plot(
            push["N"],
            push["xact_commit_per_sec"],
            marker=style_push["marker"],
            color=style_push["color"],
            linewidth=2,
            markersize=8,
            label="push xact/s",
        )
        ax.plot(
            poll["N"],
            poll["xact_commit_per_sec"],
            marker=style_poll["marker"],
            color=style_poll["color"],
            linewidth=2,
            markersize=8,
            label="poll@100ms xact/s",
        )
        ax.set_xscale("log")
        ax.set_xlabel("Consumer count N")
        ax.set_ylabel("xact_commit per sec (PG server)")
        ax.set_title(title)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(loc="upper left")
    fig.suptitle(
        "PG server-side commit rate vs consumer count "
        "(polling generates linearly more SQL traffic with N)"
    )
    fig.tight_layout()
    out = PLOTS_DIR / "scale-consumers-server-load.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def plot_retention_timeseries() -> Path:
    """Per-scenario per-transport time-series of slot lag.

    Layout: 3 scenarios (cols) x 2 transports (rows). Each cell shows
    healthy-mean lag and the special slot's lag over the 90 s window
    for N=30 (the more interesting case).
    """
    df_long = load_tsv(RETENTION_TSV)
    # Extract per-timepoint lag values: metric of the form 'lag_t{t}_bytes'.
    lag_rows = df_long[df_long["metric"].str.match(r"lag_t\d+_bytes")].copy()
    lag_rows["t"] = (
        lag_rows["metric"].str.extract(r"lag_t(\d+)_bytes")[0].astype(int)
    )

    scenarios = ["all_healthy", "one_slow", "one_crashed"]
    scenario_titles = {
        "all_healthy": "A: all healthy",
        "one_slow": "B: one slow (50ms/event)",
        "one_crashed": "C: one crashed at t=30s",
    }
    transports = ["push", "poll@100ms"]
    N = 30  # focus on N=30
    fig, axes = plt.subplots(2, 3, figsize=(16, 8), sharex=True, sharey="row")

    for ti, transport in enumerate(transports):
        for si, scenario in enumerate(scenarios):
            ax = axes[ti][si]
            scale_key = f"{scenario}_n{N}"
            sub = lag_rows[lag_rows["scale_key"] == scale_key]
            sub = sub[sub["cell_key"].str.startswith(transport)]
            if sub.empty:
                ax.text(0.5, 0.5, "no data", ha="center", va="center")
                continue
            # Classify slots: 'c0_slow', 'c0_crashed' = special; others = healthy.
            sub = sub.assign(
                role=sub["cell_key"].str.extract(
                    r"_c\d+_(\w+)$"
                )[0].fillna("healthy")
            )
            healthy_pts = (
                sub[sub["role"] == "healthy"]
                .groupby("t")["value"]
                .mean()
                .sort_index()
            )
            ax.plot(
                healthy_pts.index,
                healthy_pts.values / 1024.0,
                color="#1f77b4",
                linewidth=2,
                label=f"healthy avg (n={N - 1})",
            )
            special = sub[sub["role"].isin(["slow", "crashed"])]
            if not special.empty:
                special_pts = (
                    special.groupby("t")["value"].mean().sort_index()
                )
                role_name = special["role"].iloc[0]
                ax.plot(
                    special_pts.index,
                    special_pts.values / 1024.0,
                    color="#d62728",
                    linewidth=2,
                    linestyle="--",
                    label=f"{role_name} slot",
                )
                if role_name == "crashed":
                    ax.axvline(30, color="gray", linestyle=":", alpha=0.5)
            ax.set_title(f"{transport} - {scenario_titles[scenario]}")
            ax.set_xlabel("time (s)")
            if si == 0:
                ax.set_ylabel("slot lag (KB)")
            ax.grid(True, alpha=0.3)
            ax.legend(loc="upper left", fontsize=8)
    fig.suptitle(
        "WAL retention dynamics: per-slot lag over time, N=30 "
        "(push consumers do NOT ack in this benchmark)"
    )
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = PLOTS_DIR / "scale-retention-timeseries.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def main() -> None:
    outputs = [
        plot_throughput_latency(),
        plot_throughput_ceiling(),
        plot_consumers_latency(),
        plot_consumers_server_load(),
        plot_retention_timeseries(),
    ]
    print("Generated plots:")
    for path in outputs:
        rel = path.relative_to(BENCH_DIR.parent.parent)
        print(f"  {rel}")


if __name__ == "__main__":
    main()
