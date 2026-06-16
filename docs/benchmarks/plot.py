#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "matplotlib>=3.8",
#     "pandas>=2.2",
# ]
# ///
"""Generate plots from the scale-benchmark TSV captures.

Each plot shows the across-trial mean as a solid line and the
across-trial min/max range as a shaded band so the eye can
distinguish measurement noise from concrete differences between
cells.

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
ISOLATION_TSV = BENCH_DIR / "scale-isolation-2026-06-16.tsv"
MESH_TSV = BENCH_DIR / "scale-mesh-2026-06-16.tsv"
MESH_INDEP_TSV = BENCH_DIR / "mesh-independent-2026-06-16.tsv"
MESH_LR_TSV = BENCH_DIR / "mesh-logical-rep-2026-06-16.tsv"
MESH_SHARDED_TSV = BENCH_DIR / "mesh-sharded-2026-06-16.tsv"

# Consistent color/marker scheme across plots.
TRANSPORT_STYLE = {
    "push": {"color": "#1f77b4", "marker": "o", "label": "push"},
    "poll@10ms": {"color": "#2ca02c", "marker": "s", "label": "poll @ 10 ms"},
    "poll@100ms": {"color": "#ff7f0e", "marker": "^", "label": "poll @ 100 ms"},
    "poll@1000ms": {"color": "#d62728", "marker": "v", "label": "poll @ 1000 ms"},
}

# Theoretical per-event latency for polling at cadence P is roughly
# `P/2 + wire_RTT` (commits land uniformly within the next polling
# cycle; the polling source then drains and forwards). The wire-RTT
# floor on this hardware is ~3.5 ms (the push median across the rate
# sweep), so the theoretical polling latencies are:
WIRE_RTT_MS = 3.5
POLLING_THEORY = {
    "poll@10ms": WIRE_RTT_MS + 10 / 2,
    "poll@100ms": WIRE_RTT_MS + 100 / 2,
    "poll@1000ms": WIRE_RTT_MS + 1000 / 2,
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


def _aggregate_per_trial(
    df: pd.DataFrame, metric_prefix: str
) -> pd.DataFrame:
    """Collapse per-trial rows (`trial0_<metric>`, `trial1_<metric>`,
    ...) into mean/std/min/max per (scale_key, cell_key). Returns a
    long-form DataFrame with columns: scale_key, cell_key, mean, std,
    min, max, n_trials.
    """
    raw = df.copy()
    pattern = rf"^trial(\d+)_{metric_prefix}$"
    raw = raw[raw["metric"].str.match(pattern)].copy()
    if raw.empty:
        return raw
    grouped = (
        raw.groupby(["scale_key", "cell_key"])["value"]
        .agg(["mean", "std", "min", "max", "count"])
        .reset_index()
    )
    grouped = grouped.rename(columns={"count": "n_trials"})
    # std is NaN when n=1. Fill with 0 so plotting code can use it.
    grouped["std"] = grouped["std"].fillna(0.0)
    return grouped


def _draw_band_line(
    ax,
    x,
    mean,
    minv,
    maxv,
    *,
    color: str,
    marker: str,
    label: str,
) -> None:
    """Plot mean as a line + markers, with the across-trial min-max
    range as a shaded band. No error-bar caps — when the band is
    invisible at the current y-scale, the title's tiny-band footnote
    reports the largest measured std for the reader."""
    ax.fill_between(x, minv, maxv, color=color, alpha=0.25, linewidth=0)
    ax.plot(
        x,
        mean,
        marker=marker,
        color=color,
        linewidth=2,
        markersize=8,
        label=label,
    )


def _tiny_band_footnote(
    agg: pd.DataFrame,
    *,
    unit: str = "ms",
    fraction_threshold: float = 0.01,
) -> str | None:
    """If every cell's across-trial std is below `fraction_threshold`
    of its mean, the bands won't be visible and we return a short
    footnote string. Otherwise return None."""
    if agg.empty:
        return None
    nonzero = agg[agg["mean"] > 0]
    if nonzero.empty:
        return None
    ratio = (nonzero["std"] / nonzero["mean"]).max()
    if ratio < fraction_threshold:
        max_std = float(agg["std"].max())
        return (
            f"all across-trial stds < {fraction_threshold:.0%} of cell mean "
            f"(max std = {max_std:.2f} {unit}); bands smaller than line width"
        )
    return None


def _throughput_latency_agg() -> pd.DataFrame:
    raw = load_tsv(THROUGHPUT_TSV)
    agg = _aggregate_per_trial(raw, "median_ms")
    if agg.empty:
        wide = pivot_cell_metric(raw)
        agg = wide[["scale_key", "cell_key", "median_ms"]].rename(
            columns={"median_ms": "mean"}
        )
        agg["std"] = 0.0
        agg["min"] = agg["mean"]
        agg["max"] = agg["mean"]
        agg["n_trials"] = 1
    agg["rate"] = agg["scale_key"].str.removeprefix("rate=").astype(int)
    return agg.sort_values("rate")


def _plot_throughput_latency_axes(
    ax,
    agg: pd.DataFrame,
    *,
    log_y: bool,
    y_clip: float | None = None,
) -> list[str]:
    """Returns the labels of any cells skipped because the y-axis
    clip would have made them invisible. The caller surfaces them in
    a subtitle so the reader knows what's missing."""
    rates = sorted(agg["rate"].unique())
    skipped: list[str] = []
    for transport, style in TRANSPORT_STYLE.items():
        sub = agg[agg["cell_key"] == transport]
        if sub.empty:
            continue
        # Skip transports whose entire trace exceeds the clip; the
        # legend should not lie about what's drawn.
        if y_clip is not None and sub["min"].min() > y_clip:
            skipped.append(style["label"])
            continue
        _draw_band_line(
            ax,
            sub["rate"].to_numpy(),
            sub["mean"].to_numpy(),
            sub["min"].to_numpy(),
            sub["max"].to_numpy(),
            color=style["color"],
            marker=style["marker"],
            label=style["label"],
        )
    theory_legend_done = False
    for cadence_key, theory_ms in POLLING_THEORY.items():
        if y_clip is not None and theory_ms > y_clip:
            continue
        style = TRANSPORT_STYLE[cadence_key]
        ax.hlines(
            theory_ms,
            xmin=min(rates),
            xmax=max(rates),
            color=style["color"],
            linestyle=":",
            linewidth=1.5,
            alpha=0.6,
            label=(
                f"theory: poll P/2 + wire ({WIRE_RTT_MS:.1f} ms)"
                if not theory_legend_done
                else None
            ),
        )
        theory_legend_done = True
    ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    if y_clip is not None:
        ax.set_ylim(0, y_clip)
    ax.set_xlabel("Producer rate (events/sec)")
    ax.set_ylabel("Per-event median latency (ms)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(loc="upper right", fontsize=8)
    return skipped


def plot_throughput_latency() -> list[Path]:
    agg = _throughput_latency_agg()
    footnote = _tiny_band_footnote(agg, unit="ms")
    outs: list[Path] = []

    # Log-scale view: spans 3 orders of magnitude (3 ms to 600 ms).
    fig, ax = plt.subplots(figsize=(8, 5))
    _plot_throughput_latency_axes(ax, agg, log_y=True)
    title = "Latency vs event rate (log y)"
    if footnote:
        title += f"\n{footnote}"
    ax.set_title(title)
    fig.tight_layout()
    out = PLOTS_DIR / "scale-throughput-latency-log.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    outs.append(out)

    # Linear-scale view: clipped to 200 ms so poll@100ms region and
    # below shows differences cleanly. poll@1000ms sits at ~525 ms
    # and is intentionally hidden in this view (see log version for
    # that cell); the subtitle calls it out so the legend doesn't
    # lie about what's plotted.
    fig, ax = plt.subplots(figsize=(8, 5))
    skipped = _plot_throughput_latency_axes(
        ax, agg, log_y=False, y_clip=200.0
    )
    title = "Latency vs event rate (linear y, clipped at 200 ms)"
    if skipped:
        title += f"\nhidden by clip: {', '.join(skipped)} (see log version)"
    if footnote:
        title += f"\n{footnote}"
    ax.set_title(title)
    fig.tight_layout()
    out = PLOTS_DIR / "scale-throughput-latency-linear.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    outs.append(out)

    return outs


def _throughput_ceiling_agg() -> pd.DataFrame:
    raw = load_tsv(THROUGHPUT_TSV)
    agg = _aggregate_per_trial(raw, "drain_rate")
    if agg.empty:
        wide = pivot_cell_metric(raw)
        agg = wide[["scale_key", "cell_key", "drain_rate"]].rename(
            columns={"drain_rate": "mean"}
        )
        agg["std"] = 0.0
        agg["min"] = agg["mean"]
        agg["max"] = agg["mean"]
        agg["n_trials"] = 1
    agg["rate"] = agg["scale_key"].str.removeprefix("rate=").astype(int)
    return agg.sort_values("rate")


def _plot_throughput_ceiling_axes(ax, agg: pd.DataFrame, *, log_y: bool) -> None:
    rates = sorted(agg["rate"].unique())
    ax.plot(
        rates,
        rates,
        color="gray",
        linestyle="--",
        alpha=0.5,
        label="ideal (drain = producer)",
    )
    for transport, style in TRANSPORT_STYLE.items():
        sub = agg[agg["cell_key"] == transport]
        if sub.empty:
            continue
        _draw_band_line(
            ax,
            sub["rate"].to_numpy(),
            sub["mean"].to_numpy(),
            sub["min"].to_numpy(),
            sub["max"].to_numpy(),
            color=style["color"],
            marker=style["marker"],
            label=style["label"],
        )
    ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    ax.set_xlabel("Producer rate (events/sec)")
    ax.set_ylabel("Sustained drain rate (events/sec)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(loc="lower right")


def plot_throughput_ceiling() -> list[Path]:
    agg = _throughput_ceiling_agg()
    footnote = _tiny_band_footnote(agg, unit="ev/s")
    outs: list[Path] = []
    for log_y, suffix, label in [
        (True, "log", "log y"),
        (False, "linear", "linear y"),
    ]:
        fig, ax = plt.subplots(figsize=(8, 5))
        _plot_throughput_ceiling_axes(ax, agg, log_y=log_y)
        title = f"Throughput ceiling ({label})"
        if footnote:
            title += f"\n{footnote}"
        ax.set_title(title)
        fig.tight_layout()
        out = PLOTS_DIR / f"scale-throughput-ceiling-{suffix}.png"
        fig.savefig(out, dpi=120)
        plt.close(fig)
        outs.append(out)
    return outs


def _consumers_aggregate(experiment: str, metric_prefix: str) -> pd.DataFrame:
    """Aggregate per-trial rows for one consumers experiment."""
    raw = load_tsv(CONSUMERS_TSV)
    agg = _aggregate_per_trial(raw, metric_prefix)
    if agg.empty:
        wide = pivot_cell_metric(raw)
        legacy_key = (
            "median_ms"
            if metric_prefix == "median_ms"
            else metric_prefix
        )
        if legacy_key in wide.columns:
            agg = wide[["scale_key", "cell_key", legacy_key]].rename(
                columns={legacy_key: "mean"}
            )
            agg["std"] = 0.0
            agg["min"] = agg["mean"]
            agg["max"] = agg["mean"]
            agg["n_trials"] = 1
        else:
            return agg
    agg = agg[agg["scale_key"].str.startswith(experiment + "_n")].copy()
    if agg.empty:
        return agg
    agg["N"] = agg["scale_key"].str.removeprefix(experiment + "_n").astype(int)
    return agg.sort_values(["N", "cell_key"])


def _plot_consumers_metric(
    metric_prefix: str,
    ylabel: str,
    title_base: str,
    out_name: str,
    *,
    log_y: bool,
    y_clip: float | None = None,
) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=log_y and y_clip is None)
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
    plot_theory = metric_prefix == "median_ms"
    combined: list[pd.DataFrame] = []
    # (panel_label, plot_label, hidden_N_values)
    panel_skips: list[tuple[str, str, list[int]]] = []
    for ax, (experiment, sub_title) in zip(axes, labels.items(), strict=True):
        df = _consumers_aggregate(experiment, metric_prefix)
        combined.append(df)
        panel_label = "A" if "exp_a" in experiment else "B"
        for cell_label, plot_label, style in (
            ("push", style_push["label"], style_push),
            ("poll@100ms", "poll @ 100 ms", style_poll),
        ):
            sub = df[df["cell_key"] == cell_label]
            if sub.empty:
                continue
            if y_clip is not None:
                # Drop individual data points that exceed the clip
                # rather than skipping the whole line — otherwise
                # exp A's N=1..30 (visible) would either disappear
                # entirely or stretch into a misleading near-vertical
                # spike at N=100.
                hidden = sub.loc[sub["mean"] > y_clip, "N"].astype(int).tolist()
                if hidden:
                    panel_skips.append((panel_label, plot_label, hidden))
                sub = sub[sub["mean"] <= y_clip]
                if sub.empty:
                    continue
            _draw_band_line(
                ax,
                sub["N"].to_numpy(),
                sub["mean"].to_numpy(),
                sub["min"].to_numpy(),
                sub["max"].to_numpy(),
                color=style["color"],
                marker=style["marker"],
                label=plot_label,
            )
        if plot_theory and not df.empty:
            ns = sorted(df["N"].unique())
            theory_ms = POLLING_THEORY["poll@100ms"]
            if y_clip is None or theory_ms <= y_clip:
                ax.hlines(
                    theory_ms,
                    xmin=min(ns),
                    xmax=max(ns),
                    color=style_poll["color"],
                    linestyle=":",
                    linewidth=1.5,
                    alpha=0.6,
                    label=f"theory: poll P/2 + wire ({WIRE_RTT_MS:.1f} ms)",
                )
        ax.set_xscale("log")
        if log_y:
            ax.set_yscale("log")
        if y_clip is not None:
            ax.set_ylim(0, y_clip)
        ax.set_xlabel("Consumer count N")
        ax.set_title(sub_title)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(loc="upper left", fontsize=8)
    axes[0].set_ylabel(ylabel)
    unit = "ms" if metric_prefix == "median_ms" else "xact/s"
    full_df = pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()
    footnote = _tiny_band_footnote(full_df, unit=unit) if not full_df.empty else None
    title = title_base
    if panel_skips:
        # Format like "exp A push N=100; exp A poll @ 100 ms N=100".
        chunks = [
            f"exp {panel} {label} N={','.join(str(n) for n in hidden)}"
            for panel, label, hidden in panel_skips
        ]
        title += f"\nhidden by clip: {'; '.join(chunks)} (see log version)"
    if footnote:
        title += f"\n{footnote}"
    fig.suptitle(title)
    fig.tight_layout()
    out = PLOTS_DIR / out_name
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def plot_consumers_latency() -> list[Path]:
    return [
        _plot_consumers_metric(
            metric_prefix="median_ms",
            ylabel="Per-trial median latency (ms)",
            title_base="Per-consumer latency vs consumer count (log y)",
            out_name="scale-consumers-latency-log.png",
            log_y=True,
        ),
        # Linear view clipped at 200 ms — exp A N=100 collapse to
        # 7900 ms goes off-screen but is fully visible on the log
        # version. The linear panel reveals the near-flat shape of
        # exp_b push and the gentle climb of exp_b polling.
        _plot_consumers_metric(
            metric_prefix="median_ms",
            ylabel="Per-trial median latency (ms)",
            title_base=(
                "Per-consumer latency vs consumer count "
                "(linear y, clipped at 200 ms)"
            ),
            out_name="scale-consumers-latency-linear.png",
            log_y=False,
            y_clip=200.0,
        ),
    ]


def plot_consumers_server_load() -> list[Path]:
    return [
        _plot_consumers_metric(
            metric_prefix="xact_commit_per_sec",
            ylabel="xact_commit per sec (PG server)",
            title_base=(
                "PG server-side commit rate vs consumer count (linear y)"
            ),
            out_name="scale-consumers-server-load-linear.png",
            log_y=False,
        ),
        _plot_consumers_metric(
            metric_prefix="xact_commit_per_sec",
            ylabel="xact_commit per sec (PG server)",
            title_base=(
                "PG server-side commit rate vs consumer count (log y)"
            ),
            out_name="scale-consumers-server-load-log.png",
            log_y=True,
        ),
    ]


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
            healthy_grouped = (
                sub[sub["role"] == "healthy"]
                .groupby("t")["value"]
                .agg(["mean", "min", "max"])
                .sort_index()
            )
            ax.plot(
                healthy_grouped.index,
                healthy_grouped["mean"] / 1024.0,
                color="#1f77b4",
                linewidth=2,
                label=f"healthy mean (n={N - 1})",
            )
            ax.fill_between(
                healthy_grouped.index,
                healthy_grouped["min"] / 1024.0,
                healthy_grouped["max"] / 1024.0,
                color="#1f77b4",
                alpha=0.18,
                linewidth=0,
                label="healthy min/max",
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
        "(shaded = healthy-slot min/max range; push consumers do NOT ack)"
    )
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = PLOTS_DIR / "scale-retention-timeseries.png"
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def _isolation_writers_agg() -> pd.DataFrame:
    """Experiment W: target vs measured writer rate at each N."""
    raw = load_tsv(ISOLATION_TSV)
    raw = raw[raw["scale_key"].str.startswith("exp_w_writers_n")].copy()
    rate_agg = _aggregate_per_trial(raw, "total_actual_rate")
    if rate_agg.empty:
        return rate_agg
    wide = pivot_cell_metric(raw)
    targets = wide.set_index("scale_key")["target_total_rate"].astype(float)
    counts = wide.set_index("scale_key")["writer_count"].astype(int)
    rate_agg["writer_count"] = rate_agg["scale_key"].map(counts)
    rate_agg["target_rate"] = rate_agg["scale_key"].map(targets)
    return rate_agg.sort_values("writer_count")


def plot_isolation_writers() -> list[Path]:
    """Experiment W bar chart: target vs measured aggregate rate at each
    N. Shows that 100 writer backends saturate well below 20 k ev/s
    even with no CDC attached."""
    agg = _isolation_writers_agg()
    if agg.empty:
        return []
    outputs: list[Path] = []
    fig, ax = plt.subplots(figsize=(8, 5))
    n_vals = agg["writer_count"].to_numpy()
    targets = agg["target_rate"].to_numpy()
    means = agg["mean"].to_numpy()
    mins = agg["min"].to_numpy()
    maxs = agg["max"].to_numpy()
    x = range(len(n_vals))
    width = 0.35
    ax.bar([i - width / 2 for i in x], targets, width=width,
           color="#888888", label="target rate")
    ax.bar([i + width / 2 for i in x], means, width=width,
           color="#1f77b4", label="measured rate (mean of 3 trials)",
           yerr=[means - mins, maxs - means], capsize=4)
    ax.set_xticks(list(x))
    ax.set_xticklabels([f"N={n}" for n in n_vals])
    ax.set_ylabel("aggregate write rate (events/sec)")
    ax.set_title("Exp W: writer-side saturation at high N (no CDC consumers)")
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    out = PLOTS_DIR / "scale-isolation-writers.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    outputs.append(out)
    return outputs


def _isolation_cdc_agg() -> pd.DataFrame:
    """Experiment C: median latency by (rate, N, transport) for the
    fixed-rate-1-batched-writer + N CDC consumers setup."""
    raw = load_tsv(ISOLATION_TSV)
    raw = raw[raw["scale_key"].str.startswith("exp_c_cdc_")].copy()
    agg = _aggregate_per_trial(raw, "median_ms")
    if agg.empty:
        return agg
    wide = pivot_cell_metric(raw)
    rates = wide.set_index(["scale_key", "cell_key"])["total_rate"].astype(float)
    counts = wide.set_index(["scale_key", "cell_key"])["consumer_count"].astype(int)
    agg = agg.set_index(["scale_key", "cell_key"])
    agg["total_rate"] = rates
    agg["consumer_count"] = counts
    return agg.reset_index().sort_values(["total_rate", "consumer_count"])


def plot_isolation_cdc() -> list[Path]:
    """Experiment C: per-N median latency at two fixed total rates,
    push vs poll@100ms. Isolates the CDC fanout cost from writer-side
    contention because there is a SINGLE batched writer at fixed rate."""
    agg = _isolation_cdc_agg()
    if agg.empty:
        return []
    outputs: list[Path] = []
    rates = sorted(agg["total_rate"].unique())
    for log_y in (True, False):
        fig, ax = plt.subplots(figsize=(8, 5))
        for transport in ("push", "poll@100ms"):
            for rate in rates:
                sub = agg[(agg["cell_key"] == transport)
                          & (agg["total_rate"] == rate)]
                if sub.empty:
                    continue
                base = TRANSPORT_STYLE[transport]
                marker = "o" if rate == rates[0] else "s"
                color = base["color"]
                alpha = 1.0 if rate == rates[-1] else 0.55
                ax.fill_between(
                    sub["consumer_count"].to_numpy(),
                    sub["min"].to_numpy(),
                    sub["max"].to_numpy(),
                    color=color,
                    alpha=alpha * 0.2,
                    linewidth=0,
                )
                ax.plot(
                    sub["consumer_count"].to_numpy(),
                    sub["mean"].to_numpy(),
                    marker=marker,
                    color=color,
                    alpha=alpha,
                    linewidth=2,
                    markersize=8,
                    label=f"{base['label']} @ {int(rate)} ev/s",
                )
        ax.set_xscale("log")
        if log_y:
            ax.set_yscale("log")
        ax.set_xlabel("CDC consumers (N)")
        ax.set_ylabel("median latency (ms)")
        suffix = "log" if log_y else "linear"
        ax.set_title(
            "Exp C: CDC-side cost — 1 batched writer + N CDC consumers"
            f"   ({suffix} y)"
        )
        ax.legend(fontsize=8)
        ax.grid(True, which="both", alpha=0.3)
        out = PLOTS_DIR / f"scale-isolation-cdc-{suffix}.png"
        fig.tight_layout()
        fig.savefig(out, dpi=150)
        plt.close(fig)
        outputs.append(out)
    return outputs


def _mesh_latency_agg() -> pd.DataFrame:
    raw = load_tsv(MESH_TSV)
    agg = _aggregate_per_trial(raw, "median_ms")
    if agg.empty:
        return agg
    wide = pivot_cell_metric(raw)
    rates = wide.set_index(["scale_key", "cell_key"])["producer_rate"].astype(int)
    counts = wide.set_index(["scale_key", "cell_key"])["consumer_count"].astype(int)
    agg = agg.set_index(["scale_key", "cell_key"])
    agg["producer_rate"] = rates
    agg["consumer_count"] = counts
    return agg.reset_index()


def plot_mesh_heatmap() -> list[Path]:
    """2D heatmap of median latency over (N, rate), one per transport.
    Color uses log(median_ms) so the cliff cells don't wash out the
    floor cells."""
    import numpy as np

    agg = _mesh_latency_agg()
    if agg.empty:
        return []
    outputs: list[Path] = []
    transports = ("push", "poll@100ms")
    ns = sorted(agg["consumer_count"].unique())
    rates = sorted(agg["producer_rate"].unique())
    for transport in transports:
        sub = agg[agg["cell_key"] == transport]
        if sub.empty:
            continue
        grid = np.full((len(ns), len(rates)), np.nan)
        for _, row in sub.iterrows():
            i = ns.index(int(row["consumer_count"]))
            j = rates.index(int(row["producer_rate"]))
            grid[i, j] = row["mean"]
        fig, ax = plt.subplots(figsize=(8, 5))
        log_grid = np.log10(grid)
        im = ax.imshow(log_grid, aspect="auto", cmap="viridis", origin="lower")
        ax.set_xticks(range(len(rates)))
        ax.set_xticklabels([f"{r:,}" for r in rates])
        ax.set_yticks(range(len(ns)))
        ax.set_yticklabels([f"N={n}" for n in ns])
        ax.set_xlabel("producer rate (events/sec)")
        ax.set_ylabel("CDC consumers")
        for i in range(len(ns)):
            for j in range(len(rates)):
                v = grid[i, j]
                if np.isnan(v):
                    continue
                txt = f"{v:.1f} ms" if v < 1000 else f"{v / 1000:.2f} s"
                ax.text(j, i, txt, ha="center", va="center",
                        color="white" if log_grid[i, j] > 2.0 else "black",
                        fontsize=9)
        cbar = fig.colorbar(im, ax=ax)
        cbar.set_label("log10(median latency, ms)")
        ax.set_title(
            f"2D mesh: median latency over (N consumers x producer rate) — {transport}"
        )
        fig.tight_layout()
        out = PLOTS_DIR / f"scale-mesh-{transport.replace('@', '-')}.png"
        fig.savefig(out, dpi=150)
        plt.close(fig)
        outputs.append(out)
    return outputs


def _mesh_indep_summary() -> pd.DataFrame | None:
    if not MESH_INDEP_TSV.exists():
        return None
    raw = load_tsv(MESH_INDEP_TSV)
    raw = raw[raw["metric"].isin([
        "across_trial_cross_source_skew_mean_ms",
        "across_trial_cross_source_skew_std_ms",
        "across_trial_aggregate_drain_mean",
        "across_trial_total_xact_per_sec_mean",
        "across_trial_max_active_pids_mean",
        "across_trial_total_slot_count_mean",
        "M",
    ])].copy()
    wide = raw.pivot_table(
        index=["scale_key", "cell_key"],
        columns="metric",
        values="value",
        aggfunc="first",
    ).reset_index()
    return wide


def plot_mesh_independent_skew() -> list[Path]:
    """Cross-source skew vs M, per (load, geometry). Lower = more
    fair interleaving across sources."""
    agg = _mesh_indep_summary()
    if agg is None or agg.empty:
        return []
    agg["M"] = agg["M"].astype(int)
    fig, ax = plt.subplots(figsize=(8, 5))
    styles = {
        ("uniform", "G1_one_per_sub_push"): ("#1f77b4", "o", "uniform, G1"),
        ("uniform", "G2_many_per_sub_push"): ("#1f77b4", "s", "uniform, G2"),
        ("skewed", "G1_one_per_sub_push"): ("#d62728", "o", "skewed, G1"),
        ("skewed", "G2_many_per_sub_push"): ("#d62728", "s", "skewed, G2"),
    }
    for (load_, geom_), (color, marker, label) in styles.items():
        sub = agg[
            agg["scale_key"].str.contains(f"_{load_}", regex=False)
            & (agg["cell_key"] == geom_)
        ].sort_values("M")
        if sub.empty:
            continue
        ax.errorbar(
            sub["M"].to_numpy(),
            sub["across_trial_cross_source_skew_mean_ms"].to_numpy(),
            yerr=sub.get("across_trial_cross_source_skew_std_ms",
                         pd.Series([0.0] * len(sub))).to_numpy(),
            color=color, marker=marker, label=label, capsize=4, linewidth=2,
        )
    ax.set_xlabel("Mesh size M (PG instances)")
    ax.set_ylabel("Cross-source skew (max - min source median, ms)")
    ax.set_title("T1: cross-source skew vs M per load shape and geometry")
    ax.set_xscale("log")
    ax.set_xticks([1, 2, 4])
    ax.set_xticklabels(["1", "2", "4"])
    ax.grid(True, alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "mesh-independent-skew.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return [out]


def plot_mesh_independent_drain() -> list[Path]:
    """Aggregate drain vs M, per geometry, uniform load. Linearity
    in M confirms that adding PGs scales drain rate linearly."""
    agg = _mesh_indep_summary()
    if agg is None or agg.empty:
        return []
    agg = agg[agg["scale_key"].str.contains("uniform")].copy()
    agg["M"] = agg["M"].astype(int)
    fig, ax = plt.subplots(figsize=(8, 5))
    for geom_, color, marker, label in [
        ("G1_one_per_sub_push", "#1f77b4", "o", "G1 (one PG per sub)"),
        ("G2_many_per_sub_push", "#2ca02c", "s", "G2 (many PGs per sub)"),
    ]:
        sub = agg[agg["cell_key"] == geom_].sort_values("M")
        if sub.empty:
            continue
        ax.plot(
            sub["M"].to_numpy(),
            sub["across_trial_aggregate_drain_mean"].to_numpy(),
            color=color, marker=marker, label=label, linewidth=2, markersize=8,
        )
    # Theoretical linear: M * (per-PG rate of 1000) * N=5 consumers.
    ms = sorted(agg["M"].unique())
    theory = [m * 1_000 * 5 for m in ms]
    ax.plot(ms, theory, ":", color="gray", linewidth=1.5,
            label="theory: M x 1k ev/s x 5 consumers")
    ax.set_xlabel("Mesh size M (PG instances)")
    ax.set_ylabel("Aggregate drain (events / sec)")
    ax.set_title("T1: aggregate drain vs M (uniform load)")
    ax.set_xscale("log")
    ax.set_xticks(ms)
    ax.set_xticklabels([str(m) for m in ms])
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "mesh-independent-drain.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return [out]


def plot_mesh_logical_rep_lag() -> list[Path]:
    if not MESH_LR_TSV.exists():
        return []
    raw = load_tsv(MESH_LR_TSV)
    raw = raw[raw["metric"].isin([
        "across_trial_replication_lag_mean_ms",
        "across_trial_replication_lag_std_ms",
        "M",
    ])].copy()
    wide = raw.pivot_table(
        index=["scale_key", "cell_key"],
        columns="metric",
        values="value",
        aggfunc="first",
    ).reset_index()
    if wide.empty:
        return []
    wide["M"] = wide["M"].astype(int)
    fig, ax = plt.subplots(figsize=(8, 5))
    for rate_label, color, marker in [("normal", "#1f77b4", "o"),
                                       ("burst", "#d62728", "s")]:
        sub = wide[wide["scale_key"].str.endswith(f"_{rate_label}")].sort_values("M")
        if sub.empty:
            continue
        ax.errorbar(
            sub["M"].to_numpy(),
            sub["across_trial_replication_lag_mean_ms"].to_numpy(),
            yerr=sub.get("across_trial_replication_lag_std_ms",
                         pd.Series([0.0] * len(sub))).to_numpy(),
            color=color, marker=marker,
            label=f"{rate_label} writer rate", capsize=4, linewidth=2,
        )
    ax.set_xlabel("Mesh size M (1 primary + (M-1) replicas)")
    ax.set_ylabel("Replication lag (replica median - primary median, ms)")
    ax.set_title("T2: PG-native replication latency tax vs M")
    ax.set_xticks(sorted(wide["M"].unique()))
    ax.grid(True, alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "mesh-logical-rep-lag.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return [out]


def plot_mesh_sharded_fairness() -> list[Path]:
    if not MESH_SHARDED_TSV.exists():
        return []
    raw = load_tsv(MESH_SHARDED_TSV)
    raw = raw[raw["metric"].isin([
        "across_trial_min_fairness_mean",
        "across_trial_min_fairness_std",
        "across_trial_cross_source_skew_mean_ms",
        "M",
    ])].copy()
    wide = raw.pivot_table(
        index=["scale_key", "cell_key"],
        columns="metric",
        values="value",
        aggfunc="first",
    ).reset_index()
    if wide.empty:
        return []
    wide["M"] = wide["M"].astype(int)
    wide = wide.sort_values("M")
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(
        wide["M"].to_numpy(),
        wide["across_trial_min_fairness_mean"].to_numpy(),
        yerr=wide.get("across_trial_min_fairness_std",
                      pd.Series([0.0] * len(wide))).to_numpy(),
        color="#1f77b4", marker="o", capsize=4, linewidth=2,
        label="min source fairness (mean over trials)",
    )
    ax.axhline(1.0, linestyle=":", color="gray", linewidth=1.5,
               label="theory: drain = write rate")
    ax.set_xlabel("Mesh size M (shards)")
    ax.set_ylabel("Min per-source drain/write fairness")
    ax.set_title("T3: shard fairness under unified G2 fan-in")
    ax.set_ylim(0, max(1.2, wide["across_trial_min_fairness_mean"].max() * 1.1))
    ax.set_xticks(sorted(wide["M"].unique()))
    ax.grid(True, alpha=0.3)
    ax.legend()
    out = PLOTS_DIR / "mesh-sharded-fairness.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return [out]


def main() -> None:
    outputs: list[Path] = []
    outputs.extend(plot_throughput_latency())
    outputs.extend(plot_throughput_ceiling())
    outputs.extend(plot_consumers_latency())
    outputs.extend(plot_consumers_server_load())
    outputs.append(plot_retention_timeseries())
    if ISOLATION_TSV.exists():
        outputs.extend(plot_isolation_writers())
        outputs.extend(plot_isolation_cdc())
    if MESH_TSV.exists():
        outputs.extend(plot_mesh_heatmap())
    outputs.extend(plot_mesh_independent_skew())
    outputs.extend(plot_mesh_independent_drain())
    outputs.extend(plot_mesh_logical_rep_lag())
    outputs.extend(plot_mesh_sharded_fairness())
    # Remove obsolete single-scale PNGs from earlier versions so they
    # don't linger and confuse the report.
    for stale in ["scale-throughput-latency.png", "scale-throughput-ceiling.png",
                   "scale-consumers-latency.png", "scale-consumers-server-load.png"]:
        p = PLOTS_DIR / stale
        if p.exists():
            p.unlink()
    print("Generated plots:")
    for path in outputs:
        rel = path.relative_to(BENCH_DIR.parent.parent)
        print(f"  {rel}")


if __name__ == "__main__":
    main()
