#!/usr/bin/env bash
set -euo pipefail

SESSION="subql-fuzz"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGETS=(
    fuzz_parse_sql
    fuzz_vm_eval
    fuzz_deserialize_shard
    fuzz_canonicalize
    fuzz_codec_decode
    fuzz_pgoutput
    fuzz_wal_json_postparse
    fuzz_aggregate_consistency
)

# libFuzzer runtime knobs (passed after `--` to cargo-fuzz):
#   -timeout=15        abort a single input after 15s. Historically raised
#                      from libFuzzer's default to absorb sqlparser's
#                      exponential parse time on adversarial compound
#                      chains. That class of pathological inputs is fixed
#                      upstream in apache/datafusion-sqlparser-rs#2344
#                      (pinned via the workspace [patch.crates-io] until
#                      a release > 0.62.0 ships), but we keep the 15s
#                      headroom as defense in depth against new pathological
#                      inputs the fuzzer may surface.
#   -max_len=65536     cap generated input size at 64 KiB.
#   -rss_limit_mb=8192 raise libFuzzer's RSS ceiling from the 2 GiB default.
#                      `fuzz_aggregate_consistency` constructs and drops a
#                      fresh `SubscriptionEngine` per iteration; under ASAN
#                      the allocator fragments and total RSS drifts past
#                      2 GiB after ~50k iterations even though no single
#                      input is large. 8 GiB gives ample headroom on this
#                      machine and lets the fuzzer keep going.
LIBFUZZER_ARGS=(-timeout=15 -max_len=65536 -rss_limit_mb=8192)

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already exists. Attach with: tmux attach -t $SESSION"
    exit 1
fi

run_target() {
    local target="$1"
    # cargo-fuzz inherits the workspace `release` profile but the fuzz
    # package has `lto = false` via `[profile.release.package.subql-fuzz]`
    # in the root Cargo.toml - SanCov + ASAN need LTO off to link.
    printf 'cd %q && cargo +nightly fuzz run %q -- %s; read -r -p '\''Press enter to close...'\''' \
        "$ROOT_DIR" "$target" "${LIBFUZZER_ARGS[*]}"
}

# Create session with the first target.
tmux new-session -d -s "$SESSION" -n fuzz "$(run_target "${TARGETS[0]}")"
# Show pane titles in the border (off by default in most tmux configs).
tmux set-option -t "$SESSION" pane-border-status top
# Label the first pane after its target.
tmux select-pane -t "${SESSION}:0.0" -T "${TARGETS[0]}"

# Split into more panes for the remaining targets, labelling each one.
pane_index=0
for target in "${TARGETS[@]:1}"; do
    sleep 1
    tmux split-window -t "$SESSION" "$(run_target "$target")"
    pane_index=$((pane_index + 1))
    tmux select-pane -t "${SESSION}:0.${pane_index}" -T "$target"
    tmux select-layout -t "$SESSION" tiled
done

tmux attach -t "$SESSION"
