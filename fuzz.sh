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
)

# libFuzzer runtime knobs (passed after `--` to cargo-fuzz):
#   -timeout=15        abort a single input after 15s. sqlparser has known
#                      exponential backtracking on adversarial inputs that
#                      can run for ~hundreds of ms in libFuzzer-instrumented
#                      builds; the higher bound surfaces only true hangs.
#   -max_len=65536     cap generated input size at 64 KiB
LIBFUZZER_ARGS=(-timeout=15 -max_len=65536)

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already exists. Attach with: tmux attach -t $SESSION"
    exit 1
fi

run_target() {
    local target="$1"
    printf 'cd %q && cargo +nightly fuzz run %q -- %s; read -r -p '\''Press enter to close...'\''' \
        "$ROOT_DIR" "$target" "${LIBFUZZER_ARGS[*]}"
}

# Create session with the first target
tmux new-session -d -s "$SESSION" -n fuzz "$(run_target "${TARGETS[0]}")"

# Split into more panes for the remaining targets
for target in "${TARGETS[@]:1}"; do
    sleep 1
    tmux split-window -t "$SESSION" "$(run_target "$target")"
    tmux select-layout -t "$SESSION" tiled
done

tmux attach -t "$SESSION"
