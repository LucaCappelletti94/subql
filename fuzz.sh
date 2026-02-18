#!/usr/bin/env bash
set -euo pipefail

SESSION="subql-fuzz"
FUZZ_DIR="$(cd "$(dirname "$0")/fuzz" && pwd)"
TARGETS=(
    fuzz_parse_sql
    fuzz_vm_eval
    fuzz_deserialize_shard
    fuzz_canonicalize
    fuzz_codec_decode
)

HFUZZ_RUN_ARGS="-n 1 -t 5 --max_file_size 65536"

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session '$SESSION' already exists. Attach with: tmux attach -t $SESSION"
    exit 1
fi

# Create session with the first target
tmux new-session -d -s "$SESSION" -n fuzz \
    "cd '$FUZZ_DIR' && HFUZZ_RUN_ARGS='$HFUZZ_RUN_ARGS' cargo hfuzz run ${TARGETS[0]}; read -r -p 'Press enter to close...'"

# Split into 4 more panes for the remaining targets
for target in "${TARGETS[@]:1}"; do
    sleep 1
    tmux split-window -t "$SESSION" \
        "cd '$FUZZ_DIR' && HFUZZ_RUN_ARGS='$HFUZZ_RUN_ARGS' cargo hfuzz run $target; read -r -p 'Press enter to close...'"
    tmux select-layout -t "$SESSION" tiled
done

tmux attach -t "$SESSION"
