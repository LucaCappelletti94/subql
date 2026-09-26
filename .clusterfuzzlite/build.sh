#!/bin/bash
set -eu

cd "$SRC/subql"
cargo fuzz build -O --debug-assertions --fuzz-dir fuzz

targets=$(cargo fuzz list --fuzz-dir fuzz)
if [ -z "$targets" ]; then
    echo "cargo fuzz list named no target" >&2
    exit 1
fi

target_dir=fuzz/target/x86_64-unknown-linux-gnu/release
for name in $targets; do
    cp "$target_dir/$name" "$OUT/"
    # a fresh engine per input fragments the ASAN heap past the default ceiling
    printf '[libfuzzer]\ntimeout = 15\nmax_len = 65536\nrss_limit_mb = 6144\n' >"$OUT/$name.options"
done

# the targets that read SQL text get the SQL dictionary
for name in fuzz_parse_sql fuzz_canonicalize; do
    cp fuzz/sql.dict "$OUT/$name.dict"
done

# the runner unpacks <target>_seed_corpus.zip as the starting corpus
for dir in fuzz/seeds/*/; do
    name=$(basename "$dir")
    zip -qj "$OUT/${name}_seed_corpus.zip" "$dir"*
done
