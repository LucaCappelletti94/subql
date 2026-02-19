# subql

SQL subscription trees for Change Data Capture Fanout

## CI

The GitHub Actions workflow in `.github/workflows/ci.yml` runs a full Rust quality gate:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo check --locked` matrix (`default`, `--no-default-features`, `--all-features`)
- `cargo test --locked` matrix (`default`, `--all-features`)
- ignored cross-DB integration test (`cdc_cross_db`)
- docs + doctests with rustdoc warnings denied
- MSRV check (`1.75`)
- bench compile check (`cargo bench --no-run`)
- fuzz target compile check (`fuzz/Cargo.toml`)
- `cargo audit`

All cargo CI commands use `--locked` to enforce deterministic dependency resolution.

## Pre-commit Hook

This repo includes a Rust pre-commit hook at `.githooks/pre-commit` that runs:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `cargo test --workspace --lib --tests --locked`

Install it once per clone:

```bash
./scripts/install-git-hooks.sh
```

Optional: skip tests on a single commit:

```bash
SUBQL_PRECOMMIT_SKIP_TESTS=1 git commit -m "..."
```
