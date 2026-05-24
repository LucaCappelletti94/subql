//! Microbenchmarks for the two fuzz-discovered inputs that libFuzzer flagged
//! as timeouts under the cargo-fuzz instrumented build but parse trivially
//! in a normal release build. Establishes a precise time floor so we can
//! reason about how much headroom the libFuzzer `-timeout=N` knob needs.
//!
//! Both inputs are pinned next to this file so the benchmark stays stable
//! even if the fuzz artifacts directory is cleaned. Source artifacts:
//!
//!   fuzz/artifacts/fuzz_parse_sql/timeout-bf3ef81f...
//!   fuzz/artifacts/fuzz_canonicalize/timeout-9f326c0e...
#![allow(clippy::unwrap_used)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const PARSE_SQL_INPUT: &[u8] = include_bytes!("inputs/cursed_parse_sql_919b.bin");
const CANONICALIZE_INPUT: &[u8] = include_bytes!("inputs/cursed_canonicalize_135b.bin");
const PAREN_COMMA_INPUT: &[u8] = include_bytes!("inputs/cursed_parens_commas_dashes_583b.bin");
const SHIFT_KW_INPUT: &[u8] = include_bytes!("inputs/cursed_shift_keywords_527b.bin");
const KW_OP_SOUP_INPUT: &[u8] = include_bytes!("inputs/cursed_keyword_op_soup_903b.bin");
const DOLLAR_QUOTE_BIT_INPUT: &[u8] = include_bytes!("inputs/cursed_dollar_quote_bit_3988b.bin");
const CASE_SOUP_INPUT: &[u8] = include_bytes!("inputs/cursed_case_soup_142b.bin");

const fn cases() -> [(&'static str, &'static [u8]); 7] {
    [
        ("parse_sql_919B", PARSE_SQL_INPUT),
        ("canonicalize_135B", CANONICALIZE_INPUT),
        ("parens_commas_dashes_583B_fixed_by_2349", PAREN_COMMA_INPUT),
        ("shift_keywords_527B_fixed_combined", SHIFT_KW_INPUT),
        ("keyword_op_soup_903B_fixed_combined", KW_OP_SOUP_INPUT),
        (
            "dollar_quote_bit_3988B_fixed_combined",
            DOLLAR_QUOTE_BIT_INPUT,
        ),
        ("case_soup_142B_fixed_combined", CASE_SOUP_INPUT),
    ]
}

/// Time raw `sqlparser::parser::Parser::parse_sql` on each input. Establishes
/// the floor: any libFuzzer timeout above this is amplification, not
/// algorithmic.
fn bench_raw_parse_sql(c: &mut Criterion) {
    let mut g = c.benchmark_group("sqlparser_parse_sql_raw");
    g.sample_size(60);
    g.measurement_time(Duration::from_secs(4));
    for (name, bytes) in cases() {
        let sql = std::str::from_utf8(bytes).expect("input is valid utf8");
        g.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, sql| {
            let dialect = PostgreSqlDialect {};
            b.iter(|| {
                let _ = Parser::parse_sql(black_box(&dialect), black_box(sql));
            });
        });
    }
    g.finish();
}

/// Time the full subql parse + register + compile path that the libFuzzer
/// `fuzz_parse_sql` target exercises.
fn bench_harness_parse_sql(c: &mut Criterion) {
    let mut g = c.benchmark_group("subql_harness_parse_sql");
    g.sample_size(60);
    g.measurement_time(Duration::from_secs(4));
    for (name, bytes) in cases() {
        g.bench_with_input(BenchmarkId::from_parameter(name), &bytes, |b, bytes| {
            b.iter(|| {
                subql::test_harnesses::harness_parse_sql(black_box(bytes));
            });
        });
    }
    g.finish();
}

/// Time the canonicalize-only path that the libFuzzer `fuzz_canonicalize`
/// target exercises.
fn bench_harness_canonicalize(c: &mut Criterion) {
    let mut g = c.benchmark_group("subql_harness_canonicalize");
    g.sample_size(60);
    g.measurement_time(Duration::from_secs(4));
    for (name, bytes) in cases() {
        g.bench_with_input(BenchmarkId::from_parameter(name), &bytes, |b, bytes| {
            b.iter(|| {
                subql::test_harnesses::harness_canonicalize(black_box(bytes));
            });
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_raw_parse_sql,
    bench_harness_parse_sql,
    bench_harness_canonicalize
);
criterion_main!(benches);
