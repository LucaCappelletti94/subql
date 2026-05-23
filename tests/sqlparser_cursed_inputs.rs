//! Regression test for seven distinct exponential-parse-time inputs that
//! `subql`'s fuzz harnesses surfaced against `sqlparser` on the
//! `PostgreSqlDialect` and that have since been fixed (some on
//! `apache/datafusion-sqlparser-rs` `main` via PRs #2343, #2344, #2349,
//! the rest on the `LucaCappelletti94/sqlparser-rs` `pathological-combined`
//! branch that the workspace `[patch.crates-io]` currently pins).
//!
//! Enforces a 1-second ceiling per input. If the upstream patches are
//! reverted or a new pathological case appears in §1-§7 the test fails
//! loudly.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const DEADLINE: Duration = Duration::from_secs(1);

const CURSED_INPUTS: &[(&str, &[u8])] = &[
    (
        "section_1_dot_colon_hash",
        b"iF i.D.i.:Fi.i.Fi.Fi.D.i.:i.D.i.i.#_FFi.i.Fi.Fi.D.i.:i.D.i.i.#_F",
    ),
    (
        "section_2_dot_star",
        b"iF i.D.i.i. i.FD. i.FD8.D.i. i.D.i.i. i.FD. i.FD8.D.i.i.*~",
    ),
    (
        "section_3_keyword_splice",
        b"if-stf-localtclocal33alt.vocalocanow15alt.vocal1alt.lt.v4.l1altvcalao1lt.lt.v4.l1al33alt.vocalocanow15alt.vocal1alt.lt.v4.l1altvcalao1lt.lt.v4.l1allocaltclocal33alt.vocalocanow15at.vocal1alt.lt.v4.l1altvcallocaltclocal33alt.vocalocanow15alt.vocal1alt.lt.v4.l1altvcalao1lt.lt.v4..l1alt.ll.llocalt.ll.llocalt",
    ),
    // §4: parens + commas + `--<CR>` line-comment chains, minimised from
    // a fuzz_canonicalize timeout. Fixed by upstream PR #2349. Loaded as
    // raw bytes (not a `&str` literal) because the input contains
    // embedded `\r` line terminators that are load-bearing for the bug.
    (
        "section_4_paren_comma_cr_dashes",
        include_bytes!("../benches/inputs/cursed_parens_commas_dashes_583b.bin"),
    ),
    // §5: `<<` shift + keyword-prefix dotted chains, minimised from a
    // fuzz_parse_sql timeout. Fixed on `pathological-combined`.
    (
        "section_5_shift_keywords",
        include_bytes!("../benches/inputs/cursed_shift_keywords_527b.bin"),
    ),
    // §6: keyword-prefix dotted chains with `?` / `^` / `@` / `~` / `%`
    // / `$.` operator soup, minimised from a fuzz_parse_sql timeout.
    // Fixed on `pathological-combined`.
    (
        "section_6_keyword_op_soup",
        include_bytes!("../benches/inputs/cursed_keyword_op_soup_903b.bin"),
    ),
    // §7: PG dollar-quoted strings (with non-ASCII tags) interleaved
    // with `BIT > Ident` chains and bracketed scientific-notation
    // literals, minimised from a fuzz_canonicalize timeout. Fixed on
    // `pathological-combined`.
    (
        "section_7_dollar_quote_bit",
        include_bytes!("../benches/inputs/cursed_dollar_quote_bit_3988b.bin"),
    ),
];

#[test]
fn pathological_inputs_parse_under_deadline() {
    for (name, bytes) in CURSED_INPUTS {
        let sql = std::str::from_utf8(bytes)
            .unwrap_or_else(|e| panic!("{name}: input not valid utf-8: {e}"))
            .to_owned();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let t0 = Instant::now();
            let _ = Parser::parse_sql(&PostgreSqlDialect {}, &sql);
            let _ = tx.send(t0.elapsed());
        });
        match rx.recv_timeout(DEADLINE) {
            Ok(elapsed) => {
                assert!(
                    elapsed < DEADLINE,
                    "{name}: parse took {elapsed:?} (deadline {DEADLINE:?})",
                );
                eprintln!("{name}: {elapsed:?}");
            }
            Err(_) => panic!("{name}: parse exceeded {DEADLINE:?}"),
        }
    }
}


