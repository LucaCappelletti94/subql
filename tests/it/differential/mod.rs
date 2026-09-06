//! The differential harness: ask the engine, ask subql, compare.
//!
//! Every divergence Parts II and III corrected was found by hand, one
//! measurement at a time. This is the machine that would have found them,
//! and the part of it that matters most is the part that is easy to get
//! wrong: what counts as an answer.
//!
//! An engine has three answers for a predicate over a row, `TRUE`, `FALSE`
//! and `NULL`, and a fourth outcome that is not an answer at all: it
//! raises. Measured across Parts II and III, PostgreSQL raises on a zero
//! divisor, on an integer overflow, on a `numeric` past its digit limit
//! and on a `LIKE` pattern ending in an escape, while MySQL and SQLite
//! answer `NULL` for the first of those. A harness that folded a raise
//! into `NULL` would have called every one of those cases agreement, so
//! [`OracleVerdict`] keeps them apart and the comparison refuses to treat
//! a refusal as unknown.

pub mod generators;
pub mod layers;
pub mod oracle;
pub mod sweep;
pub mod triage;
