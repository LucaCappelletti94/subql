//! Single merged integration-test target. Each module below was a
//! standalone `tests/*.rs` binary until 2026-08-28; merging them into one
//! target removes 82 fat link steps from every build. Feature gates that
//! lived in `Cargo.toml` `[[test]]` `required-features` (and file-level
//! `#![cfg]` attributes) now live on the `mod` declarations here.
//!
//! Run one old target's tests via a nextest filter, e.g.:
//!   cargo nextest run --cargo-profile testfast --all-features -E 'test(reexec_postgres::)'

mod common;

mod agg_sum_nullability;

mod aggregate_bootstrap;

mod aggregate_totals;

mod aggregate_transition;

// Docker-backed E2E for apply_diffset_bytes_async (patchset + changeset
// arms) and apply_changeset_async against Postgres over a diesel-async
// connection. Run with:
// cargo nextest run -E 'test(apply_diffset_async_pg_e2e::)' \
// --features apply-patchset-postgres-async --run-ignored ignored-only --no-capture
#[cfg(feature = "apply-patchset-postgres-async")]
mod apply_diffset_async_pg_e2e;

// Docker-backed E2E for apply_patchset_async against MySQL over a
// diesel-async connection. Run with:
// cargo nextest run -E 'test(apply_patchset_mysql_async_e2e::)' \
// --features apply-patchset-mysql-async --run-ignored ignored-only --no-capture
#[cfg(feature = "apply-patchset-mysql-async")]
mod apply_patchset_mysql_async_e2e;

#[cfg(feature = "apply-patchset-mysql")]
mod apply_patchset_mysql_e2e;

// Docker-backed E2E for apply_patchset_async against Postgres over a
// diesel-async connection. Run with:
// cargo nextest run -E 'test(apply_patchset_pg_async_e2e::)' \
// --features apply-patchset-postgres-async --run-ignored ignored-only --no-capture
#[cfg(feature = "apply-patchset-postgres-async")]
mod apply_patchset_pg_async_e2e;

#[cfg(feature = "apply-patchset-postgres")]
mod apply_patchset_pg_e2e;

#[cfg(feature = "apply-patchset-postgres")]
mod apply_patchset_pg_uuid_e2e;

#[cfg(feature = "apply-patchset-sqlite")]
mod apply_patchset_sqlite_e2e;

#[cfg(feature = "membership-term")]
mod caller_term;

#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-mysql"
))]
mod canonical_group_key_equivalence;

mod cdc_cross_db;

// Differential equivalence test: drives a deterministic DML stream
// against two slots concurrently, drains both push + poll, asserts
// they observed identical events. Catches pgoutput parser bugs and
// pg_walstream native-backend stream framing regressions. Run with:
// cargo nextest run -E 'test(cdc_equivalence::)' --features pg-streaming --run-ignored ignored-only --no-capture
#[cfg(feature = "pg-streaming")]
mod cdc_equivalence;

mod cdc_mysql_e2e;

#[cfg(feature = "membership-term")]
mod composite_membership_term;

mod connector_default;

mod custom_scalar_types;

// Integration test for the DieselConnector. Uses in-memory SQLite. Gated
// behind the `executor-diesel` feature so default `cargo test` does not try
// to compile it.
// Docker-free proof that a result of unknown shape decodes and that the byte
// budget bounds a page. Needs the SQLite backend for a real connection plus the
// connector seam under test.
#[cfg(all(feature = "diesel-typed-sqlite", feature = "executor-diesel"))]
mod dynamic_row_decode;

mod equality_filter_kinds;

mod eviction_e2e;

// Docker-free connector round-trip for the multi-column aggregate seed
// (`execute_scalar_row`) over in-memory SQLite. Gated behind
// `executor-diesel` like `reexec_diesel`.
#[cfg(all(feature = "executor-diesel", feature = "diesel-typed-sqlite"))]
mod execute_scalar_row_sqlite;

#[cfg(feature = "diesel-typed-mysql")]
mod follow_insert_mysql;

#[cfg(feature = "diesel-typed")]
mod follow_insert_postgres;

#[cfg(feature = "diesel-typed-sqlite")]
mod follow_insert_sqlite;

// `follow_row` + emulator integration: PK-scoped follows observe
// INSERT / DELETE events routed through the pgoutput wire.
#[cfg(feature = "pg-sqlite-emu")]
mod follow_pg_sqlite_emu;

mod follow_row_dialects;

mod grouped_aggregate_totals;

mod grouped_extreme;

mod grouped_having;

mod install_trait;

#[cfg(all(feature = "diesel-typed-sqlite", feature = "executor-diesel"))]
mod keyed_capture;

#[cfg(feature = "membership-term")]
mod membership_term;

#[cfg(feature = "membership-term")]
mod membership_term_seed_e2e;

mod one_identity;

#[cfg(feature = "std")]
mod persistence_column_kinds_regression;

// INSERT / UPDATE / DELETE coverage for the emulator on single-PK and
// composite-PK tables. Composite PK is not exercised anywhere else.
#[cfg(feature = "pg-sqlite-emu")]
mod pg_sqlite_emu_dml;

// Smoke integration test for the fake-Postgres-over-SQLite CDC source.
// Gated behind `pg-sqlite-emu` so default `cargo test` skips it.
#[cfg(feature = "pg-sqlite-emu")]
mod pg_sqlite_emu_smoke;

// Docker-backed integration test for PgStreamingCdcSource (push-based PG
// CDC source using START_REPLICATION via pg_walstream). Run with:
// cargo nextest run -E 'test(pg_streaming_e2e::)' \
// --features pg-streaming --run-ignored ignored-only --no-capture
#[cfg(feature = "pg-streaming")]
mod pg_streaming_e2e;

mod placeholder_bytes_bind;

// Smoke test for the polling-based PG CDC source. Run with:
// cargo nextest run -E 'test(polling_smoke::)' --features pg-streaming --run-ignored ignored-only --no-capture
#[cfg(feature = "pg-streaming")]
mod polling_smoke;

// One-shot empirical benchmark comparing the two shipped library
// transports: the push-based `PgStreamingCdcSource` against the polling
// `PollingPgCdcSource`. Verifies the architectural claim that push
// delivers materially lower latency than polling at any poll cadence.
// Run once, paste numbers into `BENCHMARKS.md`.
#[cfg(feature = "pg-streaming")]
mod polling_vs_push_benchmark;

mod proptest_aggregate_bootstrap;

mod proptest_dispatch;

mod proptest_eviction;

mod proptest_grouped_extreme;

// Structural CDC proptest: per-event shape assertions against a
// reference model. Distinct from proptest_pg_sqlite_emu_dispatch,
// which only checks oracle notifications.
#[cfg(feature = "pg-sqlite-emu")]
mod proptest_pg_sqlite_emu_cdc;

// End-to-end dispatch proptest for the emulator: arbitrary DML,
// reference oracle, engine notifications must match per event.
#[cfg(feature = "pg-sqlite-emu")]
mod proptest_pg_sqlite_emu_dispatch;

mod proptest_reexec_plan;

mod proptest_register_batch_parity;

mod proptest_resume_cursor;

mod proptest_rls_aggregate_guard;

mod proptest_row_set_delta;

mod proptest_throttle;

#[cfg(feature = "std")]
mod reads_persist;

#[cfg(all(feature = "executor-diesel", feature = "diesel-typed-sqlite"))]
mod reexec_diesel;

// Docker-backed integration test for MysqlDieselConnector against a real MySQL
// with binary logging. Tests are #[ignore]d; run via:
// cargo nextest run -E 'test(reexec_mysql::)' --features executor-diesel-mysql --run-ignored ignored-only --no-capture
#[cfg(feature = "executor-diesel-mysql")]
mod reexec_mysql;

// Docker-backed integration test for AsyncAutoResolvingEngine +
// MysqlAsyncDieselConnector against a real MySQL 8.0 with binary logging.
// Run with:
// cargo nextest run -E 'test(reexec_mysql_async::)' \
// --features executor-diesel-async-mysql --run-ignored ignored-only --no-capture
#[cfg(feature = "executor-diesel-async-mysql")]
mod reexec_mysql_async;

// Docker-backed integration test for AutoResolvingEngine + DieselConnector
// against a real Postgres with logical replication. Tests are #[ignore]d;
// run explicitly via:
// cargo nextest run -E 'test(reexec_postgres::)' --features executor-diesel-postgres --run-ignored ignored-only --no-capture
#[cfg(feature = "executor-diesel-postgres")]
mod reexec_postgres;

// Docker-backed integration test for AsyncAutoResolvingEngine +
// PgAsyncDieselConnector against a real Postgres with logical replication.
// Run with:
// cargo nextest run -E 'test(reexec_postgres_async::)' \
// --features executor-diesel-async-postgres --run-ignored ignored-only --no-capture
#[cfg(feature = "executor-diesel-async-postgres")]
mod reexec_postgres_async;

// Docker-backed integration test for PgR2D2DieselConnector (pool-backed
// PG connector). Run with:
// cargo nextest run -E 'test(reexec_postgres_r2d2::)' \
// --features executor-diesel-postgres-r2d2 --run-ignored ignored-only --no-capture
#[cfg(feature = "executor-diesel-postgres-r2d2")]
mod reexec_postgres_r2d2;

mod reexec_proptest;

mod reexec_rls_guard;

mod reexec_throttle;

mod registration_tiers;

mod registry_plans_reads;

// Docker-backed check that `REPLICA_IDENTITY_AUDIT_SQL` names exactly the
// tables whose change stream omits the previous row. Needs only a plain
// Postgres connection, no replication slot. Run with:
// cargo nextest run -E 'test(replica_identity_audit::)' \
// --features executor-diesel-postgres --run-ignored ignored-only --no-capture
#[cfg(feature = "executor-diesel-postgres")]
mod replica_identity_audit;

mod rls_reread;

#[cfg(all(
    feature = "apply-patchset-mysql",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
mod round_trip_mysql_maxwell_e2e;

#[cfg(all(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
mod round_trip_pg_changeset_emit_e2e;

#[cfg(all(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
mod round_trip_pg_composite_default_ri_e2e;

// Docker-backed full CDC round-trip test on the pgoutput vehicle: the
// same four-type loop as the wal2json test, but driven by real pgoutput
// binary changes decoded to `ChangeEvent` and emitted via
// `pgoutput_patchset_builder`. Run with:
// cargo nextest run -E 'test(round_trip_pg_pgoutput_e2e::)' \
// --features "pgoutput-emit apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" --run-ignored ignored-only --no-capture
#[cfg(all(
    feature = "pgoutput-emit",
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
mod round_trip_pg_pgoutput_e2e;

#[cfg(all(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
mod round_trip_pg_pk_change_changeset_e2e;

// Docker-backed full CDC round-trip test on the wal2json vehicle:
// Postgres DML to wal2json CDC to emitted patchset, applied to a SQLite
// replica via `SqliteAdapter`, session patchset captured and re-applied
// to Postgres via `PgAdapter`, asserting row parity across the cycle.
// Run with:
// cargo nextest run -E 'test(round_trip_pg_wal2json_e2e::)' \
// --features "apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" --run-ignored ignored-only --no-capture
#[cfg(all(
    feature = "apply-patchset-postgres",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
mod round_trip_pg_wal2json_e2e;

mod served_statement_shape;

mod semantics_nan;

mod semantics_bool_order;

mod semantics_jsonb_order;

mod semantics_like_escape;

mod semantics_bpchar;

mod semantics_collation;

mod semantics_cross_kind;

mod semantics_arith_overflow;

mod semantics_div_zero;

mod semantics_division;

mod semantics_missing_cell;

mod semantics_float_width;

// Docker-free coverage for the connector session-setup seam (U8) over in-memory
// SQLite, plus Docker-gated cursor and async-MySQL coverage in the connector
// test files. The SQLite target needs the same features as the other sqlite
// connector tests.
#[cfg(all(feature = "executor-diesel", feature = "diesel-typed-sqlite"))]
mod session_setup;

mod sqlparser_cursed_inputs;

mod temporal_literal_forms;

mod unseeded_extreme;

mod update_non_predicate_column;

mod uuid_id_types;

// The delegated half against a real OpenFGA server, which is the one criterion
// every other visibility test proves only against a counting stand-in. Run with:
// cargo nextest run -E 'test(visibility_openfga_e2e::)' \
// --features "visibility-openfga testing" --run-ignored ignored-only --no-capture
#[cfg(all(feature = "visibility-openfga", feature = "testing"))]
mod visibility_openfga_e2e;

// Docker-backed differential test: every record subql derives from a real
// CDC row image must equal what the tuple SQL rls2fga emits for the same
// rows returns from the same Postgres. This is what stops subql's own
// value rendering drifting from the loader's. Run with:
// cargo nextest run -E 'test(visibility_records_parity::)' \
// --features "visibility-records executor-diesel-postgres" --run-ignored ignored-only --no-capture
#[cfg(all(feature = "visibility-records", feature = "executor-diesel-postgres"))]
mod visibility_records_parity;
