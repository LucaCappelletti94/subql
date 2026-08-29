//! Full CDC round-trip integration test (section 1 of the handoff), on
//! the wal2json vehicle.
//!
//! Two phases so that update and delete are real patchset ops (see
//! `common::dispatch`): a seed phase inserts rows, then a mutate phase
//! updates one row and deletes another, matched on a UUID primary key.
//! Each phase drains the wal2json slot and emits its own patchset.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test it round_trip_pg_wal2json_e2e:: \
//!     --features "apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" \
//!     -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::PgConnection;
use subql::emit::wal2json_patchset_builder;
use subql::{parse_wal2json_v2, MessageV2};

const SLOT: &str = "rt_slot";

/// Drain every pending wal2json v2 change and parse it to row events.
fn drain(pg: &mut PgConnection) -> Vec<MessageV2> {
    let mut events = Vec::new();
    for line in &common::drain_slot(pg, SLOT) {
        events.extend(parse_wal2json_v2(line.as_bytes()).unwrap());
    }
    events
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn round_trip_wal2json_dispatches_bool_uuid_domain_enum() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut pg = common::pg_connect(port);

    common::dispatch::create_schema(&mut pg);
    common::create_slot(&mut pg, SLOT);
    let catalog = common::dispatch::subql_catalog();

    // Seed phase: inserts.
    common::dispatch::seed_dml(&mut pg);
    let seed_events = drain(&mut pg);
    assert!(!seed_events.is_empty(), "seed drain yielded no row events");
    let seed_builder = wal2json_patchset_builder(&catalog, &seed_events).unwrap();

    // Mutate phase: update and delete.
    common::dispatch::mutate_dml(&mut pg);
    let mutate_events = drain(&mut pg);
    assert!(!mutate_events.is_empty(), "mutate drain yielded no events");
    let mutate_builder = wal2json_patchset_builder(&catalog, &mutate_events).unwrap();

    common::dispatch::finish_loop(&mut pg, &seed_builder, &mutate_builder);
}
