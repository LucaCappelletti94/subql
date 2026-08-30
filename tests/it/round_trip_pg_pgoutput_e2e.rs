//! Full CDC round-trip integration test (section 1 of the handoff), on
//! the pgoutput vehicle.
//!
//! Two phases so that update and delete are real patchset ops (see
//! `common::dispatch`): a seed phase inserts rows, then a mutate phase
//! updates one row and deletes another, matched on a UUID primary key.
//! Each phase drains the pgoutput slot as binary logical-replication
//! messages and decodes them to `ChangeEvent` through `PgOutputDecoder`,
//! exactly as a replication client would. One decoder spans both drains
//! so its relation cache persists.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test it round_trip_pg_pgoutput_e2e:: \
//!     --features "pgoutput-emit apply-patchset-postgres apply-patchset-sqlite sqlite-cdc" \
//!     -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, PgConnection, QueryableByName, RunQueryDsl};
use pg_walstream::{ChangeEvent, Lsn, PgOutputDecoder};
use subql::emit::pgoutput_patchset_builder;

const SLOT: &str = "rt_pgo_slot";
const PUBLICATION: &str = "rt_pub";

#[derive(QueryableByName)]
struct BinaryChange {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    data: Vec<u8>,
}

/// Drain every pending pgoutput binary message and decode it to a
/// `ChangeEvent`. The decoder is shared across phases so its relation
/// cache survives between drains.
fn drain(pg: &mut PgConnection, decoder: &mut PgOutputDecoder) -> Vec<ChangeEvent> {
    let changes: Vec<BinaryChange> = sql_query(format!(
        "SELECT data FROM pg_logical_slot_get_binary_changes('{SLOT}', NULL, NULL, 'proto_version', '1', 'publication_names', '{PUBLICATION}')"
    ))
    .load(pg)
    .unwrap();
    let mut events = Vec::new();
    for change in changes {
        if let Some(event) = decoder.decode_message(change.data, Lsn::new(0)).unwrap() {
            events.push(event);
        }
    }
    events
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn round_trip_pgoutput_dispatches_bool_uuid_domain_enum() {
    common::assert_docker_available();
    let container = common::pg_with_wal2json();
    let port = common::pg_port(&container);
    let mut pg = common::pg_connect(port);

    common::dispatch::create_schema(&mut pg);
    common::create_publication(&mut pg, PUBLICATION, "orders");
    common::create_pgoutput_slot(&mut pg, SLOT);
    let catalog = common::dispatch::subql_catalog();
    let mut decoder = PgOutputDecoder::with_protocol_version(1);

    // Seed phase: inserts.
    common::dispatch::seed_dml(&mut pg);
    let seed_events = drain(&mut pg, &mut decoder);
    assert!(!seed_events.is_empty(), "seed drain yielded no events");
    let seed_builder = pgoutput_patchset_builder(&catalog, &seed_events).unwrap();

    // Mutate phase: update and delete.
    common::dispatch::mutate_dml(&mut pg);
    let mutate_events = drain(&mut pg, &mut decoder);
    assert!(!mutate_events.is_empty(), "mutate drain yielded no events");
    let mutate_builder = pgoutput_patchset_builder(&catalog, &mutate_events).unwrap();

    common::dispatch::finish_loop(&mut pg, &seed_builder, &mutate_builder);
}
