//! DML coverage for [`PgSqliteEmuSource`].
//!
//! Replaces the pre-Phase-8.1 `tests/sqlite_cdc_dml.rs`. Exercises
//! INSERT / UPDATE / DELETE round trips on two table shapes:
//!
//! * a single-column INT PRIMARY KEY,
//! * a composite PRIMARY KEY spanning two columns.
//!
//! Both shapes prove the changeset drain, the row-lookup fallback for
//! unchanged non-PK columns on UPDATE, the `changed_columns` diff, and
//! the multi-column PK view surfaced through `RowKind::Pk`. The
//! composite-PK case is not covered anywhere else in the tree.
//!
//! Gated behind the `pg-sqlite-emu` feature.

#![cfg(feature = "pg-sqlite-emu")]
#![allow(clippy::unwrap_used)]

use subql::backend::{CdcEvent, RowKind, Value};
use subql::{catalog_helpers, ChangeEvent, EventKind, PgSqliteEmuSource};

const SINGLE_PK_PG_DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);";

const COMPOSITE_PK_PG_DDL: &str = "CREATE TABLE items (\
    region_id INT, item_id INT, name TEXT, \
    PRIMARY KEY (region_id, item_id));";

fn drain_one(source: &mut PgSqliteEmuSource) -> ChangeEvent {
    source
        .poll_next_event()
        .expect("poll succeeds")
        .expect("expected an event on the queue")
}

#[test]
#[allow(clippy::too_many_lines)]
fn single_pk_insert_update_delete_round_trip() {
    let mut source = PgSqliteEmuSource::open_in_memory(SINGLE_PK_PG_DDL).expect("build source");
    let table_id =
        catalog_helpers::table_id(source.pg_catalog(), "orders").expect("orders resolves");

    source
        .execute_sql("INSERT INTO orders (id, price, status) VALUES (1, 5.0, 'paid')")
        .unwrap();
    // Drain between DMLs so the session records each op distinctly.
    // Merged (INSERT + UPDATE + DELETE) on the same row would collapse
    // to a net zero change and yield nothing.
    let insert = drain_one(&mut source);
    assert_eq!(insert.kind(), EventKind::Insert);
    assert_eq!(insert.table_id(source.pg_catalog()), table_id);
    assert_eq!(insert.pk_columns(source.pg_catalog()), &[0u16]);
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::Pk, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::New, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::New, 1)
            .unwrap(),
        Value::Float(5.0)
    );
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::New, 2)
            .unwrap(),
        Value::String("paid".to_string()),
    );

    source
        .execute_sql("UPDATE orders SET price = 9.0, status = 'shipped' WHERE id = 1")
        .unwrap();
    // UPDATE (both non-PK columns changed, so the changeset carries
    // Some/Some on both sides; no row-lookup fallback needed).
    let update = drain_one(&mut source);
    assert_eq!(update.kind(), EventKind::Update);
    assert_eq!(update.table_id(source.pg_catalog()), table_id);
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Pk, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Old, 1)
            .unwrap(),
        Value::Float(5.0)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::New, 1)
            .unwrap(),
        Value::Float(9.0)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Old, 2)
            .unwrap(),
        Value::String("paid".to_string()),
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::New, 2)
            .unwrap(),
        Value::String("shipped".to_string()),
    );
    let mut changed = update.changed_columns(source.pg_catalog());
    changed.sort_unstable();
    assert_eq!(
        changed,
        vec![1u16, 2u16],
        "price and status changed, id did not",
    );
    source
        .execute_sql("DELETE FROM orders WHERE id = 1")
        .unwrap();

    // DELETE (full old-image is the whole point of using changesets).
    let delete = drain_one(&mut source);
    assert_eq!(delete.kind(), EventKind::Delete);
    assert_eq!(delete.table_id(source.pg_catalog()), table_id);
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Pk, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Old, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Old, 1)
            .unwrap(),
        Value::Float(9.0)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Old, 2)
            .unwrap(),
        Value::String("shipped".to_string()),
    );

    assert!(
        source.poll_next_event().unwrap().is_none(),
        "queue is empty after DELETE",
    );
}

#[test]
fn single_pk_partial_update_uses_row_lookup_fallback() {
    // Only `status` changes; `price` stays. The changeset gives us
    // `(None, None)` on the `price` column, so the emulator's row
    // lookup fires and both old and new images must still surface the
    // pre-DML price value.
    let mut source = PgSqliteEmuSource::open_in_memory(SINGLE_PK_PG_DDL).expect("build source");
    source
        .execute_sql("INSERT INTO orders (id, price, status) VALUES (2, 3.5, 'open')")
        .unwrap();
    // Consume the insert to keep event assertions per-DML.
    let _ = drain_one(&mut source);
    source
        .execute_sql("UPDATE orders SET status = 'closed' WHERE id = 2")
        .unwrap();

    let update = drain_one(&mut source);
    assert_eq!(update.kind(), EventKind::Update);
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Old, 1)
            .unwrap(),
        Value::Float(3.5)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::New, 1)
            .unwrap(),
        Value::Float(3.5)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Old, 2)
            .unwrap(),
        Value::String("open".to_string()),
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::New, 2)
            .unwrap(),
        Value::String("closed".to_string()),
    );
    // Only `status` differs; `price` is unchanged so it stays out of
    // `changed_columns`.
    assert_eq!(update.changed_columns(source.pg_catalog()), &[2u16]);
}

#[test]
#[allow(clippy::too_many_lines)]
fn composite_pk_insert_update_delete_round_trip() {
    let mut source = PgSqliteEmuSource::open_in_memory(COMPOSITE_PK_PG_DDL).expect("build source");
    let table_id = catalog_helpers::table_id(source.pg_catalog(), "items").expect("items resolves");

    source
        .execute_sql("INSERT INTO items (region_id, item_id, name) VALUES (1, 100, 'widget')")
        .unwrap();

    // INSERT
    let insert = drain_one(&mut source);
    assert_eq!(insert.kind(), EventKind::Insert);
    assert_eq!(insert.table_id(source.pg_catalog()), table_id);
    // Composite PK surfaces both columns in `pk_columns`.
    assert_eq!(insert.pk_columns(source.pg_catalog()), &[0u16, 1u16]);
    // Both PK columns readable through `RowKind::Pk`.
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::Pk, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::Pk, 1)
            .unwrap(),
        Value::Int(100)
    );
    // Non-PK columns still Missing under `RowKind::Pk`.
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::Pk, 2)
            .unwrap(),
        Value::Missing
    );
    assert_eq!(
        insert
            .value_at(source.pg_catalog(), RowKind::New, 2)
            .unwrap(),
        Value::String("widget".to_string()),
    );

    source
        .execute_sql("UPDATE items SET name = 'gadget' WHERE region_id = 1 AND item_id = 100")
        .unwrap();
    // UPDATE: the emulator's row lookup fills the two unchanged PK
    // slots so both old and new images stay complete.
    let update = drain_one(&mut source);
    assert_eq!(update.kind(), EventKind::Update);
    assert_eq!(update.pk_columns(source.pg_catalog()), &[0u16, 1u16]);
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Pk, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Pk, 1)
            .unwrap(),
        Value::Int(100)
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::Old, 2)
            .unwrap(),
        Value::String("widget".to_string()),
    );
    assert_eq!(
        update
            .value_at(source.pg_catalog(), RowKind::New, 2)
            .unwrap(),
        Value::String("gadget".to_string()),
    );
    assert_eq!(update.changed_columns(source.pg_catalog()), &[2u16]);

    source
        .execute_sql("DELETE FROM items WHERE region_id = 1 AND item_id = 100")
        .unwrap();

    // DELETE
    let delete = drain_one(&mut source);
    assert_eq!(delete.kind(), EventKind::Delete);
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Pk, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Pk, 1)
            .unwrap(),
        Value::Int(100)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Old, 0)
            .unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Old, 1)
            .unwrap(),
        Value::Int(100)
    );
    assert_eq!(
        delete
            .value_at(source.pg_catalog(), RowKind::Old, 2)
            .unwrap(),
        Value::String("gadget".to_string()),
    );

    assert!(
        source.poll_next_event().unwrap().is_none(),
        "queue is empty after DELETE",
    );
}
