//! `PgOutputBridge` round-trip proptest: `parse(encode(event)) == event`.
//!
//! Generates arbitrary [`WalEvent`] shapes (Insert / Update / Delete /
//! Truncate) over a fixed `orders (id INT PK, amount INT, status TEXT)`
//! catalog, encodes each through [`PgOutputBridge::encode_event`], feeds
//! the resulting `pgoutput` frames through [`PgOutputParser`], and
//! asserts the decoded event is structurally identical to the input
//! modulo bookkeeping the parser legitimately recomputes
//! (`changed_columns`, which the parser derives by diffing the old and
//! new images, and the synthesised `relation_id` OID).
//!
//! What this catches: encoding bugs in the bridge that would silently
//! produce wire bytes the parser misreads. The pgoutput round-trip doctest in
//! `src/sqlite_cdc/pgoutput_bridge.rs` covers the happy path on a real
//! SQLite source. This proptest hardens the encoder over the full
//! `WalEvent` shape space without needing a connection.
//!
//! Gated via `[[test]] required-features = ["sqlite-cdc"]`.
#![cfg(feature = "sqlite-cdc")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use proptest::prelude::*;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::wal::WalParser;
use subql::{
    catalog_helpers, Cell, PgOutputBridge, PgOutputParser, PrimaryKey, RowImage, TableId, WalEvent,
};

const PG_DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

/// Fixed status alphabet so the SQL-side comparison is trivial and
/// shrinking is predictable. The bridge encodes them as raw bytes, the
/// parser decodes them back into `Cell::String` via `text_to_cell_strict`
/// for OID 25 (text).
const STATUSES: &[&str] = &["paid", "open", "closed", "pending", ""];

fn build_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(PG_DDL).unwrap()
}

fn orders_table_id(catalog: &ParserDB) -> TableId {
    catalog_helpers::table_id(catalog, "orders").unwrap()
}

fn cell_id() -> impl Strategy<Value = Cell> {
    (1i64..=64).prop_map(Cell::Int)
}

fn cell_amount() -> impl Strategy<Value = Cell> {
    prop_oneof![
        1 => Just(Cell::Null),
        9 => (-200i64..=200).prop_map(Cell::Int),
    ]
}

fn cell_status() -> impl Strategy<Value = Cell> {
    prop_oneof![
        1 => Just(Cell::Null),
        9 => (0usize..STATUSES.len()).prop_map(|i| Cell::String(Arc::from(STATUSES[i]))),
    ]
}

/// Three-cell row matching the `orders (id, amount, status)` layout.
/// Every cell is concrete (no `Cell::Missing`). The bridge maps
/// `Cell::Missing` to pgoutput's `b'u'` (unchanged TOAST) tag, but
/// emitting it on the new image is invalid in the protocol, and the
/// parser rejects it.
fn row_strategy() -> impl Strategy<Value = RowImage> {
    (cell_id(), cell_amount(), cell_status()).prop_map(|(id, amount, status)| RowImage {
        cells: Arc::from(vec![id, amount, status]),
    })
}

fn make_pk(id: &Cell) -> PrimaryKey {
    PrimaryKey::new(Arc::from([0u16]), Arc::from([id.clone()])).unwrap()
}

#[derive(Clone, Debug)]
enum Shape {
    Insert(RowImage),
    Update { old: RowImage, new: RowImage },
    UpdateNoOld(RowImage),
    Delete(RowImage),
    Truncate,
}

fn shape_strategy() -> impl Strategy<Value = Shape> {
    prop_oneof![
        3 => row_strategy().prop_map(Shape::Insert),
        3 => (row_strategy(), row_strategy()).prop_map(|(old, new)| Shape::Update { old, new }),
        2 => row_strategy().prop_map(Shape::UpdateNoOld),
        3 => row_strategy().prop_map(Shape::Delete),
        1 => Just(Shape::Truncate),
    ]
}

fn build_event(shape: &Shape, table_id: TableId) -> WalEvent {
    match shape {
        Shape::Insert(row) => {
            let pk = make_pk(&row.cells[0]);
            WalEvent::builder(table_id)
                .insert()
                .pk(pk)
                .new_row(row.clone())
                .build()
                .unwrap()
        }
        Shape::Update { old, new } => {
            // pgoutput Update semantics: when the old tuple ships in
            // full, the PK identifier is the OLD row's PK column (the
            // row identity being updated). The parser derives the PK
            // from the old image whenever it is present, so the input
            // event we compare against must do the same.
            let pk = make_pk(&old.cells[0]);
            WalEvent::builder(table_id)
                .update()
                .pk(pk)
                .old_row(old.clone())
                .new_row(new.clone())
                .build()
                .unwrap()
        }
        Shape::UpdateNoOld(new) => {
            let pk = make_pk(&new.cells[0]);
            WalEvent::builder(table_id)
                .update()
                .pk(pk)
                .new_row(new.clone())
                .build()
                .unwrap()
        }
        Shape::Delete(row) => {
            let pk = make_pk(&row.cells[0]);
            WalEvent::builder(table_id)
                .delete()
                .pk(pk)
                .old_row(row.clone())
                .build()
                .unwrap()
        }
        Shape::Truncate => WalEvent::builder(table_id).truncate().build().unwrap(),
    }
}

fn cells_match(a: &[Cell], b: &[Cell]) -> bool {
    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

fn pk_match(a: &PrimaryKey, b: &PrimaryKey) -> bool {
    a.columns() == b.columns() && cells_match(a.values(), b.values())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// For every generated [`WalEvent`] shape, the parser's decoded
    /// event must agree with the input on `kind`, `table_id`,
    /// primary-key columns and values, and the cells of every row
    /// image the protocol carries (full new image for Insert / Update,
    /// full old image for Update / Delete, none for Truncate).
    #[test]
    fn parse_of_encode_matches_input(shape in shape_strategy()) {
        let catalog = build_catalog();
        let table_id = orders_table_id(&catalog);
        let input = build_event(&shape, table_id);

        let mut bridge = PgOutputBridge::new();
        let parser = PgOutputParser::new();

        let frames = bridge
            .encode_event(&input, &catalog)
            .expect("encode_event must succeed for catalog-resident table");
        let mut decoded: Vec<WalEvent> = Vec::new();
        for frame in frames {
            let events = parser
                .parse_wal_message(&frame, &catalog)
                .expect("parser must accept bridge-encoded frames");
            decoded.extend(events);
        }

        prop_assert_eq!(decoded.len(), 1, "exactly one data frame should decode to an event");
        let output = decoded.into_iter().next().unwrap();

        prop_assert_eq!(output.kind(), input.kind());
        prop_assert_eq!(output.table_id(), input.table_id());

        // PK comparison: Truncate carries an empty PK on both sides.
        prop_assert!(
            pk_match(output.pk(), input.pk()),
            "pk mismatch: output={:?} input={:?}",
            output.pk(), input.pk()
        );

        // New image (Insert / Update).
        match (input.new_row(), output.new_row()) {
            (Some(want), Some(got)) => prop_assert!(
                cells_match(&want.cells, &got.cells),
                "new_row mismatch: want={:?} got={:?}", want.cells, got.cells,
            ),
            (None, None) => {}
            (a, b) => prop_assert!(false, "new_row presence mismatch: {:?} vs {:?}", a, b),
        }

        // Old image (Update / Delete). The bridge ships the full old
        // tuple when one was supplied, so a complete old row round trips
        // unchanged. `Shape::UpdateNoOld` carries `None` on both sides.
        match (input.old_row(), output.old_row()) {
            (Some(want), Some(got)) => prop_assert!(
                cells_match(&want.cells, &got.cells),
                "old_row mismatch: want={:?} got={:?}", want.cells, got.cells,
            ),
            (None, None) => {}
            (a, b) => prop_assert!(false, "old_row presence mismatch: {:?} vs {:?}", a, b),
        }
    }
}
