#![allow(clippy::unwrap_used)]

//! The bool round-trip the four `apply_patchset` e2e suites share.
//!
//! The patchset construction and every assertion are backend independent,
//! so they are spelled once and the suites keep only their own database,
//! DDL, dialect and adapter. The reads go through the typed DSL rather
//! than the `sql_query` strings the copies used, so a change to the
//! `things` shape is a compile error in one place instead of four
//! unchecked column lists.

use diesel::prelude::*;
use sqlite_diff_rs::{
    DiffOps, Insert, PatchDelete, PatchSet, PatchsetFormat, SimpleTable, Update, Value,
};

diesel::table! {
    things (id) {
        id -> Integer,
        active -> Bool,
    }
}

#[derive(Queryable, Selectable, Debug, PartialEq, Eq)]
#[diesel(table_name = things)]
#[diesel(check_for_backend(diesel::pg::Pg, diesel::mysql::Mysql))]
pub struct ThingRow {
    pub id: i32,
    pub active: bool,
}

/// The patchset shape every op in this round-trip carries.
pub type Ops = PatchSet<SimpleTable, String, Vec<u8>>;

/// The three patchsets the round-trip applies, in order.
pub fn bool_roundtrip_ops() -> (Ops, Ops, Ops) {
    let table = SimpleTable::new("things", &["id", "active"], &[0]);
    let inserts = PatchSet::<SimpleTable, String, Vec<u8>>::new()
        .insert(
            Insert::from(table.clone())
                .set(0, 1_i64)
                .unwrap()
                .set(1, 1_i64)
                .unwrap(),
        )
        .insert(
            Insert::from(table.clone())
                .set(0, 2_i64)
                .unwrap()
                .set(1, 0_i64)
                .unwrap(),
        );
    let updates = PatchSet::<SimpleTable, String, Vec<u8>>::new().update(
        Update::<_, PatchsetFormat, String, Vec<u8>>::from(table.clone())
            .set(0, 2_i64)
            .unwrap()
            .set(1, 1_i64)
            .unwrap(),
    );
    let deletes = PatchSet::<SimpleTable, String, Vec<u8>>::new().delete(PatchDelete::<
        SimpleTable,
        String,
        Vec<u8>,
    >::new(
        table,
        vec![Value::Integer(1)],
    ));
    (inserts, updates, deletes)
}

/// What the table holds after each of the three rounds.
pub fn expected_after_inserts() -> Vec<ThingRow> {
    vec![
        ThingRow {
            id: 1,
            active: true,
        },
        ThingRow {
            id: 2,
            active: false,
        },
    ]
}

/// After the update both rows are true.
///
/// The copies read only row 2 here, with `WHERE id = 2`. Reading the whole
/// table instead also pins that row 1 was untouched, which is strictly
/// more than any copy asserted.
pub fn expected_after_update() -> Vec<ThingRow> {
    vec![
        ThingRow {
            id: 1,
            active: true,
        },
        ThingRow {
            id: 2,
            active: true,
        },
    ]
}

/// After the delete, only row 2 remains.
pub fn expected_after_delete() -> Vec<ThingRow> {
    vec![ThingRow {
        id: 2,
        active: true,
    }]
}

/// Every row, ordered, through the typed DSL.
///
/// A tuple select rather than `as_select()`: with a generic `Conn` the
/// associated `SelectExpression` is opaque, and bounding it drags in
/// `SelectableExpression`, `ValidGrouping` and `QueryId` by hand. A tuple
/// already has all three.
pub fn load_all<Conn>(conn: &mut Conn) -> Vec<ThingRow>
where
    Conn: Connection,
    diesel::dsl::Order<things::table, things::id>:
        diesel::query_dsl::methods::LoadQuery<'static, Conn, (i32, bool)>,
{
    things::table
        .order(things::id)
        .load::<(i32, bool)>(conn)
        .expect("load")
        .into_iter()
        .map(|(id, active)| ThingRow { id, active })
        .collect()
}
