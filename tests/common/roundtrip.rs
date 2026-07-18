//! Source-agnostic round-trip helper shared by the per-vehicle CDC
//! integration tests.
//!
//! Every vehicle needs [`rebuild`]: it parses the raw patchset the SQLite
//! session extension emits and reconstructs an applyable
//! `PatchSet<SimpleTable>` carrying genuine inserts, updates, and deletes,
//! so the captured client-side changes can be re-applied to the source
//! database. Only compiled for test crates that enable the SQLite apply
//! and session stack.

#![allow(dead_code)]

use sqlite_diff_rs::{
    DiffOps, Insert, ParsedDiffSet, PatchDelete, PatchSet, PatchsetFormat, PatchsetOp, SimpleTable,
    Update,
};

/// Rebuild an applyable `PatchSet<SimpleTable>` from raw session patchset
/// bytes, reconstructing inserts, updates, and deletes. Updates take the
/// primary key from the op's `pk` slice and the new non-key values from
/// its per-column entries.
pub fn rebuild(bytes: &[u8], table: &SimpleTable) -> PatchSet<SimpleTable, String, Vec<u8>> {
    let ParsedDiffSet::Patchset(diff) = ParsedDiffSet::parse(bytes).unwrap() else {
        panic!("SQLite session must emit a patchset marker");
    };
    let pk_indices = table.pk_indices();
    let mut builder = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for op in diff.iter() {
        match op {
            PatchsetOp::Insert { values, .. } => {
                let mut insert = Insert::from(table.clone());
                for (index, value) in values.iter().enumerate() {
                    insert = insert.set(index, value.clone()).unwrap();
                }
                builder = builder.insert(insert);
            }
            PatchsetOp::Update { pk, entries, .. } => {
                let mut update = Update::<_, PatchsetFormat, String, Vec<u8>>::from(table.clone());
                for (value, &col) in pk.iter().zip(pk_indices.iter()) {
                    update = update.set(col, value.clone()).unwrap();
                }
                for (index, (_unit, new)) in entries.iter().enumerate() {
                    if !pk_indices.contains(&index) {
                        if let Some(value) = new {
                            update = update.set(index, value.clone()).unwrap();
                        }
                    }
                }
                builder = builder.update(update);
            }
            PatchsetOp::Delete { pk, .. } => {
                let delete = PatchDelete::new(table.clone(), pk.to_vec());
                builder = builder.delete(delete);
            }
        }
    }
    builder
}
