//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

/// Selector for which row view of a CDC event to read.
///
/// Every CDC event concerns one row identity. `Old` and `New` name the
/// before/after images (which may be absent depending on `EventKind`).
/// `Pk` names the PK projection of that row. `value_at` called with
/// `RowKind::Pk` and a `col` that is not in
/// [`crate::backend::CdcEvent::pk_columns`] returns [`crate::backend::Value::Missing`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowKind {
    /// Old row image — populated for Delete and (source-permitting) Update.
    Old,
    /// New row image — populated for Insert and Update.
    New,
    /// Primary-key projection — always populated for row-level events.
    Pk,
}
