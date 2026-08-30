use rls2fga_types::{ActionStatement, RowVersion};

use crate::visibility::{RowView, RowWrite};
use crate::TableId;

/// The statement `write` is, so the model can be asked what answers it.
///
/// A replacement judged on the row as it stands is a locking read, which is what
/// Postgres filters by the update rule's `USING` clause as well, so that is the
/// statement it asks about rather than a replacement it cannot complete.
pub const fn statement_of<R: ?Sized>(write: &RowWrite<'_, R>) -> ActionStatement {
    match write {
        RowWrite::Insert { .. } => ActionStatement::Insert,
        RowWrite::Update { .. } => ActionStatement::Update,
        RowWrite::UpdateUsing { .. } => ActionStatement::SelectForUpdate,
        RowWrite::Delete { .. } => ActionStatement::Delete,
    }
}

/// The row image `version` names, or [`None`] when this write carries none.
///
/// A creation has no existing row and a removal no resulting one, so a judgement
/// naming a version the write does not carry cannot be answered here.
pub const fn image_of<'a, R: ?Sized>(
    write: &RowWrite<'a, R>,
    version: RowVersion,
) -> Option<&'a R> {
    match (write, version) {
        (RowWrite::Insert { new } | RowWrite::Update { new, .. }, RowVersion::Resulting) => {
            Some(new)
        }
        (
            RowWrite::Delete { old } | RowWrite::UpdateUsing { old } | RowWrite::Update { old, .. },
            RowVersion::Existing,
        ) => Some(old),
        // `RowVersion` is `#[non_exhaustive]`, and a version this does not know
        // is one it cannot supply an image for either way.
        _ => None,
    }
}

/// The table every image of `write` belongs to.
pub fn table_of<R>(write: &RowWrite<'_, R>) -> TableId
where
    R: RowView + ?Sized,
{
    match write {
        RowWrite::Insert { new } => new.table_id(),
        RowWrite::Update { old, .. } | RowWrite::UpdateUsing { old } | RowWrite::Delete { old } => {
            old.table_id()
        }
    }
}
