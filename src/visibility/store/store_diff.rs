use alloc::vec::Vec;

use rls2fga_types::Record;

use crate::backend::Backend;
use crate::visibility::records::RowRecordError;
use crate::ColumnId;

use super::Requery;

/// What one changed row moved.
///
/// `added` and `removed` are sorted and carry no duplicates, since two
/// shapes naming the same record state one fact.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreDiff<'a, B: Backend> {
    /// Facts the row now states and did not before.
    pub added: Vec<Record>,
    /// Facts the row stated before and no longer does.
    pub removed: Vec<Record>,
    /// Queries the caller replays, then hands the rows back through the
    /// terminal policy's `reconcile_records`. Due before the event is
    /// delivered: see the module doc.
    pub requeries: Vec<Requery<'a, B>>,
}

impl<B: Backend> StoreDiff<'_, B> {
    /// Nothing moved and nothing has to be replayed. The struct's fields
    /// are public, so a caller builds one literally rather than through
    /// this.
    pub(crate) const fn empty() -> Self {
        Self {
            added: Vec::new(),
            removed: Vec::new(),
            requeries: Vec::new(),
        }
    }
}

/// Why the difference could not be computed.
///
/// Every variant means the caller must not write anything, as distinct from
/// a difference that is legitimately empty.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StoreDiffError {
    /// The event names no row, which today means a truncate. An empty
    /// difference would read as "nothing moved", the opposite of the truth.
    #[error("the event names no row, so what moved is not knowable from it")]
    NotARowEvent,
    /// The event's table is not in the catalog, so no image can be read.
    #[error("the event's table is not in the catalog")]
    UnknownTable,
    /// The previous image carries its key and nothing else, so what the row
    /// granted before is not knowable and nothing can be removed.
    #[error("the previous row image carries only its key, so what it granted is not knowable")]
    IncompletePreviousImage,
    /// A query bound to this table needs a key the row does not carry, so
    /// the rows it reaches cannot be named and replaying is impossible.
    ///
    /// Distinct from a NULL key, which names no row and is simply skipped.
    /// A Postgres old image under `REPLICA IDENTITY DEFAULT` omits a
    /// non-key column, and a SQLite changeset omits a column an update left
    /// alone, so this reaches a key that is neither.
    #[error("the row does not carry column {0}, which a bound query needs as its key")]
    MissingBoundKey(ColumnId),
    /// A shape could not be evaluated against one of the images.
    #[error(transparent)]
    Row(#[from] RowRecordError),
}
