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
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct StoreDiff {
    /// Facts the row now states and did not before.
    pub added: Vec<Record>,
    /// Facts the row stated before and no longer does.
    pub removed: Vec<Record>,
}

/// Queries one changed row obliged, which nobody but the caller can run.
///
/// Handed back beside [`StoreDiff`] rather than inside it, because a caller
/// cannot receive the facts without also receiving this and saying what
/// becomes of it. Replay each one and hand the rows to the terminal
/// policy's `reconcile_records` before the event is delivered, per the
/// module doc. Leaving them unreplayed leaves every fact no single row
/// settles exactly as it was before the change, and in the allow direction
/// that is a row handed to somebody whose access has already gone, which no
/// later correction takes back.
///
/// Taking both halves is the ordinary shape, and it compiles.
///
/// ```
/// use subql::backend::CdcEvent;
/// use subql::visibility::shapes::Shapes;
/// use subql::DatabaseLike;
///
/// fn both_halves<DB: DatabaseLike, E: CdcEvent>(shapes: &Shapes<DB>, event: &E) -> usize {
///     let (diff, requeries) = shapes.diff(event).expect("the images are complete");
///     diff.added.len() + requeries.len()
/// }
/// ```
///
/// Taking the facts alone does not compile, because the queries are not a
/// field of the difference. The one difference from the example above is
/// the last line.
///
/// ```compile_fail
/// use subql::backend::CdcEvent;
/// use subql::visibility::shapes::Shapes;
/// use subql::DatabaseLike;
///
/// fn facts_alone<DB: DatabaseLike, E: CdcEvent>(shapes: &Shapes<DB>, event: &E) -> usize {
///     let (diff, requeries) = shapes.diff(event).expect("the images are complete");
///     diff.added.len() + diff.requeries.len()
/// }
/// ```
#[must_use = "a query nobody replays leaves the facts no single row settles stale, which keeps granting access that has gone"]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Requeries<'a, B: Backend>(Vec<Requery<'a, B>>);

impl<'a, B: Backend> Requeries<'a, B> {
    pub(crate) const fn new(queries: Vec<Requery<'a, B>>) -> Self {
        Self(queries)
    }

    /// Whether the change obliged no replay at all.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// How many queries the change obliged.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// The queries, in the order the shapes state them.
    #[must_use]
    pub fn as_slice(&self) -> &[Requery<'a, B>] {
        &self.0
    }

    /// Take the queries to replay them.
    #[must_use]
    pub fn into_vec(self) -> Vec<Requery<'a, B>> {
        self.0
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
