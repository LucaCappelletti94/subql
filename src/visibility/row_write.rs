use crate::backend::{Backend, Value};
use crate::{ColumnId, TableId, ValueError};

use super::WriteOp;

/// The write a question is about, carrying the row versions its verb needs.
///
/// A replacement is judged on two versions rather than one. The rule choosing
/// which rows a caller may touch reads the row as it is, and the rule admitting
/// the result reads the row as it will be, so one version cannot answer both:
/// judging the new one asks whether the **new** owner is the caller, which
/// grants a caller who holds nothing and writes themselves in as owner.
///
/// The verb and its versions therefore travel together, and the pairings that
/// cannot be answered are not constructible.
///
/// # Judging a replacement on one version, deliberately
///
/// [`UpdateUsing`](Self::UpdateUsing) is what a caller asks when it holds only
/// the row as it stands and wants to know whether a watcher may replace it at
/// all, which is the question handing on a delegated permission over a row
/// needs. It is the first rule alone, so it is a **weaker** question than
/// [`Update`](Self::Update) rather than a cheaper spelling of it.
///
/// `#[non_exhaustive]`: a verb this learns adds a variant, and an
/// implementation keeps a wildcard arm, which has to refuse rather than guess.
#[non_exhaustive]
pub enum RowWrite<'a, R: ?Sized> {
    /// Creating the row, judged on the row being created.
    Insert {
        /// The row as it will be.
        new: &'a R,
    },
    /// Replacing the row, judged on both versions.
    Update {
        /// The row as it is.
        old: &'a R,
        /// The row as it will be.
        new: &'a R,
    },
    /// Whether the row as it stands may be replaced at all, for a caller
    /// holding no other version.
    UpdateUsing {
        /// The row as it is.
        old: &'a R,
    },
    /// Removing the row, judged on the row being removed.
    Delete {
        /// The row as it is.
        old: &'a R,
    },
}

// `Clone` and `Copy` by hand rather than derived: this holds references, which
// are `Copy` whatever they point at, and the derive would demand `R: Copy` and
// so refuse every real row view.
impl<R: ?Sized> Clone for RowWrite<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R: ?Sized> Copy for RowWrite<'_, R> {}

impl<R: ?Sized> RowWrite<'_, R> {
    /// The verb, for an implementation wanting it without matching every
    /// version the question carries.
    ///
    /// [`UpdateUsing`](Self::UpdateUsing) reports [`WriteOp::Update`] because
    /// it is an update question. It is not the same question, so an
    /// implementation deciding what to consult matches the variant instead of
    /// reading this.
    #[must_use]
    pub const fn op(&self) -> WriteOp {
        match self {
            Self::Insert { .. } => WriteOp::Insert,
            Self::Update { .. } | Self::UpdateUsing { .. } => WriteOp::Update,
            Self::Delete { .. } => WriteOp::Delete,
        }
    }
}

/// Lazy per-column read of one row, bound to one version of it.
///
/// Implemented by subql over a [`CdcEvent`](crate::backend::CdcEvent) through
/// [`EventRow`](crate::visibility::EventRow), and by
/// a consumer over whatever it holds instead (a client-uploaded write,
/// say). Nothing is decoded until [`value_at`](Self::value_at) asks.
///
/// # The view must be complete
///
/// Every column of the table must be readable, not only the ones a
/// particular implementation happens to want. A row-level-security
/// backend reads the key alone, while a backend that evaluates rules
/// against the row reads whichever columns the rules mention, and a view
/// that could answer only some columns would make "this answer needed no
/// round trip" depend on which caller asked.
///
/// A caller holding only a key therefore reads the row before asking,
/// rather than handing over a view that reports the rest as
/// [`Value::Missing`].
///
/// [`EventRow`](crate::visibility::EventRow) enforces as much of this as an event's shape can settle:
/// it refuses to build a view for a version the event does not carry, or
/// for a table the catalog does not know. What it cannot settle is a
/// Postgres update whose old image is absent because the table runs
/// `REPLICA IDENTITY DEFAULT`, so a caller that asks about previous
/// versions has to require `FULL`
/// (see [`REPLICA_IDENTITY_AUDIT_SQL`](crate::REPLICA_IDENTITY_AUDIT_SQL)).
///
/// A cell that was carried but cannot be decoded is an `Err`, not a
/// [`Value::Missing`], so a corrupt row is distinguishable from an
/// absent one and an implementation can refuse rather than deny.
///
/// # Static or dynamic dispatch
///
/// [`VisibilityPolicy`](crate::visibility::VisibilityPolicy) takes the view as a generic parameter, so the
/// usual call monomorphises and costs no vtable. The trait is also
/// dyn-compatible, so a caller that would rather erase the view passes
/// `&(dyn RowView<Backend = _> + Sync)`. The `Sync` is not optional
/// there: the policy's futures are `Send`, and they hold the view.
pub trait RowView {
    /// The database whose [`Value`] shape this row carries.
    type Backend: Backend;

    /// Table the row belongs to.
    fn table_id(&self) -> TableId;

    /// Decode one cell of this row.
    ///
    /// Returns [`Value::Null`] for SQL NULL and `Err` for a cell that
    /// carried a value its catalog type cannot decode. Per the
    /// completeness contract above, a well-formed view does not answer
    /// [`Value::Missing`] for a column of its own table.
    fn value_at(&self, col: ColumnId) -> Result<Value<Self::Backend>, ValueError>;
}
