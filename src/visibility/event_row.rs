use sql_traits::prelude::DatabaseLike;

use crate::backend::{CdcEvent, RowKind, Value};
use crate::{ColumnId, EventKind, TableId, ValueError};

use super::RowView;

/// Whether `kind` carries the row image `row` names at all.
///
/// Structural only: it reports what the event kind can carry, not what a
/// particular source populated. An update whose old image is absent
/// because the table runs Postgres `REPLICA IDENTITY DEFAULT` still
/// answers `true` here, which is the residual [`EventRow`] documents.
const fn image_exists(kind: EventKind, row: RowKind) -> bool {
    !matches!(
        (kind, row),
        (EventKind::Truncate, _)
            | (EventKind::Insert, RowKind::Old)
            | (EventKind::Delete, RowKind::New)
    )
}

/// [`RowView`] over one version of a [`CdcEvent`](crate::backend::CdcEvent).
///
/// Borrows the event and the catalog and resolves the table once at
/// construction. Reading a column forwards to
/// [`CdcEvent::value_at`](crate::backend::CdcEvent::value_at), so no row image is materialised.
///
/// Build it with [`current`](Self::current) for the post-image and
/// [`previous`](Self::previous) for the pre-image. Those two names are
/// how a caller says which version a question is about.
///
/// # Construction can fail, on purpose
///
/// The constructors return [`None`] rather than a view that reads every
/// column as [`Value::Missing`], which is what a version the event does
/// not carry, or a table the catalog does not know, would otherwise
/// produce. Silence there is the wrong default for an authorization
/// seam: an implementation that reads "no rule found for this table" as
/// "no restriction" would fail open on a view nobody meant to build.
/// This is also how decision 3's "never ask about a version you have no
/// values for" stops being a rule a caller has to remember.
///
/// **The guarantee is structural, not per column.** It covers the two
/// cases the event's own shape settles: an image the event kind cannot
/// carry (a pre-image of an insert, a post-image of a delete, either of
/// a truncate) and a table absent from the catalog. It does not cover a
/// Postgres update whose old image is missing because the table runs
/// `REPLICA IDENTITY DEFAULT`, since that is indistinguishable from a
/// populated one without decoding. A caller that asks about previous
/// versions still has to require `FULL`
/// (see [`REPLICA_IDENTITY_AUDIT_SQL`](crate::REPLICA_IDENTITY_AUDIT_SQL)).
pub struct EventRow<'a, E, DB> {
    event: &'a E,
    db: &'a DB,
    row: RowKind,
    table: TableId,
}

impl<'a, E: CdcEvent, DB: DatabaseLike> EventRow<'a, E, DB> {
    /// View of the row image `row` selects, or [`None`] when the event
    /// does not carry it or its table is not in `db`.
    ///
    /// Prefer [`current`](Self::current) or [`previous`](Self::previous).
    /// This constructor exists for [`RowKind::Pk`], which projects the
    /// key alone and so does not satisfy [`RowView`]'s completeness
    /// contract even when it builds.
    ///
    /// Reads [`CdcEvent::kind`](crate::backend::CdcEvent::kind), so the event must be a row or truncate
    /// event. Sources reduce a raw stream with `into_engine_events`
    /// before anything reaches the engine, and the same applies here.
    #[must_use]
    pub fn new(event: &'a E, db: &'a DB, row: RowKind) -> Option<Self> {
        if !image_exists(event.kind(), row) {
            return None;
        }
        let table = event.table_id(db);
        // Catches the `TableId::MAX` sentinel a name-resolving event
        // returns for an unknown table, and any id the catalog lost.
        crate::catalog_helpers::table_arity(db, table).ok()?;
        Some(Self {
            event,
            db,
            row,
            table,
        })
    }

    /// View of the row as it is after the change ([`RowKind::New`]).
    ///
    /// [`None`] for a delete or a truncate, which have no post-image.
    #[must_use]
    pub fn current(event: &'a E, db: &'a DB) -> Option<Self> {
        Self::new(event, db, RowKind::New)
    }

    /// View of the row as it was before the change ([`RowKind::Old`]).
    ///
    /// [`None`] for an insert or a truncate, which have no pre-image.
    /// Complete per column only under `REPLICA IDENTITY FULL`. See
    /// [`RowView`] and the note on [`EventRow`].
    #[must_use]
    pub fn previous(event: &'a E, db: &'a DB) -> Option<Self> {
        Self::new(event, db, RowKind::Old)
    }

    /// Which row image this view reads.
    #[must_use]
    pub const fn row_kind(&self) -> RowKind {
        self.row
    }
}

impl<E: CdcEvent, DB: DatabaseLike> RowView for EventRow<'_, E, DB> {
    type Backend = E::Backend;

    fn table_id(&self) -> TableId {
        self.table
    }

    fn value_at(&self, col: ColumnId) -> Result<Value<Self::Backend>, ValueError> {
        self.event.value_at(self.db, self.row, col)
    }
}
