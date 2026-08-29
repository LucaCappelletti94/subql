//! What one changed row moves in the authorization store.
//!
//! A per-row answer is worthless if the facts behind it are stale, so the
//! change stream also drives the store. For each changed row this reports
//! the facts it added, the facts it removed, and the queries the caller has
//! to replay for the facts no single row settles.
//!
//! # A wider index than the answering one
//!
//! [`RowPolicy`](crate::visibility::policy::RowPolicy) keys recipes by the
//! table whose rows the object is keyed on, because that is what makes a
//! relation answerable from one row. This cannot, because a shape may
//! describe one type from a different table: a membership row describes a
//! team, so a change to the membership table moves records on the team.
//! Every shape whose row table matches the changed row is therefore in
//! scope, across every relation and every type, and whether one row decides
//! the relation is irrelevant here.
//!
//! # Three kinds of shape, three outcomes
//!
//! A shape the row settles yields records directly, and the difference
//! between the two images is what moved. A shape whose records span two
//! tables cannot be differenced from one of them, so the query
//! `rls2fga` already bound to one row is handed over with the key read
//! off the row, and the caller runs it: the result is the whole truth for
//! the slice the query declares, so reconciling it both writes what is new
//! and takes out what the result stopped returning. A shape that can be
//! neither differenced nor replayed is named in
//! [`Shapes::uncovered`](crate::visibility::shapes::Shapes::uncovered) and nothing else, because a caller that
//! believes its store is complete when it is not is the failure this whole
//! path exists to remove. That covers a shape in a list column, and one
//! whose slice another shape also states, which reconciling would clobber.
//!
//! # When a replayed query has to have finished
//!
//! Applying the difference is not the caller's job, and replaying the
//! queries is. The records this reports reach the store through the terminal
//! policy's own `apply`, which orders the write against the questions that
//! read it. A replayed
//! query is the one part that cannot work that way, because its rows come
//! from a database only the caller can reach, and it carries an obligation
//! worth stating rather than leaving to be discovered.
//!
//! **Replay the query and hand what it returned to the terminal policy's
//! `reconcile_records` before the event is delivered.** Until then the store
//! still holds the facts from before the
//! change, so a question about any row those facts reach is answered from a
//! world that has moved. In the deny direction that costs a row delivered
//! late. In the allow direction it is a row handed to somebody whose access
//! has already gone, which is the one failure this path exists to remove,
//! and no later correction takes the row back.
//!
//! The cost is real and it is the reason this sentence exists rather than a
//! default: the change path then waits on one database read per affected
//! shape. A caller that would rather not wait can defer the replay, and is
//! thereby choosing a window whose width is its own replay lag, during which
//! a withdrawn permission still reads. That is a decision to take
//! deliberately, so it is written here rather than inferred from silence.
//!
//! # Refusing rather than emitting part of it
//!
//! A difference that is right about some shapes and silently wrong about
//! others is worse than no difference, so anything unreadable refuses the
//! whole event. That includes a previous image carrying only its key, which
//! cannot say what the row granted, and which would otherwise report every
//! fact as added and leave the old ones in the store forever.
//!
//! What `REPLICA IDENTITY FULL` settles, and what it does not. It gives an
//! update and a delete a complete previous image. It does not give an
//! insert one, which is structural rather than configured, and an insert is
//! simply all additions. And it does not reach the SQLite changeset source,
//! where an update carries the key plus the changed columns and marks the
//! rest absent on both sides. That case is correct here for a reason worth
//! stating rather than inheriting: a column that did not change reads
//! absent on both sides, yields no record on either, and so moves nothing.

use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use rls2fga_types::{
    BoundQuery, Record, RecordDerivation, RecordDescription, RelationShapes,
    TableId as ContractTableId,
};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{Backend, CdcEvent, Value};
use crate::visibility::records::{records_from_row_view, RowRecordError};
use crate::visibility::shapes::{Shapes, TableShapes};
use crate::visibility::transition::is_key_only;
use crate::visibility::{EventRow, RowView};
use crate::{ColumnId, EventKind};

// ---------------------------------------------------------------------------
// Uncovered
// ---------------------------------------------------------------------------

/// Why a shape's records cannot be kept current from the change stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UncoveredReason {
    /// The shape reads a column a row image cannot answer, a list column or
    /// one whose declared kind has no row-side spelling, and it carries no
    /// query to fall back on.
    UnreadableColumn,
    /// The shape reads this table but carries no query bound to it, so a
    /// change arriving here has nothing to replay.
    NoBoundQuery,
    /// The slice a replay of this shape determines is also stated by another
    /// shape, so reconciling it would delete that shape's facts.
    SharedSlice,
}

/// A shape whose records this cannot keep current, named so a caller does
/// not believe its store is complete when it is not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Uncovered {
    /// `OpenFGA` type the relation is defined on.
    pub type_name: String,
    /// Relation whose records go unmaintained.
    pub relation: String,
    /// Table whose changes cannot be turned into record movement.
    pub table: String,
    /// Which of the two gaps this is.
    pub reason: UncoveredReason,
}

// ---------------------------------------------------------------------------
// Requery
// ---------------------------------------------------------------------------

/// A query to replay for one changed row, because its records span more
/// than that row.
///
/// Replaying it and handing the rows to the terminal policy's
/// `reconcile_records` is the caller's, and it is
/// due before the event is delivered. See the module's own section on when a
/// replayed query has to have finished, which says what a caller accepts by
/// deferring it. The result is the whole truth for the slice
/// [`BoundQuery::scope`](rls2fga_types::BoundQuery::scope)
/// declares, which is what makes the reconciliation able to remove.
///
/// The key is a typed [`Value`] rather than text, so the caller binds it
/// through its own type system and no cast is needed anywhere.
#[derive(Clone, Debug, PartialEq)]
pub struct Requery<'a, B: Backend> {
    /// The query `rls2fga` bound to one row of this table. Its SQL takes
    /// the key as `$1` through `$n`.
    pub query: &'a BoundQuery,
    /// The values to bind, one per column of
    /// [`BoundQuery::key_columns`](rls2fga_types::BoundQuery::key_columns)
    /// and in that order. Several of them where the key spans several columns,
    /// which is one key rather than several.
    pub key: Vec<Value<B>>,
}

// ---------------------------------------------------------------------------
// StoreDiff
// ---------------------------------------------------------------------------

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
    const fn empty() -> Self {
        Self {
            added: Vec::new(),
            removed: Vec::new(),
            requeries: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The difference one changed row makes
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// The difference one changed row makes
// ---------------------------------------------------------------------------

impl<DB: DatabaseLike> Shapes<DB> {
    /// What `event` moved.
    ///
    /// # Errors
    ///
    /// [`StoreDiffError::NotARowEvent`] for a truncate,
    /// [`StoreDiffError::UnknownTable`] when the catalog does not know the
    /// event's table, [`StoreDiffError::IncompletePreviousImage`] when a
    /// shape had to read a previous image that carries only its key, and
    /// [`StoreDiffError::Row`] when an image could not be read. None yields
    /// a partial difference.
    pub fn diff<E>(&self, event: &E) -> Result<StoreDiff<'_, E::Backend>, StoreDiffError>
    where
        E: CdcEvent,
    {
        if event.kind() == EventKind::Truncate {
            return Err(StoreDiffError::NotARowEvent);
        }
        let db = self.catalog();
        let current = EventRow::current(event, db);
        let previous = EventRow::previous(event, db);
        let Some(table) = current
            .as_ref()
            .map(RowView::table_id)
            .or_else(|| previous.as_ref().map(RowView::table_id))
        else {
            return Err(StoreDiffError::UnknownTable);
        };
        let Some(shapes) = self.table_shapes(table) else {
            return Ok(StoreDiff::empty());
        };

        // Only a settled shape has to read the row, so a key-only previous
        // image is harmless for a table reached only by a bound query.
        let before = match previous.as_ref().filter(|_| !shapes.settled.is_empty()) {
            Some(row) if is_key_only(row, event, db) => {
                return Err(StoreDiffError::IncompletePreviousImage)
            }
            Some(row) => records_of::<_, DB>(&shapes.settled, row, db)?,
            None => BTreeSet::new(),
        };
        let after = match current.as_ref() {
            Some(row) => records_of(&shapes.settled, row, db)?,
            None => BTreeSet::new(),
        };

        Ok(StoreDiff {
            added: after.difference(&before).cloned().collect(),
            removed: before.difference(&after).cloned().collect(),
            requeries: requeries(shapes, current.as_ref(), previous.as_ref())?,
        })
    }
}

/// One entry per query and per distinct key the two images carry for it.
///
/// Both images are read because moving the bound key moves records under the
/// old value and the new one, and replaying only one of them leaves the
/// other stale.
fn requeries<'a, R>(
    shapes: &'a TableShapes,
    current: Option<&R>,
    previous: Option<&R>,
) -> Result<Vec<Requery<'a, R::Backend>>, StoreDiffError>
where
    R: RowView,
{
    let mut out = Vec::new();
    for (key_columns, query) in &shapes.requeries {
        for row in [current, previous].into_iter().flatten() {
            let Some(key) = read_key(row, key_columns)? else {
                continue;
            };
            // Per query, not per key. Two shapes can bind the same table on
            // the same columns, and dropping the second because its key was
            // already seen would leave the records it reaches stale. An
            // identical query indexed twice, which happens when one source
            // feeds several relations, still collapses here.
            if out
                .iter()
                .any(|seen: &Requery<'a, R::Backend>| seen.query == query && seen.key == key)
            {
                continue;
            }
            out.push(Requery { query, key });
        }
    }
    Ok(out)
}

/// Every column of one query's key read off one image, in the order its
/// placeholders take them, or [`None`] where the key names no row.
///
/// # Errors
///
/// [`StoreDiffError::MissingBoundKey`], naming the column, when the image does
/// not carry one the query needs.
fn read_key<R>(
    row: &R,
    key_columns: &[ColumnId],
) -> Result<Option<Vec<Value<R::Backend>>>, StoreDiffError>
where
    R: RowView,
{
    let mut key = Vec::with_capacity(key_columns.len());
    for column in key_columns {
        let value = row.value_at(*column).map_err(RowRecordError::from)?;
        match value {
            // A NULL column names no row whatever the rest of the key holds,
            // since the equality the query binds is never true for it. One is
            // enough, so the remaining columns go unread.
            Value::Null => return Ok(None),
            // The source did not carry this column, so the rows this query
            // reaches cannot be named. Skipping would leave them stale with
            // nothing saying so.
            Value::Missing => return Err(StoreDiffError::MissingBoundKey(*column)),
            _ => key.push(value),
        }
    }
    Ok(Some(key))
}

/// Name the shape a gap belongs to, for [`Shapes::uncovered`](crate::visibility::shapes::Shapes::uncovered).
pub(crate) fn name_gap(
    entry: &RelationShapes,
    shape: &RecordDescription,
    table: &ContractTableId,
    reason: UncoveredReason,
) -> Uncovered {
    // A settled shape names its own type and relation, which is what a
    // reader needs when one relation is filled from several shapes. A
    // joining one carries no template, so the entry answers instead.
    let (type_name, relation) = match &shape.derivation {
        RecordDerivation::FromRow { template, .. } => {
            (template.object_type.as_str(), template.relation.as_str())
        }
        _ => (entry.type_name.as_str(), entry.relation.as_str()),
    };
    Uncovered {
        type_name: type_name.to_string(),
        relation: relation.to_string(),
        table: table.to_string(),
        reason,
    }
}

/// Every record `shapes` state about `row`, deduplicated.
fn records_of<R, DB>(
    shapes: &[RecordDescription],
    row: &R,
    db: &DB,
) -> Result<BTreeSet<Record>, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    let mut out = BTreeSet::new();
    for shape in shapes {
        out.extend(records_from_row_view::<R, DB>(shape, row, db)?);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;

    use rls2fga::generator::well_known::{
        can_delete_relation, can_select_relation, member_relation,
    };
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga_types::ConfidenceLevel;
    use rls2fga_types::RecordError;
    use rls2fga_types::RelationName;
    use rls2fga_types::RelationShapes;
    use rls2fga_types::{BoundQuery, Record, RecordDerivation, RecordDescription, ReplayScope};
    use sqlparser::dialect::PostgreSqlDialect;

    use super::{StoreDiffError, UncoveredReason};
    use crate::backend::{CdcEvent, Postgres, RowKind, Value};
    use crate::testing::TestEvent;
    use crate::visibility::records::RowRecordError;
    use crate::visibility::shapes::Shapes;
    use crate::visibility::test_names;
    use crate::{
        catalog_helpers, ColumnId, EventKind, NoCheckpoint, ParserDB, TableId, ValueError,
    };
    use sql_traits::prelude::DatabaseLike;

    // -----------------------------------------------------------------
    // Schemas, translated by the real producer
    // -----------------------------------------------------------------

    /// One settled shape on `docs`, and `notes` which nothing reads.
    const OWNERSHIP: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT, body TEXT);
CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);
";

    /// `teams#member` is settled from rows of `team_members`, so a change to
    /// one table moves records on another type.
    const MEMBERSHIP: &str = "
CREATE TABLE teams(id INTEGER PRIMARY KEY);
CREATE TABLE team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user));
";

    /// The same membership with a residual the row image can evaluate, so the
    /// shape still settles and the residual travels as a guard.
    const RESIDUAL: &str = "
CREATE TABLE teams(id INTEGER PRIMARY KEY);
CREATE TABLE team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT, active BOOLEAN);
CREATE TABLE docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user
            AND team_members.active));
";

    /// The same membership gated by the clock, which no row image can
    /// evaluate, so `teams#member` joins and reaches `team_members` through a
    /// bound query instead.
    const EXPIRING: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
    expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user
            AND team_members.expires_at > now()));
";

    /// Two membership sources feeding `teams#member`: one the row settles and
    /// one only a replay reaches. The replay's slice is also stated by the
    /// settled shape, so reconciling it would delete that shape's facts.
    const SHARED_SLICE: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE public.team_guests(team_id INTEGER REFERENCES teams(id), user_id TEXT,
    expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user)
  OR EXISTS (SELECT 1 FROM team_guests
          WHERE team_guests.team_id = docs.team_id AND team_guests.user_id = current_user
            AND team_guests.expires_at > now()));
";

    /// The subjects live in a list column, which no row image can expand.
    const ARRAY: &str = "
CREATE TABLE docs(id INTEGER PRIMARY KEY, members TEXT[]);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (current_user = ANY(members));
";

    /// A key spanning two columns, so a bound query names a row by the whole
    /// of it. The shape is hand built below, since every schema-derived shape
    /// with a residual binds a single joining column.
    const COMPOUND_KEY: &str = "
CREATE TABLE readings(tenant_id INTEGER, reading_id INTEGER, starts_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, reading_id));
";

    fn shapes(sql: &str) -> Shapes<ParserDB> {
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let relations = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .relations()
            .to_vec();
        Shapes::new::<Postgres>(db, &relations)
    }

    fn table(shapes: &Shapes<ParserDB>, name: &str) -> TableId {
        catalog_helpers::table_id(shapes.catalog(), name).unwrap()
    }

    fn text(value: &str) -> Value<Postgres> {
        Value::String(value.into())
    }

    fn record(object: &str, relation: RelationName, subject: &str) -> Record {
        Record {
            object: object.into(),
            relation,
            subject: subject.into(),
            context: None,
        }
    }

    // -----------------------------------------------------------------
    // The difference
    // -----------------------------------------------------------------

    /// The load-bearing case. A version that only ever adds passes every
    /// insert test and leaves the old grant in the store forever.
    #[test]
    fn changing_the_owner_removes_the_old_record_and_adds_the_new_one() {
        let store = shapes(OWNERSHIP);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::update(
            docs,
            vec![Value::Int(4), text("alice"), text("body")],
            vec![Value::Int(4), text("bob"), text("body")],
        )
        .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert_eq!(
            diff.added,
            [record("docs:4", test_names::relation("owner"), "user:bob")]
        );
        assert_eq!(
            diff.removed,
            [record(
                "docs:4",
                test_names::relation("owner"),
                "user:alice"
            )]
        );
    }

    /// An update that leaves the granting column alone moves nothing, so the
    /// store is not rewritten on every unrelated edit.
    #[test]
    fn changing_an_unrelated_column_moves_nothing() {
        let store = shapes(OWNERSHIP);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::update(
            docs,
            vec![Value::Int(4), text("alice"), text("before")],
            vec![Value::Int(4), text("alice"), text("after")],
        )
        .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert!(diff.added.is_empty(), "{:?}", diff.added);
        assert!(diff.removed.is_empty(), "{:?}", diff.removed);
    }

    /// An insert has no previous row, structurally, so everything is new.
    #[test]
    fn an_insert_only_adds() {
        let store = shapes(OWNERSHIP);
        let docs = table(&store, "docs");
        let event =
            TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice"), text("body")])
                .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert_eq!(
            diff.added,
            [record(
                "docs:4",
                test_names::relation("owner"),
                "user:alice"
            )]
        );
        assert!(
            diff.removed.is_empty(),
            "a new row has no previous grants to remove"
        );
    }

    /// A delete takes every record the row held with it.
    #[test]
    fn a_delete_only_removes() {
        let store = shapes(OWNERSHIP);
        let docs = table(&store, "docs");
        let event =
            TestEvent::<Postgres>::delete(docs, vec![Value::Int(4), text("alice"), text("body")])
                .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert!(
            diff.added.is_empty(),
            "a deleted row grants nobody going forward"
        );
        assert_eq!(
            diff.removed,
            [record(
                "docs:4",
                test_names::relation("owner"),
                "user:alice"
            )]
        );
    }

    /// A row whose granting column is NULL grants nobody, and a row that
    /// gains one is an addition with nothing to remove.
    #[test]
    fn a_null_granting_column_holds_no_record() {
        let store = shapes(OWNERSHIP);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::update(
            docs,
            vec![Value::Int(4), Value::Null, text("body")],
            vec![Value::Int(4), text("alice"), text("body")],
        )
        .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert_eq!(
            diff.added,
            [record(
                "docs:4",
                test_names::relation("owner"),
                "user:alice"
            )]
        );
        assert!(
            diff.removed.is_empty(),
            "the old image granted nobody so nothing was held"
        );
    }

    /// A truncate names no row. Reporting an empty difference for it would
    /// read as "nothing moved", the opposite of what a truncate does.
    #[test]
    fn a_truncate_is_refused() {
        let store = shapes(OWNERSHIP);
        let event = TestEvent::<Postgres>::truncate(table(&store, "docs"));

        assert_eq!(store.diff(&event), Err(StoreDiffError::NotARowEvent));
    }

    /// A table the catalog does not know cannot be read at all.
    #[test]
    fn an_unknown_table_is_refused() {
        let store = shapes(OWNERSHIP);
        let event = TestEvent::<Postgres>::insert(TableId::MAX, vec![Value::Int(4)]);

        assert_eq!(store.diff(&event), Err(StoreDiffError::UnknownTable));
    }

    /// A previous image carrying its key and nothing else cannot say what
    /// the row granted, so reporting only additions would leave the old
    /// record in the store forever.
    #[test]
    fn a_key_only_previous_image_is_refused() {
        let store = shapes(OWNERSHIP);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::update(
            docs,
            vec![Value::Int(4), Value::Missing, Value::Missing],
            vec![Value::Int(4), text("bob"), text("body")],
        )
        .with_pk_columns([0u16]);

        assert_eq!(
            store.diff(&event),
            Err(StoreDiffError::IncompletePreviousImage)
        );
    }

    /// A table nothing reads produces an empty difference rather than an
    /// error, so an unrelated change stream costs nothing.
    #[test]
    fn a_table_no_shape_reads_produces_an_empty_difference() {
        let store = shapes(OWNERSHIP);
        let notes = table(&store, "notes");
        let event = TestEvent::<Postgres>::insert(notes, vec![Value::Int(1), text("hi")])
            .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert!(
            diff.added.is_empty(),
            "no shape reads the notes table so nothing is added"
        );
        assert!(
            diff.removed.is_empty(),
            "no shape reads the notes table so nothing is removed"
        );
        assert!(
            diff.requeries.is_empty(),
            "no shape reads the notes table so nothing is requeried"
        );
    }

    // -----------------------------------------------------------------
    // The index is wider than the answering one
    // -----------------------------------------------------------------

    /// `teams#member` is settled from rows of `team_members`, so a change to
    /// the membership table moves records on `teams`. Keying this the way
    /// the answering path keys recipes, by the object's own table, would
    /// miss it entirely.
    #[test]
    fn a_change_to_a_membership_table_moves_records_on_another_type() {
        let store = shapes(MEMBERSHIP);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::update(
            members,
            vec![Value::Int(3), text("alice")],
            vec![Value::Int(3), text("bob")],
        );

        let diff = store.diff(&event).unwrap();

        assert_eq!(
            diff.added,
            [record("teams:3", member_relation(), "user:bob")]
        );
        assert_eq!(
            diff.removed,
            [record("teams:3", member_relation(), "user:alice")]
        );
    }

    // -----------------------------------------------------------------
    // Handing the rest over
    // -----------------------------------------------------------------

    /// A shape whose records span two tables cannot be differenced here, so
    /// the query rls2fga already bound to one row is handed over with the
    /// key read off the row.
    #[test]
    fn a_two_table_shape_hands_over_its_query_with_the_key_bound() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::insert(
            members,
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();

        assert!(diff.added.is_empty(), "no shape settles this table");
        assert!(
            diff.removed.is_empty(),
            "an insert into a two-table shape removes nothing"
        );
        assert_eq!(diff.requeries.len(), 1);
        let requery = &diff.requeries[0];
        assert_eq!(requery.query.table.name(), "team_members");
        assert_eq!(requery.query.key_columns, ["team_id"]);
        assert!(requery.query.sql.contains("$1"));
        assert_eq!(requery.key, [Value::Int(3)]);
    }

    /// Moving the key moves records under both the old value and the new
    /// one, so both have to be replayed.
    #[test]
    fn moving_the_bound_key_hands_over_both_values() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::update(
            members,
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
            vec![Value::Int(4), text("alice"), text("2027-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();

        let keys: Vec<Vec<Value<Postgres>>> =
            diff.requeries.iter().map(|r| r.key.clone()).collect();
        assert_eq!(keys, [vec![Value::Int(4)], vec![Value::Int(3)]]);
    }

    /// The same key on both sides is replayed once.
    #[test]
    fn an_unmoved_bound_key_is_handed_over_once() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::update(
            members,
            vec![Value::Int(3), text("alice"), text("2026-01-01T00:00:00Z")],
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();

        assert_eq!(diff.requeries.len(), 1);
        assert_eq!(diff.requeries[0].key, [Value::Int(3)]);
    }

    /// A key-only previous image is only a problem for a shape that had to
    /// read the row. A table reached only by a bound query needs the key,
    /// which a key-only image carries.
    #[test]
    fn a_key_only_previous_image_still_hands_over_its_query() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::update(
            members,
            vec![Value::Int(3), Value::Missing, Value::Missing],
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
        )
        .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert_eq!(diff.requeries.len(), 1);
        assert_eq!(diff.requeries[0].key, [Value::Int(3)]);
    }

    /// The finding this module's reconciliation contract closes: a residual
    /// the row image evaluates settles the shape, so deleting the row that
    /// carried a permission reports the fact it stated as removed.
    #[test]
    fn withdrawing_a_residual_guarded_grant_reports_the_removal() {
        let store = shapes(RESIDUAL);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::delete(
            members,
            vec![Value::Int(3), text("alice"), Value::Bool(true)],
        );

        let diff = store.diff(&event).unwrap();
        assert!(diff.added.is_empty(), "{:?}", diff.added);
        assert_eq!(
            diff.removed,
            [record("teams:3", member_relation(), "user:alice")],
            "alice's membership is gone, so the fact it carried has to be removed"
        );
        assert!(
            diff.requeries.is_empty(),
            "the row settles the shape, so nothing is left to replay"
        );
    }

    /// A row the residual refuses states no record, exactly as the loading
    /// SQL's `WHERE` leaves it out.
    #[test]
    fn a_row_the_residual_refuses_states_no_record() {
        let store = shapes(RESIDUAL);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::insert(
            members,
            vec![Value::Int(3), text("alice"), Value::Bool(false)],
        );

        let diff = store.diff(&event).unwrap();
        assert!(diff.added.is_empty(), "{:?}", diff.added);
    }

    /// The genuinely joined sibling: the clock keeps the shape joined, so a
    /// delete reports no removal itself and instead hands over the replay
    /// whose reconciliation takes the stale fact out. The slice it declares
    /// is what makes that removal possible, so it is pinned here.
    #[test]
    fn withdrawing_an_expiring_grant_hands_over_the_replay_and_its_slice() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::delete(
            members,
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();
        assert!(diff.added.is_empty(), "{:?}", diff.added);
        assert!(
            diff.removed.is_empty(),
            "no row image evaluates the clock, so the replay is the remover"
        );
        assert_eq!(diff.requeries.len(), 1);
        let requery = &diff.requeries[0];
        assert_eq!(requery.key, [Value::Int(3)]);
        assert_eq!(
            requery.query.scope,
            ReplayScope::Object {
                object_type: "teams".to_string(),
                relations: alloc::vec![member_relation()],
            },
            "the replay determines the one team's member facts"
        );
    }

    /// A table whose key spans two columns is replayed by the whole of it.
    ///
    /// Binding the first column alone returns every row sharing it. Each row
    /// it returns is a real grant, so nothing looks wrong, but as a set to
    /// reconcile against it is wrong in both directions: it rewrites records
    /// it need not, and a caller that removes whatever the query stopped
    /// returning removes another row's records.
    #[test]
    fn a_compound_key_hands_over_every_column_in_placeholder_order() {
        let store = compound_key_store();
        let readings = table(&store, "readings");
        let event = TestEvent::<Postgres>::insert(
            readings,
            vec![Value::Int(7), Value::Int(9), text("2026-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();

        assert_eq!(diff.requeries.len(), 1, "{:?}", diff.requeries);
        let requery = &diff.requeries[0];
        assert_eq!(requery.query.key_columns, ["tenant_id", "reading_id"]);
        assert_eq!(requery.key, [Value::Int(7), Value::Int(9)]);
        // What makes the pair above one row's key rather than two values in
        // some order: the query takes them in exactly this order.
        assert!(
            requery
                .query
                .sql
                .contains("\"tenant_id\" = $1 AND \"reading_id\" = $2"),
            "{}",
            requery.query.sql
        );
    }

    // -----------------------------------------------------------------
    // Naming what is not covered
    // -----------------------------------------------------------------

    /// A list column can be neither differenced nor queried, so the caller
    /// is told rather than left to assume the store is complete.
    #[test]
    fn a_list_column_shape_is_named_as_uncovered() {
        let store = shapes(ARRAY);
        let docs = table(&store, "docs");

        let uncovered = store.uncovered();
        assert_eq!(uncovered.len(), 1, "{uncovered:?}");
        assert_eq!(uncovered[0].type_name, "docs");
        assert_eq!(uncovered[0].relation, "members");
        assert_eq!(uncovered[0].table, "docs");
        assert_eq!(uncovered[0].reason, UncoveredReason::UnreadableColumn);

        // And it is refused rather than reported as a row granting nobody.
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Missing])
            .with_pk_columns([0u16]);
        let diff = store.diff(&event).unwrap();
        assert!(
            diff.added.is_empty(),
            "the unreadable column produces no grants to add"
        );
        assert!(
            diff.removed.is_empty(),
            "the row is new so nothing was held before"
        );
    }

    /// A replay's slice another shape also states is not its own to
    /// reconcile: what the replay stopped returning may be the other shape's
    /// living fact. The joined shape is named uncovered and hands nothing
    /// over, while the settled sibling keeps differencing.
    #[test]
    fn a_replay_whose_slice_another_shape_states_is_named_as_uncovered() {
        let store = shapes(SHARED_SLICE);

        let uncovered = store.uncovered();
        assert!(
            uncovered.iter().any(|gap| gap.table == "public.team_guests"
                && gap.reason == UncoveredReason::SharedSlice),
            "the guest shape's slice is also stated by the member shape: {uncovered:?}"
        );

        let guests = table(&store, "team_guests");
        let event = TestEvent::<Postgres>::delete(
            guests,
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
        );
        let diff = store.diff(&event).unwrap();
        assert!(
            diff.requeries.is_empty(),
            "a replay nobody may reconcile is not handed over: {:?}",
            diff.requeries
        );

        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::delete(members, vec![Value::Int(3), text("alice")]);
        let diff = store.diff(&event).unwrap();
        assert_eq!(
            diff.removed,
            [record("teams:3", member_relation(), "user:alice")],
            "the settled sibling still differences its own rows"
        );
    }

    /// A schema whose every shape is readable reports nothing uncovered, so
    /// the list above is a real signal rather than always populated.
    #[test]
    fn a_fully_readable_schema_reports_nothing_uncovered() {
        assert!(
            shapes(OWNERSHIP).uncovered().is_empty(),
            "every column in the ownership schema is readable"
        );
        assert!(
            shapes(MEMBERSHIP).uncovered().is_empty(),
            "every column in the membership schema is readable"
        );
    }

    // -----------------------------------------------------------------
    // Refusing rather than emitting a partial difference
    // -----------------------------------------------------------------

    /// A cell the row carried but could not decode makes the whole
    /// difference unknowable. Emitting the part that decoded would write a
    /// store update that is wrong in an invisible way.
    #[test]
    fn an_undecodable_cell_refuses_the_whole_difference() {
        struct Corrupt(TableId);

        impl CdcEvent for Corrupt {
            type Backend = Postgres;
            type Checkpoint = NoCheckpoint;

            fn kind(&self) -> EventKind {
                EventKind::Insert
            }
            fn table_id<DB: DatabaseLike>(&self, _db: &DB) -> TableId {
                self.0
            }
            fn checkpoint(&self) -> Option<NoCheckpoint> {
                None
            }
            fn pk_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
                vec![0]
            }
            fn changed_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
                Vec::new()
            }
            fn value_at<DB: DatabaseLike>(
                &self,
                _db: &DB,
                _row: RowKind,
                col: ColumnId,
            ) -> Result<Value<Postgres>, ValueError> {
                if col == 0 {
                    return Ok(Value::Int(4));
                }
                Err(ValueError::Builtin {
                    column: col,
                    kind: crate::backend::BuiltinKind::String,
                })
            }
        }

        let store = shapes(OWNERSHIP);
        let event = Corrupt(table(&store, "docs"));

        let got = store.diff(&event);

        assert!(
            matches!(
                got,
                Err(StoreDiffError::Row(RowRecordError::Refused(
                    RecordError::ColumnUndecodable(_)
                )))
            ),
            "{got:?}"
        );
    }

    /// The same refusal on the key a bound query needs. A table reached only
    /// by a query settles no records, so nothing else would read the row and
    /// the corrupt key would go unnoticed.
    #[test]
    fn an_undecodable_bound_key_refuses_rather_than_skipping() {
        struct Corrupt(TableId);

        impl CdcEvent for Corrupt {
            type Backend = Postgres;
            type Checkpoint = NoCheckpoint;

            fn kind(&self) -> EventKind {
                EventKind::Insert
            }
            fn table_id<DB: DatabaseLike>(&self, _db: &DB) -> TableId {
                self.0
            }
            fn checkpoint(&self) -> Option<NoCheckpoint> {
                None
            }
            fn pk_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
                Vec::new()
            }
            fn changed_columns<DB: DatabaseLike>(&self, _db: &DB) -> Vec<ColumnId> {
                Vec::new()
            }
            fn value_at<DB: DatabaseLike>(
                &self,
                _db: &DB,
                _row: RowKind,
                col: ColumnId,
            ) -> Result<Value<Postgres>, ValueError> {
                Err(ValueError::Builtin {
                    column: col,
                    kind: crate::backend::BuiltinKind::Int,
                })
            }
        }

        let store = shapes(RESIDUAL);
        let event = Corrupt(table(&store, "team_members"));

        let got = store.diff(&event);

        assert!(
            matches!(
                got,
                Err(StoreDiffError::Row(RowRecordError::Refused(
                    RecordError::ColumnUndecodable(_)
                )))
            ),
            "{got:?}"
        );
    }

    /// A NULL key names no row. The query's own `IS NOT NULL` guard selects
    /// nothing for it, so replaying it would be a round trip for no rows.
    #[test]
    fn a_null_bound_key_names_no_row_to_replay() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::insert(
            members,
            vec![Value::Null, text("alice"), text("2027-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();

        assert!(diff.requeries.is_empty(), "{:?}", diff.requeries);
    }

    /// A NULL in any column of the key names no row, not only in the first.
    #[test]
    fn a_null_later_in_a_compound_key_names_no_row_to_replay() {
        let store = compound_key_store();
        let readings = table(&store, "readings");
        let event = TestEvent::<Postgres>::insert(
            readings,
            vec![Value::Int(7), Value::Null, text("2026-01-01T00:00:00Z")],
        );

        let diff = store.diff(&event).unwrap();

        assert!(diff.requeries.is_empty(), "{:?}", diff.requeries);
    }

    /// A key the source did not carry is a different answer from a NULL one.
    /// Skipping it would leave every record that query reaches stale, with
    /// nothing saying so.
    #[test]
    fn a_bound_key_the_source_did_not_carry_is_refused() {
        let store = shapes(EXPIRING);
        let members = table(&store, "team_members");
        let event = TestEvent::<Postgres>::insert(
            members,
            vec![Value::Missing, text("alice"), text("2027-01-01T00:00:00Z")],
        );

        assert_eq!(store.diff(&event), Err(StoreDiffError::MissingBoundKey(0)));
    }

    /// Half a key names no row either, and which half was missing is what a
    /// caller needs to know, so the error names that column rather than the
    /// first of the key.
    #[test]
    fn a_compound_key_the_source_carried_in_part_is_refused() {
        let store = compound_key_store();
        let readings = table(&store, "readings");
        let event = TestEvent::<Postgres>::insert(
            readings,
            vec![Value::Int(7), Value::Missing, text("2026-01-01T00:00:00Z")],
        );

        assert_eq!(store.diff(&event), Err(StoreDiffError::MissingBoundKey(1)));
    }

    /// A joining shape is indexed by the tables its queries bind. A query
    /// naming a table or a column the catalog does not have is skipped,
    /// since that table produces no events here. A table the shape reads
    /// with no query bound to it is named instead, because a change
    /// arriving there would have nothing to replay.
    #[test]
    fn a_joining_shape_indexes_only_the_queries_the_catalog_resolves() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);",
        )
        .unwrap();
        let store = Shapes::new::<Postgres>(
            db,
            &[joined(
                &["docs", "grants"],
                vec![
                    bound("docs", &["id"]),
                    bound("absent_table", &["id"]),
                    bound("docs", &["absent_column"]),
                ],
            )],
        );

        let uncovered = store.uncovered();
        assert_eq!(uncovered.len(), 1, "{uncovered:?}");
        assert_eq!(uncovered[0].table, "grants");
        assert_eq!(uncovered[0].type_name, "docs");
        assert_eq!(uncovered[0].relation, "can_select");
        assert_eq!(uncovered[0].reason, UncoveredReason::NoBoundQuery);

        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice")])
            .with_pk_columns([0u16]);
        let diff = store.diff(&event).unwrap();

        assert_eq!(diff.requeries.len(), 1, "only the resolvable query");
        assert_eq!(diff.requeries[0].query.key_columns, ["id"]);
    }

    /// A key this catalog cannot bind whole is dropped whole, in either of the
    /// two ways that happens: a column it does not have, and no columns at all.
    /// Keeping the columns that did resolve would answer for every row sharing
    /// them, and the SQL takes one placeholder per column so it could not be
    /// run anyway. The table is named instead of being left silently unserved,
    /// which is the difference between a gap a caller can see and one it
    /// cannot.
    #[test]
    fn a_key_this_catalog_cannot_bind_drops_the_query_and_names_the_table() {
        for key_columns in [vec!["id", "absent_column"], Vec::new()] {
            let db = ParserDB::parse::<PostgreSqlDialect>(
                "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);",
            )
            .unwrap();
            let store = Shapes::new::<Postgres>(
                db,
                &[joined(&["docs"], vec![bound("docs", &key_columns)])],
            );

            let uncovered = store.uncovered();
            assert_eq!(uncovered.len(), 1, "{key_columns:?}: {uncovered:?}");
            assert_eq!(uncovered[0].table, "docs", "{key_columns:?}");
            assert_eq!(
                uncovered[0].reason,
                UncoveredReason::NoBoundQuery,
                "{key_columns:?}"
            );

            let docs = table(&store, "docs");
            let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice")])
                .with_pk_columns([0u16]);
            let diff = store.diff(&event).unwrap();
            assert!(
                diff.requeries.is_empty(),
                "{key_columns:?}: {:?}",
                diff.requeries
            );
        }
    }

    /// One shape per relation, both binding the same table on the same
    /// column. Deduplicating on the key alone would drop the second query
    /// and leave every record it reaches stale.
    #[test]
    fn two_queries_binding_the_same_key_are_both_handed_over() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);",
        )
        .unwrap();
        let relations = vec![
            joining(
                can_select_relation(),
                &[can_select_relation()],
                "SELECT 'read' WHERE id = $1;",
            ),
            joining(
                can_delete_relation(),
                &[can_delete_relation()],
                "SELECT 'delete' WHERE id = $1;",
            ),
        ];
        let store = Shapes::new::<Postgres>(db, &relations);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice")])
            .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        let mut sql: Vec<&str> = diff
            .requeries
            .iter()
            .map(|requery| requery.query.sql.as_str())
            .collect();
        sql.sort_unstable();
        assert_eq!(
            sql,
            [
                "SELECT 'delete' WHERE id = $1;",
                "SELECT 'read' WHERE id = $1;"
            ]
        );
        assert!(diff.requeries.iter().all(|r| r.key == [Value::Int(4)]));
    }

    /// The same query reached through two relations, which is what one tuple
    /// source feeding several of them produces, is still replayed once.
    #[test]
    fn the_same_query_reached_twice_is_handed_over_once() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);",
        )
        .unwrap();
        let same = "SELECT 'read' WHERE id = $1;";
        let relations = vec![
            joining(
                can_select_relation(),
                &[can_select_relation(), can_delete_relation()],
                same,
            ),
            joining(
                can_delete_relation(),
                &[can_select_relation(), can_delete_relation()],
                same,
            ),
        ];
        let store = Shapes::new::<Postgres>(db, &relations);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice")])
            .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        assert_eq!(diff.requeries.len(), 1);
    }

    /// One relation filled by a joining shape whose query binds `docs.id`,
    /// with `scope_relations` the relations the source declares it fills:
    /// its own for a source of one relation, and every one it feeds for a
    /// source shared across entries, exactly as rls2fga derives the scope
    /// from the source alone.
    fn joining(
        relation: RelationName,
        scope_relations: &[RelationName],
        sql: &str,
    ) -> RelationShapes {
        RelationShapes {
            type_name: test_names::docs_type(),
            relation,
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("docs")],
                derivation: RecordDerivation::Joined {
                    queries: vec![BoundQuery {
                        table: test_names::table("docs"),
                        key_columns: vec![test_names::column("id")],
                        sql: sql.to_string(),
                        condition: None,
                        scope: ReplayScope::Object {
                            object_type: test_names::docs_type().as_str().to_string(),
                            relations: scope_relations.to_vec(),
                        },
                    }],
                    reason: "two rows".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        }
    }

    /// A query bound to `key_columns` of `table`, spelled the way rls2fga
    /// spells one: an equality per column, numbered in that order.
    fn bound(table: &str, key_columns: &[&str]) -> BoundQuery {
        let predicate = key_columns
            .iter()
            .enumerate()
            .map(|(index, column)| alloc::format!("\"{column}\" = ${}", index + 1))
            .collect::<Vec<String>>()
            .join(" AND ");
        BoundQuery {
            table: test_names::table(table),
            key_columns: key_columns
                .iter()
                .copied()
                .map(test_names::column)
                .collect(),
            sql: alloc::format!("SELECT 1 FROM \"{table}\" WHERE {predicate};"),
            condition: None,
            scope: ReplayScope::Object {
                object_type: table.to_string(),
                relations: alloc::vec![can_select_relation()],
            },
        }
    }

    /// A store over one compound-key table, reached only by a bound query.
    ///
    /// Hand built, because every schema-derived joining shape binds a single
    /// column at the current pin: the compound binds belong to the grant
    /// shapes, whose recognition needs a function registry the translator
    /// here does not carry.
    fn compound_key_store() -> Shapes<ParserDB> {
        let db = ParserDB::parse::<PostgreSqlDialect>(COMPOUND_KEY).unwrap();
        let entry = RelationShapes {
            // The entry's own type name is irrelevant to the mechanics under
            // test, so the one the helpers already mint serves.
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("readings")],
                derivation: RecordDerivation::Joined {
                    queries: vec![bound("readings", &["tenant_id", "reading_id"])],
                    reason: "the guard is settled by the request".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        Shapes::new::<Postgres>(db, &[entry])
    }

    /// One `can_select` relation filled by a joining shape that reads `tables`
    /// and binds `queries` to them.
    fn joined(tables: &[&str], queries: Vec<BoundQuery>) -> RelationShapes {
        RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: tables.iter().copied().map(test_names::table).collect(),
                derivation: RecordDerivation::Joined {
                    queries,
                    reason: "a grant row and the resource row it names are separate".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        }
    }
}
