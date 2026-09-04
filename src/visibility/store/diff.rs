use alloc::collections::BTreeSet;
use alloc::string::ToString;
use alloc::vec::Vec;

use rls2fga_types::{
    Record, RecordDerivation, RecordDescription, RelationShapes, TableId as ContractTableId,
};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{CdcEvent, Value};
use crate::visibility::records::{records_from_row_view, RowRecordError};
use crate::visibility::shapes::{Shapes, TableShapes};
use crate::visibility::transition::is_key_only;
use crate::visibility::{EventRow, RowView};
use crate::{ColumnId, EventKind};

use super::{KeyedRequery, Requery, StoreDiff, StoreDiffError, Uncovered, UncoveredReason};

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

        // Only a differenced shape has to read the row, so a key-only previous
        // image is harmless for a table reached only by a query, whether that
        // is a bound one or a group's.
        let before = match previous.as_ref().filter(|_| !shapes.differenced.is_empty()) {
            Some(row) if is_key_only(row, event, db) => {
                return Err(StoreDiffError::IncompletePreviousImage)
            }
            Some(row) => records_of::<_, DB>(shapes, row, db)?,
            None => BTreeSet::new(),
        };
        let after = match current.as_ref() {
            Some(row) => records_of(shapes, row, db)?,
            None => BTreeSet::new(),
        };

        Ok(StoreDiff {
            added: after.difference(&before).cloned().collect(),
            removed: before.difference(&after).cloned().collect(),
            requeries: requeries(self, shapes, current.as_ref(), previous.as_ref())?,
        })
    }
}

/// One entry per group the change obliged, then one per query and per distinct
/// key the two images carry for it.
///
/// Both images are read for a keyed query because moving the bound key moves
/// records under the old value and the new one, and replaying only one of them
/// leaves the other stale. A group takes no key, so one entry covers the
/// change however many of its tables the event touched.
fn requeries<'a, R, DB>(
    index: &'a Shapes<DB>,
    shapes: &'a TableShapes,
    current: Option<&R>,
    previous: Option<&R>,
) -> Result<Vec<Requery<'a, R::Backend>>, StoreDiffError>
where
    R: RowView,
    DB: DatabaseLike,
{
    let mut out = Vec::new();
    for group in &shapes.groups {
        out.push(Requery::Whole(&index.materialisations()[*group]));
    }
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
            if out.iter().any(|seen| {
                matches!(seen, Requery::Keyed(seen) if seen.query == query && seen.key == key)
            }) {
                continue;
            }
            out.push(Requery::Keyed(KeyedRequery { query, key }));
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
pub fn name_gap(
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

/// Every record the shapes this differences state about `row`, deduplicated.
///
/// Reads [`TableShapes::differenced`] rather than every settled shape: a shape
/// a group maintains is still settled, because a row of the table still
/// implies its records, but differencing it beside the group would be a second
/// authoritative operation over one region.
fn records_of<R, DB>(
    shapes: &TableShapes,
    row: &R,
    db: &DB,
) -> Result<BTreeSet<Record>, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    let mut out = BTreeSet::new();
    for position in &shapes.differenced {
        out.extend(records_from_row_view::<R, DB>(
            &shapes.settled[*position],
            row,
            db,
        )?);
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::vec;
    use rls2fga_types::{BoundQuery, Record, RecordDerivation, RecordDescription, ReplayScope};

    use rls2fga::generator::well_known::{
        can_delete_relation, can_select_relation, member_relation,
    };
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga_types::ConfidenceLevel;
    use rls2fga_types::RecordError;
    use rls2fga_types::RelationName;
    use rls2fga_types::RelationShapes;
    use sqlparser::dialect::PostgreSqlDialect;

    use crate::backend::{CdcEvent, Postgres, RowKind, Value};
    use crate::testing::TestEvent;
    use crate::visibility::records::RowRecordError;
    use crate::visibility::shapes::Shapes;
    use crate::visibility::store::{Enumeration, Requery, StoreDiffError, UncoveredReason};
    use crate::visibility::test_names;
    use crate::{
        catalog_helpers, ColumnId, EventKind, NoCheckpoint, ParserDB, TableId, ValueError,
    };
    use sql_traits::prelude::DatabaseLike;

    // Schemas, translated by the real producer

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

    /// A membership whose residual compares one row against an aggregate over
    /// the whole table, so no key narrows the replay and the shape carries the
    /// unnarrowed query instead.
    const WHOLE_SHAPE: &str = "
CREATE TABLE public.papers(id INTEGER PRIMARY KEY, owner TEXT);
CREATE TABLE public.paper_shares(paper_id INTEGER REFERENCES papers(id), viewer TEXT,
    weight NUMERIC, PRIMARY KEY(paper_id, viewer));
ALTER TABLE papers ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON papers FOR SELECT USING (
  EXISTS (SELECT 1 FROM paper_shares s
          WHERE s.paper_id = papers.id AND s.viewer = current_user
            AND s.weight > (SELECT avg(weight) FROM paper_shares)));
";

    /// Two membership sources a row settles on its own, on different tables.
    /// Differencing one of them cannot see the other, so a fact both state is
    /// deleted when either row goes.
    const TWO_SETTLED: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE public.team_leads(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user)
  OR EXISTS (SELECT 1 FROM team_leads
          WHERE team_leads.team_id = docs.team_id AND team_leads.user_id = current_user));
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
        let outputs = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .outputs_accepting_gaps();
        // A skipped query carries no description, so `filter_map` drops exactly
        // the entries that enumerate nothing.
        let enumerations: Vec<Enumeration<'_>> = outputs
            .tuple_queries()
            .iter()
            .filter_map(|query| {
                query.description.as_ref().map(|description| Enumeration {
                    description,
                    sql: &query.sql,
                    condition: query.condition.as_deref(),
                })
            })
            .collect();
        Shapes::new::<Postgres>(db, outputs.translation().relations(), &enumerations)
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

    // The difference

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

    // The index is wider than the answering one

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

    // Handing the rest over

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
        let Requery::Keyed(keyed) = &diff.requeries[0] else {
            panic!("expected Keyed requery, got {:?}", diff.requeries[0]);
        };
        assert_eq!(keyed.query.table().name(), "team_members");
        assert_eq!(keyed.query.key_columns(), ["team_id"]);
        assert!(keyed.query.sql().contains("$1"));
        assert_eq!(keyed.key, [Value::Int(3)]);
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

        let keys: Vec<_> = diff
            .requeries
            .iter()
            .filter_map(|r| match r {
                Requery::Keyed(keyed) => Some(keyed.key.clone()),
                Requery::Whole(_) => None,
            })
            .collect();
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
        let Requery::Keyed(keyed) = &diff.requeries[0] else {
            panic!("expected Keyed requery");
        };
        assert_eq!(keyed.key, [Value::Int(3)]);
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
        let Requery::Keyed(keyed) = &diff.requeries[0] else {
            panic!("expected Keyed requery");
        };
        assert_eq!(keyed.key, [Value::Int(3)]);
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
        let Requery::Keyed(keyed) = &diff.requeries[0] else {
            panic!("expected Keyed requery");
        };
        assert_eq!(keyed.key, [Value::Int(3)]);
        assert_eq!(
            *keyed.query.scope(),
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
        let Requery::Keyed(keyed) = &diff.requeries[0] else {
            panic!("expected Keyed requery");
        };
        assert_eq!(keyed.query.key_columns(), ["tenant_id", "reading_id"]);
        assert_eq!(keyed.key, [Value::Int(7), Value::Int(9)]);
        assert!(
            keyed
                .query
                .sql()
                .contains("\"tenant_id\" = $1 AND \"reading_id\" = $2"),
            "{}",
            keyed.query.sql()
        );
    }

    // Naming what is not covered

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

    // Refusing rather than emitting a partial difference

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
            &[],
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

        let [Requery::Keyed(keyed)] = diff.requeries.as_slice() else {
            panic!("only the resolvable query: {:?}", diff.requeries);
        };
        assert_eq!(keyed.query.key_columns(), ["id"]);
    }

    /// A key naming a column this catalog does not have is dropped whole.
    /// Keeping the columns that did resolve would answer for every row sharing
    /// them, and the SQL takes one placeholder per column so it could not be
    /// run anyway. The table is named instead of being left silently unserved,
    /// which is the difference between a gap a caller can see and one it
    /// cannot. A key with no columns at all cannot arrive: `BoundQuery` refuses
    /// one at construction.
    #[test]
    fn a_key_this_catalog_cannot_bind_drops_the_query_and_names_the_table() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);",
        )
        .unwrap();
        let store = Shapes::new::<Postgres>(
            db,
            &[joined(
                &["docs"],
                vec![bound("docs", &["id", "absent_column"])],
            )],
            &[],
        );

        let uncovered = store.uncovered();
        assert_eq!(uncovered.len(), 1, "{uncovered:?}");
        assert_eq!(uncovered[0].table, "docs");
        assert_eq!(uncovered[0].reason, UncoveredReason::NoBoundQuery);

        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice")])
            .with_pk_columns([0u16]);
        let diff = store.diff(&event).unwrap();
        assert!(diff.requeries.is_empty(), "{:?}", diff.requeries);
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
        let store = Shapes::new::<Postgres>(db, &relations, &[]);
        let docs = table(&store, "docs");
        let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice")])
            .with_pk_columns([0u16]);

        let diff = store.diff(&event).unwrap();

        let mut sql: Vec<&str> = diff
            .requeries
            .iter()
            .filter_map(|r| match r {
                Requery::Keyed(keyed) => Some(keyed.query.sql()),
                Requery::Whole(_) => None,
            })
            .collect();
        sql.sort_unstable();
        assert_eq!(
            sql,
            [
                "SELECT 'delete' WHERE id = $1;",
                "SELECT 'read' WHERE id = $1;"
            ]
        );
        assert!(diff.requeries.iter().all(|r| match r {
            Requery::Keyed(keyed) => keyed.key == [Value::Int(4)],
            Requery::Whole(_) => false,
        }));
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
        let store = Shapes::new::<Postgres>(db, &relations, &[]);
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
                    queries: vec![BoundQuery::new(
                        test_names::table("docs"),
                        vec![test_names::column("id")],
                        sql.to_string(),
                        None,
                        ReplayScope::Object {
                            object_type: test_names::docs_type().as_str().to_string(),
                            relations: scope_relations.to_vec(),
                        },
                    )
                    .expect("the fixture query binds its key")],
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
        BoundQuery::new(
            test_names::table(table),
            key_columns
                .iter()
                .copied()
                .map(test_names::column)
                .collect(),
            alloc::format!("SELECT 1 FROM \"{table}\" WHERE {predicate};"),
            None,
            ReplayScope::Object {
                object_type: table.to_string(),
                relations: alloc::vec![can_select_relation()],
            },
        )
        .expect("the fixture query binds its key")
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
        Shapes::new::<Postgres>(db, &[entry], &[])
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

    /// A whole shape carries the unnarrowed query and declares the slice it
    /// determines, so naming it as carrying no query to fall back on was
    /// wrong. It is indexed against every table it reads.
    #[test]
    fn a_whole_shape_is_indexed_rather_than_uncovered() {
        let store = shapes(WHOLE_SHAPE);
        // The premise, asserted rather than assumed: a translation that stopped
        // emitting this derivation would otherwise make the claim below vacuous.
        assert!(
            store.materialisations().iter().any(|group| {
                group
                    .region()
                    .parts()
                    .iter()
                    .any(|part| part.object_type() == "papers")
            }),
            "the aggregate residual groups on the type it grants: {:?}",
            store.materialisations()
        );
        assert!(
            store.uncovered().is_empty(),
            "the aggregate residual carries its own query: {:?}",
            store.uncovered()
        );
    }

    /// A change to a table a whole shape reads schedules the whole group,
    /// because no key narrows what that change moved.
    #[test]
    fn an_event_on_a_trigger_table_schedules_the_whole_group() {
        let store = shapes(WHOLE_SHAPE);
        let shares = table(&store, "paper_shares");

        let event =
            TestEvent::<Postgres>::insert(shares, vec![Value::Int(7), text("alice"), text("5")])
                .with_pk_columns([0u16, 1u16]);
        let diff = store.diff(&event).unwrap();

        let [Requery::Whole(group)] = diff.requeries.as_slice() else {
            panic!("one group, and no keyed replay: {:?}", diff.requeries);
        };
        let [part] = group.region().parts() else {
            panic!("one relation on one type: {group:?}");
        };
        assert_eq!(part.object_type(), "papers");
        assert_eq!(part.relation(), &member_relation());
        assert_eq!(
            part.subject_type(),
            None,
            "an object-scoped member is authoritative over every subject"
        );
        let [member] = group.members() else {
            panic!("one producer states this region: {group:?}");
        };
        assert!(
            member.sql().contains("avg(weight)"),
            "the residual travels in the query rather than being dropped: {}",
            member.sql()
        );
    }

    /// Two producers stating one region form one group, where the old code
    /// refused the pair outright. That their facts survive the reconcile is a
    /// property of the reconcile, proven against a real store in
    /// `tests/it/visibility_openfga_e2e.rs`.
    #[test]
    fn a_group_of_two_producers_is_formed_over_the_shared_region() {
        let store = shapes(SHARED_SLICE);
        assert!(
            store.uncovered().is_empty(),
            "both producers enumerate, so the region is reconcilable: {:?}",
            store.uncovered()
        );

        let guests = table(&store, "team_guests");
        let event = TestEvent::<Postgres>::delete(
            guests,
            vec![Value::Int(3), text("alice"), text("2027-01-01T00:00:00Z")],
        );
        let diff = store.diff(&event).unwrap();

        let [Requery::Whole(group)] = diff.requeries.as_slice() else {
            panic!("one group over the shared region: {:?}", diff.requeries);
        };
        assert_eq!(
            group.members().len(),
            2,
            "the settled member and the replayed one both state this region: {group:?}"
        );
        assert!(
            group
                .members()
                .iter()
                .any(|member| member.sql().contains("team_members")),
            "the sibling's own facts are in the union: {group:?}"
        );
        assert!(
            group
                .members()
                .iter()
                .any(|member| member.sql().contains("team_guests")),
            "and so are the changed table's: {group:?}"
        );
        assert!(
            diff.removed.is_empty(),
            "a grouped producer is not differenced as well, since the group is \
             the one authoritative operation over its region: {diff:?}"
        );
    }

    /// Two row-settled producers on different tables state one region, which
    /// differencing alone gets wrong: deleting a row on one table removes a
    /// fact the other table's producer still states.
    #[test]
    fn two_settled_producers_on_one_region_are_grouped() {
        let store = shapes(TWO_SETTLED);
        assert!(store.uncovered().is_empty(), "{:?}", store.uncovered());

        let leads = table(&store, "team_leads");
        let event = TestEvent::<Postgres>::delete(leads, vec![Value::Int(3), text("alice")]);
        let diff = store.diff(&event).unwrap();

        let [Requery::Whole(group)] = diff.requeries.as_slice() else {
            panic!("one group over the shared region: {:?}", diff.requeries);
        };
        assert_eq!(group.members().len(), 2, "{group:?}");
        assert!(
            diff.removed.is_empty(),
            "the fact the lead row stated may still be stated by a member row, \
             so only the group's union may remove it: {diff:?}"
        );
    }

    /// A whole shape reaches the subject scope too, and a subject-scoped
    /// region is confined to one subject type: a fact granting to another
    /// subject type is outside it and must not be deleted by its reconcile.
    #[test]
    fn a_subject_scoped_whole_shape_confines_its_region_to_one_subject_type() {
        let db = ParserDB::parse::<PostgreSqlDialect>(MEMBERSHIP).unwrap();
        let entry = RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("team_members")],
                derivation: RecordDerivation::WholeShape {
                    query: "SELECT object, relation, subject FROM held;".to_string(),
                    condition: None,
                    scope: ReplayScope::Subject {
                        subject_type: "user".to_string(),
                        relation: can_select_relation(),
                        object_type: "docs".to_string(),
                    },
                    reason: "the holder is decided by rows no key names".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        let store = Shapes::new::<Postgres>(db, &[entry], &[]);

        let [group] = store.materialisations() else {
            panic!("one group: {:?}", store.materialisations());
        };
        let [part] = group.region().parts() else {
            panic!("one relation on one type: {group:?}");
        };
        assert_eq!(part.object_type(), "docs");
        assert_eq!(
            part.subject_type(),
            Some("user"),
            "a subject-scoped member is authoritative over one subject type only"
        );
        assert!(
            group
                .region()
                .holds("docs:4", can_select_relation().as_str(), "user:alice"),
            "a fact granting to that subject type is inside the region"
        );
        assert!(
            !group
                .region()
                .holds("docs:4", can_select_relation().as_str(), "team:ops"),
            "and one granting to another subject type is not, so the reconcile \
             leaves it alone"
        );
    }

    /// Two producers on one type and one relation whose subject types differ
    /// state disjoint facts, so they are two regions rather than one.
    ///
    /// Grouping them would hand each the authority to delete the other's
    /// facts, which is the clobber the region arithmetic exists to refuse. It
    /// is also why a row-settled producer's region keeps its template's
    /// subject type instead of spanning every subject.
    #[test]
    fn producers_differing_only_in_subject_type_are_not_grouped() {
        let db = ParserDB::parse::<PostgreSqlDialect>(MEMBERSHIP).unwrap();
        let held = |subject_type: &str| RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("team_members")],
                derivation: RecordDerivation::WholeShape {
                    query: alloc::format!("SELECT object, relation, subject FROM {subject_type};"),
                    condition: None,
                    scope: ReplayScope::Subject {
                        subject_type: subject_type.to_string(),
                        relation: can_select_relation(),
                        object_type: "docs".to_string(),
                    },
                    reason: "the holder is decided by rows no key names".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        let store = Shapes::new::<Postgres>(db, &[held("user"), held("team")], &[]);

        assert_eq!(
            store.materialisations().len(),
            2,
            "disjoint subject types are disjoint regions: {:?}",
            store.materialisations()
        );
        for group in store.materialisations() {
            assert_eq!(
                group.members().len(),
                1,
                "neither group may delete over the other's subjects: {group:?}"
            );
        }
    }

    /// One source feeding two relations is reported under both, and is still
    /// one producer.
    ///
    /// Counted twice its region would look shared and a group would form,
    /// which costs a full enumeration and a whole-store pass per event where
    /// differencing the row was correct and free.
    #[test]
    fn one_source_reported_under_two_relations_is_one_producer() {
        let db = ParserDB::parse::<PostgreSqlDialect>(OWNERSHIP).unwrap();
        let outputs = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .outputs_accepting_gaps();
        // The real description, reported a second time under another relation,
        // which is what one source feeding two relations looks like.
        let mut relations = outputs.translation().relations().to_vec();
        let mut again = relations
            .iter()
            .find(|entry| !entry.shapes.is_empty())
            .expect("the ownership policy describes records")
            .clone();
        again.relation = can_delete_relation();
        relations.push(again);
        let store = Shapes::new::<Postgres>(db, &relations, &[]);

        assert!(
            store.materialisations().is_empty(),
            "one producer alone in its region needs no group: {:?}",
            store.materialisations()
        );
        let docs = table(&store, "docs");
        let event =
            TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), text("alice"), text("b")])
                .with_pk_columns([0u16]);
        let diff = store.diff(&event).unwrap();
        assert_eq!(
            diff.added,
            [record(
                "docs:4",
                test_names::relation("owner"),
                "user:alice"
            )],
            "and it is differenced from the row, once: {diff:?}"
        );
    }

    /// A constant fact inside a group's region joins the union, so the
    /// reconcile does not delete what the load wrote and no query returns.
    #[test]
    fn a_constant_inside_a_region_is_donated_to_its_group() {
        let db = ParserDB::parse::<PostgreSqlDialect>(MEMBERSHIP).unwrap();
        let replayed = RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("team_members")],
                derivation: RecordDerivation::WholeShape {
                    query: "SELECT object, relation, subject FROM held;".to_string(),
                    condition: None,
                    scope: ReplayScope::Object {
                        object_type: "docs".to_string(),
                        relations: vec![can_select_relation()],
                    },
                    reason: "the holder is decided by rows no key names".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        let granted = record("docs:4", can_select_relation(), "user:root");
        let constant = RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: Vec::new(),
                derivation: RecordDerivation::Constant {
                    record: granted.clone(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        let store = Shapes::new::<Postgres>(db, &[replayed, constant], &[]);

        let [group] = store.materialisations() else {
            panic!("one group: {:?}", store.materialisations());
        };
        assert_eq!(
            group.constants(),
            [granted],
            "the constant lies in the region, so the union has to state it"
        );
    }

    /// A constant beside a row-settled producer in one region pulls it into a
    /// group rather than leaving it differenced.
    ///
    /// Differencing reports what that producer stopped stating, so deleting
    /// its row removes a fact the constant still states, and the load would
    /// put it back only on the next restart. The constant carries no query, so
    /// it donates its fact to the union rather than refusing the region.
    #[test]
    fn a_constant_pulls_a_settled_producer_into_a_group() {
        let db = ParserDB::parse::<PostgreSqlDialect>(OWNERSHIP).unwrap();
        let outputs = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .outputs_accepting_gaps();
        let enumerations: Vec<Enumeration<'_>> = outputs
            .tuple_queries()
            .iter()
            .filter_map(|query| {
                query.description.as_ref().map(|description| Enumeration {
                    description,
                    sql: &query.sql,
                    condition: query.condition.as_deref(),
                })
            })
            .collect();
        let mut relations = outputs.translation().relations().to_vec();
        // A fact the load writes outright, in the very region the ownership
        // producer states its own facts in.
        let granted = record("docs:9", test_names::relation("owner"), "user:root");
        relations.push(RelationShapes {
            type_name: test_names::docs_type(),
            relation: test_names::relation("owner"),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: Vec::new(),
                derivation: RecordDerivation::Constant {
                    record: granted.clone(),
                },
            }],
            decision: None,
            grants_nobody: false,
        });
        let store = Shapes::new::<Postgres>(db, &relations, &enumerations);

        assert!(store.uncovered().is_empty(), "{:?}", store.uncovered());
        let [group] = store.materialisations() else {
            panic!("one group: {:?}", store.materialisations());
        };
        assert_eq!(group.constants(), [granted]);
        assert_eq!(
            group.members().len(),
            1,
            "the settled producer enumerates, the constant needs no query: {group:?}"
        );

        let docs = table(&store, "docs");
        let event =
            TestEvent::<Postgres>::delete(docs, vec![Value::Int(4), text("alice"), text("b")]);
        let diff = store.diff(&event).unwrap();
        let [Requery::Whole(scheduled)] = diff.requeries.as_slice() else {
            panic!("the change schedules the group: {diff:?}");
        };
        assert_eq!(scheduled.region(), group.region());
        assert!(
            diff.removed.is_empty(),
            "only the group's union may remove here: {diff:?}"
        );
    }

    /// A producer whose facts nothing can place refuses every group.
    ///
    /// A joining shape carrying no query at all states facts and the load
    /// writes them, but its scope lives inside the query it does not have, so
    /// it can neither be gathered into the group whose region covers it nor be
    /// shown to lie outside it. Forming the group anyway would delete those
    /// facts, which is what reconciling nothing never did.
    #[test]
    fn a_producer_nothing_can_place_refuses_every_group() {
        let db = ParserDB::parse::<PostgreSqlDialect>(MEMBERSHIP).unwrap();
        let unbound = RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("team_members")],
                derivation: RecordDerivation::Joined {
                    queries: Vec::new(),
                    reason: "nothing could be bound".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        let replayed = RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("team_members")],
                derivation: RecordDerivation::WholeShape {
                    query: "SELECT object, relation, subject FROM held;".to_string(),
                    condition: None,
                    scope: ReplayScope::Object {
                        object_type: "docs".to_string(),
                        relations: vec![can_select_relation()],
                    },
                    reason: "rows no key names".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        };
        let store = Shapes::new::<Postgres>(db, &[unbound, replayed], &[]);

        assert!(
            store.materialisations().is_empty(),
            "the whole shape's region covers facts the unbound producer states, \
             so nothing may delete over it: {:?}",
            store.materialisations()
        );
        let reasons: Vec<UncoveredReason> =
            store.uncovered().iter().map(|gap| gap.reason).collect();
        assert!(
            reasons.contains(&UncoveredReason::NoBoundQuery),
            "the producer that caused it is named: {:?}",
            store.uncovered()
        );
        assert!(
            reasons.contains(&UncoveredReason::MissingEnumeration)
                && !reasons.contains(&UncoveredReason::UnknownDerivation),
            "and the whole shape is reported as a region nothing could reconcile, \
             not as a derivation this does not understand: {:?}",
            store.uncovered()
        );
    }

    /// While a producer nothing can place exists, a row-settled producer stops
    /// differencing too, because a difference deletes.
    ///
    /// Refusing only the groups would leave the settled producer maintaining
    /// itself, and deleting its row then removes a fact the unplaceable
    /// producer may still state. Nothing may delete until every producer's
    /// facts can be placed.
    #[test]
    fn an_unplaceable_producer_stops_the_differencing_beside_it() {
        let db = ParserDB::parse::<PostgreSqlDialect>(OWNERSHIP).unwrap();
        let outputs = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .outputs_accepting_gaps();
        let mut relations = outputs.translation().relations().to_vec();
        relations.push(RelationShapes {
            type_name: test_names::docs_type(),
            relation: test_names::relation("owner"),
            from_one_row: false,
            shapes: vec![RecordDescription {
                tables: vec![test_names::table("docs")],
                derivation: RecordDerivation::Joined {
                    queries: Vec::new(),
                    reason: "nothing could be bound".to_string(),
                },
            }],
            decision: None,
            grants_nobody: false,
        });
        let store = Shapes::new::<Postgres>(db, &relations, &[]);

        let docs = table(&store, "docs");
        let event =
            TestEvent::<Postgres>::delete(docs, vec![Value::Int(4), text("alice"), text("b")]);
        let diff = store.diff(&event).unwrap();

        assert!(
            diff.removed.is_empty(),
            "the fact the row stated may be stated by the producer nothing can \
             place, so nothing may remove it: {diff:?}"
        );
        assert!(
            diff.added.is_empty() && diff.requeries.is_empty(),
            "and nothing is maintained at all: {diff:?}"
        );
        assert!(
            store
                .uncovered()
                .iter()
                .any(|gap| gap.reason == UncoveredReason::MissingEnumeration),
            "the settled producer's refusal is reported: {:?}",
            store.uncovered()
        );
        let docs_table = table(&store, "docs");
        assert!(
            store
                .table_records(docs_table)
                .is_some_and(|shapes| !shapes.is_empty()),
            "but what a row of the table implies is a different question, and a \
             write's resulting-row check reads it, so refusing the maintenance \
             must not empty it"
        );
    }

    /// Two enumerations for one producer that agree on the SQL but differ on
    /// the condition are still an ambiguity.
    ///
    /// The condition decides whether the replayed rows carry one and which,
    /// so taking either would grant on terms the other did not state. The
    /// region is refused instead, which is what a caller can see.
    #[test]
    fn enumerations_disagreeing_only_on_the_condition_refuse_the_region() {
        let db = ParserDB::parse::<PostgreSqlDialect>(MEMBERSHIP).unwrap();
        let shape = RecordDescription {
            tables: vec![test_names::table("team_members")],
            derivation: RecordDerivation::WholeShape {
                query: "SELECT object, relation, subject FROM held;".to_string(),
                condition: None,
                scope: ReplayScope::Object {
                    object_type: "docs".to_string(),
                    relations: vec![can_select_relation()],
                },
                reason: "rows no key names".to_string(),
            },
        };
        // A second producer on the same region, so the group needs both and
        // this one is reached through the enumerations rather than its own
        // query.
        let settled = RecordDescription {
            tables: vec![test_names::table("team_members")],
            derivation: RecordDerivation::Joined {
                queries: vec![BoundQuery::new(
                    test_names::table("team_members"),
                    vec![test_names::column("team_id")],
                    "SELECT 1 WHERE team_id = $1;".to_string(),
                    None,
                    ReplayScope::Object {
                        object_type: "docs".to_string(),
                        relations: vec![can_select_relation()],
                    },
                )
                .unwrap()],
                reason: "two rows".to_string(),
            },
        };
        let entry = |shape: RecordDescription| RelationShapes {
            type_name: test_names::docs_type(),
            relation: can_select_relation(),
            from_one_row: false,
            shapes: vec![shape],
            decision: None,
            grants_nobody: false,
        };
        let same = "SELECT object, relation, subject FROM grants;";
        let store = Shapes::new::<Postgres>(
            db,
            &[entry(shape), entry(settled.clone())],
            &[
                Enumeration {
                    description: &settled,
                    sql: same,
                    condition: None,
                },
                Enumeration {
                    description: &settled,
                    sql: same,
                    condition: Some("when_expires_at"),
                },
            ],
        );

        assert!(
            store.materialisations().is_empty(),
            "the condition is not this index's to choose: {:?}",
            store.materialisations()
        );
        assert!(
            store
                .uncovered()
                .iter()
                .any(|gap| gap.reason == UncoveredReason::MissingEnumeration),
            "and the region is reported rather than reconciled: {:?}",
            store.uncovered()
        );
    }
}
