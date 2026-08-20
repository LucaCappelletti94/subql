//! Which authorization records one changed row implies.
//!
//! [`rls2fga`] describes a relation's records as structure: a template
//! naming where the object key and the subject key come from, plus guards
//! the row must satisfy. Given a row's column values it evaluates that
//! description with no database. This module is the adapter that lets a
//! subql [`RowView`] be those column values.
//!
//! # Refusing is part of the contract
//!
//! [`rls2fga::generator::records::RowValues`] answers `None` for anything
//! it cannot read, and `records_from_row` turns a `None` object key into
//! an empty record set. That is correct when the row genuinely says
//! nothing, and wrong when this adapter simply cannot read the shape,
//! because "no records" reads as "this row grants nobody" and silently
//! withdraws access.
//!
//! So the adapter never guesses. [`records_from_row_view`] checks the
//! description against what a [`RowView`] can answer and returns
//! [`RowRecordError::UnsupportedValueSource`] rather than an empty set.

use alloc::borrow::Cow;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::Cell;

use rls2fga::classifier::patterns::{AttributeLiteral, AttributePredicate};
use rls2fga::generator::records::{
    records_from_row, Guard, Record, RecordDerivation, RecordDescription, RecordError, RowValues,
    ValueSource,
};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{ScalarKind, Value};
use crate::catalog_helpers;
use crate::visibility::RowView;
use crate::{TableId, ValueError};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a description could not be evaluated against one row view.
///
/// Every variant means "ask somebody else", never "this row grants
/// nobody". Collapsing any of them into an empty record set is a silent
/// withdrawal of access.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RowRecordError {
    /// [`rls2fga`] refused to produce records for this row.
    ///
    /// Wrapped whole rather than mapped arm by arm, because
    /// [`RecordError`] is `#[non_exhaustive]` and every arm of it is a
    /// refusal. A mapping would have to grow with each new reason, and
    /// the reason is the producer's to word.
    #[error(transparent)]
    Refused(#[from] RecordError),
    /// The description reads a column shape a [`RowView`] cannot answer.
    ///
    /// Today that is only a list column: [`Value`] has no array variant,
    /// so an `= ANY(members)` shape cannot be expanded from a row image.
    #[error("a row view cannot answer a {0} column")]
    UnsupportedValueSource(&'static str),
    /// A cell the row carried could not be decoded, so what the row
    /// implies is not knowable.
    ///
    /// Distinct from a cell the source never carried, which is
    /// [`Value::Missing`] and genuinely says nothing. Reading a corrupt
    /// cell as no records would withdraw access the row may still grant.
    #[error(transparent)]
    Undecodable(#[from] ValueError),
    /// The description reads a column the row side cannot answer the way
    /// the loading SQL does.
    ///
    /// The loading SQL spells every kind through `::text`, while the row
    /// side spells only the kinds whose text form provably matches it, so
    /// serving such a shape would load records no changed row could ever
    /// produce or withdraw. The kind is the catalog's, not any row's, so
    /// this is refused once at setup through [`is_evaluable`] rather than
    /// rediscovered per row.
    #[error("a row view cannot read {0}")]
    UnreadableColumn(String),
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// The records `description` says `row` implies.
///
/// # Errors
///
/// [`RowRecordError::Refused`] for a description rls2fga will not evaluate,
/// [`RowRecordError::UnsupportedValueSource`] for a shape whose columns a
/// row view cannot read, and [`RowRecordError::Undecodable`] for a cell
/// the row carried but could not decode. None is an empty record set, on
/// purpose.
pub fn records_from_row_view<R, DB>(
    description: &RecordDescription,
    row: &R,
    db: &DB,
) -> Result<Vec<Record>, RowRecordError>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    if let Some(refusal) = unsupported_description(description, db) {
        return Err(refusal);
    }
    let view = RowValuesView {
        row,
        db,
        undecodable: Cell::new(None),
    };
    let records = records_from_row(description, &view)?;
    if let Some(error) = view.undecodable.into_inner() {
        return Err(RowRecordError::Undecodable(error));
    }
    Ok(records)
}

/// Whether [`records_from_row_view`] can evaluate `description` at all.
///
/// Depends only on the description's shape and the catalog, never on a
/// row, so a caller holding the descriptions for a whole schema settles
/// this once at setup rather than rediscovering the refusal on every
/// changed row.
#[must_use]
pub fn is_evaluable<DB: DatabaseLike>(description: &RecordDescription, db: &DB) -> bool {
    unsupported_description(description, db).is_none()
}

/// Why a row view cannot answer `description`, or [`None`].
fn unsupported_description<DB: DatabaseLike>(
    description: &RecordDescription,
    db: &DB,
) -> Option<RowRecordError> {
    let RecordDerivation::FromRow {
        table,
        template,
        guards,
        ..
    } = &description.derivation
    else {
        // A joining description is refused by `records_from_row` itself,
        // with the reason it recorded, which is better than one invented
        // here.
        return None;
    };
    let Some(table) = catalog_helpers::table_id(db, table) else {
        return Some(RowRecordError::UnreadableColumn(alloc::format!(
            "any column of {table:?}, a table the catalog does not know"
        )));
    };
    // An object name is built from every part in order, so one part the
    // view cannot read loses the whole name rather than a piece of it.
    template
        .object_key
        .parts()
        .iter()
        .find_map(|part| unsupported(part, db, table))
        .or_else(|| unsupported(template.subject_key.part(), db, table))
        .or_else(|| {
            guards
                .iter()
                .find_map(|guard| unsupported_guard(guard, db, table))
        })
        .or_else(|| {
            template.context.as_ref().and_then(|context| {
                context
                    .entries
                    .iter()
                    .find_map(|entry| unsupported(&entry.value, db, table))
            })
        })
}

/// The refusal for a value source a row view cannot answer, or [`None`].
fn unsupported<DB: DatabaseLike>(
    source: &ValueSource,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match source {
        ValueSource::Column(column) => text_read(db, table, column.as_str()),
        ValueSource::JsonPath { column, .. } => document_read(db, table, column.as_str()),
        ValueSource::Literal(_) => None,
        // `Value` has no array variant, so there is nothing to expand.
        ValueSource::ListElements(_) => Some(RowRecordError::UnsupportedValueSource("list")),
        // `ValueSource` is `#[non_exhaustive]`: an unrecognised shape is
        // refused rather than read as absent.
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised")),
    }
}

/// The refusal for a guard a row view cannot answer, or [`None`].
///
/// Each arm names the read `guard_holds` performs: `NotNull` and the
/// textual comparisons read the cell as text, `IsTrue` and the boolean
/// comparison read it as a boolean.
fn unsupported_guard<DB: DatabaseLike>(
    guard: &Guard,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match guard {
        Guard::NotNull(column) => text_read(db, table, column.as_str()),
        Guard::IsTrue(column) => boolean_read(db, table, column.as_str()),
        Guard::Compare(predicate) => unsupported_comparison(predicate, db, table),
        // `Guard` is `#[non_exhaustive]`: an unrecognised guard is refused
        // rather than read as absent.
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised guard")),
    }
}

/// The refusal for a comparison guard, or [`None`].
fn unsupported_comparison<DB: DatabaseLike>(
    predicate: &AttributePredicate,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let column = predicate.column.as_str();
    match &predicate.value {
        AttributeLiteral::Boolean(_) => boolean_read(db, table, column),
        AttributeLiteral::Number(_) | AttributeLiteral::Text(_) => text_read(db, table, column),
        // `AttributeLiteral` is `#[non_exhaustive]`: a literal this does
        // not recognise is refused rather than compared as text.
        _ => Some(RowRecordError::UnsupportedValueSource(
            "unrecognised comparison",
        )),
    }
}

/// The kinds [`render_text`] spells.
///
/// Kept in lockstep with it by the exhaustive kind test below: a kind
/// spelled there but refused here loses service for no reason, and one
/// admitted here but unspelled there reopens the silent no-record path
/// this gate exists to close.
const fn spells_as_text(kind: ScalarKind) -> bool {
    matches!(
        kind,
        ScalarKind::String
            | ScalarKind::Int
            | ScalarKind::Uuid
            | ScalarKind::Bool
            | ScalarKind::Timestamp
            | ScalarKind::TimestampTz
    )
}

/// The refusal for a column read as text, or [`None`] when its kind spells.
fn text_read<DB: DatabaseLike>(db: &DB, table: TableId, column: &str) -> Option<RowRecordError> {
    match column_kind(db, table, column) {
        Err(refusal) => Some(refusal),
        Ok(kind) if spells_as_text(kind) => None,
        Ok(kind) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} as text: it holds a {kind:?}, whose row-side spelling is not known \
             to match the loading SQL's"
        ))),
    }
}

/// The refusal for a column read as a boolean, or [`None`].
fn boolean_read<DB: DatabaseLike>(db: &DB, table: TableId, column: &str) -> Option<RowRecordError> {
    match column_kind(db, table, column) {
        Err(refusal) => Some(refusal),
        Ok(ScalarKind::Bool) => None,
        Ok(kind) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} as a boolean: it holds a {kind:?}"
        ))),
    }
}

/// The refusal for a column read as a JSON document, or [`None`].
fn document_read<DB: DatabaseLike>(
    db: &DB,
    table: TableId,
    column: &str,
) -> Option<RowRecordError> {
    match column_kind(db, table, column) {
        Err(refusal) => Some(refusal),
        Ok(ScalarKind::Json | ScalarKind::Jsonb) => None,
        Ok(kind) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} as a JSON document: it holds a {kind:?}"
        ))),
    }
}

/// A column's declared kind, or the refusal for one the catalog cannot name.
fn column_kind<DB: DatabaseLike>(
    db: &DB,
    table: TableId,
    column: &str,
) -> Result<ScalarKind, RowRecordError> {
    let refuse = || {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column}, which the catalog does not know or cannot type"
        ))
    };
    let id = catalog_helpers::column_id(db, table, column).ok_or_else(refuse)?;
    catalog_helpers::column_scalar_kind(db, table, id).ok_or_else(refuse)
}

// ---------------------------------------------------------------------------
// The adapter
// ---------------------------------------------------------------------------

/// A [`RowView`] seen as one row's column values.
///
/// Resolves a column name to its ordinal through the catalog on every
/// read, which is a linear scan of the table's columns. Descriptions name
/// one or two columns, so the scan is bounded by the table's arity and
/// happens twice per changed row.
/// See `row` as one row's column values, for rls2fga's own readers.
///
/// [`ObjectKey::render`](rls2fga::generator::records::ObjectKey::render) wants
/// this, and naming a row is the one thing a caller needs it for that computing
/// records does not already cover. Sharing the reader is the point: an object
/// name spelled by a second reader would drift from the one the records carry.
pub fn row_values<'a, R, DB>(row: &'a R, db: &'a DB) -> impl RowValues + 'a
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    RowValuesView {
        row,
        db,
        undecodable: Cell::new(None),
    }
}

struct RowValuesView<'a, R: ?Sized, DB> {
    row: &'a R,
    db: &'a DB,
    /// The first cell that failed to decode, which
    /// [`records_from_row_view`] turns into a refusal.
    ///
    /// [`RowValues`] answers `Option`, so a reader here cannot report a
    /// failure inline, and reporting none would make a corrupt cell
    /// indistinguishable from a row that grants nobody.
    undecodable: Cell<Option<ValueError>>,
}

impl<R, DB> RowValuesView<'_, R, DB>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    /// The cell `column` names, or [`None`] when the column is unknown to
    /// the catalog or the cell could not be decoded.
    ///
    /// [`Value::Missing`] and [`Value::Null`] are returned as they are.
    /// Every reader below matches a specific variant, so an absent cell
    /// already falls through to "the row does not say", and filtering it
    /// here would be a guard that can never change an answer.
    fn cell(&self, column: &str) -> Option<Value<R::Backend>> {
        let id = catalog_helpers::column_id(self.db, self.row.table_id(), column)?;
        match self.row.value_at(id) {
            Ok(value) => Some(value),
            Err(error) => {
                let seen = self.undecodable.take();
                self.undecodable.set(seen.or(Some(error)));
                None
            }
        }
    }
}

impl<R, DB> RowValues for RowValuesView<'_, R, DB>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    fn text(&self, column: &str) -> Option<Cow<'_, str>> {
        render_text(&self.cell(column)?).map(Cow::Owned)
    }

    fn boolean(&self, column: &str) -> Option<bool> {
        match self.cell(column)? {
            Value::Bool(b) => as_json(&b)?.as_bool(),
            _ => None,
        }
    }

    fn json_text(&self, column: &str, path: &[String]) -> Option<Cow<'_, str>> {
        // `Json` and `Jsonb` are separate associated types, so they need
        // separate arms even though both serialize the same way.
        let document = match self.cell(column)? {
            Value::Json(json) => as_json(&json)?,
            Value::Jsonb(jsonb) => as_json(&jsonb)?,
            _ => return None,
        };
        json_at(&document, path).map(Cow::Owned)
    }

    // `list` keeps the trait default of `None`. `records_from_row_view`
    // refuses a `ListElements` shape before reaching here, so the default
    // is unreachable rather than a silent wrong answer.
}

/// A scalar payload as JSON.
///
/// [`ScalarCore`](crate::backend::ScalarCore) guarantees `Serialize` and
/// nothing that renders text, so this is the one route to a payload's
/// value that every backend already provides. It is also the route that
/// happens to agree with Postgres: `i64` serializes to a JSON number
/// whose text is the decimal form, `uuid::Uuid` to its canonical
/// hyphenated string, and `bool` to `true` or `false`.
fn as_json<T: serde::Serialize>(payload: &T) -> Option<serde_json::Value> {
    serde_json::to_value(payload).ok()
}

/// One cell as the text the loading SQL would have produced for it.
///
/// Only the spellings whose text form is unambiguous and matches what
/// `'type:' || column` writes. Anything else is [`None`], which yields no
/// record rather than a record keyed on a rendering that may not match
/// the whole-table query. A caller needing one of those must widen this
/// deliberately, with a test pinning the rendering against the loader.
pub(crate) fn render_text<B: crate::backend::Backend>(value: &Value<B>) -> Option<String> {
    let scalar = match value {
        Value::String(s) => return Some(s.as_ref().to_string()),
        Value::Int(i) => as_json(i)?,
        Value::Uuid(u) => as_json(u)?,
        Value::Bool(b) => as_json(b)?,
        Value::Timestamp(t) => return timestamp_text(as_json(t)?),
        Value::TimestampTz(t) => return timestamp_text(as_json(t)?),
        _ => return None,
    };
    match scalar {
        serde_json::Value::String(text) => Some(text),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(if b { "true" } else { "false" }.to_string()),
        _ => None,
    }
}

/// A serialized timestamp respelled the way `to_jsonb` prints one, which is
/// what the loading SQL writes into a record's context: `Z` widens to
/// `+00:00`, and the fraction drops its trailing zeros, then itself when
/// nothing is left. The wal adapters normalize a `timestamptz` to UTC, so
/// `Z` and no suffix are the only spellings that reach this. Anything else
/// passes through untouched rather than being reshaped on a guess.
fn timestamp_text(scalar: serde_json::Value) -> Option<String> {
    let serde_json::Value::String(text) = scalar else {
        return None;
    };
    let (body, zone) = match text.strip_suffix('Z') {
        Some(body) => (body, "+00:00"),
        None if text.contains('+') => return Some(text),
        None => (text.as_str(), ""),
    };
    let body = match body.split_once('.') {
        Some((whole, fraction)) => {
            let fraction = fraction.trim_end_matches('0');
            if fraction.is_empty() {
                alloc::borrow::Cow::Borrowed(whole)
            } else {
                alloc::borrow::Cow::Owned(alloc::format!("{whole}.{fraction}"))
            }
        }
        None => alloc::borrow::Cow::Borrowed(body),
    };
    Some(alloc::format!("{body}{zone}"))
}

/// Walk `path` into `json`, outermost field first, and read the leaf as
/// text. A JSON string yields its contents rather than its quoted form,
/// matching `->>`.
fn json_at(json: &serde_json::Value, path: &[String]) -> Option<String> {
    let mut node = json;
    for field in path {
        node = node.get(field.as_str())?;
    }
    match node {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::vec;

    use rls2fga::classifier::patterns::ConfidenceLevel;
    use rls2fga::generator::records::{RowValues, SubjectKey};
    use rls2fga::parser::identifiers::{ColumnName, RelationName};
    use rls2fga::translator::TranslatorBuilder;
    use sqlparser::dialect::PostgreSqlDialect;

    use super::*;
    use crate::backend::Postgres;
    use crate::testing::TestEvent;
    use crate::visibility::EventRow;
    use crate::{catalog_helpers, ParserDB};

    const DDL: &str = "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, public BOOL, \
                       meta JSONB, key UUID, note JSON, score DOUBLE PRECISION, \
                       taken DATE, blob BYTEA, at TIMESTAMPTZ);";

    /// The same table with the ownership policy that makes rls2fga describe it, so the
    /// names in the description below are ones a translation decided.
    const POLICIED: &str = "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, public BOOL, \
                            meta JSONB, key UUID, note JSON, score DOUBLE PRECISION, \
                            taken DATE, blob BYTEA, at TIMESTAMPTZ);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_owner ON docs USING (owner = current_user);";

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap()
    }

    /// Name a column the one way a caller outside rls2fga can, so a guard and a JSON path
    /// name one too rather than spelling it as text.
    fn column(name: &str) -> ColumnName {
        match ValueSource::column(name) {
            ValueSource::Column(column) => column,
            other => unreachable!("`ValueSource::column` names a column, got {other:?}"),
        }
    }

    /// The description rls2fga emits for `owner = current_user` on `docs`.
    ///
    /// Read off a real translation rather than assembled here, since the object type and
    /// the relation are names the translation decides and nothing outside it may mint.
    fn translated_ownership() -> RecordDescription {
        let db = ParserDB::parse::<PostgreSqlDialect>(POLICIED).unwrap();
        TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .relations()
            .into_iter()
            .flat_map(|entry| entry.shapes)
            .find(|shape| {
                matches!(&shape.derivation, RecordDerivation::FromRow { template, .. }
                    if template.subject_type == "user")
            })
            .expect("the ownership policy describes records read from the row")
    }

    /// The relation a description names, for an assertion that must agree with it.
    fn relation_of(description: &RecordDescription) -> RelationName {
        match &description.derivation {
            RecordDerivation::FromRow { template, .. } => template.relation.clone(),
            other => unreachable!("the ownership description reads the row, got {other:?}"),
        }
    }

    /// That description with the subject and the guards this test wants.
    fn description(subject: ValueSource, guards: Vec<Guard>) -> RecordDescription {
        let mut described = translated_ownership();
        let RecordDerivation::FromRow {
            template,
            guards: existing,
            ..
        } = &mut described.derivation
        else {
            unreachable!("the ownership description reads the row");
        };
        template.subject_key = SubjectKey::new(subject);
        *existing = guards;
        described
    }

    fn owner_description() -> RecordDescription {
        description(ValueSource::column("owner"), vec![])
    }

    /// An insert carrying `[id, owner, public, meta, key, note, score]`.
    fn event(row: Vec<Value<Postgres>>) -> TestEvent<Postgres> {
        let db = catalog();
        let docs = catalog_helpers::table_id(&db, "docs").unwrap();
        TestEvent::insert(docs, row).with_pk_columns([0u16])
    }

    fn row_of(owner: Value<Postgres>) -> Vec<Value<Postgres>> {
        vec![
            Value::Int(4),
            owner,
            Value::Bool(false),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]
    }

    fn records(
        description: &RecordDescription,
        row: Vec<Value<Postgres>>,
    ) -> Result<Vec<Record>, RowRecordError> {
        let db = catalog();
        let event = event(row);
        let view = EventRow::current(&event, &db).unwrap();
        records_from_row_view(description, &view, &db)
    }

    /// The whole point: an owner column becomes a record naming that user,
    /// with no database.
    #[test]
    fn a_text_owner_column_becomes_one_record() {
        let described = owner_description();
        let got = records(&described, row_of(Value::String("alice".into()))).unwrap();
        assert_eq!(
            got,
            vec![Record {
                object: "docs:4".into(),
                relation: relation_of(&described),
                subject: "user:alice".into(),
                context: None,
            }]
        );
    }

    /// The object key is an `INT` primary key, which is the common case.
    /// Rendering it as anything but its decimal form keys the record on an
    /// object the loader never wrote.
    #[test]
    fn an_integer_key_renders_as_its_decimal_form() {
        let got = records(&owner_description(), row_of(Value::String("bob".into()))).unwrap();
        assert_eq!(got[0].object, "docs:4");
    }

    /// A UUID subject must render canonically, lowercase and hyphenated,
    /// which is what `uuid::text` writes.
    #[test]
    fn a_uuid_renders_canonically() {
        let id = uuid::Uuid::parse_str("550E8400-E29B-41D4-A716-446655440000").unwrap();
        let mut row = row_of(Value::Null);
        row[4] = Value::Uuid(id);
        let got = records(&description(ValueSource::column("key"), vec![]), row).unwrap();
        assert_eq!(got[0].subject, "user:550e8400-e29b-41d4-a716-446655440000");
    }

    /// A `timestamptz` renders exactly as `to_jsonb` prints one, which is
    /// what the loading SQL writes into a record's context: `T` separator,
    /// `+00:00` rather than `Z`, and the fraction trimmed of trailing zeros
    /// down to nothing.
    #[test]
    fn a_timestamptz_renders_as_to_jsonb_prints_it() {
        use super::render_text;
        use crate::backend::Postgres;

        let whole = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            render_text::<Postgres>(&Value::TimestampTz(whole)).as_deref(),
            Some("2026-01-01T00:00:00+00:00")
        );

        let fractional = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123400Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            render_text::<Postgres>(&Value::TimestampTz(fractional)).as_deref(),
            Some("2026-01-01T00:00:00.1234+00:00"),
            "the fraction drops its trailing zeros exactly as PostgreSQL prints it"
        );

        let naive =
            chrono::NaiveDateTime::parse_from_str("2026-01-01 00:00:30", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        assert_eq!(
            render_text::<Postgres>(&Value::Timestamp(naive)).as_deref(),
            Some("2026-01-01T00:00:30"),
            "a timestamp without a zone carries none"
        );
    }

    /// A NULL subject grants nobody, and says so by producing no record
    /// rather than `user:`.
    #[test]
    fn a_null_subject_produces_no_record() {
        assert_eq!(
            records(&owner_description(), row_of(Value::Null)).unwrap(),
            vec![]
        );
    }

    /// A column the catalog does not know refuses rather than answering as
    /// an empty set, which would read as "this row grants nobody".
    #[test]
    fn an_unknown_column_is_refused_rather_than_answered_empty() {
        let d = description(ValueSource::column("nonexistent"), vec![]);
        let Err(RowRecordError::UnreadableColumn(reason)) =
            records(&d, row_of(Value::String("alice".into())))
        else {
            panic!("an uncatalogued column must refuse");
        };
        assert!(
            reason.contains("nonexistent"),
            "the refusal names the column: {reason}"
        );
    }

    /// A column whose kind the row side cannot spell refuses at setup, never
    /// answers as an empty set. The loading SQL spells every kind through
    /// `::text`, so an empty answer here would silently withhold additions
    /// and withdrawals the loader states.
    #[test]
    fn an_unspellable_kind_is_refused_rather_than_answered_empty() {
        let db = catalog();
        for (name, kind) in [
            ("taken", "Date"),
            ("blob", "Bytes"),
            ("score", "Float"),
            ("meta", "Jsonb"),
        ] {
            let d = description(ValueSource::column(name), vec![]);
            assert!(
                !is_evaluable(&d, &db),
                "{name} holds a {kind}, which text cannot spell"
            );
            let Err(RowRecordError::UnreadableColumn(reason)) =
                records(&d, row_of(Value::String("alice".into())))
            else {
                panic!("{name} must refuse");
            };
            assert!(
                reason.contains(name) && reason.contains(kind),
                "the refusal names the column and its kind: {reason}"
            );
        }
        // The contrast that keeps the gate honest: a spellable kind stays
        // evaluable through the same path.
        assert!(is_evaluable(
            &description(ValueSource::column("at"), vec![]),
            &db
        ));
    }

    /// A guard is read at a kind too: `IsTrue` wants a boolean and the
    /// textual comparisons want a spellable kind, so a guard over the wrong
    /// kind refuses rather than failing silently for every row.
    #[test]
    fn a_guard_over_the_wrong_kind_is_refused() {
        use rls2fga::classifier::patterns::{AttributeOperator, AttributePredicate};

        let subject = || ValueSource::column("owner");
        let cases: [(Guard, &str); 3] = [
            (Guard::IsTrue(column("owner")), "as a boolean"),
            (Guard::NotNull(column("taken")), "Date"),
            (
                Guard::Compare(AttributePredicate {
                    column: column("score"),
                    operator: AttributeOperator::Gt,
                    value: AttributeLiteral::Number("0".into()),
                }),
                "Float",
            ),
        ];
        for (guard, expected) in cases {
            let d = description(subject(), vec![guard]);
            let Err(RowRecordError::UnreadableColumn(reason)) =
                records(&d, row_of(Value::String("alice".into())))
            else {
                panic!("a guard over the wrong kind must refuse");
            };
            assert!(
                reason.contains(expected),
                "the refusal names what diverged: {reason}"
            );
        }
    }

    /// A JSON path wants a document, so a path into a scalar column refuses.
    #[test]
    fn a_json_path_into_a_scalar_column_is_refused() {
        let d = description(
            ValueSource::JsonPath {
                column: column("owner"),
                path: vec!["sub".into()],
            },
            vec![],
        );
        let Err(RowRecordError::UnreadableColumn(reason)) =
            records(&d, row_of(Value::String("alice".into())))
        else {
            panic!("a path into a scalar must refuse");
        };
        assert!(
            reason.contains("JSON document"),
            "the refusal names the read: {reason}"
        );
    }

    /// The gate and the speller cannot drift: for every kind, the setup gate
    /// admits exactly the kinds `render_text` spells. Exhaustive on purpose,
    /// mirroring the term lookup's gate test.
    #[test]
    fn the_setup_gate_matches_what_render_text_spells() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).expect("epoch is a valid instant");
        let cases: [(ScalarKind, Value<Postgres>); 13] = [
            (ScalarKind::Bool, Value::Bool(true)),
            (ScalarKind::Int, Value::Int(1)),
            (ScalarKind::Float, Value::Float(1.0)),
            (ScalarKind::String, Value::String("x".into())),
            (ScalarKind::Bytes, Value::Bytes(vec![1])),
            (ScalarKind::Uuid, Value::Uuid(uuid::Uuid::nil())),
            (ScalarKind::Timestamp, Value::Timestamp(epoch.naive_utc())),
            (ScalarKind::TimestampTz, Value::TimestampTz(epoch)),
            (ScalarKind::Date, Value::Date(epoch.date_naive())),
            (ScalarKind::Time, Value::Time(epoch.time())),
            (
                ScalarKind::Decimal,
                Value::Decimal(bigdecimal::BigDecimal::from(1)),
            ),
            (ScalarKind::Json, Value::Json(serde_json::Value::Null)),
            (ScalarKind::Jsonb, Value::Jsonb(serde_json::Value::Null)),
        ];
        for (kind, value) in cases {
            assert_eq!(
                spells_as_text(kind),
                render_text(&value).is_some(),
                "{kind:?} disagrees between the gate and the speller"
            );
        }
    }

    /// A guard that fails withdraws the record entirely.
    #[test]
    fn a_failing_guard_drops_the_record() {
        let d = description(
            ValueSource::column("owner"),
            vec![Guard::IsTrue(column("public"))],
        );
        assert_eq!(
            records(&d, row_of(Value::String("alice".into()))).unwrap(),
            vec![]
        );

        let mut row = row_of(Value::String("alice".into()));
        row[2] = Value::Bool(true);
        assert_eq!(records(&d, row).unwrap().len(), 1);
    }

    /// A JSON path subject reads the leaf as text, not as its quoted JSON
    /// form, matching `->>`.
    #[test]
    fn a_json_path_subject_reads_the_leaf_as_text() {
        let mut row = row_of(Value::Null);
        row[3] = Value::Jsonb(serde_json::json!({"acl": {"owner": "carol"}}));
        let d = description(
            ValueSource::JsonPath {
                column: column("meta"),
                path: vec!["acl".into(), "owner".into()],
            },
            vec![],
        );
        assert_eq!(records(&d, row).unwrap()[0].subject, "user:carol");
    }

    /// The one that matters. `Value` has no array variant, so a list shape
    /// cannot be expanded. Answering it as an empty set would read as
    /// "this row grants nobody", silently withdrawing every member's
    /// access, so it must be an error the caller has to handle.
    #[test]
    fn a_list_shape_is_refused_rather_than_answered_empty() {
        let d = description(ValueSource::ListElements(column("owner")), vec![]);
        assert_eq!(
            records(&d, row_of(Value::String("alice".into()))),
            Err(RowRecordError::UnsupportedValueSource("list"))
        );
    }

    /// A shape reading a second table is refused with the reason rls2fga
    /// recorded, never an empty set.
    #[test]
    fn a_joining_shape_is_refused_with_its_reason() {
        let d = RecordDescription {
            tables: vec!["docs".into(), "grants".into()],
            derivation: RecordDerivation::Joined {
                queries: vec![],
                reason: "reads the grants table".into(),
            },
        };
        assert_eq!(
            records(&d, row_of(Value::String("alice".into()))),
            Err(RowRecordError::Refused(
                RecordError::NotDerivableFromOneRow("reads the grants table".into())
            ))
        );
    }

    /// A cell the event does not carry is absent, not empty text.
    #[test]
    fn a_missing_cell_produces_no_record() {
        assert_eq!(
            records(&owner_description(), row_of(Value::Missing)).unwrap(),
            vec![]
        );
    }

    /// The previous image answers about the row as it was, which is what
    /// a withdrawal has to be computed from.
    #[test]
    fn the_previous_image_answers_about_the_old_owner() {
        let db = catalog();
        let docs = catalog_helpers::table_id(&db, "docs").unwrap();
        let event = TestEvent::<Postgres>::update(
            docs,
            row_of(Value::String("alice".into())),
            row_of(Value::String("bob".into())),
        )
        .with_pk_columns([0u16]);
        let d = owner_description();

        let before = EventRow::previous(&event, &db).unwrap();
        let after = EventRow::current(&event, &db).unwrap();
        assert_eq!(
            records_from_row_view(&d, &before, &db).unwrap()[0].subject,
            "user:alice"
        );
        assert_eq!(
            records_from_row_view(&d, &after, &db).unwrap()[0].subject,
            "user:bob"
        );
    }

    /// The adapter reads booleans through the trait's own accessor, which
    /// is what `Guard::IsTrue` calls.
    #[test]
    fn the_boolean_accessor_reads_a_bool_column() {
        let db = catalog();
        let mut row = row_of(Value::Null);
        row[2] = Value::Bool(true);
        let event = event(row);
        let view = EventRow::current(&event, &db).unwrap();
        let values = RowValuesView {
            row: &view,
            db: &db,
            undecodable: Cell::new(None),
        };
        assert_eq!(values.boolean("public"), Some(true));
        assert_eq!(values.boolean("owner"), None, "a text column is not a bool");
        assert_eq!(values.list("owner"), None, "no array variant to expand");
    }

    /// `json` and `jsonb` are separate associated types and separate
    /// match arms, so both need exercising or one can rot.
    #[test]
    fn a_json_column_reads_the_same_as_a_jsonb_one() {
        let mut row = row_of(Value::Null);
        row[5] = Value::Json(serde_json::json!({"acl": {"owner": "dan"}}));
        let d = description(
            ValueSource::JsonPath {
                column: column("note"),
                path: vec!["acl".into(), "owner".into()],
            },
            vec![],
        );
        assert_eq!(records(&d, row).unwrap()[0].subject, "user:dan");
    }

    /// A JSON leaf that is null, or a path that runs off the document,
    /// grants nobody rather than granting `user:null`.
    #[test]
    fn an_absent_or_null_json_leaf_produces_no_record() {
        for document in [
            serde_json::json!({ "acl": { "owner": serde_json::Value::Null } }),
            serde_json::json!({ "acl": {} }),
            serde_json::json!({}),
        ] {
            let mut row = row_of(Value::Null);
            row[3] = Value::Jsonb(document);
            let d = description(
                ValueSource::JsonPath {
                    column: column("meta"),
                    path: vec!["acl".into(), "owner".into()],
                },
                vec![],
            );
            assert_eq!(records(&d, row).unwrap(), vec![]);
        }
    }

    /// A non-string JSON leaf keeps its own text, which is what `->>`
    /// yields for a number.
    #[test]
    fn a_numeric_json_leaf_reads_as_its_digits() {
        let mut row = row_of(Value::Null);
        row[3] = Value::Jsonb(serde_json::json!({"acl": {"owner": 12}}));
        let d = description(
            ValueSource::JsonPath {
                column: column("meta"),
                path: vec!["acl".into(), "owner".into()],
            },
            vec![],
        );
        assert_eq!(records(&d, row).unwrap()[0].subject, "user:12");
    }

    /// A boolean subject renders as Postgres writes it, not as `1`.
    #[test]
    fn a_boolean_renders_as_true_or_false() {
        for (flag, expected) in [(true, "user:true"), (false, "user:false")] {
            let mut row = row_of(Value::Null);
            row[2] = Value::Bool(flag);
            let d = description(ValueSource::column("public"), vec![]);
            assert_eq!(records(&d, row).unwrap()[0].subject, expected);
        }
    }

    /// Both refusals say which one they are, since a caller routes on the
    /// difference: one delegates to a query, the other is a gap.
    #[test]
    fn both_refusals_render_distinctly() {
        use alloc::format;
        assert_eq!(
            format!(
                "{}",
                RowRecordError::Refused(RecordError::NotDerivableFromOneRow("reads grants".into()))
            ),
            "records do not follow from one row and must be queried: reads grants"
        );
        assert_eq!(
            format!("{}", RowRecordError::UnsupportedValueSource("list")),
            "a row view cannot answer a list column"
        );
    }
}
