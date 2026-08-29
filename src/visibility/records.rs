//! Which authorization records one changed row implies.
//!
//! `rls2fga-types` describes a relation's records as structure: a template
//! naming where the object key and the subject key come from, plus guards
//! the row must satisfy. Given a row's column values it evaluates that
//! description with no database. This module is the adapter that lets a
//! subql [`RowView`] be those column values.
//!
//! # Refusing is part of the contract
//!
//! [`rls2fga_types::RowValues`] answers `None` for anything
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

use rls2fga_types::{
    records_from_row, ColumnKind, ColumnRead, Guard, Record, RecordDerivation, RecordDescription,
    RecordError, RowCell, RowList, RowValues, ValueSource,
};
use rls2fga_types::{AttributeLiteral, AttributeOperator, AttributePredicate};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{
    Backend, BuiltinKind, JsonDocument, ScalarKindOf, ScalarText, ScalarTruth, Value,
};
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
    /// `rls2fga-types` refused to produce records for this row.
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
    if let Some(refusal) = unsupported_description::<R::Backend, DB>(description, db) {
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
pub fn is_evaluable<B: crate::backend::Backend, DB: DatabaseLike>(
    description: &RecordDescription,
    db: &DB,
) -> bool {
    unsupported_description::<B, DB>(description, db).is_none()
}

/// Why a row view cannot answer `description`, or [`None`].
fn unsupported_description<B: crate::backend::Backend, DB: DatabaseLike>(
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
        return None;
    };
    let Some(table) = catalog_helpers::contract_table_id(db, table) else {
        return Some(RowRecordError::UnreadableColumn(alloc::format!(
            "any column of {table:?}, a table the catalog does not know"
        )));
    };
    template
        .object_key
        .parts()
        .iter()
        .find_map(|part| unsupported_value::<B, DB>(part, db, table))
        .or_else(|| unsupported_value::<B, DB>(template.subject_key.part(), db, table))
        .or_else(|| {
            guards
                .iter()
                .find_map(|guard| unsupported_guard::<B, DB>(guard, db, table))
        })
        .or_else(|| {
            template.context.as_ref().and_then(|context| {
                context
                    .entries
                    .iter()
                    .find_map(|entry| unsupported_value::<B, DB>(&entry.value, db, table))
            })
        })
}

fn unsupported_value<B: crate::backend::Backend, DB: DatabaseLike>(
    source: &ValueSource,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match source {
        ValueSource::Column(column) => direct_column_read::<B, DB>(column, db, table),
        ValueSource::JsonPath { column, .. } => document_read::<B, DB>(column, db, table),
        ValueSource::Literal(_) => None,
        ValueSource::ListElements(_) => Some(RowRecordError::UnsupportedValueSource("list")),
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised")),
    }
}

fn unsupported_guard<B: crate::backend::Backend, DB: DatabaseLike>(
    guard: &Guard,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    match guard {
        Guard::NotNull(column) => direct_column_read::<B, DB>(column, db, table),
        Guard::IsTrue(column) => bool_read::<B, DB>(column, db, table),
        Guard::Compare { column, predicate } => {
            comparison_read::<B, DB>(column, predicate, db, table)
        }
        _ => Some(RowRecordError::UnsupportedValueSource("unrecognised guard")),
    }
}

fn comparison_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    predicate: &AttributePredicate,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    match (&predicate.value, kind) {
        (AttributeLiteral::Boolean(_), ColumnKind::Bool)
        | (AttributeLiteral::Number(_), ColumnKind::Integer | ColumnKind::Decimal) => None,
        (AttributeLiteral::Text(_), ColumnKind::Text)
            if matches!(
                predicate.operator,
                AttributeOperator::Eq | AttributeOperator::NotEq
            ) =>
        {
            None
        }
        (AttributeLiteral::Text(_), ColumnKind::Text) => Some(RowRecordError::UnreadableColumn(
            alloc::format!("comparison over column {column} needs a query"),
        )),
        (
            AttributeLiteral::Boolean(_) | AttributeLiteral::Number(_) | AttributeLiteral::Text(_),
            _,
        ) => Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} cannot be compared as {:?}",
            predicate.value
        ))),
        _ => Some(RowRecordError::UnsupportedValueSource(
            "unrecognised comparison",
        )),
    }
}

fn direct_column_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if direct_kind(kind) {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} has unsupported kind {kind:?}"
        )))
    }
}

fn bool_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if kind == ColumnKind::Bool {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {kind:?}, not Bool"
        )))
    }
}

fn document_read<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Option<RowRecordError> {
    let kind = match column_read_kind::<B, DB>(column, db, table) {
        Ok(kind) => kind,
        Err(refusal) => return Some(refusal),
    };
    if kind == ColumnKind::Json {
        None
    } else {
        Some(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {kind:?}, not Json"
        )))
    }
}

const fn direct_kind(kind: ColumnKind) -> bool {
    !matches!(kind, ColumnKind::Json | ColumnKind::Unsupported)
}

fn column_read_kind<B: crate::backend::Backend, DB: DatabaseLike>(
    column: &ColumnRead,
    db: &DB,
    table: TableId,
) -> Result<ColumnKind, RowRecordError> {
    let refuse = || {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column}, which the catalog does not know or cannot type"
        ))
    };
    let id = catalog_helpers::column_id(db, table, column.as_str()).ok_or_else(refuse)?;
    let scalar = catalog_helpers::column_scalar_kind::<B, DB>(db, table, id).ok_or_else(refuse)?;
    // A custom type answers `None`: it has no renderable column kind, so a
    // shape that reads this column is refused rather than served a spelling
    // subql cannot prove (R1).
    let actual = column_kind_from_scalar::<B>(scalar).ok_or_else(|| {
        RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} holds a custom type, which has no spelling this side can prove"
        ))
    })?;
    if actual == column.kind() {
        Ok(actual)
    } else {
        Err(RowRecordError::UnreadableColumn(alloc::format!(
            "column {column} is {actual:?} in the catalog but the shape reads {:?}",
            column.kind()
        )))
    }
}

fn column_kind_from_scalar<B: crate::backend::Backend>(
    kind: ScalarKindOf<B>,
) -> Option<ColumnKind> {
    // A custom type has no column kind here on purpose: rendering it would
    // mean asserting a text form subql cannot prove matches the loading SQL
    // (R1), so a shape that renders such a column is reported uncovered.
    Some(match kind.as_builtin()? {
        BuiltinKind::Bool => ColumnKind::Bool,
        BuiltinKind::Int => ColumnKind::Integer,
        BuiltinKind::Float => ColumnKind::Unsupported,
        BuiltinKind::String => ColumnKind::Text,
        BuiltinKind::Bytes => ColumnKind::Bytea,
        BuiltinKind::Uuid => ColumnKind::Uuid,
        BuiltinKind::Timestamp => ColumnKind::Timestamp,
        BuiltinKind::TimestampTz => ColumnKind::TimestampTz,
        BuiltinKind::Date => ColumnKind::Date,
        BuiltinKind::Time => ColumnKind::Time,
        BuiltinKind::Decimal => ColumnKind::Decimal,
        BuiltinKind::Json | BuiltinKind::Jsonb => ColumnKind::Json,
    })
}

// ---------------------------------------------------------------------------
// The adapter
// ---------------------------------------------------------------------------

/// A [`RowView`] seen as one row's column values.
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
    undecodable: Cell<Option<ValueError>>,
}

enum CellRead {
    Absent,
    Undecodable,
}

impl<R, DB> RowValuesView<'_, R, DB>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    fn value(&self, column: &str) -> Result<Value<R::Backend>, CellRead> {
        let Some(id) = catalog_helpers::column_id(self.db, self.row.table_id(), column) else {
            return Err(CellRead::Absent);
        };
        match self.row.value_at(id) {
            Ok(value) => Ok(value),
            Err(error) => {
                let seen = self.undecodable.take();
                self.undecodable.set(seen.or(Some(error)));
                Err(CellRead::Undecodable)
            }
        }
    }
}

impl<R, DB> RowValues for RowValuesView<'_, R, DB>
where
    R: RowView + ?Sized,
    DB: DatabaseLike,
{
    fn cell(&self, column: &str, _kind: ColumnKind) -> RowCell<'_> {
        match self.value(column) {
            Ok(value) => row_cell(&value),
            Err(CellRead::Absent) => RowCell::Absent,
            Err(CellRead::Undecodable) => RowCell::Undecodable,
        }
    }

    fn list(&self, column: &str, _kind: ColumnKind) -> RowList<'_> {
        match self.value(column) {
            Ok(Value::Missing) | Err(CellRead::Absent) => RowList::Absent,
            Ok(Value::Null) => RowList::Null,
            Ok(_) | Err(CellRead::Undecodable) => RowList::Undecodable,
        }
    }

    fn json_text(&self, column: &str, path: &[String]) -> RowCell<'_> {
        match self.value(column) {
            Ok(Value::Missing) | Err(CellRead::Absent) => RowCell::Absent,
            Ok(Value::Null) => RowCell::Null,
            Ok(Value::Json(json)) => json
                .json_document()
                .map_or(RowCell::Undecodable, |document| json_at(document, path)),
            Ok(Value::Jsonb(jsonb)) => jsonb
                .json_document()
                .map_or(RowCell::Undecodable, |document| json_at(document, path)),
            Ok(_) | Err(CellRead::Undecodable) => RowCell::Undecodable,
        }
    }
}

fn row_cell<B: Backend>(value: &Value<B>) -> RowCell<'static> {
    match value {
        Value::Missing => RowCell::Absent,
        Value::Null => RowCell::Null,

        Value::Bool(value) => RowCell::Bool(value.scalar_truth()),
        Value::Int(value) => RowCell::Integer(Cow::Owned(value.scalar_text().into_owned())),
        // A float and a JSON document have no column kind on the other side,
        // and a custom type has no spelling this side can prove matches the
        // loading SQL (R1), so all of them read as undecodable rather than as
        // a rendered value.
        Value::Float(_) | Value::Json(_) | Value::Jsonb(_) | Value::Custom(_) => {
            RowCell::Undecodable
        }
        Value::String(value) => RowCell::Text(Cow::Owned(value.as_ref().to_string())),
        Value::Bytes(value) => RowCell::Bytea(Cow::Owned(value.as_ref().to_vec())),
        Value::Uuid(value) => RowCell::Uuid(Cow::Owned(value.scalar_text().into_owned())),
        Value::Timestamp(value) => RowCell::Timestamp(Cow::Owned(value.scalar_text().into_owned())),
        Value::TimestampTz(value) => {
            RowCell::TimestampTz(Cow::Owned(value.scalar_text().into_owned()))
        }
        Value::Date(value) => RowCell::Date(Cow::Owned(value.scalar_text().into_owned())),
        Value::Time(value) => RowCell::Time(Cow::Owned(value.scalar_text().into_owned())),
        Value::Decimal(value) => RowCell::Decimal(Cow::Owned(value.scalar_text().into_owned())),
    }
}

fn json_at(json: &serde_json::Value, path: &[String]) -> RowCell<'static> {
    let mut node = json;
    for field in path {
        let Some(next) = node.get(field.as_str()) else {
            return RowCell::Null;
        };
        node = next;
    }
    match node {
        serde_json::Value::Null => RowCell::Null,
        serde_json::Value::String(s) => RowCell::Text(Cow::Owned(s.clone())),
        other => RowCell::Text(Cow::Owned(other.to_string())),
    }
}

pub(crate) fn render_text<B: Backend>(value: &Value<B>) -> Option<String> {
    render_sql_text(&row_cell(value))
}

fn render_sql_text(cell: &RowCell<'_>) -> Option<String> {
    match cell {
        RowCell::Absent | RowCell::Null | RowCell::Undecodable => None,
        RowCell::Text(value)
        | RowCell::Uuid(value)
        | RowCell::Integer(value)
        | RowCell::Decimal(value)
        | RowCell::Date(value)
        | RowCell::Time(value) => Some(value.to_string()),
        RowCell::Bool(flag) => Some(if *flag { "true" } else { "false" }.to_string()),
        RowCell::Timestamp(value) => Some(timestamp_sql_text(value.as_ref())),
        RowCell::TimestampTz(value) => timestamptz_sql_text(value.as_ref()),
        RowCell::Bytea(bytes) => Some(bytea_sql_text(bytes.as_ref())),
    }
}

fn timestamp_sql_text(value: &str) -> String {
    let mut text = value.replace('T', " ");
    trim_fraction(&mut text);
    text
}

fn trim_fraction(text: &mut String) {
    let Some(dot) = text.rfind('.') else {
        return;
    };
    let end = text[dot + 1..]
        .find(|ch: char| !ch.is_ascii_digit())
        .map_or(text.len(), |offset| dot + 1 + offset);
    let trimmed = text[dot + 1..end].trim_end_matches('0').len();
    if trimmed == 0 {
        text.replace_range(dot..end, "");
    } else {
        text.replace_range(dot + 1 + trimmed..end, "");
    }
}

fn utc_timestamptz_base(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    for suffix in ["+00:00", "-00:00", "+00", "-00", "Z"] {
        if let Some(base) = trimmed.strip_suffix(suffix) {
            return Some(base);
        }
    }
    None
}

fn timestamptz_sql_text(value: &str) -> Option<String> {
    let base = utc_timestamptz_base(value)?;
    let mut text = timestamp_sql_text(base);
    text.push_str("+00");
    Some(text)
}

fn bytea_sql_text(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::backend::{BuiltinKind, ScalarKind};
    use alloc::vec;

    use rls2fga::translator::TranslatorBuilder;
    use rls2fga_types::ConfidenceLevel;
    use rls2fga_types::{ColumnName, RelationName};
    use rls2fga_types::{ColumnRead, ContextRendering, RowValues, SubjectKey};
    use sqlparser::dialect::PostgreSqlDialect;

    use super::*;
    use crate::backend::Postgres;
    use crate::testing::TestEvent;
    use crate::visibility::test_names;
    use crate::visibility::EventRow;
    use crate::{catalog_helpers, ParserDB};

    const DDL: &str = "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, public BOOL, \
                       meta JSONB, key UUID, note JSON, score DOUBLE PRECISION, \
                       taken DATE, blob BYTEA, at TIMESTAMPTZ);";

    const POLICIED: &str = "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, public BOOL, \
                            meta JSONB, key UUID, note JSON, score DOUBLE PRECISION, \
                            taken DATE, blob BYTEA, at TIMESTAMPTZ);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_owner ON docs USING (owner = current_user);";

    fn catalog() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(DDL).unwrap()
    }

    fn name(name: &str) -> ColumnName {
        serde_json::from_value(serde_json::Value::String(name.to_owned())).unwrap()
    }

    fn column(name: &str) -> ColumnRead {
        ColumnRead::text(name)
    }

    fn typed_column(column: &str, kind: ColumnKind) -> ColumnRead {
        ColumnRead::new(name(column), kind)
    }

    fn translated_ownership() -> RecordDescription {
        let db = ParserDB::parse::<PostgreSqlDialect>(POLICIED).unwrap();
        TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .relations()
            .iter()
            .flat_map(|entry| entry.shapes.clone())
            .find(|shape| {
                matches!(&shape.derivation, RecordDerivation::FromRow { template, .. }
                    if template.subject_type == "user")
            })
            .expect("the ownership policy describes records read from the row")
    }

    fn relation_of(description: &RecordDescription) -> RelationName {
        match &description.derivation {
            RecordDerivation::FromRow { template, .. } => template.relation.clone(),
            other => unreachable!("the ownership description reads the row, got {other:?}"),
        }
    }

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

    #[test]
    fn an_integer_key_renders_as_its_decimal_form() {
        let got = records(&owner_description(), row_of(Value::String("bob".into()))).unwrap();
        assert_eq!(got[0].object, "docs:4");
    }

    #[test]
    fn a_uuid_renders_canonically() {
        let id = uuid::Uuid::parse_str("550E8400-E29B-41D4-A716-446655440000").unwrap();
        let mut row = row_of(Value::Null);
        row[4] = Value::Uuid(id);
        let got = records(
            &description(
                ValueSource::typed_column(name("key"), ColumnKind::Uuid),
                vec![],
            ),
            row,
        )
        .unwrap();
        assert_eq!(got[0].subject, "user:550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn a_timestamptz_renders_as_sql_text() {
        use super::render_text;
        use crate::backend::Postgres;

        let whole = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            render_text::<Postgres>(&Value::TimestampTz(whole)).as_deref(),
            Some("2026-01-01 00:00:00+00")
        );

        let fractional = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123400Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            render_text::<Postgres>(&Value::TimestampTz(fractional)).as_deref(),
            Some("2026-01-01 00:00:00.1234+00")
        );

        let naive =
            chrono::NaiveDateTime::parse_from_str("2026-01-01 00:00:30", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        assert_eq!(
            render_text::<Postgres>(&Value::Timestamp(naive)).as_deref(),
            Some("2026-01-01 00:00:30")
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

    #[test]
    fn unsupported_direct_kinds_are_refused_at_setup() {
        let db = catalog();
        for (name, source, expected) in [
            (
                "score",
                ValueSource::typed_column(name("score"), ColumnKind::Unsupported),
                "Unsupported",
            ),
            (
                "meta",
                ValueSource::typed_column(name("meta"), ColumnKind::Json),
                "Json",
            ),
        ] {
            let d = description(source, vec![]);
            assert!(!is_evaluable::<Postgres, _>(&d, &db), "{name} must refuse");
            let Err(RowRecordError::UnreadableColumn(reason)) =
                records(&d, row_of(Value::String("alice".into())))
            else {
                panic!("{name} must refuse");
            };
            assert!(
                reason.contains(name) && reason.contains(expected),
                "the refusal names the column and kind: {reason}"
            );
        }
        for source in [
            ValueSource::typed_column(name("taken"), ColumnKind::Date),
            ValueSource::typed_column(name("blob"), ColumnKind::Bytea),
            ValueSource::typed_column(name("at"), ColumnKind::TimestampTz),
        ] {
            assert!(is_evaluable::<Postgres, _>(
                &description(source, vec![]),
                &db
            ));
        }
    }

    #[test]
    fn a_timestamp_identity_is_served_with_sql_text() {
        let db = catalog();
        let d = description(
            ValueSource::typed_column(name("at"), ColumnKind::TimestampTz),
            vec![],
        );
        assert!(is_evaluable::<Postgres, _>(&d, &db));

        let moment = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut row = row_of(Value::String("alice".into()));
        row.extend([Value::Null, Value::Null, Value::TimestampTz(moment)]);
        let got = records(&d, row).unwrap();
        assert!(got[0].subject.starts_with("user:~"));
    }

    #[test]
    fn a_timestamp_comparison_is_refused_and_presence_is_not() {
        use rls2fga_types::{AttributeOperator, AttributePredicate};

        let db = catalog();
        let compared = description(
            ValueSource::column("owner"),
            vec![Guard::Compare {
                column: typed_column("at", ColumnKind::TimestampTz),
                predicate: AttributePredicate {
                    column: name("at"),
                    operator: AttributeOperator::LtEq,
                    value: AttributeLiteral::Text("2026-01-01".into()),
                },
            }],
        );
        assert!(!is_evaluable::<Postgres, _>(&compared, &db));

        let present = description(
            ValueSource::column("owner"),
            vec![Guard::NotNull(typed_column("at", ColumnKind::TimestampTz))],
        );
        assert!(is_evaluable::<Postgres, _>(&present, &db));
    }

    #[test]
    fn a_timestamp_context_keeps_serving() {
        use rls2fga_types::{RecordContext, RecordContextEntry};

        let db = catalog();
        let mut d = description(ValueSource::column("owner"), vec![]);
        let RecordDerivation::FromRow { template, .. } = &mut d.derivation else {
            unreachable!("the ownership description reads the row");
        };
        template.context = Some(RecordContext {
            condition: "when_docs".into(),
            entries: vec![RecordContextEntry {
                key: "at".into(),
                value: ValueSource::typed_column(name("at"), ColumnKind::TimestampTz),
                rendering: ContextRendering::Json,
            }],
        });
        assert!(is_evaluable::<Postgres, _>(&d, &db));

        let moment = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let mut row = row_of(Value::String("alice".into()));
        row.extend([Value::Null, Value::Null, Value::TimestampTz(moment)]);
        let got = records(&d, row).unwrap();
        let context = got[0].context.as_ref().expect("the record carries it");
        assert_eq!(
            context.values.get("at").map(alloc::string::String::as_str),
            Some("2026-01-01T00:00:00+00:00")
        );
    }

    #[test]
    fn a_guard_over_the_wrong_kind_is_refused() {
        use rls2fga_types::{AttributeOperator, AttributePredicate};

        let subject = || ValueSource::column("owner");
        let cases: [(Guard, &str); 3] = [
            (Guard::IsTrue(column("owner")), "not Bool"),
            (
                Guard::NotNull(typed_column("meta", ColumnKind::Json)),
                "Json",
            ),
            (
                Guard::Compare {
                    column: typed_column("score", ColumnKind::Unsupported),
                    predicate: AttributePredicate {
                        column: name("score"),
                        operator: AttributeOperator::Gt,
                        value: AttributeLiteral::Number("0".into()),
                    },
                },
                "compared",
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
            reason.contains("Json"),
            "the refusal names the read: {reason}"
        );
    }

    #[test]
    fn the_setup_gate_matches_what_render_text_spells() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).expect("epoch is a valid instant");
        let cases: [(BuiltinKind, Value<Postgres>); 13] = [
            (BuiltinKind::Bool, Value::Bool(true)),
            (BuiltinKind::Int, Value::Int(1)),
            (BuiltinKind::Float, Value::Float(1.0)),
            (BuiltinKind::String, Value::String("x".into())),
            (BuiltinKind::Bytes, Value::Bytes(vec![1])),
            (BuiltinKind::Uuid, Value::Uuid(uuid::Uuid::nil())),
            (BuiltinKind::Timestamp, Value::Timestamp(epoch.naive_utc())),
            (BuiltinKind::TimestampTz, Value::TimestampTz(epoch)),
            (BuiltinKind::Date, Value::Date(epoch.date_naive())),
            (BuiltinKind::Time, Value::Time(epoch.time())),
            (
                BuiltinKind::Decimal,
                Value::Decimal(bigdecimal::BigDecimal::from(1)),
            ),
            (BuiltinKind::Json, Value::Json(serde_json::Value::Null)),
            (BuiltinKind::Jsonb, Value::Jsonb(serde_json::Value::Null)),
        ];
        for (kind, value) in cases {
            assert_eq!(
                direct_kind(
                    column_kind_from_scalar::<Postgres>(ScalarKind::from(kind))
                        .expect("every builtin kind has a column kind"),
                ),
                render_text(&value).is_some(),
                "{kind:?} disagrees between the gate and the speller"
            );
        }
    }

    #[test]
    fn a_failing_guard_drops_the_record() {
        let d = description(
            ValueSource::column("owner"),
            vec![Guard::IsTrue(typed_column("public", ColumnKind::Bool))],
        );
        assert_eq!(
            records(&d, row_of(Value::String("alice".into()))).unwrap(),
            vec![]
        );

        let mut row = row_of(Value::String("alice".into()));
        row[2] = Value::Bool(true);
        assert_eq!(records(&d, row).unwrap().len(), 1);
    }

    #[test]
    fn a_json_path_subject_reads_the_leaf_as_text() {
        let mut row = row_of(Value::Null);
        row[3] = Value::Jsonb(serde_json::json!({"acl": {"owner": "carol"}}));
        let d = description(
            ValueSource::JsonPath {
                column: typed_column("meta", ColumnKind::Json),
                path: vec!["acl".into(), "owner".into()],
            },
            vec![],
        );
        assert_eq!(records(&d, row).unwrap()[0].subject, "user:carol");
    }

    #[test]
    fn a_list_shape_is_refused_rather_than_answered_empty() {
        let d = description(ValueSource::ListElements(column("owner")), vec![]);
        assert_eq!(
            records(&d, row_of(Value::String("alice".into()))),
            Err(RowRecordError::UnsupportedValueSource("list"))
        );
    }

    #[test]
    fn a_joining_shape_is_refused_with_its_reason() {
        let d = RecordDescription {
            tables: vec![test_names::table("docs"), test_names::table("grants")],
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

    #[test]
    fn a_missing_cell_is_refused() {
        assert_eq!(
            records(&owner_description(), row_of(Value::Missing)),
            Err(RowRecordError::Refused(RecordError::ColumnAbsent(
                "owner".to_string()
            )))
        );
    }

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

    #[test]
    fn the_typed_cell_reader_preserves_kind() {
        let db = catalog();
        let mut row = row_of(Value::String("alice".into()));
        row[2] = Value::Bool(true);
        let event = event(row);
        let view = EventRow::current(&event, &db).unwrap();
        let values = RowValuesView {
            row: &view,
            db: &db,
            undecodable: Cell::new(None),
        };
        assert_eq!(values.cell("public", ColumnKind::Bool), RowCell::Bool(true));
        assert!(matches!(
            values.cell("owner", ColumnKind::Bool),
            RowCell::Text(_)
        ));
        assert_eq!(values.list("owner", ColumnKind::Text), RowList::Undecodable);
    }

    #[test]
    fn a_json_column_reads_the_same_as_a_jsonb_one() {
        let mut row = row_of(Value::Null);
        row[5] = Value::Json(serde_json::json!({"acl": {"owner": "dan"}}));
        let d = description(
            ValueSource::JsonPath {
                column: typed_column("note", ColumnKind::Json),
                path: vec!["acl".into(), "owner".into()],
            },
            vec![],
        );
        assert_eq!(records(&d, row).unwrap()[0].subject, "user:dan");
    }

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
                    column: typed_column("meta", ColumnKind::Json),
                    path: vec!["acl".into(), "owner".into()],
                },
                vec![],
            );
            assert_eq!(records(&d, row).unwrap(), vec![]);
        }
    }

    #[test]
    fn a_numeric_json_leaf_reads_as_its_digits() {
        let mut row = row_of(Value::Null);
        row[3] = Value::Jsonb(serde_json::json!({"acl": {"owner": 12}}));
        let d = description(
            ValueSource::JsonPath {
                column: typed_column("meta", ColumnKind::Json),
                path: vec!["acl".into(), "owner".into()],
            },
            vec![],
        );
        assert_eq!(records(&d, row).unwrap()[0].subject, "user:12");
    }

    #[test]
    fn a_boolean_renders_as_true_or_false() {
        for (flag, expected) in [(true, "user:true"), (false, "user:false")] {
            let mut row = row_of(Value::Null);
            row[2] = Value::Bool(flag);
            let d = description(
                ValueSource::typed_column(name("public"), ColumnKind::Bool),
                vec![],
            );
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
