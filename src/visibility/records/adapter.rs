use alloc::borrow::Cow;
use alloc::string::{String, ToString};
use core::cell::Cell;

use rls2fga_types::{ColumnKind, RowCell, RowList, RowValues};
use sql_traits::prelude::DatabaseLike;

use crate::backend::{Backend, JsonDocument, ScalarText, ScalarTruth, Value};
use crate::catalog_helpers;
use crate::visibility::RowView;
use crate::ValueError;

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

pub(super) struct RowValuesView<'a, R: ?Sized, DB> {
    pub(super) row: &'a R,
    pub(super) db: &'a DB,
    pub(super) undecodable: Cell<Option<ValueError>>,
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

pub fn render_text<B: Backend>(value: &Value<B>) -> Option<String> {
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
    use crate::backend::{ScalarFamily, ScalarKind};
    use alloc::vec;

    use rls2fga::translator::TranslatorBuilder;
    use rls2fga_types::ConfidenceLevel;
    use rls2fga_types::{ColumnName, RelationName};
    use rls2fga_types::{ColumnRead, ContextRendering, RowValues, SubjectKey};
    use sqlparser::dialect::PostgreSqlDialect;

    use rls2fga_types::{
        AttributeLiteral, ColumnKind, Guard, Record, RecordDerivation, RecordDescription,
        RecordError, RowCell, RowList, ValueSource,
    };

    use super::super::entry_point::{column_kind_from_scalar, direct_kind};
    use super::{render_text, RowValuesView};
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use crate::visibility::records::{is_evaluable, records_from_row_view, RowRecordError};
    use crate::visibility::test_names;
    use crate::visibility::EventRow;
    use crate::{catalog_helpers, ParserDB};
    use core::cell::Cell;

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
        let cases: [(ScalarFamily, Value<Postgres>); 13] = [
            (ScalarFamily::Bool, Value::Bool(true)),
            (ScalarFamily::Int, Value::Int(1)),
            (ScalarFamily::Float, Value::Float(1.0)),
            (ScalarFamily::String, Value::String("x".into())),
            (ScalarFamily::Bytes, Value::Bytes(vec![1])),
            (ScalarFamily::Uuid, Value::Uuid(uuid::Uuid::nil())),
            (ScalarFamily::Timestamp, Value::Timestamp(epoch.naive_utc())),
            (ScalarFamily::TimestampTz, Value::TimestampTz(epoch)),
            (ScalarFamily::Date, Value::Date(epoch.date_naive())),
            (ScalarFamily::Time, Value::Time(epoch.time())),
            (
                ScalarFamily::Decimal,
                Value::Decimal(bigdecimal::BigDecimal::from(1)),
            ),
            (ScalarFamily::Json, Value::Json(serde_json::Value::Null)),
            (ScalarFamily::Jsonb, Value::Jsonb(serde_json::Value::Null)),
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
