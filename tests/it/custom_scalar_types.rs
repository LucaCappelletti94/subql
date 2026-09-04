//! A backend that teaches subql two of its own SQL types.
//!
//! Every custom arm in the engine is unreachable for the three shipped
//! backends, whose custom set is uninhabited on purpose, so this is the only
//! place the custom path runs at all. It exercises the whole of it: a column
//! is classified, a wire cell is decoded through the type's carrier and its
//! conversion, a literal in subscription text goes through that same
//! conversion, and each refusal is reported as itself rather than as silence.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::ast::Value as SqlValue;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::Pg18;
use subql::backend::Postgres;
use subql::backend::{
    Backend, BuiltinKind, Carried, CustomScalars, ScalarKind, ScalarKindOf, Value,
};
use subql::compiler::SqlLiteralParse;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, ValueError,
};

/// A mood, spelled in the database as one of two labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
enum Mood {
    Happy,
    Sad,
}

/// The two types this backend adds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
enum MyKind {
    /// Carried as text.
    Mood,
    /// Carried as an integer.
    Build,
}

/// A decoded value of either.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
enum MyValue {
    Mood(Mood),
    Build(u32),
}

#[derive(Debug)]
struct MyScalars;

impl CustomScalars for MyScalars {
    type Kind = MyKind;
    type Value = MyValue;
    type Carrier = Custom;

    fn classify(declared_type: &str) -> Option<Self::Kind> {
        match declared_type.to_ascii_lowercase().as_str() {
            "mood" => Some(MyKind::Mood),
            "build" => Some(MyKind::Build),
            _ => None,
        }
    }

    fn carrier(kind: Self::Kind) -> BuiltinKind {
        match kind {
            MyKind::Mood => BuiltinKind::String,
            MyKind::Build => BuiltinKind::Int,
        }
    }

    fn convert(kind: Self::Kind, carried: Carried<'_, Custom>) -> Option<Self::Value> {
        match (kind, carried) {
            // Two spellings mean one value, which is what makes keying on the
            // converted value rather than the carrier observable.
            (MyKind::Mood, Carried::String(text)) => match text.to_ascii_lowercase().as_str() {
                "happy" | "glad" => Some(MyValue::Mood(Mood::Happy)),
                "sad" => Some(MyValue::Mood(Mood::Sad)),
                _ => None,
            },
            (MyKind::Build, Carried::Int(n)) => u32::try_from(*n).ok().map(MyValue::Build),
            _ => None,
        }
    }

    fn can_key(kind: Self::Kind) -> bool {
        // A build number keys, a mood refuses, so the per-variant answer is
        // exercised rather than assumed uniform.
        matches!(kind, MyKind::Build)
    }

    fn kind_of(value: &Self::Value) -> Self::Kind {
        match value {
            MyValue::Mood(_) => MyKind::Mood,
            MyValue::Build(_) => MyKind::Build,
        }
    }
}

/// Postgres' scalar shapes plus the two types above.
#[derive(Debug)]
struct Custom;

impl Backend for Custom {
    const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

    /// No cross-kind numeric comparison: this backend's fixtures compare
    /// same-kind values only.
    fn numeric_widening(
        _left: subql::backend::BuiltinKind,
        _right: subql::backend::BuiltinKind,
    ) -> Option<subql::backend::NumericWidening> {
        None
    }

    /// Byte comparison, which is all this backend's fixtures need.
    fn text_rule(
        _comparison: &subql::backend::ComparisonContext<'_, Self>,
        _operation: subql::backend::TextOperation,
    ) -> Option<subql::backend::TextRule> {
        Some(subql::backend::TextRule::EXACT)
    }

    type Dialect = PostgreSqlDialect;
    type Custom = MyScalars;
    type Bool = bool;
    type Int = i64;
    type Float = f64;
    type String = String;
    type Bytes = Vec<u8>;
    type Uuid = uuid::Uuid;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    type Jsonb = serde_json::Value;
    type JsonbVersion = Pg18;
}

impl SqlLiteralParse for Custom {
    fn parse_literal(
        sql: &SqlValue,
        target: ScalarKindOf<Self>,
    ) -> Result<Value<Self>, RegisterError> {
        // A backend with custom types implements the builtin arms itself and
        // routes the custom one through the engine, which is what keeps a
        // literal and a row cell on one conversion.
        if let ScalarKind::Custom(custom) = target {
            return subql::compiler::parse_custom_literal::<Self>(sql, custom);
        }
        let builtin = target.as_builtin().expect("not custom, so builtin");
        Ok(widen(Postgres::<Pg18>::parse_literal(
            sql,
            ScalarKind::from(builtin),
        )?))
    }
}

/// This backend's scalar shapes are Postgres', so a builtin literal parsed
/// there transfers unchanged. Only the custom position differs.
fn widen(value: Value<Postgres>) -> Value<Custom> {
    match value {
        Value::Missing => Value::Missing,
        Value::Null => Value::Null,
        Value::Bool(v) => Value::Bool(v),
        Value::Int(v) => Value::Int(v),
        Value::Float(v) => Value::Float(v),
        Value::String(v) => Value::String(v),
        Value::Bytes(v) => Value::Bytes(v),
        Value::Uuid(v) => Value::Uuid(v),
        Value::Timestamp(v) => Value::Timestamp(v),
        Value::TimestampTz(v) => Value::TimestampTz(v),
        Value::Date(v) => Value::Date(v),
        Value::Time(v) => Value::Time(v),
        Value::Decimal(v) => Value::Decimal(v),
        Value::Json(v) => Value::Json(v),
        Value::Jsonb(v) => Value::Jsonb(v),
        Value::Custom(none) => match none {},
    }
}

const DDL: &str = "CREATE TABLE feelings (id INT PRIMARY KEY, how mood, build build, \
                   note TEXT);";

fn db() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL")
}

fn kind_of_column(name: &str) -> Option<ScalarKindOf<Custom>> {
    let db = db();
    let table = catalog_helpers::table_id(&db, "feelings").unwrap();
    let column = catalog_helpers::column_id(&db, table, name).unwrap();
    catalog_helpers::column_scalar_kind::<Custom, _>(&db, table, column)
}

/// A column declaring one of the backend's own types classifies as it, and a
/// column declaring a type subql already knows still classifies as the
/// builtin, so an embedder cannot shadow a type the engine understands.
#[test]
fn a_declared_custom_type_classifies_as_itself() {
    assert_eq!(
        kind_of_column("how"),
        Some(ScalarKind::Custom(MyKind::Mood)),
        "a mood column is the backend's own type"
    );
    assert_eq!(
        kind_of_column("build"),
        Some(ScalarKind::Custom(MyKind::Build))
    );
    assert_eq!(
        kind_of_column("note"),
        Some(BuiltinKind::String.into()),
        "a builtin declaration wins over the backend's classifier"
    );
    assert_eq!(
        kind_of_column("id"),
        Some(BuiltinKind::Int.into()),
        "and so does an integer"
    );
}

/// A cell arrives as the carrier shape and reaches the conversion, so what
/// lands in the engine is the decoded custom value.
#[test]
fn a_wire_cell_decodes_through_the_carrier_and_the_conversion() {
    let decoded =
        subql::backend::decode_cell::<Custom, _>(0, ScalarKind::Custom(MyKind::Mood), |carrier| {
            assert_eq!(carrier, BuiltinKind::String, "a mood travels as text");
            Value::String("happy".to_owned())
        });
    assert_eq!(decoded, Ok(Value::Custom(MyValue::Mood(Mood::Happy))));

    let decoded =
        subql::backend::decode_cell::<Custom, _>(1, ScalarKind::Custom(MyKind::Build), |carrier| {
            assert_eq!(carrier, BuiltinKind::Int, "a build travels as an integer");
            Value::Int(1234)
        });
    assert_eq!(decoded, Ok(Value::Custom(MyValue::Build(1234))));
}

/// The two failures stay apart: a carrier that could not be read is reported
/// against the carrier, and a carrier that read fine but was declined is
/// reported against the type that declined it.
#[test]
fn a_refused_conversion_is_reported_as_itself_not_as_a_bad_carrier() {
    let refused =
        subql::backend::decode_cell::<Custom, _>(7, ScalarKind::Custom(MyKind::Mood), |_| {
            Value::String("furious".to_owned())
        });
    assert_eq!(
        refused,
        Err(ValueError::Custom {
            column: 7,
            custom: "Mood".to_owned()
        }),
        "the carrier read fine and the type declined it"
    );

    let malformed =
        subql::backend::decode_cell::<Custom, _>(9, ScalarKind::Custom(MyKind::Build), |_| {
            Value::Missing
        });
    assert_eq!(
        malformed,
        Err(ValueError::Builtin {
            column: 9,
            kind: BuiltinKind::Int
        }),
        "the carrier itself could not be read"
    );
}

/// A literal in subscription text goes through the same conversion a row's
/// cell does, so two spellings meaning one value both reach it, and a
/// spelling the type declines is a registration error rather than a filter
/// that silently never matches.
#[test]
fn a_custom_literal_parses_through_the_same_conversion() {
    let happy = Custom::parse_literal(
        &SqlValue::SingleQuotedString("happy".to_owned()),
        ScalarKind::Custom(MyKind::Mood),
    )
    .expect("happy is a mood");
    assert_eq!(happy, Value::Custom(MyValue::Mood(Mood::Happy)));

    let glad = Custom::parse_literal(
        &SqlValue::SingleQuotedString("glad".to_owned()),
        ScalarKind::Custom(MyKind::Mood),
    )
    .expect("glad is a mood");
    assert_eq!(
        glad, happy,
        "two spellings the conversion maps together are one value"
    );

    let refused = Custom::parse_literal(
        &SqlValue::SingleQuotedString("furious".to_owned()),
        ScalarKind::Custom(MyKind::Mood),
    );
    assert!(
        matches!(refused, Err(RegisterError::TypeError(_))),
        "a declined spelling refuses at registration, got {refused:?}"
    );

    let build = Custom::parse_literal(
        &SqlValue::Number("42".to_owned(), false),
        ScalarKind::Custom(MyKind::Build),
    )
    .expect("42 is a build");
    assert_eq!(build, Value::Custom(MyValue::Build(42)));
}

/// Keying is answered per type, not for the custom set as a whole.
#[test]
fn keying_is_answered_per_custom_type() {
    assert!(
        subql::term::kind_can_key::<Custom>(ScalarKind::Custom(MyKind::Build)),
        "a build number keys"
    );
    assert!(
        !subql::term::kind_can_key::<Custom>(ScalarKind::Custom(MyKind::Mood)),
        "a mood refuses keying, and the engine must respect that"
    );
    assert!(
        subql::term::kind_can_key::<Custom>(BuiltinKind::Int.into()),
        "builtins keep their own rule"
    );
    assert!(!subql::term::kind_can_key::<Custom>(
        BuiltinKind::Json.into()
    ));
}

/// A custom value names its own kind, so a decoded cell can still say what it
/// is without consulting the catalog again.
#[test]
fn a_custom_value_names_its_own_kind() {
    let value: Value<Custom> = Value::Custom(MyValue::Mood(Mood::Sad));
    assert_eq!(value.scalar_kind(), Some(ScalarKind::Custom(MyKind::Mood)));
    let value: Value<Custom> = Value::Custom(MyValue::Build(3));
    assert_eq!(value.scalar_kind(), Some(ScalarKind::Custom(MyKind::Build)));
}

/// The whole path, end to end: a subscription filtering on a column of the
/// backend's own type fires on the row that satisfies it and stays silent on
/// the row that does not.
///
/// This is the test that matters. Parsing a literal and decoding a cell can
/// both be right while the comparison between them answers `false`, which
/// registers a filter that never fires and reports nothing. That failure has
/// happened twice in this codebase, so it is pinned here rather than assumed.
#[test]
fn a_custom_filter_fires_on_the_row_it_names() {
    let table = catalog_helpers::table_id(&db(), "feelings").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Custom>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM feelings WHERE how = 'happy'",
        ))
        .expect("a filter on a custom column registers");

    let row = |id: i64, mood: Mood| {
        TestEvent::<Custom>::insert(
            table,
            vec![
                Value::Int(id),
                Value::Custom(MyValue::Mood(mood)),
                Value::Custom(MyValue::Build(1)),
                Value::String("note".to_owned()),
            ],
        )
        .with_pk_columns([0u16])
    };

    assert_eq!(
        engine
            .consumers(&row(1, Mood::Happy))
            .expect("dispatch")
            .inserted(),
        vec![1u64],
        "the happy row satisfies the filter"
    );
    assert!(
        engine
            .consumers(&row(2, Mood::Sad))
            .expect("dispatch")
            .inserted()
            .is_empty(),
        "the sad row does not"
    );
}

/// Two spellings the conversion maps to one value compare equal, which is the
/// point of keying and comparing on the converted value rather than on the
/// text that carried it.
#[test]
fn a_filter_written_with_one_spelling_matches_a_row_written_with_another() {
    let table = catalog_helpers::table_id(&db(), "feelings").unwrap();
    let mut engine: SubscriptionEngine<TestEvent<Custom>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db(), PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(
            1u64,
            // `glad` and `happy` are one value after conversion.
            "SELECT * FROM feelings WHERE how = 'glad'",
        ))
        .expect("registers");

    let event = TestEvent::<Custom>::insert(
        table,
        vec![
            Value::Int(1),
            Value::Custom(MyValue::Mood(Mood::Happy)),
            Value::Custom(MyValue::Build(1)),
            Value::String("note".to_owned()),
        ],
    )
    .with_pk_columns([0u16]);

    assert_eq!(
        engine.consumers(&event).expect("dispatch").inserted(),
        vec![1u64],
        "a row spelled `happy` satisfies a filter spelled `glad`"
    );
}
