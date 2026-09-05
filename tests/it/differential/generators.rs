//! Schema first, then rows, then predicates over that schema.
//!
//! The order is the point. A predicate generated without knowing the
//! schema emits ill-typed SQL, and then a registration refusal means
//! "the generator produced nonsense" instead of "subql classified this",
//! which is the difference between a harness that finds divergences and
//! one that produces noise. So a column is generated with its declared
//! type and collation, a cell is generated for that column's kind, and a
//! predicate is generated from the columns it will read.
//!
//! What the generator has to force is not random data: it is the shapes
//! Parts II and III each turned out to be wrong about. Every one of those
//! is in here by name, so a regression in any of them is reachable from
//! generation alone:
//!
//! ```text
//! phase  shape the generator forces
//! C1     NaN in `real`, `double precision` and `numeric`
//! C2     `bool` ordering
//! C3     `jsonb` ordering
//! C4     a backslash in a `LIKE` pattern, and an explicit `ESCAPE`
//! C5     `char(n)` holding trailing spaces
//! C6     case-insensitive and nondeterministic collations
//! C7     a comparison between two columns of different kinds
//! C8     arithmetic that overflows `i64`
//! C9     a zero divisor
//! C11    `real` against `double precision`
//! C12    integer division
//! D1b    values astride 2^53 and astride `i64::MAX`
//! D3     infinities in a float column
//! D5     a case-insensitive pattern
//! ```
//!
//! `NULL` inside an `IN` list is here too, and the TOAST case belongs to
//! Phase E3, which is the layer that reads a row off the wire: a value
//! left unchanged by an `UPDATE` is a property of the event, not of the
//! row.
#![allow(clippy::unwrap_used)]

use bigdecimal::BigDecimal;
use core::str::FromStr as _;
use proptest::prelude::*;
use subql::backend::{Backend, BuiltinKind, Value};

use super::oracle::Engine;

/// One column of the generated table: what it is called, what family it
/// belongs to, and how each engine spells it.
///
/// `None` where an engine has no such type at all, which is a fact about
/// the engine rather than a gap: MySQL has no `UUID` or `JSONB` type and
/// SQLite has neither those nor a distinct temporal one.
pub struct ColumnSpec {
    pub name: &'static str,
    pub kind: BuiltinKind,
    pub postgres: Option<&'static str>,
    pub mysql: Option<&'static str>,
    pub sqlite: Option<&'static str>,
}

impl ColumnSpec {
    /// How `engine` spells this column's type, or `None` when it has no
    /// such type.
    #[must_use]
    pub const fn declared(&self, engine: Engine) -> Option<&'static str> {
        match engine {
            Engine::Postgres => self.postgres,
            Engine::MySql => self.mysql,
            Engine::Sqlite => self.sqlite,
        }
    }
}

/// Every column the harness generates, covering every
/// [`BuiltinKind`] and, for text, every declared type a comparison reads
/// differently: `char(n)` pads, `varchar` and `text` do not.
pub const COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        name: "flag",
        kind: BuiltinKind::Bool,
        postgres: Some("BOOLEAN"),
        mysql: Some("BOOLEAN"),
        sqlite: Some("BOOLEAN"),
    },
    ColumnSpec {
        name: "narrow",
        kind: BuiltinKind::Int,
        postgres: Some("INT"),
        mysql: Some("INT"),
        sqlite: Some("INTEGER"),
    },
    ColumnSpec {
        name: "wide",
        kind: BuiltinKind::Int,
        postgres: Some("BIGINT"),
        mysql: Some("BIGINT"),
        sqlite: Some("INTEGER"),
    },
    ColumnSpec {
        name: "single",
        kind: BuiltinKind::Float,
        postgres: Some("REAL"),
        mysql: Some("FLOAT"),
        sqlite: Some("REAL"),
    },
    ColumnSpec {
        name: "twice",
        kind: BuiltinKind::Float,
        postgres: Some("DOUBLE PRECISION"),
        mysql: Some("DOUBLE"),
        sqlite: Some("REAL"),
    },
    ColumnSpec {
        name: "exact",
        kind: BuiltinKind::Decimal,
        postgres: Some("NUMERIC"),
        mysql: Some("DECIMAL(20,4)"),
        sqlite: None,
    },
    ColumnSpec {
        name: "unbounded",
        kind: BuiltinKind::String,
        postgres: Some("TEXT"),
        mysql: Some("TEXT"),
        sqlite: Some("TEXT"),
    },
    ColumnSpec {
        name: "varying",
        kind: BuiltinKind::String,
        postgres: Some("VARCHAR(16)"),
        mysql: Some("VARCHAR(16)"),
        sqlite: Some("VARCHAR(16)"),
    },
    ColumnSpec {
        name: "padded",
        kind: BuiltinKind::String,
        postgres: Some("CHAR(5)"),
        mysql: Some("CHAR(5)"),
        sqlite: Some("CHAR(5)"),
    },
    ColumnSpec {
        name: "raw",
        kind: BuiltinKind::Bytes,
        postgres: Some("BYTEA"),
        mysql: Some("VARBINARY(32)"),
        sqlite: Some("BLOB"),
    },
    ColumnSpec {
        name: "identifier",
        kind: BuiltinKind::Uuid,
        postgres: Some("UUID"),
        mysql: None,
        sqlite: None,
    },
    ColumnSpec {
        name: "day",
        kind: BuiltinKind::Date,
        postgres: Some("DATE"),
        mysql: Some("DATE"),
        sqlite: None,
    },
    ColumnSpec {
        name: "clock",
        kind: BuiltinKind::Time,
        postgres: Some("TIME"),
        mysql: Some("TIME"),
        sqlite: None,
    },
    ColumnSpec {
        name: "stamp",
        kind: BuiltinKind::Timestamp,
        postgres: Some("TIMESTAMP"),
        mysql: Some("DATETIME"),
        sqlite: None,
    },
    ColumnSpec {
        name: "zoned",
        kind: BuiltinKind::TimestampTz,
        postgres: Some("TIMESTAMPTZ"),
        mysql: Some("TIMESTAMP"),
        sqlite: None,
    },
    ColumnSpec {
        name: "document",
        kind: BuiltinKind::Json,
        postgres: Some("JSON"),
        mysql: Some("JSON"),
        sqlite: None,
    },
    ColumnSpec {
        name: "binary_document",
        kind: BuiltinKind::Jsonb,
        postgres: Some("JSONB"),
        mysql: None,
        sqlite: None,
    },
];

/// A collation the harness declares a text column under.
///
/// The variants are the ones a comparison is answered differently under,
/// which Phase C6 measured: a byte-ordered collation reproduces every
/// operation, a named deterministic one reproduces equality but not
/// ordering, a case-insensitive one folds, and a nondeterministic one
/// reports two spellings of one letter equal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollationVariant {
    /// No `COLLATE` clause: the database default.
    DatabaseDefault,
    /// `C` on PostgreSQL, `BINARY` on SQLite, a `_bin` collation on
    /// MySQL. Byte-ordered, so every operation is reproducible.
    ByteOrdered,
    /// Named and deterministic, whose ordering is the locale's.
    NamedDeterministic,
    /// Case-insensitive, which folds.
    CaseInsensitive,
    /// Nondeterministic, where equality is not byte equality.
    Nondeterministic,
}

impl CollationVariant {
    /// Every variant, exhaustively, so adding one breaks this list rather
    /// than silently going ungenerated.
    pub const ALL: &'static [Self] = &[
        Self::DatabaseDefault,
        Self::ByteOrdered,
        Self::NamedDeterministic,
        Self::CaseInsensitive,
        Self::Nondeterministic,
    ];

    /// The `COLLATE` clause this variant becomes on `engine`, or `None`
    /// where that engine cannot express it.
    ///
    /// SQLite has three built-in collations and no way to declare a
    /// nondeterministic one, and MySQL's case-insensitive collations are
    /// exactly its default family.
    #[must_use]
    pub const fn clause(self, engine: Engine) -> Option<&'static str> {
        match (self, engine) {
            (Self::DatabaseDefault, _) => Some(""),
            (Self::ByteOrdered, Engine::Postgres) => Some(" COLLATE \"C\""),
            (Self::ByteOrdered, Engine::MySql) => Some(" COLLATE utf8mb4_bin"),
            (Self::ByteOrdered, Engine::Sqlite) => Some(" COLLATE BINARY"),
            (Self::NamedDeterministic, Engine::Postgres) => Some(" COLLATE \"ucs_basic\""),
            (Self::NamedDeterministic, Engine::MySql) => Some(" COLLATE utf8mb4_0900_as_cs"),
            (Self::NamedDeterministic, Engine::Sqlite) => Some(" COLLATE RTRIM"),
            (Self::CaseInsensitive, Engine::MySql) => Some(" COLLATE utf8mb4_0900_ai_ci"),
            (Self::CaseInsensitive, Engine::Sqlite) => Some(" COLLATE NOCASE"),
            // One collation answers both on PostgreSQL, and truthfully:
            // the ICU collation declared `deterministic = false` at
            // level 2 is the case-insensitive one, which is why Phase C6
            // measured it reporting two spellings of a letter equal.
            (Self::CaseInsensitive | Self::Nondeterministic, Engine::Postgres) => {
                Some(" COLLATE \"ci\"")
            }
            (Self::Nondeterministic, Engine::MySql | Engine::Sqlite) => None,
        }
    }
}

/// The statement that has to precede the schema for a nondeterministic
/// collation to exist, on the one engine that can declare one.
#[must_use]
pub const fn collation_preamble(engine: Engine) -> &'static str {
    match engine {
        Engine::Postgres => {
            "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', \
             deterministic = false)"
        }
        Engine::MySql | Engine::Sqlite => "",
    }
}

/// The generated schema for `engine`, as the statements that build it.
///
/// A list rather than one string, because PostgreSQL refuses two commands
/// in one prepared statement: measured, a schema needing a `CREATE
/// COLLATION` first answered `cannot insert multiple commands into a
/// prepared statement`. Every column that engine can express, the text
/// ones repeated under each collation it can declare, so one schema
/// carries every comparison the engine answers differently.
#[must_use]
pub fn schema_statements(engine: Engine) -> Vec<String> {
    let preamble = collation_preamble(engine);
    let mut statements = Vec::new();
    if !preamble.is_empty() {
        statements.push(preamble.to_string());
    }
    statements.push(schema_ddl(engine));
    statements
}

/// The same schema as one text, for `ParserDB`, which reads several
/// statements at once and needs the collation declaration to classify the
/// columns that name it.
#[must_use]
pub fn schema_catalog_ddl(engine: Engine) -> String {
    schema_statements(engine).join(";\n")
}

/// Just the `CREATE TABLE`.
#[must_use]
pub fn schema_ddl(engine: Engine) -> String {
    let mut columns = Vec::new();
    columns.push("id INT PRIMARY KEY".to_string());
    for column in COLUMNS {
        let Some(declared) = column.declared(engine) else {
            continue;
        };
        if column.kind == BuiltinKind::String {
            for variant in CollationVariant::ALL {
                let Some(clause) = variant.clause(engine) else {
                    continue;
                };
                columns.push(format!(
                    "{}_{} {declared}{clause}",
                    column.name,
                    collation_suffix(*variant)
                ));
            }
        } else {
            columns.push(format!("{} {declared}", column.name));
        }
    }
    format!("CREATE TABLE t ({})", columns.join(", "))
}

/// A column-name suffix per collation variant, so one text type can appear
/// under several collations in one table.
const fn collation_suffix(variant: CollationVariant) -> &'static str {
    match variant {
        CollationVariant::DatabaseDefault => "default",
        CollationVariant::ByteOrdered => "bytes",
        CollationVariant::NamedDeterministic => "named",
        CollationVariant::CaseInsensitive => "nocase",
        CollationVariant::Nondeterministic => "nondet",
    }
}

/// One generated cell: what it is as SQL, and what it is as a subql value
/// where the carrier is one the shipped backends share.
///
/// The temporal and UUID kinds carry `chrono` and `uuid` types, which the
/// wire decoders produce and this generator does not: Phase E3's outer
/// layer reads those off the replication stream, which is the only place
/// they are decoded at all.
#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(BigDecimal),
    Text(String),
    Bytes(Vec<u8>),
}

impl Cell {
    /// This cell as a SQL literal, for the statement the oracle runs.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(flag) => flag.to_string(),
            Self::Int(value) => value.to_string(),
            // A non-finite float has no portable literal, so it is
            // spelled the way each engine accepts one: measured in Phase
            // D3, MySQL refuses the value outright, which is a verdict of
            // its own rather than a case to skip.
            Self::Float(value) if value.is_nan() => "'NaN'".to_string(),
            Self::Float(value) if value.is_infinite() && *value > 0.0 => "'Infinity'".to_string(),
            Self::Float(value) if value.is_infinite() => "'-Infinity'".to_string(),
            Self::Float(value) => format!("{value:?}"),
            Self::Decimal(value) => value.to_string(),
            Self::Text(text) => format!("'{}'", text.replace('\'', "''")),
            Self::Bytes(bytes) => format!("'{}'", hex(bytes)),
        }
    }

    /// This cell as a subql value, for the inner layer's hand-built event.
    #[must_use]
    pub fn value<B>(&self) -> Value<B>
    where
        B: Backend<Bool = bool, Int = i64, Float = f64, Decimal = BigDecimal, String = String>,
        B::Bytes: From<Vec<u8>>,
    {
        match self {
            Self::Null => Value::Null,
            Self::Bool(flag) => Value::Bool(*flag),
            Self::Int(value) => Value::Int(*value),
            Self::Float(value) => Value::Float(*value),
            Self::Decimal(value) => Value::Decimal(value.clone()),
            Self::Text(text) => Value::String(text.clone()),
            Self::Bytes(bytes) => Value::Bytes(bytes.clone().into()),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use core::fmt::Write as _;

    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// One generated row: the columns it fills and the cells it fills them
/// with.
///
/// Only the columns whose carrier this generator builds, which is why the
/// row is a column list rather than a positional tuple: the temporal and
/// UUID columns exist in the schema and stay `NULL` here, and Phase E3
/// reads their values off the wire instead.
#[derive(Clone, Debug)]
pub struct Row {
    pub cells: Vec<(&'static str, Cell)>,
}

impl Row {
    /// The statement that puts this row in the table.
    #[must_use]
    pub fn insert_sql(&self) -> String {
        let mut columns = vec!["id".to_string()];
        let mut values = vec!["1".to_string()];
        for (name, cell) in &self.cells {
            columns.push((*name).to_string());
            values.push(cell.sql());
        }
        format!(
            "INSERT INTO t ({}) VALUES ({})",
            columns.join(", "),
            values.join(", ")
        )
    }
}

/// A row over every column of `engine`'s schema whose kind this generator
/// builds cells for, including the text columns under each collation.
pub fn row_strategy(engine: Engine) -> impl Strategy<Value = Row> {
    let mut cells: Vec<BoxedStrategy<(&'static str, Cell)>> = Vec::new();
    for column in COLUMNS {
        if column.declared(engine).is_none() {
            continue;
        }
        let Some(strategy) = cell_strategy(column.kind) else {
            continue;
        };
        if column.kind == BuiltinKind::String {
            for variant in CollationVariant::ALL {
                if variant.clause(engine).is_none() {
                    continue;
                }
                let name: &'static str = collated_name(column.name, *variant);
                cells.push(
                    cell_strategy(column.kind)
                        .expect("text has a cell strategy")
                        .prop_map(move |cell| (name, cell))
                        .boxed(),
                );
            }
        } else {
            let name = column.name;
            cells.push(strategy.prop_map(move |cell| (name, cell)).boxed());
        }
    }
    cells.prop_map(|cells| Row { cells })
}

/// The generated name of one text column under one collation, which has to
/// match [`schema_ddl`]'s spelling exactly.
fn collated_name(column: &'static str, variant: CollationVariant) -> &'static str {
    // Leaked rather than formatted per case: the set is fixed by the
    // catalogue and the variants, so this is a handful of strings for the
    // life of the process, and a `&'static str` is what a column name is
    // everywhere else in the harness.
    Box::leak(format!("{column}_{}", collation_suffix(variant)).into_boxed_str())
}

/// Integer values astride the two boundaries that were measured wrong:
/// `2^53`, where `f64` stops being exact, and `i64::MAX`, where an
/// addition overflows.
pub fn integer_strategy() -> impl Strategy<Value = Cell> {
    prop_oneof![
        Just(Cell::Int(9_007_199_254_740_992)),
        Just(Cell::Int(9_007_199_254_740_993)),
        Just(Cell::Int(i64::MAX)),
        Just(Cell::Int(i64::MIN)),
        Just(Cell::Int(0)),
        Just(Cell::Int(-1)),
        Just(Cell::Null),
        any::<i64>().prop_map(Cell::Int),
    ]
}

/// Floating values including the non-finite ones, which Phase D3 measured
/// the engines folding and answering with.
pub fn float_strategy() -> impl Strategy<Value = Cell> {
    prop_oneof![
        Just(Cell::Float(f64::NAN)),
        Just(Cell::Float(f64::INFINITY)),
        Just(Cell::Float(f64::NEG_INFINITY)),
        Just(Cell::Float(0.1)),
        Just(Cell::Float(0.0)),
        Just(Cell::Null),
        any::<f64>().prop_map(Cell::Float),
    ]
}

/// Exact decimals, including one past what `f64` can hold and one whose
/// scale a sum has to keep.
pub fn decimal_strategy() -> impl Strategy<Value = Cell> {
    prop_oneof![
        Just(Cell::Decimal(
            BigDecimal::from_str("9007199254740993.25").unwrap()
        )),
        Just(Cell::Decimal(BigDecimal::from_str("0.10").unwrap())),
        Just(Cell::Decimal(BigDecimal::from_str("0").unwrap())),
        Just(Cell::Null),
        any::<i64>().prop_map(|value| Cell::Decimal(BigDecimal::from(value))),
    ]
}

/// Text values including the ones a comparison reads differently: trailing
/// spaces, which a `char(n)` column pads, mixed case, which a collation
/// may fold, and a backslash, which a `LIKE` pattern may escape.
pub fn text_strategy() -> impl Strategy<Value = Cell> {
    prop_oneof![
        Just(Cell::Text("ab   ".to_string())),
        Just(Cell::Text("ab".to_string())),
        Just(Cell::Text("AB".to_string())),
        Just(Cell::Text(r"a\b".to_string())),
        Just(Cell::Text("a%b".to_string())),
        Just(Cell::Text(String::new())),
        Just(Cell::Null),
        "[a-zA-Z ]{0,5}".prop_map(Cell::Text),
    ]
}

/// A cell for a column of `kind`, or `None` for a kind whose carrier this
/// generator does not build.
pub fn cell_strategy(kind: BuiltinKind) -> Option<BoxedStrategy<Cell>> {
    match kind {
        BuiltinKind::Bool => Some(
            prop_oneof![
                Just(Cell::Bool(true)),
                Just(Cell::Bool(false)),
                Just(Cell::Null)
            ]
            .boxed(),
        ),
        BuiltinKind::Int => Some(integer_strategy().boxed()),
        BuiltinKind::Float => Some(float_strategy().boxed()),
        BuiltinKind::Decimal => Some(decimal_strategy().boxed()),
        BuiltinKind::String => Some(text_strategy().boxed()),
        BuiltinKind::Bytes => Some(
            prop_oneof![
                Just(Cell::Bytes(vec![0x00, 0xff])),
                Just(Cell::Bytes(Vec::new())),
                Just(Cell::Null),
            ]
            .boxed(),
        ),
        BuiltinKind::Uuid
        | BuiltinKind::Date
        | BuiltinKind::Time
        | BuiltinKind::Timestamp
        | BuiltinKind::TimestampTz
        | BuiltinKind::Json
        | BuiltinKind::Jsonb => None,
    }
}

/// A predicate over the generated schema, in the forms each corrected
/// phase turned out to be wrong about.
///
/// Well typed by construction: every comparison names a column and a
/// literal of that column's own family, or two columns, which is legal SQL
/// on all three engines even when their answers differ. A refusal from
/// registration is therefore subql's classification and not a generator
/// bug, which is the property `predicate_generator_only_emits_well_typed_sql`
/// holds in place.
pub fn predicate_strategy(engine: Engine) -> impl Strategy<Value = String> {
    let mut forms: Vec<String> = vec![
        // C7: two columns of different kinds.
        "narrow = wide".to_string(),
        "single = twice".to_string(),
        // C11: one float width against the other.
        "single + single + single > 0.30000000447034836".to_string(),
        // C8 and C9: arithmetic that overflows, and a zero divisor.
        "wide + wide > 0".to_string(),
        "narrow / wide > 3".to_string(),
        "narrow % wide = 0".to_string(),
        // C12: integer division.
        "narrow / 2 > 3".to_string(),
        // C2: boolean ordering.
        "flag < true".to_string(),
        "flag = true".to_string(),
        // C1: NaN, which orders above every number on PostgreSQL.
        "twice > 1e308".to_string(),
        "twice = twice".to_string(),
        // D1b: a decimal comparison that a double cannot hold.
        "unbounded_bytes = 'ab'".to_string(),
        // C4: a backslash in a pattern, and an explicit escape.
        r"unbounded_bytes LIKE 'a\%b'".to_string(),
        r"unbounded_bytes LIKE 'a!%b' ESCAPE '!'".to_string(),
        // C5: a padded column, whose trailing spaces a pattern reads.
        "padded_bytes LIKE 'ab'".to_string(),
        "padded_bytes = 'ab'".to_string(),
        // NULL inside an IN list, which is unknown rather than false.
        "narrow IN (1, NULL)".to_string(),
        // C6: the collations, whose comparisons are answered differently.
        "unbounded_nocase = 'AB'".to_string(),
        "unbounded_nocase < 'AB'".to_string(),
        "unbounded_named < 'AB'".to_string(),
        "unbounded_default < 'AB'".to_string(),
    ];
    if engine == Engine::Postgres {
        // C3: `jsonb` ordering, and D5: a case-insensitive pattern, both
        // of which only this engine has.
        forms.push("binary_document = '{}'".to_string());
        forms.push("unbounded_nondet = 'AB'".to_string());
        forms.push("unbounded_bytes ILIKE 'ab'".to_string());
        forms.push("exact / 3 > 0.33333333333333333333".to_string());
    }
    if engine == Engine::MySql {
        forms.push("exact / 3 > 0.333333333".to_string());
    }
    proptest::sample::select(forms)
}

#[cfg(test)]
mod tests {
    use super::{
        cell_strategy, predicate_strategy, schema_catalog_ddl, schema_statements, CollationVariant,
        COLUMNS,
    };
    use crate::differential::oracle::Engine;
    use proptest::prelude::*;
    use sql_traits::structs::ParserDB;
    use subql::backend::{Backend, BuiltinKind, MySql, Postgres, SQLite};
    use subql::compiler::SqlLiteralParse;
    use subql::testing::TestEvent;
    use subql::{DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};

    /// Every builtin kind is generated, checked exhaustively so that a new
    /// kind fails to compile here rather than going quietly ungenerated.
    #[test]
    fn schema_strategy_covers_every_builtin_kind() {
        for kind in [
            BuiltinKind::Bool,
            BuiltinKind::Int,
            BuiltinKind::Float,
            BuiltinKind::Decimal,
            BuiltinKind::String,
            BuiltinKind::Bytes,
            BuiltinKind::Uuid,
            BuiltinKind::Date,
            BuiltinKind::Time,
            BuiltinKind::Timestamp,
            BuiltinKind::TimestampTz,
            BuiltinKind::Json,
            BuiltinKind::Jsonb,
        ] {
            assert!(
                COLUMNS.iter().any(|column| column.kind == kind),
                "no column generates {kind:?}, so no case can reach it"
            );
            // The exhaustive arm: a kind added upstream lands here as a
            // non-exhaustive match rather than as silent absence.
            match kind {
                BuiltinKind::Bool
                | BuiltinKind::Int
                | BuiltinKind::Float
                | BuiltinKind::Decimal
                | BuiltinKind::String
                | BuiltinKind::Bytes
                | BuiltinKind::Uuid
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Json
                | BuiltinKind::Jsonb => {}
            }
        }
        assert!(
            COLUMNS
                .iter()
                .all(|column| column.postgres.is_some() || column.kind == BuiltinKind::Bool),
            "PostgreSQL spells every kind, so a missing spelling is a gap rather than a fact"
        );
    }

    /// The three declared text types a comparison reads differently, all
    /// present in the schema PostgreSQL runs.
    #[test]
    fn schema_strategy_covers_the_comparison_relevant_declared_types() {
        let ddl = schema_catalog_ddl(Engine::Postgres);
        for declared in ["CHAR(5)", "VARCHAR(16)", "TEXT"] {
            assert!(
                ddl.contains(declared),
                "`{declared}` is missing from the generated schema: {ddl}"
            );
        }
        // And the catalog agrees about which one pads, which is the fact
        // Phase C5 turned on.
        let database = ParserDB::parse::<sqlparser::dialect::PostgreSqlDialect>(&ddl)
            .expect("the generated DDL parses");
        let table = subql::catalog_helpers::table_id(&database, "t").expect("t is cataloged");
        let padded = subql::catalog_helpers::column_id(&database, table, "padded_bytes")
            .expect("the padded column is cataloged");
        let comparison =
            subql::catalog_helpers::column_comparison::<Postgres, _>(&database, table, padded)
                .expect("its comparison facts resolve");
        assert!(
            comparison
                .kind
                .builtin()
                .is_some_and(subql::backend::BuiltinType::is_fixed_width_text),
            "a `char(n)` column has to be generated as fixed width, got {:?}",
            comparison.kind
        );
    }

    /// Every collation variant is generated on every engine that can
    /// express it, and the ones that cannot are named rather than
    /// forgotten.
    #[test]
    fn schema_strategy_covers_every_group_key_collation_variant() {
        for variant in CollationVariant::ALL {
            let expressible = [Engine::Postgres, Engine::MySql, Engine::Sqlite]
                .into_iter()
                .filter(|engine| variant.clause(*engine).is_some())
                .collect::<Vec<_>>();
            assert!(
                !expressible.is_empty(),
                "{variant:?} is generated nowhere, so nothing exercises it"
            );
            for engine in expressible {
                let ddl = schema_catalog_ddl(engine);
                let clause = variant.clause(engine).expect("just filtered");
                assert!(
                    clause.is_empty() || ddl.contains(clause.trim()),
                    "{variant:?} is missing from {engine:?}'s schema: {ddl}"
                );
            }
        }
        assert!(
            CollationVariant::Nondeterministic
                .clause(Engine::Sqlite)
                .is_none(),
            "SQLite cannot declare a nondeterministic collation, which is a fact about it"
        );
    }

    /// Every generated predicate is legal SQL over the generated schema,
    /// so a registration refusal is subql's classification rather than a
    /// generator bug.
    ///
    /// A refusal is allowed and expected: measured across Parts II and
    /// III, a nondeterministic collation, a `jsonb` ordering and a MySQL
    /// `ILIKE` are all refused on purpose. A type error is not, because
    /// it would mean the generator emitted nonsense and the harness would
    /// be reporting on itself.
    fn assert_well_typed<B, D>(engine: Engine, dialect: &D)
    where
        B: Backend<Dialect = D> + SqlLiteralParse + 'static,
        D: sqlparser::dialect::Dialect + Default + Clone,
    {
        let ddl = schema_catalog_ddl(engine);
        let database = ParserDB::parse::<D>(&ddl).expect("the generated DDL parses");
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        });
        runner
            .run(&predicate_strategy(engine), |predicate| {
                let mut subscriptions: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
                    SubscriptionEngine::new(database.clone(), dialect.clone());
                match subscriptions.register(SubscriptionRequest::new(
                    1u64,
                    format!("SELECT * FROM t WHERE {predicate}"),
                )) {
                    // Served, or classified and routed to a read: both
                    // are subql's answer about a legal statement, which
                    // is what this property is about.
                    Ok(_) | Err(RegisterError::NotServedInProcess(_)) => Ok(()),
                    Err(other) => Err(TestCaseError::fail(format!(
                        "`{predicate}` is not well typed for {engine:?}: {other:?}"
                    ))),
                }
            })
            .expect("every generated predicate is well typed");
    }

    #[test]
    fn predicate_generator_only_emits_well_typed_sql() {
        assert_well_typed::<Postgres, _>(
            Engine::Postgres,
            &sqlparser::dialect::PostgreSqlDialect {},
        );
        assert_well_typed::<MySql, _>(Engine::MySql, &sqlparser::dialect::MySqlDialect {});
        assert_well_typed::<SQLite, _>(Engine::Sqlite, &sqlparser::dialect::SQLiteDialect {});
    }

    /// The generated schema is not merely parseable, it creates.
    ///
    /// A schema that `ParserDB` accepts and the server refuses would make
    /// every case on that engine a generator failure reported as a
    /// divergence, so each engine is asked to build one for real.
    fn assert_schema_creates<O: crate::differential::oracle::Oracle>(
        oracle: &mut O,
        engine: Engine,
    ) {
        use crate::differential::oracle::OracleVerdict;

        let statements = schema_statements(engine);
        let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();
        let verdict = oracle.answer_case(&borrowed, "INSERT INTO t (id) VALUES (1)", "id = 1");
        assert_eq!(
            verdict,
            OracleVerdict::Answered(subql::compiler::Tri::True),
            "{engine:?} could not build the generated schema: {statements:?}"
        );
    }

    #[test]
    fn the_generated_schema_creates_on_sqlite() {
        let mut oracle = crate::differential::oracle::SqliteOracle::open();
        assert_schema_creates(&mut oracle, Engine::Sqlite);
    }

    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn the_generated_schema_creates_on_postgres() {
        let container = crate::common::pg_with_wal2json();
        let port = crate::common::pg_port(&container);
        let mut oracle = crate::differential::oracle::PgOracle {
            connection: crate::common::pg_connect(port),
        };
        assert_schema_creates(&mut oracle, Engine::Postgres);
    }

    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn the_generated_schema_creates_on_mysql() {
        let container = crate::common::mysql_8();
        let port = crate::common::mysql_port(&container);
        let mut oracle = crate::differential::oracle::MySqlOracle {
            connection: crate::common::mysql_connect(port),
        };
        assert_schema_creates(&mut oracle, Engine::MySql);
    }

    /// A generated row is a statement the engine understands.
    ///
    /// Not "the engine accepts it": a refusal can be the engine's own
    /// verdict on the value, which is a case rather than a bug. Measured
    /// in Phase D3, MySQL refuses a non-finite double outright. What must
    /// never happen is a syntax error, which would mean the generator
    /// emitted something no engine can read, and that is exactly the bug
    /// this found: `double` is reserved on MySQL, so the whole schema was
    /// rejected until the column was renamed.
    fn assert_rows_are_readable<O: crate::differential::oracle::Oracle>(
        oracle: &mut O,
        engine: Engine,
    ) {
        use crate::differential::oracle::OracleVerdict;

        let statements = schema_statements(engine);
        let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 32,
            ..ProptestConfig::default()
        });
        // proptest hands the case closure out as `Fn`, so the connection
        // is shared through a cell rather than borrowed mutably.
        let oracle = core::cell::RefCell::new(oracle);
        runner
            .run(&super::row_strategy(engine), |row| {
                let insert = row.insert_sql();
                let verdict = oracle.borrow_mut().answer_case(&borrowed, &insert, "id = 1");
                if let OracleVerdict::Refused(message) = &verdict {
                    prop_assert!(
                        !message.to_lowercase().contains("syntax"),
                        "the generator emitted something {engine:?} cannot read: {message}\n{insert}"
                    );
                }
                Ok(())
            })
            .expect("every generated row is a statement the engine understands");
    }

    #[test]
    fn generated_rows_are_readable_by_sqlite() {
        let mut oracle = crate::differential::oracle::SqliteOracle::open();
        assert_rows_are_readable(&mut oracle, Engine::Sqlite);
    }

    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn generated_rows_are_readable_by_postgres() {
        let container = crate::common::pg_with_wal2json();
        let port = crate::common::pg_port(&container);
        let mut oracle = crate::differential::oracle::PgOracle {
            connection: crate::common::pg_connect(port),
        };
        assert_rows_are_readable(&mut oracle, Engine::Postgres);
    }

    #[test]
    #[ignore = "requires Docker; run with --ignored"]
    fn generated_rows_are_readable_by_mysql() {
        let container = crate::common::mysql_8();
        let port = crate::common::mysql_port(&container);
        let mut oracle = crate::differential::oracle::MySqlOracle {
            connection: crate::common::mysql_connect(port),
        };
        assert_rows_are_readable(&mut oracle, Engine::MySql);
    }

    /// A generated cell becomes the subql value that carries the same
    /// number, which is what the inner layer folds.
    #[test]
    fn a_generated_cell_becomes_the_matching_value() {
        use subql::backend::Value;

        assert_eq!(
            super::Cell::Int(9_007_199_254_740_993).value::<Postgres>(),
            Value::Int(9_007_199_254_740_993)
        );
        assert_eq!(
            super::Cell::Text("ab   ".to_string()).value::<Postgres>(),
            Value::String("ab   ".to_string())
        );
        assert_eq!(
            super::Cell::Null.value::<Postgres>(),
            Value::<Postgres>::Null
        );
        let nan = super::Cell::Float(f64::NAN).value::<Postgres>();
        assert!(
            matches!(nan, Value::Float(value) if value.is_nan()),
            "a NaN cell stays a NaN, which Phase D3 measured the engines folding"
        );
        assert_eq!(super::Cell::Float(f64::INFINITY).sql(), "'Infinity'");
    }

    /// A cell is generated for every kind whose carrier the shipped
    /// backends share, and the kinds without one are named rather than
    /// missing by accident.
    #[test]
    fn a_cell_is_generated_for_every_standard_carrier() {
        for kind in [
            BuiltinKind::Bool,
            BuiltinKind::Int,
            BuiltinKind::Float,
            BuiltinKind::Decimal,
            BuiltinKind::String,
            BuiltinKind::Bytes,
        ] {
            assert!(
                cell_strategy(kind).is_some(),
                "{kind:?} has a shared carrier, so a cell has to be generated for it"
            );
        }
        for kind in [
            BuiltinKind::Uuid,
            BuiltinKind::Date,
            BuiltinKind::Json,
            BuiltinKind::Jsonb,
        ] {
            assert!(
                cell_strategy(kind).is_none(),
                "{kind:?} is decoded off the wire by Phase E3, not built here"
            );
        }
    }
}
