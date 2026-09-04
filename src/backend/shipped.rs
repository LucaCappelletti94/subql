//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::scalar_value::{
    decode_exact_group_value, encode_mysql_component, encode_postgres_component,
    encode_sqlite_component, mysql_binary_text_rule, postgres_reproduces, single_column_rule,
    sqlite_text_rule, widen_i64_to_f64,
};
use super::{
    Backend, BuiltinKind, ColumnComparisonOf, GroupKeyEncoder, NoCustomScalars, NumericWidening,
    ScalarKindOf, SqliteJson, TextOperation, TextRule, Value,
};
use alloc::string::ToString;

/// The `text` row is not an inconsistency: converting `char` to `text`
/// strips the padding, and `text` comparison is then exact. So a `char`
/// against `text` answers `false` for `ab` versus `ab   `, while the same
/// pair as `char` against `varchar` answers `true`.
fn postgres_trailing_spaces<V: postgres_jsonb_canonical::PgVersion + 'static>(
    comparison: &super::scalar_value::ComparisonContext<'_, Postgres<V>>,
) -> super::TrailingSpaces {
    use super::TrailingSpaces;

    let declares_char = |side: Option<&ColumnComparisonOf<Postgres<V>>>| {
        side.is_some_and(ColumnComparisonOf::<Postgres<V>>::declares_char_type)
    };
    // `text` is the one partner that keeps its own trailing spaces while
    // stripping the `char` side. A literal carries no facts and resolves
    // to the `char` side's type, so it is not this case.
    let declares_text = |side: Option<&ColumnComparisonOf<Postgres<V>>>| {
        side.is_some_and(|facts| facts.declared_type.trim().eq_ignore_ascii_case("text"))
    };

    match (
        declares_char(comparison.left),
        declares_char(comparison.right),
    ) {
        (true, true) => TrailingSpaces::BothIgnored,
        (true, false) if declares_text(comparison.right) => TrailingSpaces::LeftStripped,
        (false, true) if declares_text(comparison.left) => TrailingSpaces::RightStripped,
        (true, false) | (false, true) => TrailingSpaces::BothIgnored,
        (false, false) => TrailingSpaces::BothSignificant,
    }
}

/// Postgres backend marker, parameterised by the server major it targets.
///
/// The default covers the newest supported server. Name another, as `Postgres<Pg14>`, to
/// hold `jsonb` to what an older major accepts. Only acceptance changes, never the bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Postgres<V = postgres_jsonb_canonical::Pg18>(core::marker::PhantomData<V>);

impl<V: postgres_jsonb_canonical::PgVersion + 'static> Backend for Postgres<V> {
    /// Measured: PostgreSQL raises `bigint out of range`.
    fn integer_binary(
        operation: crate::compiler::vm::arithmetic::ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::arithmetic::ArithmeticFailure> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::arithmetic::IntegerOverflow::Fails,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(
        value: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::arithmetic::ArithmeticFailure> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::arithmetic::IntegerOverflow::Fails,
            crate::compiler::vm::arithmetic::ArithmeticOp::Negate,
            value,
            value,
        )
    }

    /// Measured: against a float, both engines cast the other operand to
    /// `double precision`, so two integers `f64` cannot separate compare
    /// equal. Against a decimal an integer compares exactly.
    fn numeric_widening(left: BuiltinKind, right: BuiltinKind) -> Option<NumericWidening> {
        match (left, right) {
            (BuiltinKind::Float, BuiltinKind::Int | BuiltinKind::Decimal)
            | (BuiltinKind::Int | BuiltinKind::Decimal, BuiltinKind::Float) => {
                Some(NumericWidening::AtFloatWidth)
            }
            (BuiltinKind::Int, BuiltinKind::Decimal) | (BuiltinKind::Decimal, BuiltinKind::Int) => {
                Some(NumericWidening::Exact)
            }
            _ => None,
        }
    }

    fn compare_cross_kind_numeric(
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        crate::backend::cross_kind_numeric_ordering(left, right)
    }

    /// Measured on 16.11. Equality under a deterministic collation is byte
    /// equality, including for the database default, since `CREATE
    /// DATABASE` cannot select a nondeterministic collation. Ordering is
    /// the locale's and byte order does not reproduce it, so only `C` and
    /// `POSIX` are served. A `char(n)` column's padding is then handled on
    /// top, per operation: `=` and `<` ignore it, `LIKE` does not.
    fn text_rule(
        comparison: &super::scalar_value::ComparisonContext<'_, Self>,
        operation: crate::backend::TextOperation,
    ) -> Option<crate::backend::TextRule> {
        use crate::backend::{TextOperation, TextRule};

        for side in [comparison.left, comparison.right] {
            let Some(facts) = side else { continue };
            if !postgres_reproduces(facts, operation) {
                return None;
            }
        }
        let rule = TextRule::EXACT;
        Some(match operation {
            // `LIKE` reads the stored value, padding included: measured,
            // a `char(5)` holding `ab` does not match the pattern `ab`.
            TextOperation::Pattern => rule,
            TextOperation::Equality | TextOperation::Ordering => {
                rule.with_spaces(postgres_trailing_spaces(comparison))
            }
        })
    }

    /// Measured: a backslash escapes the next character.
    const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

    type Custom = NoCustomScalars<Self>;

    /// PostgreSQL's own float rule: NaN equals NaN. IEEE, which is what
    /// `PartialOrd` on `f64` implements, says a NaN equals nothing, so
    /// `WHERE value = value` skipped the row the server returns.
    ///
    /// Only the float variants differ. `numeric` also has a NaN in the
    /// server, but [`Backend::Decimal`] is a `BigDecimal`, which cannot
    /// represent one, so no such value reaches here.
    fn scalars_equal(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> bool {
        match (left, right) {
            (Value::Float(x), Value::Float(y)) if x.is_nan() || y.is_nan() => {
                x.is_nan() && y.is_nan()
            }
            (Value::String(x), Value::String(y)) => {
                Self::text_rule(&comparison, TextOperation::Equality)
                    .unwrap_or(TextRule::EXACT)
                    .equal(x, y)
            }
            _ => crate::compiler::value_cmp::structural_equality(left, right),
        }
    }

    /// NaN is PostgreSQL's largest float: above every non-NaN value, and
    /// equal to another NaN. IEEE leaves every such pair unordered, which
    /// answered `Tri::Unknown` and dropped the row.
    fn compare_scalars(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        match (left, right) {
            (Value::Float(x), Value::Float(y)) if x.is_nan() || y.is_nan() => {
                Some(if x.is_nan() && y.is_nan() {
                    core::cmp::Ordering::Equal
                } else if x.is_nan() {
                    core::cmp::Ordering::Greater
                } else {
                    core::cmp::Ordering::Less
                })
            }
            (Value::String(x), Value::String(y)) => Some(
                Self::text_rule(&comparison, TextOperation::Ordering)
                    .unwrap_or(TextRule::EXACT)
                    .compare(x, y),
            ),
            _ => crate::compiler::value_cmp::structural_ordering(left, right),
        }
    }

    fn group_key_encoder(
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind.as_builtin() {
            Some(
                BuiltinKind::Int
                | BuiltinKind::Bool
                | BuiltinKind::Bytes
                | BuiltinKind::Uuid
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Float
                | BuiltinKind::Jsonb,
            ) => true,
            Some(BuiltinKind::String) => single_column_rule::<Self>(column).is_some(),
            // PostgreSQL numeric waits on Diesel #5168 for infinity support.
            Some(BuiltinKind::Decimal | BuiltinKind::Json) | None => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_postgres_component))
    }

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind.as_builtin(), value) {
            (Some(BuiltinKind::Float), Value::Int(value)) => {
                Some(Value::Float(widen_i64_to_f64(value)))
            }
            (Some(BuiltinKind::Float), Value::Decimal(value)) => {
                value.to_string().parse().ok().map(Value::Float)
            }
            (_, value) => (!value.is_missing()).then_some(value),
        }
    }
    type Dialect = sqlparser::dialect::PostgreSqlDialect;
    type Bool = bool;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    type Uuid = uuid::Uuid;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    type Jsonb = serde_json::Value;
    type JsonbVersion = V;
}

/// MySQL backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MySql;

impl Backend for MySql {
    /// Measured: MySQL raises `BIGINT value is out of range`.
    fn integer_binary(
        operation: crate::compiler::vm::arithmetic::ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::arithmetic::ArithmeticFailure> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::arithmetic::IntegerOverflow::Fails,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(
        value: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::arithmetic::ArithmeticFailure> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::arithmetic::IntegerOverflow::Fails,
            crate::compiler::vm::arithmetic::ArithmeticOp::Negate,
            value,
            value,
        )
    }

    /// Measured: against a float, both engines cast the other operand to
    /// `double precision`, so two integers `f64` cannot separate compare
    /// equal. Against a decimal an integer compares exactly.
    fn numeric_widening(left: BuiltinKind, right: BuiltinKind) -> Option<NumericWidening> {
        match (left, right) {
            (BuiltinKind::Float, BuiltinKind::Int | BuiltinKind::Decimal)
            | (BuiltinKind::Int | BuiltinKind::Decimal, BuiltinKind::Float) => {
                Some(NumericWidening::AtFloatWidth)
            }
            (BuiltinKind::Int, BuiltinKind::Decimal) | (BuiltinKind::Decimal, BuiltinKind::Int) => {
                Some(NumericWidening::Exact)
            }
            _ => None,
        }
    }

    fn compare_cross_kind_numeric(
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        crate::backend::cross_kind_numeric_ordering(left, right)
    }

    /// A `PAD SPACE` collation ignores trailing spaces on both sides, which
    /// no structural comparison reproduces.
    fn scalars_equal(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> bool {
        match (left, right) {
            (Value::String(x), Value::String(y)) => {
                Self::text_rule(&comparison, TextOperation::Equality)
                    .unwrap_or(TextRule::EXACT)
                    .equal(x, y)
            }
            _ => crate::compiler::value_cmp::structural_equality(left, right),
        }
    }

    /// Ordering reads trailing spaces the same way equality does.
    fn compare_scalars(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        match (left, right) {
            (Value::String(x), Value::String(y)) => Some(
                Self::text_rule(&comparison, TextOperation::Ordering)
                    .unwrap_or(TextRule::EXACT)
                    .compare(x, y),
            ),
            _ => crate::compiler::value_cmp::structural_ordering(left, right),
        }
    }

    /// Measured on 8.4.11: only the binary collations are reproducible.
    /// The server default folds case and accents, and a case-sensitive UCA
    /// collation is not byte-exact either, since `utf8mb4_0900_as_cs`
    /// reports the NFC and NFD spellings of one letter equal where
    /// `utf8mb4_bin` reports them different. Every other collation, and an
    /// unnamed database default, is a database read.
    fn text_rule(
        comparison: &super::scalar_value::ComparisonContext<'_, Self>,
        _operation: crate::backend::TextOperation,
    ) -> Option<crate::backend::TextRule> {
        use crate::backend::TextRule;

        for side in [comparison.left, comparison.right] {
            let Some(facts) = side else { continue };
            mysql_binary_text_rule(facts)?;
        }
        // Both sides agree, so either resolves the pair; a literal side
        // takes the column's collation.
        comparison
            .left
            .or(comparison.right)
            .and_then(mysql_binary_text_rule)
            .or(Some(TextRule::EXACT))
    }

    /// Measured: a backslash escapes the next character.
    const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

    type Custom = NoCustomScalars<Self>;

    fn group_key_encoder(
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind.as_builtin() {
            Some(
                BuiltinKind::Int
                | BuiltinKind::Bool
                | BuiltinKind::Bytes
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Decimal,
            ) => true,
            Some(BuiltinKind::String | BuiltinKind::Uuid) => {
                single_column_rule::<Self>(column).is_some()
            }
            // MySQL 8.0 groups persisted signed zero into two groups.
            Some(BuiltinKind::Float | BuiltinKind::Json | BuiltinKind::Jsonb) | None => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_mysql_component))
    }

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind.as_builtin(), value) {
            (Some(BuiltinKind::Float), Value::Int(value)) => {
                Some(Value::Float(widen_i64_to_f64(value)))
            }
            (Some(BuiltinKind::Float), Value::Decimal(value)) => {
                value.to_string().parse().ok().map(Value::Float)
            }
            (_, value) => (!value.is_missing()).then_some(value),
        }
    }
    type Dialect = sqlparser::dialect::MySqlDialect;
    type Bool = bool;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    // MySQL stores UUIDs as CHAR(36) or BINARY(16) with no native type.
    // Downstream code treats them as strings on the wire.
    type Uuid = alloc::string::String;
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = serde_json::Value;
    // MySQL does not distinguish JSON from JSONB. Keep the type alias for
    // symmetry with Postgres so the engine surface stays uniform.
    type Jsonb = serde_json::Value;
    // Named because associated type defaults are unstable; MySQL and SQLite JSON
    // semantics never route through the PostgreSQL crate.
    type JsonbVersion = postgres_jsonb_canonical::Pg18;
}

/// SQLite backend marker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SQLite;

impl Backend for SQLite {
    /// Measured: SQLite carries the overflowed result as a real.
    fn integer_binary(
        operation: crate::compiler::vm::arithmetic::ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::arithmetic::ArithmeticFailure> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::arithmetic::IntegerOverflow::PromotesToFloat,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(
        value: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::arithmetic::ArithmeticFailure> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::arithmetic::IntegerOverflow::PromotesToFloat,
            crate::compiler::vm::arithmetic::ArithmeticOp::Negate,
            value,
            value,
        )
    }

    /// Measured: SQLite compares an integer against a real without
    /// rounding either, so the pair is exact. It has no decimal type.
    fn numeric_widening(left: BuiltinKind, right: BuiltinKind) -> Option<NumericWidening> {
        match (left, right) {
            (BuiltinKind::Int, BuiltinKind::Float) | (BuiltinKind::Float, BuiltinKind::Int) => {
                Some(NumericWidening::Exact)
            }
            _ => None,
        }
    }

    fn compare_cross_kind_numeric(
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        crate::backend::cross_kind_numeric_ordering(left, right)
    }

    /// `NOCASE` folds ASCII case and `RTRIM` ignores trailing spaces, and
    /// no structural comparison reproduces either.
    fn scalars_equal(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> bool {
        match (left, right) {
            (Value::String(x), Value::String(y)) => {
                Self::text_rule(&comparison, TextOperation::Equality)
                    .unwrap_or(TextRule::EXACT)
                    .equal(x, y)
            }
            _ => crate::compiler::value_cmp::structural_equality(left, right),
        }
    }

    /// Ordering reads the collation the same way equality does.
    fn compare_scalars(
        comparison: super::scalar_value::ComparisonContext<'_, Self>,
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        match (left, right) {
            (Value::String(x), Value::String(y)) => Some(
                Self::text_rule(&comparison, TextOperation::Ordering)
                    .unwrap_or(TextRule::EXACT)
                    .compare(x, y),
            ),
            _ => crate::compiler::value_cmp::structural_ordering(left, right),
        }
    }

    /// SQLite's three built-in collations are all exactly reproducible,
    /// and the same rule serves every operation: `BINARY` compares bytes,
    /// `NOCASE` folds ASCII case only, measured as leaving a ligature
    /// alone, and `RTRIM` ignores trailing spaces.
    fn text_rule(
        comparison: &super::scalar_value::ComparisonContext<'_, Self>,
        _operation: crate::backend::TextOperation,
    ) -> Option<crate::backend::TextRule> {
        let mut rule = crate::backend::TextRule::EXACT;
        for side in [comparison.left, comparison.right] {
            let Some(facts) = side else { continue };
            rule = sqlite_text_rule(facts)?;
        }
        Some(rule)
    }

    /// Measured: SQLite has no default escape, so a backslash in a
    /// pattern matches a backslash.
    const LIKE_DEFAULT_ESCAPE: Option<char> = None;

    type Custom = NoCustomScalars<Self>;

    fn group_key_encoder(
        columns: alloc::vec::Vec<ColumnComparisonOf<Self>>,
    ) -> Option<GroupKeyEncoder<Self>> {
        let supported = columns.iter().all(|column| match column.kind.as_builtin() {
            Some(
                BuiltinKind::Int
                | BuiltinKind::Bool
                | BuiltinKind::Bytes
                | BuiltinKind::Timestamp
                | BuiltinKind::TimestampTz
                | BuiltinKind::Date
                | BuiltinKind::Time
                | BuiltinKind::Float
                | BuiltinKind::Json
                | BuiltinKind::Jsonb,
            ) => true,
            Some(BuiltinKind::String | BuiltinKind::Uuid) => {
                single_column_rule::<Self>(column).is_some()
            }
            Some(BuiltinKind::Decimal) | None => false,
        });
        supported.then(|| GroupKeyEncoder::new(columns, encode_sqlite_component))
    }
    type Dialect = sqlparser::dialect::SQLiteDialect;
    // SQLite has no native BOOL. The column-type contract stores 0 or 1
    // as INTEGER. The backend surfaces the wire type rather than inventing
    // a `bool`.
    type Bool = i64;
    type Int = i64;
    type Float = f64;
    type String = alloc::string::String;
    type Bytes = alloc::vec::Vec<u8>;
    // SQLite stores UUIDs as TEXT (36-byte hyphenated) by convention.
    type Uuid = alloc::string::String;
    // SQLite has no native temporal types. Downstream code stores dates and
    // times as ISO-8601 TEXT. `Timestamp` and related types carry parsed
    // `chrono` values after decoding.
    type Timestamp = chrono::NaiveDateTime;
    type TimestampTz = chrono::DateTime<chrono::Utc>;
    type Date = chrono::NaiveDate;
    type Time = chrono::NaiveTime;
    type Decimal = bigdecimal::BigDecimal;
    type Json = SqliteJson;

    fn decode_group_value(kind: ScalarKindOf<Self>, value: Value<Self>) -> Option<Value<Self>> {
        match (kind.as_builtin(), value) {
            (Some(BuiltinKind::Bool), Value::Int(value)) => Some(Value::Bool(value)),
            (Some(BuiltinKind::Uuid), Value::String(value)) => Some(Value::Uuid(value)),
            (Some(BuiltinKind::Timestamp), Value::String(value)) => {
                sql_scalar_text::parse_timestamp(&value).map(Value::Timestamp)
            }
            (Some(BuiltinKind::TimestampTz), Value::String(value)) => {
                sql_scalar_text::parse_timestamp_tz(&value).map(Value::TimestampTz)
            }
            (Some(BuiltinKind::Date), Value::String(value)) => {
                sql_scalar_text::parse_date(&value).map(Value::Date)
            }
            (Some(BuiltinKind::Time), Value::String(value)) => {
                sql_scalar_text::parse_time(&value).map(Value::Time)
            }
            (Some(BuiltinKind::Decimal), Value::String(value)) => {
                sql_scalar_text::parse_decimal(&value).map(Value::Decimal)
            }
            (Some(BuiltinKind::Float), Value::Int(value)) => {
                Some(Value::Float(widen_i64_to_f64(value)))
            }
            (Some(BuiltinKind::Decimal), Value::Int(value)) => {
                Some(Value::Decimal(bigdecimal::BigDecimal::from(value)))
            }
            (Some(BuiltinKind::Decimal), Value::Float(value)) => {
                value.to_string().parse().ok().map(Value::Decimal)
            }
            (Some(BuiltinKind::Json), Value::String(value)) => {
                Some(Value::Json(SqliteJson::text(value)))
            }
            (Some(BuiltinKind::Json), Value::Int(value)) => {
                Some(Value::Json(SqliteJson::integer(value)))
            }
            (Some(BuiltinKind::Json), Value::Float(value)) => {
                Some(Value::Json(SqliteJson::real(value)))
            }
            (Some(BuiltinKind::Json), Value::Bytes(value)) => {
                Some(Value::Json(SqliteJson::blob(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::String(value)) => {
                Some(Value::Jsonb(SqliteJson::text(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::Int(value)) => {
                Some(Value::Jsonb(SqliteJson::integer(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::Float(value)) => {
                Some(Value::Jsonb(SqliteJson::real(value)))
            }
            (Some(BuiltinKind::Jsonb), Value::Bytes(value)) => {
                Some(Value::Jsonb(SqliteJson::blob(value)))
            }
            (_, value) => decode_exact_group_value(kind, value),
        }
    }
    type Jsonb = SqliteJson;
    // Named because associated type defaults are unstable; MySQL and SQLite JSON
    // semantics never route through the PostgreSQL crate.
    type JsonbVersion = postgres_jsonb_canonical::Pg18;
}
