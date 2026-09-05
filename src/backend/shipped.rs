//! Split out of the former single `backend.rs`; the module boundary is now real
//! rather than a banner comment.

use super::scalar_value::{
    decode_exact_group_value, encode_mysql_component, encode_postgres_component,
    encode_sqlite_component, mysql_binary_text_rule, postgres_reproduces, single_column_rule,
    sqlite_text_rule, widen_i64_to_f64,
};
use super::{
    Backend, BuiltinKind, ColumnComparisonOf, GroupKeyEncoder, NoCustomScalars, NumericWidening,
    SqliteJson, TextRule, Value,
};
use alloc::string::ToString;

/// How PostgreSQL reads trailing spaces for one text comparison.
///
/// Measured on 16.11, with a `char(5)` column holding `ab` (so stored as
/// `ab   `) and a `varchar`/`text` column holding `ab   `:
///
/// | left   | right             | answer for `=`               |
/// |--------|-------------------|------------------------------|
/// | `char` | `char`            | trailing spaces ignored      |
/// | `char` | literal           | trailing spaces ignored      |
/// | `char` | `varchar`         | trailing spaces ignored      |
/// | `char` | `text`            | `char` side stripped, then exact |
/// | other  | other             | exact                        |
///
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
    fn hold_float_at_single(value: f64) -> f64 {
        super::scalar_value::at_float4(value)
    }

    /// Measured on 16.11: two float4 operands keep a float4 result, while
    /// any other pair promotes. `real * 3` has no operator, so an integer
    /// operand lands in the promoting arm.
    fn float_arithmetic_width(
        left: Option<super::scalar_value::FloatWidth>,
        right: Option<super::scalar_value::FloatWidth>,
    ) -> Option<super::scalar_value::FloatWidth> {
        use super::scalar_value::FloatWidth;

        match (left, right) {
            (Some(FloatWidth::Single), Some(FloatWidth::Single)) => Some(FloatWidth::Single),
            (Some(_), _) | (_, Some(_)) => Some(FloatWidth::Double),
            (None, None) => None,
        }
    }

    /// Measured: `real` and `float4` are float4, `double precision` and
    /// `float8` are float8.
    fn refine_builtin(
        family: super::scalar_value::BuiltinKind,
        declared_type: &str,
    ) -> super::scalar_value::BuiltinType {
        let declared = declared_type.trim();
        let float = if ["real", "float4"]
            .iter()
            .any(|name| declared.eq_ignore_ascii_case(name))
        {
            super::scalar_value::FloatWidth::Single
        } else {
            super::scalar_value::FloatWidth::Double
        };
        super::scalar_value::refined_builtin(
            family,
            super::scalar_value::declares_sixty_four_bit_int(declared),
            float,
            super::scalar_value::declares_fixed_width_text(declared),
        )
    }

    /// Measured: PostgreSQL raises `division by zero`.
    const DIVISION_BY_ZERO: crate::compiler::vm::refusal::DivisionByZero =
        crate::compiler::vm::refusal::DivisionByZero::Fails;

    /// Measured: `avg(int)` of 1 and 2 is `1.5000000000000000`, which is
    /// this engine's own `numeric` division of the total by the count.
    const MEAN: super::scalar_value::MeanRule = super::scalar_value::MeanRule::Exact;

    /// Measured: `sum(smallint)` and `sum(int)` are `bigint`, `sum(bigint)`
    /// and `sum(numeric)` are `numeric`, and `sum(double precision)` is a
    /// double. `numeric` stops at 131072 integer digits.
    fn sum_rule(column: super::scalar_value::BuiltinType) -> super::scalar_value::SumRule {
        use super::scalar_value::{BuiltinType, IntWidth, SumRule};

        match column {
            BuiltinType::Int(IntWidth::UpToThirtyTwo) => SumRule::Integer,
            BuiltinType::Int(IntWidth::SixtyFour) | BuiltinType::Decimal => SumRule::Decimal {
                integer_digits: Some(131_072),
            },
            _ => SumRule::Double,
        }
    }

    /// Measured: `7 / 2` is `3`, and `1::numeric / 3` is
    /// `0.33333333333333333333`, twenty digits and rounded up.
    const DIVISION: super::scalar_value::DivisionRule =
        super::scalar_value::DivisionRule::IntegersTruncate;

    /// The quotient's scale is the engine's, resolved at registration.
    fn decimal_quotient(
        dividend: bigdecimal::BigDecimal,
        divisor: bigdecimal::BigDecimal,
        quotient: crate::compiler::bytecode::Quotient,
    ) -> bigdecimal::BigDecimal {
        match quotient {
            crate::compiler::bytecode::Quotient::FromTheOperands => {
                crate::compiler::vm::arithmetic::quotient_at_significant_digits(&dividend, &divisor)
            }
            crate::compiler::bytecode::Quotient::InWordsAt(increment) => {
                crate::compiler::vm::arithmetic::quotient_in_words(&dividend, &divisor, increment)
            }
        }
    }

    /// Never called: this engine's `/` truncates two integers rather than
    /// answering a decimal, so the integer arm stays on
    /// [`Backend::integer_binary`].
    fn integer_quotient(
        dividend: i64,
        divisor: i64,
        increment: super::scalar_value::DivisionPrecisionIncrement,
    ) -> bigdecimal::BigDecimal {
        crate::compiler::vm::arithmetic::quotient_in_words(
            &bigdecimal::BigDecimal::from(dividend),
            &bigdecimal::BigDecimal::from(divisor),
            increment,
        )
    }

    type Custom = NoCustomScalars<Self>;

    /// Measured: a backslash escapes, and a pattern ending with one is
    /// refused once the matcher reaches it with input still to read.
    const LIKE_ESCAPE: Option<crate::compiler::vm::refusal::LikeEscape> =
        Some(crate::compiler::vm::refusal::LikeEscape {
            character: '\\',
            dangling: crate::compiler::vm::refusal::DanglingEscape::Fails,
        });

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

    /// Measured: PostgreSQL raises `bigint out of range`.
    fn integer_binary(
        operation: crate::compiler::vm::refusal::ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::refusal::IntegerOverflow::Fails,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(
        value: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::refusal::IntegerOverflow::Fails,
            crate::compiler::vm::refusal::ArithmeticOp::Negate,
            value,
            0,
        )
    }

    /// Measured: a float operand puts the comparison at `double precision`
    /// width, and an integer against a decimal is exact.
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
            // The rule was resolved at registration, so no collation is
            // consulted here. Its absence means the comparison came from a
            // path that carries no facts, such as extreme maintenance,
            // where byte comparison is the standing behaviour.
            (Value::String(x), Value::String(y)) => {
                comparison.text.unwrap_or(TextRule::EXACT).equal(x, y)
            }
            _ => crate::compiler::value_cmp::structural_equality(comparison, left, right),
        }
    }

    /// NaN is PostgreSQL's largest float: above every non-NaN value, and
    /// equal to another NaN. IEEE leaves every such pair unordered, which
    /// answered `Tri::Unknown` and dropped the row. Text ordering reads
    /// trailing spaces the same way equality does.
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
            _ => crate::compiler::value_cmp::structural_ordering(comparison, left, right),
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

    fn decode_group_value(
        kind: super::scalar_value::ValueKindOf<Self>,
        value: Value<Self>,
    ) -> Option<Value<Self>> {
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
    fn hold_float_at_single(value: f64) -> f64 {
        super::scalar_value::at_float4(value)
    }

    /// Measured on 8.4.11: `FLOAT + FLOAT + FLOAT` answers the double sum,
    /// so a float4 operand promotes and nothing is held at float4.
    fn float_arithmetic_width(
        left: Option<super::scalar_value::FloatWidth>,
        right: Option<super::scalar_value::FloatWidth>,
    ) -> Option<super::scalar_value::FloatWidth> {
        left.or(right)
            .map(|_| super::scalar_value::FloatWidth::Double)
    }

    /// Measured: MySQL's `FLOAT` is float4 and its `DOUBLE` is float8.
    fn refine_builtin(
        family: super::scalar_value::BuiltinKind,
        declared_type: &str,
    ) -> super::scalar_value::BuiltinType {
        let declared = declared_type.trim();
        let float = if declared.eq_ignore_ascii_case("float") {
            super::scalar_value::FloatWidth::Single
        } else {
            super::scalar_value::FloatWidth::Double
        };
        super::scalar_value::refined_builtin(
            family,
            super::scalar_value::declares_sixty_four_bit_int(declared),
            float,
            super::scalar_value::declares_fixed_width_text(declared),
        )
    }

    type Custom = NoCustomScalars<Self>;

    /// Measured: a backslash escapes, and a pattern ending with one
    /// answers 0 whether or not input remains, so it never raises.
    const LIKE_ESCAPE: Option<crate::compiler::vm::refusal::LikeEscape> =
        Some(crate::compiler::vm::refusal::LikeEscape {
            character: '\\',
            dangling: crate::compiler::vm::refusal::DanglingEscape::NoMatch,
        });

    /// Measured: MySQL answers `NULL` with warning 1365, even with
    /// `ERROR_FOR_DIVISION_BY_ZERO` in `sql_mode`, which raises on writes.
    const DIVISION_BY_ZERO: crate::compiler::vm::refusal::DivisionByZero =
        crate::compiler::vm::refusal::DivisionByZero::IsNull;

    /// Measured: `avg` over 1, 2 and 2 compares as `1.666666666`, which is
    /// this engine's own `/` applied to the total and the count, and it
    /// follows the declared increment exactly as `/` does.
    const MEAN: super::scalar_value::MeanRule = super::scalar_value::MeanRule::Exact;

    /// Measured: every integer width and every decimal sums into a decimal,
    /// `decimal(32,0)` for an `int` column and `decimal(41,0)` for a
    /// `bigint` one, and a floating column sums into a double. No bound is
    /// reachable in a `SELECT`.
    fn sum_rule(column: super::scalar_value::BuiltinType) -> super::scalar_value::SumRule {
        use super::scalar_value::{BuiltinType, SumRule};

        match column {
            BuiltinType::Int(_) | BuiltinType::Decimal => SumRule::Decimal {
                integer_digits: None,
            },
            _ => SumRule::Double,
        }
    }

    /// Measured: `7 / 2` is `3.5000` and `2 / 3` compares as
    /// `0.666666666`, truncated at a nine-digit word. MySQL spells integer
    /// division `DIV`, which is a different operator.
    const DIVISION: super::scalar_value::DivisionRule =
        super::scalar_value::DivisionRule::QuotientsAreDecimalInWords;

    /// The quotient's scale is the engine's, resolved at registration.
    fn decimal_quotient(
        dividend: bigdecimal::BigDecimal,
        divisor: bigdecimal::BigDecimal,
        quotient: crate::compiler::bytecode::Quotient,
    ) -> bigdecimal::BigDecimal {
        match quotient {
            crate::compiler::bytecode::Quotient::FromTheOperands => {
                crate::compiler::vm::arithmetic::quotient_at_significant_digits(&dividend, &divisor)
            }
            crate::compiler::bytecode::Quotient::InWordsAt(increment) => {
                crate::compiler::vm::arithmetic::quotient_in_words(&dividend, &divisor, increment)
            }
        }
    }

    /// Two integers divide to a decimal here, so both widen and take the
    /// same word quantisation any other pair takes.
    fn integer_quotient(
        dividend: i64,
        divisor: i64,
        increment: super::scalar_value::DivisionPrecisionIncrement,
    ) -> bigdecimal::BigDecimal {
        crate::compiler::vm::arithmetic::quotient_in_words(
            &bigdecimal::BigDecimal::from(dividend),
            &bigdecimal::BigDecimal::from(divisor),
            increment,
        )
    }

    /// Measured: MySQL raises `BIGINT value is out of range`. Its unary
    /// minus on the smallest integer promotes past `i64` instead, which is
    /// reported as a failure here rather than answered from a number this
    /// carrier cannot hold.
    fn integer_binary(
        operation: crate::compiler::vm::refusal::ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::refusal::IntegerOverflow::Fails,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(
        value: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::refusal::IntegerOverflow::Fails,
            crate::compiler::vm::refusal::ArithmeticOp::Negate,
            value,
            0,
        )
    }

    /// Measured: a float operand puts the comparison at `double precision`
    /// width, and an integer against a decimal is exact.
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

    fn decode_group_value(
        kind: super::scalar_value::ValueKindOf<Self>,
        value: Value<Self>,
    ) -> Option<Value<Self>> {
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
    fn hold_float_at_single(value: f64) -> f64 {
        super::scalar_value::at_float4(value)
    }

    /// SQLite has one floating type and it is float8, so no result is held
    /// at float4.
    fn float_arithmetic_width(
        left: Option<super::scalar_value::FloatWidth>,
        right: Option<super::scalar_value::FloatWidth>,
    ) -> Option<super::scalar_value::FloatWidth> {
        left.or(right)
            .map(|_| super::scalar_value::FloatWidth::Double)
    }

    /// SQLite has one floating type, `REAL`, and it is float8. Its
    /// `CHAR(n)` is advisory, stored as given, so no text type is fixed
    /// width here.
    fn refine_builtin(
        family: super::scalar_value::BuiltinKind,
        _declared_type: &str,
    ) -> super::scalar_value::BuiltinType {
        // SQLite's one integer is 64 bits wide, whatever a column
        // declares, so every integer column resolves the same way.
        super::scalar_value::refined_builtin(
            family,
            super::scalar_value::IntWidth::SixtyFour,
            super::scalar_value::FloatWidth::Double,
            super::scalar_value::TextWidth::Varying,
        )
    }

    type Custom = NoCustomScalars<Self>;

    /// Measured: SQLite answers `NULL`.
    const DIVISION_BY_ZERO: crate::compiler::vm::refusal::DivisionByZero =
        crate::compiler::vm::refusal::DivisionByZero::IsNull;

    /// Measured: `typeof(avg(x))` is `real` for every column, and
    /// `avg` of one row of `9007199254740993` is `9.00719925474099e+15`,
    /// so the mean is inexact there and reproducing it means staying in
    /// `f64`.
    const MEAN: super::scalar_value::MeanRule = super::scalar_value::MeanRule::Double;

    /// Measured: integers sum as one 64-bit integer, answering `integer
    /// overflow` past it, and the total turns real as soon as a
    /// non-integer joins. SQLite has no decimal type, so a column
    /// declaring one carries integers or reals and follows the same rule.
    fn sum_rule(column: super::scalar_value::BuiltinType) -> super::scalar_value::SumRule {
        use super::scalar_value::{BuiltinType, SumRule};

        match column {
            BuiltinType::Int(_) => SumRule::IntegerPromotingToDouble,
            _ => SumRule::Double,
        }
    }

    /// Measured: `7 / 2` is `3`. Only the integer half of this rule can
    /// run, since SQLite has no decimal type.
    const DIVISION: super::scalar_value::DivisionRule =
        super::scalar_value::DivisionRule::IntegersTruncate;

    /// SQLite decodes no decimal cell, so this states the rule it declares rather than a second one.
    fn decimal_quotient(
        dividend: bigdecimal::BigDecimal,
        divisor: bigdecimal::BigDecimal,
        quotient: crate::compiler::bytecode::Quotient,
    ) -> bigdecimal::BigDecimal {
        match quotient {
            crate::compiler::bytecode::Quotient::FromTheOperands => {
                crate::compiler::vm::arithmetic::quotient_at_significant_digits(&dividend, &divisor)
            }
            crate::compiler::bytecode::Quotient::InWordsAt(increment) => {
                crate::compiler::vm::arithmetic::quotient_in_words(&dividend, &divisor, increment)
            }
        }
    }

    /// Never called: this engine's `/` truncates two integers.
    fn integer_quotient(
        dividend: i64,
        divisor: i64,
        increment: super::scalar_value::DivisionPrecisionIncrement,
    ) -> bigdecimal::BigDecimal {
        crate::compiler::vm::arithmetic::quotient_in_words(
            &bigdecimal::BigDecimal::from(dividend),
            &bigdecimal::BigDecimal::from(divisor),
            increment,
        )
    }

    /// Measured: SQLite carries an overflowed integer result as a real.
    fn integer_binary(
        operation: crate::compiler::vm::refusal::ArithmeticOp,
        left: i64,
        right: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::refusal::IntegerOverflow::PromotesToFloat,
            operation,
            left,
            right,
        )
    }

    fn integer_negate(
        value: i64,
    ) -> Result<Value<Self>, crate::compiler::vm::refusal::EvaluationRefusal> {
        crate::compiler::vm::arithmetic::checked_integer_binary(
            crate::compiler::vm::refusal::IntegerOverflow::PromotesToFloat,
            crate::compiler::vm::refusal::ArithmeticOp::Negate,
            value,
            0,
        )
    }

    /// SQLite compares an integer against a real without rounding either,
    /// measured: `9007199254740993 = 9007199254740992.0` is 0 where the
    /// other two engines answer 1. It has no decimal type, so a decimal
    /// operand has no measured rule and is not served.
    fn numeric_widening(left: BuiltinKind, right: BuiltinKind) -> Option<NumericWidening> {
        match (left, right) {
            (BuiltinKind::Int, BuiltinKind::Float) | (BuiltinKind::Float, BuiltinKind::Int) => {
                Some(NumericWidening::Exact)
            }
            _ => None,
        }
    }

    /// SQLite gives `LIKE` no default escape: a backslash in a pattern
    /// matches a backslash, so no pattern can end with one dangling.
    const LIKE_ESCAPE: Option<crate::compiler::vm::refusal::LikeEscape> = None;

    fn compare_cross_kind_numeric(
        left: &Value<Self>,
        right: &Value<Self>,
    ) -> Option<core::cmp::Ordering> {
        crate::backend::cross_kind_numeric_ordering(left, right)
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

    fn decode_group_value(
        kind: super::scalar_value::ValueKindOf<Self>,
        value: Value<Self>,
    ) -> Option<Value<Self>> {
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
