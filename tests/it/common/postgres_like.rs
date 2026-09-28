//! A test backend that takes PostgreSQL's rules on the standard carriers.
//!
//! Each suite that needs a backend of its own differs from PostgreSQL in one
//! or two rules, so the rules they share are written once here and each
//! suite states only its own.

/// Implement `Backend` for `$name` with PostgreSQL's rules, its custom
/// scalar set `$custom`, integer overflow `$overflow`, and the further items
/// `$extra`, and give it `from_postgres`, which carries a value PostgreSQL
/// parsed over unchanged.
macro_rules! postgres_like_backend {
    ($name:ident, custom = $custom:ty, overflow = $overflow:expr, { $($extra:tt)* }) => {
        impl subql::backend::Backend for $name {
            /// The embedder's own engine here is PostgreSQL's, whose
            /// delimited identifiers keep their case.
            const COLUMN_NAME_CASE: sql_traits::structs::IdentifierCase =
                sql_traits::structs::IdentifierCase::AsWritten;

            const WRITTEN_TABLE_NAME_CASE: sql_traits::structs::IdentifierCase =
                sql_traits::structs::IdentifierCase::AsWritten;

            const WIRE_TABLE_NAME_CASE: sql_traits::structs::IdentifierCase =
                sql_traits::structs::IdentifierCase::Exact;

            /// This backend speaks the PostgreSQL dialect, so it takes
            /// PostgreSQL's `LIKE` escape rule with it.
            const LIKE_DEFAULT_ESCAPE: Option<char> = Some('\\');

            const LIKE_DANGLING_ESCAPE: subql::compiler::vm::refusal::DanglingEscape =
                subql::compiler::vm::refusal::DanglingEscape::Fails;

            fn like_escape_clause(written: &str) -> Result<Option<char>, &'static str> {
                <subql::backend::Postgres as subql::backend::Backend>::like_escape_clause(written)
            }

            /// PostgreSQL's dialect, so PostgreSQL's rule.
            const DIVISION_BY_ZERO: subql::compiler::vm::refusal::DivisionByZero =
                subql::compiler::vm::refusal::DivisionByZero::Fails;

            const INTEGER_OVERFLOW: subql::compiler::vm::refusal::IntegerOverflow = $overflow;

            /// This backend carries its integers in `i64`, so it takes the
            /// checked arithmetic.
            fn integer_binary(
                operation: subql::compiler::vm::refusal::ArithmeticOp,
                left: i64,
                right: i64,
            ) -> Result<
                subql::backend::Value<Self>,
                subql::compiler::vm::refusal::EvaluationRefusal,
            > {
                subql::compiler::vm::arithmetic::checked_integer_binary(
                    Self::INTEGER_OVERFLOW,
                    operation,
                    left,
                    right,
                )
            }

            fn integer_negate(
                value: i64,
            ) -> Result<
                subql::backend::Value<Self>,
                subql::compiler::vm::refusal::EvaluationRefusal,
            > {
                subql::compiler::vm::arithmetic::checked_integer_negate(
                    Self::INTEGER_OVERFLOW,
                    value,
                )
            }

            /// No cross-kind numeric comparison: the fixtures compare
            /// same-kind values only.
            fn numeric_widening(
                _left: subql::backend::ScalarFamily,
                _right: subql::backend::ScalarFamily,
            ) -> Option<subql::backend::NumericWidening> {
                None
            }

            /// And raises when a floating total leaves range, as PostgreSQL
            /// does.
            const FLOAT_SUM_OVERFLOW: subql::backend::FloatSumOverflow =
                subql::backend::FloatSumOverflow::Raises;

            /// It orders floats the way PostgreSQL does.
            const FLOAT_ORDER: subql::backend::FloatOrder =
                subql::backend::FloatOrder::NanIsGreatest;

            /// And reads a variance back the way it does, from its own answer.
            const VARIANCE_SEED: subql::backend::VarianceSeed =
                subql::backend::VarianceSeed::EnginesOwn;

            /// And averages like it: an exact total's mean is an exact decimal.
            const MEAN: subql::backend::MeanRule = subql::backend::MeanRule::Exact;

            /// It sums like PostgreSQL: a narrow integer column totals into a
            /// 64-bit integer and everything exact totals into a decimal.
            fn sum_rule(column: subql::backend::DeclaredType) -> subql::backend::SumRule {
                match column {
                    subql::backend::DeclaredType::Int(
                        subql::backend::IntWidth::UpToThirtyTwo,
                    ) => subql::backend::SumRule::Integer,
                    subql::backend::DeclaredType::Int(subql::backend::IntWidth::SixtyFour)
                    | subql::backend::DeclaredType::Decimal => subql::backend::SumRule::Decimal {
                        integer_digits: Some(131_072),
                    },
                    _ => subql::backend::SumRule::Double,
                }
            }

            /// It divides like PostgreSQL: two integers truncate, and a
            /// decimal quotient takes the significant-digit scale.
            const DIVISION: subql::backend::DivisionRule =
                subql::backend::DivisionRule::IntegersTruncate;

            const NULL_SAFE_EQUALITY: subql::backend::NullSafeEquality =
                subql::backend::NullSafeEquality::DistinctFrom;

            const READS_IS_UNKNOWN: bool = true;

            const RANGE_WITH_NULL_BOUND_COMPARES_DOUBLES: bool = false;

            fn decimal_quotient(
                dividend: bigdecimal::BigDecimal,
                divisor: bigdecimal::BigDecimal,
                quotient: subql::compiler::bytecode::Quotient,
            ) -> bigdecimal::BigDecimal {
                subql::compiler::vm::arithmetic::quotient_by_rule(&dividend, &divisor, quotient)
            }

            /// Never called: this backend's `/` truncates two integers.
            fn integer_quotient(
                dividend: i64,
                divisor: i64,
                increment: subql::backend::DivisionPrecisionIncrement,
            ) -> bigdecimal::BigDecimal {
                subql::compiler::vm::arithmetic::integer_quotient_in_words(
                    dividend, divisor, increment,
                )
            }

            /// On the standard carrier, so the shared narrowing serves even
            /// though this backend never resolves a single-width result.
            fn hold_float_at_single(value: f64) -> f64 {
                subql::backend::at_float4(value)
            }

            /// The fixtures declare no single-width column, so no result is
            /// held at float4 and this backend narrows nothing.
            fn float_arithmetic_width(
                left: Option<subql::backend::FloatWidth>,
                right: Option<subql::backend::FloatWidth>,
            ) -> Option<subql::backend::FloatWidth> {
                left.or(right).map(|_| subql::backend::FloatWidth::Double)
            }

            /// The fixtures declare no fixed-width or single-width column, so
            /// the common refinements serve.
            fn refine_declared_type(
                family: subql::backend::ScalarFamily,
                declared_type: &str,
            ) -> subql::backend::DeclaredType {
                subql::backend::declared_type_of(
                    family,
                    subql::backend::declares_sixty_four_bit_int(declared_type),
                    subql::backend::FloatWidth::Double,
                    subql::backend::TextWidth::Varying,
                )
            }

            /// Byte comparison, which is all the fixtures need.
            fn text_rule(
                _comparison: &subql::backend::ComparisonContext<'_, Self>,
                _operation: subql::backend::TextOperation,
            ) -> subql::backend::TextResolution {
                subql::backend::TextResolution::Rule(subql::backend::TextRule::EXACT)
            }

            $($extra)*

            type Dialect = sqlparser::dialect::PostgreSqlDialect;
            type Custom = $custom;
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
            type JsonbVersion = subql::backend::Pg18;
        }

        impl $name {
            /// A builtin value PostgreSQL parsed, carried over unchanged,
            /// since this backend's builtin shapes are PostgreSQL's.
            fn from_postgres(
                value: subql::backend::Value<subql::backend::Postgres>,
            ) -> subql::backend::Value<Self> {
                use subql::backend::Value;
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
        }
    };
}

pub(crate) use postgres_like_backend;
