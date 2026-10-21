use super::{AggAccumulator, FoldRule, SumOutOfRange, Total};
use crate::backend::{Backend, NumericWidening, ScalarFamily};
use crate::compiler::sql_shape::HavingFunction;
use crate::{AggHaving, AggValue, HavingOp, HavingSubject, NumericValue};
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use core::cmp::Ordering;

/// A grouped predicate with its constant conversions cached.
#[derive(serde::Serialize)]
pub struct GroupHaving {
    subject: HavingSubject,
    op: HavingOp,
    threshold: NumericThreshold,
}

impl GroupHaving {
    pub(crate) fn new<B: Backend>(having: &AggHaving) -> Option<Self> {
        Some(Self {
            subject: having.subject,
            op: having.op,
            threshold: NumericThreshold::new::<B>(&having.threshold)?,
        })
    }

    pub(crate) fn comparisons_served<B: Backend>(&self, rule: FoldRule) -> bool {
        use crate::backend::{MeanRule, SumRule};
        let mut promoted = None;
        let family = match self.subject {
            HavingSubject::RowCount | HavingSubject::Aggregate(HavingFunction::CountColumn) => {
                ScalarFamily::Int
            }
            HavingSubject::Aggregate(HavingFunction::Sum) => match rule.total {
                SumRule::Integer => ScalarFamily::Int,
                SumRule::IntegerPromotingToDouble => {
                    promoted = Some(ScalarFamily::Float);
                    ScalarFamily::Int
                }
                SumRule::Decimal { .. } => ScalarFamily::Decimal,
                SumRule::Single | SumRule::Double => ScalarFamily::Float,
            },
            HavingSubject::Aggregate(HavingFunction::Avg) => match (rule.total, rule.mean) {
                (SumRule::Single | SumRule::Double, _) | (_, MeanRule::Double) => {
                    ScalarFamily::Float
                }
                (_, MeanRule::Exact) => ScalarFamily::Decimal,
            },
            HavingSubject::Aggregate(
                HavingFunction::VarPop
                | HavingFunction::VarSamp
                | HavingFunction::StddevPop
                | HavingFunction::StddevSamp,
            ) => ScalarFamily::Float,
        };
        self.threshold.widening::<B>(family).is_some()
            && promoted.is_none_or(|family| self.threshold.widening::<B>(family).is_some())
    }

    pub(crate) fn passes<B: Backend>(
        &self,
        rows: i64,
        accumulator: &AggAccumulator,
    ) -> Result<bool, SumOutOfRange> {
        let ordering = match self.subject {
            HavingSubject::RowCount => self.threshold.integer::<B>(rows),
            HavingSubject::Aggregate(HavingFunction::CountColumn) => {
                self.threshold.integer::<B>(accumulator.contributions)
            }
            HavingSubject::Aggregate(HavingFunction::Sum) => {
                if accumulator.contributions == 0 {
                    return Ok(false);
                }
                match &accumulator.total {
                    Total::Integer { value, reals, .. } if reals.is_zero() => {
                        self.threshold.integer::<B>(*value)
                    }
                    Total::Integer { .. } => self.threshold.number::<B>(&accumulator.total.value()),
                    Total::Decimal { value, .. } => self.threshold.decimal::<B>(value),
                    Total::Single(parts, _) | Total::Double(parts, _) => {
                        self.threshold.float::<B>(parts.value())
                    }
                }
            }
            HavingSubject::Aggregate(function) => match accumulator.value_as(function) {
                AggValue::CountStar(count) | AggValue::CountColumn(count) => {
                    self.threshold.integer::<B>(count)
                }
                AggValue::Sum(value) | AggValue::Avg(value) => match value {
                    Some(value) => self.threshold.number::<B>(&value),
                    None => return Ok(false),
                },
                AggValue::VarPop(value)
                | AggValue::VarSamp(value)
                | AggValue::StddevPop(value)
                | AggValue::StddevSamp(value) => match value {
                    Some(value) => self.threshold.float::<B>(value),
                    None => return Ok(false),
                },
            },
        }?;
        Ok(ordering.is_some_and(|ordering| self.op.admits(ordering)))
    }
}

struct NumericThreshold {
    family: ScalarFamily,
    exact: Option<BigDecimal>,
    float: f64,
    integer: Option<IntegerThreshold>,
    float_residual: Ordering,
}

impl serde::Serialize for NumericThreshold {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use crate::backend::{
            declared_type_of, FloatWidth, IntWidth, NoCustom, ScalarKind, TextWidth,
        };

        let kind = ScalarKind::<NoCustom>::Builtin(declared_type_of(
            self.family,
            IntWidth::SixtyFour,
            FloatWidth::Double,
            TextWidth::Varying,
        ));
        serde::Serialize::serialize(&(kind, &self.exact, self.float), serializer)
    }
}

impl NumericThreshold {
    fn new<B: Backend>(text: &str) -> Option<Self> {
        let family = B::numeric_literal_family(text);
        let float = sql_scalar_text::parse_f64(text)?;
        let exact = if family == ScalarFamily::Float {
            BigDecimal::from_f64(float)
        } else {
            Some(sql_scalar_text::parse_decimal(text)?)
        };
        let integer = exact.as_ref().map(IntegerThreshold::new);
        let float_residual = exact.as_ref().map_or(Ordering::Equal, |exact| {
            BigDecimal::from_f64(float).map_or_else(
                || {
                    if float.is_sign_negative() {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                },
                |rounded| rounded.cmp(exact),
            )
        });
        Some(Self {
            family,
            exact,
            float,
            integer,
            float_residual,
        })
    }

    fn widening<B: Backend>(&self, left: ScalarFamily) -> Option<NumericWidening> {
        if left == self.family {
            Some(if left == ScalarFamily::Float {
                NumericWidening::AtFloatWidth
            } else {
                NumericWidening::Exact
            })
        } else {
            B::numeric_widening(left, self.family)
        }
    }

    fn float_width<B: Backend>(&self, left: f64) -> Result<Option<Ordering>, SumOutOfRange> {
        if self.family != ScalarFamily::Float && !self.float.is_finite() {
            return Err(SumOutOfRange);
        }
        Ok(B::FLOAT_ORDER.compare(left, self.float))
    }

    fn integer<B: Backend>(&self, left: i64) -> Result<Option<Ordering>, SumOutOfRange> {
        match self.widening::<B>(ScalarFamily::Int) {
            Some(NumericWidening::AtFloatWidth) => {
                self.float_width::<B>(crate::backend::widen_i64_to_f64(left))
            }
            Some(NumericWidening::Exact) => Ok(self.integer.as_ref().map_or_else(
                || B::FLOAT_ORDER.compare(0.0, self.float),
                |threshold| Some(threshold.compare(left)),
            )),
            None => Ok(None),
        }
    }

    fn decimal<B: Backend>(&self, left: &BigDecimal) -> Result<Option<Ordering>, SumOutOfRange> {
        match self.widening::<B>(ScalarFamily::Decimal) {
            Some(NumericWidening::AtFloatWidth) => {
                let left = left
                    .to_f64()
                    .filter(|value| value.is_finite())
                    .ok_or(SumOutOfRange)?;
                self.float_width::<B>(left)
            }
            Some(NumericWidening::Exact) => Ok(self.exact.as_ref().map_or_else(
                || B::FLOAT_ORDER.compare(0.0, self.float),
                |right| Some(left.cmp(right)),
            )),
            None => Ok(None),
        }
    }

    fn float<B: Backend>(&self, left: f64) -> Result<Option<Ordering>, SumOutOfRange> {
        match self.widening::<B>(ScalarFamily::Float) {
            Some(NumericWidening::AtFloatWidth) => self.float_width::<B>(left),
            // Break rounded equality using the constant's exact boundary.
            Some(NumericWidening::Exact) => Ok(B::FLOAT_ORDER
                .compare(left, self.float)
                .map(|ordering| ordering.then(self.float_residual))),
            None => Ok(None),
        }
    }

    fn number<B: Backend>(&self, left: &NumericValue) -> Result<Option<Ordering>, SumOutOfRange> {
        match left {
            NumericValue::Integer(value) => self.integer::<B>(*value),
            NumericValue::Decimal(value) => self.decimal::<B>(value),
            NumericValue::Double(value) => self.float::<B>(*value),
        }
    }
}

enum IntegerThreshold {
    Below,
    Above,
    Within { integer: i64, residual: Ordering },
}

impl IntegerThreshold {
    fn new(value: &BigDecimal) -> Self {
        if value < &BigDecimal::from(i64::MIN) {
            return Self::Below;
        }
        if value > &BigDecimal::from(i64::MAX) {
            return Self::Above;
        }
        let integer = if value > &BigDecimal::from(-1) && value < &BigDecimal::from(1) {
            0
        } else {
            value
                .to_i64()
                .expect("a bounded decimal has an integer part")
        };
        Self::Within {
            integer,
            residual: BigDecimal::from(integer).cmp(value),
        }
    }

    fn compare(&self, left: i64) -> Ordering {
        match self {
            Self::Below => Ordering::Greater,
            Self::Above => Ordering::Less,
            Self::Within { integer, residual } => match left.cmp(integer) {
                Ordering::Equal => *residual,
                ordering => ordering,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NumericThreshold, SumOutOfRange};
    use crate::backend::{Backend, MySql, Postgres, SQLite};
    use crate::NumericValue;
    use bigdecimal::BigDecimal;
    use core::cmp::Ordering;

    fn compare<B: Backend>(value: &NumericValue, literal: &str) -> Option<Ordering> {
        NumericThreshold::new::<B>(literal)
            .expect("the numeric literal parses")
            .number::<B>(value)
            .expect("the comparison stays in range")
    }

    #[test]
    fn integer_comparisons_preserve_fractional_and_range_boundaries() {
        for (integer, literal, ordering) in [
            (9_007_199_254_740_993, "9007199254740992", Ordering::Greater),
            (
                9_007_199_254_740_993,
                "9007199254740992.5",
                Ordering::Greater,
            ),
            (i64::MAX, "9223372036854775807", Ordering::Equal),
            (i64::MAX, "9223372036854775807.5", Ordering::Less),
            (i64::MIN, "-9223372036854775808.5", Ordering::Greater),
            (i64::MIN, "-9223372036854775807.5", Ordering::Less),
            (0, "1e-10000", Ordering::Less),
            (0, "-1e-10000", Ordering::Greater),
            (i64::MAX, "1e10000", Ordering::Less),
            (i64::MIN, "-1e10000", Ordering::Greater),
        ] {
            assert_eq!(
                compare::<Postgres>(&NumericValue::Integer(integer), literal),
                Some(ordering),
                "{integer} against {literal}"
            );
        }
    }

    #[test]
    fn exponent_literals_follow_backend_numeric_widening() {
        let total = NumericValue::Decimal(BigDecimal::from(9_007_199_254_740_993_i64));
        assert_eq!(
            compare::<Postgres>(&total, "9.0071992547409925e15"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare::<MySql>(&total, "9.0071992547409925e15"),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare::<MySql>(&total, "9007199254740992.5"),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn sqlite_does_not_round_integer_float_pairs() {
        let integer = NumericValue::Integer(9_007_199_254_740_993);
        let real = NumericValue::Double(9_007_199_254_740_992.0);
        assert_eq!(
            compare::<SQLite>(&integer, "9007199254740992.0"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare::<SQLite>(&real, "9007199254740993"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare::<Postgres>(&real, "9007199254740993"),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn overflowing_float_coercion_fails_instead_of_comparing_infinities() {
        let threshold = NumericThreshold::new::<Postgres>("1e10000").expect("an exact literal");
        assert_eq!(threshold.float::<Postgres>(1.0), Err(SumOutOfRange));
        let value = NumericValue::Decimal("1e10000".parse().expect("an exact total"));
        assert_eq!(
            NumericThreshold::new::<MySql>("1e0")
                .expect("a floating literal")
                .number::<MySql>(&value),
            Err(SumOutOfRange)
        );
    }
}
