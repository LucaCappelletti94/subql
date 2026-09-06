//! Arithmetic instructions split out of `vm`.

use super::refusal::{ArithmeticOp, DivisionByZero, EvaluationRefusal, IntegerOverflow};
use crate::backend::{Backend, DivisionPrecisionIncrement, Value};
use alloc::string::ToString as _;
use bigdecimal::{
    num_bigint::{BigInt, Sign},
    BigDecimal,
};

/// `Value::Missing` / `Value::Null` on either side propagates to
/// `Value::Null` (SQL NULL propagation).
pub(crate) const fn null_propagate_binary<B: Backend>(
    a: &Value<B>,
    b: &Value<B>,
) -> Option<Value<B>> {
    if a.is_absent() || b.is_absent() {
        Some(Value::Null)
    } else {
        None
    }
}

/// The checked integer arithmetic a backend on the standard `i64` carrier
/// delegates to, applying `overflow` when the result does not fit.
///
/// Bounded rather than written for every `Backend`, because `B::Int` and
/// `B::Float` are associated types with no `checked_add` and no conversion
/// to `f64`. A backend carrying its integers otherwise states its own rule.
///
/// # Errors
///
/// [`EvaluationRefusal::IntegerOverflow`] when the result does not fit and
/// this backend raises rather than promoting.
pub fn checked_integer_binary<B>(
    overflow: IntegerOverflow,
    operation: ArithmeticOp,
    a: i64,
    b: i64,
) -> Result<Value<B>, EvaluationRefusal>
where
    B: Backend<Int = i64, Float = f64>,
{
    let checked = match operation {
        ArithmeticOp::Add => a.checked_add(b),
        ArithmeticOp::Subtract => a.checked_sub(b),
        ArithmeticOp::Multiply => a.checked_mul(b),
        ArithmeticOp::Negate => a.checked_neg(),
        // A zero divisor is answered before reaching here, so `None` on
        // division means the quotient does not fit, which is only
        // `i64::MIN / -1`. Measured: PostgreSQL raises `bigint out of
        // range` for it and SQLite promotes to a real, exactly as for the
        // other operators.
        ArithmeticOp::Divide => a.checked_div(b),
        // Modulo has no such case: `i64::MIN % -1` is 0, which fits, and
        // all three engines answer 0. Rust's `checked_rem` reports `None`
        // for it because the *division* would overflow, so using it here
        // would invent a failure no engine has.
        ArithmeticOp::Modulo => Some(a.wrapping_rem(b)),
    };
    match (checked, overflow) {
        (Some(value), _) => Ok(Value::Int(value)),
        (None, IntegerOverflow::PromotesToFloat) => Ok(Value::Float(promoted(operation, a, b))),
        (None, IntegerOverflow::Fails) => Err(EvaluationRefusal::IntegerOverflow { operation }),
    }
}

/// The overflowed result computed in `f64`, which is what SQLite carries.
fn promoted(operation: ArithmeticOp, a: i64, b: i64) -> f64 {
    let (a, b) = (
        crate::backend::widen_i64_to_f64(a),
        crate::backend::widen_i64_to_f64(b),
    );
    match operation {
        ArithmeticOp::Add => a + b,
        ArithmeticOp::Subtract => a - b,
        ArithmeticOp::Multiply => a * b,
        ArithmeticOp::Negate => -a,
        ArithmeticOp::Divide => a / b,
        // Unreachable: modulo never overflows, so it is never promoted.
        ArithmeticOp::Modulo => a % b,
    }
}

/// Add: same-scalar only.
pub(crate) fn arithmetic_add<B: Backend>(
    a: Value<B>,
    b: Value<B>,
) -> Result<Value<B>, EvaluationRefusal> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return Ok(null);
    }
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => return B::integer_binary(ArithmeticOp::Add, x, y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x + y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x + y),
        _ => Value::Null,
    })
}

/// Subtract: same-scalar only.
pub(crate) fn arithmetic_subtract<B: Backend>(
    a: Value<B>,
    b: Value<B>,
) -> Result<Value<B>, EvaluationRefusal> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return Ok(null);
    }
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => return B::integer_binary(ArithmeticOp::Subtract, x, y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x - y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x - y),
        _ => Value::Null,
    })
}

/// Multiply: same-scalar only.
pub(crate) fn arithmetic_multiply<B: Backend>(
    a: Value<B>,
    b: Value<B>,
) -> Result<Value<B>, EvaluationRefusal> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return Ok(null);
    }
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => return B::integer_binary(ArithmeticOp::Multiply, x, y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x * y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x * y),
        _ => Value::Null,
    })
}

/// Divide: same-scalar only.
///
/// A zero divisor is the engine's answer, not one rule: measured,
/// PostgreSQL raises `division by zero` for every numeric type while MySQL
/// and SQLite answer `NULL`.
///
/// Zero is detected by `b - b == b`, an identity that holds only for the
/// additive identity of a `Sub<Output = Self> + PartialEq` type. This
/// avoids adding a `Zero` bound to `Backend`.
///
/// # Errors
///
/// [`EvaluationRefusal::DivisionByZero`] on a zero divisor where this
/// backend raises, and [`EvaluationRefusal::IntegerOverflow`] for the one
/// quotient that does not fit, `i64::MIN / -1`.
pub(crate) fn arithmetic_divide<B: Backend>(
    a: Value<B>,
    b: Value<B>,
    quotient: crate::compiler::bytecode::Quotient,
) -> Result<Value<B>, EvaluationRefusal> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return Ok(null);
    }
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if let Some(null) = zero_divisor::<B, _>(&y, ArithmeticOp::Divide)? {
                return Ok(null);
            }
            match quotient {
                crate::compiler::bytecode::Quotient::FromTheOperands => {
                    return B::integer_binary(ArithmeticOp::Divide, x, y)
                }
                crate::compiler::bytecode::Quotient::InWordsAt(increment) => {
                    Value::Decimal(B::integer_quotient(x, y, increment))
                }
            }
        }
        (Value::Float(x), Value::Float(y)) => {
            if let Some(null) = zero_divisor::<B, _>(&y, ArithmeticOp::Divide)? {
                return Ok(null);
            }
            Value::Float(x / y)
        }
        (Value::Decimal(x), Value::Decimal(y)) => {
            if let Some(null) = zero_divisor::<B, _>(&y, ArithmeticOp::Divide)? {
                return Ok(null);
            }
            Value::Decimal(B::decimal_quotient(x, y, quotient))
        }
        _ => Value::Null,
    })
}

/// Modulo: `Int % Int` only, same zero rule as division.
///
/// # Errors
///
/// As [`arithmetic_divide`].
pub(crate) fn arithmetic_modulo<B: Backend>(
    a: Value<B>,
    b: Value<B>,
) -> Result<Value<B>, EvaluationRefusal> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return Ok(null);
    }
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if let Some(null) = zero_divisor::<B, _>(&y, ArithmeticOp::Modulo)? {
                return Ok(null);
            }
            return B::integer_binary(ArithmeticOp::Modulo, x, y);
        }
        _ => Value::Null,
    })
}

/// This backend's answer for a zero divisor: `None` when the divisor is not
/// zero, `Some(Value::Null)` where the engine answers null, and the failure
/// where it raises.
///
/// # Errors
///
/// [`EvaluationRefusal::DivisionByZero`] where this backend raises.
fn zero_divisor<B: Backend, T>(
    divisor: &T,
    operation: ArithmeticOp,
) -> Result<Option<Value<B>>, EvaluationRefusal>
where
    T: Clone + PartialEq + core::ops::Sub<Output = T>,
{
    if !is_zero_scalar(divisor) {
        return Ok(None);
    }
    match B::DIVISION_BY_ZERO {
        DivisionByZero::IsNull => Ok(Some(Value::Null)),
        DivisionByZero::Fails => Err(EvaluationRefusal::DivisionByZero { operation }),
    }
}

/// Negate: same-scalar only.
pub(crate) fn arithmetic_negate<B: Backend>(a: Value<B>) -> Result<Value<B>, EvaluationRefusal> {
    if a.is_absent() {
        return Ok(Value::Null);
    }
    Ok(match a {
        Value::Int(x) => return B::integer_negate(x),
        Value::Float(x) => Value::Float(-x),
        Value::Decimal(x) => Value::Decimal(-x),
        _ => Value::Null,
    })
}

/// Trait-generic "is zero" check.
///
/// A scalar `x` is zero iff `x - x == x` (holds only for the additive
/// identity of a `Sub<Output = Self> + PartialEq` type). Backend requires
/// both bounds on `Int` / `Float` / `Decimal`, so this specialises cleanly
/// per scalar without a `num_traits::Zero` bound.
pub(crate) fn is_zero_scalar<T>(x: &T) -> bool
where
    T: Clone + PartialEq + core::ops::Sub<Output = T>,
{
    let cleared = x.clone() - x.clone();
    &cleared == x
}

/// One decimal quotient's fractional digits, and how the digit after them
/// is spent.
///
/// Both engines compute a quotient to a scale they pick and neither keeps
/// more, so a quotient is a scale plus a rule for the first digit dropped.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Quantisation {
    /// Fractional digits kept.
    scale: i64,
    /// Whether a dropped half rounds the last kept digit away from zero.
    rounds: bool,
}

/// MySQL's quotient of two decimals, at the increment the deployment
/// declared.
///
/// The quotient is truncated, and the scale it is truncated at is
/// measured rather than derived, per
/// [`DivisionRule::QuotientsAreDecimalInWords`](crate::backend::DivisionRule::QuotientsAreDecimalInWords).
/// A backend on the standard `bigdecimal` carrier delegates here.
///
/// # The scale
///
/// Measured on MySQL 8.0 over declared `DECIMAL(30,n)` columns, by
/// searching for the smallest number of digits that compares equal to
/// the quotient. One scaled operand quantises the way the name of the
/// rule suggests, breaking at 6 and 15:
///
/// ```text
/// one operand scaled  0 1 2 4 5  6 9 10 14  15
/// compared at         9 9 9 9 9 18 18 18 18  27
/// ```
///
/// Two scaled operands do not add that way, which is what the earlier
/// formula here assumed. `(1, 1)` compares at 18 where adding the scales
/// gives 9, and `(10, 10)` at 36 where it gives 27, so each operand is
/// framed to whole words *before* the two contribute:
///
/// ```text
/// digits = 9 * max(words(dividend_scale) + words(divisor_scale),
///                  words(dividend_scale + divisor_scale + increment))
/// ```
///
/// That fits every measured point: both axes to twenty digits, a joint
/// grid, seventeen randomly chosen pairs, and increments 2, 4, 10 and
/// 20. The two terms disagree on 72 of the 441 scale pairs up to twenty
/// digits, and the earlier formula computed only the second.
///
/// The scale MySQL *prints* is a different number, `dividend_scale +
/// increment`, which is why this hid: `7.00 / 3.00` displays as
/// `2.333333` and compares equal to eighteen digits of `3`.
#[must_use]
pub fn quotient_in_words(
    dividend: &BigDecimal,
    divisor: &BigDecimal,
    increment: DivisionPrecisionIncrement,
) -> BigDecimal {
    let dividend_scale = fractional_digits(dividend);
    let divisor_scale = fractional_digits(divisor);
    let framed = whole_words(dividend_scale) + whole_words(divisor_scale);
    let summed = whole_words(dividend_scale + divisor_scale + i64::from(increment.digits()));
    quotient(
        dividend,
        divisor,
        Quantisation {
            scale: framed.max(summed),
            rounds: false,
        },
    )
}

/// PostgreSQL's quotient of two `numeric` values.
///
/// The scale is whatever gives sixteen significant digits, floored by
/// either operand's own scale, and the quotient is rounded half away from
/// zero, per
/// [`DivisionRule::IntegersTruncate`](crate::backend::DivisionRule::IntegersTruncate).
/// A backend on the standard
/// `bigdecimal` carrier delegates here.
#[must_use]
pub fn quotient_at_significant_digits(dividend: &BigDecimal, divisor: &BigDecimal) -> BigDecimal {
    /// `NUMERIC_MIN_SIG_DIGITS`, the significant digits a quotient gets.
    const SIGNIFICANT: i64 = 16;
    /// `DEC_DIGITS`, the decimal digits one stored word holds.
    const PER_WORD: i64 = 4;
    /// `NUMERIC_MAX_DISPLAY_SCALE`.
    const MAX_SCALE: i64 = 1000;

    let (dividend_weight, dividend_lead) = leading_word(dividend);
    let (divisor_weight, divisor_lead) = leading_word(divisor);
    // The server's own conservative estimate: equal leading words leave the
    // ordering undecided, so it assumes the quotient is the smaller one and
    // buys another word of scale.
    let quotient_weight =
        dividend_weight - divisor_weight - i64::from(dividend_lead <= divisor_lead);
    let scale = (SIGNIFICANT - quotient_weight * PER_WORD)
        .max(fractional_digits(dividend))
        .max(fractional_digits(divisor))
        .clamp(0, MAX_SCALE);
    quotient(
        dividend,
        divisor,
        Quantisation {
            scale,
            rounds: true,
        },
    )
}

/// `dividend / divisor` at exactly `quantisation`.
///
/// Computed as one integer division rather than by dividing and then
/// re-scaling: `bigdecimal` divides to a fixed number of significant
/// digits, and a quotient here can want eighty fractional digits on top of
/// an integer part, which would put the digit being kept past that
/// horizon.
fn quotient(dividend: &BigDecimal, divisor: &BigDecimal, quantisation: Quantisation) -> BigDecimal {
    let (dividend_digits, dividend_scale) = dividend.as_bigint_and_exponent();
    let (divisor_digits, divisor_scale) = divisor.as_bigint_and_exponent();
    // dividend / divisor * 10^scale, as a ratio of integers.
    let shift = quantisation.scale + divisor_scale - dividend_scale;
    let (numerator, denominator) = if shift >= 0 {
        (dividend_digits * power_of_ten(shift), divisor_digits)
    } else {
        (dividend_digits, divisor_digits * power_of_ten(-shift))
    };
    let negative = (numerator.sign() == Sign::Minus) != (denominator.sign() == Sign::Minus);
    let numerator = numerator.magnitude().clone();
    let denominator = denominator.magnitude().clone();
    let mut digits = &numerator / &denominator;
    if quantisation.rounds {
        let remainder = numerator % &denominator;
        if remainder * 2u8 >= denominator {
            digits += 1u8;
        }
    }
    let signed = BigInt::from_biguint(if negative { Sign::Minus } else { Sign::Plus }, digits);
    BigDecimal::new(signed, quantisation.scale)
}

/// `10^exponent`, for a non-negative exponent.
fn power_of_ten(exponent: i64) -> BigInt {
    let exponent = u32::try_from(exponent).unwrap_or(u32::MAX);
    BigInt::from(10u8).pow(exponent)
}

/// The value's own fractional digits, never negative.
///
/// `1E+2` carries a negative scale, which is no engine's fractional digit
/// count: both floor theirs at zero.
fn fractional_digits(value: &BigDecimal) -> i64 {
    value.as_bigint_and_exponent().1.max(0)
}

/// `digits` rounded up to a whole nine-digit MySQL word.
///
/// `ROUND_UP(X) * DIG_PER_DEC1` in `mysys/decimal.cc`.
const fn whole_words(digits: i64) -> i64 {
    /// `DIG_PER_DEC1`, the decimal digits one stored word holds.
    const PER_WORD: i64 = 9;

    (digits + PER_WORD - 1) / PER_WORD * PER_WORD
}

/// The value's leading four-digit word: which word it is, and what it
/// holds.
///
/// PostgreSQL stores a `numeric` in base 10000 aligned on the decimal
/// point, and its division scale reads both the leading word's position
/// and its value. Word 0 covers the digits `10^0` through `10^3`, so `7`
/// is word 0 holding 7, `12345` is word 1 holding 1, and `0.5` is word -1
/// holding 5000.
fn leading_word(value: &BigDecimal) -> (i64, i64) {
    /// `DEC_DIGITS`.
    const PER_WORD: i64 = 4;

    let (digits, scale) = value.as_bigint_and_exponent();
    if digits.sign() == Sign::NoSign {
        return (0, 0);
    }
    let spelled = digits.magnitude().to_string();
    // Position of the most significant digit, as a power of ten.
    let most_significant = i64::try_from(spelled.len()).unwrap_or(i64::MAX) - 1 - scale;
    let weight = most_significant.div_euclid(PER_WORD);
    // The leading word holds every digit from the word's top down to the
    // most significant one, zero-padded when the value has no more.
    let width = usize::try_from(most_significant - weight * PER_WORD + 1).unwrap_or(1);
    let mut lead = spelled;
    lead.truncate(width);
    while lead.len() < width {
        lead.push('0');
    }
    (weight, lead.parse::<i64>().unwrap_or(0))
}
