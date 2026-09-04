//! Arithmetic instructions split out of `vm`.

use super::refusal::{ArithmeticOp, DivisionByZero, EvaluationRefusal, IntegerOverflow};
use crate::backend::{Backend, Value};

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
) -> Result<Value<B>, EvaluationRefusal> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return Ok(null);
    }
    Ok(match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if let Some(null) = zero_divisor::<B, _>(&y, ArithmeticOp::Divide)? {
                return Ok(null);
            }
            return B::integer_binary(ArithmeticOp::Divide, x, y);
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
            Value::Decimal(x / y)
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
