//! Arithmetic instructions split out of `vm`.

use crate::backend::{Backend, Value};

/// `Value::Missing` / `Value::Null` on either side propagates to
/// `Value::Null` (SQL NULL propagation).
pub(super) const fn null_propagate_binary<B: Backend>(
    a: &Value<B>,
    b: &Value<B>,
) -> Option<Value<B>> {
    if a.is_absent() || b.is_absent() {
        Some(Value::Null)
    } else {
        None
    }
}

/// Add: same-scalar only.
pub(super) fn arithmetic_add<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x + y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x + y),
        _ => Value::Null,
    }
}

/// Subtract: same-scalar only.
pub(super) fn arithmetic_subtract<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x - y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x - y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x - y),
        _ => Value::Null,
    }
}

/// Multiply: same-scalar only.
pub(super) fn arithmetic_multiply<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Value::Int(x * y),
        (Value::Float(x), Value::Float(y)) => Value::Float(x * y),
        (Value::Decimal(x), Value::Decimal(y)) => Value::Decimal(x * y),
        _ => Value::Null,
    }
}

/// Divide: same-scalar only. Division by zero yields `Value::Null`.
///
/// Zero is detected by `b - b == b`, an identity that holds only for the
/// additive identity of a `Sub<Output = Self> + PartialEq` type. This
/// avoids adding a `Zero` bound to `Backend`.
pub(super) fn arithmetic_divide<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Int(x / y)
            }
        }
        (Value::Float(x), Value::Float(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Float(x / y)
            }
        }
        (Value::Decimal(x), Value::Decimal(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Decimal(x / y)
            }
        }
        _ => Value::Null,
    }
}

/// Modulo: `Int % Int` only. Modulo by zero yields `Value::Null`.
pub(super) fn arithmetic_modulo<B: Backend>(a: Value<B>, b: Value<B>) -> Value<B> {
    if let Some(null) = null_propagate_binary(&a, &b) {
        return null;
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            if is_zero_scalar(&y) {
                Value::Null
            } else {
                Value::Int(x % y)
            }
        }
        _ => Value::Null,
    }
}

/// Negate: same-scalar only.
pub(super) fn arithmetic_negate<B: Backend>(a: Value<B>) -> Value<B> {
    if a.is_absent() {
        return Value::Null;
    }
    match a {
        Value::Int(x) => Value::Int(-x),
        Value::Float(x) => Value::Float(-x),
        Value::Decimal(x) => Value::Decimal(-x),
        _ => Value::Null,
    }
}

/// Trait-generic "is zero" check.
///
/// A scalar `x` is zero iff `x - x == x` (holds only for the additive
/// identity of a `Sub<Output = Self> + PartialEq` type). Backend requires
/// both bounds on `Int` / `Float` / `Decimal`, so this specialises cleanly
/// per scalar without a `num_traits::Zero` bound.
pub(super) fn is_zero_scalar<T>(x: &T) -> bool
where
    T: Clone + PartialEq + core::ops::Sub<Output = T>,
{
    let cleared = x.clone() - x.clone();
    &cleared == x
}
