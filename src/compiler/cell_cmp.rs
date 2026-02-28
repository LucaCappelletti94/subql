//! Shared cell comparison helpers used by both the VM and the prefilter planner.
//!
//! Centralising these functions ensures identical comparison semantics across
//! all evaluation paths (bytecode VM, prefilter pruning).

use crate::{compiler::Tri, Cell};

/// Structural equality for two cells, with cross-type Int/Float coercion.
///
/// Returns `true`  when values are equal (including mixed Int ↔ Float).
/// Returns `false` for type mismatches that cannot be coerced, or when either
/// operand is NULL / Missing.
#[allow(clippy::cast_precision_loss)]
pub fn cells_equal(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Bool(x), Cell::Bool(y)) => x == y,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Float(x), Cell::Float(y)) => x
            .partial_cmp(y)
            .is_some_and(|ord| ord == std::cmp::Ordering::Equal),
        // Mixed numeric comparisons: coerce to float
        (Cell::Int(x), Cell::Float(y)) => {
            let xf = *x as f64;
            xf.partial_cmp(y)
                .is_some_and(|ord| ord == std::cmp::Ordering::Equal)
        }
        (Cell::Float(x), Cell::Int(y)) => {
            let yf = *y as f64;
            x.partial_cmp(&yf)
                .is_some_and(|ord| ord == std::cmp::Ordering::Equal)
        }
        (Cell::String(x), Cell::String(y)) => x == y,
        // NULL = NULL is Unknown, not True; all other mismatches → false
        _ => false,
    }
}

/// Ordered comparison for two cells, with cross-type Int/Float coercion.
///
/// Returns `Tri::Unknown` when either operand is NULL / Missing, NaN, or when
/// the types are incompatible (e.g. Bool vs Int).
#[allow(clippy::many_single_char_names, clippy::cast_precision_loss)]
pub fn compare_ordered_cells<F>(lhs: &Cell, rhs: &Cell, predicate: F) -> Tri
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    // NULL or Missing → Unknown
    if lhs.is_null() || lhs.is_missing() || rhs.is_null() || rhs.is_missing() {
        return Tri::Unknown;
    }

    let ord = match (lhs, rhs) {
        (Cell::Int(x), Cell::Int(y)) => x.cmp(y),
        (Cell::Float(x), Cell::Float(y)) => {
            if x.is_nan() || y.is_nan() {
                return Tri::Unknown;
            }
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        // Mixed Int/Float comparisons — coerce to Float
        (Cell::Int(x), Cell::Float(y)) => {
            let x_float = *x as f64;
            if y.is_nan() {
                return Tri::Unknown;
            }
            x_float.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Cell::Float(x), Cell::Int(y)) => {
            let y_float = *y as f64;
            if x.is_nan() {
                return Tri::Unknown;
            }
            x.partial_cmp(&y_float).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Cell::String(x), Cell::String(y)) => x.cmp(y),
        _ => return Tri::Unknown, // Type mismatch
    };

    if predicate(ord) {
        Tri::True
    } else {
        Tri::False
    }
}
