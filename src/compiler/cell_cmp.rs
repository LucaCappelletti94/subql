//! Shared cell comparison helpers used by both the VM and the prefilter planner.
//!
//! Centralising these functions ensures identical comparison semantics across
//! all evaluation paths (bytecode VM, prefilter pruning).

use crate::{compiler::Tri, Cell};

/// Structural equality for two cells, with cross-type Int/Float coercion.
///
/// Returns `true`  when values are equal (including mixed Int/Float).
/// Returns `false` for type mismatches that cannot be coerced, or when either
/// operand is NULL / Missing.
#[allow(clippy::cast_precision_loss)]
pub fn cells_equal(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Bool(x), Cell::Bool(y)) => x == y,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Float(x), Cell::Float(y)) => x
            .partial_cmp(y)
            .is_some_and(|ord| ord == core::cmp::Ordering::Equal),
        // Mixed numeric comparisons: coerce to float
        (Cell::Int(x), Cell::Float(y)) => {
            let xf = *x as f64;
            xf.partial_cmp(y)
                .is_some_and(|ord| ord == core::cmp::Ordering::Equal)
        }
        (Cell::Float(x), Cell::Int(y)) => {
            let yf = *y as f64;
            x.partial_cmp(&yf)
                .is_some_and(|ord| ord == core::cmp::Ordering::Equal)
        }
        (Cell::String(x), Cell::String(y)) => x == y,
        // NULL = NULL is Unknown, not True. All other mismatches are false
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
    F: FnOnce(core::cmp::Ordering) -> bool,
{
    // NULL or Missing -> Unknown
    if lhs.is_null() || lhs.is_missing() || rhs.is_null() || rhs.is_missing() {
        return Tri::Unknown;
    }

    let ord = match (lhs, rhs) {
        (Cell::Int(x), Cell::Int(y)) => x.cmp(y),
        (Cell::Float(x), Cell::Float(y)) => {
            if x.is_nan() || y.is_nan() {
                return Tri::Unknown;
            }
            x.partial_cmp(y).unwrap_or(core::cmp::Ordering::Equal)
        }
        // Mixed Int/Float comparisons: coerce to Float
        (Cell::Int(x), Cell::Float(y)) => {
            let x_float = *x as f64;
            if y.is_nan() {
                return Tri::Unknown;
            }
            x_float.partial_cmp(y).unwrap_or(core::cmp::Ordering::Equal)
        }
        (Cell::Float(x), Cell::Int(y)) => {
            let y_float = *y as f64;
            if x.is_nan() {
                return Tri::Unknown;
            }
            x.partial_cmp(&y_float)
                .unwrap_or(core::cmp::Ordering::Equal)
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    //! Property-based tests for [`cells_equal`] and
    //! [`compare_ordered_cells`].
    //!
    //! These two functions underpin every predicate evaluation in
    //! subql (VM, prefilter, dispatch). Their NULL / Missing / NaN
    //! semantics in particular need to be airtight, since divergence
    //! between the VM and the prefilter would let predicates
    //! short-circuit incorrectly on real CDC traffic.
    //!
    //! Properties tested
    //! 1. **Equality is symmetric.** `cells_equal(a, b) == cells_equal(b, a)`.
    //! 2. **Present-cell reflexivity.** `cells_equal(x, x)` is `true`
    //!    for any non-NULL, non-Missing, non-NaN cell.
    //! 3. **NULL/Missing/NaN are not self-equal.** `cells_equal(x, x)`
    //!    is `false` for `Null`, `Missing`, and `Float(NaN)`.
    //! 4. **Int/Float coercion is consistent.** For every `i64 n`,
    //!    `cells_equal(Int(n), Float(n as f64))` is `true`.
    //! 5. **Ordered comparison short-circuits on NULL/Missing.** Any
    //!    NULL or Missing operand collapses to `Tri::Unknown`
    //!    regardless of the predicate.
    //! 6. **Ordered comparison short-circuits on NaN.** Any `Float(NaN)`
    //!    operand collapses to `Tri::Unknown`.
    //! 7. **Ordered comparison short-circuits on type mismatch.**
    //!    Mixing a `Bool` with anything but a `Bool`, or a `String`
    //!    with anything but a `String`, yields `Tri::Unknown`.
    //! 8. **Strict + non-strict are consistent.** `compare(a, b, |o| o
    //!    == Less)` and `compare(a, b, |o| o != Less)` partition the
    //!    `Tri::True` / `Tri::False` outcomes between them whenever
    //!    the result is not `Unknown`.

    use super::*;
    use alloc::string::String;
    use alloc::sync::Arc;
    use core::cmp::Ordering;
    use proptest::prelude::*;

    /// Strategy producing every `Cell` variant the comparison helpers
    /// need to handle, including NULL, Missing, and `Float(NaN)`. We
    /// keep the integer / float / string ranges small so shrinking
    /// produces tractable counterexamples.
    fn arb_cell() -> impl Strategy<Value = Cell> {
        prop_oneof![
            1 => Just(Cell::Null),
            1 => Just(Cell::Missing),
            2 => any::<bool>().prop_map(Cell::Bool),
            3 => (-32i64..=32).prop_map(Cell::Int),
            3 => prop_oneof![
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                (-32.0f64..=32.0).prop_map(|f| f),
            ].prop_map(Cell::Float),
            2 => "[a-c]{0,3}".prop_map(|s: String| Cell::String(Arc::from(s.as_str()))),
        ]
    }

    fn is_present(c: &Cell) -> bool {
        c.is_present()
    }

    fn is_nan(c: &Cell) -> bool {
        matches!(c, Cell::Float(f) if f.is_nan())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 512,
            ..ProptestConfig::default()
        })]

        /// Equality is symmetric in both arguments.
        #[test]
        fn cells_equal_is_symmetric(a in arb_cell(), b in arb_cell()) {
            prop_assert_eq!(cells_equal(&a, &b), cells_equal(&b, &a));
        }

        /// Reflexivity for present, non-NaN cells.
        #[test]
        fn cells_equal_is_reflexive_for_present_non_nan(c in arb_cell()) {
            if is_present(&c) && !is_nan(&c) {
                prop_assert!(
                    cells_equal(&c, &c),
                    "present non-NaN cell {:?} not equal to itself",
                    c,
                );
            }
        }

        /// `Null`, `Missing`, and `Float(NaN)` are never self-equal.
        /// These are the three cell shapes that should propagate as
        /// `Unknown` in SQL three-valued logic.
        #[test]
        fn null_missing_nan_are_not_self_equal(c in arb_cell()) {
            if !is_present(&c) || is_nan(&c) {
                prop_assert!(
                    !cells_equal(&c, &c),
                    "non-present-or-NaN cell {:?} unexpectedly self-equal",
                    c,
                );
            }
        }

        /// `Int(n) == Float(n as f64)` for every representable `n` in
        /// the strategy range. Floats can exactly represent any
        /// `i64` in `[-2^53, 2^53]`, well outside our `[-32, 32]`
        /// range.
        #[test]
        fn int_float_coercion_is_consistent(n in -32i64..=32) {
            let i = Cell::Int(n);
            #[allow(clippy::cast_precision_loss)]
            let f = Cell::Float(n as f64);
            prop_assert!(cells_equal(&i, &f), "Int({n}) != Float({n}.0)");
            prop_assert!(cells_equal(&f, &i), "Float({n}.0) != Int({n})");
        }

        /// Ordered comparison collapses to `Unknown` whenever either
        /// operand is NULL or Missing, regardless of the predicate.
        #[test]
        fn ordered_cmp_unknown_on_null_or_missing(a in arb_cell(), b in arb_cell()) {
            if !is_present(&a) || !is_present(&b) {
                for pred in [Ordering::is_lt as fn(Ordering) -> bool, Ordering::is_le, Ordering::is_eq, Ordering::is_gt, Ordering::is_ge] {
                    let got = compare_ordered_cells(&a, &b, pred);
                    prop_assert_eq!(got, Tri::Unknown, "expected Unknown for null/missing operand, got {:?} for ({:?}, {:?})", got, a, b);
                }
            }
        }

        /// Ordered comparison collapses to `Unknown` whenever either
        /// `Float` operand is NaN.
        #[test]
        fn ordered_cmp_unknown_on_nan(a in arb_cell(), b in arb_cell()) {
            if (is_nan(&a) || is_nan(&b)) && is_present(&a) && is_present(&b) {
                let got = compare_ordered_cells(&a, &b, Ordering::is_lt);
                prop_assert_eq!(got, Tri::Unknown, "expected Unknown for NaN operand, got {:?} for ({:?}, {:?})", got, a, b);
            }
        }

        /// `<` and `>=` partition the `Tri::True`/`Tri::False` space
        /// when the comparison is defined: exactly one is `True`, the
        /// other is `False`. (When the result is `Unknown` the
        /// partition is vacuous, so we only assert the non-Unknown
        /// case.)
        #[test]
        fn lt_and_ge_partition_defined_pairs(a in arb_cell(), b in arb_cell()) {
            let lt = compare_ordered_cells(&a, &b, Ordering::is_lt);
            let ge = compare_ordered_cells(&a, &b, Ordering::is_ge);
            if lt != Tri::Unknown && ge != Tri::Unknown {
                prop_assert_ne!(lt, ge, "`<` and `>=` agreed on ({:?}, {:?})", a, b);
            }
        }

        /// Type-mismatched non-NULL operands (e.g. `Bool` vs `Int`,
        /// `String` vs `Float`) collapse to `Unknown`. The compatible
        /// pairs are `(Int|Float, Int|Float)`, `(String, String)`,
        /// `(Bool, Bool)`.
        #[test]
        fn ordered_cmp_unknown_on_type_mismatch(a in arb_cell(), b in arb_cell()) {
            if !is_present(&a) || !is_present(&b) || is_nan(&a) || is_nan(&b) {
                return Ok(());
            }
            let compatible = matches!(
                (&a, &b),
                (Cell::Int(_) | Cell::Float(_), Cell::Int(_) | Cell::Float(_))
                | (Cell::String(_), Cell::String(_))
                | (Cell::Bool(_), Cell::Bool(_)),
            );
            if !compatible {
                let got = compare_ordered_cells(&a, &b, Ordering::is_lt);
                prop_assert_eq!(
                    got,
                    Tri::Unknown,
                    "incompatible types ({:?}, {:?}) did not yield Unknown",
                    a,
                    b,
                );
            }
        }
    }
}
