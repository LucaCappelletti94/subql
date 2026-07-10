//! Shared value comparison helpers used by both the VM and the prefilter
//! planner.
//!
//! Centralising these functions ensures identical comparison semantics
//! across every evaluation path (bytecode VM, prefilter pruning).
//!
//! # Same-scalar rule
//!
//! Every comparison here is same-scalar-only. Two [`Value`]s carrying
//! different [`crate::backend::Backend`] scalar variants never compare equal
//! ([`values_equal`] returns `false`) and never compare ordered
//! ([`compare_ordered_values`] returns [`Tri::Unknown`]). The VM MUST NOT
//! coerce `Int` to `Float` or otherwise reinterpret scalars — the compiler
//! is expected to have emitted an explicit cast if the query needed one.
//! See `docs/refactor-cdc-event-handoff.md` gotcha 4 for the rationale.
//!
//! # Ordered-comparison support
//!
//! [`compare_ordered_values`] is defined for the ten scalar variants whose
//! [`Backend`](crate::backend::Backend) associated types carry a
//! [`PartialOrd`] bound (`Int`, `Float`, `String`, `Bytes`, `Uuid`,
//! `Timestamp`, `TimestampTz`, `Date`, `Time`, `Decimal`). `Bool`, `Json`,
//! and `Jsonb` intentionally lack ordering in the backend trait; ordered
//! comparison on those variants collapses to `Tri::Unknown` (same as SQL
//! `<`/`>` on boolean/json values).
//!
//! # NaN
//!
//! `PartialOrd::partial_cmp` returns `None` on NaN operands; the helper
//! propagates that as `Tri::Unknown` rather than crashing.

use crate::backend::{Backend, Value};
use crate::compiler::Tri;

/// Structural equality between two same-scalar [`Value`]s.
///
/// * Both operands carrying the same scalar variant with equal payload -> `true`.
/// * Both operands carrying the same scalar variant with unequal payload -> `false`.
/// * Cross-scalar mismatch -> `false`.
/// * Either operand `Missing` or `Null` -> `false` (the VM lifts those to
///   `Tri::Unknown` before calling this helper; this function itself never
///   returns tri-state).
/// * `Value::Float(NaN)` == anything -> `false` (SQL/IEEE rule; `NaN != NaN`).
pub fn values_equal<B: Backend>(a: &Value<B>, b: &Value<B>) -> bool {
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bytes(x), Value::Bytes(y)) => x == y,
        (Value::Uuid(x), Value::Uuid(y)) => x == y,
        (Value::Timestamp(x), Value::Timestamp(y)) => x == y,
        (Value::TimestampTz(x), Value::TimestampTz(y)) => x == y,
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::Time(x), Value::Time(y)) => x == y,
        (Value::Decimal(x), Value::Decimal(y)) => x == y,
        (Value::Json(x), Value::Json(y)) => x == y,
        (Value::Jsonb(x), Value::Jsonb(y)) => x == y,
        // Missing/Null and cross-scalar pairs are never equal here.
        _ => false,
    }
}

/// Ordered comparison between two same-scalar [`Value`]s under a caller
/// predicate on [`core::cmp::Ordering`].
///
/// * Either operand `Missing` or `Null` -> `Tri::Unknown`.
/// * Cross-scalar mismatch -> `Tri::Unknown`.
/// * Variant lacks a `PartialOrd` bound in [`Backend`] (`Bool`, `Json`,
///   `Jsonb`) -> `Tri::Unknown`.
/// * `partial_cmp` returns `None` (NaN operand on `Float`) -> `Tri::Unknown`.
/// * Otherwise the predicate is invoked on the resolved `Ordering` and the
///   result is lifted to `Tri::True` / `Tri::False`.
pub fn compare_ordered_values<B, F>(lhs: &Value<B>, rhs: &Value<B>, predicate: F) -> Tri
where
    B: Backend,
    F: FnOnce(core::cmp::Ordering) -> bool,
{
    // Missing / Null -> Unknown.
    if lhs.is_absent() || rhs.is_absent() {
        return Tri::Unknown;
    }

    let ord = match (lhs, rhs) {
        (Value::Int(x), Value::Int(y)) => x.partial_cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::String(x), Value::String(y)) => x.partial_cmp(y),
        (Value::Bytes(x), Value::Bytes(y)) => x.partial_cmp(y),
        (Value::Uuid(x), Value::Uuid(y)) => x.partial_cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.partial_cmp(y),
        (Value::TimestampTz(x), Value::TimestampTz(y)) => x.partial_cmp(y),
        (Value::Date(x), Value::Date(y)) => x.partial_cmp(y),
        (Value::Time(x), Value::Time(y)) => x.partial_cmp(y),
        (Value::Decimal(x), Value::Decimal(y)) => x.partial_cmp(y),
        // Bool / Json / Jsonb have no PartialOrd bound in Backend.
        // Cross-scalar mismatch also lands here.
        _ => return Tri::Unknown,
    };

    match ord {
        Some(o) if predicate(o) => Tri::True,
        Some(_) => Tri::False,
        None => Tri::Unknown,
    }
}

#[cfg(any())] // Phase 10 test port pending
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    //! Property-based tests for [`values_equal`] and
    //! [`compare_ordered_values`].
    //!
    //! These two functions underpin every predicate evaluation in subql
    //! (VM, prefilter, dispatch). Their `Missing` / `Null` / NaN /
    //! cross-scalar semantics need to be airtight: divergence between the
    //! VM and the prefilter would let predicates short-circuit incorrectly
    //! on real CDC traffic.
    //!
    //! Properties covered
    //! 1. **Equality is symmetric.** `values_equal(a, b) == values_equal(b, a)`.
    //! 2. **Present-cell reflexivity.** `values_equal(x, x)` is `true` for
    //!    any concrete non-NaN scalar `Value`.
    //! 3. **`Missing` / `Null` / `Float(NaN)` are not self-equal.**
    //! 4. **Cross-scalar equality is always false.** No coercion — SQL
    //!    semantics is that mixed scalar comparisons never satisfy `=`
    //!    without an explicit cast.
    //! 5. **Ordered comparison collapses on `Missing` / `Null`.**
    //! 6. **Ordered comparison collapses on NaN.**
    //! 7. **Ordered comparison collapses on cross-scalar and on scalars
    //!    without a `PartialOrd` bound (`Bool`, `Json`, `Jsonb`).**
    //! 8. **Strict + non-strict are consistent.** `<` and `>=` partition
    //!    the `Tri::True` / `Tri::False` outcomes whenever the resolved
    //!    ordering is defined.

    use super::*;
    use crate::backend::Postgres;
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;
    use core::cmp::Ordering;
    use proptest::prelude::*;

    /// Strategy producing every `Value<Postgres>` variant the comparison
    /// helpers need to handle, including `Missing`, `Null`, and
    /// `Float(NaN)`. Ranges are small so shrinking produces tractable
    /// counter-examples.
    fn arb_value() -> impl Strategy<Value = Value<Postgres>> {
        prop_oneof![
            1 => Just(Value::Missing),
            1 => Just(Value::Null),
            2 => any::<bool>().prop_map(Value::Bool),
            3 => (-32i64..=32).prop_map(Value::Int),
            3 => prop_oneof![
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                (-32.0f64..=32.0),
            ].prop_map(Value::Float),
            2 => "[a-c]{0,3}".prop_map(|s: String| Value::String(s)),
            2 => proptest::collection::vec(any::<u8>(), 0..=4).prop_map(Value::Bytes),
        ]
    }

    fn is_present<B: Backend>(v: &Value<B>) -> bool {
        !v.is_absent()
    }

    fn is_nan(v: &Value<Postgres>) -> bool {
        matches!(v, Value::Float(f) if f.is_nan())
    }

    /// Two values are "same scalar variant" when they can meaningfully
    /// participate in a same-scalar comparison. Missing/Null pairs are
    /// excluded (they collapse to Unknown regardless).
    fn same_variant(a: &Value<Postgres>, b: &Value<Postgres>) -> bool {
        core::mem::discriminant(a) == core::mem::discriminant(b) && is_present(a)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 512,
            ..ProptestConfig::default()
        })]

        /// Equality is symmetric in both arguments.
        #[test]
        fn values_equal_is_symmetric(a in arb_value(), b in arb_value()) {
            prop_assert_eq!(values_equal(&a, &b), values_equal(&b, &a));
        }

        /// Reflexivity for present, non-NaN values.
        #[test]
        fn values_equal_is_reflexive_for_present_non_nan(v in arb_value()) {
            if is_present(&v) && !is_nan(&v) {
                prop_assert!(
                    values_equal(&v, &v),
                    "present non-NaN value {:?} not equal to itself",
                    v,
                );
            }
        }

        /// `Missing`, `Null`, and `Float(NaN)` are never self-equal. These
        /// three shapes are the ones the VM lifts to `Tri::Unknown`.
        #[test]
        fn null_missing_nan_are_not_self_equal(v in arb_value()) {
            if !is_present(&v) || is_nan(&v) {
                prop_assert!(
                    !values_equal(&v, &v),
                    "non-present-or-NaN value {:?} unexpectedly self-equal",
                    v,
                );
            }
        }

        /// Cross-scalar pairs never compare equal (no coercion).
        #[test]
        fn cross_scalar_pairs_are_never_equal(a in arb_value(), b in arb_value()) {
            if is_present(&a) && is_present(&b)
                && core::mem::discriminant(&a) != core::mem::discriminant(&b)
            {
                prop_assert!(
                    !values_equal(&a, &b),
                    "cross-scalar pair ({:?}, {:?}) reported equal",
                    a,
                    b,
                );
            }
        }

        /// Ordered comparison collapses to `Unknown` whenever either
        /// operand is `Missing` or `Null`.
        #[test]
        fn ordered_cmp_unknown_on_null_or_missing(a in arb_value(), b in arb_value()) {
            if !is_present(&a) || !is_present(&b) {
                let preds: [fn(Ordering) -> bool; 5] = [
                    Ordering::is_lt,
                    Ordering::is_le,
                    Ordering::is_eq,
                    Ordering::is_gt,
                    Ordering::is_ge,
                ];
                for pred in preds {
                    let got = compare_ordered_values(&a, &b, pred);
                    prop_assert_eq!(
                        got,
                        Tri::Unknown,
                        "expected Unknown for missing/null operand, got {:?} for ({:?}, {:?})",
                        got,
                        a,
                        b,
                    );
                }
            }
        }

        /// Ordered comparison collapses to `Unknown` whenever either
        /// `Float` operand is NaN.
        #[test]
        fn ordered_cmp_unknown_on_nan(a in arb_value(), b in arb_value()) {
            if (is_nan(&a) || is_nan(&b)) && is_present(&a) && is_present(&b) {
                let got = compare_ordered_values(&a, &b, Ordering::is_lt);
                prop_assert_eq!(
                    got,
                    Tri::Unknown,
                    "expected Unknown for NaN operand, got {:?} for ({:?}, {:?})",
                    got,
                    a,
                    b,
                );
            }
        }

        /// Cross-scalar pairs, and same-scalar pairs on variants without a
        /// `PartialOrd` bound (`Bool`, `Json`, `Jsonb`), collapse to
        /// `Tri::Unknown`.
        #[test]
        fn ordered_cmp_unknown_on_incomparable(a in arb_value(), b in arb_value()) {
            if !is_present(&a) || !is_present(&b) || is_nan(&a) || is_nan(&b) {
                return Ok(());
            }
            let cross_scalar =
                core::mem::discriminant(&a) != core::mem::discriminant(&b);
            let no_partial_ord = matches!(&a, Value::Bool(_)) && same_variant(&a, &b);
            if cross_scalar || no_partial_ord {
                let got = compare_ordered_values(&a, &b, Ordering::is_lt);
                prop_assert_eq!(
                    got,
                    Tri::Unknown,
                    "incomparable pair ({:?}, {:?}) did not yield Unknown",
                    a,
                    b,
                );
            }
        }

        /// `<` and `>=` partition the `Tri::True`/`Tri::False` space on
        /// defined comparisons: exactly one holds, the other does not.
        /// `Unknown` results are excluded from the assertion (they can be
        /// mutually vacuous).
        #[test]
        fn lt_and_ge_partition_defined_pairs(a in arb_value(), b in arb_value()) {
            let lt = compare_ordered_values(&a, &b, Ordering::is_lt);
            let ge = compare_ordered_values(&a, &b, Ordering::is_ge);
            if lt != Tri::Unknown && ge != Tri::Unknown {
                prop_assert_ne!(lt, ge, "`<` and `>=` agreed on ({:?}, {:?})", a, b);
            }
        }
    }

    /// Sanity check: cross-scalar equality on hand-picked concrete values
    /// returns `false` (no coercion).
    #[test]
    fn cross_scalar_equality_is_false_for_concrete_values() {
        let int_one = Value::<Postgres>::Int(1);
        let float_one = Value::<Postgres>::Float(1.0);
        assert!(!values_equal(&int_one, &float_one));
        assert!(!values_equal(&float_one, &int_one));

        let str_one = Value::<Postgres>::String("1".to_string());
        assert!(!values_equal(&int_one, &str_one));
    }

    /// Sanity check: ordered comparison on `Bool` is `Unknown` because
    /// `Backend::Bool` has no `PartialOrd` bound.
    #[test]
    fn bool_ordered_comparison_is_unknown() {
        let f = Value::<Postgres>::Bool(false);
        let t = Value::<Postgres>::Bool(true);
        assert_eq!(
            compare_ordered_values(&f, &t, Ordering::is_lt),
            Tri::Unknown
        );
        assert_eq!(
            compare_ordered_values(&t, &f, Ordering::is_lt),
            Tri::Unknown
        );
    }

    /// `Bytes` scalars compare by lexicographic byte order (matches
    /// `Vec<u8>::cmp`).
    #[test]
    fn bytes_ordered_comparison_is_lexicographic() {
        let a = Value::<Postgres>::Bytes(vec![0, 1, 2]);
        let b = Value::<Postgres>::Bytes(vec![0, 1, 3]);
        assert_eq!(compare_ordered_values(&a, &b, Ordering::is_lt), Tri::True);
        assert_eq!(compare_ordered_values(&b, &a, Ordering::is_lt), Tri::False);
    }
}
