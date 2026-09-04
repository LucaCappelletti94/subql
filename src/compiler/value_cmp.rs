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

use crate::backend::{Backend, ComparisonContext, Value};
use crate::compiler::Tri;

/// Structural equality between two same-scalar [`Value`]s: the default a
/// backend inherits from [`Backend::scalars_equal`].
///
/// * Same scalar variant, equal payload -> `true`; unequal payload -> `false`.
/// * Cross-scalar mismatch -> `false`.
/// * Either operand `Missing` or `Null` -> `false` (the VM lifts those to
///   `Tri::Unknown` before asking; this function never returns tri-state).
/// * `Value::Float(NaN)` against anything -> `false`, the IEEE rule.
///   PostgreSQL disagrees, which is why a backend can override it.
pub fn structural_equality<B: Backend>(a: &Value<B>, b: &Value<B>) -> bool {
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
        (Value::Jsonb(x), Value::Jsonb(y)) => crate::backend::jsonb_payloads_equal::<B>(x, y),
        // A custom pair compares by the value the type's own conversion
        // produced, so two spellings the conversion maps together are equal.
        // Without this arm the wildcard answers `false` and a filter on a
        // custom column registers and then never fires.
        (Value::Custom(x), Value::Custom(y)) => x == y,
        // Missing/Null and cross-scalar pairs are never equal here.
        _ => false,
    }
}

/// Structural ordering between two same-scalar [`Value`]s: the default a
/// backend inherits from [`Backend::compare_scalars`].
///
/// `None` means the pair has no defined order: `Missing`, `Null`, a
/// cross-scalar pair, a variant with no `PartialOrd` bound (`Bool`, `Json`,
/// `Jsonb`), or a NaN operand.
pub fn structural_ordering<B: Backend>(
    lhs: &Value<B>,
    rhs: &Value<B>,
) -> Option<core::cmp::Ordering> {
    if lhs.is_absent() || rhs.is_absent() {
        return None;
    }
    match (lhs, rhs) {
        (Value::Bool(x), Value::Bool(y)) => x.partial_cmp(y),
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
        // Json / Jsonb have no order here; a cross-scalar mismatch also
        // lands on this arm.
        _ => None,
    }
}

/// Equality as backend `B` answers it for this pair of operands.
///
/// Every evaluation path (VM, prefilter, re-execution maintenance) asks
/// here, so one backend rule serves them all.
pub fn values_equal<B: Backend>(
    comparison: ComparisonContext<'_, B>,
    a: &Value<B>,
    b: &Value<B>,
) -> bool {
    B::scalars_equal(comparison, a, b)
}

/// Ordered comparison as backend `B` answers it, lifted to [`Tri`] under a
/// caller predicate on [`core::cmp::Ordering`].
///
/// `Tri::Unknown` whenever the backend reports no defined order.
pub fn compare_ordered_values<B, F>(
    comparison: ComparisonContext<'_, B>,
    lhs: &Value<B>,
    rhs: &Value<B>,
    predicate: F,
) -> Tri
where
    B: Backend,
    F: FnOnce(core::cmp::Ordering) -> bool,
{
    match B::compare_scalars(comparison, lhs, rhs) {
        Some(ordering) if predicate(ordering) => Tri::True,
        Some(_) => Tri::False,
        None => Tri::Unknown,
    }
}

/// The descriptor the compiler resolves per compared column, and the backend
/// trait that consults it.
#[cfg(all(test, feature = "std"))]
#[allow(clippy::unwrap_used)]
mod comparison_descriptor_tests {
    use crate::backend::{Backend, ColumnComparisonOf, Postgres, TextKey};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;

    fn comparison(ddl: &str, column: &str) -> ColumnComparisonOf<Postgres> {
        let db = ParserDB::parse::<PostgreSqlDialect>(ddl).expect("the DDL parses");
        let table = crate::catalog_helpers::table_id(&db, "t").expect("t is cataloged");
        let column_id = crate::catalog_helpers::column_id(&db, table, column).expect("the column");
        crate::catalog_helpers::column_comparison::<Postgres, _>(&db, table, column_id)
            .expect("the column classifies")
    }

    #[test]
    fn descriptor_reports_char_padding_from_the_declared_type() {
        for ddl in [
            "CREATE TABLE t (code CHAR(5));",
            "CREATE TABLE t (code CHARACTER(5));",
        ] {
            assert!(
                comparison(ddl, "code").is_blank_padded(),
                "a declared char type is blank padded: {ddl}"
            );
        }
        for ddl in [
            "CREATE TABLE t (code VARCHAR(5));",
            "CREATE TABLE t (code TEXT);",
        ] {
            assert!(
                !comparison(ddl, "code").is_blank_padded(),
                "a varying type keeps trailing spaces: {ddl}"
            );
        }
    }

    #[test]
    fn descriptor_refuses_a_nondeterministic_collation() {
        let ci = comparison(
            "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', \
             deterministic = false); CREATE TABLE t (name TEXT COLLATE ci);",
            "name",
        );
        assert_eq!(<Postgres as Backend>::text_key(&ci), None);
        let plain = comparison("CREATE TABLE t (name TEXT);", "name");
        assert_eq!(
            <Postgres as Backend>::text_key(&plain),
            Some(TextKey::Exact)
        );
    }

    #[test]
    fn bytecode_carries_the_resolved_descriptor() {
        let db =
            ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE t (id INT PRIMARY KEY, name TEXT);")
                .expect("the DDL parses");
        let compiled = crate::compiler::parse_compile_normalize_and_prefilter::<Postgres, _>(
            "SELECT * FROM t WHERE name = 'x'",
            &PostgreSqlDialect {},
            &db,
        )
        .expect("the query compiles");
        let name = crate::catalog_helpers::column_id(&db, 0, "name").expect("the column");
        let carried = compiled
            .program
            .comparison_for(name)
            .expect("the compiler resolved the compared column's descriptor");
        assert_eq!(carried.declared_type, "TEXT");
    }
}

#[cfg(test)]
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

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 512,
            ..ProptestConfig::default()
        })]

        /// Equality is symmetric in both arguments.
        #[test]
        fn values_equal_is_symmetric(a in arb_value(), b in arb_value()) {
            prop_assert_eq!(values_equal(ComparisonContext::none(), &a, &b), values_equal(ComparisonContext::none(), &b, &a));
        }

        /// Reflexivity for present, non-NaN values.
        #[test]
        fn values_equal_is_reflexive_for_present_values(v in arb_value()) {
            if is_present(&v) {
                prop_assert!(
                    values_equal(ComparisonContext::none(), &v, &v),
                    "present value {:?} not equal to itself",
                    v,
                );
            }
        }

        /// `Missing` and `Null` are never self-equal, which is what the VM
        /// lifts to `Tri::Unknown`. A NaN is not in that set under this
        /// backend: PostgreSQL answers `NaN = NaN` true.
        #[test]
        fn null_and_missing_are_not_self_equal(v in arb_value()) {
            if !is_present(&v) {
                prop_assert!(
                    !values_equal(ComparisonContext::none(), &v, &v),
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
                    !values_equal(ComparisonContext::none(), &a, &b),
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
                    let got = compare_ordered_values(ComparisonContext::none(), &a, &b, pred);
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

        /// NaN follows PostgreSQL's float order: above every non-NaN value
        /// and equal to another NaN, rather than IEEE's no-order. A NaN
        /// against a non-float is still a cross-scalar pair.
        #[test]
        fn ordered_cmp_follows_postgres_nan_order(a in arb_value(), b in arb_value()) {
            if (is_nan(&a) || is_nan(&b)) && is_present(&a) && is_present(&b) {
                let got = compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_lt);
                let expected = match (&a, &b) {
                    (Value::Float(_), Value::Float(_)) if is_nan(&a) => Tri::False,
                    (Value::Float(_), Value::Float(_)) => Tri::True,
                    _ => Tri::Unknown,
                };
                prop_assert_eq!(
                    got,
                    expected,
                    "expected {:?} for ({:?}, {:?}), got {:?}",
                    expected,
                    a,
                    b,
                    got,
                );
            }
        }

        /// Booleans order, `false` below `true`, because SQL orders them and
        /// the engines agree.
        #[test]
        fn ordered_cmp_orders_booleans(a in arb_value(), b in arb_value()) {
            if let (Value::Bool(x), Value::Bool(y)) = (&a, &b) {
                let got = compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_lt);
                let expected = if !x && *y { Tri::True } else { Tri::False };
                prop_assert_eq!(
                    got,
                    expected,
                    "expected {:?} for ({:?}, {:?})",
                    expected,
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
            if core::mem::discriminant(&a) != core::mem::discriminant(&b) {
                let got = compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_lt);
                prop_assert_eq!(
                    got,
                    Tri::Unknown,
                    "cross-scalar pair ({:?}, {:?}) did not yield Unknown",
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
            let lt = compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_lt);
            let ge = compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_ge);
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
        assert!(!values_equal(
            ComparisonContext::none(),
            &int_one,
            &float_one
        ));
        assert!(!values_equal(
            ComparisonContext::none(),
            &float_one,
            &int_one
        ));

        let str_one = Value::<Postgres>::String("1".to_string());
        assert!(!values_equal(ComparisonContext::none(), &int_one, &str_one));
    }

    /// SQL orders booleans, `false` below `true`, and all three engines
    /// agree. The comparator used to answer `Unknown`, which dropped the row.
    #[test]
    fn bool_ordered_comparison_puts_false_below_true() {
        let f = Value::<Postgres>::Bool(false);
        let t = Value::<Postgres>::Bool(true);
        assert_eq!(
            compare_ordered_values(ComparisonContext::none(), &f, &t, Ordering::is_lt),
            Tri::True
        );
        assert_eq!(
            compare_ordered_values(ComparisonContext::none(), &t, &f, Ordering::is_lt),
            Tri::False
        );
        assert_eq!(
            compare_ordered_values(ComparisonContext::none(), &t, &t, Ordering::is_le),
            Tri::True
        );
    }

    /// `Bytes` scalars compare by lexicographic byte order (matches
    /// `Vec<u8>::cmp`).
    #[test]
    fn bytes_ordered_comparison_is_lexicographic() {
        let a = Value::<Postgres>::Bytes(vec![0, 1, 2]);
        let b = Value::<Postgres>::Bytes(vec![0, 1, 3]);
        assert_eq!(
            compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_lt),
            Tri::True
        );
        assert_eq!(
            compare_ordered_values(ComparisonContext::none(), &b, &a, Ordering::is_lt),
            Tri::False
        );
    }
}
