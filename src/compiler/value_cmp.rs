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
//! `Timestamp`, `TimestampTz`, `Date`, `Time`, `Decimal`, `Bool`). SQL
//! orders booleans, so `flag > false` is a row rather than an unknown, and
//! the order is the carrier's: integer order where a backend's boolean is
//! an integer. `Json` and `Jsonb` have no order here, and ordered
//! comparison on them collapses to `Tri::Unknown`.
//!
//! # NaN
//!
//! `PartialOrd::partial_cmp` returns `None` on NaN operands, which is the
//! IEEE rule and not the SQL one. It is the default here, and a backend
//! whose engine disagrees overrides it: PostgreSQL orders NaN equal to
//! itself and above every non-NaN value, so both helpers below are only the
//! structural starting point, never the answer. Ask through
//! [`Backend::scalars_equal`](crate::backend::Backend::scalars_equal) and
//! [`Backend::compare_scalars`](crate::backend::Backend::compare_scalars).

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
///
/// Text is the one scalar whose equality is not its payload's: the rule the
/// compiler resolved says whether case or trailing spaces count. Every
/// other variant compares by payload.
pub fn structural_equality<B: Backend>(
    comparison: ComparisonContext<'_, B>,
    a: &Value<B>,
    b: &Value<B>,
) -> bool {
    if let (Value::String(x), Value::String(y)) = (a, b) {
        return comparison
            .text
            .unwrap_or(crate::backend::TextRule::EXACT)
            .equal(x.as_ref(), y.as_ref());
    }
    if let Some(ordering) = B::compare_cross_kind_numeric(a, b) {
        return ordering == core::cmp::Ordering::Equal;
    }
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
/// cross-scalar pair, a variant with no order at all (`Json`, `Jsonb`), or
/// a NaN operand.
///
/// `Bool` orders by its carrier: `false` below `true` for a real boolean,
/// and integer order where the boolean is an integer, as SQLite's is. SQL
/// orders booleans, so collapsing the pair to `Tri::Unknown` answered
/// `flag > false` wrongly.
pub fn structural_ordering<B: Backend>(
    comparison: ComparisonContext<'_, B>,
    lhs: &Value<B>,
    rhs: &Value<B>,
) -> Option<core::cmp::Ordering> {
    if lhs.is_absent() || rhs.is_absent() {
        return None;
    }
    if let (Value::String(x), Value::String(y)) = (lhs, rhs) {
        return Some(
            comparison
                .text
                .unwrap_or(crate::backend::TextRule::EXACT)
                .compare(x.as_ref(), y.as_ref()),
        );
    }
    if let Some(ordering) = B::compare_cross_kind_numeric(lhs, rhs) {
        return Some(ordering);
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

/// The builtin kind a runtime numeric scalar carries, or `None` when the
/// value is not numeric.
const fn numeric_kind<B: Backend>(value: &Value<B>) -> Option<crate::backend::BuiltinKind> {
    match value {
        Value::Int(_) => Some(crate::backend::BuiltinKind::Int),
        Value::Float(_) => Some(crate::backend::BuiltinKind::Float),
        Value::Decimal(_) => Some(crate::backend::BuiltinKind::Decimal),
        _ => None,
    }
}

/// How a numeric pair of two different scalars orders, or `None` when the
/// pair is not one this backend widens.
///
/// The widening is the backend's, because the engines disagree: measured,
/// PostgreSQL and MySQL cast the other operand to `double precision`
/// against a float and compare exactly against a decimal, while SQLite
/// compares an integer against a real exactly.
///
/// Bounded on the standard numeric carriers rather than written for every
/// `Backend`, because `B::Int` and `B::Float` are associated types with no
/// conversion to `i64` and `f64`. A backend carrying its numbers otherwise
/// implements the comparison itself.
pub fn cross_kind_numeric_ordering<B>(
    left: &Value<B>,
    right: &Value<B>,
) -> Option<core::cmp::Ordering>
where
    B: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal>,
{
    use crate::backend::NumericWidening;

    let (left_kind, right_kind) = (numeric_kind(left)?, numeric_kind(right)?);
    if left_kind == right_kind {
        // Same scalar on both sides: not this function's question.
        return None;
    }
    match B::numeric_widening(left_kind, right_kind)? {
        // Widened to a double on both sides, so this is a float pair and
        // the engine's float order governs it. Asking IEEE here is what
        // dropped a NaN row whose other operand was an `int`.
        NumericWidening::AtFloatWidth => {
            B::FLOAT_ORDER.compare(at_float_width(left)?, at_float_width(right)?)
        }
        // An exact comparison still has to order an infinity, which has
        // no decimal to be exact about: `BigDecimal::from_f64` answers
        // `None` for one, the `?` propagated it, and the row was dropped
        // as unknown. Measured on SQLite 3.51.1, which is the engine
        // this arm serves: a `REAL` column holding `9e307 * 10` stores
        // `inf`, and `r > i` is 1 while `i > r` is 0.
        //
        // Widening loses nothing in that case. An infinity outranks
        // every finite number whatever the other side's precision, which
        // is the only reason this arm exists.
        NumericWidening::Exact
            if matches!(left, Value::Float(value) if !value.is_finite())
                || matches!(right, Value::Float(value) if !value.is_finite()) =>
        {
            B::FLOAT_ORDER.compare(at_float_width(left)?, at_float_width(right)?)
        }
        NumericWidening::Exact => exactly(left)?.partial_cmp(&exactly(right)?),
    }
}

/// One numeric operand as `f64`, reproducing a cast to `double precision`.
fn at_float_width<B>(value: &Value<B>) -> Option<f64>
where
    B: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal>,
{
    match value {
        Value::Int(int) => Some(crate::backend::widen_i64_to_f64(*int)),
        Value::Float(float) => Some(*float),
        // The parse is the cast the server performs. A decimal outside
        // `f64`'s range has no `double precision` to be cast to, so the
        // pair has no order.
        Value::Decimal(decimal) => {
            <bigdecimal::BigDecimal as bigdecimal::ToPrimitive>::to_f64(decimal)
        }
        _ => None,
    }
}

/// One numeric operand with every digit kept.
///
/// A non-finite float has no decimal to compare against, so such a pair has
/// no order, which is the same answer the structural rule gives.
fn exactly<B>(value: &Value<B>) -> Option<bigdecimal::BigDecimal>
where
    B: Backend<Int = i64, Float = f64, Decimal = bigdecimal::BigDecimal>,
{
    match value {
        Value::Int(int) => Some(bigdecimal::BigDecimal::from(*int)),
        Value::Float(float) => {
            <bigdecimal::BigDecimal as bigdecimal::FromPrimitive>::from_f64(*float)
        }
        Value::Decimal(decimal) => Some(decimal.clone()),
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
    use crate::backend::{
        Backend, ColumnComparisonOf, ComparisonContext, Postgres, TextOperation, TextRule,
    };
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
    fn descriptor_reports_a_char_type_from_the_declared_type() {
        for ddl in [
            "CREATE TABLE t (code CHAR(5));",
            "CREATE TABLE t (code CHARACTER(5));",
        ] {
            assert!(
                comparison(ddl, "code").declares_char_type(),
                "a declared char type is fixed width: {ddl}"
            );
        }
        for ddl in [
            "CREATE TABLE t (code VARCHAR(5));",
            "CREATE TABLE t (code TEXT);",
        ] {
            assert!(
                !comparison(ddl, "code").declares_char_type(),
                "a varying type keeps trailing spaces: {ddl}"
            );
        }
    }

    /// A float column's declared type fixes its width, and the type says so
    /// rather than a later layer re-deriving it from the spelling.
    ///
    /// PostgreSQL's `real` is float4 and `double precision` is float8, and
    /// the wire text for a float4 column is the shortest round-trip text of
    /// the float4 value, so a decoder that cannot tell them apart parses the
    /// wrong value.
    #[test]
    fn a_declared_float_type_carries_its_width() {
        use crate::backend::{BuiltinType, FloatWidth};

        for (ddl, width) in [
            ("CREATE TABLE t (v REAL);", FloatWidth::Single),
            ("CREATE TABLE t (v FLOAT4);", FloatWidth::Single),
            ("CREATE TABLE t (v DOUBLE PRECISION);", FloatWidth::Double),
            ("CREATE TABLE t (v FLOAT8);", FloatWidth::Double),
        ] {
            assert_eq!(
                comparison(ddl, "v").kind.builtin(),
                Some(BuiltinType::Float(width)),
                "the type carries the width the declaration fixes: {ddl}"
            );
        }
    }

    /// The same for text: a fixed-width character type is a different type
    /// from a varying one, which the padding rule reads off the type instead
    /// of matching the declared spelling itself.
    #[test]
    fn a_declared_char_type_is_fixed_width() {
        use crate::backend::{BuiltinType, TextWidth};

        assert_eq!(
            comparison("CREATE TABLE t (code CHAR(5));", "code")
                .kind
                .builtin(),
            Some(BuiltinType::Text(TextWidth::Fixed))
        );
        assert_eq!(
            comparison("CREATE TABLE t (code TEXT);", "code")
                .kind
                .builtin(),
            Some(BuiltinType::Text(TextWidth::Varying))
        );
    }

    /// A compiled program persists the kinds of the columns it loads, so the
    /// refinements have to survive the stored form: a `real` column that
    /// reloads as float8 would answer differently after a restart.
    #[test]
    fn a_persisted_kind_round_trips_its_refinements() {
        use crate::backend::{BuiltinType, FloatWidth, ScalarKindOf, TextWidth};

        for kind in [
            BuiltinType::Float(FloatWidth::Single),
            BuiltinType::Float(FloatWidth::Double),
            BuiltinType::Text(TextWidth::Fixed),
            BuiltinType::Text(TextWidth::Varying),
            BuiltinType::Int(crate::backend::IntWidth::SixtyFour),
        ] {
            let stored: ScalarKindOf<Postgres> = kind.into();
            let bytes = postcard::to_allocvec(&stored).expect("the kind serializes");
            assert_eq!(
                postcard::from_bytes::<ScalarKindOf<Postgres>>(&bytes)
                    .expect("the kind deserializes"),
                stored,
                "{kind:?} must reload as itself"
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
        assert_eq!(
            crate::backend::single_column_rule::<Postgres>(&ci),
            None,
            "a nondeterministic collation folds case in equality itself"
        );
        let plain = comparison("CREATE TABLE t (name TEXT);", "name");
        assert_eq!(
            crate::backend::single_column_rule::<Postgres>(&plain),
            Some(TextRule::EXACT),
            "equality under a deterministic collation is byte equality"
        );
    }

    /// Reproducibility does not factor per column: the same column has
    /// byte equality and unreproducible ordering under PostgreSQL's
    /// default collation, which is why the backend is asked per operation.
    #[test]
    fn one_column_answers_differently_per_operation() {
        let plain = comparison("CREATE TABLE t (name TEXT);", "name");
        let context = ComparisonContext {
            left: Some(&plain),
            right: None,
            text: None,
        };
        assert_eq!(
            <Postgres as Backend>::text_rule(&context, TextOperation::Equality).rule(),
            Some(TextRule::EXACT)
        );
        assert_eq!(
            <Postgres as Backend>::text_rule(&context, TextOperation::Ordering).rule(),
            None,
            "the server answers 'a' < 'B' true, which byte order does not"
        );

        let c = comparison("CREATE TABLE t (name TEXT COLLATE \"C\");", "name");
        let context = ComparisonContext {
            left: Some(&c),
            right: None,
            text: None,
        };
        assert_eq!(
            <Postgres as Backend>::text_rule(&context, TextOperation::Ordering).rule(),
            Some(TextRule::EXACT),
            "C orders by byte, measured"
        );
    }

    /// Compile `sql` against `ddl` and return the program.
    fn compiled(ddl: &str, sql: &str) -> crate::compiler::BytecodeProgram<Postgres> {
        let db = ParserDB::parse::<PostgreSqlDialect>(ddl).expect("the DDL parses");
        crate::compiler::parse_compile_normalize_and_prefilter::<Postgres, _>(
            sql,
            &PostgreSqlDialect {},
            &db,
        )
        .expect("the query compiles")
        .program
    }

    /// The declared type at one interned slot.
    fn declared_at(
        program: &crate::compiler::BytecodeProgram<Postgres>,
        slot: Option<u16>,
    ) -> Option<alloc::string::String> {
        Some(
            program
                .comparison_at(slot)
                .expect("the slot exists")?
                .declared_type
                .clone(),
        )
    }

    #[test]
    fn a_column_to_column_comparison_interns_both_sides_in_order() {
        let program = compiled(
            "CREATE TABLE t (id INT PRIMARY KEY, small INT, wide BIGINT);",
            "SELECT * FROM t WHERE small = wide",
        );
        let Some(crate::compiler::Instruction::Equal(reference)) = program
            .instructions
            .iter()
            .find(|instruction| matches!(instruction, crate::compiler::Instruction::Equal(_)))
        else {
            panic!("the comparison compiled to an Equal instruction");
        };
        assert_eq!(
            declared_at(&program, reference.left).as_deref(),
            Some("INT"),
            "the left operand's facts, not the right's"
        );
        assert_eq!(
            declared_at(&program, reference.right).as_deref(),
            Some("BIGINT")
        );
    }

    #[test]
    fn between_interns_the_value_against_each_bound() {
        let program = compiled(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, lo BIGINT, hi SMALLINT);",
            "SELECT * FROM t WHERE v BETWEEN lo AND hi",
        );
        let Some(crate::compiler::Instruction::Between { lower, upper }) =
            program.instructions.iter().find(|instruction| {
                matches!(instruction, crate::compiler::Instruction::Between { .. })
            })
        else {
            panic!("the range compiled to a Between instruction");
        };
        assert_eq!(declared_at(&program, lower.left).as_deref(), Some("INT"));
        assert_eq!(
            declared_at(&program, lower.right).as_deref(),
            Some("BIGINT")
        );
        assert_eq!(
            declared_at(&program, upper.left).as_deref(),
            Some("INT"),
            "both contexts compare the same value"
        );
        assert_eq!(
            declared_at(&program, upper.right).as_deref(),
            Some("SMALLINT")
        );
    }

    #[test]
    fn like_interns_the_string_and_the_pattern() {
        let program = compiled(
            "CREATE TABLE t (id INT PRIMARY KEY, body TEXT, needle VARCHAR(8));",
            "SELECT * FROM t WHERE body LIKE needle",
        );
        let Some(crate::compiler::Instruction::Like { comparison, .. }) = program
            .instructions
            .iter()
            .find(|instruction| matches!(instruction, crate::compiler::Instruction::Like { .. }))
        else {
            panic!("the pattern match compiled to a Like instruction");
        };
        assert_eq!(
            declared_at(&program, comparison.left).as_deref(),
            Some("TEXT")
        );
        assert_eq!(
            declared_at(&program, comparison.right).as_deref(),
            Some("VARCHAR"),
            "a pattern can be a column, so the right side carries facts too"
        );
    }

    #[test]
    fn an_in_list_interns_only_the_tested_operand() {
        let program = compiled(
            "CREATE TABLE t (id INT PRIMARY KEY, status TEXT);",
            "SELECT * FROM t WHERE status IN ('a', 'b')",
        );
        let Some(crate::compiler::Instruction::In { comparison, .. }) = program
            .instructions
            .iter()
            .find(|instruction| matches!(instruction, crate::compiler::Instruction::In { .. }))
        else {
            panic!("the set test compiled to an In instruction");
        };
        assert_eq!(
            declared_at(&program, comparison.left).as_deref(),
            Some("TEXT")
        );
        assert_eq!(
            comparison.right, None,
            "the set holds literals, which carry no column"
        );
    }

    #[test]
    fn arithmetic_operands_intern_nothing() {
        let program = compiled(
            "CREATE TABLE t (id INT PRIMARY KEY, amount INT, quantity INT);",
            "SELECT * FROM t WHERE amount + quantity > 100",
        );
        assert!(
            program.column_comparisons.is_empty(),
            "an arithmetic operand has no declared type to carry, so nothing is \
             interned: {:?}",
            program.column_comparisons
        );
    }

    #[test]
    fn a_comparison_naming_a_slot_the_program_lacks_is_refused() {
        use crate::backend::Value;
        use crate::compiler::{bytecode::ComparisonRef, BytecodeProgram, Instruction, Vm};
        use crate::testing::TestEvent;

        let db = ParserDB::parse::<PostgreSqlDialect>("CREATE TABLE t (id INT PRIMARY KEY);")
            .expect("the DDL parses");
        // A program carrying no facts, whose comparison names slot 0 anyway:
        // what a truncated or corrupt stored program looks like.
        let program: BytecodeProgram<Postgres> = BytecodeProgram::new(alloc::vec![
            Instruction::LoadColumn(0),
            Instruction::PushLiteral(Value::Int(1)),
            Instruction::Equal(ComparisonRef::new(Some(0), None)),
        ]);
        let event =
            TestEvent::<Postgres>::insert(0, alloc::vec![Value::Int(1)]).with_pk_columns([0u16]);
        let mut vm: Vm<Postgres> = Vm::new();
        assert_eq!(
            vm.eval(&program, &event, crate::backend::RowKind::New, &db),
            Err(crate::compiler::VmError::MalformedProgram),
            "a dangling slot must refuse, not compare as though the column had no facts"
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
        let Some(crate::compiler::Instruction::Equal(reference)) = compiled
            .program
            .instructions
            .iter()
            .find(|instruction| matches!(instruction, crate::compiler::Instruction::Equal(_)))
        else {
            panic!("the comparison compiled to an Equal instruction");
        };
        let carried = compiled
            .program
            .comparison_at(reference.left)
            .expect("the interned slot exists")
            .expect("the compiler interned the compared column's descriptor");
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
    //!    any concrete present scalar `Value`, NaN included: this backend
    //!    is PostgreSQL, which answers `NaN = NaN` true.
    //! 3. **`Missing` and `Null` are not self-equal.**
    //! 4. **Cross-scalar equality is false outside the numeric family.**
    //!    A numeric pair is widened as the engine widens it; nothing else
    //!    coerces.
    //! 5. **Ordered comparison collapses on `Missing` / `Null`.**
    //! 6. **NaN follows PostgreSQL's float order**, above every non-NaN
    //!    value and equal to another NaN, rather than IEEE's no-order.
    //! 7. **Booleans are ordered**, `false` below `true`.
    //! 8. **Ordered comparison collapses on non-numeric cross-scalar
    //!    pairs** and on `Json` / `Jsonb`, which carry no order here.
    //! 9. **Strict + non-strict are consistent.** `<` and `>=` partition
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

    /// Whether both values are numeric, which is the one cross-scalar
    /// family this backend widens rather than refuses.
    fn numeric_pair(a: &Value<Postgres>, b: &Value<Postgres>) -> bool {
        let numeric =
            |v: &Value<Postgres>| matches!(v, Value::Int(_) | Value::Float(_) | Value::Decimal(_));
        numeric(a) && numeric(b)
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

        /// Reflexivity for every present value. NaN is included: PostgreSQL
        /// answers `NaN = NaN` true, so `WHERE value = value` returns the
        /// row.
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

        /// `Missing` and `Null` are never self-equal: those are the shapes
        /// the VM lifts to `Tri::Unknown`. A NaN is not one of them under
        /// this backend.
        #[test]
        fn null_and_missing_are_not_self_equal(v in arb_value()) {
            if !is_present(&v) {
                prop_assert!(
                    !values_equal(ComparisonContext::none(), &v, &v),
                    "absent value {:?} unexpectedly self-equal",
                    v,
                );
            }
        }

        /// Cross-scalar pairs never compare equal (no coercion).
        #[test]
        fn cross_scalar_pairs_are_never_equal(a in arb_value(), b in arb_value()) {
            if is_present(&a) && is_present(&b)
                && core::mem::discriminant(&a) != core::mem::discriminant(&b)
                && !numeric_pair(&a, &b)
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

        /// A NaN is ordered under this backend, not unknown: PostgreSQL puts
        /// it above every non-NaN number and equal to another NaN, so
        /// `a < b` holds exactly when `a` is the non-NaN side.
        ///
        /// That holds across kinds too, and this model used to say
        /// otherwise. Measured on 16.15, `'NaN'::float8 > 1::int` and
        /// `'NaN'::float8 > 1::numeric` are both true, because the engine
        /// widens the other operand to a double and then applies this
        /// order. A pair the backend does not widen at all, a float
        /// against a string say, is still a cross-scalar pair and stays
        /// `Unknown`.
        #[test]
        fn ordered_cmp_follows_postgres_nan_order(a in arb_value(), b in arb_value()) {
            if (is_nan(&a) || is_nan(&b)) && is_present(&a) && is_present(&b) {
                let got = compare_ordered_values(ComparisonContext::none(), &a, &b, Ordering::is_lt);
                let widened = |value: &Value<Postgres>| {
                    matches!(value, Value::Float(_) | Value::Int(_) | Value::Decimal(_))
                };
                let expected = if widened(&a) && widened(&b) {
                    if is_nan(&a) { Tri::False } else { Tri::True }
                } else {
                    Tri::Unknown
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

        /// Cross-scalar pairs collapse to `Tri::Unknown`: there is no
        /// coercion between scalars, so no order across them either.
        #[test]
        fn ordered_cmp_unknown_on_incomparable(a in arb_value(), b in arb_value()) {
            if !is_present(&a) || !is_present(&b) || is_nan(&a) || is_nan(&b) {
                return Ok(());
            }
            if core::mem::discriminant(&a) != core::mem::discriminant(&b)
                && !numeric_pair(&a, &b)
            {
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

        /// Booleans are ordered: `false` below `true` for this backend's
        /// `bool` carrier, which is what SQL answers.
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
    fn cross_scalar_equality_is_false_outside_the_numeric_family() {
        let int_one = Value::<Postgres>::Int(1);

        // A numeric pair is compared under the backend's widening, which
        // is what the engine does: `1 = 1.0` is a row.
        let float_one = Value::<Postgres>::Float(1.0);
        assert!(values_equal(
            ComparisonContext::none(),
            &int_one,
            &float_one
        ));
        assert!(values_equal(
            ComparisonContext::none(),
            &float_one,
            &int_one
        ));

        // Outside it, nothing coerces: PostgreSQL has no operator for this
        // pair, so there is nothing to reproduce.
        let str_one = Value::<Postgres>::String("1".to_string());
        assert!(!values_equal(ComparisonContext::none(), &int_one, &str_one));
    }

    /// Ordered comparison on `Bool` follows SQL's boolean order rather
    /// than the absent `PartialOrd` bound on `Backend::Bool`.
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
