//! A comparison with no column on either side.
//!
//! Every engine answers `1 = 7` and `(1 + 2) > 0`, so a filter spelling one
//! is valid SQL whose answer is the same for every row. subql types a
//! literal from the column beside it, and with no column there the literals
//! themselves name the type. Literals of two kinds, as in `1 < 2.5`, are
//! typed differently by each engine, so that comparison is a database read.
use subql::backend::{Backend, MySql, Postgres, SQLite, Value};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT, s TEXT)";

/// Whether `predicate` registers, then `None` when it is not served in
/// process, otherwise whether it delivers the row `(1, 0, 'a')`.
fn delivers<B>(predicate: &str) -> Option<bool>
where
    B: Backend<Int = i64, String = String> + subql::compiler::SqlLiteralParse + core::fmt::Debug,
    B::Dialect: sqlparser::dialect::Dialect + Default + 'static,
{
    let sql = format!("SELECT * FROM t WHERE {predicate}");
    let registered = crate::common::semantics::register::<B>(DDL, &sql);
    registered.not_served_because.is_none().then(|| {
        crate::common::semantics::notifies::<B>(
            DDL,
            "t",
            &sql,
            vec![Value::Int(1), Value::Int(0), Value::String("a".into())],
        )
    })
}

/// Integers compared with integers are served, with every engine's answer.
#[test]
fn integers_compared_with_no_column_are_served() {
    for (predicate, expected) in [
        ("1 = 7", false),
        ("1 = 1", true),
        ("(1 + 2) > 0", true),
        ("(2 * 3) <> 6", false),
    ] {
        assert_eq!(
            delivers::<Postgres>(predicate),
            Some(expected),
            "PostgreSQL, {predicate}"
        );
        assert_eq!(
            delivers::<MySql>(predicate),
            Some(expected),
            "MySQL, {predicate}"
        );
        assert_eq!(
            delivers::<SQLite>(predicate),
            Some(expected),
            "SQLite, {predicate}"
        );
    }
    // MySQL spells null-safe equality `<=>`.
    assert_eq!(delivers::<Postgres>("1 IS NOT DISTINCT FROM 1"), Some(true));
    assert_eq!(delivers::<SQLite>("1 IS NOT DISTINCT FROM 1"), Some(true));
    assert_eq!(delivers::<MySql>("1 <=> 1"), Some(true));
}

/// Integer division truncates on PostgreSQL and SQLite and yields a decimal
/// on MySQL, so `1 / 2 = 0` holds on the first two only.
#[test]
fn integer_division_with_no_column_follows_each_engine() {
    assert_eq!(delivers::<Postgres>("(1 / 2) = 0"), Some(true));
    assert_eq!(delivers::<SQLite>("(1 / 2) = 0"), Some(true));
    assert_ne!(delivers::<MySql>("(1 / 2) = 0"), Some(true));
}

/// Arithmetic over literals beside a column of another kind is left to the
/// engine. `r > (1 / 2)` divides integers to `0` on PostgreSQL and SQLite, and
/// read at the float column's kind it would be `0.5`. MySQL compares a
/// boolean as its integer and PostgreSQL refuses `f > (1 - 1)`.
#[test]
fn literal_arithmetic_beside_a_column_of_another_kind_is_routed() {
    const KINDS: &str = "CREATE TABLE t (id INT PRIMARY KEY, r DOUBLE PRECISION, f BOOLEAN)";
    let sql = |predicate: &str| format!("SELECT * FROM t WHERE {predicate}");
    for predicate in ["r > (1 / 2)", "f > (1 - 1)", "f = (0 + 1)"] {
        assert!(
            crate::common::semantics::register::<Postgres>(KINDS, &sql(predicate))
                .not_served_because
                .is_some(),
            "PostgreSQL, {predicate}"
        );
        assert!(
            crate::common::semantics::register::<MySql>(KINDS, &sql(predicate))
                .not_served_because
                .is_some(),
            "MySQL, {predicate}"
        );
        assert!(
            crate::common::semantics::register::<SQLite>(KINDS, &sql(predicate))
                .not_served_because
                .is_some(),
            "SQLite, {predicate}"
        );
    }
}

/// Literals of two kinds are left to the engine, which types them by its own
/// rules.
#[test]
fn literals_of_two_kinds_with_no_column_are_routed() {
    for predicate in ["1 < 2.5", "(1 + 2.5) > 0", "'1' = 1"] {
        assert_eq!(
            delivers::<Postgres>(predicate),
            None,
            "PostgreSQL, {predicate}"
        );
        assert_eq!(delivers::<MySql>(predicate), None, "MySQL, {predicate}");
        assert_eq!(delivers::<SQLite>(predicate), None, "SQLite, {predicate}");
    }
}

#[test]
fn boolean_conditions_preserve_their_truth_on_every_backend() {
    for (predicate, expected) in [
        ("TRUE", true),
        ("FALSE", false),
        ("(FALSE)", false),
        ("NOT FALSE", true),
        ("TRUE AND n = 0", true),
        ("FALSE AND n = 0", false),
        ("n = 0 AND TRUE", true),
        ("n = 0 AND FALSE", false),
        ("TRUE OR n = 1", true),
        ("FALSE OR n = 0", true),
        ("n = 1 OR TRUE", true),
        ("n = 1 OR FALSE", false),
    ] {
        assert_eq!(
            delivers::<Postgres>(predicate),
            Some(expected),
            "{predicate}"
        );
        assert_eq!(delivers::<MySql>(predicate), Some(expected), "{predicate}");
        assert_eq!(delivers::<SQLite>(predicate), Some(expected), "{predicate}");
    }
}
