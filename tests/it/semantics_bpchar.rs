//! `CHAR(n)` comparison, measured against each engine.
//!
//! PostgreSQL pads a `char(n)` value out to `n` on write and then ignores
//! those trailing spaces when comparing, so a stored `ab   ` equals `'ab'`.
//! SubQL erased the declared type into a plain string and compared the
//! padded bytes, so it answered no-match on a row the database returns.
//!
//! The two engines reach the rule by different routes, which is why this is
//! not one global rule. Measured 2026-09-04 on PostgreSQL 16.11 and MySQL
//! 8.4.11:
//!
//! * PostgreSQL: type-driven. `char = char`, `char` against a literal, and
//!   `char = varchar` all ignore trailing spaces; `char = text` does not,
//!   because converting `char` to `text` strips the padding and then the
//!   comparison is exact. `varchar` and `text` keep trailing spaces
//!   significant.
//! * MySQL: collation-driven. A `CHAR` column strips trailing spaces on
//!   write, so a padded cell never reaches the stream at all, and whether
//!   comparison ignores them is a property of the collation: `PAD SPACE`
//!   ignores, `NO PAD` (the 8.0 default `utf8mb4_0900_ai_ci`) does not.
//! * SQLite: `CHAR(n)` is just `TEXT`, stored as given, compared exactly.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{
    Backend, BuiltinKind, CollationFacts, CollationName, ColumnComparison, ComparisonContext,
    MySql, Postgres, SQLite, TextOperation, Value,
};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, SubscriptionEngine, SubscriptionRequest};

/// `code` is the padded type under test, `loose` a `varchar`, `free` a
/// `text`, so one schema serves the whole rule.
const PG_DDL: &str = "CREATE TABLE codes (id INT PRIMARY KEY, code CHAR(5), \
                      loose VARCHAR(9), free TEXT)";

macro_rules! notifies {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $cells:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "codes").expect("codes is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("the predicate registers");
        let notifications = engine
            .consumers(&TestEvent::insert(table, $cells))
            .expect("dispatch succeeds");
        !notifications.inserted().is_empty()
    }};
}

/// A `codes` row: `(id, code, loose, free)`, each text cell as given.
fn row(code: &str, loose: &str, free: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(1),
        Value::String(code.to_string()),
        Value::String(loose.to_string()),
        Value::String(free.to_string()),
    ]
}

fn pg_notifies(predicate: &str, cells: Vec<Value<Postgres>>) -> bool {
    notifies!(Postgres, PostgreSqlDialect, PG_DDL, predicate, cells)
}

/// The finding: the padding a `char(n)` column carries is not part of its
/// value for comparison.
#[test]
fn char_comparison_ignores_trailing_spaces() {
    assert!(
        pg_notifies(
            "SELECT * FROM codes WHERE code = 'ab'",
            row("ab   ", "", "")
        ),
        "the stored padding is not significant, so the row is in the answer"
    );
    assert!(
        pg_notifies(
            "SELECT * FROM codes WHERE code = 'ab   '",
            row("ab", "", "")
        ),
        "and the literal's own trailing spaces are not significant either"
    );
    assert!(
        !pg_notifies(
            "SELECT * FROM codes WHERE code = 'abx'",
            row("ab   ", "", "")
        ),
        "ignoring padding is not ignoring content"
    );
}

/// Ordering follows the same padding rule, asserted on a `C`-collated
/// column because that is where PostgreSQL orders by byte. Under a locale
/// collation the order is the locale's, which nothing here reproduces, so
/// such a comparison is a database read and is asserted below.
#[test]
fn char_ordering_ignores_trailing_spaces() {
    let ddl = "CREATE TABLE codes (id INT PRIMARY KEY, code CHAR(5) COLLATE \"C\")";
    let notifies = |predicate: &str, code: &str| {
        let cells = vec![Value::<Postgres>::Int(1), Value::String(code.to_string())];
        notifies!(Postgres, PostgreSqlDialect, ddl, predicate, cells)
    };
    assert!(
        !notifies("SELECT * FROM codes WHERE code > 'ab'", "ab   "),
        "the padded value is not above the bound"
    );
    assert!(
        !notifies("SELECT * FROM codes WHERE code < 'ab'", "ab   "),
        "nor below it"
    );
    assert!(
        notifies("SELECT * FROM codes WHERE code < 'ac'", "ab   "),
        "and real content still orders"
    );
}

/// Ordering a `char` column under a locale collation is a database read:
/// the padding rule is reproducible but the order is not, and both have to
/// be for the comparison to be answered in process.
#[test]
fn char_ordering_under_a_locale_collation_is_a_read() {
    let db = ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM codes WHERE code > 'ab'",
        ))
        .expect("a read answers it");
    assert!(
        registered.served().is_none(),
        "byte order does not reproduce the database collation's order"
    );
}

/// The control: `varchar` and `text` keep trailing spaces significant, so
/// the rule must not become a global string rule.
#[test]
fn varchar_and_text_keep_trailing_spaces() {
    assert!(
        !pg_notifies(
            "SELECT * FROM codes WHERE loose = 'ab'",
            row("", "ab   ", "")
        ),
        "a varchar's trailing spaces are part of its value"
    );
    assert!(
        !pg_notifies(
            "SELECT * FROM codes WHERE free = 'ab'",
            row("", "", "ab   ")
        ),
        "and so are a text column's"
    );
}

/// The asymmetric case, measured: `char` against `varchar` ignores the
/// padding on both sides, while `char` against `text` strips only the
/// `char` side, because that is what converting `char` to `text` does.
#[test]
fn char_against_varchar_pads_but_against_text_strips() {
    assert!(
        pg_notifies(
            "SELECT * FROM codes WHERE code = loose",
            row("ab", "ab   ", "")
        ),
        "char against varchar compares as char, so padding is ignored"
    );
    assert!(
        !pg_notifies(
            "SELECT * FROM codes WHERE code = free",
            row("ab", "", "ab   ")
        ),
        "char against text strips the char side and then compares exactly"
    );
    assert!(
        pg_notifies(
            "SELECT * FROM codes WHERE code = free",
            row("ab   ", "", "ab")
        ),
        "so a padded char equals the unpadded text"
    );
}

/// MySQL's rule is the collation's, not the type's, and only its binary
/// collations are reproducible at all. Padding still differs inside that
/// family, and the two names are measured on 8.4.11 against
/// `information_schema.COLLATIONS.PAD_ATTRIBUTE`: `utf8mb4_bin` is
/// `PAD SPACE` and answers `'ab   ' = 'ab'` as 1, `utf8mb4_0900_bin` is
/// `NO PAD` and answers 0.
///
/// The rule is asked for, then applied, which is the path a registration
/// takes: resolve once, carry the answer.
#[test]
fn mysql_binary_collation_padding_is_per_collation() {
    let facts = |collation: &str| ColumnComparison {
        kind: BuiltinKind::String.into(),
        declared_type: "CHAR".to_string(),
        collation: CollationFacts::Named {
            name: CollationName {
                name: collation.to_string(),
                name_is_quoted: false,
                schema: None,
                schema_is_quoted: false,
            },
            postgres_deterministic: None,
            padding: None,
        },
    };
    let padded = Value::<MySql>::String("ab   ".to_string());
    let bare = Value::<MySql>::String("ab".to_string());

    let equal_under = |collation: &str| {
        let facts = facts(collation);
        let mut context = ComparisonContext {
            left: Some(&facts),
            right: None,
            text: None,
        };
        context.text = MySql::text_rule(&context, TextOperation::Equality);
        assert!(
            context.text.is_some(),
            "a binary collation is reproducible: {collation}"
        );
        MySql::scalars_equal(context, &padded, &bare)
    };

    assert!(
        equal_under("utf8mb4_bin"),
        "PAD SPACE ignores trailing spaces, measured as 1"
    );
    assert!(
        !equal_under("utf8mb4_0900_bin"),
        "NO PAD keeps them significant, measured as 0"
    );
}

/// With the padding unknown, which is every catalog parsed from DDL, the
/// comparison is exact. That is right for MySQL 8.0's own default
/// `utf8mb4_0900_ai_ci`, which is `NO PAD`, and wrong for a legacy
/// `PAD SPACE` collation, so a named collation whose rules subql cannot
/// establish is a candidate for classification as a database read rather
/// than an in-process answer. That decision belongs with the collation
/// work and is not taken here.
#[test]
fn mysql_unknown_padding_compares_exactly() {
    let ddl = "CREATE TABLE codes (id INT PRIMARY KEY, \
               code CHAR(5) COLLATE utf8mb4_general_ci)";
    let cells = vec![Value::<MySql>::Int(1), Value::String("ab   ".to_string())];
    assert!(
        !notifies!(
            MySql,
            MySqlDialect,
            ddl,
            "SELECT * FROM codes WHERE code = 'ab'",
            cells
        ),
        "the padding is not known from the name, so the bytes decide"
    );
}

/// SQLite has no `CHAR(n)` semantics: the declared type is advisory, the
/// value is stored as given, and comparison is exact.
#[test]
fn sqlite_char_keeps_trailing_spaces() {
    let ddl = "CREATE TABLE codes (id INTEGER PRIMARY KEY, code CHAR(5))";
    let cells = vec![Value::<SQLite>::Int(1), Value::String("ab   ".to_string())];
    assert!(
        !notifies!(
            SQLite,
            SQLiteDialect,
            ddl,
            "SELECT * FROM codes WHERE code = 'ab'",
            cells
        ),
        "SQLite stores what it was given and compares it exactly"
    );
}

/// A pattern keeps trailing spaces significant whatever the collation
/// says about equality.
///
/// Measured, and the two engines reach it by different routes while
/// agreeing on the answer. On MySQL 8.0 with a `VARCHAR` column holding
/// `ab   ` under `utf8mb4_bin`, which is `PAD SPACE`:
///
/// ```text
/// b = 'ab'         1        the collation pads, so equality ignores them
/// b LIKE 'ab'      0        the pattern does not
/// b LIKE 'ab   '   1
/// ```
///
/// On SQLite 3.51.1 with a `TEXT COLLATE RTRIM` column holding the same:
///
/// ```text
/// r = 'ab'         1        RTRIM ignores trailing spaces
/// r LIKE 'ab'      0        LIKE is a function and does not consult the
/// r LIKE 'ab   '   1        collation at all
/// ```
///
/// So a padding collation's trailing-space rule belongs to equality and
/// ordering, and a pattern must override it. PostgreSQL already answers
/// this way, because its `Pattern` arm returns `TextRule::EXACT` rather
/// than reading the padding.
#[test]
fn a_pattern_keeps_trailing_spaces_whatever_the_collation_pads() {
    let facts = |collation: &str| ColumnComparison {
        kind: BuiltinKind::String.into(),
        declared_type: "VARCHAR".to_string(),
        collation: CollationFacts::Named {
            name: CollationName {
                name: collation.to_string(),
                name_is_quoted: false,
                schema: None,
                schema_is_quoted: false,
            },
            postgres_deterministic: None,
            padding: None,
        },
    };

    let rule_for = |collation: &str, operation| {
        let facts = facts(collation);
        let context = ComparisonContext {
            left: Some(&facts),
            right: None,
            text: None,
        };
        MySql::text_rule(&context, operation).expect("a binary collation is reproducible")
    };

    assert_eq!(
        rule_for("utf8mb4_bin", TextOperation::Equality).spaces,
        subql::backend::TrailingSpaces::BothIgnored,
        "PAD SPACE pads for equality, which is what the collation is for"
    );
    assert_eq!(
        rule_for("utf8mb4_bin", TextOperation::Pattern).spaces,
        subql::backend::TrailingSpaces::BothSignificant,
        "and a pattern under the same collation keeps them, measured as `b LIKE 'ab'` = 0"
    );

    let sqlite_rule = |collation: &str, operation| {
        let facts = ColumnComparison {
            kind: BuiltinKind::String.into(),
            declared_type: "TEXT".to_string(),
            collation: CollationFacts::Named {
                name: CollationName {
                    name: collation.to_string(),
                    name_is_quoted: false,
                    schema: None,
                    schema_is_quoted: false,
                },
                postgres_deterministic: None,
                padding: None,
            },
        };
        let context = ComparisonContext {
            left: Some(&facts),
            right: None,
            text: None,
        };
        SQLite::text_rule(&context, operation).expect("RTRIM is reproducible")
    };

    assert_eq!(
        sqlite_rule("RTRIM", TextOperation::Equality).spaces,
        subql::backend::TrailingSpaces::BothIgnored,
        "RTRIM ignores trailing spaces for equality, measured as `r = 'ab'` = 1"
    );
    assert_eq!(
        sqlite_rule("RTRIM", TextOperation::Pattern).spaces,
        subql::backend::TrailingSpaces::BothSignificant,
        "and keeps them for a pattern, measured as `r LIKE 'ab'` = 0"
    );
}

/// `BETWEEN` applies the padding rule to both of its bounds.
///
/// Two ordered comparisons, so the classification the lower pair
/// resolves is the upper pair's too. It was resolved once and attached
/// once: the upper bound's `ComparisonRef` was built with the lower's
/// left operand and the upper's own facts, and no text rule at all, so
/// it compared the padded bytes while the lower bound compared as the
/// engine does.
///
/// Measured on PostgreSQL 16.15 with a `char(5) COLLATE "C"` column
/// holding `ab`:
///
/// ```text
/// code BETWEEN 'aa' AND 'ab'   t
/// code <= 'ab'                 t     the upper bound alone
/// code >= 'aa'                 t     the lower bound alone
/// code BETWEEN 'ab' AND 'ac'   t
/// ```
///
/// The wire carries the padded `ab   `, so an upper bound comparing
/// bytes answers false where the engine answers true, and the row is
/// dropped.
#[test]
fn between_applies_the_padding_rule_to_both_bounds() {
    let ddl = "CREATE TABLE codes (id INT PRIMARY KEY, code CHAR(5) COLLATE \"C\")";
    let notifies = |predicate: &str, code: &str| {
        let cells = vec![Value::<Postgres>::Int(1), Value::String(code.to_string())];
        notifies!(Postgres, PostgreSqlDialect, ddl, predicate, cells)
    };

    assert!(
        notifies(
            "SELECT * FROM codes WHERE code BETWEEN 'aa' AND 'ab'",
            "ab   "
        ),
        "the upper bound ignores the padding, so the row is in the answer"
    );
    assert!(
        notifies(
            "SELECT * FROM codes WHERE code BETWEEN 'ab' AND 'ac'",
            "ab   "
        ),
        "and so does the lower bound, which already did"
    );
    assert!(
        !notifies(
            "SELECT * FROM codes WHERE code BETWEEN 'aa' AND 'aa'",
            "ab   "
        ),
        "ignoring padding is still not ignoring content"
    );
}

/// And each bound resolves its own rule, because the two can differ.
///
/// The mutation battery is why this test exists: with both bounds given
/// the lower pair's rule, nothing changed, since the literals in the test
/// above resolve alike. Two columns of different declared types do not.
///
/// Measured on PostgreSQL 16.15 with `free` a `text` holding `ab   `,
/// `loose` a `varchar` holding the same, and `code` a `char(5)` holding
/// `ab`:
///
/// ```text
/// free >= loose                    t    text against varchar is exact
/// free <= code                     f    the char converts to text and loses its padding
/// free BETWEEN loose AND code      f
/// ```
///
/// So the lower bound compares exactly and the upper strips the other
/// side's padding, and a rule resolved once for both answers the wrong
/// one of those.
#[test]
fn each_between_bound_resolves_its_own_rule() {
    let ddl = "CREATE TABLE codes (id INT PRIMARY KEY, code CHAR(5) COLLATE \"C\", \
               loose VARCHAR(9) COLLATE \"C\", free TEXT COLLATE \"C\")";
    let cells = vec![
        Value::<Postgres>::Int(1),
        Value::String("ab   ".to_string()),
        Value::String("ab   ".to_string()),
        Value::String("ab   ".to_string()),
    ];
    assert!(
        !notifies!(
            Postgres,
            PostgreSqlDialect,
            ddl,
            "SELECT * FROM codes WHERE free BETWEEN loose AND code",
            cells.clone()
        ),
        "measured: PostgreSQL answers false, because the upper bound strips the \
         char's padding while the lower bound compares exactly"
    );
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            ddl,
            "SELECT * FROM codes WHERE free >= loose",
            cells
        ),
        "and the lower bound on its own is true, which is the pair that differs"
    );
}
