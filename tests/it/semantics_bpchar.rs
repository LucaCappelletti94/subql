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
    MySql, Postgres, SQLite, TrailingSpacePadding, Value,
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

/// MySQL's rule is the collation's, not the type's. Measured on 8.4.11
/// against `information_schema.COLLATIONS.PAD_ATTRIBUTE`: a `PAD SPACE`
/// collation answers `'ab   ' = 'ab'` as 1, a `NO PAD` one as 0.
///
/// Asserted on the fact the catalog carries, because that is what a
/// comparison reads. Deriving the attribute from a collation name is a
/// separate question and belongs with the collation work.
#[test]
fn mysql_padding_follows_the_collation_when_the_catalog_knows_it() {
    let facts = |padding: TrailingSpacePadding| ColumnComparison {
        kind: BuiltinKind::String.into(),
        declared_type: "CHAR".to_string(),
        collation: CollationFacts::Named {
            name: CollationName {
                name: "utf8mb4_bin".to_string(),
                name_is_quoted: false,
                schema: None,
                schema_is_quoted: false,
            },
            postgres_deterministic: None,
            padding: Some(padding),
        },
    };
    let padded = Value::<MySql>::String("ab   ".to_string());
    let bare = Value::<MySql>::String("ab".to_string());

    let equal_under = |padding: TrailingSpacePadding| {
        let facts = facts(padding);
        let context = ComparisonContext {
            left: Some(&facts),
            right: None,
        };
        MySql::scalars_equal(context, &padded, &bare)
    };

    assert!(
        equal_under(TrailingSpacePadding::PadSpace),
        "PAD SPACE ignores trailing spaces, measured as 1"
    );
    assert!(
        !equal_under(TrailingSpacePadding::NoPad),
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

/// A grouped answer keys a `char` column by the value the comparison sees,
/// so the two spellings of one padded value land in one group.
///
/// Reading only the collation answered exact bytes here and split `ab` from
/// `ab   `, which is a grouped count the server never reports.
#[test]
fn a_char_column_groups_its_padded_and_bare_spellings_together() {
    let facts = |declared_type: &str| ColumnComparison {
        kind: BuiltinKind::String.into(),
        declared_type: declared_type.to_string(),
        collation: CollationFacts::DatabaseDefault,
    };
    let padded = <Postgres as Backend>::group_key_encoder(vec![facts("CHAR")])
        .expect("a deterministic text column can key");
    assert_eq!(
        padded.encode(&[Value::String("ab".to_string())]),
        padded.encode(&[Value::String("ab   ".to_string())]),
        "a char column's padding is not part of its identity"
    );

    let exact = <Postgres as Backend>::group_key_encoder(vec![facts("TEXT")])
        .expect("a deterministic text column can key");
    assert_ne!(
        exact.encode(&[Value::String("ab".to_string())]),
        exact.encode(&[Value::String("ab   ".to_string())]),
        "a text column keeps trailing spaces, so they stay part of its identity"
    );
}
