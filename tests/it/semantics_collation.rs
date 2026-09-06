//! Collation, measured: what the in-process comparator can reproduce, and
//! what it must hand to the database instead.
//!
//! A collation defines two independent things, an equality relation and an
//! order, and reproducibility does not factor per column. PostgreSQL's
//! default collation is the case that proves it: equality is byte equality,
//! while `'a' < 'B'` is true in the server and false in bytes. So the
//! question is asked per operation, and the answer is resolved once at
//! registration.
//!
//! Measured 2026-09-04 on PostgreSQL 16.11 (`datcollate=en_US.utf8`), MySQL
//! 8.4.11 and SQLite 3.51.1:
//!
//! ```text
//! backend collation            equality                    ordering
//! pg      deterministic        byte equality               locale, not bytes
//! pg      C / POSIX            byte equality               bytes
//! pg      nondeterministic     'a' = 'A' is true           not bytes
//! mysql   utf8mb4_bin          byte equality               bytes
//! mysql   utf8mb4_0900_as_cs   NFC = NFD is 1, not bytes   locale
//! mysql   *_ai_ci, general_ci  case and accent folded      locale
//! sqlite  BINARY               bytes                       bytes
//! sqlite  NOCASE               ASCII fold only             ASCII fold
//! sqlite  RTRIM                trailing spaces ignored     same
//! ```
//!
//! The MySQL rows are why only the `_bin` family is served there. A
//! case-sensitive UCA collation is not byte-exact either: `utf8mb4_0900_as_cs`
//! reports the NFC and NFD spellings of the same letter equal, where `_bin`
//! reports them different.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, NotServed, SubscriptionEngine, SubscriptionRequest};

/// Registers `predicate`, then reports whether a one-text-column row
/// carrying `cell` reaches the consumer.
macro_rules! notifies {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $cell:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "people").expect("people is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("registration succeeds, in process or as a read");
        let row = vec![Value::Int(1), Value::String($cell.to_string())];
        let notifications = engine
            .consumers(&TestEvent::insert(table, row))
            .expect("dispatch succeeds");
        !notifications.inserted().is_empty()
    }};
}

/// The registration for `predicate`, for the tests that assert a tier
/// rather than an answer.
macro_rules! registered {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("a read answers it, so registration succeeds")
    }};
}

const PG_DDL: &str = "CREATE TABLE people (id INT PRIMARY KEY, name TEXT)";
const PG_C_DDL: &str = "CREATE TABLE people (id INT PRIMARY KEY, name TEXT COLLATE \"C\")";
const SQLITE_DDL: &str = "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)";
const SQLITE_RTRIM_DDL: &str =
    "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT COLLATE RTRIM)";

/// SQLite's two non-binary collations are exactly reproducible, so they are
/// answered in process. `NOCASE` folds ASCII only, which is why the
/// ligature case must still not match.
#[test]
fn sqlite_nocase_and_rtrim_are_reproduced() {
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM people WHERE name = 'ALICE'",
            "alice"
        ),
        "NOCASE equality folds ASCII case, so the row matches"
    );
    assert!(
        !notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM people WHERE name = '\u{c9}'",
            "\u{e9}"
        ),
        "NOCASE folds ASCII only, so the accented pair stays unequal, \
         measured as 0. Unicode case folding would answer 1 here, which is \
         what makes this the vector that distinguishes the two."
    );
    assert!(
        !notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_DDL,
            "SELECT * FROM people WHERE name = 'fi'",
            "\u{fb01}"
        ),
        "and the ligature is not decomposed either"
    );
    assert!(
        notifies!(
            SQLite,
            SQLiteDialect,
            SQLITE_RTRIM_DDL,
            "SELECT * FROM people WHERE name = 'ab'",
            "ab  "
        ),
        "RTRIM ignores trailing spaces"
    );
}

/// The control: PostgreSQL equality under a deterministic collation is byte
/// equality, and must keep answering `false` for a case difference and for
/// two spellings of one letter.
#[test]
fn deterministic_pg_text_equality_stays_byte_exact() {
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM people WHERE name = 'alice'",
            "alice"
        ),
        "equal bytes are equal"
    );
    assert!(
        !notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM people WHERE name = 'ALICE'",
            "alice"
        ),
        "a deterministic collation does not fold case"
    );
    assert!(
        !notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_DDL,
            "SELECT * FROM people WHERE name = 'e\u{301}'",
            "\u{e9}"
        ),
        "nor does it equate the NFC and NFD spellings, measured as f"
    );
}

/// Ordering under a locale collation is not byte ordering, and nothing here
/// reproduces it, so the comparison becomes a database read carrying a
/// typed cause.
#[test]
fn locale_ordering_is_classified_not_served() {
    let registered = registered!(
        Postgres,
        PostgreSqlDialect,
        PG_DDL,
        "SELECT * FROM people WHERE name < 'B'"
    );
    assert!(
        registered.served().is_none(),
        "byte ordering answers 'a' < 'B' false where the server answers true"
    );
    let column = catalog_helpers::column_id(
        &ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses"),
        catalog_helpers::table_id(
            &ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses"),
            "people",
        )
        .expect("people is in the catalog"),
        "name",
    )
    .expect("name is in the catalog");
    assert_eq!(
        registered.not_served_because,
        Some(NotServed::CollationNotReproducible {
            column,
            collation: None,
        }),
        "the cause names the column, and the database default has no name"
    );
}

/// `C` and `POSIX` order by byte, which is reproducible, so ordering on
/// such a column stays in process and answers what the server answers.
#[test]
fn c_collation_ordering_is_served() {
    let registered = registered!(
        Postgres,
        PostgreSqlDialect,
        PG_C_DDL,
        "SELECT * FROM people WHERE name < 'B'"
    );
    assert!(
        registered.served().is_some(),
        "byte ordering reproduces the C collation exactly"
    );
    assert!(
        !notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_C_DDL,
            "SELECT * FROM people WHERE name < 'B'",
            "a"
        ),
        "under C, lowercase a is above uppercase B, measured as f"
    );
    assert!(
        notifies!(
            Postgres,
            PostgreSqlDialect,
            PG_C_DDL,
            "SELECT * FROM people WHERE name < 'B'",
            "A"
        ),
        "and uppercase A is below it"
    );
}

/// A nondeterministic collation folds case in equality itself, which byte
/// equality cannot reproduce, so even equality becomes a read.
#[test]
fn nondeterministic_pg_equality_is_classified() {
    let ddl = "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', \
               deterministic = false); \
               CREATE TABLE people (id INT PRIMARY KEY, name TEXT COLLATE ci)";
    let registered = registered!(
        Postgres,
        PostgreSqlDialect,
        ddl,
        "SELECT * FROM people WHERE name = 'A'"
    );
    assert!(
        registered.served().is_none(),
        "the server answers 'a' = 'A' true here, which bytes cannot"
    );
    assert!(
        matches!(
            registered.not_served_because,
            Some(NotServed::CollationNotReproducible { .. })
        ),
        "got {:?}",
        registered.not_served_because
    );
}

/// MySQL's own default collation is case- and accent-insensitive, and no
/// in-process comparison reproduces that, so a text comparison there is a
/// read unless the column declares a binary collation.
#[test]
fn mysql_text_comparison_is_classified_unless_binary() {
    let default_ddl = "CREATE TABLE people (id INT PRIMARY KEY, name TEXT)";
    let registered = registered!(
        MySql,
        MySqlDialect,
        default_ddl,
        "SELECT * FROM people WHERE name = 'A'"
    );
    assert!(
        registered.served().is_none(),
        "MySQL's server default folds case, measured as 1 for 'a' = 'A'"
    );

    let bin_ddl = "CREATE TABLE people (id INT PRIMARY KEY, name TEXT COLLATE utf8mb4_bin)";
    assert!(
        registered!(
            MySql,
            MySqlDialect,
            bin_ddl,
            "SELECT * FROM people WHERE name = 'A'"
        )
        .served()
        .is_some(),
        "utf8mb4_bin is byte comparison, which is reproducible"
    );
    assert!(
        !notifies!(
            MySql,
            MySqlDialect,
            bin_ddl,
            "SELECT * FROM people WHERE name = 'A'",
            "a"
        ),
        "and under utf8mb4_bin the case difference is a mismatch, measured as 0"
    );
}

/// A case-sensitive UCA collation is not byte-exact, so it is not served
/// either: `utf8mb4_0900_as_cs` reports the NFC and NFD spellings of one
/// letter equal, where a binary collation reports them different.
#[test]
fn mysql_case_sensitive_uca_collation_is_not_byte_exact() {
    let ddl = "CREATE TABLE people (id INT PRIMARY KEY, \
               name TEXT COLLATE utf8mb4_0900_as_cs)";
    let registered = registered!(
        MySql,
        MySqlDialect,
        ddl,
        "SELECT * FROM people WHERE name = 'A'"
    );
    assert!(
        registered.served().is_none(),
        "case sensitivity is not byte exactness: NFC equals NFD here, measured as 1"
    );
}
