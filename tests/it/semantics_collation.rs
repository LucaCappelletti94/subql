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
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::{catalog_helpers, NotServed};

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
        crate::common::semantics::notifies::<SQLite>(
            SQLITE_DDL,
            "people",
            "SELECT * FROM people WHERE name = 'ALICE'",
            vec![Value::Int(1), Value::String("alice".to_string())]
        ),
        "NOCASE equality folds ASCII case, so the row matches"
    );
    assert!(
        !crate::common::semantics::notifies::<SQLite>(
            SQLITE_DDL,
            "people",
            "SELECT * FROM people WHERE name = '\u{c9}'",
            vec![Value::Int(1), Value::String("\u{e9}".to_string())]
        ),
        "NOCASE folds ASCII only, so the accented pair stays unequal, \
         measured as 0. Unicode case folding would answer 1 here, which is \
         what makes this the vector that distinguishes the two."
    );
    assert!(
        !crate::common::semantics::notifies::<SQLite>(
            SQLITE_DDL,
            "people",
            "SELECT * FROM people WHERE name = 'fi'",
            vec![Value::Int(1), Value::String("\u{fb01}".to_string())]
        ),
        "and the ligature is not decomposed either"
    );
    assert!(
        crate::common::semantics::notifies::<SQLite>(
            SQLITE_RTRIM_DDL,
            "people",
            "SELECT * FROM people WHERE name = 'ab'",
            vec![Value::Int(1), Value::String("ab  ".to_string())]
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
        crate::common::semantics::notifies::<Postgres>(
            PG_DDL,
            "people",
            "SELECT * FROM people WHERE name = 'alice'",
            vec![Value::Int(1), Value::String("alice".to_string())]
        ),
        "equal bytes are equal"
    );
    assert!(
        !crate::common::semantics::notifies::<Postgres>(
            PG_DDL,
            "people",
            "SELECT * FROM people WHERE name = 'ALICE'",
            vec![Value::Int(1), Value::String("alice".to_string())]
        ),
        "a deterministic collation does not fold case"
    );
    assert!(
        !crate::common::semantics::notifies::<Postgres>(
            PG_DDL,
            "people",
            "SELECT * FROM people WHERE name = 'e\u{301}'",
            vec![Value::Int(1), Value::String("\u{e9}".to_string())]
        ),
        "nor does it equate the NFC and NFD spellings, measured as f"
    );
}

/// Ordering under a locale collation is not byte ordering, and nothing here
/// reproduces it, so the comparison becomes a database read carrying a
/// typed cause.
#[test]
fn locale_ordering_is_classified_not_served() {
    let registered = crate::common::semantics::register::<Postgres>(
        PG_DDL,
        "SELECT * FROM people WHERE name < 'B'",
    );
    assert!(
        registered.served().is_none(),
        "byte ordering answers 'a' < 'B' false where the server answers true"
    );
    let column = catalog_helpers::column_id(
        &ParserDB::parse::<PostgreSqlDialect>(PG_DDL).expect("DDL parses"),
        catalog_helpers::table_id::<subql::backend::Postgres, _>(
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
    let registered = crate::common::semantics::register::<Postgres>(
        PG_C_DDL,
        "SELECT * FROM people WHERE name < 'B'",
    );
    assert!(
        registered.served().is_some(),
        "byte ordering reproduces the C collation exactly"
    );
    assert!(
        !crate::common::semantics::notifies::<Postgres>(
            PG_C_DDL,
            "people",
            "SELECT * FROM people WHERE name < 'B'",
            vec![Value::Int(1), Value::String("a".to_string())]
        ),
        "under C, lowercase a is above uppercase B, measured as f"
    );
    assert!(
        crate::common::semantics::notifies::<Postgres>(
            PG_C_DDL,
            "people",
            "SELECT * FROM people WHERE name < 'B'",
            vec![Value::Int(1), Value::String("A".to_string())]
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
    let registered = crate::common::semantics::register::<Postgres>(
        ddl,
        "SELECT * FROM people WHERE name = 'A'",
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
    let registered = crate::common::semantics::register::<MySql>(
        default_ddl,
        "SELECT * FROM people WHERE name = 'A'",
    );
    assert!(
        registered.served().is_none(),
        "MySQL's server default folds case, measured as 1 for 'a' = 'A'"
    );

    let bin_ddl = "CREATE TABLE people (id INT PRIMARY KEY, name TEXT COLLATE utf8mb4_bin)";
    assert!(
        crate::common::semantics::register::<MySql>(
            bin_ddl,
            "SELECT * FROM people WHERE name = 'A'"
        )
        .served()
        .is_some(),
        "utf8mb4_bin is byte comparison, which is reproducible"
    );
    assert!(
        !crate::common::semantics::notifies::<MySql>(
            bin_ddl,
            "people",
            "SELECT * FROM people WHERE name = 'A'",
            vec![Value::Int(1), Value::String("a".to_string())]
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
    let registered =
        crate::common::semantics::register::<MySql>(ddl, "SELECT * FROM people WHERE name = 'A'");
    assert!(
        registered.served().is_none(),
        "case sensitivity is not byte exactness: NFC equals NFD here, measured as 1"
    );
}
