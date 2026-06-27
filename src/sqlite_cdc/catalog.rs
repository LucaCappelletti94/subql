//! PG-to-SQLite DDL translation helper.
//!
//! Tests that want to drive [`SqliteCdcSource`](super::SqliteCdcSource)
//! from a single PostgreSQL `CREATE TABLE` source pass the same DDL
//! string both to subql's `ParserDB` (the engine's catalog) and to
//! `pg2sqlite::translate` (the SQLite-side schema). This module wraps
//! the latter so callers do not have to thread `Pg2SqliteOptions`,
//! string conversions, and Diesel `batch_execute` chains themselves.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use diesel::connection::SimpleConnection;
use diesel::sqlite::SqliteConnection;
use pg2sqlite::prelude::{Pg2Sqlite, Pg2SqliteOptions};

use super::error::SqliteCdcError;

/// Translate a PG-dialect DDL stream into SQLite-compatible DDL statements.
///
/// Returns one [`String`] per translated statement; callers apply them in
/// order via their preferred Diesel driver. Use [`apply_pg_ddl`] for the
/// common case of "translate and run against this connection".
///
/// # Errors
///
/// Surfaces any parse or translation failure from `pg2sqlite` as
/// [`SqliteCdcError::Pg2Sqlite`].
///
/// # Examples
///
/// ```
/// use subql::sqlite_cdc::translate_pg_ddl;
///
/// let statements = translate_pg_ddl(
///     "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT);",
/// )?;
/// assert!(!statements.is_empty());
/// assert!(statements[0].to_uppercase().contains("CREATE TABLE"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn translate_pg_ddl(pg_ddl: &str) -> Result<Vec<String>, SqliteCdcError> {
    let statements = Pg2Sqlite::default()
        .sql(pg_ddl)
        .map_err(|e| SqliteCdcError::Pg2Sqlite(format!("{e}")))?
        .translate(&Pg2SqliteOptions::default())
        .map_err(|e| SqliteCdcError::Pg2Sqlite(format!("{e}")))?;
    Ok(statements.iter().map(ToString::to_string).collect())
}

/// Translate a PG-dialect DDL stream and apply every resulting statement
/// to the SQLite connection.
///
/// # Errors
///
/// Returns [`SqliteCdcError::Pg2Sqlite`] on translation failure and
/// [`SqliteCdcError::Diesel`] when any of the translated statements
/// fails to execute against the connection.
///
/// # Examples
///
/// ```
/// use diesel::{sql_query, Connection, RunQueryDsl, SqliteConnection};
/// use subql::sqlite_cdc::apply_pg_ddl;
///
/// let mut conn = SqliteConnection::establish(":memory:")?;
/// apply_pg_ddl(&mut conn, "CREATE TABLE orders (id INT PRIMARY KEY);")?;
///
/// // Schema is live on the connection.
/// sql_query("INSERT INTO orders (id) VALUES (1)").execute(&mut conn)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn apply_pg_ddl(connection: &mut SqliteConnection, pg_ddl: &str) -> Result<(), SqliteCdcError> {
    for stmt in translate_pg_ddl(pg_ddl)? {
        connection
            .batch_execute(&stmt)
            .map_err(SqliteCdcError::from)?;
    }
    Ok(())
}
