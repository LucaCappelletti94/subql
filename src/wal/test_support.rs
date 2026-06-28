//! Shared test catalogs for the WAL parser modules. Each helper builds a
//! [`ParserDB`] with the schema shape that the parser fixtures expect. The
//! function names are preserved so the existing test bodies keep compiling
//! without further changes.
//!
//! Each fixture declares a placeholder table whose name sorts before
//! `orders` alphabetically. This keeps the `orders` table id stable at
//! `1`, matching the existing test bodies' hardcoded
//! `assert_eq!(ev.table_id(), 1)` expectations under `ParserDB`'s
//! deterministic, sort-order-based table id assignment.

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

const PAD_TABLE: &str = "CREATE TABLE _wal_pad (id INT);\n";

/// Shared test catalog: table="orders",
/// columns: id=0, amount=1, status=2, comment=3, PK=[id].
/// Used by parsers that emit a full 4-column orders row (e.g. Debezium).
pub(super) fn orders_catalog() -> ParserDB {
    let ddl = format!(
        "{PAD_TABLE}CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT, comment TEXT);"
    );
    ParserDB::parse::<PostgreSqlDialect>(&ddl).expect("orders DDL parses")
}

/// Same as `orders_catalog()` but without primary key metadata.
pub(super) fn orders_no_pk_catalog() -> ParserDB {
    let ddl =
        format!("{PAD_TABLE}CREATE TABLE orders (id INT, amount INT, status TEXT, comment TEXT);");
    ParserDB::parse::<PostgreSqlDialect>(&ddl).expect("orders (no-PK) DDL parses")
}

/// Shared test catalog: table="orders",
/// columns: id=0, customer=1, amount=2, status=3, PK=[id].
/// Used by parsers that model customer-centric orders fixtures
/// (e.g. wal2json and pgoutput).
pub(super) fn orders_customer_catalog() -> ParserDB {
    let ddl = format!(
        "{PAD_TABLE}CREATE TABLE orders (id INT PRIMARY KEY, customer INT, amount INT, status TEXT);"
    );
    ParserDB::parse::<PostgreSqlDialect>(&ddl).expect("orders+customer DDL parses")
}

/// Same as `orders_customer_catalog()` but without primary key metadata.
pub(super) fn orders_customer_no_pk_catalog() -> ParserDB {
    let ddl =
        format!("{PAD_TABLE}CREATE TABLE orders (id INT, customer INT, amount INT, status TEXT);");
    ParserDB::parse::<PostgreSqlDialect>(&ddl).expect("orders+customer (no-PK) DDL parses")
}
