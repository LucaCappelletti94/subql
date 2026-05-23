//! sqlite-side harness: open in-memory sqlite, translate the preset's PG DDL
//! to sqlite DDL via pg2sqlite, apply it, and load seed rows.
//!
//! The companion [`capture`] module installs diesel hooks that turn sqlite
//! `INSERT`/`UPDATE`/`DELETE` into SubQL `WalEvent`s.

use std::sync::Arc;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use pg2sqlite::prelude::{Pg2Sqlite, Pg2SqliteOptions};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::{catalog_helpers, Cell, ColumnId, TableId};

use crate::presets::PresetSchema;

pub mod capture;

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("diesel connection: {0}")]
    Connection(#[from] diesel::ConnectionError),
    #[error("diesel query: {0}")]
    Query(#[from] diesel::result::Error),
    #[error("pg2sqlite: {0}")]
    Translate(String),
    #[error("sql-traits parse: {0}")]
    ParserDb(String),
    #[error("table `{0}` not found in parsed schema")]
    UnknownTable(String),
    #[error("column `{0}` not found in parsed schema")]
    UnknownColumn(String),
}

pub struct SqliteHarness {
    pub conn: SqliteConnection,
    pub database: Arc<ParserDB>,
    pub table_id: TableId,
    pub table_name: String,
    pub columns: Vec<String>,
    pub column_ids: Vec<ColumnId>,
}

impl SqliteHarness {
    /// Build a fresh in-memory sqlite, apply the preset's translated DDL,
    /// build a SubQL `ParserDB` from the same PG source, and load seed rows.
    pub fn open(preset: &PresetSchema) -> Result<Self, HarnessError> {
        let mut conn = SqliteConnection::establish(":memory:")?;

        let sqlite_statements = Pg2Sqlite::default()
            .sql(preset.pg_ddl)
            .map_err(|e| HarnessError::Translate(format!("{e}")))?
            .translate(&Pg2SqliteOptions::default())
            .map_err(|e| HarnessError::Translate(format!("{e}")))?;

        for stmt in &sqlite_statements {
            conn.batch_execute(&stmt.to_string())?;
        }

        let database = ParserDB::parse::<PostgreSqlDialect>(preset.pg_ddl)
            .map_err(|e| HarnessError::ParserDb(format!("{e}")))?;
        let database = Arc::new(database);

        let table_id = catalog_helpers::table_id(database.as_ref(), preset.table_name)
            .ok_or_else(|| HarnessError::UnknownTable(preset.table_name.into()))?;

        let column_ids = preset
            .columns
            .iter()
            .map(|col| {
                catalog_helpers::column_id(database.as_ref(), table_id, col)
                    .ok_or_else(|| HarnessError::UnknownColumn((*col).into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut harness = Self {
            conn,
            database,
            table_id,
            table_name: preset.table_name.into(),
            columns: preset.columns.iter().map(|s| (*s).to_string()).collect(),
            column_ids,
        };

        for row in (preset.seed_rows)() {
            harness.exec_insert(&row)?;
        }

        Ok(harness)
    }

    /// Run an `INSERT` against the wrapped table. Returns the assigned `rowid`.
    pub fn exec_insert(&mut self, row: &[Cell]) -> Result<i64, HarnessError> {
        let cols = self.columns.join(", ");
        let values = row
            .iter()
            .map(cell_to_sql_literal)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {table} ({cols}) VALUES ({values})",
            table = self.table_name,
        );
        self.conn.batch_execute(&sql)?;
        let rowid = diesel::dsl::sql::<diesel::sql_types::BigInt>("SELECT last_insert_rowid()")
            .get_result::<i64>(&mut self.conn)?;
        Ok(rowid)
    }

    /// Replace the row at `rowid` with the given cell values.
    pub fn exec_update(&mut self, rowid: i64, new_row: &[Cell]) -> Result<(), HarnessError> {
        let sets = self
            .columns
            .iter()
            .zip(new_row.iter())
            .map(|(c, v)| format!("{c} = {}", cell_to_sql_literal(v)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE {table} SET {sets} WHERE rowid = {rowid}",
            table = self.table_name,
        );
        self.conn.batch_execute(&sql)?;
        Ok(())
    }

    /// Delete the row at `rowid`.
    pub fn exec_delete(&mut self, rowid: i64) -> Result<(), HarnessError> {
        let sql = format!(
            "DELETE FROM {table} WHERE rowid = {rowid}",
            table = self.table_name,
        );
        self.conn.batch_execute(&sql)?;
        Ok(())
    }

    /// Wipe the entire table.
    pub fn exec_truncate(&mut self) -> Result<(), HarnessError> {
        let sql = format!("DELETE FROM {table}", table = self.table_name);
        self.conn.batch_execute(&sql)?;
        Ok(())
    }
}

/// Render a `Cell` as a sqlite literal. Sufficient for the preset-driven
/// demo where data shape is fully controlled by the simulation.
fn cell_to_sql_literal(cell: &Cell) -> String {
    match cell {
        Cell::Missing | Cell::Null => "NULL".into(),
        Cell::Bool(b) => (if *b { "1" } else { "0" }).into(),
        Cell::Int(i) => i.to_string(),
        Cell::Float(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                "NULL".into()
            }
        }
        Cell::String(s) => format!("'{}'", s.replace('\'', "''")),
        // Cell is `#[non_exhaustive]`; treat any future variant we don't know
        // about as NULL on the sqlite side. The demo only emits the variants
        // above through its preset generators, so this should not fire in
        // practice.
        _ => "NULL".into(),
    }
}
