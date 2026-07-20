//! sqlite-side harness: open in-memory sqlite, translate the preset's PG DDL
//! to sqlite DDL via pg2sqlite, apply it, and load seed rows.
//!
//! The companion [`capture`] module installs diesel hooks that turn sqlite
//! `INSERT`/`UPDATE`/`DELETE` into SubQL `TestEvent`s.

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use pg2sqlite::prelude::{Pg2Sqlite, Pg2SqliteOptions};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use subql::{backend::Postgres, backend::Value, catalog_helpers, ColumnId, TableId};

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
    #[error("table or column `{0}` not found in parsed schema")]
    UnknownTable(String),
}

pub struct SqliteHarness {
    pub conn: SqliteConnection,
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

        let resolved = catalog_helpers::resolve_table(&database, preset.table_name, preset.columns)
            .ok_or_else(|| HarnessError::UnknownTable(preset.table_name.into()))?;
        let table_id = resolved.table_id;
        let column_ids = resolved.column_ids;

        let mut harness = Self {
            conn,
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
    pub fn exec_insert(&mut self, row: &[Value<Postgres>]) -> Result<i64, HarnessError> {
        let cols = self.columns.join(", ");
        let values = row
            .iter()
            .map(value_to_sql_literal)
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
    pub fn exec_update(
        &mut self,
        rowid: i64,
        new_row: &[Value<Postgres>],
    ) -> Result<(), HarnessError> {
        let sets = self
            .columns
            .iter()
            .zip(new_row.iter())
            .map(|(c, v)| format!("{c} = {}", value_to_sql_literal(v)))
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

/// Render a `Value` as a sqlite literal. Sufficient for the preset-driven
/// demo where data shape is fully controlled by the simulation.
fn value_to_sql_literal(value: &Value<Postgres>) -> String {
    match value {
        Value::Missing | Value::Null => "NULL".into(),
        Value::Bool(b) => (if *b { "1" } else { "0" }).into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_finite() {
                format!("{f}")
            } else {
                "NULL".into()
            }
        }
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        // The demo only emits the variants above through its preset
        // generators, so any other value maps to NULL on the sqlite side.
        _ => "NULL".into(),
    }
}
