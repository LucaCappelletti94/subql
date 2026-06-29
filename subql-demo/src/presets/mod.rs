use rand::rngs::SmallRng;

use subql::Cell;

pub mod orders;
pub mod readings;
pub mod users;

pub type Row = Vec<Cell>;

/// A demo schema: one Postgres DDL (one or more `CREATE TABLE`s) plus the
/// per-table seed data, row generators, and starter queries that drive the
/// simulation.
pub struct PresetSchema {
    pub name: &'static str,
    /// Full schema DDL. May contain several `CREATE TABLE` statements; the
    /// sqlite source translates and applies each in turn.
    pub pg_ddl: &'static str,
    pub tables: &'static [PresetTable],
}

/// One table within a [`PresetSchema`]. `columns` is the canonical column
/// order: `seed_rows` and `generator` both yield a [`Row`] whose cells line up
/// with it.
pub struct PresetTable {
    pub table_name: &'static str,
    pub columns: &'static [&'static str],
    pub column_types: &'static [&'static str],
    pub starter_queries: &'static [&'static str],
    pub seed_rows: fn() -> Vec<Row>,
    pub generator: fn(&mut SmallRng) -> Row,
}

#[must_use]
pub fn all() -> &'static [&'static PresetSchema] {
    &[&orders::PRESET, &readings::PRESET, &users::PRESET]
}

#[must_use]
pub fn by_name(name: &str) -> Option<&'static PresetSchema> {
    all().iter().copied().find(|p| p.name == name)
}
