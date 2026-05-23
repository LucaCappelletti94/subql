use rand::rngs::SmallRng;

use subql::Cell;

pub mod orders;
pub mod readings;
pub mod users;

pub type Row = Vec<Cell>;

pub struct PresetSchema {
    pub name: &'static str,
    pub table_name: &'static str,
    pub columns: &'static [&'static str],
    pub column_types: &'static [&'static str],
    pub pg_ddl: &'static str,
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
