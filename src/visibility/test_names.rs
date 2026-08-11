//! The name kinds a test needs, taken from a real translation rather than spelled.
//!
//! rls2fga mints a type name, a relation name and a column name itself, so nothing here
//! may invent one. A test that wants `docs` or `owner` asks a translation for it, which
//! also means the spelling under test is the spelling the generator actually decides.

use alloc::string::String;
use alloc::vec::Vec;

use rls2fga::classifier::patterns::ConfidenceLevel;
use rls2fga::generator::records::ValueSource;
use rls2fga::generator::relations::RelationShapes;
use rls2fga::parser::identifiers::{ColumnName, RelationName, TypeName};
use rls2fga::translator::TranslatorBuilder;
use sqlparser::dialect::PostgreSqlDialect;

use crate::ParserDB;

/// A table owned by a column, which is the smallest schema that mints all three kinds.
const OWNED: &str = "CREATE TABLE docs (id INTEGER PRIMARY KEY, owner_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_owner ON docs USING (owner_id = current_user);";

fn translated(sql: &str) -> Vec<RelationShapes> {
    let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("the fixture parses");
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
        .translate(&db)
        .relations()
}

/// The type rls2fga names `docs`.
#[must_use]
pub fn docs_type() -> TypeName {
    translated(OWNED)
        .into_iter()
        .map(|entry| entry.type_name)
        .find(|name| name == "docs")
        .expect("the guarded table gets a type")
}

/// A relation of that model, by the name the translation gave it.
///
/// Panics rather than inventing one, so a test naming a relation the generator stopped
/// emitting fails here instead of passing against a name nothing reads.
#[must_use]
pub fn relation(name: &str) -> RelationName {
    translated(OWNED)
        .into_iter()
        .map(|entry| entry.relation)
        .find(|relation| relation == name)
        .unwrap_or_else(|| panic!("the model declares no relation named '{name}'"))
}

/// Name a column the one way a caller outside rls2fga can.
#[must_use]
pub fn column(name: &str) -> ColumnName {
    match ValueSource::column(String::from(name)) {
        ValueSource::Column(column) => column,
        other => unreachable!("`ValueSource::column` names a column, got {other:?}"),
    }
}
