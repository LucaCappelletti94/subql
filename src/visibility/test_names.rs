//! The name kinds a test needs, taken from a real translation rather than spelled.
//!
//! rls2fga mints a type name, a relation name and a column name itself, so nothing here
//! may invent one. A test that wants `docs` or `owner` asks a translation for it, which
//! also means the spelling under test is the spelling the generator actually decides.

use alloc::string::String;
use alloc::vec::Vec;

use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
use rls2fga::translator::TranslatorBuilder;
use rls2fga_types::ConfidenceLevel;
use rls2fga_types::RelationShapes;
use rls2fga_types::TableId;
use rls2fga_types::{ColumnKind, ColumnRead, RecordDerivation};
use rls2fga_types::{ColumnName, RelationName, TypeName};
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
        .expect("the ownership fixture translates")
        .relations()
        .to_vec()
}

/// A stored table identity, as [`crate::catalog_helpers::contract_table_id`]
/// resolves it: no schema, the bare name.
#[must_use]
pub fn table(name: &str) -> TableId {
    TableId::from_stored(None, String::from(name))
}

/// The type rls2fga mints for a table, by its own canonicalisation.
///
/// For a fixture naming a table whose translation it does not build, so
/// there is no model to ask. Still not spelled here: the name is whatever
/// the generator's own rule makes of it.
#[must_use]
pub fn object_type(name: &str) -> TypeName {
    TypeName::canonicalized(name)
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

/// The shape connetto writes on every table: the caller's identity, or a key
/// the caller's request holds, in one policy.
const HELD_KEYS: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, owner TEXT);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes USING (
  owner = current_setting('app.user_id', true)
  OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));";

/// The relation whose records carry a condition, and the condition they name.
///
/// The held-keys arm is the one rls2fga gates on a condition over the wildcard
/// rather than on a subject, and it classifies only when the request-scoped
/// values are declared. Both names carry a hash of the policy they came from,
/// so neither can be spelled here.
#[must_use]
pub fn gated_relation() -> (RelationName, String) {
    let db = ParserDB::parse::<PostgreSqlDialect>(HELD_KEYS).expect("the fixture parses");
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .with_session_attributes([
            SessionAttribute::setting("app.user_id", SessionAttributeKind::CallerId),
            SessionAttribute::setting("app.subjects", SessionAttributeKind::SetAttribute),
        ])
        .build()
        .translate(&db)
        .expect("the held-keys fixture translates")
        .relations()
        .iter()
        .flat_map(|entry| entry.shapes.clone())
        .find_map(|shape| match shape.derivation {
            RecordDerivation::FromRow { template, .. } => {
                let template = *template;
                let context = template.context?;
                Some((template.relation, context.condition))
            }
            // A joining shape carries no template, and a variant added later
            // reads as carrying no condition rather than being guessed at.
            _ => None,
        })
        .expect("the held-keys arm is gated on a condition")
}

#[must_use]
pub fn column(name: &str) -> ColumnName {
    serde_json::from_value(serde_json::Value::String(String::from(name)))
        .expect("the column name deserializes")
}

#[must_use]
pub fn column_read(name: &str) -> ColumnRead {
    ColumnRead::new(column(name), ColumnKind::Text)
}
