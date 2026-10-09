//! Two membership sources stating one relation, built by hand.
//!
//! rls2fga gives every membership source a relation of its own, so no translated schema puts two
//! producers in one region. Grouping still has to hold when a translation does, and restating one
//! source's shapes under another's relation is how a test reaches it.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use rls2fga_types::{
    BoundQuery, RecordDerivation, RecordDescription, RelationName, RelationShapes, ReplayScope,
};

/// The relation among `relations` whose shapes read rows of `table`.
///
/// # Panics
///
/// When no relation reads `table`, since a fixture naming one is wrong.
#[must_use]
pub fn relation_fed_by(relations: &[RelationShapes], table: &str) -> RelationName {
    relations
        .iter()
        .find(|entry| {
            entry
                .shapes
                .iter()
                .any(|shape| shape.tables.iter().any(|read| read.name() == table))
        })
        .map_or_else(
            || panic!("no relation is fed by rows of '{table}'"),
            |entry| entry.relation.clone(),
        )
}

/// The relation one membership table's rows feed, and the one it is restated under.
#[derive(Debug, Clone)]
pub struct RelationPool {
    from: RelationName,
    onto: RelationName,
}

impl RelationPool {
    /// Pools the relation fed by rows of `from` into the one fed by rows of `onto`.
    ///
    /// # Panics
    ///
    /// When either table feeds no relation.
    #[must_use]
    pub fn between(relations: &[RelationShapes], from: &str, onto: &str) -> Self {
        Self {
            from: relation_fed_by(relations, from),
            onto: relation_fed_by(relations, onto),
        }
    }

    /// The relation restated away.
    #[must_use]
    pub const fn from(&self) -> &RelationName {
        &self.from
    }

    /// The relation both sources state after pooling.
    #[must_use]
    pub const fn onto(&self) -> &RelationName {
        &self.onto
    }

    /// `relations` with every mention of the pooled relation restated.
    ///
    /// # Panics
    ///
    /// On a derivation this fixture has no restatement for.
    #[must_use]
    pub fn relations(&self, relations: &[RelationShapes]) -> Vec<RelationShapes> {
        relations
            .iter()
            .map(|entry| RelationShapes {
                type_name: entry.type_name.clone(),
                relation: self.relation(&entry.relation),
                from_one_row: entry.from_one_row,
                shapes: entry
                    .shapes
                    .iter()
                    .map(|shape| self.description(shape))
                    .collect(),
                decision: entry.decision.clone(),
                grants_nobody: entry.grants_nobody,
            })
            .collect()
    }

    /// One description with every mention of the pooled relation restated.
    ///
    /// # Panics
    ///
    /// On a derivation this fixture has no restatement for.
    #[must_use]
    pub fn description(&self, shape: &RecordDescription) -> RecordDescription {
        let derivation = match &shape.derivation {
            RecordDerivation::FromRow {
                table,
                template,
                guards,
            } => {
                let mut template = template.clone();
                template.relation = self.relation(&template.relation);
                RecordDerivation::FromRow {
                    table: table.clone(),
                    template,
                    guards: guards.clone(),
                }
            }
            RecordDerivation::Constant { record } => {
                let mut record = record.clone();
                record.relation = self.relation(&record.relation);
                RecordDerivation::Constant { record }
            }
            RecordDerivation::Joined { queries, reason } => RecordDerivation::Joined {
                queries: queries.iter().map(|query| self.query(query)).collect(),
                reason: reason.clone(),
            },
            RecordDerivation::WholeShape {
                query,
                condition,
                scope,
                reason,
            } => RecordDerivation::WholeShape {
                query: self.sql(query),
                condition: condition.clone(),
                scope: self.scope(scope),
                reason: reason.clone(),
            },
            other => panic!("no restatement is written for {other:?}"),
        };
        RecordDescription {
            tables: shape.tables.clone(),
            derivation,
        }
    }

    /// Enumerating SQL whose rows name the pooled relation by its new name.
    #[must_use]
    pub fn sql(&self, sql: &str) -> String {
        sql.replace(&format!("'{}'", self.from), &format!("'{}'", self.onto))
    }

    fn relation(&self, relation: &RelationName) -> RelationName {
        if *relation == self.from {
            self.onto.clone()
        } else {
            relation.clone()
        }
    }

    fn query(&self, query: &BoundQuery) -> BoundQuery {
        BoundQuery::new(
            query.table().clone(),
            query.key_columns().to_vec(),
            self.sql(query.sql()),
            query.condition().cloned(),
            self.scope(query.scope()),
        )
        .expect("restating a relation leaves the placeholders as they were")
    }

    fn scope(&self, scope: &ReplayScope) -> ReplayScope {
        match scope {
            ReplayScope::Object {
                object_type,
                relations,
            } => ReplayScope::Object {
                object_type: object_type.clone(),
                relations: relations
                    .iter()
                    .map(|relation| self.relation(relation))
                    .collect(),
            },
            ReplayScope::Subject {
                subject_type,
                relation,
                object_type,
            } => ReplayScope::Subject {
                subject_type: subject_type.clone(),
                relation: self.relation(relation),
                object_type: object_type.clone(),
            },
        }
    }
}
