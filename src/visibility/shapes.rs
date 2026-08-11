//! Everything one translation says about rows, resolved against one catalog
//! and built once.
//!
//! Three things read it. [`RowPolicy`](crate::visibility::policy::RowPolicy)
//! answers from the changed row where the schema decides the relation, a
//! terminal policy asks the authorization service about everything else, and
//! [`Shapes::diff`] reports what each changed row moved in that service's
//! store. All three need the same descriptions resolved against the same
//! catalog.
//!
//! # Why one index rather than three
//!
//! Each reader taking the catalog and the descriptions for itself lets a caller
//! hand one reader a catalog that disagrees with another's, and every record
//! then names rows that do not exist. Building once removes the argument rather
//! than documenting the hazard.
//!
//! # What the naming is for
//!
//! Asking the service anything about a row means naming that row as the model
//! names it, which is neither the table's name nor a function of it: the model
//! assigns a type, appending a suffix where two tables canonicalise alike, and
//! builds the object from the row's key. Only rls2fga knows that, and it
//! reports it through
//! [`Translation::row_naming`](rls2fga::translator::Translation::row_naming).
//! Reading it off a fact-shape instead is ambiguous, because a table whose
//! whole key is a foreign key is keyed identically by its own shape and by a
//! shape describing its parent.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use core::ops::Not;
use hashbrown::HashMap;
use rls2fga::generator::action_relations::{ActionAnswer, ActionRelations, ActionStatement};
use rls2fga::generator::notes::TranslationNote;
use rls2fga::generator::records::{BoundQuery, RecordDerivation, RecordDescription};
use rls2fga::generator::relations::{RelationShapes, RowDecision};
use rls2fga::generator::row_naming::RowNaming;

use rls2fga::parser::identifiers::RelationName;
use sql_traits::prelude::DatabaseLike;

use crate::visibility::records::is_evaluable;
use crate::visibility::store::{name_gap, Uncovered, UncoveredReason};
use crate::{catalog_helpers, ColumnId, TableId};

// ---------------------------------------------------------------------------
// RequiredParameter
// ---------------------------------------------------------------------------

/// A condition parameter every question's context has to carry.
///
/// rls2fga reports these because a model carrying a request-gated grant is
/// unusable without them: a question that omits one is refused by the service
/// outright rather than answered no.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredParameter {
    /// The name a context uses for it.
    pub parameter: String,
    /// The session setting it mirrors, so a watcher can be asked for it.
    /// [`None`] means no watcher can answer it, which today is the clock.
    pub setting_key: Option<String>,
    /// Whether it holds a set rather than one value.
    ///
    /// Read off the reported separator and nothing else. The separator itself
    /// is how the values reach Postgres joined into one string, and a watcher
    /// holds them apart, so splitting one here would duplicate a rule the
    /// deployment already owns.
    pub list: bool,
}

impl RequiredParameter {
    /// Whether a watcher can be asked for this one.
    #[must_use]
    pub const fn watcher_supplied(&self) -> bool {
        self.setting_key.is_some()
    }
}

// ---------------------------------------------------------------------------
// TableShapes
// ---------------------------------------------------------------------------

/// Shapes reaching one table's rows, resolved to subql ids once.
#[derive(Debug, Default)]
pub(crate) struct TableShapes {
    /// Shapes whose records a row of this table settles on its own.
    pub(crate) settled: Vec<RecordDescription>,
    /// Queries to replay when a row of this table changes, with the column
    /// each one binds.
    pub(crate) requeries: Vec<(ColumnId, BoundQuery)>,
}

// ---------------------------------------------------------------------------
// Shapes
// ---------------------------------------------------------------------------

/// One translation's descriptions, indexed for every reader of them.
///
/// Build it from [`Translation`](rls2fga::translator::Translation) and the
/// catalog those relations were planned against, then share it.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use rls2fga::classifier::patterns::ConfidenceLevel;
/// use rls2fga::translator::TranslatorBuilder;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use rls2fga::generator::action_relations::ActionStatement;
/// use subql::visibility::shapes::Shapes;
/// use subql::{catalog_helpers, ParserDB};
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);
///      ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
///      CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);",
/// )?;
/// let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
///
/// let translator = TranslatorBuilder::new()
///     .with_min_confidence(ConfidenceLevel::B)
///     .build();
/// let translation = translator.translate(&db);
/// let relations = translation.relations();
/// let naming = translation.row_naming();
/// let answers = translation.action_relations();
///
/// let shapes = Arc::new(
///     Shapes::new(db, &relations)
///         .with_row_naming(&naming)
///         .with_action_relations(&answers),
/// );
///
/// assert!(shapes.answers_locally(docs, ActionStatement::Select));
/// // The model calls a row of `docs` by its own type and key.
/// let named = shapes.naming(docs).expect("docs names its rows");
/// assert_eq!(named.type_name, "docs");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct Shapes<DB> {
    db: DB,
    /// Only the recipes that can be evaluated from a row image, keyed by the
    /// table whose rows they judge and the relation they decide. Absent means
    /// delegate.
    ///
    /// Keyed by relation rather than by a fixed set of actions, because which
    /// relations answer a statement is the model's to say and varies with the
    /// clauses a policy wrote.
    recipes: HashMap<(TableId, RelationName), RowDecision>,
    /// What the model says answers each statement, by the type it is asked on.
    answers: HashMap<String, HashMap<ActionStatement, ActionAnswer>>,
    /// Every shape indexed by the table whose changes move it, which is a wider
    /// index than the answering one: a shape may describe one type from another
    /// table's rows.
    by_table: HashMap<TableId, TableShapes>,
    /// Shapes whose records nothing here can keep current.
    uncovered: Vec<Uncovered>,
    /// How the model names a row of each table it names rows of.
    naming: HashMap<TableId, RowNaming>,
    /// Parameters every question's context has to carry.
    parameters: Vec<RequiredParameter>,
}

impl<DB: DatabaseLike> Shapes<DB> {
    /// Index `relations` against the catalog `db`.
    ///
    /// A recipe is kept only when it is one of the actions consulted, one row
    /// decides it, every leaf shape is evaluable from a row image, and every
    /// leaf reads the one table the recipe is then keyed by. Everything else is
    /// simply absent, and an absent recipe delegates.
    ///
    /// `can_update` is never one of them. It is
    /// `can_update_using and can_update_check`, an intersection across the row
    /// as it is and the row as it will be, so a replacement consults the two
    /// halves against the version each judges. Keying the conjunction and
    /// evaluating it against one image asks whether the **new** owner is the
    /// caller, which grants a caller who holds nothing and writes themselves in
    /// as owner and editor.
    ///
    /// A shape reading a table the catalog does not know is skipped rather than
    /// reported: that table produces no events here, so skipping it introduces
    /// no staleness and there would be no remedy to name.
    #[must_use]
    pub fn new(db: DB, relations: &[RelationShapes]) -> Self {
        let mut recipes = HashMap::new();
        let mut by_table: HashMap<TableId, TableShapes> = HashMap::new();
        let mut uncovered = Vec::new();

        for entry in relations {
            index_recipe(&db, entry, &mut recipes);
            for shape in &entry.shapes {
                index_shape(&db, entry, shape, &mut by_table, &mut uncovered);
            }
        }

        Self {
            db,
            recipes,
            by_table,
            uncovered,
            answers: HashMap::new(),
            naming: HashMap::new(),
            parameters: Vec::new(),
        }
    }

    /// Take how the model names each table's rows, from
    /// [`Translation::row_naming`](rls2fga::translator::Translation::row_naming).
    ///
    /// A reader that has to name a row refuses without this rather than
    /// guessing a name, because a guessed name reaches another row.
    #[must_use]
    pub fn with_row_naming(mut self, naming: &[RowNaming]) -> Self {
        self.naming = naming
            .iter()
            .filter_map(|entry| {
                catalog_helpers::table_id(&self.db, &entry.table).map(|id| (id, entry.clone()))
            })
            .collect();
        self
    }

    /// Take the parameters every question's context has to carry, read out of
    /// [`Translation::notes`](rls2fga::translator::Translation::notes).
    #[must_use]
    pub fn with_required_parameters(mut self, notes: &[TranslationNote]) -> Self {
        self.parameters = notes
            .iter()
            .filter_map(|note| match note {
                TranslationNote::CallerSuppliesConditionParameter {
                    parameter,
                    setting_key,
                    separator,
                } => Some(RequiredParameter {
                    parameter: parameter.clone(),
                    setting_key: setting_key.clone(),
                    list: separator.is_some(),
                }),
                _ => None,
            })
            .collect();
        self
    }

    /// The catalog every reader resolves tables against.
    pub const fn catalog(&self) -> &DB {
        &self.db
    }

    /// Take what the model says answers each statement, from
    /// [`Translation::action_relations`](rls2fga::translator::Translation::action_relations).
    ///
    /// Which relations answer a replacement depends on whether a policy spelled
    /// its two clauses separately, and only the model knows which shape a schema
    /// produced. Without this a reader has to guess, and a reader that guesses
    /// asks for relations the model does not define.
    #[must_use]
    pub fn with_action_relations(mut self, answers: &[ActionRelations]) -> Self {
        self.answers.clear();
        for entry in answers {
            // Keyed by the type name as text, because the naming report spells a
            // type name as a `String` while this one spells it as a `TypeName`,
            // and text is what the two share. Nested so a lookup borrows the
            // name rather than cloning it.
            self.answers
                .entry_ref(entry.type_name.as_str())
                .or_default()
                .insert(entry.statement, entry.answer.clone());
        }
        self
    }

    /// What answers `statement` on rows of `table`, or [`None`] when nothing
    /// said.
    ///
    /// [`None`] means the index was built without the report, or the model names
    /// no type for the table, and either way a reader has to delegate rather
    /// than pick a relation itself.
    #[must_use]
    pub fn answer(&self, table: TableId, statement: ActionStatement) -> Option<&ActionAnswer> {
        self.answers
            .get(self.naming.get(&table)?.type_name.as_str())?
            .get(&statement)
    }

    /// Whether a question about `statement` on a row of `table` can be answered
    /// without a round trip.
    ///
    /// Reports the recipes, not the row: a row whose cell fails to decode is
    /// still delegated, and so is a watcher that cannot supply a value a recipe
    /// compares against.
    #[must_use]
    pub fn answers_locally(&self, table: TableId, statement: ActionStatement) -> bool {
        match self.answer(table, statement) {
            // The database restricts nothing here, so there is nothing to ask.
            Some(ActionAnswer::Unrestricted) => true,
            Some(ActionAnswer::Judged(judges)) => {
                // Nothing to require is not a grant, so an empty list is not an
                // answer.
                judges.is_empty().not()
                    && judges
                        .iter()
                        .all(|judge| self.recipe(table, &judge.relation).is_some())
            }
            // One relation fusing both versions cannot be answered from either,
            // and asking it grants a change the check clause refuses.
            _ => false,
        }
    }

    /// The recipe deciding `relation` on a row of `table`, or [`None`] to
    /// delegate.
    #[must_use]
    pub fn recipe(&self, table: TableId, relation: &RelationName) -> Option<&RowDecision> {
        self.recipes.get(&(table, relation.clone()))
    }

    /// How the model names a row of `table`, or [`None`] when it names none.
    #[must_use]
    pub fn naming(&self, table: TableId) -> Option<&RowNaming> {
        self.naming.get(&table)
    }

    /// The parameters every question's context has to carry.
    #[must_use]
    pub fn required_parameters(&self) -> &[RequiredParameter] {
        &self.parameters
    }

    /// Whether any recipe grants through a comparison the caller's own request
    /// value completes.
    ///
    /// A reader asking the service has to carry those values, so one that finds
    /// this true and [`required_parameters`](Self::required_parameters) empty
    /// was built without the report and refuses rather than sending contexts
    /// that are silently incomplete.
    #[must_use]
    pub fn has_request_gated_recipe(&self) -> bool {
        self.recipes.values().any(is_request_gated)
    }

    /// Shapes whose records this cannot keep current, settled once at
    /// construction because it depends on the schema rather than on any row.
    #[must_use]
    pub fn uncovered(&self) -> &[Uncovered] {
        &self.uncovered
    }

    /// Shapes whose records a row of `table` settles on its own.
    ///
    /// A question about a row the store has never seen carries the facts that
    /// row implies, and these are the shapes that state them.
    #[must_use]
    pub fn table_records(&self, table: TableId) -> Option<&[RecordDescription]> {
        self.by_table
            .get(&table)
            .map(|shapes| shapes.settled.as_slice())
    }

    /// Shapes reaching rows of `table`, for the store difference.
    pub(crate) fn table_shapes(&self, table: TableId) -> Option<&TableShapes> {
        self.by_table.get(&table)
    }
}

/// Whether `decision` grants anywhere through a request-gated comparison.
fn is_request_gated(decision: &RowDecision) -> bool {
    match decision {
        RowDecision::RequestGated { .. } => true,
        RowDecision::Any(children) | RowDecision::All(children) => {
            children.iter().any(is_request_gated)
        }
        // A leaf grants names outright, and a composition this does not
        // recognise cannot be evaluated at all, so it is never indexed and
        // cannot reach here.
        _ => false,
    }
}

/// Key `entry`'s recipe by the table whose rows it judges and the relation it
/// decides, when every leaf can be read from that table's row image.
///
/// Every relation is indexed, not a chosen few: which of them answers a
/// statement is what the model reports, and a reader that filtered here would
/// be deciding that question in the wrong place.
fn index_recipe<DB: DatabaseLike>(
    db: &DB,
    entry: &RelationShapes,
    recipes: &mut HashMap<(TableId, RelationName), RowDecision>,
) {
    let Some(decision) = entry.decision.as_ref() else {
        return;
    };
    let Some(table) = usable_table(decision).and_then(|name| catalog_helpers::table_id(db, name))
    else {
        return;
    };
    recipes.insert((table, entry.relation.clone()), decision.clone());
}

/// Index one shape by the table whose changes move it, naming it uncovered
/// when nothing here can keep its records current.
fn index_shape<DB: DatabaseLike>(
    db: &DB,
    entry: &RelationShapes,
    shape: &RecordDescription,
    by_table: &mut HashMap<TableId, TableShapes>,
    uncovered: &mut Vec<Uncovered>,
) {
    match &shape.derivation {
        RecordDerivation::FromRow { table, .. } => {
            if !is_evaluable(shape) {
                uncovered.push(name_gap(
                    entry,
                    shape,
                    table,
                    UncoveredReason::UnreadableColumn,
                ));
                return;
            }
            if let Some(id) = catalog_helpers::table_id(db, table) {
                by_table.entry(id).or_default().settled.push(shape.clone());
            }
        }
        RecordDerivation::Joined { queries, .. } => {
            for query in queries {
                let Some(id) = catalog_helpers::table_id(db, &query.table) else {
                    continue;
                };
                let Some(key) = catalog_helpers::column_id(db, id, query.key_column.as_str())
                else {
                    continue;
                };
                by_table
                    .entry(id)
                    .or_default()
                    .requeries
                    .push((key, query.clone()));
            }
            // A table the shape reads with no query bound to it has nothing to
            // replay when a change arrives there.
            for table in &shape.tables {
                if !queries.iter().any(|query| query.table == *table) {
                    uncovered.push(name_gap(entry, shape, table, UncoveredReason::NoBoundQuery));
                }
            }
        }
        // `RecordDerivation` is `#[non_exhaustive]`: a shape this does not
        // understand is named rather than assumed covered.
        _ => uncovered.extend(
            shape
                .tables
                .iter()
                .map(|table| name_gap(entry, shape, table, UncoveredReason::UnreadableColumn)),
        ),
    }
}

/// The one table every leaf of `decision` reads, or [`None`] when the recipe
/// cannot be answered from a row image at all.
///
/// Refuses a composition over no children, because folding an intersection of
/// nothing yields every subject, which is the one error direction that matters.
/// It names no table either, so both fall out of the same walk rather than
/// needing a guard that says so twice.
///
/// A recipe shape this does not recognise falls to the wildcard and delegates.
/// That is the whole protection against a composition a later rls2fga adds.
fn usable_table(decision: &RowDecision) -> Option<&str> {
    let mut table: Option<&str> = None;
    match decision {
        RowDecision::Leaf { shapes, .. } | RowDecision::RequestGated { shapes, .. } => {
            for shape in shapes {
                if !is_evaluable(shape) {
                    return None;
                }
                let RecordDerivation::FromRow { table: name, .. } = &shape.derivation else {
                    return None;
                };
                if *table.get_or_insert(name) != name.as_str() {
                    return None;
                }
            }
        }
        RowDecision::Any(children) | RowDecision::All(children) => {
            for child in children {
                let name = usable_table(child)?;
                if *table.get_or_insert(name) != name {
                    return None;
                }
            }
        }
        // `RowDecision` is `#[non_exhaustive]`: a composition this does not
        // understand is delegated rather than guessed at.
        _ => return None,
    }
    table
}

/// A handle several readers share.
pub type SharedShapes<DB> = Arc<Shapes<DB>>;
