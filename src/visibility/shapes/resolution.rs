use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use core::ops::Not;
use hashbrown::{HashMap, HashSet};
use rls2fga_types::RowNaming;
use rls2fga_types::TranslationNote;
use rls2fga_types::UnrestrictedTable;
use rls2fga_types::{ActionAnswer, ActionRelations, ActionStatement};
use rls2fga_types::{RecordDerivation, RecordDescription, ReplayScope};
use rls2fga_types::{RelationShapes, RowDecision};

use rls2fga_types::{ColumnName, RelationName, TableId as ContractTableId};
use sql_traits::prelude::DatabaseLike;

use crate::visibility::records::is_evaluable;
use crate::visibility::store::{name_gap, Uncovered, UncoveredReason};
use crate::{catalog_helpers, ColumnId, TableId};

use super::required_parameter::{RequiredParameter, TableShapes};

/// The answer every statement on an unrestricted table gets.
///
/// Held once so [`Shapes::answer`] can hand out a reference to it: the table is
/// in a set rather than in the answer map, so there is no stored value to
/// borrow.
static UNRESTRICTED: ActionAnswer = ActionAnswer::Unrestricted;

/// One translation's descriptions, indexed for every reader of them.
///
/// Build it from [`Translation`](rls2fga::translator::Translation) and the
/// catalog those relations were planned against, then share it.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use subql::backend::Postgres;
/// use rls2fga_types::ConfidenceLevel;
/// use rls2fga::translator::TranslatorBuilder;
/// use sqlparser::dialect::PostgreSqlDialect;
/// use rls2fga_types::ActionStatement;
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
/// let translation = translator.translate(&db)?;
/// // `relations` borrows the translation, which borrows `db`, so take an owned
/// // copy and end the borrow before `Shapes::new` takes the catalog.
/// let relations = translation.relations().to_vec();
/// let naming = Vec::from(translation.row_naming());
/// let answers = translation.action_relations();
/// drop(translation);
///
/// let shapes = Arc::new(
///     Shapes::new::<Postgres>(db, &relations)
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
    /// Tables the database filters nothing on, which the action report cannot
    /// carry because it is keyed by the type the model gives a table and these
    /// have none.
    unrestricted: HashSet<TableId>,
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
    pub fn new<B: crate::backend::Backend>(db: DB, relations: &[RelationShapes]) -> Self {
        let mut recipes = HashMap::new();
        let mut by_table: HashMap<TableId, TableShapes> = HashMap::new();
        let mut uncovered = Vec::new();
        let contested = contested_pairs(relations);

        for entry in relations {
            index_recipe::<B, DB>(&db, entry, &mut recipes);
            for shape in &entry.shapes {
                index_shape::<B, DB>(&db, entry, shape, &contested, &mut by_table, &mut uncovered);
            }
        }

        Self {
            db,
            recipes,
            by_table,
            uncovered,
            answers: HashMap::new(),
            naming: HashMap::new(),
            unrestricted: HashSet::new(),
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
                catalog_helpers::contract_table_id(&self.db, &entry.table)
                    .map(|id| (id, entry.clone()))
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

    /// Take the tables the database filters nothing on, from
    /// [`Translation::unrestricted_tables`](rls2fga::translator::Translation::unrestricted_tables).
    ///
    /// Read positively and beside
    /// [`with_action_relations`](Self::with_action_relations): a table here
    /// shows every row to everybody, so a question about one of its rows is
    /// granted with nothing asked. **A table in neither report stays
    /// unanswered**, because absent means uncovered and uncovered still
    /// delegates.
    ///
    /// The action report cannot carry these on its own. It is keyed by the type
    /// the model gives a table, and the database leaves open plenty of tables
    /// the model types nothing for: one nothing can name a row of, and one no
    /// policy anywhere reaches. Those answer nowhere else, so without this a
    /// reader delegates a question the model defines no type to ask.
    ///
    /// # Examples
    ///
    /// ```
    /// use rls2fga_types::ConfidenceLevel;
    /// use rls2fga_types::ActionStatement;
    /// use rls2fga::translator::TranslatorBuilder;
    /// use sqlparser::dialect::PostgreSqlDialect;
    /// use subql::backend::Postgres;
    /// use subql::visibility::shapes::Shapes;
    /// use subql::{catalog_helpers, ParserDB};
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE orders (id INT PRIMARY KEY, quantity BIGINT NOT NULL);",
    /// )
    /// .expect("the schema parses");
    /// let orders = catalog_helpers::table_id(&db, "orders").expect("orders is in the catalog");
    ///
    /// let translator = TranslatorBuilder::new()
    ///     .with_min_confidence(ConfidenceLevel::B)
    ///     .build();
    /// let translation = translator
    ///     .translate(&db)
    ///     .expect("the schema translates");
    /// // `relations` borrows the translation, which borrows `db`, so take an
    /// // owned copy and end the borrow before `Shapes::new` takes the catalog.
    /// let relations = translation.relations().to_vec();
    /// let naming = Vec::from(translation.row_naming());
    /// let answers = translation.action_relations();
    /// let open = translation.unrestricted_tables();
    /// drop(translation);
    ///
    /// let shapes = Shapes::new::<Postgres>(db, &relations)
    ///     .with_row_naming(&naming)
    ///     .with_action_relations(&answers)
    ///     .with_unrestricted_tables(&open);
    ///
    /// // Row-level security is off, so there is nothing to ask anybody.
    /// assert!(shapes.answers_locally(orders, ActionStatement::Select));
    /// ```
    #[must_use]
    pub fn with_unrestricted_tables(mut self, tables: &[UnrestrictedTable]) -> Self {
        self.unrestricted = tables
            .iter()
            .filter_map(|entry| catalog_helpers::contract_table_id(&self.db, &entry.table))
            .collect();
        self
    }

    /// What answers `statement` on rows of `table`, or [`None`] when nothing
    /// said.
    ///
    /// [`None`] means the index was built without either report, or neither
    /// covered the table, and either way a reader has to delegate rather than
    /// pick a relation itself.
    ///
    /// **The action report wins where both speak.** `rls2fga` reports a table it
    /// types and leaves open through both surfaces, and the two agree there, so
    /// the order costs nothing in that case. It matters only if they ever
    /// disagree, and then the typed answer is the one that restricts, which is
    /// the direction a wrong answer must fall.
    #[must_use]
    pub fn answer(&self, table: TableId, statement: ActionStatement) -> Option<&ActionAnswer> {
        self.naming
            .get(&table)
            .and_then(|named| self.answers.get(named.type_name.as_str()))
            .and_then(|answers| answers.get(&statement))
            .or_else(|| self.unrestricted.contains(&table).then_some(&UNRESTRICTED))
    }

    /// Whether a question about `statement` on a row of `table` can be answered
    /// without a round trip.
    ///
    /// Reports the recipes, not the row: a row whose cell fails to decode is
    /// still delegated, and so is a watcher that cannot supply a value a recipe
    /// compares against.
    ///
    /// A refusal counts, because it is knowledge rather than the absence of it,
    /// and it is the cheapest answer there is: nobody is granted, so no watcher
    /// needs a question.
    #[must_use]
    pub fn answers_locally(&self, table: TableId, statement: ActionStatement) -> bool {
        match self.answer(table, statement) {
            // Everybody or nobody. Which of the two is the verdict's business,
            // and either way the report answered and no watcher needs a
            // question.
            Some(ActionAnswer::Unrestricted | ActionAnswer::Denied) => true,
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
fn index_recipe<B: crate::backend::Backend, DB: DatabaseLike>(
    db: &DB,
    entry: &RelationShapes,
    recipes: &mut HashMap<(TableId, RelationName), RowDecision>,
) {
    let Some(decision) = entry.decision.as_ref() else {
        return;
    };
    let Some(table) = usable_table::<B, DB>(decision, db)
        .and_then(|name| catalog_helpers::contract_table_id(db, name))
    else {
        return;
    };
    recipes.insert((table, entry.relation.clone()), decision.clone());
}

/// Index one shape by the table whose changes move it, naming it uncovered
/// when nothing here can keep its records current.
fn index_shape<B: crate::backend::Backend, DB: DatabaseLike>(
    db: &DB,
    entry: &RelationShapes,
    shape: &RecordDescription,
    contested: &HashSet<(String, RelationName)>,
    by_table: &mut HashMap<TableId, TableShapes>,
    uncovered: &mut Vec<Uncovered>,
) {
    match &shape.derivation {
        RecordDerivation::FromRow { table, .. } => {
            if !is_evaluable::<B, DB>(shape, db) {
                uncovered.push(name_gap(
                    entry,
                    shape,
                    table,
                    UncoveredReason::UnreadableColumn,
                ));
                return;
            }
            if let Some(id) = catalog_helpers::contract_table_id(db, table) {
                by_table.entry(id).or_default().settled.push(shape.clone());
            }
        }
        RecordDerivation::Joined { .. }
            if stated_pairs(shape)
                .iter()
                .any(|pair| contested.contains(pair)) =>
        {
            // A slice another shape also states is not this one's to
            // reconcile: what the replay stopped returning may be the other
            // shape's living fact. The whole shape is named rather than
            // partially maintained, since a difference right about some
            // relations and silently wrong about others is worse than none.
            uncovered.extend(
                shape
                    .tables
                    .iter()
                    .map(|table| name_gap(entry, shape, table, UncoveredReason::SharedSlice)),
            );
        }
        RecordDerivation::Joined { queries, .. } => {
            let mut bound: Vec<&ContractTableId> = Vec::with_capacity(queries.len());
            for query in queries {
                let Some(id) = catalog_helpers::contract_table_id(db, &query.table) else {
                    continue;
                };
                let Some(key) = resolve_key(db, id, &query.key_columns) else {
                    continue;
                };
                bound.push(&query.table);
                by_table
                    .entry(id)
                    .or_default()
                    .requeries
                    .push((key, query.clone()));
            }
            // A table the shape reads with no query this catalog can bind has
            // nothing to replay when a change arrives there. Asked of the
            // queries that were indexed rather than of the ones present, so a
            // query dropped for a column this catalog does not have is a named
            // gap instead of a silent one.
            for table in &shape.tables {
                if !bound.contains(&table) {
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

/// The (type, relation) pairs stated by more than one distinct description.
///
/// A replay's slice inside such a pair belongs to nobody alone, and two
/// entries carrying the same description state each pair once: the same
/// source feeding two relations is deduplicated by value, not by entry.
fn contested_pairs(relations: &[RelationShapes]) -> HashSet<(String, RelationName)> {
    let mut stated: HashMap<(String, RelationName), Vec<&RecordDescription>> = HashMap::new();
    for entry in relations {
        for shape in &entry.shapes {
            for pair in stated_pairs(shape) {
                let bucket = stated.entry(pair).or_default();
                if !bucket.contains(&shape) {
                    bucket.push(shape);
                }
            }
        }
    }
    stated
        .into_iter()
        .filter(|(_, shapes)| shapes.len() > 1)
        .map(|(pair, _)| pair)
        .collect()
}

/// Every (type, relation) pair `shape` states records for.
fn stated_pairs(shape: &RecordDescription) -> Vec<(String, RelationName)> {
    match &shape.derivation {
        RecordDerivation::FromRow { template, .. } => {
            alloc::vec![(template.object_type.clone(), template.relation.clone())]
        }
        RecordDerivation::Joined { queries, .. } => {
            let mut out = Vec::new();
            for query in queries {
                match &query.scope {
                    ReplayScope::Object {
                        object_type,
                        relations,
                    } => {
                        for relation in relations {
                            out.push((object_type.clone(), relation.clone()));
                        }
                    }
                    ReplayScope::Subject {
                        object_type,
                        relation,
                        ..
                    } => out.push((object_type.clone(), relation.clone())),
                }
            }
            out
        }
        // `RecordDerivation` is `#[non_exhaustive]`: a shape this does not
        // understand states nothing here, and `index_shape` already names it
        // uncovered.
        _ => Vec::new(),
    }
}

/// Every column of `columns` resolved against `table`, in that order.
///
/// All of them or none, since the query binds one placeholder per column: a
/// key short of a column cannot be run at all, and were it run it would name
/// every row sharing the columns that remain. An empty list is refused for the
/// same reason, a query bound to nothing naming the whole table.
fn resolve_key<DB: DatabaseLike>(
    db: &DB,
    table: TableId,
    columns: &[ColumnName],
) -> Option<Vec<ColumnId>> {
    if columns.is_empty() {
        return None;
    }
    columns
        .iter()
        .map(|column| catalog_helpers::column_id(db, table, column.as_str()))
        .collect()
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
fn usable_table<'a, B: crate::backend::Backend, DB: DatabaseLike>(
    decision: &'a RowDecision,
    db: &DB,
) -> Option<&'a ContractTableId> {
    let mut table: Option<&ContractTableId> = None;
    match decision {
        RowDecision::Leaf { shapes, .. } | RowDecision::RequestGated { shapes, .. } => {
            for shape in shapes {
                if !is_evaluable::<B, DB>(shape, db) {
                    return None;
                }
                let RecordDerivation::FromRow { table: name, .. } = &shape.derivation else {
                    return None;
                };
                if *table.get_or_insert(name) != name {
                    return None;
                }
            }
        }
        RowDecision::Any(children) | RowDecision::All(children) => {
            for child in children {
                let name = usable_table::<B, DB>(child, db)?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use crate::backend::Postgres;
    use alloc::vec::Vec;

    use core::ops::Not;

    use rls2fga::translator::TranslatorBuilder;
    use rls2fga_types::ConfidenceLevel;
    use rls2fga_types::{ActionAnswer, ActionStatement};
    use sqlparser::dialect::PostgreSqlDialect;

    use super::Shapes;
    use crate::{catalog_helpers, ParserDB, TableId};

    /// Every statement a table can be asked about, so a test says "every" rather
    /// than "the one I remembered".
    const EVERY_STATEMENT: [ActionStatement; 8] = [
        ActionStatement::Select,
        ActionStatement::Insert,
        ActionStatement::Update,
        ActionStatement::Delete,
        ActionStatement::SelectForUpdate,
        ActionStatement::InsertOnConflictUpdate,
        ActionStatement::InsertReturning,
        ActionStatement::UpdateWithoutWhere,
    ];

    /// Build the index the way a real caller does, from all three reports.
    fn shapes(sql: &str) -> Shapes<ParserDB> {
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let translation = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap();
        let (relations, naming, answers, unrestricted) = (
            translation.relations().to_vec(),
            Vec::from(translation.row_naming()),
            translation.action_relations(),
            translation.unrestricted_tables(),
        );
        drop(translation);
        Shapes::new::<Postgres>(db, &relations)
            .with_row_naming(&naming)
            .with_action_relations(&answers)
            .with_unrestricted_tables(&unrestricted)
    }

    fn table(shapes: &Shapes<ParserDB>, name: &str) -> TableId {
        catalog_helpers::table_id(shapes.catalog(), name).unwrap()
    }

    /// The reported case, and the one the action report cannot carry: `orders`
    /// gets no type, because no policy anywhere reaches it, so it is answered
    /// nowhere else.
    #[test]
    fn an_open_table_the_model_types_nothing_for_is_granted_without_asking() {
        let shapes = shapes("CREATE TABLE orders (id INT PRIMARY KEY, quantity BIGINT NOT NULL);");
        let orders = table(&shapes, "orders");
        assert!(
            shapes.naming(orders).is_none(),
            "nothing types this table, which is why the action report is empty for it"
        );
        for statement in EVERY_STATEMENT {
            assert_eq!(
                shapes.answer(orders, statement),
                Some(&ActionAnswer::Unrestricted),
                "row-level security is off, so the database restricts nothing on {statement:?}"
            );
            assert!(shapes.answers_locally(orders, statement));
        }
    }

    /// The report grants the open table and nothing beside it. A reader that
    /// swept the whole schema in would grant every row of the guarded one.
    #[test]
    fn a_guarded_table_beside_an_open_one_is_not_swept_up() {
        let shapes = shapes(
            "CREATE TABLE orders (id INT PRIMARY KEY, quantity BIGINT NOT NULL);
             CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT);
             ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
             CREATE POLICY d ON docs USING (owner = current_user);",
        );
        let docs = table(&shapes, "docs");
        assert_ne!(
            shapes.answer(docs, ActionStatement::Delete),
            Some(&ActionAnswer::Unrestricted),
            "a policy filters this table's rows, so nothing may call it open"
        );
        assert_eq!(
            shapes.answer(table(&shapes, "orders"), ActionStatement::Select),
            Some(&ActionAnswer::Unrestricted)
        );
    }

    /// Absent from both reports means uncovered, and uncovered still delegates.
    /// Row-level security is on here, so the table is not open, and nothing can
    /// name a row of it, so the model gives it no type either.
    #[test]
    fn a_table_in_neither_report_stays_unanswered() {
        let shapes = shapes(
            "CREATE TABLE audit (message TEXT);
             ALTER TABLE audit ENABLE ROW LEVEL SECURITY;",
        );
        let audit = table(&shapes, "audit");
        for statement in EVERY_STATEMENT {
            assert_eq!(
                shapes.answer(audit, statement),
                None,
                "not being covered says nothing about what the database allows"
            );
            assert!(shapes.answers_locally(audit, statement).not());
        }
    }

    /// Row-level security on with no policy grants nobody, which must not
    /// collapse into the state where it is off and grants everybody, and which
    /// is an answer rather than the absence of one.
    #[test]
    fn row_level_security_with_no_policy_is_not_open_and_is_still_answered() {
        let shapes = shapes(
            "CREATE TABLE orders (id INT PRIMARY KEY, quantity BIGINT NOT NULL);
             ALTER TABLE orders ENABLE ROW LEVEL SECURITY;",
        );
        let orders = table(&shapes, "orders");
        for statement in EVERY_STATEMENT {
            assert_eq!(
                shapes.answer(orders, statement),
                Some(&ActionAnswer::Denied),
                "the database shows nobody anything here"
            );
            assert!(
                shapes.answers_locally(orders, statement),
                "nobody is granted {statement:?}, which is knowledge and needs no round trip"
            );
        }
    }

    /// A refusal is reported per statement, so a table that refuses writes and
    /// still grants reads is answered locally on both, by two different routes.
    #[test]
    fn a_table_that_refuses_writes_and_grants_reads_answers_both_locally() {
        let shapes = shapes(
            "CREATE TABLE docs (id INT PRIMARY KEY, owner_id TEXT);
             ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
             CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);",
        );
        let docs = table(&shapes, "docs");
        assert!(
            matches!(
                shapes.answer(docs, ActionStatement::Select),
                Some(ActionAnswer::Judged(_))
            ),
            "the policy names who reads, so the read is a rule to satisfy"
        );
        assert!(shapes.answers_locally(docs, ActionStatement::Select));
        for statement in [
            ActionStatement::Insert,
            ActionStatement::Update,
            ActionStatement::Delete,
            ActionStatement::SelectForUpdate,
        ] {
            assert_eq!(
                shapes.answer(docs, statement),
                Some(&ActionAnswer::Denied),
                "no policy admits {statement:?} here"
            );
            assert!(shapes.answers_locally(docs, statement));
        }
    }

    /// Two tables a guarded policy reaches, which the database itself filters
    /// nothing on. A reader gets the same answer for both without having to
    /// know which of the two reports carried them.
    #[test]
    fn a_table_a_policy_reaches_is_still_open_if_the_database_leaves_it_open() {
        let shapes = shapes(
            "CREATE TABLE projects(id INTEGER PRIMARY KEY);
             CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id));
             CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT);
             ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
             CREATE POLICY d ON docs USING (project_id IN (
               SELECT project_id FROM project_members
               WHERE user_id = current_setting('app.user_id', true)));",
        );
        let listed: Vec<TableId> = ["projects", "project_members"]
            .into_iter()
            .map(|name| table(&shapes, name))
            .collect();
        for open in listed {
            assert_eq!(
                shapes.answer(open, ActionStatement::Select),
                Some(&ActionAnswer::Unrestricted),
                "the database filters none of these rows, by either report"
            );
        }
    }

    #[test]
    fn a_shape_keyed_on_an_unsupported_kind_is_uncovered() {
        use crate::visibility::store::UncoveredReason;

        let shapes = shapes(
            "CREATE TABLE snaps (score DOUBLE PRECISION PRIMARY KEY, owner TEXT);
             ALTER TABLE snaps ENABLE ROW LEVEL SECURITY;
             CREATE POLICY s ON snaps USING (owner = current_user);",
        );
        assert!(
            shapes
                .uncovered()
                .iter()
                .any(|gap| gap.table == "snaps" && gap.reason == UncoveredReason::UnreadableColumn),
            "the unsupported key must be named: {:?}",
            shapes.uncovered()
        );
    }

    #[test]
    fn a_time_keyed_table_is_served() {
        use crate::visibility::store::UncoveredReason;

        let shapes = shapes(
            "CREATE TABLE readings (device_id INT, recorded_at TIMESTAMPTZ, owner TEXT, \
             PRIMARY KEY (device_id, recorded_at));
             ALTER TABLE readings ENABLE ROW LEVEL SECURITY;
             CREATE POLICY r ON readings USING (owner = current_user);",
        );
        assert!(
            !shapes
                .uncovered()
                .iter()
                .any(|gap| gap.table == "readings"
                    && gap.reason == UncoveredReason::UnreadableColumn),
            "a time part now has an identity spelling: {:?}",
            shapes.uncovered()
        );
    }
}
