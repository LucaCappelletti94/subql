use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use core::ops::Not;
use hashbrown::{HashMap, HashSet};
use rls2fga_types::RowNaming;
use rls2fga_types::TranslationNote;
use rls2fga_types::UnrestrictedTable;
use rls2fga_types::{ActionAnswer, ActionRelations, ActionStatement};
use rls2fga_types::{Record, RecordDerivation, RecordDescription};
use rls2fga_types::{RelationShapes, RowDecision};

use rls2fga_types::{ColumnName, RelationName, TableId as ContractTableId};
use sql_traits::prelude::DatabaseLike;

use crate::visibility::records::is_evaluable;
use crate::visibility::store::{
    name_gap, Enumeration, Materialisation, Region, Replay, Uncovered, UncoveredReason,
};
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
/// use subql::visibility::store::Enumeration;
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
/// let outputs = translator.translate(&db)?.outputs_accepting_gaps();
/// let translation = outputs.translation();
/// // A skipped query carries no description, so this drops exactly the
/// // entries that enumerate nothing.
/// let enumerations: Vec<Enumeration<'_>> = outputs
///     .tuple_queries()
///     .iter()
///     .filter_map(|query| {
///         query.description.as_ref().map(|description| Enumeration {
///             description,
///             sql: &query.sql,
///             condition: query.condition.as_deref(),
///         })
///     })
///     .collect();
/// let naming = std::borrow::Cow::from(translation.row_naming()).into_owned();
/// let answers = translation.action_relations().to_vec();
///
/// let shapes = Arc::new(
///     Shapes::new::<Postgres>(db, translation.relations(), &enumerations)
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
    /// Every region reconciled as one unit, which a table's entry indexes into.
    groups: Vec<Materialisation>,
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
    pub fn new<B: crate::backend::Backend>(
        db: DB,
        relations: &[RelationShapes],
        enumerations: &[Enumeration<'_>],
    ) -> Self {
        let mut recipes = HashMap::new();
        let mut by_table: HashMap<TableId, TableShapes> = HashMap::new();
        let mut uncovered = Vec::new();

        let producers = producers(relations);
        let plan = plan_groups(&producers, enumerations);

        for entry in relations {
            index_recipe::<B, DB>(&db, entry, &mut recipes);
        }
        for (position, producer) in producers.iter().enumerate() {
            index_shape::<B, DB>(
                &db,
                producer,
                plan.of(position),
                &mut by_table,
                &mut uncovered,
            );
        }

        Self {
            db,
            recipes,
            by_table,
            uncovered,
            groups: plan.groups,
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
    /// use subql::visibility::store::Enumeration;
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
    /// let outputs = translator
    ///     .translate(&db)
    ///     .expect("the schema translates")
    ///     .outputs_accepting_gaps();
    /// let translation = outputs.translation();
    /// let enumerations: Vec<Enumeration<'_>> = outputs
    ///     .tuple_queries()
    ///     .iter()
    ///     .filter_map(|query| {
    ///         query.description.as_ref().map(|description| Enumeration {
    ///             description,
    ///             sql: &query.sql,
    ///             condition: query.condition.as_deref(),
    ///         })
    ///     })
    ///     .collect();
    /// let naming = std::borrow::Cow::from(translation.row_naming()).into_owned();
    /// let answers = translation.action_relations().to_vec();
    /// let open = translation.unrestricted_tables().to_vec();
    ///
    /// let shapes = Shapes::new::<Postgres>(db, translation.relations(), &enumerations)
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

    /// Every region reconciled as one unit, which the load runs in full.
    ///
    /// The load and an event run the same operation: the load runs all of
    /// these, an event runs the ones its own change obliged. That is what makes
    /// the load self-healing, and it is why the replay path is exercised by
    /// every fixture at startup rather than only by a change that happens to
    /// arrive.
    #[must_use]
    pub fn materialisations(&self) -> &[Materialisation] {
        &self.groups
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

/// One description, with the region it states facts in.
struct Producer<'a> {
    entry: &'a RelationShapes,
    shape: &'a RecordDescription,
    /// Absent for a producer that declares no region here: a constant, or a
    /// derivation this version does not understand, or a joining shape
    /// carrying no query at all.
    region: Option<Region>,
}

/// Every distinct description across `relations`, deduplicated by value.
///
/// By value rather than by entry, because one source feeding two relations
/// appears under both and is still one producer. Counting it twice would make
/// a region look shared and force a group where a lone producer's cheaper
/// path is correct.
fn producers(relations: &[RelationShapes]) -> Vec<Producer<'_>> {
    let mut out: Vec<Producer<'_>> = Vec::new();
    for entry in relations {
        for shape in &entry.shapes {
            if out.iter().any(|seen| seen.shape == shape) {
                continue;
            }
            out.push(Producer {
                entry,
                shape,
                region: region_of(shape),
            });
        }
    }
    out
}

/// Whether `shape` states facts whose region nothing here can bound.
///
/// A joining shape carrying no query at all is the reachable case: `rls2fga`
/// reports the shape and the scope lives inside the query, so there is nothing
/// to read the region off. A derivation this version does not understand is
/// the same problem by another route.
///
/// It matters because a group deletes over its whole region, and a producer
/// whose facts cannot be placed can neither be gathered into that group nor
/// proven to lie outside it.
const fn bounds_no_region(shape: &RecordDescription) -> bool {
    match &shape.derivation {
        RecordDerivation::FromRow { .. }
        | RecordDerivation::Constant { .. }
        | RecordDerivation::WholeShape { .. } => false,
        RecordDerivation::Joined { queries, .. } => queries.is_empty(),
        _ => true,
    }
}

/// The region `shape` states facts in, unnarrowed.
///
/// A joining shape's queries are folded, all of them: the contract holds a
/// list because one producer can be replayed from several tables, and taking
/// one scope would leave the rest of its facts outside the region that
/// reconciles them.
fn region_of(shape: &RecordDescription) -> Option<Region> {
    match &shape.derivation {
        RecordDerivation::FromRow { template, .. } => Some(Region::of_template(
            template.object_type.as_str(),
            &template.relation,
            template.subject_type.as_str(),
        )),
        RecordDerivation::Joined { queries, .. } => {
            let mut region: Option<Region> = None;
            for query in queries {
                let next = Region::of(query.scope());
                match region.as_mut() {
                    Some(held) => held.absorb(&next),
                    None => region = Some(next),
                }
            }
            region
        }
        RecordDerivation::WholeShape { scope, .. } => Some(Region::of(scope)),
        // A constant is authoritative over its one fact and nothing more, so it
        // holds no region: a region would be a type and a relation wide, and two
        // constants alone would then form a group claiming the authority to
        // delete every other fact in it. It joins a group as a donor instead,
        // and its presence is what makes that group necessary.
        //
        // `RecordDerivation` is `#[non_exhaustive]`, and a derivation this does
        // not understand states nothing here either.
        _ => None,
    }
}

/// The fact a constant producer states, if that is what it is.
const fn constant_of(shape: &RecordDescription) -> Option<&Record> {
    match &shape.derivation {
        RecordDerivation::Constant { record } => Some(record),
        _ => None,
    }
}

/// Where one producer's records come from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// Maintained on its own, by differencing or by its own keyed replay.
    Alone,
    /// Maintained by the group at this position.
    Grouped(usize),
    /// Shares a region needing one authoritative reconcile with a producer
    /// whose facts nothing here enumerates.
    Refused,
}

/// Which group each producer belongs to, and the groups themselves.
struct GroupPlan {
    groups: Vec<Materialisation>,
    /// One entry per producer, in the order [`producers`] returned them.
    placed: Vec<Placement>,
}

impl GroupPlan {
    fn of(&self, producer: usize) -> Placement {
        self.placed[producer]
    }
}

/// Gather producers whose regions overlap into groups, and decide which
/// regions need one.
///
/// A region needs a group when a whole shape states facts in it, since nothing
/// narrower can maintain one, when more than one producer states facts in it,
/// since then no producer alone may delete over the region, or when a constant
/// states a fact inside it, since differencing a sibling would delete what the
/// constant still states.
///
/// The region and its constants are settled before that decision, because two
/// of the three reasons cannot be read off the members alone.
///
/// # A producer nothing can place refuses every removal
///
/// Every removal here deletes over some region: a group over its whole one, a
/// keyed replay over the slice its key names, and a difference over whatever
/// the changed row stopped stating. A producer whose facts cannot be placed
/// could hold facts in any of them, so while one exists nothing may delete at
/// all, and every producer that states a region is refused rather than left to
/// maintain itself. Refusing only the groups would leave a row-settled
/// producer differencing, and a difference deletes.
///
/// That is what this path did before groups existed, by reconciling nothing,
/// and it was safe for the same reason. The unplaceable producer keeps its own
/// placement so that the gap it raises names its own cause, and every refusal
/// is reported through [`Shapes::uncovered`](Shapes::uncovered) rather than
/// being silent.
fn plan_groups(producers: &[Producer<'_>], enumerations: &[Enumeration<'_>]) -> GroupPlan {
    let mut placed = alloc::vec![Placement::Alone; producers.len()];
    if producers
        .iter()
        .any(|producer| bounds_no_region(producer.shape))
    {
        for (position, producer) in producers.iter().enumerate() {
            if producer.region.is_some() {
                placed[position] = Placement::Refused;
            }
        }
        return GroupPlan {
            groups: Vec::new(),
            placed,
        };
    }

    let mut groups = Vec::new();
    for members in components(producers) {
        let outcome = plan_component(producers, &members, enumerations);
        let placement = match outcome {
            Planned::Alone => continue,
            Planned::Refused => Placement::Refused,
            Planned::Grouped(group) => {
                groups.push(group);
                Placement::Grouped(groups.len() - 1)
            }
        };
        for position in &members {
            placed[*position] = placement;
        }
    }

    GroupPlan { groups, placed }
}

/// What one component of overlapping producers turned out to need.
enum Planned {
    /// No group: each member maintains itself.
    Alone,
    /// A group is needed and a member's facts are enumerated by nothing.
    Refused,
    /// The group, ready to reconcile.
    Grouped(Materialisation),
}

/// Decide what `members` need, and build the group where they need one.
fn plan_component(
    producers: &[Producer<'_>],
    members: &[usize],
    enumerations: &[Enumeration<'_>],
) -> Planned {
    let Some(region) = union_region(producers, members) else {
        return Planned::Alone;
    };
    let constants = constants_within(producers, &region);
    if !needs_group(producers, members, &constants) {
        return Planned::Alone;
    }
    let mut replays = Vec::with_capacity(members.len());
    for position in members {
        let producer = &producers[*position];
        let Some(own) = producer.region.as_ref() else {
            continue;
        };
        match replay_of(producer, own, enumerations) {
            Some(replay) => replays.push(replay),
            None => return Planned::Refused,
        }
    }
    Planned::Grouped(Materialisation::new(region, replays, constants))
}

/// The union of every region `members` state facts in.
fn union_region(producers: &[Producer<'_>], members: &[usize]) -> Option<Region> {
    let mut region: Option<Region> = None;
    for position in members {
        if let Some(own) = producers[*position].region.as_ref() {
            match region.as_mut() {
                Some(held) => held.absorb(own),
                None => region = Some(own.clone()),
            }
        }
    }
    region
}

/// Every constant fact lying inside `region`.
///
/// Gathered by the fact each one states, since a constant holds no region to
/// overlap with.
fn constants_within(producers: &[Producer<'_>], region: &Region) -> Vec<Record> {
    producers
        .iter()
        .filter_map(|producer| constant_of(producer.shape))
        .filter(|record| region.holds_record(record))
        .cloned()
        .collect()
}

/// The unnarrowed query for one producer, or [`None`] where nothing enumerates
/// its facts.
fn replay_of(
    producer: &Producer<'_>,
    region: &Region,
    enumerations: &[Enumeration<'_>],
) -> Option<Replay> {
    if let RecordDerivation::WholeShape {
        query, condition, ..
    } = &producer.shape.derivation
    {
        return Some(Replay::new(
            query.clone(),
            condition.clone(),
            region.clone(),
        ));
    }
    let mut found: Option<&Enumeration<'_>> = None;
    for candidate in enumerations {
        if candidate.description != producer.shape {
            continue;
        }
        // One description reported twice is an ambiguity nothing here can
        // settle, so the region is refused rather than reconciled from a
        // guess. The condition counts as much as the text: it decides whether
        // the rows carry a condition and which one, so taking either of two
        // would grant on terms the other did not state.
        if found
            .is_some_and(|held| held.sql != candidate.sql || held.condition != candidate.condition)
        {
            return None;
        }
        found = Some(candidate);
    }
    let enumeration = found?;
    Some(Replay::new(
        enumeration.sql.to_string(),
        enumeration.condition.map(ToString::to_string),
        region.clone(),
    ))
}

/// Whether the region these producers share has to be reconciled as one unit.
fn needs_group(producers: &[Producer<'_>], members: &[usize], constants: &[Record]) -> bool {
    members.len() > 1
        || !constants.is_empty()
        || members.iter().any(|position| {
            matches!(
                producers[*position].shape.derivation,
                RecordDerivation::WholeShape { .. }
            )
        })
}

/// Producers grouped into components by region overlap, transitively.
///
/// Transitive because overlap is not transitive on its own: two regions that
/// miss each other can both meet a third, and reconciling either without the
/// other would delete over facts the third states.
fn components(producers: &[Producer<'_>]) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..producers.len()).collect();
    for (left, one) in producers.iter().enumerate() {
        let Some(one) = one.region.as_ref() else {
            continue;
        };
        for (right, two) in producers.iter().enumerate().skip(left + 1) {
            if two.region.as_ref().is_some_and(|two| one.overlaps(two)) {
                union(&mut parent, left, right);
            }
        }
    }

    let mut out: Vec<Vec<usize>> = Vec::new();
    let mut roots: Vec<(usize, usize)> = Vec::new();
    for (position, producer) in producers.iter().enumerate() {
        if producer.region.is_none() {
            continue;
        }
        let root = find(&mut parent, position);
        if let Some((_, bucket)) = roots.iter().find(|(seen, _)| *seen == root) {
            out[*bucket].push(position);
        } else {
            roots.push((root, out.len()));
            out.push(alloc::vec![position]);
        }
    }
    out
}

/// The representative of `position`'s component, halving the path on the way.
const fn find(parent: &mut [usize], mut position: usize) -> usize {
    while parent[position] != position {
        let grandparent = parent[parent[position]];
        parent[position] = grandparent;
        position = grandparent;
    }
    position
}

/// Put both components under one representative.
const fn union(parent: &mut [usize], left: usize, right: usize) {
    let left = find(parent, left);
    let right = find(parent, right);
    if left != right {
        parent[right] = left;
    }
}

/// Index one producer by the table whose changes move it, naming it uncovered
/// when nothing here can keep its records current.
fn index_shape<B: crate::backend::Backend, DB: DatabaseLike>(
    db: &DB,
    producer: &Producer<'_>,
    placement: Placement,
    by_table: &mut HashMap<TableId, TableShapes>,
    uncovered: &mut Vec<Uncovered>,
) {
    let Producer { entry, shape, .. } = producer;
    match placement {
        Placement::Refused => {
            uncovered.extend(
                shape.tables.iter().map(|table| {
                    name_gap(entry, shape, table, UncoveredReason::MissingEnumeration)
                }),
            );
            // Refused for maintenance, which is a different question from what
            // a row implies. The row policy and the resulting-row check of a
            // write both read that, and neither deletes anything, so keeping it
            // cannot reach the store. Dropping it would deny a write the row's
            // own facts would have admitted.
            index_implied(db, shape, by_table);
            return;
        }
        Placement::Grouped(group) => {
            index_grouped(db, shape, group, by_table);
            return;
        }
        Placement::Alone => {}
    }

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
                let held = by_table.entry(id).or_default();
                held.differenced.push(held.settled.len());
                held.settled.push((*shape).clone());
            }
        }
        RecordDerivation::Joined { queries, .. } => {
            let mut bound: Vec<&ContractTableId> = Vec::with_capacity(queries.len());
            for query in queries {
                let Some(id) = catalog_helpers::contract_table_id(db, query.table()) else {
                    continue;
                };
                let Some(key) = resolve_key(db, id, query.key_columns()) else {
                    continue;
                };
                bound.push(query.table());
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
        // A constant needs no table and no query: the load writes it and no
        // event moves it.
        RecordDerivation::Constant { .. } => {}
        // A whole shape only ever reaches here when its region was refused a
        // group, since nothing narrower can maintain one. Naming it as a
        // derivation this does not understand would be the same false report
        // this arm exists to stop making.
        RecordDerivation::WholeShape { .. } => uncovered.extend(
            shape
                .tables
                .iter()
                .map(|table| name_gap(entry, shape, table, UncoveredReason::MissingEnumeration)),
        ),
        // `RecordDerivation` is `#[non_exhaustive]`: a shape this does not
        // understand is named rather than assumed covered.
        _ => uncovered.extend(
            shape
                .tables
                .iter()
                .map(|table| name_gap(entry, shape, table, UncoveredReason::UnknownDerivation)),
        ),
    }
}

/// Index a producer whose region a group maintains.
///
/// The group covers every table the shape reads, so no gap is named, and
/// neither differencing nor a keyed replay is indexed beside it: the group's
/// reconcile is the one authoritative operation over that region.
fn index_grouped<DB: DatabaseLike>(
    db: &DB,
    shape: &RecordDescription,
    group: usize,
    by_table: &mut HashMap<TableId, TableShapes>,
) {
    for table in &shape.tables {
        let Some(id) = catalog_helpers::contract_table_id(db, table) else {
            continue;
        };
        let held = by_table.entry(id).or_default();
        if !held.groups.contains(&group) {
            held.groups.push(group);
        }
    }
    index_implied(db, shape, by_table);
}

/// Record what a row of the table implies, without maintaining anything.
///
/// A different question from who keeps the store current: the row policy and a
/// write's resulting-row check both read this, and neither removes a fact, so
/// it is recorded whatever the maintenance decision was.
fn index_implied<DB: DatabaseLike>(
    db: &DB,
    shape: &RecordDescription,
    by_table: &mut HashMap<TableId, TableShapes>,
) {
    if let RecordDerivation::FromRow { table, .. } = &shape.derivation {
        if let Some(id) = catalog_helpers::contract_table_id(db, table) {
            by_table.entry(id).or_default().settled.push(shape.clone());
        }
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

    use super::{Enumeration, Shapes};
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
        let outputs = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db)
            .unwrap()
            .outputs_accepting_gaps();
        let translation = outputs.translation();
        // A skipped query carries no description, so `filter_map` drops exactly
        // the entries that enumerate nothing.
        let enumerations: Vec<Enumeration<'_>> = outputs
            .tuple_queries()
            .iter()
            .filter_map(|query| {
                query.description.as_ref().map(|description| Enumeration {
                    description,
                    sql: &query.sql,
                    condition: query.condition.as_deref(),
                })
            })
            .collect();
        let naming = alloc::borrow::Cow::from(translation.row_naming()).into_owned();
        let answers = translation.action_relations().to_vec();
        let unrestricted = translation.unrestricted_tables().to_vec();
        Shapes::new::<Postgres>(db, translation.relations(), &enumerations)
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
