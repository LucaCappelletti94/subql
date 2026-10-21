//! Durable evidence from actual source commits and transactional `PostgreSQL` storage.

#![expect(
    clippy::too_many_lines,
    reason = "complete recovery proofs keep source commits and transactional assertions in one scope"
)]

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::common;

use diesel::dsl::{count, count_star, max, min, sum};
use diesel::prelude::*;
use diesel::sql_query;
#[cfg(feature = "membership-term")]
use rls2fga::translator::TranslatorBuilder;
#[cfg(feature = "membership-term")]
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{CdcEvent, Postgres, Value};
use subql::reexec::ReExecutionRead;
#[cfg(feature = "membership-term")]
use subql::term::TermDescription;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, AggValue, Checkpoint, DefaultIds, DurableAuthorizationError, DurableCommit,
    DurableContext, DurableEngine, DurableError, DurableFailure, DurableOperation,
    DurablePersistError, GroupedScalarSeedInstall, Install, NumericValue, OpaqueCheckpoint,
    PgCommitPosition, PgLsn, ReplayAnswer, ReplayTable, ReplayValue, ScalarInstall,
    SubscriptionEngine, SubscriptionRequest, TableId, Tier, Wal2JsonV2Event,
};

type Event = Wal2JsonV2Event;
type C = PgCommitPosition;
type Compiler = SubscriptionEngine<Event, DefaultIds, ParserDB>;
type Engine = DurableEngine<Event, DefaultIds, ParserDB>;
type LiveEvent = TestEvent<Postgres, PgLsn>;
type StoreReceipt = DurableCommit<DefaultIds, C>;

const SOURCE: &[u8] = b"pg-source-generation-1";
const AUTHORITY: &[u8] = b"pg-policy-1";
const QUOTA: usize = 1_000_000;

/// Source DDL, one string for the catalog and the database.
const SCHEMA: &str = "
CREATE TABLE items (id INTEGER PRIMARY KEY, cost INTEGER, qty INTEGER, status TEXT);
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE project_members (project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY (project_id, user_id));
CREATE TABLE docs (id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT);
";

diesel::table! {
    items (id) {
        id -> Integer,
        cost -> Integer,
        qty -> Nullable<Integer>,
        status -> Text,
    }
}

diesel::table! {
    notes (id) {
        id -> Integer,
        body -> Text,
    }
}

diesel::table! {
    projects (id) {
        id -> Integer,
        name -> Text,
    }
}

diesel::table! {
    project_members (project_id, user_id) {
        project_id -> Integer,
        user_id -> Text,
    }
}

diesel::table! {
    docs (id) {
        id -> Integer,
        project_id -> Integer,
        title -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(docs, project_members);

diesel::table! {
    durable_state (id) {
        id -> BigInt,
        revision -> BigInt,
        boundary -> Bytea,
        checkpoint -> Bytea,
    }
}

diesel::table! {
    durable_evidence (instance) {
        instance -> BigInt,
        boundary -> Bytea,
    }
}

diesel::table! {
    durable_failure (instance) {
        instance -> BigInt,
        at -> Bytea,
        last_valid -> Bytea,
        quota -> BigInt,
    }
}

/// Why the store refused a persistence.
#[derive(Debug, thiserror::Error)]
enum StoreError {
    /// The expected storage revision is gone.
    #[error("storage revision compare-and-swap failed")]
    RevisionMismatch,
    /// The callback refused the persistence.
    #[error("storage refused the persistence")]
    Refused,
    /// A store statement failed.
    #[error(transparent)]
    Sql(#[from] diesel::result::Error),
}

/// Where a forced server error strikes inside the persistence transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    /// No forced failure.
    None,
    /// After the revision compare-and-swap, before any evidence write.
    AfterCas,
    /// After the evidence writes, before the failure records.
    AfterEvidence,
}

/// The consumer's real SQL store behind the persistence callbacks.
struct Store {
    conn: PgConnection,
    database: common::pg::PgDatabase,
}

impl Store {
    /// Open the store tables on a dedicated connection.
    fn new() -> Self {
        let database = common::pg_database();
        let mut conn = database.connect();
        // DDL, which the query DSL does not express.
        sql_query(
            "CREATE TABLE durable_state (id BIGINT PRIMARY KEY, revision BIGINT NOT NULL CHECK (revision >= 0), \
             boundary BYTEA NOT NULL, checkpoint BYTEA NOT NULL)",
        )
        .execute(&mut conn)
        .expect("the state table");
        sql_query(
            "CREATE TABLE durable_evidence (instance BIGINT PRIMARY KEY, boundary BYTEA NOT NULL)",
        )
        .execute(&mut conn)
        .expect("the evidence table");
        sql_query(
            "CREATE TABLE durable_failure (instance BIGINT PRIMARY KEY, at BYTEA NOT NULL, \
             last_valid BYTEA NOT NULL, quota BIGINT NOT NULL)",
        )
        .execute(&mut conn)
        .expect("the failure table");
        Self { conn, database }
    }

    fn install(&mut self, id: i64, boundary: &C) {
        diesel::insert_into(durable_state::table)
            .values((
                durable_state::id.eq(id),
                durable_state::revision.eq(0),
                durable_state::boundary.eq(boundary.to_opaque().0),
                durable_state::checkpoint.eq(Vec::<u8>::new()),
            ))
            .execute(&mut self.conn)
            .expect("the state row installs");
    }

    /// Persist one receipt in one store transaction under the revision compare-and-swap.
    fn apply(
        &mut self,
        id: i64,
        receipt: &StoreReceipt,
        fail: FailurePoint,
    ) -> Result<(), StoreError> {
        self.conn.transaction::<(), StoreError, _>(|tx| {
            let cas = diesel::update(durable_state::table.find(id))
                .set((
                    durable_state::revision
                        .eq(i64::try_from(receipt.revision).expect("the revision fits")),
                    durable_state::boundary.eq(receipt.at.to_opaque().0),
                    durable_state::checkpoint.eq(&receipt.checkpoint),
                ))
                .filter(
                    durable_state::revision
                        .eq(i64::try_from(receipt.expected_revision).expect("the revision fits")),
                )
                .execute(tx)?;
            if cas != 1 {
                return Err(StoreError::RevisionMismatch);
            }
            if fail == FailurePoint::AfterCas {
                diesel::update(durable_state::table.find(id))
                    .set(durable_state::revision.eq(-1_i64))
                    .execute(tx)?;
            }
            match &receipt.operation {
                DurableOperation::Source => {
                    for change in &receipt.changed {
                        diesel::insert_into(durable_evidence::table)
                            .values((
                                durable_evidence::instance
                                    .eq(i64::try_from(change.instance).expect("the instance fits")),
                                durable_evidence::boundary.eq(change.at.to_opaque().0),
                            ))
                            .on_conflict(durable_evidence::instance)
                            .do_update()
                            .set(durable_evidence::boundary.eq(change.at.to_opaque().0))
                            .execute(tx)?;
                    }
                    if fail == FailurePoint::AfterEvidence {
                        diesel::update(durable_state::table.find(id))
                            .set(durable_state::revision.eq(-1_i64))
                            .execute(tx)?;
                    }
                    for failure in &receipt.failures {
                        diesel::insert_into(durable_failure::table)
                            .values(
                                (
                                    durable_failure::instance
                                        .eq(i64::try_from(failure.instance)
                                            .expect("the instance fits")),
                                    durable_failure::at.eq(failure.at.to_opaque().0),
                                    durable_failure::last_valid
                                        .eq(failure.last_valid.to_opaque().0),
                                    durable_failure::quota
                                        .eq(i64::try_from(failure.quota).expect("the quota fits")),
                                ),
                            )
                            .on_conflict(durable_failure::instance)
                            .do_update()
                            .set((
                                durable_failure::at.eq(failure.at.to_opaque().0),
                                durable_failure::last_valid.eq(failure.last_valid.to_opaque().0),
                                durable_failure::quota
                                    .eq(i64::try_from(failure.quota).expect("the quota fits")),
                            ))
                            .execute(tx)?;
                    }
                }
                DurableOperation::Acknowledge { instance, unread } => match unread {
                    Some(unread) => {
                        diesel::insert_into(durable_evidence::table)
                            .values((
                                durable_evidence::instance
                                    .eq(i64::try_from(*instance).expect("the instance fits")),
                                durable_evidence::boundary.eq(unread.to_opaque().0),
                            ))
                            .on_conflict(durable_evidence::instance)
                            .do_update()
                            .set(durable_evidence::boundary.eq(unread.to_opaque().0))
                            .execute(tx)?;
                    }
                    None => {
                        diesel::delete(
                            durable_evidence::table
                                .find(i64::try_from(*instance).expect("the instance fits")),
                        )
                        .execute(tx)?;
                    }
                },
                DurableOperation::Remove { instance } => {
                    diesel::delete(
                        durable_evidence::table
                            .find(i64::try_from(*instance).expect("the instance fits")),
                    )
                    .execute(tx)?;
                    diesel::delete(
                        durable_failure::table
                            .find(i64::try_from(*instance).expect("the instance fits")),
                    )
                    .execute(tx)?;
                }
                DurableOperation::Register { .. } => {}
            }
            Ok(())
        })
    }

    fn state(&mut self, id: i64) -> (i64, Vec<u8>, Vec<u8>) {
        durable_state::table
            .find(id)
            .select((
                durable_state::revision,
                durable_state::boundary,
                durable_state::checkpoint,
            ))
            .first::<(i64, Vec<u8>, Vec<u8>)>(&mut self.conn)
            .expect("the state row persists")
    }

    fn revision(&mut self, id: i64) -> i64 {
        self.state(id).0
    }

    fn evidence(&mut self, instance: i64) -> Option<Vec<u8>> {
        durable_evidence::table
            .find(instance)
            .select(durable_evidence::boundary)
            .first::<Vec<u8>>(&mut self.conn)
            .optional()
            .expect("the evidence read runs")
    }

    fn failure(&mut self, instance: i64) -> Option<(Vec<u8>, Vec<u8>, i64)> {
        durable_failure::table
            .find(instance)
            .select((
                durable_failure::at,
                durable_failure::last_valid,
                durable_failure::quota,
            ))
            .first::<(Vec<u8>, Vec<u8>, i64)>(&mut self.conn)
            .optional()
            .expect("the failure read runs")
    }
}

/// The receipt facts the assertions need, captured while persisting.
#[derive(Debug)]
struct ReceiptData {
    previous: C,
    at: C,
    changed: Vec<(u64, C)>,
    failures: Vec<DurableFailure<C>>,
    operation: DurableOperation<C>,
}

fn receipt_data(receipt: &StoreReceipt) -> ReceiptData {
    ReceiptData {
        previous: receipt.previous,
        at: receipt.at,
        changed: receipt
            .changed
            .iter()
            .map(|change| (change.instance, change.at))
            .collect(),
        failures: receipt.failures.clone(),
        operation: receipt.operation.clone(),
    }
}

/// How a restore's fresh adoption decides.
#[derive(Clone, Copy)]
enum RestoreAuth {
    Ok,
    Denied,
    Unavailable,
}

fn authorize(
    outcome: RestoreAuth,
) -> impl FnMut(&SubscriptionRequest<DefaultIds, Postgres>) -> Result<(), DurableAuthorizationError>
{
    move |_request: &SubscriptionRequest<DefaultIds, Postgres>| match outcome {
        RestoreAuth::Ok => Ok(()),
        RestoreAuth::Denied => Err(DurableAuthorizationError::Denied),
        RestoreAuth::Unavailable => Err(DurableAuthorizationError::Unavailable),
    }
}

fn context() -> DurableContext {
    DurableContext {
        source: SOURCE.to_vec(),
        authority: AUTHORITY.to_vec(),
    }
}

fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(SCHEMA).expect("the catalog parses")
}

fn table_id(name: &str) -> TableId {
    catalog_helpers::table_id::<Postgres, _>(&catalog(), name).expect("the table resolves")
}

fn compiler() -> Compiler {
    SubscriptionEngine::new(catalog(), PostgreSqlDialect {})
}

#[cfg(feature = "membership-term")]
fn membership_compiler() -> Compiler {
    compiler().with_translator(
        TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build(),
    )
}

fn durable_engine(bootstrap: C) -> Engine {
    DurableEngine::new(compiler(), context(), bootstrap)
        .expect("a durable engine at the bootstrap boundary")
}

/// Restore from the store's committed bytes under the given adoption outcome.
fn restore(store: &mut Store, id: i64, outcome: RestoreAuth) -> Engine {
    restore_with_compiler(store, id, outcome, compiler())
}

fn restore_with_compiler(
    store: &mut Store,
    id: i64,
    outcome: RestoreAuth,
    compiler: Compiler,
) -> Engine {
    let (revision, boundary, checkpoint) = store.state(id);
    let at = C::from_opaque(&OpaqueCheckpoint(boundary)).expect("the stored boundary decodes");
    DurableEngine::restore(
        compiler,
        context(),
        u64::try_from(revision).expect("the revision fits"),
        at,
        &checkpoint,
        authorize(outcome),
    )
    .expect("the checkpoint restores")
}

/// The boundary a bootstrap read at this moment of the stream acknowledges.
fn wal_boundary(pg: &mut PgConnection) -> C {
    // Vendor WAL function, which the query DSL does not express.
    #[derive(diesel::QueryableByName)]
    struct Lsn {
        #[diesel(sql_type = diesel::sql_types::Text)]
        lsn: String,
    }
    let row: Lsn = sql_query("SELECT pg_current_wal_lsn()::text AS lsn")
        .get_result(pg)
        .expect("the wal lsn reads");
    C::before_commit(PgLsn::parse(&row.lsn).expect("the lsn parses"))
}

fn create_schema(pg: &mut PgConnection) {
    // DDL, which the query DSL does not express.
    for statement in SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
        sql_query(statement).execute(pg).expect("schema statement");
    }
}

fn seed(pg: &mut PgConnection, rows: &[(i32, i32, Option<i32>, &str)]) {
    for (id, cost, qty, status) in rows {
        diesel::insert_into(items::table)
            .values((
                items::id.eq(*id),
                items::cost.eq(*cost),
                items::qty.eq(*qty),
                items::status.eq(*status),
            ))
            .execute(pg)
            .expect("seed row");
    }
}

/// The complete items table as the row family projects it.
fn bootstrap_items(pg: &mut PgConnection) -> Vec<Vec<Value<Postgres>>> {
    let rows: Vec<(i32, i32, Option<i32>, String)> = items::table
        .select((items::id, items::cost, items::qty, items::status))
        .order(items::id.asc())
        .load(pg)
        .expect("the bootstrap read runs");
    rows.into_iter()
        .map(|(id, cost, qty, status)| {
            vec![
                Value::Int(i64::from(id)),
                Value::Int(i64::from(cost)),
                qty.map_or(Value::Null, |qty| Value::Int(i64::from(qty))),
                Value::String(status),
            ]
        })
        .collect()
}

fn items_table(rows: Vec<Vec<Value<Postgres>>>) -> ReplayTable<Postgres> {
    ReplayTable {
        table_id: table_id("items"),
        rows,
    }
}

fn docs_table(rows: Vec<Vec<Value<Postgres>>>) -> ReplayTable<Postgres> {
    ReplayTable {
        table_id: table_id("docs"),
        rows,
    }
}

fn members_table(rows: Vec<Vec<Value<Postgres>>>) -> ReplayTable<Postgres> {
    ReplayTable {
        table_id: table_id("project_members"),
        rows,
    }
}

/// Drain the slot into complete commits of retained events, in commit order.
fn drain_commits(pg: &mut PgConnection, slot: &str) -> Vec<(C, Vec<Event>)> {
    let events = common::read_wal2json_v2(&common::drain_slot(pg, slot));
    let mut commits: Vec<(C, Vec<Event>)> = Vec::new();
    for event in events {
        let at = event
            .checkpoint()
            .expect("the slot drains with include-lsn and include-xids")
            .transaction_boundary()
            .expect("a row position maps to its commit");
        match commits.last_mut() {
            Some((boundary, rows)) if *boundary == at => rows.push(event),
            _ => commits.push((at, vec![event])),
        }
    }
    commits
}

/// Admit one instance from plain SQL and persist the admission through the store.
fn admit(
    engine: &mut Engine,
    store: &mut Store,
    id: i64,
    instance: u64,
    sql: &str,
    tables: Vec<ReplayTable<Postgres>>,
    quota: usize,
) {
    engine
        .prepare_register(
            instance,
            SubscriptionRequest::new(instance, sql),
            *engine.checkpoint_position(),
            tables,
            quota,
            |_| Ok(()),
        )
        .expect("the admission is authorized")
        .persist(|receipt| store.apply(id, receipt, FailurePoint::None))
        .expect("the store persists the admission");
}

/// Admit one instance from a request that already states its terms.
fn admit_request(
    engine: &mut Engine,
    store: &mut Store,
    id: i64,
    instance: u64,
    request: SubscriptionRequest<DefaultIds, Postgres>,
    tables: Vec<ReplayTable<Postgres>>,
    quota: usize,
) {
    engine
        .prepare_register(
            instance,
            request,
            *engine.checkpoint_position(),
            tables,
            quota,
            |_| Ok(()),
        )
        .expect("the admission is authorized")
        .persist(|receipt| store.apply(id, receipt, FailurePoint::None))
        .expect("the store persists the admission");
}

/// Feed one drained commit through the engine and the store, returning the receipt facts.
fn advance_one(
    engine: &mut Engine,
    store: &mut Store,
    id: i64,
    source: &[u8],
    at: C,
    events: &[Event],
    fail: FailurePoint,
) -> ReceiptData {
    let captured = RefCell::new(None);
    {
        let after = *engine.checkpoint_position();
        engine.prepare_transaction(source, &after, at, events)
    }
    .expect("a complete commit prepares")
    .expect("a fresh commit is not a duplicate")
    .persist(|receipt| {
        captured.borrow_mut().replace(receipt_data(receipt));
        store.apply(id, receipt, fail)
    })
    .expect("the store persists the commit");
    captured.into_inner().expect("the callback ran")
}

fn acknowledge(
    engine: &mut Engine,
    store: &mut Store,
    id: i64,
    instance: u64,
    through: C,
    outcome: RestoreAuth,
) -> Result<ReceiptData, DurableError> {
    let captured = RefCell::new(None);
    let prepared = engine.prepare_acknowledge(instance, &through, authorize(outcome))?;
    prepared
        .persist(|receipt| {
            captured.borrow_mut().replace(receipt_data(receipt));
            store.apply(id, receipt, FailurePoint::None)
        })
        .expect("the store persists the acknowledgement");
    Ok(captured.into_inner().expect("the callback ran"))
}

fn remove(
    engine: &mut Engine,
    store: &mut Store,
    id: i64,
    instance: u64,
) -> Result<ReceiptData, DurableError> {
    let captured = RefCell::new(None);
    let prepared = engine.prepare_remove(instance, authorize(RestoreAuth::Ok))?;
    prepared
        .persist(|receipt| {
            captured.borrow_mut().replace(receipt_data(receipt));
            store.apply(id, receipt, FailurePoint::None)
        })
        .expect("the store persists the removal");
    Ok(captured.into_inner().expect("the callback ran"))
}

fn answer(engine: &Engine, instance: u64) -> ReplayAnswer<Postgres> {
    engine
        .answer(instance)
        .cloned()
        .unwrap_or_else(|| panic!("instance {instance} holds an answer"))
}

/// The fold a scalar aggregate answered.
fn folded_of(answer: ReplayAnswer<Postgres>) -> AggValue {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Folded(fold)) => fold,
        other => panic!("a fold query answers a fold, got {other:?}"),
    }
}

/// The value an extreme answered.
fn scalar_of(answer: ReplayAnswer<Postgres>) -> Value<Postgres> {
    match answer {
        ReplayAnswer::Scalar(ReplayValue::Scalar(value)) => value,
        other => panic!("an extreme query answers a value, got {other:?}"),
    }
}

const fn fold_of(value: &ReplayValue<Postgres>) -> Option<&AggValue> {
    match value {
        ReplayValue::Folded(fold) => Some(fold),
        ReplayValue::Scalar(_) => None,
    }
}

const fn value_of(value: &ReplayValue<Postgres>) -> Option<&Value<Postgres>> {
    match value {
        ReplayValue::Folded(_) => None,
        ReplayValue::Scalar(value) => Some(value),
    }
}

/// A grouped result keyed by its first group cell's text.
fn grouped_of(answer: ReplayAnswer<Postgres>) -> BTreeMap<String, ReplayValue<Postgres>> {
    match answer {
        ReplayAnswer::Grouped(groups) => {
            let mut by_group = BTreeMap::new();
            for (identity, value) in groups {
                let status = match identity.values.first().expect("one group column") {
                    Value::String(status) => status.clone(),
                    other => panic!("the group column is text, got {other:?}"),
                };
                by_group.insert(status, value);
            }
            by_group
        }
        other => panic!("a grouped query answers groups, got {other:?}"),
    }
}

/// One comparable proof cell.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum CellKey {
    Null,
    Int(i64),
    Text(String),
}

fn cell_key(cell: &Value<Postgres>) -> CellKey {
    match cell {
        Value::Null => CellKey::Null,
        Value::Int(value) => CellKey::Int(*value),
        Value::String(text) => CellKey::Text(text.clone()),
        other => panic!("the proof rows carry ints and text, got {other:?}"),
    }
}

/// The database's own answers to the fold and extreme families.
fn sql_folds(pg: &mut PgConnection) -> (i64, i64, i64, i32, i32) {
    let (rows, present, total, low, high) = items::table
        .select((
            count_star(),
            count(items::qty),
            sum(items::cost),
            min(items::cost),
            max(items::cost),
        ))
        .first::<(i64, i64, Option<i64>, Option<i32>, Option<i32>)>(pg)
        .expect("the aggregate read runs");
    (
        rows,
        present,
        total.expect("the fixture has non-null costs"),
        low.expect("the fixture has non-null costs"),
        high.expect("the fixture has non-null costs"),
    )
}

/// The grouped database answer, per status the count, the sum and the max.
fn sql_groups(pg: &mut PgConnection) -> BTreeMap<String, (i64, i64, i64, i32, i32)> {
    items::table
        .group_by(items::status)
        .select((
            items::status,
            count_star(),
            count(items::qty),
            sum(items::cost),
            min(items::cost),
            max(items::cost),
        ))
        .load::<(String, i64, i64, Option<i64>, Option<i32>, Option<i32>)>(pg)
        .expect("the grouped read runs")
        .into_iter()
        .map(|(status, rows, present, total, low, high)| {
            (
                status,
                (
                    rows,
                    present,
                    total.expect("the fixture has non-null costs"),
                    low.expect("the fixture has non-null costs"),
                    high.expect("the fixture has non-null costs"),
                ),
            )
        })
        .collect()
}

/// The complete items table as a bag with multiplicity.
fn sql_items(pg: &mut PgConnection) -> BTreeMap<Vec<CellKey>, u64> {
    let rows: Vec<(i32, i32, Option<i32>, String)> = items::table
        .select((items::id, items::cost, items::qty, items::status))
        .order(items::id.asc())
        .load(pg)
        .expect("the items read runs");
    let mut bag = BTreeMap::new();
    for (id, cost, qty, status) in rows {
        let key = vec![
            cell_key(&Value::Int(i64::from(id))),
            cell_key(&Value::Int(i64::from(cost))),
            cell_key(&qty.map_or(Value::Null, |qty| Value::Int(i64::from(qty)))),
            cell_key(&Value::String(status)),
        ];
        *bag.entry(key).or_insert(0) += 1;
    }
    bag
}

/// The row bag a row query answered, with multiplicity.
fn answer_bag(answer: ReplayAnswer<Postgres>) -> BTreeMap<Vec<CellKey>, u64> {
    match answer {
        ReplayAnswer::Rows(rows) => {
            let mut bag = BTreeMap::new();
            for (cells, multiplicity) in rows {
                let key = cells.iter().map(cell_key).collect();
                *bag.entry(key).or_insert(0) += multiplicity;
            }
            bag
        }
        other => panic!("a row query answers rows, got {other:?}"),
    }
}

const MEMBER_TERM: &str = "SELECT * FROM docs WHERE project_id IN \
     (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";
const CALLER: &str = "alice";

/// The docs the caller's memberships admit, straight from the database.
fn sql_member_docs(pg: &mut PgConnection, caller: &str) -> BTreeMap<Vec<CellKey>, u64> {
    let rows: Vec<(i32, i32, String)> = docs::table
        .select((docs::id, docs::project_id, docs::title))
        .filter(
            docs::project_id.eq_any(
                project_members::table
                    .select(project_members::project_id)
                    .filter(project_members::user_id.eq(caller)),
            ),
        )
        .order(docs::id.asc())
        .load(pg)
        .expect("the member read runs");
    let mut bag = BTreeMap::new();
    for (id, project_id, title) in rows {
        let key = vec![
            cell_key(&Value::Int(i64::from(id))),
            cell_key(&Value::Int(i64::from(project_id))),
            cell_key(&Value::String(title)),
        ];
        bag.insert(key, 1);
    }
    bag
}

/// Ordinary extreme queries enter the read tiers in the live engine and removing the current
/// extreme emits a read trigger, which is why the durable wrapper keeps them as retained
/// evidence instead.
#[test]
fn live_extremes_enter_read_tiers_and_emit_read_triggers() {
    // The ordinary scalar MIN query.
    let mut engine =
        SubscriptionEngine::<LiveEvent, DefaultIds, _>::new(catalog(), PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(1, "SELECT MIN(cost) FROM items"))
        .expect("a scalar extreme registers");
    assert!(
        matches!(registered.tier, Tier::Scalar { .. }),
        "the ordinary MIN query enters the read-backed tier"
    );
    Install::install(
        &mut engine,
        registered.subscription_id,
        ScalarInstall {
            value: Value::Int(10),
            checkpoint: Some(PgLsn(5)),
            fence: None,
        },
    )
    .expect("the current minimum installs");
    let removed = TestEvent::delete(
        table_id("items"),
        vec![
            Value::Int(1),
            Value::Int(10),
            Value::Int(2),
            Value::String("a".into()),
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&removed).expect("the removal dispatches");
    assert_eq!(
        output.triggers().len(),
        1,
        "removing the current extreme emits a read trigger"
    );
    assert!(
        matches!(output.triggers()[0].read, ReExecutionRead::Subscription),
        "the live path must re-read the source, which cannot recover an intermediate extreme after a compensation"
    );

    // The ordinary grouped MAX query.
    let mut engine =
        SubscriptionEngine::<LiveEvent, DefaultIds, _>::new(catalog(), PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            2,
            "SELECT status, MAX(cost) FROM items GROUP BY status",
        ))
        .expect("a grouped extreme registers");
    assert!(
        matches!(registered.tier, Tier::GroupedScalar { .. }),
        "the ordinary grouped MAX query enters the read-backed tier"
    );
    Install::install(
        &mut engine,
        registered.subscription_id,
        GroupedScalarSeedInstall {
            rows: vec![
                vec![Value::String("a".into()), Value::Int(10), Value::Int(2)],
                vec![Value::String("b".into()), Value::Int(30), Value::Int(1)],
            ],
            fence: Some(PgLsn(5)),
        },
    )
    .expect("the grouped seed installs");
    let removed = TestEvent::delete(
        table_id("items"),
        vec![
            Value::Int(1),
            Value::Int(10),
            Value::Int(2),
            Value::String("a".into()),
        ],
    )
    .with_pk_columns([0u16])
    .with_checkpoint(PgLsn(10));
    let output = engine.dispatch(&removed).expect("the removal dispatches");
    assert_eq!(
        output.triggers().len(),
        1,
        "only the displaced group re-reads"
    );
    let ReExecutionRead::GroupedScalar { query, .. } = &output.triggers()[0].read else {
        panic!("the displaced group answers with its scoped read");
    };
    assert_eq!(
        query.binds(),
        &[Value::String("a".into())],
        "group a's scoped read triggers"
    );
}

const SEED_ITEMS: [(i32, i32, Option<i32>, &str); 5] = [
    (1, 10, Some(2), "a"),
    (2, 20, None, "a"),
    (3, 30, Some(1), "b"),
    (4, 40, None, "b"),
    (5, 50, Some(3), "c"),
];

fn assert_family_answers(engine: &Engine, pg: &mut PgConnection, ids: [u64; 6]) {
    let [rows, count_id, present_id, total_id, min_id, max_id] = ids;
    assert_eq!(answer_bag(answer(engine, rows)), sql_items(pg));
    let (count, present, total, min, max) = sql_folds(pg);
    assert_eq!(
        folded_of(answer(engine, count_id)),
        AggValue::CountStar(count)
    );
    assert_eq!(
        folded_of(answer(engine, present_id)),
        AggValue::CountColumn(present)
    );
    assert_eq!(
        folded_of(answer(engine, total_id)),
        AggValue::Sum(Some(NumericValue::Integer(total)))
    );
    assert_eq!(
        scalar_of(answer(engine, min_id)),
        Value::Int(i64::from(min))
    );
    assert_eq!(
        scalar_of(answer(engine, max_id)),
        Value::Int(i64::from(max))
    );
}

fn assert_group_answers(
    engine: &Engine,
    expected: &BTreeMap<String, (i64, i64, i64, i32, i32)>,
    ids: [u64; 5],
) {
    let [count_id, present_id, total_id, min_id, max_id] = ids;
    let count = grouped_of(answer(engine, count_id));
    let present = grouped_of(answer(engine, present_id));
    let total = grouped_of(answer(engine, total_id));
    let min = grouped_of(answer(engine, min_id));
    let max = grouped_of(answer(engine, max_id));
    for (status, (rows, contributing, sum, low, high)) in expected {
        assert_eq!(
            fold_of(count.get(status).expect("the group answers")),
            Some(&AggValue::CountStar(*rows))
        );
        assert_eq!(
            fold_of(present.get(status).expect("the group answers")),
            Some(&AggValue::CountColumn(*contributing))
        );
        assert_eq!(
            fold_of(total.get(status).expect("the group answers")),
            Some(&AggValue::Sum(Some(NumericValue::Integer(*sum))))
        );
        assert_eq!(
            value_of(min.get(status).expect("the group answers")),
            Some(&Value::Int(i64::from(*low)))
        );
        assert_eq!(
            value_of(max.get(status).expect("the group answers")),
            Some(&Value::Int(i64::from(*high)))
        );
    }
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn families_emit_exact_evidence_and_survive_checkpoint_restore() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("families");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);

    let instances: &[(u64, &str)] = &[
        (1, "SELECT * FROM items"),
        (2, "SELECT COUNT(*) FROM items"),
        (3, "SELECT COUNT(qty) FROM items"),
        (4, "SELECT SUM(cost) FROM items"),
        (5, "SELECT MIN(cost) FROM items"),
        (6, "SELECT MAX(cost) FROM items"),
    ];
    for (instance, sql) in instances {
        admit(
            &mut engine,
            &mut store,
            1,
            *instance,
            sql,
            vec![items_table(bootstrap_rows.clone())],
            QUOTA,
        );
    }

    // The bootstrap answers match the real table.
    assert_family_answers(&engine, &mut pg, [1, 2, 3, 4, 5, 6]);
    assert!(
        engine.unread(1).is_none(),
        "the bootstrap emits no evidence"
    );

    // C1 a new row moves the rows, count, sum and max answers
    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Option::<i32>::None),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    assert_eq!(commits.len(), 1);
    let (at1, c1) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(1, at1), (2, at1), (4, at1), (6, at1)]);
    assert_family_answers(&engine, &mut pg, [1, 2, 3, 4, 5, 6]);
    assert_eq!(engine.unread(6), Some(&at1));

    // A nullable cell changes the projected row and its nullable count.
    diesel::update(items::table.find(2))
        .set(items::qty.eq(Some(7)))
        .execute(&mut pg)
        .expect("one committed update");
    let mut commits = drain_commits(&mut pg, &slot);
    assert_eq!(commits.len(), 1);
    let (at2, c2) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at2,
        &c2,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(1, at2), (3, at2)]);
    assert_eq!(folded_of(answer(&engine, 3)), AggValue::CountColumn(4));

    // C3 a non-extreme nullable row leaves, everything that projects it moves
    diesel::delete(items::table.find(5))
        .execute(&mut pg)
        .expect("one committed delete");
    let mut commits = drain_commits(&mut pg, &slot);
    assert_eq!(commits.len(), 1);
    let (at3, c3) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at3,
        &c3,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(1, at3), (2, at3), (3, at3), (4, at3)]);

    // Checkpoint and stop, the store holds the complete published state
    let (revision, _boundary, stored_checkpoint) = store.state(1);
    assert_eq!(
        engine.checkpoint().expect("the checkpoint encodes"),
        stored_checkpoint,
        "the store holds the complete published state"
    );
    assert_eq!(
        i64::try_from(engine.revision()).expect("the revision fits"),
        revision
    );
    drop(engine);

    // The source keeps committing while the engine is stopped
    // C4 one commit whose insert and delete cancel each other
    pg.transaction::<(), diesel::result::Error, _>(|tx| {
        diesel::insert_into(items::table)
            .values((
                items::id.eq(7),
                items::cost.eq(15),
                items::qty.eq(Some(1)),
                items::status.eq("b"),
            ))
            .execute(tx)?;
        diesel::delete(items::table.find(7)).execute(tx)?;
        Ok(())
    })
    .expect("one cancelling commit");
    // C5 the minimum doubles
    diesel::update(items::table.find(1))
        .set(items::cost.eq(100))
        .execute(&mut pg)
        .expect("one committed update");
    // C6 a row that C7 compensates
    diesel::insert_into(items::table)
        .values((
            items::id.eq(8),
            items::cost.eq(25),
            items::qty.eq(Some(2)),
            items::status.eq("c"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    // C7 the compensation
    diesel::delete(items::table.find(8))
        .execute(&mut pg)
        .expect("one committed delete");
    let commits = drain_commits(&mut pg, &slot);
    let [c4, c5, c6, c7]: [(C, Vec<Event>); 4] = commits.try_into().expect("four commits");

    // Restore and replay the commits the stopped engine never saw
    let mut restored = restore(&mut store, 1, RestoreAuth::Ok);
    assert_eq!(
        restored.revision(),
        u64::try_from(revision).expect("the revision fits")
    );
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        c4.0,
        &c4.1,
        FailurePoint::None,
    );
    assert!(
        data.changed.is_empty(),
        "a cancelling commit emits no evidence"
    );
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        c5.0,
        &c5.1,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(1, c5.0), (4, c5.0), (5, c5.0), (6, c5.0)]
    );
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        c6.0,
        &c6.1,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(1, c6.0), (2, c6.0), (3, c6.0), (4, c6.0)]
    );
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        c7.0,
        &c7.1,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(1, c7.0), (2, c7.0), (3, c7.0), (4, c7.0)]
    );
    for instance in [1, 2, 3, 4] {
        assert_eq!(restored.unread(instance), Some(&c7.0));
    }
    assert_eq!(restored.unread(5), Some(&c5.0));
    assert_eq!(restored.unread(6), Some(&c5.0));

    // The recovered answers match the real table
    assert_family_answers(&restored, &mut pg, [1, 2, 3, 4, 5, 6]);
    assert_eq!(restored.checkpoint_position(), &c7.0);
    let (revision2, boundary, _) = store.state(1);
    assert_eq!(
        revision2,
        i64::try_from(restored.revision()).expect("the revision fits")
    );
    assert_eq!(boundary, c7.0.to_opaque().0);
}

const SEED_GROUPED: [(i32, i32, Option<i32>, &str); 5] = [
    (1, 10, Some(2), "a"),
    (2, 10, None, "a"),
    (3, 20, Some(1), "b"),
    (4, 30, None, "b"),
    (5, 40, Some(3), "c"),
];

#[test]
#[ignore = "requires Docker and --ignored"]
fn grouped_folds_having_and_grouped_extrema_survive_checkpoint_restore() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("grouped");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_GROUPED);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    for (instance, sql) in [
        (11, "SELECT status, COUNT(*) FROM items GROUP BY status"),
        (12, "SELECT status, SUM(cost) FROM items GROUP BY status"),
        (
            13,
            "SELECT status, COUNT(*) FROM items GROUP BY status HAVING COUNT(*) > 1",
        ),
        (14, "SELECT status, MAX(cost) FROM items GROUP BY status"),
        (15, "SELECT MIN(cost) FROM items"),
        (16, "SELECT status, MIN(cost) FROM items GROUP BY status"),
        (17, "SELECT status, COUNT(qty) FROM items GROUP BY status"),
    ] {
        admit(
            &mut engine,
            &mut store,
            1,
            instance,
            sql,
            vec![items_table(bootstrap_rows.clone())],
            QUOTA,
        );
    }

    // The bootstrap answers match the real table, group a's minimum is tied
    let groups = sql_groups(&mut pg);
    assert_eq!(groups.len(), 3);
    assert_group_answers(&engine, &groups, [11, 17, 12, 16, 14]);
    assert!(
        !grouped_of(answer(&engine, 13)).contains_key("c"),
        "the singleton group does not pass the HAVING"
    );
    assert_eq!(scalar_of(answer(&engine, 15)), Value::Int(10));

    // C1 one of the tied minima leaves, the minimum holds and group a exits the HAVING
    diesel::delete(items::table.find(1))
        .execute(&mut pg)
        .expect("one committed delete");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(11, at1), (12, at1), (13, at1), (17, at1)]
    );
    assert_eq!(
        scalar_of(answer(&engine, 15)),
        Value::Int(10),
        "the tied minimum survives one of its rows leaving"
    );

    // C2 the last tied minimum leaves, the replacement comes from the retained candidates
    diesel::delete(items::table.find(2))
        .execute(&mut pg)
        .expect("one committed delete");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at2, c2) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at2,
        &c2,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![
            (11, at2),
            (12, at2),
            (14, at2),
            (15, at2),
            (16, at2),
            (17, at2)
        ],
        "group a is already out of the HAVING"
    );
    assert!(
        !grouped_of(answer(&engine, 11)).contains_key("a"),
        "the emptied group is absent from the grouped answers"
    );
    assert_eq!(
        scalar_of(answer(&engine, 15)),
        Value::Int(20),
        "the displaced minimum is replaced from the retained candidates"
    );

    // C3 group b's second row leaves, no group passes the HAVING
    diesel::delete(items::table.find(4))
        .execute(&mut pg)
        .expect("one committed delete");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at3, c3) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at3,
        &c3,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(11, at3), (12, at3), (13, at3), (14, at3)]
    );
    assert!(
        grouped_of(answer(&engine, 13)).is_empty(),
        "no group passes the HAVING"
    );

    for instance in 11..=17 {
        acknowledge(&mut engine, &mut store, 1, instance, at3, RestoreAuth::Ok)
            .expect("the checkpoint starts caught up");
    }
    // Checkpoint and stop
    let (revision, _boundary, stored_checkpoint) = store.state(1);
    assert_eq!(
        engine.checkpoint().expect("the checkpoint encodes"),
        stored_checkpoint,
        "the store holds the complete published state"
    );
    drop(engine);

    for (id, cost) in [(6, 5), (7, 60)] {
        diesel::insert_into(items::table)
            .values((
                items::id.eq(id),
                items::cost.eq(cost),
                items::qty.eq(Some(1)),
                items::status.eq("b"),
            ))
            .execute(&mut pg)
            .expect("a committed insertion");
        diesel::delete(items::table.find(id))
            .execute(&mut pg)
            .expect("a committed compensation");
    }
    let [low_insert, low_delete, high_insert, high_delete]: [(C, Vec<Event>); 4] =
        drain_commits(&mut pg, &slot)
            .try_into()
            .expect("four retained commits");
    let low_through = low_delete.0;
    let high_through = high_delete.0;
    let mut restored = restore(&mut store, 1, RestoreAuth::Ok);
    assert_eq!(
        restored.revision(),
        u64::try_from(revision).expect("the revision fits")
    );
    for (affected, (at, events)) in [
        (&[11, 12, 13, 15, 16, 17][..], low_insert),
        (&[11, 12, 13, 15, 16, 17][..], low_delete),
        (&[11, 12, 13, 14, 17][..], high_insert),
        (&[11, 12, 13, 14, 17][..], high_delete),
    ] {
        let data = advance_one(
            &mut restored,
            &mut store,
            1,
            SOURCE,
            at,
            &events,
            FailurePoint::None,
        );
        let expected = affected.iter().map(|&id| (id, at)).collect::<Vec<_>>();
        assert_eq!(data.changed, expected);
    }
    let groups = sql_groups(&mut pg);
    assert_group_answers(&restored, &groups, [11, 17, 12, 16, 14]);
    assert!(grouped_of(answer(&restored, 13)).is_empty());
    assert_eq!(scalar_of(answer(&restored, 15)), Value::Int(20));
    assert_eq!(restored.unread(14), Some(&high_through));
    assert_eq!(restored.unread(16), Some(&low_through));
    assert_eq!(restored.unread(17), Some(&high_through));
    assert_eq!(restored.checkpoint_position(), &high_through);
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn durable_extreme_replay_ignores_diverged_source() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("extreme");
    create_schema(&mut pg);
    seed(
        &mut pg,
        &[
            (1, 5, Some(1), "a"),
            (2, 10, Some(2), "b"),
            (3, 20, Some(3), "c"),
        ],
    );
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);
    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    for (instance, sql) in [
        (20, "SELECT MAX(cost) FROM items"),
        (21, "SELECT MIN(cost) FROM items"),
    ] {
        admit(
            &mut engine,
            &mut store,
            1,
            instance,
            sql,
            vec![items_table(bootstrap_rows.clone())],
            QUOTA,
        );
    }
    drop(engine);
    pg.transaction::<(), diesel::result::Error, _>(|tx| {
        diesel::delete(items::table.find(3)).execute(tx)?;
        diesel::insert_into(items::table)
            .values((
                items::id.eq(4),
                items::cost.eq(4),
                items::qty.eq(Some(1)),
                items::status.eq("d"),
            ))
            .execute(tx)?;
        Ok(())
    })
    .expect("the first committed transition");
    pg.transaction::<(), diesel::result::Error, _>(|tx| {
        diesel::insert_into(items::table)
            .values((
                items::id.eq(3),
                items::cost.eq(20),
                items::qty.eq(Some(3)),
                items::status.eq("c"),
            ))
            .execute(tx)?;
        diesel::delete(items::table.find(4)).execute(tx)?;
        Ok(())
    })
    .expect("the committed compensation");
    let [first, second]: [(C, Vec<Event>); 2] = drain_commits(&mut pg, &slot)
        .try_into()
        .expect("two complete retained commits");
    diesel::update(items::table)
        .set(items::cost.eq(99_999_999))
        .execute(&mut pg)
        .expect("a later source commit");
    let mut restored = restore(&mut store, 1, RestoreAuth::Ok);
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        first.0,
        &first.1,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(20, first.0), (21, first.0)]);
    assert_eq!(scalar_of(answer(&restored, 20)), Value::Int(10));
    assert_eq!(scalar_of(answer(&restored, 21)), Value::Int(4));
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        second.0,
        &second.1,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(20, second.0), (21, second.0)]);
    assert_eq!(scalar_of(answer(&restored, 20)), Value::Int(20));
    assert_eq!(scalar_of(answer(&restored, 21)), Value::Int(5));
    let source_max = items::table
        .select(max(items::cost))
        .first::<Option<i32>>(&mut pg)
        .expect("the source maximum reads");
    assert_eq!(source_max, Some(99_999_999));
    assert_eq!(restored.unread(20), Some(&second.0));
    assert_eq!(restored.unread(21), Some(&second.0));
}

#[cfg(feature = "membership-term")]
#[test]
#[ignore = "requires Docker and --ignored"]
fn membership_movement_replays_retained_inputs() {
    #[derive(diesel::QueryableByName)]
    struct Grant {
        #[diesel(sql_type = diesel::sql_types::Text)]
        user_id: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        project_id: i32,
    }
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("membership");
    create_schema(&mut pg);
    diesel::insert_into(projects::table)
        .values([
            (projects::id.eq(1), projects::name.eq("first")),
            (projects::id.eq(2), projects::name.eq("second")),
        ])
        .execute(&mut pg)
        .expect("the projects seed");
    diesel::insert_into(project_members::table)
        .values([
            (
                project_members::project_id.eq(1),
                project_members::user_id.eq(CALLER),
            ),
            (
                project_members::project_id.eq(2),
                project_members::user_id.eq("bob"),
            ),
        ])
        .execute(&mut pg)
        .expect("the members seed");
    diesel::insert_into(docs::table)
        .values([
            (
                docs::id.eq(10),
                docs::project_id.eq(1),
                docs::title.eq("spec"),
            ),
            (
                docs::id.eq(20),
                docs::project_id.eq(2),
                docs::title.eq("draft"),
            ),
        ])
        .execute(&mut pg)
        .expect("the docs seed");
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);

    let compiler = membership_compiler();
    // Describe the term and run its seed read as the caller on the real database
    let mut request =
        SubscriptionRequest::new(50, MEMBER_TERM).subscriber(Value::String(CALLER.into()));
    let mut terms = compiler
        .describe_terms(&request)
        .expect("the terms describe");
    let TermDescription::Membership(description) = terms.remove(0) else {
        panic!("the filter carries one membership term");
    };
    // Session binding, which the query DSL does not express.
    sql_query(format!("SET app.user_id = '{CALLER}'"))
        .execute(&mut pg)
        .expect("the caller binds");
    // Engine-emitted seed read, which no typed schema can express.
    let grants: Vec<Grant> = sql_query(&description.seed_sql)
        .load(&mut pg)
        .expect("the seed read runs");
    assert_eq!(grants.len(), 1, "the caller holds one membership");
    let columns: Vec<String> = description
        .pairs
        .iter()
        .map(|pair| pair.column.clone())
        .collect();
    request = request.term_values(
        columns,
        grants
            .iter()
            .map(|grant| {
                (
                    Value::String(grant.user_id.clone()),
                    vec![Value::Int(i64::from(grant.project_id))],
                )
            })
            .collect(),
    );

    let mut engine =
        DurableEngine::new(compiler, context(), bootstrap).expect("the durable engine");
    admit_request(
        &mut engine,
        &mut store,
        1,
        50,
        request,
        vec![
            docs_table(vec![
                vec![Value::Int(10), Value::Int(1), Value::String("spec".into())],
                vec![Value::Int(20), Value::Int(2), Value::String("draft".into())],
            ]),
            members_table(vec![
                vec![Value::Int(1), Value::String(CALLER.into())],
                vec![Value::Int(2), Value::String("bob".into())],
            ]),
        ],
        QUOTA,
    );

    // The bootstrap answer is only the docs the caller already reaches
    assert_eq!(
        answer_bag(answer(&engine, 50)),
        sql_member_docs(&mut pg, CALLER)
    );

    // C1 a doc the caller does not reach emits no evidence
    diesel::insert_into(docs::table)
        .values((
            docs::id.eq(30),
            docs::project_id.eq(2),
            docs::title.eq("late"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert!(
        data.changed.is_empty(),
        "a row outside the caller's reach emits no evidence"
    );
    assert_eq!(engine.unread(50), None);

    // C2 a membership movement re-evaluates the retained docs
    diesel::insert_into(project_members::table)
        .values((
            project_members::project_id.eq(2),
            project_members::user_id.eq(CALLER),
        ))
        .execute(&mut pg)
        .expect("one committed membership insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at2, c2) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at2,
        &c2,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(50, at2)],
        "the movement re-evaluates the retained docs"
    );
    assert_eq!(
        answer_bag(answer(&engine, 50)),
        sql_member_docs(&mut pg, CALLER),
        "the retained draft doc becomes visible without a source read"
    );
    assert_eq!(engine.unread(50), Some(&at2));

    // C3 the caller loses the other project
    diesel::delete(
        project_members::table
            .filter(project_members::project_id.eq(1))
            .filter(project_members::user_id.eq(CALLER)),
    )
    .execute(&mut pg)
    .expect("one committed membership delete");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at3, c3) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at3,
        &c3,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(50, at3)]);
    assert_eq!(
        answer_bag(answer(&engine, 50)),
        sql_member_docs(&mut pg, CALLER)
    );

    acknowledge(&mut engine, &mut store, 1, 50, at3, RestoreAuth::Ok)
        .expect("the checkpoint starts caught up");
    // Checkpoint and stop
    let (revision, _boundary, stored_checkpoint) = store.state(1);
    assert_eq!(
        engine.checkpoint().expect("the checkpoint encodes"),
        stored_checkpoint,
        "the store holds the complete published state"
    );
    drop(engine);

    diesel::delete(
        project_members::table
            .filter(project_members::project_id.eq(2))
            .filter(project_members::user_id.eq(CALLER)),
    )
    .execute(&mut pg)
    .expect("the committed revocation");
    diesel::insert_into(project_members::table)
        .values((
            project_members::project_id.eq(2),
            project_members::user_id.eq(CALLER),
        ))
        .execute(&mut pg)
        .expect("the committed compensation");
    let [revoked, granted]: [(C, Vec<Event>); 2] = drain_commits(&mut pg, &slot)
        .try_into()
        .expect("two membership commits");

    let mut restored = restore_with_compiler(&mut store, 1, RestoreAuth::Ok, membership_compiler());
    assert_eq!(
        restored.revision(),
        u64::try_from(revision).expect("the revision fits")
    );
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        revoked.0,
        &revoked.1,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(50, revoked.0)]);
    assert!(answer_bag(answer(&restored, 50)).is_empty());
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        granted.0,
        &granted.1,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(50, granted.0)]);
    assert_eq!(
        answer_bag(answer(&restored, 50)),
        sql_member_docs(&mut pg, CALLER)
    );
    assert_eq!(restored.unread(50), Some(&granted.0));
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn irrelevant_rows_and_projected_cancellation_emit_no_evidence() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("irrelevant");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        31,
        "SELECT SUM(cost) FROM items",
        vec![items_table(bootstrap_rows.clone())],
        QUOTA,
    );
    admit(
        &mut engine,
        &mut store,
        1,
        32,
        "SELECT * FROM items",
        vec![items_table(bootstrap_rows)],
        QUOTA,
    );

    // C1 a row on a table no instance captures
    diesel::insert_into(notes::table)
        .values((notes::id.eq(1), notes::body.eq("unrelated")))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert!(
        data.changed.is_empty(),
        "the irrelevant table emits no evidence"
    );
    assert_eq!(engine.checkpoint_position(), &at1);

    // C2 a column no fold projects, the row family still moves
    diesel::update(items::table.find(3))
        .set(items::qty.eq(99))
        .execute(&mut pg)
        .expect("one committed update");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at2, c2) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at2,
        &c2,
        FailurePoint::None,
    );
    assert_eq!(
        data.changed,
        vec![(32, at2)],
        "the fold ignores a column it does not project"
    );
    assert_eq!(engine.unread(31), None);
    assert_eq!(engine.unread(32), Some(&at2));

    // C3 one commit whose insert and delete cancel each other
    pg.transaction::<(), diesel::result::Error, _>(|tx| {
        diesel::insert_into(items::table)
            .values((
                items::id.eq(6),
                items::cost.eq(60),
                items::qty.eq(Some(1)),
                items::status.eq("a"),
            ))
            .execute(tx)?;
        diesel::delete(items::table.find(6)).execute(tx)?;
        Ok(())
    })
    .expect("one cancelling commit");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at3, c3) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at3,
        &c3,
        FailurePoint::None,
    );
    assert!(
        data.changed.is_empty(),
        "the projected cancellation emits no evidence"
    );
    assert_eq!(engine.checkpoint_position(), &at3);

    // The answers match the real table and the fold never earned evidence
    let (_count, _count_qty, total, _min, _max) = sql_folds(&mut pg);
    assert_eq!(
        folded_of(answer(&engine, 31)),
        AggValue::Sum(Some(NumericValue::Integer(total)))
    );
    assert_eq!(answer_bag(answer(&engine, 32)), sql_items(&mut pg));
    assert_eq!(engine.unread(31), None);
    assert_eq!(engine.unread(32), Some(&at2));
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn duplicate_delivery_and_history_gaps_cannot_advance_progress() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("gaps");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        41,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows)],
        QUOTA,
    );

    // C1 publishes
    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );

    // A duplicate delivery of the same commit is a no-op
    assert!(engine
        .prepare_transaction(SOURCE, &at1, at1, &c1)
        .expect("a duplicate prepares")
        .is_none());

    // C2 and C3 land without C2 being published
    diesel::insert_into(items::table)
        .values((
            items::id.eq(7),
            items::cost.eq(70),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    diesel::insert_into(items::table)
        .values((
            items::id.eq(8),
            items::cost.eq(80),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let commits = drain_commits(&mut pg, &slot);
    let [p2, p3]: [(C, Vec<Event>); 2] = commits.try_into().expect("two commits");
    let (at2, c2) = (p2.0, p2.1);
    let (at3, c3) = (p3.0, p3.1);

    // A mismatched preceding boundary is a history gap
    assert!(matches!(
        engine.prepare_transaction(SOURCE, &at2, at3, &c3),
        Err(DurableError::HistoryGap)
    ));
    // A mismatched source identity
    assert!(matches!(
        engine.prepare_transaction(b"other-source", &at1, at2, &c2),
        Err(DurableError::Context)
    ));
    // Events that do not belong to the declared commit
    assert!(matches!(
        engine.prepare_transaction(SOURCE, &at1, at2, &c1),
        Err(DurableError::EventBoundary)
    ));
    let revision = engine.revision();
    assert_eq!(engine.checkpoint_position(), &at1);

    // Then the real history continues
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at2,
        &c2,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(41, at2)]);
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at3,
        &c3,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(41, at3)]);
    assert_eq!(engine.revision(), revision + 2);
    let (count, _count_qty, _total, _min, _max) = sql_folds(&mut pg);
    assert_eq!(folded_of(answer(&engine, 41)), AggValue::CountStar(count));
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn persistence_failures_keep_store_and_engine_unchanged() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("persist");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        51,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows)],
        QUOTA,
    );

    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    let revision = engine.revision();
    let boundary = *engine.checkpoint_position();
    let fits = i64::try_from(revision).expect("the revision fits");

    // (a) A callback that fails before it writes anything
    let prepared = {
        let after = *engine.checkpoint_position();
        engine.prepare_transaction(SOURCE, &after, at1, &c1)
    }
    .expect("prepares")
    .expect("fresh");
    assert!(prepared.persist(|_| Err(StoreError::Refused)).is_err());
    assert_eq!(engine.revision(), revision);
    assert_eq!(engine.checkpoint_position(), &boundary);
    assert_eq!(store.revision(1), fits);

    // (b) A dropped preparation publishes nothing
    drop(
        {
            let after = *engine.checkpoint_position();
            engine.prepare_transaction(SOURCE, &after, at1, &c1)
        }
        .expect("prepares")
        .expect("fresh"),
    );
    assert_eq!(store.revision(1), fits);
    assert_eq!(engine.revision(), revision);

    // (c) A store transaction that rolls back mid-flight after the compare-and-swap
    let prepared = {
        let after = *engine.checkpoint_position();
        engine.prepare_transaction(SOURCE, &after, at1, &c1)
    }
    .expect("prepares")
    .expect("fresh");
    assert!(prepared
        .persist(|receipt| store.apply(1, receipt, FailurePoint::AfterCas))
        .is_err());
    // The uncommitted compare-and-swap is not visible from another session
    let mut other = store.database.connect();
    let visible: i64 = durable_state::table
        .find(1)
        .select(durable_state::revision)
        .first(&mut other)
        .expect("the revision reads");
    assert_eq!(
        visible, fits,
        "the rollback released the uncommitted compare-and-swap"
    );
    assert_eq!(engine.checkpoint_position(), &boundary);

    // (d) The retry publishes once from the same published state
    let data = advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(51, at1)]);
    assert_eq!(engine.revision(), revision + 1);
    assert_eq!(engine.unread(51), Some(&at1));
    assert_eq!(engine.checkpoint_position(), &at1);
}

#[ignore = "requires Docker and --ignored"]
#[test]
fn cancelled_async_persistence_keeps_store_and_engine_unchanged() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("async");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        91,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows)],
        QUOTA,
    );

    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    let revision = engine.revision();
    let prepared = {
        let after = *engine.checkpoint_position();
        engine.prepare_transaction(SOURCE, &after, at1, &c1)
    }
    .expect("prepares")
    .expect("fresh");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("the cancellation clock starts");
    let cancelled = runtime.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            prepared.persist_async(async |receipt: &StoreReceipt| {
                let _ = receipt;
                core::future::pending::<Result<(), StoreError>>().await
            }),
        )
        .await
        .is_err()
    });
    assert!(cancelled, "the bounded wait expires before persistence");
    assert_eq!(engine.revision(), revision);
    assert_eq!(engine.checkpoint_position(), &bootstrap);
    assert_eq!(
        store.revision(1),
        i64::try_from(revision).expect("the revision fits")
    );

    let prepared = engine
        .prepare_transaction(SOURCE, &bootstrap, at1, &c1)
        .expect("the cancelled commit prepares again")
        .expect("the commit is unpublished");
    runtime
        .block_on(prepared.persist_async(async |receipt: &StoreReceipt| {
            store.apply(1, receipt, FailurePoint::None)
        }))
        .expect("the asynchronous callback commits the SQL transaction");
    assert_eq!(store.evidence(91), Some(at1.to_opaque().0));
    assert_eq!(engine.unread(91), Some(&at1));
    assert_eq!(engine.checkpoint_position(), &at1);
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn quota_admission_and_isolated_overflow_with_failed_failure_persistence() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("quota");
    create_schema(&mut pg);
    seed(&mut pg, &[(1, 10, Some(2), "a"), (2, 20, Some(1), "b")]);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);

    // The probe engine measures the encoded record around the overflow commit
    let mut probe = durable_engine(bootstrap);
    admit(
        &mut probe,
        &mut store,
        1,
        61,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows.clone())],
        QUOTA,
    );
    admit(
        &mut probe,
        &mut store,
        1,
        62,
        "SELECT SUM(cost) FROM items",
        vec![items_table(bootstrap_rows.clone())],
        QUOTA,
    );

    // C0 one row, the victim holds a live unread before the overflow
    diesel::insert_into(items::table)
        .values((
            items::id.eq(99),
            items::cost.eq(30),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at0, c0) = commits.pop().expect("the commit drained");
    advance_one(
        &mut probe,
        &mut store,
        1,
        SOURCE,
        at0,
        &c0,
        FailurePoint::None,
    );
    let before = probe
        .state_bytes(61)
        .expect("the record encodes")
        .expect("the instance exists");

    // C1 the commit that overflows the quota
    let rows = (0..3000)
        .map(|g| {
            (
                items::id.eq(100 + g),
                items::cost.eq(g % 7),
                items::qty.eq(Some(g % 3)),
                items::status.eq("q"),
            )
        })
        .collect::<Vec<_>>();
    diesel::insert_into(items::table)
        .values(&rows)
        .execute(&mut pg)
        .expect("one committed bulk insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    advance_one(
        &mut probe,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    let after = probe
        .state_bytes(61)
        .expect("the record encodes")
        .expect("the instance exists");
    assert!(
        after > before,
        "the overflow commit grows the retained record"
    );
    let quota = before + (after - before) / 2;
    assert!(quota > before && quota < after);

    drop(probe);
    drop(store);
    let mut store = Store::new();
    store.install(2, &bootstrap);
    // The real engine admits the same bootstrap under the measured quota
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        2,
        61,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows.clone())],
        quota,
    );
    admit(
        &mut engine,
        &mut store,
        2,
        62,
        "SELECT SUM(cost) FROM items",
        vec![items_table(bootstrap_rows.clone())],
        QUOTA,
    );
    // A one-byte quota refuses the admission
    assert!(matches!(
        engine.prepare_register(
            63,
            SubscriptionRequest::new(63, "SELECT COUNT(*) FROM items"),
            *engine.checkpoint_position(),
            vec![items_table(bootstrap_rows)],
            1,
            |_| Ok(()),
        ),
        Err(DurableError::Capacity { .. })
    ));

    // C0 succeeds on the real engine
    let data = advance_one(
        &mut engine,
        &mut store,
        2,
        SOURCE,
        at0,
        &c0,
        FailurePoint::None,
    );
    assert_eq!(data.changed, vec![(61, at0), (62, at0)]);
    assert_eq!(engine.unread(61), Some(&at0));

    // C1 overflows the victim and the failure-record write fails, so nothing publishes
    let prepared = {
        let after = *engine.checkpoint_position();
        engine.prepare_transaction(SOURCE, &after, at1, &c1)
    }
    .expect("prepares")
    .expect("fresh");
    assert!(prepared
        .persist(|receipt| {
            assert_eq!(
                receipt.failures.len(),
                1,
                "only the oversized instance fails"
            );
            store.apply(2, receipt, FailurePoint::AfterEvidence)
        })
        .is_err());
    assert_eq!(
        engine.checkpoint_position(),
        &at0,
        "the failed failure-record persistence publishes no progress"
    );
    assert!(engine.failure(61).is_none());
    assert_eq!(
        store.revision(2),
        i64::try_from(engine.revision()).expect("the revision fits")
    );

    // The retry persists the isolated failure with the progress
    let data = advance_one(
        &mut engine,
        &mut store,
        2,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert_eq!(data.failures.len(), 1);
    let failure = &data.failures[0];
    assert_eq!(failure.instance, 61);
    assert_eq!(failure.at, at1);
    assert_eq!(failure.last_valid, at0);
    assert!(
        data.changed.iter().any(|(instance, _)| *instance == 62),
        "the unrelated instance continues"
    );
    let stored_failure = store.failure(61).expect("the failure record lands");
    assert_eq!(
        stored_failure.1,
        at0.to_opaque().0,
        "the last valid boundary persists"
    );
    assert_eq!(
        engine.failure(61).expect("the failure reports").last_valid,
        at0
    );
    assert_eq!(
        folded_of(answer(&engine, 61)),
        AggValue::CountStar(3),
        "the failed instance keeps its last valid answer"
    );
    assert_eq!(engine.last_valid(61), Some(&at0));
    assert_eq!(
        engine.unread(61),
        Some(&at0),
        "the failed instance retains its unread"
    );
    assert_eq!(engine.unread(62), Some(&at1));
    let (_count, _count_qty, total, _min, _max) = sql_folds(&mut pg);
    assert_eq!(
        folded_of(answer(&engine, 62)),
        AggValue::Sum(Some(NumericValue::Integer(total)))
    );
    assert_eq!(engine.checkpoint_position(), &at1);
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn restore_refuses_drifted_catalog_and_unauthorized_adoption() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("restore");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        71,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows)],
        QUOTA,
    );
    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );

    let (revision, _boundary, bytes) = store.state(1);
    let revision = u64::try_from(revision).expect("the revision fits");
    assert_eq!(engine.unread(71), Some(&at1));

    // A drifted catalog refuses the operator's state
    let drifted = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, cost TEXT, qty INTEGER, status TEXT);",
    )
    .expect("the drifted catalog parses");
    assert!(DurableEngine::restore(
        Compiler::new(drifted, PostgreSqlDialect {}),
        context(),
        revision,
        at1,
        &bytes,
        authorize(RestoreAuth::Ok),
    )
    .is_err());
    // A wrong source generation
    let mut wrong_source = context();
    wrong_source.source = b"other-generation".to_vec();
    assert!(DurableEngine::restore(
        compiler(),
        wrong_source,
        revision,
        at1,
        &bytes,
        authorize(RestoreAuth::Ok),
    )
    .is_err());
    // A wrong authority revision
    let mut wrong_authority = context();
    wrong_authority.authority = b"other-policy".to_vec();
    assert!(DurableEngine::restore(
        compiler(),
        wrong_authority,
        revision,
        at1,
        &bytes,
        authorize(RestoreAuth::Ok),
    )
    .is_err());
    // A wrong receipt
    assert!(DurableEngine::restore(
        compiler(),
        context(),
        revision + 1,
        at1,
        &bytes,
        authorize(RestoreAuth::Ok),
    )
    .is_err());
    assert!(DurableEngine::restore(
        compiler(),
        context(),
        revision,
        bootstrap,
        &bytes,
        authorize(RestoreAuth::Ok),
    )
    .is_err());
    // Trailing bytes
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(DurableEngine::restore(
        compiler(),
        context(),
        revision,
        at1,
        &trailing,
        authorize(RestoreAuth::Ok),
    )
    .is_err());
    // A refused or an unavailable authority
    assert!(DurableEngine::restore(
        compiler(),
        context(),
        revision,
        at1,
        &bytes,
        authorize(RestoreAuth::Denied),
    )
    .is_err());
    assert!(DurableEngine::restore(
        compiler(),
        context(),
        revision,
        at1,
        &bytes,
        authorize(RestoreAuth::Unavailable),
    )
    .is_err());
    // The store never moved
    assert_eq!(
        store.revision(1),
        i64::try_from(revision).expect("the revision fits")
    );

    // The matching receipt adopts cleanly and keeps its evidence
    let restored = restore(&mut store, 1, RestoreAuth::Ok);
    assert_eq!(restored.checkpoint_position(), &at1);
    assert_eq!(
        restored.unread(71),
        Some(&at1),
        "the evidence survives the restore"
    );
    let (count, _count_qty, _total, _min, _max) = sql_folds(&mut pg);
    assert_eq!(folded_of(answer(&restored, 71)), AggValue::CountStar(count));
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn acknowledge_and_remove_persist_without_source_movement() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("acknowledge");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let bootstrap_rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);

    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        81,
        "SELECT COUNT(*) FROM items",
        vec![items_table(bootstrap_rows)],
        QUOTA,
    );

    // C1 earns evidence
    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at1, c1) = commits.pop().expect("the commit drained");
    advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at1,
        &c1,
        FailurePoint::None,
    );
    assert_eq!(engine.unread(81), Some(&at1));

    // A future boundary is refused
    let future = C::before_commit(PgLsn(at1.commit_lsn().0 + 1));
    assert!(acknowledge(&mut engine, &mut store, 1, 81, future, RestoreAuth::Ok).is_err());
    assert_eq!(engine.unread(81), Some(&at1));

    // An unauthorized acknowledgement
    assert!(acknowledge(&mut engine, &mut store, 1, 81, at1, RestoreAuth::Denied).is_err());
    assert_eq!(engine.unread(81), Some(&at1));

    // An older boundary persists the engine's final highwater, which is still at1
    let data = acknowledge(&mut engine, &mut store, 1, 81, bootstrap, RestoreAuth::Ok)
        .expect("older catch-up persists");
    assert_eq!(
        data.operation,
        DurableOperation::Acknowledge {
            instance: 81,
            unread: Some(at1),
        }
    );
    assert_eq!(data.previous, at1);
    assert_eq!(data.at, at1, "acknowledgement does not move the source");
    assert_eq!(engine.unread(81), Some(&at1));
    assert_eq!(store.evidence(81), Some(at1.to_opaque().0));

    // A covering boundary clears the evidence
    let data = acknowledge(&mut engine, &mut store, 1, 81, at1, RestoreAuth::Ok)
        .expect("catch-up persists");
    assert_eq!(
        data.operation,
        DurableOperation::Acknowledge {
            instance: 81,
            unread: None,
        }
    );
    assert_eq!(engine.unread(81), None);
    assert_eq!(store.evidence(81), None);

    // C2, then a removal
    diesel::insert_into(items::table)
        .values((
            items::id.eq(7),
            items::cost.eq(70),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at2, c2) = commits.pop().expect("the commit drained");
    advance_one(
        &mut engine,
        &mut store,
        1,
        SOURCE,
        at2,
        &c2,
        FailurePoint::None,
    );
    let data = remove(&mut engine, &mut store, 1, 81).expect("the removal persists");
    assert_eq!(data.operation, DurableOperation::Remove { instance: 81 });
    assert_eq!(engine.answer(81), None);
    assert_eq!(store.evidence(81), None);
    assert_eq!(engine.checkpoint_position(), &at2);

    // The removed instance does not come back and the source continues
    let mut restored = restore(&mut store, 1, RestoreAuth::Ok);
    assert_eq!(restored.answer(81), None);
    assert_eq!(restored.unread(81), None);
    let revision = restored.revision();
    diesel::insert_into(items::table)
        .values((
            items::id.eq(8),
            items::cost.eq(80),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("one committed insert");
    let mut commits = drain_commits(&mut pg, &slot);
    let (at3, c3) = commits.pop().expect("the commit drained");
    let data = advance_one(
        &mut restored,
        &mut store,
        1,
        SOURCE,
        at3,
        &c3,
        FailurePoint::None,
    );
    assert!(data.changed.is_empty(), "no instance is left to change");
    assert_eq!(restored.revision(), revision + 1);
    let (count, _count_qty, _total, _min, _max) = sql_folds(&mut pg);
    assert_eq!(count, 8);
}

#[test]
#[ignore = "requires Docker and --ignored"]
fn revision_conflicts_and_unknown_commit_outcomes_require_reload() {
    common::assert_docker_available();
    let db = common::pg_database();
    let mut pg = db.connect();
    let slot = db.slot("receipt");
    create_schema(&mut pg);
    seed(&mut pg, &SEED_ITEMS);
    let rows = bootstrap_items(&mut pg);
    let bootstrap = wal_boundary(&mut pg);
    common::create_slot(&mut pg, &slot);
    let mut store = Store::new();
    store.install(1, &bootstrap);
    let mut engine = durable_engine(bootstrap);
    admit(
        &mut engine,
        &mut store,
        1,
        101,
        "SELECT COUNT(*) FROM items",
        vec![items_table(rows)],
        QUOTA,
    );
    let mut stale = restore(&mut store, 1, RestoreAuth::Ok);
    diesel::insert_into(items::table)
        .values((
            items::id.eq(6),
            items::cost.eq(60),
            items::qty.eq(Some(1)),
            items::status.eq("a"),
        ))
        .execute(&mut pg)
        .expect("a committed insertion");
    let [commit]: [(C, Vec<Event>); 1] = drain_commits(&mut pg, &slot)
        .try_into()
        .expect("one retained commit");
    let rejected = engine
        .prepare_transaction(SOURCE, &bootstrap, commit.0, &commit.1)
        .expect("prepare")
        .expect("fresh commit")
        .persist(|receipt| {
            store.apply(1, receipt, FailurePoint::None)?;
            Err::<(), _>(StoreError::Refused)
        });
    let DurablePersistError::Callback { error, preparation } =
        rejected.expect_err("the acknowledgement is lost");
    assert!(matches!(error, StoreError::Refused));
    drop(preparation);
    assert_eq!(engine.checkpoint_position(), &bootstrap);
    assert_eq!(store.state(1).1, commit.0.to_opaque().0);
    for unpublished in [&mut engine, &mut stale] {
        let rejected = unpublished
            .prepare_transaction(SOURCE, &bootstrap, commit.0, &commit.1)
            .expect("prepare from the unpublished boundary")
            .expect("fresh in memory")
            .persist(|receipt| store.apply(1, receipt, FailurePoint::None));
        let DurablePersistError::Callback { error, preparation } =
            rejected.expect_err("the stored revision has advanced");
        assert!(matches!(error, StoreError::RevisionMismatch));
        drop(preparation);
        assert_eq!(unpublished.checkpoint_position(), &bootstrap);
        assert_eq!(folded_of(answer(unpublished, 101)), AggValue::CountStar(5));
    }
    let mut recovered = restore(&mut store, 1, RestoreAuth::Ok);
    assert_eq!(folded_of(answer(&recovered, 101)), AggValue::CountStar(6));
    assert_eq!(recovered.unread(101), Some(&commit.0));
    assert!(recovered
        .prepare_transaction(SOURCE, &bootstrap, commit.0, &commit.1)
        .expect("the durable receipt covers the replay")
        .is_none());
    assert_eq!(folded_of(answer(&recovered, 101)), AggValue::CountStar(6));
}
