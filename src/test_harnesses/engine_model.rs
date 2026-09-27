//! The whole engine against a model table, over generated sequences.
//!
//! One byte string draws a sequence of registrations, removals and writes on
//! the table [`predicate_grammar::ddl`] creates. The writes go to an
//! in-memory SQLite database first and then reach an [`AutoResolvingEngine`]
//! on the SQLite backend as change events, and its re-execution reads go to
//! the same database through a [`DieselConnector`]. After every write each
//! subscription's answer, as a subscriber holding its notifications would
//! know it, must equal what SQLite answers for the same statement.
//!
//! A subscription the engine refuses to answer for a write, by an evaluation
//! failure, an unanswered cell or a failed read, is dropped from the
//! comparison from then on, since no answer is not a wrong one.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU64, Ordering};

use arbitrary::Unstructured;
use diesel::{
    Connection as _, ExpressionMethods as _, QueryDsl as _, RunQueryDsl as _, SqliteConnection,
};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::SQLiteDialect;

use super::predicate_grammar::{self, t, Engine, Expr, Row};
use crate::backend::{Backend, SQLite, Value, ValueKind};
use crate::reexec::{
    AutoResolvingEngine, Connector as _, DieselConnector, SnapshotResult, SyncMode,
};
use crate::testing::TestEvent;
use crate::{
    catalog_helpers, AggValue, AggregateBootstrap, AggregateResultValue, AggregateSeedInstall,
    AggregateValueChange, AggregateValueUpdate, DefaultIds, NumericValue, SubscriptionEngine,
    SubscriptionId, SubscriptionRequest, TableId, Tier, TierKind,
};

type Model = AutoResolvingEngine<
    TestEvent<SQLite>,
    DefaultIds,
    ParserDB,
    SyncMode<DieselConnector<SqliteConnection, SQLite>>,
>;

/// Keys a write may touch, few so writes collide.
const KEYS: i32 = 6;

/// An aggregate over the integer column `a`.
#[derive(Clone, Copy, Debug)]
enum Aggregate {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl Aggregate {
    const ALL: [Self; 6] = [
        Self::CountStar,
        Self::Count,
        Self::Sum,
        Self::Avg,
        Self::Min,
        Self::Max,
    ];

    const fn sql(self) -> &'static str {
        match self {
            Self::CountStar => "COUNT(*)",
            Self::Count => "COUNT(a)",
            Self::Sum => "SUM(a)",
            Self::Avg => "AVG(a)",
            Self::Min => "MIN(a)",
            Self::Max => "MAX(a)",
        }
    }
}

/// What a statement projects.
#[derive(Clone, Debug)]
enum Shape {
    /// `SELECT *`.
    Rows,
    /// `SELECT` some columns of `t`, by index into `id, a, b, r, s, f`.
    Columns(Vec<usize>),
    /// One aggregate over the whole answer.
    Aggregate(Aggregate),
    /// `SELECT s, aggregate ... GROUP BY s`.
    Grouped(Aggregate),
}

const COLUMN_NAMES: [&str; 6] = ["id", "a", "b", "r", "s", "f"];

/// One subscription statement.
#[derive(Clone, Debug)]
struct Statement {
    shape: Shape,
    filter: Option<Expr>,
}

impl Statement {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let shape = match u.int_in_range(0u8..=5)? {
            0 | 1 => Shape::Rows,
            2 => {
                let mut columns: Vec<usize> = (0..COLUMN_NAMES.len())
                    .filter(|_| u.ratio(1u8, 2u8).unwrap_or(false))
                    .collect();
                if columns.is_empty() {
                    columns.push(u.choose_index(COLUMN_NAMES.len())?);
                }
                Shape::Columns(columns)
            }
            3 | 4 => Shape::Aggregate(*u.choose(&Aggregate::ALL)?),
            _ => Shape::Grouped(*u.choose(&Aggregate::ALL)?),
        };
        let filter = if u.ratio(1u8, 4u8)? {
            None
        } else {
            Some(Expr::arbitrary(u, 3)?)
        };
        Ok(Self { shape, filter })
    }

    fn where_clause(&self) -> String {
        self.filter.as_ref().map_or_else(String::new, |filter| {
            format!(" WHERE {}", filter.render(Engine::Sqlite))
        })
    }

    fn sql(&self) -> String {
        let projection = match &self.shape {
            Shape::Rows => String::from("*"),
            Shape::Columns(columns) => columns
                .iter()
                .map(|&column| COLUMN_NAMES[column])
                .collect::<Vec<_>>()
                .join(", "),
            Shape::Aggregate(aggregate) => String::from(aggregate.sql()),
            Shape::Grouped(aggregate) => format!("s, {}", aggregate.sql()),
        };
        let group = if matches!(self.shape, Shape::Grouped(_)) {
            " GROUP BY s"
        } else {
            ""
        };
        format!("SELECT {projection} FROM t{}{group}", self.where_clause())
    }
}

/// One step of a sequence.
#[derive(Clone, Debug)]
enum Step {
    Register(Statement),
    Unregister(usize),
    Insert(i32, Row),
    Restart,
    /// From one key to another, the event carrying the old image or, when
    /// `old_image` is false and the key stays, none of it, as a source
    /// logging only new rows does. An aggregate stops on that.
    Update {
        id: i32,
        to: i32,
        row: Row,
        old_image: bool,
    },
    Delete(i32),
    Truncate,
}

impl Step {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let key = |u: &mut Unstructured<'_>| u.int_in_range(1..=KEYS);
        Ok(match u.int_in_range(0u8..=16)? {
            0..=3 => Self::Register(Statement::arbitrary(u)?),
            4 => Self::Unregister(u.arbitrary()?),
            5..=8 => Self::Insert(key(u)?, Row::arbitrary(u)?),
            9..=12 => Self::Update {
                id: key(u)?,
                to: key(u)?,
                row: Row::arbitrary(u)?,
                old_image: !u.ratio(1u8, 8)?,
            },
            13 | 14 => Self::Delete(key(u)?),
            15 => Self::Truncate,
            _ => Self::Restart,
        })
    }
}

/// A cell as both sides are compared, with the boolean read as its integer
/// as SQLite stores it.
#[derive(Clone, Debug)]
enum Cell {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl Cell {
    fn of(value: &Value<SQLite>) -> Self {
        match value {
            Value::Int(n) | Value::Bool(n) => Self::Int(*n),
            Value::Float(x) => Self::Real(*x),
            Value::String(text) => Self::Text(text.clone()),
            _ => Self::Null,
        }
    }

    fn of_numeric(value: Option<&NumericValue>) -> Self {
        match value {
            None => Self::Null,
            Some(NumericValue::Integer(n)) => Self::Int(*n),
            Some(NumericValue::Double(x)) => Self::Real(*x),
            Some(NumericValue::Decimal(d)) => Self::Real(d.to_string().parse().unwrap_or(f64::NAN)),
        }
    }

    fn of_aggregate(value: &AggValue) -> Self {
        match value {
            AggValue::CountStar(n) | AggValue::CountColumn(n) => Self::Int(*n),
            AggValue::Sum(v) | AggValue::Avg(v) => Self::of_numeric(v.as_ref()),
            _ => Self::Null,
        }
    }

    /// Equal as SQL compares them, numbers across integer and real.
    fn same(&self, other: &Self) -> bool {
        #[allow(clippy::cast_precision_loss)]
        let number = |cell: &Self| match cell {
            Self::Int(n) => Some(*n as f64),
            Self::Real(x) => Some(*x),
            _ => None,
        };
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            _ => match (number(self), number(other)) {
                (Some(a), Some(b)) => (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0),
                _ => false,
            },
        }
    }

    /// An order that sorts equal cells together, for comparing multisets.
    fn rank(&self) -> (u8, i64, String) {
        #[allow(clippy::cast_possible_truncation)]
        match self {
            Self::Null => (0, 0, String::new()),
            Self::Int(n) => (1, *n, String::new()),
            Self::Real(x) => (1, (*x * 1e6).round() as i64, String::new()),
            Self::Text(text) => (2, 0, text.clone()),
        }
    }
}

type Answer = Vec<Vec<Cell>>;

/// A row's sort key, which equal rows share.
type RowKey = Vec<(u8, i64, String)>;

fn same_answer(left: &[Vec<Cell>], right: &[Vec<Cell>]) -> bool {
    let sorted = |answer: &[Vec<Cell>]| {
        let mut rows = answer.to_vec();
        rows.sort_by(|x, y| {
            let key = |row: &[Cell]| row.iter().map(Cell::rank).collect::<Vec<_>>();
            key(x).cmp(&key(y))
        });
        rows
    };
    let (left, right) = (sorted(left), sorted(right));
    left.len() == right.len()
        && left
            .iter()
            .zip(&right)
            .all(|(x, y)| x.len() == y.len() && x.iter().zip(y).all(|(a, b)| a.same(b)))
}

/// What a subscriber holds for one subscription.
enum View {
    /// Rows answered in process, kept from the event images.
    Stream(Answer),
    /// Rows a re-read replaced, or a keyed read maintains, by key.
    Keyed(BTreeMap<RowKey, Vec<Cell>>),
    /// Rows a whole re-read replaced.
    Whole(Answer),
    /// Aggregate values by group, the empty group for an ungrouped one.
    Groups(BTreeMap<RowKey, (Vec<Cell>, Cell)>),
}

impl View {
    fn answer(&self) -> Answer {
        match self {
            Self::Stream(rows) | Self::Whole(rows) => rows.clone(),
            Self::Keyed(rows) => rows.values().cloned().collect(),
            Self::Groups(groups) => groups
                .values()
                .map(|(group, value)| {
                    let mut row = group.clone();
                    row.push(value.clone());
                    row
                })
                .collect(),
        }
    }
}

struct Subscription {
    id: SubscriptionId,
    consumer: u64,
    statement: Statement,
    view: View,
    /// The tier the engine last reported, which a restart has to keep.
    tier: TierKind,
}

fn key_of(cells: &[Cell]) -> RowKey {
    cells.iter().map(Cell::rank).collect()
}

fn project(statement: &Statement, row: &[Cell]) -> Vec<Cell> {
    match &statement.shape {
        Shape::Columns(columns) => columns.iter().map(|&column| row[column].clone()).collect(),
        _ => row.to_vec(),
    }
}

type FullRow = (
    i32,
    Option<i64>,
    Option<i64>,
    Option<f64>,
    Option<String>,
    Option<bool>,
);

fn cells_of(row: FullRow) -> Vec<Cell> {
    let (key, first, second, real, text, flag) = row;
    let or_null = |cell: Option<Cell>| cell.unwrap_or(Cell::Null);
    alloc::vec![
        Cell::Int(i64::from(key)),
        or_null(first.map(Cell::Int)),
        or_null(second.map(Cell::Int)),
        or_null(real.map(Cell::Real)),
        or_null(text.map(Cell::Text)),
        or_null(flag.map(|flag| Cell::Int(i64::from(flag)))),
    ]
}

/// What SQLite answers for `statement`, `None` when it refuses.
fn sqlite_answers(connection: &mut SqliteConnection, statement: &Statement) -> Option<Answer> {
    use diesel::dsl::sql;
    use diesel::sql_types::{BigInt, Bool, Double, Nullable};

    let filter = || {
        sql::<Bool>(
            &statement
                .filter
                .as_ref()
                .map_or_else(|| String::from("1"), |filter| filter.render(Engine::Sqlite)),
        )
    };
    match &statement.shape {
        Shape::Rows | Shape::Columns(_) => {
            let rows: Vec<FullRow> = t::table.filter(filter()).load(connection).ok()?;
            Some(
                rows.into_iter()
                    .map(|row| project(statement, &cells_of(row)))
                    .collect(),
            )
        }
        Shape::Aggregate(aggregate) => {
            let value = match aggregate {
                Aggregate::Avg => t::table
                    .filter(filter())
                    .select(sql::<Nullable<Double>>(aggregate.sql()))
                    .get_result::<Option<f64>>(connection)
                    .ok()?
                    .map_or(Cell::Null, Cell::Real),
                _ => t::table
                    .filter(filter())
                    .select(sql::<Nullable<BigInt>>(aggregate.sql()))
                    .get_result::<Option<i64>>(connection)
                    .ok()?
                    .map_or(Cell::Null, Cell::Int),
            };
            Some(alloc::vec![alloc::vec![value]])
        }
        Shape::Grouped(aggregate) => {
            let group = |s: Option<String>| s.map_or(Cell::Null, Cell::Text);
            Some(match aggregate {
                Aggregate::Avg => t::table
                    .filter(filter())
                    .group_by(t::s)
                    .select((t::s, sql::<Nullable<Double>>(aggregate.sql())))
                    .load::<(Option<String>, Option<f64>)>(connection)
                    .ok()?
                    .into_iter()
                    .map(|(s, v)| alloc::vec![group(s), v.map_or(Cell::Null, Cell::Real)])
                    .collect(),
                _ => t::table
                    .filter(filter())
                    .group_by(t::s)
                    .select((t::s, sql::<Nullable<BigInt>>(aggregate.sql())))
                    .load::<(Option<String>, Option<i64>)>(connection)
                    .ok()?
                    .into_iter()
                    .map(|(s, v)| alloc::vec![group(s), v.map_or(Cell::Null, Cell::Int)])
                    .collect(),
            })
        }
    }
}

/// One sequence run: the engine, the model database and every live
/// subscription.
/// A directory for the engine's store, removed when the run ends.
struct Store {
    path: std::path::PathBuf,
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// What one sequence compared, so a run that compares nothing is visible.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineModelCoverage {
    /// Comparisons of rows or columns maintained in process.
    pub rows: usize,
    /// Comparisons of aggregates folded in process.
    pub aggregates: usize,
    /// Comparisons of answers a database read keeps.
    pub reads: usize,
    /// Tier changes the engine reported for a compared subscription.
    pub transitions: usize,
    /// Restarts every live subscription came back from.
    pub restarts: usize,
}

impl core::ops::AddAssign for EngineModelCoverage {
    fn add_assign(&mut self, other: Self) {
        self.rows += other.rows;
        self.aggregates += other.aggregates;
        self.reads += other.reads;
        self.transitions += other.transitions;
        self.restarts += other.restarts;
    }
}

struct Run {
    coverage: EngineModelCoverage,
    engine: Model,
    store: Store,
    url: String,
    model: SqliteConnection,
    seeds: DieselConnector<SqliteConnection, SQLite>,
    table: TableId,
    rows: BTreeMap<i32, Row>,
    subscriptions: Vec<Subscription>,
    next_consumer: u64,
    script: Vec<String>,
}

fn database_url() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "file:subql_engine_model_{}?mode=memory&cache=shared",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

impl Run {
    fn new() -> Self {
        let url = database_url();
        let mut model = SqliteConnection::establish(&url).expect("the model database opens");
        // DDL, which the typed DSL does not build.
        diesel::sql_query(predicate_grammar::ddl(Engine::Sqlite))
            .execute(&mut model)
            .expect("the model table is created");
        let reads = SqliteConnection::establish(&url).expect("the read connection opens");
        let seeds = SqliteConnection::establish(&url).expect("the seed connection opens");
        let database = predicate_grammar::catalog::<SQLiteDialect>(Engine::Sqlite);
        let table = catalog_helpers::table_id::<SQLite, _>(&database, "t")
            .expect("the model table is in the catalog");
        let store = Store {
            path: std::env::temp_dir().join(url.replace(['?', '=', '&', ':'], "_")),
        };
        let mut inner =
            SubscriptionEngine::with_storage(database, SQLiteDialect {}, store.path.clone())
                .expect("the store opens")
                .into_parts()
                .0;
        // Every registration and removal is on disk before it returns, which
        // is the durability a restart can be held to.
        inner.set_rotation_threshold(0);
        Self {
            coverage: EngineModelCoverage::default(),
            engine: AutoResolvingEngine::new(inner, SyncMode(DieselConnector::new(reads))),
            store,
            url,
            model,
            seeds: DieselConnector::new(seeds),
            table,
            rows: BTreeMap::new(),
            subscriptions: Vec::new(),
            next_consumer: 1,
            script: Vec::new(),
        }
    }

    fn register(&mut self, statement: Statement) {
        let consumer = self.next_consumer;
        self.next_consumer += 1;
        let sql = statement.sql();
        self.script.push(format!("-- register {consumer}: {sql}"));
        let Ok(registered) = self
            .engine
            .register(SubscriptionRequest::new(consumer, sql), ())
        else {
            return;
        };
        let id = registered.subscription_id;
        let view = match &registered.tier {
            Tier::InProcess(served) => match &served.aggregate_bootstrap {
                None => sqlite_answers(&mut self.model, &statement).map(View::Stream),
                Some(bootstrap) => self.seed(id, bootstrap),
            },
            tier => self.snapshot(id, tier),
        };
        let Some(view) = view else {
            self.engine.unregister_subscription(id);
            return;
        };
        self.subscriptions.push(Subscription {
            id,
            consumer,
            statement,
            view,
            tier: registered.tier.kind(),
        });
    }

    /// Seed a folding aggregate from its bootstrap read, as its subscriber
    /// does, and start its view from what the install reports.
    fn seed(&mut self, id: SubscriptionId, bootstrap: &AggregateBootstrap<SQLite>) -> Option<View> {
        let read = bootstrap.query.as_read_query();
        let rows = if bootstrap.group_columns == 0 {
            let (row, _) = self
                .seeds
                .execute_scalar_row(&read, &bootstrap.kinds, &())
                .ok()?;
            alloc::vec![row]
        } else {
            let mut rows = self
                .seeds
                .read_page(&read, usize::MAX, &())
                .ok()?
                .value
                .rows;
            for row in &mut rows {
                for (value, kind) in row.iter_mut().zip(&bootstrap.kinds) {
                    let raw = core::mem::replace(value, Value::Missing);
                    *value = SQLite::decode_group_value(ValueKind::from(*kind), raw)
                        .unwrap_or(Value::Missing);
                }
            }
            rows
        };
        let updates = crate::Install::install(
            &mut self.engine,
            id,
            AggregateSeedInstall::<SQLite> { rows, fence: None },
        )
        .ok()?;
        let mut view = View::Groups(BTreeMap::new());
        apply_aggregates(&mut view, id, &updates);
        Some(view)
    }

    /// Prime a read-maintained answer through the engine's snapshot, as its
    /// subscriber does, and start its view from what it answers.
    fn snapshot(&mut self, id: SubscriptionId, tier: &Tier<SQLite>) -> Option<View> {
        Some(match self.engine.snapshot(id).ok()?? {
            SnapshotResult::Scalar(value, _) => {
                let mut groups = BTreeMap::new();
                groups.insert(Vec::new(), (Vec::new(), Cell::of(&value)));
                View::Groups(groups)
            }
            SnapshotResult::GroupedAggregate { updates, .. } => {
                let mut view = View::Groups(BTreeMap::new());
                apply_aggregates(&mut view, id, &updates);
                view
            }
            SnapshotResult::Rows { columns, rows, .. } => rows_view(tier, &columns, &rows),
        })
    }

    /// Drop the engine, reopen it from its store and adopt what came back,
    /// then prime every answer a restart leaves unprimed.
    ///
    /// Every subscription has to come back: the catalog did not change, so
    /// nothing justifies dropping one.
    fn restart(&mut self) {
        self.script.push(String::from("-- restart"));
        let reads = SqliteConnection::establish(&self.url).expect("the read connection opens");
        let placeholder = AutoResolvingEngine::new(
            SubscriptionEngine::new(
                predicate_grammar::catalog::<SQLiteDialect>(Engine::Sqlite),
                SQLiteDialect {},
            ),
            SyncMode(DieselConnector::new(
                SqliteConnection::establish(&self.url).expect("a connection opens"),
            )),
        );
        drop(core::mem::replace(&mut self.engine, placeholder));
        let restored = SubscriptionEngine::with_storage(
            predicate_grammar::catalog::<SQLiteDialect>(Engine::Sqlite),
            SQLiteDialect {},
            self.store.path.clone(),
        )
        .unwrap_or_else(|error| panic!("the store reopens: {error}\n{}", self.script.join("\n")));
        let returned = restored.reads().clone();
        self.engine = AutoResolvingEngine::adopt(
            restored,
            SyncMode(DieselConnector::new(reads)),
            |_| (),
            |_| (),
        );
        // A reopened engine starts at the default threshold, which writes no
        // shard for a small table.
        self.engine.set_rotation_threshold(0);
        let subscriptions = core::mem::take(&mut self.subscriptions);
        for mut subscription in subscriptions {
            let id = subscription.id;
            // A restart plans a read again, so a keyed read may come back
            // whole, but only a shard brings back an answer served in
            // process, and only the reads file one served by a read.
            let read_tier = returned
                .restored
                .iter()
                .any(|read| read.subscription_id == id);
            assert_eq!(
                read_tier,
                subscription.tier != TierKind::InProcess,
                "subscription `{}` came back served another way than at {:?}\n{}",
                subscription.statement.sql(),
                subscription.tier,
                self.script.join("\n")
            );
            let view = if let Some(read) = returned
                .restored
                .iter()
                .find(|read| read.subscription_id == id)
            {
                self.snapshot(id, &read.tier)
            } else if let Some(answer) = returned
                .in_process
                .iter()
                .find(|answer| answer.subscription_id == id)
            {
                match &answer.aggregate_bootstrap {
                    Some(bootstrap) => self.seed(id, bootstrap),
                    None => Some(subscription.view),
                }
            } else {
                panic!(
                    "subscription `{}` did not come back from the store, dropped {:?}\n{}",
                    subscription.statement.sql(),
                    returned
                        .dropped
                        .iter()
                        .map(|dropped| (&dropped.sql, &dropped.reason))
                        .collect::<Vec<_>>(),
                    self.script.join("\n")
                )
            };
            if let Some(view) = view {
                subscription.view = view;
                self.subscriptions.push(subscription);
            } else {
                self.engine.unregister_subscription(id);
            }
        }
        // The model connection is held across the restart, so the shared
        // in-memory database, and every row in it, is still there.
        let stored: i64 = t::table
            .count()
            .get_result(&mut self.model)
            .expect("the model table counts");
        assert_eq!(
            usize::try_from(stored).ok(),
            Some(self.rows.len()),
            "the model table lost rows across a restart\n{}",
            self.script.join("\n")
        );
        self.coverage.restarts += 1;
    }

    fn unregister(&mut self, index: usize) {
        if self.subscriptions.is_empty() {
            return;
        }
        let subscription = self.subscriptions.remove(index % self.subscriptions.len());
        self.script
            .push(format!("-- unregister {}", subscription.consumer));
        self.engine.unregister_subscription(subscription.id);
    }

    /// Apply one write to the model and to the engine, then check every
    /// subscription.
    fn write(&mut self, step: &Step) {
        let Some((event, old, new)) = self.store(step) else {
            return;
        };
        self.dispatch(&event, matches!(step, Step::Truncate), old, new);
        self.compare();
    }

    /// Apply `step` to the model, and hand back its change event with the
    /// row images it carries, `None` when the model refused or ignored it.
    #[allow(clippy::type_complexity)]
    fn store(
        &mut self,
        step: &Step,
    ) -> Option<(
        TestEvent<SQLite>,
        Option<Vec<Value<SQLite>>>,
        Option<Vec<Value<SQLite>>>,
    )> {
        Some(match step {
            Step::Insert(id, row) => {
                if self.rows.contains_key(id) {
                    return None;
                }
                if diesel::insert_into(t::table)
                    .values(row.values_at(*id))
                    .execute(&mut self.model)
                    .is_err()
                {
                    return None;
                }
                self.script
                    .push(format!("{};", row.insert_sql_at(*id, Engine::Sqlite)));
                self.rows.insert(*id, row.clone());
                let cells = row.cells_at::<SQLite>(*id);
                (
                    TestEvent::insert(self.table, cells.clone()).with_pk_columns([0u16]),
                    None,
                    Some(cells),
                )
            }
            Step::Update {
                id,
                to,
                row,
                old_image,
            } => {
                let before = self.rows.get(id).cloned()?;
                if to != id && self.rows.contains_key(to) {
                    return None;
                }
                if diesel::delete(t::table.filter(t::id.eq(*id)))
                    .execute(&mut self.model)
                    .is_err()
                    || diesel::insert_into(t::table)
                        .values(row.values_at(*to))
                        .execute(&mut self.model)
                        .is_err()
                {
                    return None;
                }
                self.script.push(format!(
                    "DELETE FROM t WHERE id = {id}; {};",
                    row.insert_sql_at(*to, Engine::Sqlite)
                ));
                self.rows.remove(id);
                self.rows.insert(*to, row.clone());
                let old = before.cells_at::<SQLite>(*id);
                let new = row.cells_at::<SQLite>(*to);
                let changed = changed_columns(&old, &new);
                (
                    TestEvent::update(
                        self.table,
                        if *old_image || to != id {
                            old.clone()
                        } else {
                            Vec::new()
                        },
                        new.clone(),
                    )
                    .with_pk_columns([0u16])
                    .with_changed_columns(changed),
                    Some(old),
                    Some(new),
                )
            }
            Step::Delete(id) => {
                let before = self.rows.remove(id)?;
                if diesel::delete(t::table.filter(t::id.eq(*id)))
                    .execute(&mut self.model)
                    .is_err()
                {
                    return None;
                }
                self.script.push(format!("DELETE FROM t WHERE id = {id};"));
                let old = before.cells_at::<SQLite>(*id);
                (
                    TestEvent::delete(self.table, old.clone()).with_pk_columns([0u16]),
                    Some(old),
                    None,
                )
            }
            Step::Truncate => {
                if diesel::delete(t::table).execute(&mut self.model).is_err() {
                    return None;
                }
                self.script.push(String::from("DELETE FROM t;"));
                self.rows.clear();
                (TestEvent::truncate(self.table), None, None)
            }
            Step::Register(_) | Step::Unregister(_) | Step::Restart => return None,
        })
    }

    /// Dispatch `event` and fold what it answered into every subscription.
    fn dispatch(
        &mut self,
        event: &TestEvent<SQLite>,
        truncate: bool,
        old: Option<Vec<Value<SQLite>>>,
        new: Option<Vec<Value<SQLite>>>,
    ) {
        let dispatch = self
            .engine
            .apply(event)
            .unwrap_or_else(|error| panic!("dispatch failed: {error}\n{}", self.script.join("\n")));
        let settled = dispatch.resolve_collect();
        let notifications = &settled.dispatched.engine;
        let old = old.map(|cells| cells.iter().map(Cell::of).collect::<Vec<_>>());
        let new = new.map(|cells| cells.iter().map(Cell::of).collect::<Vec<_>>());
        let reads = settled.reads.ok();
        let mut unknown = Vec::new();
        for subscription in &mut self.subscriptions {
            let consumer = subscription.consumer;
            let refused = notifications
                .evaluation_failures()
                .iter()
                .any(|failure| failure.consumer_id == consumer)
                || notifications
                    .unanswered()
                    .iter()
                    .any(|cell| cell.consumer_id == consumer);
            if refused || reads.is_none() {
                unknown.push(subscription.id);
                continue;
            }
            fold_rows(
                subscription,
                notifications,
                truncate,
                old.as_deref(),
                new.as_deref(),
            );
            apply_aggregates(
                &mut subscription.view,
                subscription.id,
                &settled.dispatched.aggregate_updates,
            );
            apply_scalars(subscription, &settled.dispatched.scalar_updates);
            if let Some(reads) = &reads {
                absorb_reads(subscription, reads);
            }
            for transition in settled
                .dispatched
                .transitions
                .iter()
                .chain(reads.iter().flat_map(|reads| &reads.transitions))
            {
                if transition.subscription_id == subscription.id {
                    self.coverage.transitions += 1;
                    subscription.tier = transition.to.kind();
                }
            }
        }
        self.subscriptions
            .retain(|subscription| !unknown.contains(&subscription.id));
    }

    /// Require every subscription to hold what SQLite answers.
    fn compare(&mut self) {
        let Self {
            model,
            subscriptions,
            script,
            coverage,
            ..
        } = self;
        for subscription in subscriptions.iter() {
            let Some(expected) = sqlite_answers(model, &subscription.statement) else {
                continue;
            };
            match (subscription.tier, &subscription.view) {
                (TierKind::InProcess, View::Groups(_)) => coverage.aggregates += 1,
                (TierKind::InProcess, _) => coverage.rows += 1,
                _ => coverage.reads += 1,
            }
            let held = subscription.view.answer();
            assert!(
                same_answer(&held, &expected),
                "subscription `{}` holds {held:?} where SQLite answers {expected:?}\n{}",
                subscription.statement.sql(),
                script.join("\n")
            );
        }
    }
}

/// The columns an update changed, a `NULL` on either side counting as a
/// change against any value.
fn changed_columns(old: &[Value<SQLite>], new: &[Value<SQLite>]) -> Vec<u16> {
    (0u16..6)
        .filter(|&column| {
            let column = usize::from(column);
            !Cell::of(&old[column]).same(&Cell::of(&new[column]))
                || matches!(
                    (&old[column], &new[column]),
                    (Value::Null, v) | (v, Value::Null) if !matches!(v, Value::Null)
                )
        })
        .collect()
}

/// Fold an in-process row notification into `subscription`'s held rows,
/// the old image leaving and the new one arriving as the lists name it.
fn fold_rows(
    subscription: &mut Subscription,
    notifications: &crate::ConsumerNotifications<DefaultIds, crate::NoCheckpoint, SQLite>,
    truncate: bool,
    old: Option<&[Cell]>,
    new: Option<&[Cell]>,
) {
    let View::Stream(rows) = &mut subscription.view else {
        return;
    };
    let consumer = subscription.consumer;
    let listed = |ids: &[u64]| ids.contains(&consumer);
    let deleted = listed(notifications.deleted());
    let updated = listed(notifications.updated());
    if truncate && deleted {
        rows.clear();
    }
    if let Some(old) = old.filter(|_| deleted || updated) {
        let projected = project(&subscription.statement, old);
        if let Some(at) = rows.iter().position(|held| {
            held.len() == projected.len() && held.iter().zip(&projected).all(|(a, b)| a.same(b))
        }) {
            rows.remove(at);
        }
    }
    if let Some(new) = new.filter(|_| listed(notifications.inserted()) || updated) {
        rows.push(project(&subscription.statement, new));
    }
}

/// Replace `subscription`'s view with the scalar value an update names.
fn apply_scalars(
    subscription: &mut Subscription,
    updates: &[crate::reexec::ScalarUpdate<DefaultIds, SQLite, crate::NoCheckpoint>],
) {
    for update in updates {
        if update.subscription_id == subscription.id {
            let mut groups = BTreeMap::new();
            groups.insert(Vec::new(), (Vec::new(), Cell::of(&update.value)));
            subscription.view = View::Groups(groups);
        }
    }
}

/// Fold what the re-execution reads delivered into `subscription`.
fn absorb_reads(
    subscription: &mut Subscription,
    reads: &crate::reexec::ResolvedReads<DefaultIds, SQLite, crate::NoCheckpoint>,
) {
    apply_aggregates(
        &mut subscription.view,
        subscription.id,
        &reads.aggregate_updates,
    );
    apply_scalars(subscription, &reads.scalar_updates);
    for update in &reads.rows_updates {
        if update.subscription_id == subscription.id {
            subscription.view = View::Whole(
                update
                    .rows
                    .iter()
                    .map(|row| row.iter().map(Cell::of).collect())
                    .collect(),
            );
        }
    }
    for delta in &reads.row_deltas {
        if delta.subscription_id != subscription.id {
            continue;
        }
        if !matches!(subscription.view, View::Keyed(_)) {
            subscription.view = View::Keyed(BTreeMap::new());
        }
        if let View::Keyed(rows) = &mut subscription.view {
            let key = key_of(&delta.key.iter().map(Cell::of).collect::<Vec<_>>());
            match &delta.row {
                Some(row) => {
                    rows.insert(key, row.iter().map(Cell::of).collect());
                }
                None => {
                    rows.remove(&key);
                }
            }
        }
    }
}

/// The view a re-read's rows start, keyed when the tier reads by key.
fn rows_view(tier: &Tier<SQLite>, columns: &[String], rows: &[Vec<Value<SQLite>>]) -> View {
    let cells: Answer = rows
        .iter()
        .map(|row| row.iter().map(Cell::of).collect())
        .collect();
    match (tier, columns.iter().position(|name| name == "id")) {
        (Tier::KeyedRows { .. }, Some(key)) => View::Keyed(
            cells
                .into_iter()
                .map(|row| (key_of(core::slice::from_ref(&row[key])), row))
                .collect(),
        ),
        _ => View::Whole(cells),
    }
}

/// Fold aggregate updates for `id` into its view.
fn apply_aggregates(
    view: &mut View,
    id: SubscriptionId,
    updates: &[AggregateValueUpdate<DefaultIds, SQLite>],
) {
    for update in updates {
        if update.subscription != id {
            continue;
        }
        if !matches!(view, View::Groups(_)) {
            *view = View::Groups(BTreeMap::new());
        }
        let View::Groups(groups) = view else {
            continue;
        };
        let group: Vec<Cell> = update.group.as_ref().map_or_else(Vec::new, |group| {
            group.values.iter().map(Cell::of).collect()
        });
        let key = key_of(&group);
        match &update.change {
            AggregateValueChange::Set(value) => {
                let value = match value {
                    AggregateResultValue::Folded(value) => Cell::of_aggregate(value),
                    AggregateResultValue::Scalar(value) => Cell::of(value),
                };
                groups.insert(key, (group, value));
            }
            AggregateValueChange::Remove => {
                groups.remove(&key);
            }
        }
    }
}

/// Draw a sequence and run it against the model.
///
/// Contract: a panic is a subscription whose answer, as its subscriber
/// would hold it, differs from SQLite's for the same statement.
pub fn harness_engine_model_sqlite(data: &[u8]) {
    let _ = engine_model_sqlite(data);
}

/// [`harness_engine_model_sqlite`], reporting what the sequence compared.
///
/// # Panics
///
/// As [`harness_engine_model_sqlite`].
#[must_use]
pub fn engine_model_sqlite(data: &[u8]) -> EngineModelCoverage {
    let mut u = Unstructured::new(data);
    let Ok(len) = u.int_in_range(1usize..=24) else {
        return EngineModelCoverage::default();
    };
    let mut run = Run::new();
    for _ in 0..len {
        let Ok(step) = Step::arbitrary(&mut u) else {
            break;
        };
        match step {
            Step::Register(statement) => run.register(statement),
            Step::Unregister(index) => run.unregister(index),
            Step::Restart => run.restart(),
            write => run.write(&write),
        }
    }
    run.coverage
}
