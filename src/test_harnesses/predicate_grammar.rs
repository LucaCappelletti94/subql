//! Generated filters over one fixed table, rendered for each engine.
//!
//! Every choice is drawn from [`Unstructured`], so one byte string names one
//! case in the libFuzzer target and in the Docker sweep alike. The grammar
//! draws expressions in every position subql compiles: the condition itself,
//! comparison operands, `IN` lists, `BETWEEN` bounds, `LIKE` patterns with
//! and without `ESCAPE`, `COALESCE` arguments, truth tests, null-safe
//! equality and arithmetic. Nothing restricts a value to where a boolean is
//! expected, because a shape nobody wrote down is what this is for.

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use arbitrary::{Arbitrary as _, Unstructured};
use sql_traits::structs::ParserDB;

use crate::backend::{Backend, Value};
use crate::compiler::SqlLiteralParse;
use crate::testing::TestEvent;
use crate::{catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest};

/// The engine a case is rendered for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    /// PostgreSQL.
    Postgres,
    /// MySQL.
    MySql,
    /// SQLite.
    Sqlite,
}

/// The table every case reads, `t (id, a, b, r, s, f)`.
#[must_use]
pub const fn ddl(engine: Engine) -> &'static str {
    match engine {
        Engine::Postgres => {
            "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT, b BIGINT, r DOUBLE PRECISION, s TEXT, f BOOLEAN)"
        }
        Engine::MySql => {
            "CREATE TABLE t (id INT PRIMARY KEY, a BIGINT, b BIGINT, r DOUBLE, s VARCHAR(64), f BOOLEAN)"
        }
        Engine::Sqlite => {
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, r REAL, s TEXT, f BOOLEAN)"
        }
    }
}

const INTS: [i64; 10] = [0, 1, -1, 2, 7, 100, -100, 1 << 40, i64::MAX, i64::MIN];
const FLOATS: [f64; 6] = [0.0, 0.5, -1.5, 2.0, 1e10, 2.5e-3];
const STRINGS: [&str; 9] = ["", "a", "A", "ab", "a%", "_", "x'y", " a", "a!%"];

/// One row of `t`, each column possibly `NULL`.
#[derive(Clone, Debug)]
pub struct Row {
    a: Option<i64>,
    b: Option<i64>,
    r: Option<f64>,
    s: Option<&'static str>,
    f: Option<bool>,
}

impl Row {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(Self {
            a: maybe(u, |u| pick(u, &INTS))?,
            b: maybe(u, |u| pick(u, &INTS))?,
            r: maybe(u, |u| pick(u, &FLOATS))?,
            s: maybe(u, |u| pick(u, &STRINGS))?,
            f: maybe(u, bool::arbitrary)?,
        })
    }

    /// The statement that stores the row with `id = 1`.
    #[must_use]
    pub fn insert_sql(&self, engine: Engine) -> String {
        let int = |v: Option<i64>| v.map_or_else(|| "NULL".into(), |n| int_literal(n, engine));
        format!(
            "INSERT INTO t (id, a, b, r, s, f) VALUES (1, {}, {}, {}, {}, {})",
            int(self.a),
            int(self.b),
            self.r.map_or_else(|| "NULL".into(), float_literal),
            self.s.map_or_else(|| "NULL".into(), string_literal),
            self.f.map_or_else(
                || String::from("NULL"),
                |flag| bool_literal(flag, engine).into()
            ),
        )
    }

    /// The row as a change event carries it, in column order.
    #[must_use]
    pub fn cells<B>(&self) -> Vec<Value<B>>
    where
        B: Backend<Int = i64, Float = f64, String = String>,
        B::Bool: From<bool>,
    {
        let or_null = |value: Option<Value<B>>| value.unwrap_or(Value::Null);
        alloc::vec![
            Value::Int(1),
            or_null(self.a.map(Value::Int)),
            or_null(self.b.map(Value::Int)),
            or_null(self.r.map(Value::Float)),
            or_null(self.s.map(|text| Value::String(text.into()))),
            or_null(self.f.map(|flag| Value::Bool(flag.into()))),
        ]
    }
}

/// A column of `t` other than the key.
#[derive(Clone, Copy, Debug)]
pub enum Column {
    /// `a`, an integer.
    A,
    /// `b`, an integer.
    B,
    /// `r`, a float.
    R,
    /// `s`, text.
    S,
    /// `f`, a boolean.
    F,
}

/// A binary operator, rendered as its SQL spelling.
#[derive(Clone, Copy, Debug)]
pub enum Op {
    /// `=`
    Eq,
    /// `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `AND`
    And,
    /// `OR`
    Or,
}

/// The value a truth test names.
#[derive(Clone, Copy, Debug)]
pub enum TruthValue {
    /// `TRUE`
    True,
    /// `FALSE`
    False,
    /// `UNKNOWN`
    Unknown,
}

/// An expression tree, rendered fully parenthesised.
#[derive(Clone, Debug)]
pub enum Expr {
    /// A column of `t`.
    Column(Column),
    /// An integer literal.
    Int(i64),
    /// A float literal.
    Float(f64),
    /// A string literal.
    Str(&'static str),
    /// A boolean literal.
    Bool(bool),
    /// `NULL`.
    Null,
    /// `NOT e`.
    Not(Box<Self>),
    /// `-e`.
    Neg(Box<Self>),
    /// A binary operator.
    Binary(Op, Box<Self>, Box<Self>),
    /// `e IS [NOT] NULL`.
    IsNull(Box<Self>, bool),
    /// `e IS [NOT] TRUE`, `FALSE` or `UNKNOWN`.
    IsTruth(Box<Self>, TruthValue, bool),
    /// Null-safe equality, negated for `IS DISTINCT FROM`.
    NullSafe(Box<Self>, Box<Self>, bool),
    /// `e [NOT] IN (list)`.
    In(Box<Self>, Vec<Self>, bool),
    /// `e [NOT] BETWEEN low AND high`.
    Between(Box<Self>, Box<Self>, Box<Self>, bool),
    /// `e [NOT] LIKE pattern [ESCAPE c]`.
    Like(Box<Self>, Box<Self>, Option<char>, bool),
    /// `COALESCE(args)`.
    Coalesce(Vec<Self>),
}

impl Expr {
    fn arbitrary(u: &mut Unstructured<'_>, depth: u8) -> arbitrary::Result<Self> {
        if depth == 0 || u.ratio(1u8, 4u8)? {
            return Self::leaf(u);
        }
        let deeper = |u: &mut Unstructured<'_>| Self::arbitrary(u, depth - 1).map(Box::new);
        Ok(match u.int_in_range(0u8..=10)? {
            0 => Self::Not(deeper(u)?),
            1 => Self::Neg(deeper(u)?),
            2 | 3 => {
                let op = pick(
                    u,
                    &[
                        Op::Eq,
                        Op::Ne,
                        Op::Lt,
                        Op::Le,
                        Op::Gt,
                        Op::Ge,
                        Op::Add,
                        Op::Sub,
                        Op::Mul,
                        Op::Div,
                        Op::Mod,
                        Op::And,
                        Op::Or,
                    ],
                )?;
                Self::Binary(op, deeper(u)?, deeper(u)?)
            }
            4 => Self::IsNull(deeper(u)?, u.arbitrary()?),
            5 => Self::IsTruth(
                deeper(u)?,
                pick(
                    u,
                    &[TruthValue::True, TruthValue::False, TruthValue::Unknown],
                )?,
                u.arbitrary()?,
            ),
            6 => Self::NullSafe(deeper(u)?, deeper(u)?, u.arbitrary()?),
            7 => {
                let tested = deeper(u)?;
                let len = u.int_in_range(1usize..=4)?;
                let list = (0..len)
                    .map(|_| Self::arbitrary(u, depth - 1))
                    .collect::<arbitrary::Result<_>>()?;
                Self::In(tested, list, u.arbitrary()?)
            }
            8 => Self::Between(deeper(u)?, deeper(u)?, deeper(u)?, u.arbitrary()?),
            9 => Self::Like(
                deeper(u)?,
                deeper(u)?,
                pick(u, &[None, Some('!'), Some('\\')])?,
                u.arbitrary()?,
            ),
            _ => {
                let len = u.int_in_range(1usize..=3)?;
                Self::Coalesce(
                    (0..len)
                        .map(|_| Self::arbitrary(u, depth - 1))
                        .collect::<arbitrary::Result<_>>()?,
                )
            }
        })
    }

    fn leaf(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0u8..=7)? {
            0..=2 => Self::Column(pick(
                u,
                &[Column::A, Column::B, Column::R, Column::S, Column::F],
            )?),
            3 => Self::Int(pick(u, &INTS)?),
            4 => Self::Float(pick(u, &FLOATS)?),
            5 => Self::Str(pick(u, &STRINGS)?),
            6 => Self::Bool(u.arbitrary()?),
            _ => Self::Null,
        })
    }

    /// The expression as `engine` spells it.
    #[must_use]
    pub fn render(&self, engine: Engine) -> String {
        let not = |negated: bool| if negated { "NOT " } else { "" };
        match self {
            Self::Column(column) => match column {
                Column::A => "a",
                Column::B => "b",
                Column::R => "r",
                Column::S => "s",
                Column::F => "f",
            }
            .into(),
            Self::Int(n) => int_literal(*n, engine),
            Self::Float(x) => float_literal(*x),
            Self::Str(text) => string_literal(text),
            Self::Bool(flag) => bool_literal(*flag, engine).into(),
            Self::Null => "NULL".into(),
            Self::Not(inner) => format!("(NOT {})", inner.render(engine)),
            Self::Neg(inner) => format!("(- {})", inner.render(engine)),
            Self::Binary(op, left, right) => {
                let op = match op {
                    Op::Eq => "=",
                    Op::Ne => "<>",
                    Op::Lt => "<",
                    Op::Le => "<=",
                    Op::Gt => ">",
                    Op::Ge => ">=",
                    Op::Add => "+",
                    Op::Sub => "-",
                    Op::Mul => "*",
                    Op::Div => "/",
                    Op::Mod => "%",
                    Op::And => "AND",
                    Op::Or => "OR",
                };
                format!("({} {op} {})", left.render(engine), right.render(engine))
            }
            Self::IsNull(inner, negated) => {
                format!("({} IS {}NULL)", inner.render(engine), not(*negated))
            }
            Self::IsTruth(inner, value, negated) => {
                let value = match value {
                    TruthValue::True => "TRUE",
                    TruthValue::False => "FALSE",
                    TruthValue::Unknown => "UNKNOWN",
                };
                format!("({} IS {}{value})", inner.render(engine), not(*negated))
            }
            Self::NullSafe(left, right, distinct) => match engine {
                Engine::MySql => {
                    let equal = format!("({} <=> {})", left.render(engine), right.render(engine));
                    if *distinct {
                        format!("(NOT {equal})")
                    } else {
                        equal
                    }
                }
                Engine::Postgres | Engine::Sqlite => format!(
                    "({} IS {}DISTINCT FROM {})",
                    left.render(engine),
                    if *distinct { "" } else { "NOT " },
                    right.render(engine)
                ),
            },
            Self::In(tested, list, negated) => {
                let list: Vec<String> = list.iter().map(|e| e.render(engine)).collect();
                format!(
                    "({} {}IN ({}))",
                    tested.render(engine),
                    not(*negated),
                    list.join(", ")
                )
            }
            Self::Between(tested, low, high, negated) => format!(
                "({} {}BETWEEN {} AND {})",
                tested.render(engine),
                not(*negated),
                low.render(engine),
                high.render(engine)
            ),
            Self::Like(tested, pattern, escape, negated) => {
                let escape = escape.map_or_else(String::new, |c| {
                    format!(" ESCAPE {}", escape_literal(c, engine))
                });
                format!(
                    "({} {}LIKE {}{escape})",
                    tested.render(engine),
                    not(*negated),
                    pattern.render(engine)
                )
            }
            Self::Coalesce(args) => {
                let args: Vec<String> = args.iter().map(|e| e.render(engine)).collect();
                format!("COALESCE({})", args.join(", "))
            }
        }
    }
}

/// One generated case: a row of `t` and a filter over it.
#[derive(Clone, Debug)]
pub struct Case {
    /// The row stored in `t`.
    pub row: Row,
    /// The filter, rendered per engine with [`Expr::render`].
    pub filter: Expr,
}

impl Case {
    /// Draw a case from fuzzer-controlled bytes.
    ///
    /// # Errors
    ///
    /// When the bytes run out before the case is complete.
    pub fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        Ok(Self {
            row: Row::arbitrary(u)?,
            filter: Expr::arbitrary(u, 4)?,
        })
    }

    /// DDL, insert and filter, which is what a divergence has to carry.
    #[must_use]
    pub fn reproduction(&self, engine: Engine) -> String {
        format!(
            "{};\n{};\nSELECT COUNT(*) FROM t WHERE {};",
            ddl(engine),
            self.row.insert_sql(engine),
            self.filter.render(engine)
        )
    }
}

fn maybe<'a, T>(
    u: &mut Unstructured<'a>,
    draw: impl FnOnce(&mut Unstructured<'a>) -> arbitrary::Result<T>,
) -> arbitrary::Result<Option<T>> {
    if u.ratio(1u8, 5u8)? {
        Ok(None)
    } else {
        draw(u).map(Some)
    }
}

fn pick<T: Copy>(u: &mut Unstructured<'_>, choices: &[T]) -> arbitrary::Result<T> {
    u.choose(choices).copied()
}

/// `i64::MIN` has no positive literal to negate, so it is written as the
/// difference every engine folds exactly.
fn int_literal(n: i64, _engine: Engine) -> String {
    if n == i64::MIN {
        "(-9223372036854775807 - 1)".into()
    } else if n < 0 {
        format!("({n})")
    } else {
        format!("{n}")
    }
}

fn float_literal(x: f64) -> String {
    if x < 0.0 {
        format!("({x:?})")
    } else {
        format!("{x:?}")
    }
}

fn string_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// MySQL reads a backslash inside a string literal as an escape, so its
/// one-backslash literal is written doubled.
fn escape_literal(c: char, engine: Engine) -> String {
    match (c, engine) {
        ('\\', Engine::MySql) => "'\\\\'".into(),
        _ => format!("'{c}'"),
    }
}

const fn bool_literal(flag: bool, engine: Engine) -> &'static str {
    match (flag, engine) {
        (true, Engine::Sqlite) => "1",
        (false, Engine::Sqlite) => "0",
        (true, _) => "TRUE",
        (false, _) => "FALSE",
    }
}

/// The catalog of [`ddl`] for `engine`.
///
/// # Panics
///
/// If the fixed DDL stops parsing.
#[must_use]
pub fn catalog<D: sqlparser::dialect::Dialect + Default>(engine: Engine) -> ParserDB {
    ParserDB::parse::<D>(ddl(engine)).expect("the case table parses")
}

/// Whether subql, serving the filter in process, delivers the row.
///
/// `None` when subql does not answer in process: the statement is routed to
/// a read, refused, or the row leaves the answer to the engine. Only a served
/// answer can diverge.
///
/// `database` is [`catalog`] for `engine`, taken by value so a caller judging
/// many cases parses it once and clones it.
#[must_use]
pub fn subql_selects<B>(case: &Case, engine: Engine, database: ParserDB) -> Option<bool>
where
    B: Backend<Int = i64, Float = f64, String = String> + SqlLiteralParse + 'static,
    B::Bool: From<bool>,
    B::Dialect: sqlparser::dialect::Dialect + Default,
{
    let table = catalog_helpers::table_id::<B, _>(&database, "t")?;
    let mut subql: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(database, B::Dialect::default());
    let registered = match subql.register(SubscriptionRequest::new(
        1u64,
        format!("SELECT * FROM t WHERE {}", case.filter.render(engine)),
    )) {
        Ok(registered) => registered,
        Err(RegisterError::NotServedInProcess(_) | RegisterError::RefusedByEngine { .. }) => {
            return None;
        }
        Err(_) => return None,
    };
    if registered.not_served_because.is_some() {
        return None;
    }
    let notifications = subql
        .consumers(&TestEvent::insert(table, case.row.cells::<B>()))
        .ok()?;
    if !notifications.evaluation_failures().is_empty() || !notifications.unanswered().is_empty() {
        return None;
    }
    Some(!notifications.inserted().is_empty())
}

#[cfg(feature = "pg-sqlite-emu")]
diesel::table! {
    /// The table every generated case reads, as [`ddl`] creates it.
    t (id) {
        /// The key, always `1`.
        id -> Integer,
        /// An integer column.
        a -> Nullable<BigInt>,
        /// A second integer column.
        b -> Nullable<BigInt>,
        /// A float column.
        r -> Nullable<Double>,
        /// A text column.
        s -> Nullable<Text>,
        /// A boolean column.
        f -> Nullable<Bool>,
    }
}

#[cfg(feature = "pg-sqlite-emu")]
impl Row {
    /// The row's columns as a typed insert into [`t`].
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn values(
        &self,
    ) -> (
        diesel::dsl::Eq<t::id, i32>,
        diesel::dsl::Eq<t::a, Option<i64>>,
        diesel::dsl::Eq<t::b, Option<i64>>,
        diesel::dsl::Eq<t::r, Option<f64>>,
        diesel::dsl::Eq<t::s, Option<&'static str>>,
        diesel::dsl::Eq<t::f, Option<bool>>,
    ) {
        use diesel::ExpressionMethods as _;
        (
            t::id.eq(1),
            t::a.eq(self.a),
            t::b.eq(self.b),
            t::r.eq(self.r),
            t::s.eq(self.s),
            t::f.eq(self.f),
        )
    }
}

/// Whether SQLite, filtering the stored row with the case, keeps it.
///
/// `None` when SQLite refuses the statement.
#[cfg(feature = "pg-sqlite-emu")]
fn sqlite_selects(connection: &mut diesel::SqliteConnection, case: &Case) -> Option<bool> {
    use diesel::{QueryDsl as _, RunQueryDsl as _};

    diesel::delete(t::table).execute(connection).ok()?;
    diesel::insert_into(t::table)
        .values(case.row.values())
        .execute(connection)
        .ok()?;
    let kept: i64 = t::table
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
            &case.filter.render(Engine::Sqlite),
        ))
        .count()
        .get_result(connection)
        .ok()?;
    Some(kept == 1)
}

/// Draw a case, filter the row with it in SQLite and in subql serving the
/// SQLite backend, and require the two to agree wherever subql serves it.
///
/// Contract: a panic is a filter subql serves with the wrong answer.
#[cfg(feature = "pg-sqlite-emu")]
pub fn harness_predicate_verdict_sqlite(data: &[u8]) {
    use core::cell::RefCell;
    use diesel::{Connection as _, RunQueryDsl as _};

    std::thread_local! {
        static CATALOG: ParserDB = catalog::<sqlparser::dialect::SQLiteDialect>(Engine::Sqlite);
        static SQLITE: RefCell<diesel::SqliteConnection> = RefCell::new({
            let mut connection = diesel::SqliteConnection::establish(":memory:")
                .expect("an in-memory SQLite database opens");
            // DDL, which the typed DSL does not build.
            diesel::sql_query(ddl(Engine::Sqlite))
                .execute(&mut connection)
                .expect("the case table is created");
            connection
        });
    }

    let mut u = Unstructured::new(data);
    let Ok(case) = Case::arbitrary(&mut u) else {
        return;
    };
    let database = CATALOG.with(Clone::clone);
    let Some(served) = subql_selects::<crate::backend::SQLite>(&case, Engine::Sqlite, database)
    else {
        return;
    };
    let Some(kept) = SQLITE.with(|connection| sqlite_selects(&mut connection.borrow_mut(), &case))
    else {
        return;
    };
    assert_eq!(
        served,
        kept,
        "subql serves this filter in process with a different answer from SQLite:\n{}",
        case.reproduction(Engine::Sqlite)
    );
}
