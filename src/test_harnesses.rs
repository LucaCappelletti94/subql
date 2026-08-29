//! Shared fuzz harness functions.
//!
//! Each `harness_*` function takes raw bytes and exercises a library subsystem.
//! The contract: errors are fine, **panics are bugs**.
//!
//! This module is only compiled under `#[cfg(feature = "testing")]`.

// Clippy allows scoped to this fuzz-harness module. These lints flag
// stylistic patterns (manual let-else, items after statements, doc
// paragraph length, identical match arms, by-value generated test
// data, and `BTreeMap` contains_key+insert) that are intentional or
// load-bearing for readability in arbitrary-driven test code. The
// module is feature-gated behind `testing` and is not part of the
// production lib build.
#![allow(
    clippy::manual_let_else,
    clippy::too_long_first_doc_paragraph,
    clippy::items_after_statements,
    clippy::needless_pass_by_value,
    clippy::map_entry,
    clippy::match_same_arms
)]

use std::collections::BTreeMap;

use arbitrary::{Arbitrary, Unstructured};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::{CdcEvent, Postgres, RowKind, Value};
use crate::compiler::bytecode::{BytecodeProgram, Instruction};
use crate::compiler::canonicalize::{hash_sql, normalize_sql};
use crate::compiler::parser::parse_and_compile;
use crate::compiler::vm::Vm;
use crate::persistence::codec;
use crate::persistence::shard::{deserialize_shard, ShardPayload};
use crate::runtime::aggregate::AggAccumulator;
use crate::testing::TestEvent;
use crate::wal::{parse_maxwell, parse_wal2json_v1, parse_wal2json_v2};
use crate::{
    catalog_helpers, AggSpec, AggValue, DefaultIds, RegisterError, SubscriptionEngine,
    SubscriptionRequest,
};
use crate::{Registered, Tier};
use pg_walstream::{Lsn, PgOutputDecoder};

/// Build a permissive fuzz schema as a [`ParserDB`].
///
/// Declares an `orders` table with column names the SQL fuzzer commonly
/// produces (`amount`, `status`, `id`, plus generic `c0`-`c15`). SQL
/// referencing columns or tables absent from this fixture fails SQL
/// resolution, which is fine for crash testing.
#[must_use]
pub fn fuzz_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (\
             id INT PRIMARY KEY, amount INT, status TEXT, \
             c0 INT, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, \
             c8 INT, c9 INT, c10 INT, c11 INT, c12 INT, c13 INT, c14 INT, c15 INT \
         );",
    )
    .expect("fuzz fixture DDL parses")
}

/// Generate a [`Value<Postgres>`] from fuzzer-controlled bytes.
///
/// Six shapes mirror what the retired `Cell` enum carried plus the
/// three-valued `Missing` / `Null` semantics: `Missing`, `Null`,
/// `Bool`, `Int`, `Float`, `String`. The three-valued shapes exercise
/// the VM's `IS NULL` / arithmetic-against-null paths.
fn arb_value(u: &mut Unstructured<'_>) -> arbitrary::Result<Value<Postgres>> {
    match u.int_in_range(0u8..=5)? {
        0 => Ok(Value::Null),
        1 => Ok(Value::Missing),
        2 => Ok(Value::Bool(bool::arbitrary(u)?)),
        3 => Ok(Value::Int(i64::arbitrary(u)?)),
        4 => Ok(Value::Float(f64::arbitrary(u)?)),
        _ => {
            let len = u.int_in_range(0usize..=64)?;
            let bytes: Vec<u8> = (0..len)
                .map(|_| u.arbitrary())
                .collect::<arbitrary::Result<_>>()?;
            Ok(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        }
    }
}

/// Generate an [`Instruction<Postgres>`] from fuzzer-controlled bytes.
fn arb_instruction(u: &mut Unstructured<'_>) -> arbitrary::Result<Instruction<Postgres>> {
    match u.int_in_range(0u8..=23)? {
        0 => Ok(Instruction::PushLiteral(arb_value(u)?)),
        1 => Ok(Instruction::LoadColumn(u.int_in_range(0u16..=63)?)),
        2 => Ok(Instruction::Equal),
        3 => Ok(Instruction::NotEqual),
        4 => Ok(Instruction::LessThan),
        5 => Ok(Instruction::LessThanOrEqual),
        6 => Ok(Instruction::GreaterThan),
        7 => Ok(Instruction::GreaterThanOrEqual),
        8 => Ok(Instruction::IsNull),
        9 => Ok(Instruction::IsNotNull),
        10 => Ok(Instruction::And),
        11 => Ok(Instruction::Or),
        12 => Ok(Instruction::Not),
        13 => Ok(Instruction::Add),
        14 => Ok(Instruction::Subtract),
        15 => Ok(Instruction::Multiply),
        16 => Ok(Instruction::Divide),
        17 => Ok(Instruction::Modulo),
        18 => Ok(Instruction::Negate),
        19 => {
            let len = u.int_in_range(0usize..=8)?;
            let list: Vec<Value<Postgres>> = (0..len)
                .map(|_| arb_value(u))
                .collect::<arbitrary::Result<_>>()?;
            Ok(Instruction::In(list))
        }
        20 => Ok(Instruction::Between),
        21 => Ok(Instruction::Like {
            case_sensitive: bool::arbitrary(u)?,
        }),
        // Jump instructions with bounded offsets (0..=31 to stay within any reasonable program)
        22 => Ok(Instruction::JumpIfFalse(u.int_in_range(0usize..=31)?)),
        _ => Ok(Instruction::JumpIfTrue(u.int_in_range(0usize..=31)?)),
    }
}

// ---------------------------------------------------------------------------
// Harness functions
// ---------------------------------------------------------------------------

/// Parse SQL using PostgreSqlDialect, the dialect subql's CDC pipeline
/// targets.
///
/// We deliberately do NOT also fuzz with `GenericDialect`: it accepts
/// many more parse paths (PG/MySQL/MSSQL tokens all valid), making it
/// prone to deep backtracking on adversarial input. That is an upstream
/// sqlparser perf characteristic, not subql code, and previous fuzz
/// timeouts traced back to it.
pub fn harness_parse_sql(data: &[u8]) {
    // Real callers pass valid UTF-8 SQL. `from_utf8_lossy` would
    // introduce U+FFFD bytes that drive sqlparser into pathological
    // backtracking, code paths unreachable in production.
    let Ok(sql) = core::str::from_utf8(data) else {
        return;
    };
    let catalog = fuzz_catalog();
    let pg = PostgreSqlDialect {};

    let _ = parse_and_compile::<Postgres, _>(sql, &pg, &catalog);
}

/// Generate random bytecode + row and evaluate with the VM.
pub fn harness_vm_eval(data: &[u8]) {
    let mut u = Unstructured::new(data);

    // Generate 1-32 instructions
    let Ok(n_instr) = u.int_in_range(1usize..=32) else {
        return;
    };
    let instructions: Vec<Instruction<Postgres>> = match (0..n_instr)
        .map(|_| arb_instruction(&mut u))
        .collect::<arbitrary::Result<_>>()
    {
        Ok(v) => v,
        Err(_) => return,
    };

    // Generate 0-16 row cells
    let Ok(n_cells) = u.int_in_range(0usize..=16) else {
        return;
    };
    let cells: Vec<Value<Postgres>> = match (0..n_cells)
        .map(|_| arb_value(&mut u))
        .collect::<arbitrary::Result<_>>()
    {
        Ok(v) => v,
        Err(_) => return,
    };

    let program = BytecodeProgram::new(instructions);
    let event = TestEvent::<Postgres>::insert(0, cells);

    let mut vm = Vm::<Postgres>::new();
    let _ = vm.eval(&program, &event, RowKind::New, &fuzz_catalog());
}

/// Feed raw bytes to shard deserialization.
pub fn harness_deserialize_shard(data: &[u8]) {
    let catalog = fuzz_catalog();
    let _ = deserialize_shard::<DefaultIds, _>(data, &catalog);
}

/// Normalize and hash SQL, asserting determinism. PostgreSqlDialect only.
/// See `harness_parse_sql` for the rationale.
pub fn harness_canonicalize(data: &[u8]) {
    let Ok(sql) = core::str::from_utf8(data) else {
        return;
    };
    let dialect = &PostgreSqlDialect {} as &dyn sqlparser::dialect::Dialect;
    if let Ok(normalized) = normalize_sql(sql, dialect) {
        let h1 = hash_sql(&normalized);
        let h2 = hash_sql(&normalized);
        assert_eq!(h1, h2, "hash_sql is not deterministic");
    }
}

/// Try decoding raw bytes as different types.
pub fn harness_codec_decode(data: &[u8]) {
    let _ = codec::decode::<ShardPayload<DefaultIds>>(data);
    let _ = codec::decode::<Vec<u8>>(data);
    let _ = codec::decode::<String>(data);
}

/// Generate a JSON scalar from fuzzer-controlled bytes.
#[derive(Debug, Arbitrary)]
enum JsonScalar {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
}

impl JsonScalar {
    fn into_value(self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(b) => serde_json::Value::Bool(b),
            Self::I64(i) => serde_json::Value::from(i),
            Self::F64(f) if f.is_finite() => serde_json::Number::from_f64(f)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            Self::F64(_) => serde_json::Value::Null,
            Self::Str(s) => serde_json::Value::String(s),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct V1OldKeys {
    keynames: Vec<String>,
    keytypes: Vec<String>,
    keyvalues: Vec<JsonScalar>,
}

#[derive(Debug, Arbitrary)]
struct V1Change {
    kind: String,
    schema: String,
    table: String,
    columnnames: Vec<String>,
    columntypes: Vec<String>,
    columnvalues: Vec<JsonScalar>,
    oldkeys: Option<V1OldKeys>,
}

#[derive(Debug, Arbitrary)]
struct V1Envelope {
    xid: Option<u64>,
    change: Vec<V1Change>,
}

#[derive(Debug, Arbitrary)]
struct V2Column {
    name: String,
    type_name: String,
    value: JsonScalar,
}

#[derive(Debug, Arbitrary)]
struct V2PkColumn {
    name: String,
    type_name: String,
}

#[derive(Debug, Arbitrary)]
struct V2Envelope {
    action: String,
    schema: Option<String>,
    table: Option<String>,
    columns: Option<Vec<V2Column>>,
    identity: Option<Vec<V2Column>>,
    pk: Option<Vec<V2PkColumn>>,
}

#[derive(Debug, Arbitrary)]
struct MaxwellEnvelope {
    database: String,
    table: String,
    event_type: String,
    data: Option<Vec<(String, JsonScalar)>>,
    old: Option<Vec<(String, JsonScalar)>>,
    primary_key_columns: Option<Vec<String>>,
}

#[derive(Debug, Arbitrary)]
enum WalJsonInput {
    V1(V1Envelope),
    V2(V2Envelope),
    Maxwell(MaxwellEnvelope),
}

/// Build a JSON object from key/value pairs, using a sorted `BTreeMap`-like
/// `Map` to give deterministic ordering and to allow repeated keys (last
/// one wins, mirroring serde_json's own behaviour).
fn obj(pairs: Vec<(&str, serde_json::Value)>) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for (k, v) in pairs {
        m.insert(k.to_owned(), v);
    }
    serde_json::Value::Object(m)
}

fn arr(items: impl IntoIterator<Item = serde_json::Value>) -> serde_json::Value {
    serde_json::Value::Array(items.into_iter().collect())
}

impl V1Envelope {
    fn into_json(self) -> serde_json::Value {
        let changes: Vec<serde_json::Value> =
            self.change.into_iter().map(V1Change::into_json).collect();
        let mut pairs: Vec<(&str, serde_json::Value)> = Vec::new();
        if let Some(xid) = self.xid {
            pairs.push(("xid", serde_json::Value::from(xid)));
        }
        pairs.push(("change", serde_json::Value::Array(changes)));
        obj(pairs)
    }
}

impl V1Change {
    fn into_json(self) -> serde_json::Value {
        let mut pairs = vec![
            ("kind", serde_json::Value::String(self.kind)),
            ("schema", serde_json::Value::String(self.schema)),
            ("table", serde_json::Value::String(self.table)),
            (
                "columnnames",
                arr(self.columnnames.into_iter().map(serde_json::Value::String)),
            ),
            (
                "columntypes",
                arr(self.columntypes.into_iter().map(serde_json::Value::String)),
            ),
            (
                "columnvalues",
                arr(self.columnvalues.into_iter().map(JsonScalar::into_value)),
            ),
        ];
        if let Some(old) = self.oldkeys {
            pairs.push(("oldkeys", old.into_json()));
        }
        obj(pairs)
    }
}

impl V1OldKeys {
    fn into_json(self) -> serde_json::Value {
        obj(vec![
            (
                "keynames",
                arr(self.keynames.into_iter().map(serde_json::Value::String)),
            ),
            (
                "keytypes",
                arr(self.keytypes.into_iter().map(serde_json::Value::String)),
            ),
            (
                "keyvalues",
                arr(self.keyvalues.into_iter().map(JsonScalar::into_value)),
            ),
        ])
    }
}

impl V2Column {
    fn into_json(self) -> serde_json::Value {
        obj(vec![
            ("name", serde_json::Value::String(self.name)),
            ("type", serde_json::Value::String(self.type_name)),
            ("value", self.value.into_value()),
        ])
    }
}

impl V2PkColumn {
    fn into_json(self) -> serde_json::Value {
        obj(vec![
            ("name", serde_json::Value::String(self.name)),
            ("type", serde_json::Value::String(self.type_name)),
        ])
    }
}

impl V2Envelope {
    fn into_json(self) -> serde_json::Value {
        let mut pairs = vec![("action", serde_json::Value::String(self.action))];
        if let Some(s) = self.schema {
            pairs.push(("schema", serde_json::Value::String(s)));
        }
        if let Some(t) = self.table {
            pairs.push(("table", serde_json::Value::String(t)));
        }
        if let Some(cs) = self.columns {
            pairs.push(("columns", arr(cs.into_iter().map(V2Column::into_json))));
        }
        if let Some(cs) = self.identity {
            pairs.push(("identity", arr(cs.into_iter().map(V2Column::into_json))));
        }
        if let Some(pk) = self.pk {
            pairs.push(("pk", arr(pk.into_iter().map(V2PkColumn::into_json))));
        }
        obj(pairs)
    }
}

impl MaxwellEnvelope {
    fn into_json(self) -> serde_json::Value {
        fn map_obj(entries: Vec<(String, JsonScalar)>) -> serde_json::Value {
            let mut m = serde_json::Map::new();
            for (k, v) in entries {
                m.insert(k, v.into_value());
            }
            serde_json::Value::Object(m)
        }
        let mut pairs = vec![
            ("database", serde_json::Value::String(self.database)),
            ("table", serde_json::Value::String(self.table)),
            ("type", serde_json::Value::String(self.event_type)),
        ];
        if let Some(d) = self.data {
            pairs.push(("data", map_obj(d)));
        }
        if let Some(o) = self.old {
            pairs.push(("old", map_obj(o)));
        }
        if let Some(pk) = self.primary_key_columns {
            pairs.push((
                "primary_key_columns",
                arr(pk.into_iter().map(serde_json::Value::String)),
            ));
        }
        obj(pairs)
    }
}

/// Drive arbitrary-shaped JSON envelopes through the wal2json v1, wal2json
/// v2, and Maxwell parsers, skipping raw-bytes JSON tokenisation (which
/// is serde_json's job, already heavily fuzzed upstream) and exercising
/// subql's post-parse semantic-validation layer: column-count vs
/// relation-cache mismatches, JSON-value-to-typed coercion, sparse-old-row
/// handling, PK extraction, action-tag dispatch.
///
/// Contract: panics are bugs. Any `Err(WalParseError)` is fine.
pub fn harness_wal_json_postparse(data: &[u8]) {
    let mut u = Unstructured::new(data);
    let Ok(input) = WalJsonInput::arbitrary(&mut u) else {
        return;
    };

    let (json_value, parser_kind) = match input {
        WalJsonInput::V1(v) => (v.into_json(), 0u8),
        WalJsonInput::V2(v) => (v.into_json(), 1u8),
        WalJsonInput::Maxwell(v) => (v.into_json(), 2u8),
    };

    let Ok(bytes) = serde_json::to_vec(&json_value) else {
        return;
    };

    match parser_kind {
        0 => {
            let _ = parse_wal2json_v1(&bytes);
        }
        1 => {
            let _ = parse_wal2json_v2(&bytes);
        }
        _ => {
            let _ = parse_maxwell(&bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate-consistency harness
// ---------------------------------------------------------------------------

/// Mutation operation against the virtual `orders` table used by
/// [`harness_aggregate_consistency`]. `Truncate` is absent because the engine
/// answers it from the held totals rather than from row images, which the unit
/// tests in `tests/it/aggregate_totals.rs` cover directly.
///
/// `amount` is bounded to `i16` (not `i32`) so that squared values stay
/// well inside f64's exact-integer range (2^53). Streaming variance over
/// widely varying magnitudes hits unavoidable catastrophic-cancellation
/// noise when squared values approach or exceed 2^53, and that noise is
/// not a routing or correctness bug in the engine. The harness's purpose
/// is to catch routing/semantic drift, so the bound removes the
/// f64-precision confounder.
#[derive(Debug, Arbitrary)]
enum AggOp {
    Insert {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Update {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Delete {
        id: u8,
    },
}

/// In-virtual-table representation of one row.
#[derive(Clone, Debug)]
struct VirtRow {
    amount: Option<i64>,
    status: Option<String>,
}

impl VirtRow {
    fn from_op(amount: Option<i16>, status: Option<u8>) -> Self {
        Self {
            amount: amount.map(i64::from),
            status: status.map(|b| match b % 4 {
                0 => "open".into(),
                1 => "closed".into(),
                2 => "shipped".into(),
                _ => "pending".into(),
            }),
        }
    }
}

/// Build the 3-cell row image `(id, amount, status)` matching
/// `agg_catalog()`'s schema, using `Value<Postgres>` variants.
fn agg_row_values(id: i64, row: &VirtRow) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        row.amount.map_or(Value::Null, Value::Int),
        row.status
            .as_deref()
            .map_or(Value::Null, |s| Value::String(s.to_string())),
    ]
}

/// Build the `agg_catalog()` `ParserDB` once: three columns, single-
/// column INT PK. Distinct from `fuzz_catalog()` so the column-id
/// mapping is predictable (id=0, amount=1, status=2).
fn agg_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    )
    .expect("agg fuzz fixture DDL parses")
}

/// Pre-built engine + table metadata shared across every iteration of
/// [`harness_aggregate_consistency`] within a fuzz worker. Reusing the
/// engine across iterations is the load-bearing reason this exists: the
/// harness would otherwise drop and re-create a `SubscriptionEngine` on
/// every call, and under ASAN that allocator churn drifts the worker's
/// RSS past libFuzzer's default limit after tens of thousands of
/// iterations even though no individual iteration leaks.
///
/// Cargo-fuzz runs single-threaded, so the `thread_local!` cell is
/// shared by every iteration on the only worker thread. Re-entrancy is
/// not possible.
struct AggEngineCell {
    engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
    table_id: crate::TableId,
    pk_col: crate::ColumnId,
    /// Every registered aggregate in consumer order, with the id the engine
    /// files its running total under and the function that total maintains.
    totals: Vec<(u64, crate::SubscriptionId, AggSpec)>,
}

impl AggEngineCell {
    fn new() -> Self {
        let database = agg_catalog();
        let table_id = catalog_helpers::table_id(&database, "orders")
            .expect("agg_catalog must expose an `orders` table");
        let pk_col = catalog_helpers::column_id(&database, table_id, "id")
            .expect("agg_catalog `orders` must expose an `id` column");
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        // One consumer per flavor. The four VAR/STDDEV flavors share one
        // kernel and hold identical running numbers, so registering all four
        // catches per-variant routing and hash-collision bugs as well as
        // kernel correctness.
        let mut totals = Vec::new();
        for (cid, sql) in [
            (1_u64, "SELECT COUNT(*) FROM orders"),
            (2_u64, "SELECT SUM(amount) FROM orders"),
            (3_u64, "SELECT VAR_POP(amount) FROM orders"),
            (4_u64, "SELECT VAR_SAMP(amount) FROM orders"),
            (5_u64, "SELECT STDDEV_POP(amount) FROM orders"),
            (6_u64, "SELECT STDDEV_SAMP(amount) FROM orders"),
            (7_u64, "SELECT AVG(amount) FROM orders"),
        ] {
            let registered = engine
                .register(SubscriptionRequest::<DefaultIds>::new(cid, sql))
                .expect("registering an aggregate consumer should succeed against agg_catalog");
            let Tier::InProcess(served) = &registered.tier else {
                panic!("an aggregate the engine maintains registers in process")
            };
            let spec = served
                .aggregate_spec()
                .expect("an aggregate registration carries its spec")
                .clone();
            totals.push((cid, registered.subscription_id, spec));
        }
        Self {
            engine,
            table_id,
            pk_col,
            totals,
        }
    }
}

std::thread_local! {
    static AGG_ENGINE: std::cell::RefCell<AggEngineCell> =
        std::cell::RefCell::new(AggEngineCell::new());
}

/// Every aggregate flavor spanning both families (in-process delta and
/// captured `MIN`/`MAX`), used by the RLS-guard invariant in
/// [`harness_aggregate_consistency`].
const RLS_GUARD_FLAVORS: &[&str] = &[
    "SELECT COUNT(*) FROM orders",
    "SELECT COUNT(amount) FROM orders",
    "SELECT SUM(amount) FROM orders",
    "SELECT AVG(amount) FROM orders",
    "SELECT VAR_POP(amount) FROM orders",
    "SELECT VAR_SAMP(amount) FROM orders",
    "SELECT STDDEV_POP(amount) FROM orders",
    "SELECT STDDEV_SAMP(amount) FROM orders",
    "SELECT MIN(amount) FROM orders",
    "SELECT MAX(amount) FROM orders",
];

/// `agg_catalog()`'s `orders` table with row-level security enabled, so
/// [`catalog_helpers::table_has_rls`] returns true for it.
fn rls_agg_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT); \
         ALTER TABLE orders ENABLE ROW LEVEL SECURITY;",
    )
    .expect("rls agg fuzz fixture DDL parses")
}

/// Pre-built reexec wrappers over an RLS and a non-RLS `orders` catalog,
/// shared across iterations of [`harness_aggregate_consistency`] so the
/// invariant check adds no per-call engine allocation (same reasoning as
/// [`AggEngineCell`]).
struct RlsGuardCell {
    rls_engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
    plain_engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>,
    rls_table_id: crate::TableId,
}

impl RlsGuardCell {
    fn new() -> Self {
        let rls_db = rls_agg_catalog();
        let rls_table_id = catalog_helpers::table_id(&rls_db, "orders")
            .expect("rls_agg_catalog must expose an `orders` table");
        let rls_engine = SubscriptionEngine::new(rls_db, PostgreSqlDialect {});
        let plain_engine = SubscriptionEngine::new(agg_catalog(), PostgreSqlDialect {});
        Self {
            rls_engine,
            plain_engine,
            rls_table_id,
        }
    }

    /// Assert the RLS guard: every aggregate flavor is rejected on the
    /// RLS table with `AggregatorOnRlsTable` (never an in-process tier),
    /// while `plain_flavor` is accepted on the non-RLS table. RLS
    /// registration errors before mutating engine state, so looping all
    /// flavors adds no state growth. The non-RLS acceptance is
    /// register-then-unregister so the plain engine stays bounded.
    fn check(&mut self, plain_flavor: &str) {
        let consumer = 1u64;
        for flavor in RLS_GUARD_FLAVORS {
            match self
                .rls_engine
                .register(SubscriptionRequest::<DefaultIds>::new(consumer, *flavor))
            {
                Err(RegisterError::AggregatorOnRlsTable { table_id }) => {
                    assert_eq!(
                        table_id, self.rls_table_id,
                        "`{flavor}` rejected for the wrong table id"
                    );
                }
                other => panic!("`{flavor}` on RLS table must be rejected, got {other:?}"),
            }
        }
        match self
            .plain_engine
            .register(SubscriptionRequest::<DefaultIds>::new(
                consumer,
                plain_flavor,
            )) {
            Ok(Registered {
                tier: Tier::InProcess(_),
                ..
            }) => {
                let _ = self.plain_engine.unregister_query(consumer, plain_flavor);
            }
            Ok(Registered {
                subscription_id, ..
            }) => {
                assert!(self.plain_engine.unregister_reread(subscription_id));
            }
            Err(e) => panic!("`{plain_flavor}` without RLS must be accepted, got Err({e:?})"),
        }
    }
}

std::thread_local! {
    static RLS_GUARD: std::cell::RefCell<RlsGuardCell> =
        std::cell::RefCell::new(RlsGuardCell::new());
}

/// Bootstrap seed components over the virtual table, mirroring what
/// [`crate::AggregateBootstrap`] projects (`c0`, `c1`, `c2`).
struct AggComponents {
    count_star: i64,
    count_col: i64,
    sum: i64,
    sum_sq: i64,
    numeric: i64,
}

impl AggComponents {
    // Amounts originate from `i16`, so the running sums stay well inside
    // f64's exact-integer range; the precision-loss lint is theoretical.
    #[allow(clippy::cast_precision_loss)]
    const fn sum_f64(&self) -> f64 {
        self.sum as f64
    }
    #[allow(clippy::cast_precision_loss)]
    const fn sum_sq_f64(&self) -> f64 {
        self.sum_sq as f64
    }
    /// SUM / SUM(sq) components read back as NULL when no non-NULL row matched.
    const fn sum_cell(&self) -> Value<Postgres> {
        if self.numeric == 0 {
            Value::Null
        } else {
            Value::Int(self.sum)
        }
    }
    const fn sum_sq_cell(&self) -> Value<Postgres> {
        if self.numeric == 0 {
            Value::Null
        } else {
            Value::Int(self.sum_sq)
        }
    }
}

fn agg_components(virt: &BTreeMap<i64, VirtRow>) -> AggComponents {
    let mut c = AggComponents {
        count_star: i64::try_from(virt.len()).unwrap_or(i64::MAX),
        count_col: 0,
        sum: 0,
        sum_sq: 0,
        numeric: 0,
    };
    for a in virt.values().filter_map(|r| r.amount) {
        c.count_col += 1;
        c.numeric += 1;
        c.sum += a;
        c.sum_sq += a * a;
    }
    c
}

/// Textbook aggregate value over the components, using the same formulas
/// as [`AggAccumulator::value`], so seeding matches it exactly.
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)]
fn oracle_agg_value(spec: &AggSpec, c: &AggComponents) -> AggValue {
    let n = c.numeric as f64;
    let sum = c.sum as f64;
    let sum_sq = c.sum_sq as f64;
    let var_pop = (c.numeric > 0).then(|| sum_sq / n - (sum / n).powi(2));
    let var_samp = (c.numeric >= 2).then(|| (sum_sq - sum.powi(2) / n) / (n - 1.0));
    match spec {
        AggSpec::CountStar => AggValue::Count(c.count_star),
        AggSpec::CountColumn { .. } => AggValue::Count(c.count_col),
        AggSpec::Sum { .. } => AggValue::Sum(sum),
        AggSpec::Avg { .. } => AggValue::Real((c.numeric > 0).then(|| sum / n)),
        AggSpec::VarPop { .. } => AggValue::Real(var_pop),
        AggSpec::VarSamp { .. } => AggValue::Real(var_samp),
        AggSpec::StddevPop { .. } => AggValue::Real(var_pop.map(f64::sqrt)),
        AggSpec::StddevSamp { .. } => AggValue::Real(var_samp.map(f64::sqrt)),
    }
}

/// Whether a value the engine folded event by event agrees with one
/// recomputed from scratch.
///
/// Tolerance scales with the components in play. A variance is a difference of
/// two large numbers, so where they nearly cancel the value keeps far fewer
/// significant digits than its inputs, and a standard deviation takes a square
/// root of that. Still orders of magnitude tighter than any dropped or
/// misrouted delta, which moves these values by whole units, because the
/// amounts driving them are `i16`.
#[allow(clippy::cast_precision_loss)]
fn agg_values_agree(engine: AggValue, oracle: AggValue, c: &AggComponents) -> bool {
    match (engine, oracle) {
        (AggValue::Count(a), AggValue::Count(b)) => a == b,
        (AggValue::Sum(a), AggValue::Sum(b)) => {
            (a - b).abs() <= 1e-9_f64.max(c.sum_f64().abs() * 1e-12)
        }
        (AggValue::Real(None), AggValue::Real(None)) => true,
        (AggValue::Real(Some(a)), AggValue::Real(Some(b))) => {
            (a - b).abs() <= 1e-3_f64.max(c.sum_sq_f64().abs().sqrt() * 1e-5)
        }
        _ => false,
    }
}

/// The bootstrap component row for `spec` over `c`, in the column order
/// [`crate::AggregateBootstrap`] projects.
fn seed_row(spec: &AggSpec, c: &AggComponents) -> Vec<Value<Postgres>> {
    match spec {
        AggSpec::CountStar => alloc::vec![Value::Int(c.count_star)],
        AggSpec::CountColumn { .. } => alloc::vec![Value::Int(c.count_col)],
        AggSpec::Sum { .. } => alloc::vec![c.sum_cell()],
        AggSpec::Avg { .. } => alloc::vec![c.sum_cell(), Value::Int(c.numeric)],
        AggSpec::VarPop { .. }
        | AggSpec::VarSamp { .. }
        | AggSpec::StddevPop { .. }
        | AggSpec::StddevSamp { .. } => {
            alloc::vec![c.sum_cell(), c.sum_sq_cell(), Value::Int(c.numeric)]
        }
    }
}

/// Seeding from the bootstrap component row must equal a direct recompute
/// for every `AggSpec`. Exact: seed and oracle share f64 inputs and math.
fn assert_seed_matches_oracle(c: &AggComponents) {
    let specs = [
        AggSpec::CountStar,
        AggSpec::CountColumn { column: 1 },
        AggSpec::Sum { column: 1 },
        AggSpec::Avg { column: 1 },
        AggSpec::VarPop { column: 1 },
        AggSpec::VarSamp { column: 1 },
        AggSpec::StddevPop { column: 1 },
        AggSpec::StddevSamp { column: 1 },
    ];
    for spec in specs {
        assert_eq!(
            AggAccumulator::seed_from_row(&spec, &seed_row(&spec, c)).value(),
            oracle_agg_value(&spec, c),
            "seed decode drift for {spec:?}",
        );
    }
}

/// Drive an arbitrary sequence of insert/update/delete operations against a
/// fixed agg-only consumer set and assert that the value the engine holds for
/// every subscription equals a from-scratch oracle after every event.
///
/// Two properties. The held value equals the oracle after each event, and a
/// value that moved was reported. The second is the one a silent engine
/// breaks, and it holds because a value cannot move without a non-zero delta.
///
/// Covers `COUNT(*)`, `SUM`, `AVG`, and all four of
/// `VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`, one consumer each.
///
/// Contract: panics are bugs. Assertion failures are bugs.
#[allow(clippy::too_many_lines)]
pub fn harness_aggregate_consistency(data: &[u8]) {
    // RLS guard invariant, independent of the ops stream below so it does
    // not perturb the aggregate-consistency coverage: registering any
    // aggregate flavor against an RLS-marked table is rejected with
    // AggregatorOnRlsTable (never an in-process tier), while the flavor
    // chosen from the raw input is accepted on the non-RLS table.
    RLS_GUARD.with(|cell| {
        let idx = usize::from(data.first().copied().unwrap_or(0)) % RLS_GUARD_FLAVORS.len();
        cell.borrow_mut().check(RLS_GUARD_FLAVORS[idx]);
    });

    let mut u = Unstructured::new(data);
    type PrepopRow = (u8, Option<i16>, Option<u8>);
    let Ok((prepop, ops)): arbitrary::Result<(Vec<PrepopRow>, Vec<AggOp>)> = (|| {
        let k = u.int_in_range(0usize..=16)?;
        let prepop = (0..k)
            .map(|_| Ok((u.arbitrary()?, u.arbitrary()?, u.arbitrary()?)))
            .collect::<arbitrary::Result<Vec<PrepopRow>>>()?;
        let n = u.int_in_range(0usize..=64)?;
        let ops = (0..n)
            .map(|_| AggOp::arbitrary(&mut u))
            .collect::<arbitrary::Result<Vec<AggOp>>>()?;
        Ok((prepop, ops))
    })() else {
        return;
    };

    AGG_ENGINE.with(|cell| {
        let mut cell = cell.borrow_mut();
        let AggEngineCell {
            engine,
            table_id,
            pk_col,
            totals,
        } = &mut *cell;
        let table_id = *table_id;
        let pk_col = *pk_col;

        // Virtual table (id -> row), the source of truth for the oracle.
        // Pre-populate an arbitrary S0 so the accumulators start from a
        // bootstrap seed over a non-empty table, not from empty.
        let mut virt: BTreeMap<i64, VirtRow> = BTreeMap::new();
        for (id, amount, status) in prepop {
            virt.entry(i64::from(id))
                .or_insert_with(|| VirtRow::from_op(amount, status));
        }
        let s0 = agg_components(&virt);

        // Exercise the seed decode over the arbitrary S0: seeding from the
        // bootstrap component row must equal a direct recompute.
        assert_seed_matches_oracle(&s0);

        // The engine holds the running values now, so each iteration resets
        // every total and seeds it from S0. The cell is reused across
        // iterations, so without the reset the second iteration would be
        // refused as already seeded.
        for (consumer, subscription, spec) in totals.iter() {
            assert!(
                engine.reset_aggregate_value(*subscription),
                "subscription {subscription} should be a live aggregate",
            );
            let seeded = crate::Install::install(
                engine,
                *subscription,
                crate::AggregateSeedInstall {
                    rows: vec![seed_row(spec, &s0)],
                    read_at: None,
                },
            )
            .expect("a seed with nothing folded against it lands");
            assert_eq!(seeded.len(), 1, "one ungrouped opening value");
            assert_eq!(seeded[0].subscription, *subscription);
            assert_eq!(seeded[0].consumer, *consumer);
            assert_eq!(seeded[0].group, None);
            assert_eq!(
                seeded[0].change,
                crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                    oracle_agg_value(spec, &s0),
                )),
                "seed value drift for {spec:?}",
            );
        }

        // The value each subscription held before the event about to be
        // dispatched, so "a value that moved was reported" can be checked.
        let mut previous: Vec<AggValue> = totals
            .iter()
            .map(|(_, _, spec)| oracle_agg_value(spec, &s0))
            .collect();

        for op in ops {
            let (event, mutated): (Option<TestEvent<Postgres>>, bool) = match op {
                AggOp::Insert { id, amount, status } => {
                    let id = i64::from(id);
                    if virt.contains_key(&id) {
                        (None, false)
                    } else {
                        let row = VirtRow::from_op(amount, status);
                        let values = agg_row_values(id, &row);
                        virt.insert(id, row);
                        let event = TestEvent::<Postgres>::insert(table_id, values)
                            .with_pk_columns([pk_col]);
                        (Some(event), true)
                    }
                }
                AggOp::Update { id, amount, status } => {
                    let id = i64::from(id);
                    let Some(old) = virt.get(&id).cloned() else {
                        continue;
                    };
                    let new_row = VirtRow::from_op(amount, status);
                    let old_values = agg_row_values(id, &old);
                    let new_values = agg_row_values(id, &new_row);
                    virt.insert(id, new_row);
                    let event = TestEvent::<Postgres>::update(table_id, old_values, new_values)
                        .with_pk_columns([pk_col]);
                    (Some(event), true)
                }
                AggOp::Delete { id } => {
                    let id = i64::from(id);
                    let Some(old) = virt.remove(&id) else {
                        continue;
                    };
                    let old_values = agg_row_values(id, &old);
                    let event = TestEvent::<Postgres>::delete(table_id, old_values)
                        .with_pk_columns([pk_col]);
                    (Some(event), true)
                }
            };

            if !mutated {
                continue;
            }
            let Some(event) = event else { continue };

            let updates = match engine.aggregate_updates(&event) {
                Ok(u) => u,
                Err(_) => return,
            };
            let reported: BTreeMap<crate::SubscriptionId, AggValue> = updates
                .iter()
                .map(|u| {
                    let crate::AggregateValueChange::Set(crate::AggregateResultValue::Folded(
                        value,
                    )) = &u.change
                    else {
                        panic!("ungrouped aggregate cannot remove a group")
                    };
                    (u.subscription, *value)
                })
                .collect();

            let now = agg_components(&virt);
            for (slot, (cid, subscription, spec)) in totals.iter().enumerate() {
                let oracle = oracle_agg_value(spec, &now);
                let held = engine
                    .current_aggregate_value(*subscription)
                    .expect("a seeded aggregate holds a value");
                assert!(
                    agg_values_agree(held, oracle, &now),
                    "consumer {cid} value drift: engine={held:?} oracle={oracle:?}",
                );
                match reported.get(subscription) {
                    Some(&value) => assert_eq!(
                        value, held,
                        "consumer {cid} reported a value it does not hold",
                    ),
                    None => assert!(
                        previous[slot] == oracle,
                        "consumer {cid} moved from {:?} to {oracle:?} without reporting",
                        previous[slot],
                    ),
                }
                previous[slot] = oracle;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Snapshot + restore round-trip harness
// ---------------------------------------------------------------------------

/// Fixed pool of `SELECT *` queries the snapshot/restore harness picks
/// from. All target the same `agg_catalog()` `orders` table so
/// registrations always succeed. The fuzzer controls which subset of
/// the pool ends up registered and in what order.
const SNAPSHOT_REGISTER_SQLS: &[&str] = &[
    "SELECT * FROM orders WHERE amount > 100",
    "SELECT * FROM orders WHERE status = 'open'",
    "SELECT * FROM orders WHERE amount IS NULL",
    "SELECT * FROM orders WHERE id IN (1, 2, 3)",
    "SELECT * FROM orders WHERE amount BETWEEN 10 AND 100",
    "SELECT * FROM orders WHERE status = 'shipped' OR amount > 500",
    "SELECT * FROM orders WHERE amount > 0 AND status = 'pending'",
    "SELECT * FROM orders WHERE status != 'cancelled'",
];

#[derive(Debug, Arbitrary)]
struct SnapRegister {
    consumer_id: u8,
    sql_idx: u8,
}

#[derive(Debug, Arbitrary)]
enum SnapEvent {
    Insert {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Update {
        id: u8,
        amount: Option<i16>,
        status: Option<u8>,
    },
    Delete {
        id: u8,
    },
}

/// Per-process working directory for the snapshot/restore harness.
/// libFuzzer spawns one worker process per parallel run. Pinning the
/// path to `pid` keeps separate workers from clobbering each other's
/// shard files, and a per-iteration `remove_dir_all` + `create_dir_all`
/// starts each round-trip from a clean slate.
///
/// Prefers `/dev/shm` (Linux tmpfs / RAM) over `std::env::temp_dir()`
/// (often a btrfs / ext4 mount), because the harness's bottleneck is
/// the snapshot write's `fsync` and the restore's directory scan. On
/// systems without `/dev/shm` the fallback to the platform temp dir
/// keeps the harness working with slower iteration speed.
fn snapshot_workdir() -> std::path::PathBuf {
    let shm = std::path::Path::new("/dev/shm");
    let mut p = if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    };
    p.push(format!(
        "subql-fuzz-snapshot-restore-{}",
        std::process::id()
    ));
    p
}

fn snap_event_to_event(
    op: SnapEvent,
    table_id: crate::TableId,
    pk_col: crate::ColumnId,
    virt: &mut BTreeMap<i64, VirtRow>,
) -> Option<TestEvent<Postgres>> {
    match op {
        SnapEvent::Insert { id, amount, status } => {
            let id = i64::from(id);
            if virt.contains_key(&id) {
                return None;
            }
            let row = VirtRow::from_op(amount, status);
            let values = agg_row_values(id, &row);
            virt.insert(id, row);
            Some(TestEvent::<Postgres>::insert(table_id, values).with_pk_columns([pk_col]))
        }
        SnapEvent::Update { id, amount, status } => {
            let id = i64::from(id);
            let old = virt.get(&id).cloned()?;
            let new_row = VirtRow::from_op(amount, status);
            let old_values = agg_row_values(id, &old);
            let new_values = agg_row_values(id, &new_row);
            virt.insert(id, new_row);
            Some(
                TestEvent::<Postgres>::update(table_id, old_values, new_values)
                    .with_pk_columns([pk_col]),
            )
        }
        SnapEvent::Delete { id } => {
            let id = i64::from(id);
            let old = virt.remove(&id)?;
            let old_values = agg_row_values(id, &old);
            Some(TestEvent::<Postgres>::delete(table_id, old_values).with_pk_columns([pk_col]))
        }
    }
}

fn notifications_equal(
    a: &crate::ConsumerNotifications<DefaultIds>,
    b: &crate::ConsumerNotifications<DefaultIds>,
) -> bool {
    let mut a_ins = a.inserted().to_vec();
    let mut b_ins = b.inserted().to_vec();
    let mut a_upd = a.updated().to_vec();
    let mut b_upd = b.updated().to_vec();
    let mut a_del = a.deleted().to_vec();
    let mut b_del = b.deleted().to_vec();
    a_ins.sort_unstable();
    b_ins.sort_unstable();
    a_upd.sort_unstable();
    b_upd.sort_unstable();
    a_del.sort_unstable();
    b_del.sort_unstable();
    a_ins == b_ins && a_upd == b_upd && a_del == b_del
}

/// Build an engine, register an arbitrary set of subscriptions, snapshot
/// them to disk, rebuild a fresh engine from the same on-disk shards,
/// then dispatch an arbitrary event sequence through both engines and
/// assert their `ConsumerNotifications` match for every event.
///
/// Strong oracle: any drift between the in-memory state of the
/// registering engine and the restored engine surfaces as a real test
/// failure.
///
/// Contract: panics are bugs. Assertion failures are bugs. Errors from
/// `register`, `snapshot_table`, `with_storage`, or `consumers` are
/// fine - the harness simply bails out cleanly on any of them.
pub fn harness_snapshot_restore_roundtrip(data: &[u8]) {
    let mut u = Unstructured::new(data);
    // Bounded register and event counts: the harness is by far the
    // slowest one because it constructs two engines and snapshots /
    // restores per iteration. Capping at 4 / 16 keeps a typical
    // iteration well under 1 second even under disk / CPU contention
    // from the other fuzz panes.
    let Ok(n_reg) = u.int_in_range(1usize..=4) else {
        return;
    };
    let Ok(regs): arbitrary::Result<Vec<SnapRegister>> = (0..n_reg)
        .map(|_| SnapRegister::arbitrary(&mut u))
        .collect()
    else {
        return;
    };
    let Ok(n_events) = u.int_in_range(0usize..=16) else {
        return;
    };
    let Ok(events): arbitrary::Result<Vec<SnapEvent>> = (0..n_events)
        .map(|_| SnapEvent::arbitrary(&mut u))
        .collect()
    else {
        return;
    };

    let workdir = snapshot_workdir();
    let _ = std::fs::remove_dir_all(&workdir);
    if std::fs::create_dir_all(&workdir).is_err() {
        return;
    }

    let database = agg_catalog();
    let Some(table_id) = catalog_helpers::table_id(&database, "orders") else {
        return;
    };
    let pk_col = match catalog_helpers::column_id(&database, table_id, "id") {
        Some(c) => c,
        None => return,
    };

    let mut engine_a: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        match SubscriptionEngine::with_storage(agg_catalog(), PostgreSqlDialect {}, workdir.clone())
        {
            Ok((e, _reads)) => e,
            Err(_) => return,
        };

    // Track which (consumer_id, sql) pairs we've registered to avoid
    // duplicate consumer_id collisions, which would fail on the engine
    // side and desynchronise A and B's view of the consumer set.
    use std::collections::HashSet;
    let mut seen_consumers: HashSet<u64> = HashSet::new();
    for reg in &regs {
        let cid = u64::from(reg.consumer_id);
        if !seen_consumers.insert(cid) {
            continue;
        }
        let sql = SNAPSHOT_REGISTER_SQLS[(reg.sql_idx as usize) % SNAPSHOT_REGISTER_SQLS.len()];
        let _ = engine_a.register(SubscriptionRequest::<DefaultIds>::new(cid, sql));
    }

    if engine_a.snapshot_table(table_id).is_err() {
        return;
    }

    let mut engine_b: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        match SubscriptionEngine::with_storage(database, PostgreSqlDialect {}, workdir) {
            Ok((e, _reads)) => e,
            Err(_) => return,
        };

    let mut virt: BTreeMap<i64, VirtRow> = BTreeMap::new();
    for op in events {
        let Some(event) = snap_event_to_event(op, table_id, pk_col, &mut virt) else {
            continue;
        };
        let notif_a = match engine_a.consumers(&event) {
            Ok(n) => n,
            Err(_) => return,
        };
        let notif_b = match engine_b.consumers(&event) {
            Ok(n) => n,
            Err(_) => return,
        };
        assert!(
            notifications_equal(&notif_a, &notif_b),
            "snapshot/restore drift: A={:?} B={:?} event_kind={:?}",
            (notif_a.inserted(), notif_a.updated(), notif_a.deleted()),
            (notif_b.inserted(), notif_b.updated(), notif_b.deleted()),
            event.kind(),
        );
    }
}

/// Drive raw bytes through the pgoutput binary parser. Exercises both
/// the cursor-parsing paths in every message-type branch (single-message
/// mode) and the relation-cache cross-message state (sequenced mode).
///
/// Contract: panics are bugs. Any decode error is fine.
pub fn harness_pgoutput(data: &[u8]) {
    // Single-message mode: the whole input is one pgoutput message body.
    {
        let mut decoder = PgOutputDecoder::with_protocol_version(1);
        let _ = decoder.decode_message(data.to_vec(), Lsn::new(0));
    }

    // Sequenced mode: up to 8 length-prefixed chunks through one decoder
    // so a relation chunk can prime the cache for a later chunk.
    {
        let mut decoder = PgOutputDecoder::with_protocol_version(1);
        let mut cur = data;
        for _ in 0..8 {
            if cur.len() < 2 {
                break;
            }
            let len = u16::from_le_bytes([cur[0], cur[1]]) as usize;
            cur = &cur[2..];
            let take = len.min(cur.len());
            let (chunk, rest) = cur.split_at(take);
            cur = rest;
            let _ = decoder.decode_message(chunk.to_vec(), Lsn::new(0));
        }
    }
}

/// End-to-end pgoutput fuzz harness on the fake-Postgres-over-SQLite
/// emulator.
///
/// Drives an arbitrary DML stream through [`crate::PgSqliteEmuSource`],
/// which internally re-encodes each session changeset as pgoutput wire
/// bytes, decodes them with `pg_walstream`'s `PgOutputDecoder`, and
/// dispatches every emitted [`crate::ChangeEvent`] through a
/// populated [`SubscriptionEngine`]. Exercises the whole pipeline
/// (catalog plus pg2sqlite plus session extension plus changeset->
/// pgoutput encode plus pgoutput decode plus VM dispatch) on every
/// input.
///
/// # Invariants enforced at fixture build time
///
/// Several seams use fixed inputs (the DDL string, the connection
/// target, the subscription SQL). A regression that breaks any of them
/// should crash the very first fuzz iteration, never report "green"
/// while silently fuzzing nothing. The init path asserts:
///
/// * the fixed PG DDL parses and applies through `pg2sqlite`,
/// * the in-memory SQLite connection opens,
/// * every fixed [`SubscriptionRequest`] compiles and registers.
///
/// # Per-iteration contract
///
/// Panics inside `source.execute`, `source.poll_next_event`, or
/// `engine.consumers` are bugs. Errors at those seams are fine because
/// adversarial DML can legitimately produce them (constraint
/// violations, dispatch errors when an UPDATE arrives without the old
/// row), but the result is fed to [`core::hint::black_box`] so the
/// optimizer cannot dead-code-eliminate the dispatch.
#[cfg(feature = "pg-sqlite-emu")]
pub fn harness_sqlite_pgoutput_e2e(data: &[u8]) {
    use core::cell::RefCell;
    use core::hint::black_box;

    // libfuzzer hands us tiny inputs too: bail out early so we do not
    // pay the (cached but still nonzero) fixture-borrow cost.
    if data.len() < 2 {
        return;
    }

    // Reuse one fixture per thread across iterations. The init path
    // hard-asserts every constant invariant so a regression crashes
    // the first iter rather than producing silent "green" runs.
    thread_local! {
        static FIXTURE: RefCell<E2eFixture> = RefCell::new(E2eFixture::init());
    }

    FIXTURE.with(|cell| {
        let mut fixture = cell.borrow_mut();
        fixture.reset();
        let mut u = Unstructured::new(data);

        // With a cached fixture each op stays cheap, so cap higher than
        // the per-iter throw-away version: more mutations per input
        // reach the dispatch path.
        let op_count = u.int_in_range(0u8..=64).unwrap_or(0);
        for _ in 0..op_count {
            // 4 % chance of injecting a synthetic Truncate event. The
            // session extension has no TRUNCATE analog, so this is the
            // only way the engine's Truncate dispatch fires in this
            // harness.
            if u.int_in_range(0u8..=24).unwrap_or(0) == 0 {
                let _ = fixture.inject_truncate();
                continue;
            }

            let Some(sql) = next_dml(&mut u, fixture.table_id) else {
                return;
            };
            // Execute the DML against SQLite. Errors here are
            // adversarial-input territory (constraint violations,
            // syntax we did not anticipate) and skipped.
            if fixture.execute_sql(&sql).is_err() {
                continue;
            }
            fixture.drain_and_dispatch(&mut |ev| {
                let _ = black_box(ev);
            });
        }
    });
}

#[cfg(feature = "pg-sqlite-emu")]
struct E2eFixture {
    source: crate::PgSqliteEmuSource,
    engine: SubscriptionEngine<crate::ChangeEvent, DefaultIds, ParserDB>,
    table_id: crate::TableId,
}

#[cfg(feature = "pg-sqlite-emu")]
impl E2eFixture {
    /// Fixed PG-dialect DDL for the `orders` table. Single-column INT
    /// primary key, one nullable INT, one nullable TEXT. Composite PK
    /// and a wider column set belong in a separate harness with its
    /// own fixture.
    const PG_DDL: &'static str =
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

    /// Subscriptions the engine dispatches against.
    const SUBSCRIPTIONS: &'static [(u64, &'static str)] = &[
        (1, "SELECT * FROM orders WHERE amount > 100"),
        (2, "SELECT * FROM orders WHERE status = 'paid'"),
        (3, "SELECT * FROM orders WHERE amount < 50"),
        (4, "SELECT * FROM orders WHERE id = 5"),
        (5, "SELECT * FROM orders WHERE amount IS NULL"),
    ];

    fn init() -> Self {
        let source = crate::PgSqliteEmuSource::open_in_memory(Self::PG_DDL)
            .expect("PgSqliteEmuSource fixture must construct from fixed PG DDL");
        let table_id = catalog_helpers::table_id(source.pg_catalog(), "orders")
            .expect("fuzz fixture orders table must resolve");

        let mut engine: SubscriptionEngine<crate::ChangeEvent, DefaultIds, ParserDB> =
            SubscriptionEngine::new(source.pg_catalog().clone(), PostgreSqlDialect {});
        for (consumer_id, sql) in Self::SUBSCRIPTIONS {
            engine
                .register(SubscriptionRequest::new(*consumer_id, *sql))
                .expect("fuzz fixture subscription must register");
        }

        Self {
            source,
            engine,
            table_id,
        }
    }

    /// Clear table state between iterations. The source, session, and
    /// engine all survive.
    fn reset(&mut self) {
        // The bulk DELETE fires one changeset op per row, which then
        // flows through the drain loop; we discard everything so the
        // next iter starts with an empty stream.
        let _ = self.source.execute_sql("DELETE FROM orders");
        while let Ok(Some(_)) = self.source.poll_next_event() {
            // Discard residual events from the bulk DELETE.
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<usize, crate::PgSqliteEmuError> {
        self.source.execute_sql(sql)
    }

    fn inject_truncate(&mut self) -> Result<(), crate::PgSqliteEmuError> {
        self.source.inject_truncate(self.table_id)
    }

    /// Drain every event the source has for us and dispatch each
    /// through the engine, feeding the dispatch result to `sink` so the
    /// optimizer cannot dead-code-eliminate the whole loop.
    fn drain_and_dispatch<F>(&mut self, sink: &mut F)
    where
        F: FnMut(
            &Result<crate::ConsumerNotifications<DefaultIds, crate::PgLsn>, crate::DispatchError>,
        ),
    {
        loop {
            let event = match self.source.poll_next_event() {
                Ok(Some(ev)) => ev,
                Ok(None) | Err(_) => break,
            };
            let result = self.engine.consumers(&event);
            sink(&result);
        }
    }
}

#[cfg(feature = "pg-sqlite-emu")]
fn next_dml(u: &mut Unstructured<'_>, _table_id: crate::TableId) -> Option<String> {
    // Six branches widen the previous `0u8..=2` mix: NULL inserts, NULL
    // updates, and PK-changing updates all exercise paths the original
    // generator left starved.
    Some(match u.int_in_range(0u8..=5).ok()? {
        0 => {
            // INSERT with concrete values.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = u.int_in_range(-200i32..=200).ok()?;
            let status = pick_status(u);
            alloc_format(format_args!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {amount}, '{status}')"
            ))
        }
        1 => {
            // INSERT with NULL amount and / or status. Exercises the
            // Value::Null branch end-to-end, including the
            // `amount IS NULL` subscription registered above.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                u.int_in_range(-200i32..=200).ok()?.to_string()
            };
            let status = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                alloc_format(format_args!("'{}'", pick_status(u)))
            };
            alloc_format(format_args!(
                "INSERT INTO orders (id, amount, status) VALUES ({id}, {amount}, {status})"
            ))
        }
        2 => {
            // UPDATE with concrete values, may be a no-op.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = u.int_in_range(-200i32..=200).ok()?;
            let status = pick_status(u);
            alloc_format(format_args!(
                "UPDATE orders SET amount = {amount}, status = '{status}' WHERE id = {id}"
            ))
        }
        3 => {
            // UPDATE that sets amount or status to NULL.
            let id = u.int_in_range(1i32..=8).ok()?;
            let amount = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                u.int_in_range(-200i32..=200).ok()?.to_string()
            };
            let status = if bool::arbitrary(u).ok()? {
                "NULL".to_string()
            } else {
                alloc_format(format_args!("'{}'", pick_status(u)))
            };
            alloc_format(format_args!(
                "UPDATE orders SET amount = {amount}, status = {status} WHERE id = {id}"
            ))
        }
        4 => {
            // PK-changing UPDATE. SQLite allows it directly when the
            // new id is unused. Otherwise the statement fails with a
            // UNIQUE constraint, which the harness swallows. Either
            // outcome is interesting for the dispatch path.
            let old_id = u.int_in_range(1i32..=8).ok()?;
            let new_id = u.int_in_range(1i32..=8).ok()?;
            alloc_format(format_args!(
                "UPDATE orders SET id = {new_id} WHERE id = {old_id}"
            ))
        }
        _ => {
            let id = u.int_in_range(1i32..=8).ok()?;
            alloc_format(format_args!("DELETE FROM orders WHERE id = {id}"))
        }
    })
}

#[cfg(feature = "pg-sqlite-emu")]
#[allow(clippy::range_minus_one)] // `Unstructured::int_in_range` takes `RangeInclusive` only.
fn pick_status(u: &mut Unstructured<'_>) -> &'static str {
    const STATUSES: &[&str] = &["paid", "open", "closed", "pending"];
    let idx = u.int_in_range(0usize..=STATUSES.len() - 1).unwrap_or(0);
    STATUSES[idx]
}

#[cfg(feature = "pg-sqlite-emu")]
fn alloc_format(args: core::fmt::Arguments<'_>) -> String {
    use core::fmt::Write;
    let mut out = String::new();
    let _ = out.write_fmt(args);
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn value_kind(v: &Value<Postgres>) -> u8 {
        match v {
            Value::Null => 0,
            Value::Missing => 1,
            Value::Bool(_) => 2,
            Value::Int(_) => 3,
            Value::Float(_) => 4,
            Value::String(_) => 5,
            // arb_value never emits these variants; keep exhaustive.
            _ => 99,
        }
    }

    fn instruction_kind(instr: &Instruction<Postgres>) -> u8 {
        match instr {
            Instruction::PushLiteral(_) => 0,
            Instruction::LoadColumn(_) => 1,
            Instruction::Equal => 2,
            Instruction::NotEqual => 3,
            Instruction::LessThan => 4,
            Instruction::LessThanOrEqual => 5,
            Instruction::GreaterThan => 6,
            Instruction::GreaterThanOrEqual => 7,
            Instruction::IsNull => 8,
            Instruction::IsNotNull => 9,
            Instruction::And => 10,
            Instruction::Or => 11,
            Instruction::Not => 12,
            Instruction::Add => 13,
            Instruction::Subtract => 14,
            Instruction::Multiply => 15,
            Instruction::Divide => 16,
            Instruction::Modulo => 17,
            Instruction::Negate => 18,
            Instruction::In(_) => 19,
            Instruction::Between => 20,
            Instruction::Like { .. } => 21,
            Instruction::JumpIfFalse(_) => 22,
            Instruction::JumpIfTrue(_) => 23,
            Instruction::TermTruth(_) => 24,
        }
    }

    #[test]
    fn test_fuzz_catalog_resolves_orders_fixture() {
        use sql_traits::prelude::DatabaseLike;

        let catalog = fuzz_catalog();
        let tid = crate::catalog_helpers::table_id(&catalog, "orders")
            .expect("orders must be resolvable in fuzz fixture");
        assert!(catalog.number_of_tables() > 0);
        let arity = crate::catalog_helpers::table_arity(&catalog, tid)
            .expect("orders arity should be known");
        assert!(
            arity >= 3,
            "fuzz orders should have at least id/amount/status"
        );

        let id_col = crate::catalog_helpers::column_id(&catalog, tid, "id");
        let amount_col = crate::catalog_helpers::column_id(&catalog, tid, "amount");
        assert!(id_col.is_some());
        assert!(amount_col.is_some());
        assert_ne!(id_col, amount_col);
    }

    #[test]
    fn test_arb_value_covers_all_generated_variants() {
        let mut seen = BTreeSet::new();
        for seed in u8::MIN..=u8::MAX {
            let mut data = vec![0u8; 1024];
            data[0] = seed;
            let mut u = Unstructured::new(&data);
            if let Ok(v) = arb_value(&mut u) {
                seen.insert(value_kind(&v));
            }
        }

        assert_eq!(
            seen.len(),
            6,
            "expected all 6 arb_value shapes, saw {seen:?}"
        );
    }

    #[test]
    fn test_arb_instruction_covers_all_variants() {
        let mut seen = BTreeSet::new();
        for seed in u8::MIN..=u8::MAX {
            let mut data = vec![0u8; 2048];
            data[0] = seed;
            let mut u = Unstructured::new(&data);
            if let Ok(instr) = arb_instruction(&mut u) {
                seen.insert(instruction_kind(&instr));
            }
        }

        assert_eq!(
            seen.len(),
            24,
            "expected all Instruction variants, saw {seen:?}"
        );
    }

    #[test]
    fn test_harness_entrypoints_do_not_panic() {
        harness_parse_sql(b"SELECT * FROM orders WHERE amount > 10");
        harness_parse_sql(&[0xFF, 0x00, 0xAA, 0x42]);

        harness_vm_eval(&vec![0x11; 4096]);
        harness_vm_eval(&vec![0xEE; 4096]);

        harness_deserialize_shard(&[0x00, 0x01, 0x02, 0x03]);
        harness_canonicalize(b"SELECT * FROM orders WHERE status = 'open'");

        let encoded_vec = codec::encode(&vec![1_u8, 2, 3, 4]).unwrap();
        harness_codec_decode(&encoded_vec);
        harness_codec_decode(&[0xFF, 0x00, 0xAA]);
    }

    #[test]
    fn test_harness_vm_eval_exercises_early_return_paths() {
        harness_vm_eval(&[]);

        for a in u8::MIN..=u8::MAX {
            harness_vm_eval(&[a]);
        }

        for a in 0_u8..=63 {
            for b in 0_u8..=63 {
                harness_vm_eval(&[a, b]);
            }
        }
    }

    #[test]
    fn test_instruction_kind_jump_variants() {
        assert_eq!(instruction_kind(&Instruction::JumpIfFalse(3)), 22);
        assert_eq!(instruction_kind(&Instruction::JumpIfTrue(4)), 23);
    }
}

// ---------------------------------------------------------------------------
// Regression tests: replay crash files from tests/crashes/{harness_name}/
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::manual_let_else)]
mod regression_tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static REPLAY_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_harness(_data: &[u8]) {
        REPLAY_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    /// Run a harness function against every file in the given crash directory.
    /// Missing or empty directories pass silently (no regressions to check yet).
    fn replay_crashes(dir_name: &str, harness: fn(&[u8])) {
        let crash_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("crashes")
            .join(dir_name);

        let entries = match fs::read_dir(&crash_dir) {
            Ok(e) => e,
            Err(_) => return, // directory missing, nothing to replay
        };

        for entry in entries {
            let entry = entry.expect("failed to read directory entry");
            let path = entry.path();

            // Skip non-files (e.g. .gitkeep is fine to read, but dirs are not)
            if !path.is_file() {
                continue;
            }

            // Skip .gitkeep
            if path.file_name().is_some_and(|n| n == ".gitkeep") {
                continue;
            }

            let data = fs::read(&path).unwrap_or_else(|e| {
                panic!("failed to read crash file {}: {e}", path.display());
            });

            harness(&data);
        }
    }

    #[test]
    fn regression_fuzz_parse_sql() {
        replay_crashes("fuzz_parse_sql", harness_parse_sql);
    }

    #[test]
    fn regression_fuzz_vm_eval() {
        replay_crashes("fuzz_vm_eval", harness_vm_eval);
    }

    #[test]
    fn regression_fuzz_deserialize_shard() {
        replay_crashes("fuzz_deserialize_shard", harness_deserialize_shard);
    }

    #[test]
    fn regression_fuzz_canonicalize() {
        replay_crashes("fuzz_canonicalize", harness_canonicalize);
    }

    #[test]
    fn regression_fuzz_codec_decode() {
        replay_crashes("fuzz_codec_decode", harness_codec_decode);
    }

    #[test]
    fn regression_fuzz_pgoutput() {
        replay_crashes("fuzz_pgoutput", harness_pgoutput);
    }

    #[test]
    fn regression_fuzz_wal_json_postparse() {
        replay_crashes("fuzz_wal_json_postparse", harness_wal_json_postparse);
    }

    #[test]
    fn regression_fuzz_aggregate_consistency() {
        replay_crashes("fuzz_aggregate_consistency", harness_aggregate_consistency);
    }

    #[test]
    fn regression_fuzz_snapshot_restore_roundtrip() {
        replay_crashes(
            "fuzz_snapshot_restore_roundtrip",
            harness_snapshot_restore_roundtrip,
        );
    }

    #[test]
    fn replay_crashes_ignores_missing_directory() {
        replay_crashes("definitely-missing-subdir-for-coverage", harness_parse_sql);
    }

    #[test]
    fn replay_crashes_skips_non_files_and_gitkeep_and_replays_payloads() {
        let unique = format!(
            "cov-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        );
        let crash_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("crashes")
            .join(&unique);

        fs::create_dir_all(crash_dir.join("nested")).expect("should create nested directory");
        fs::write(crash_dir.join(".gitkeep"), b"").expect("should create .gitkeep");
        fs::write(crash_dir.join("sample.fuzz"), b"\x01\x02\x03")
            .expect("should create crash payload");

        REPLAY_COUNT.store(0, Ordering::Relaxed);
        replay_crashes(&unique, count_harness);
        assert_eq!(REPLAY_COUNT.load(Ordering::Relaxed), 1);

        fs::remove_dir_all(crash_dir).expect("should remove temporary crash directory");
    }

    #[cfg(unix)]
    #[test]
    fn replay_crashes_panics_on_unreadable_file() {
        use std::os::unix::fs::PermissionsExt;

        let unique = format!(
            "cov-unreadable-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        );
        let crash_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("crashes")
            .join(&unique);
        fs::create_dir_all(&crash_dir).expect("should create crash dir");

        let unreadable = crash_dir.join("unreadable.fuzz");
        fs::write(&unreadable, b"data").expect("should create unreadable file");
        let mut perms = fs::metadata(&unreadable)
            .expect("should stat unreadable file")
            .permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&unreadable, perms).expect("should set unreadable perms");

        let result = std::panic::catch_unwind(|| replay_crashes(&unique, super::harness_parse_sql));
        assert!(
            result.is_err(),
            "expected panic when reading unreadable file"
        );

        let mut reset = fs::metadata(&unreadable)
            .expect("should stat unreadable file")
            .permissions();
        reset.set_mode(0o644);
        fs::set_permissions(&unreadable, reset).expect("should restore permissions");
        fs::remove_dir_all(crash_dir).expect("should remove temporary crash directory");
    }
}
