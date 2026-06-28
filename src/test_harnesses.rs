//! Shared fuzz harness functions.
//!
//! Each `harness_*` function takes raw bytes and exercises a library subsystem.
//! The contract: errors are fine, **panics are bugs**.
//!
//! This module is only compiled under `#[cfg(any(feature = "testing", test))]`.

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

use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use std::collections::BTreeMap;

use crate::compiler::bytecode::{BytecodeProgram, Instruction};
use crate::compiler::canonicalize::{hash_sql, normalize_sql};
use crate::compiler::parser::parse_and_compile;
use crate::compiler::vm::Vm;
use crate::persistence::codec;
use crate::persistence::shard::{deserialize_shard, ShardPayload};
use crate::types::{Cell, PrimaryKey, RowImage};
use crate::wal::{MaxwellParser, PgOutputParser, Wal2JsonV1Parser, Wal2JsonV2Parser, WalParser};
use crate::{
    catalog_helpers, AggDelta, DefaultIds, SubscriptionEngine, SubscriptionRequest, WalEvent,
};

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
             c8 INT, c9 INT, c10 INT, c11 INT, c12 INT, c13 INT, c14 INT, c15 INT\
         );",
    )
    .expect("fuzz fixture DDL parses")
}

/// Generate a [`Cell`] from fuzzer-controlled bytes.
fn arb_cell(u: &mut Unstructured<'_>) -> arbitrary::Result<Cell> {
    match u.int_in_range(0u8..=5)? {
        0 => Ok(Cell::Null),
        1 => Ok(Cell::Missing),
        2 => Ok(Cell::Bool(bool::arbitrary(u)?)),
        3 => Ok(Cell::Int(i64::arbitrary(u)?)),
        4 => Ok(Cell::Float(f64::arbitrary(u)?)),
        _ => {
            let len = u.int_in_range(0usize..=64)?;
            let bytes: Vec<u8> = (0..len)
                .map(|_| u.arbitrary())
                .collect::<arbitrary::Result<_>>()?;
            Ok(Cell::String(String::from_utf8_lossy(&bytes).into()))
        }
    }
}

/// Generate an [`Instruction`] from fuzzer-controlled bytes.
fn arb_instruction(u: &mut Unstructured<'_>) -> arbitrary::Result<Instruction> {
    match u.int_in_range(0u8..=23)? {
        0 => Ok(Instruction::PushLiteral(arb_cell(u)?)),
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
            let list: Vec<Cell> = (0..len)
                .map(|_| arb_cell(u))
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

    let _ = parse_and_compile(sql, &pg, &catalog);
}

/// Generate random bytecode + row and evaluate with the VM.
pub fn harness_vm_eval(data: &[u8]) {
    let mut u = Unstructured::new(data);

    // Generate 1-32 instructions
    let Ok(n_instr) = u.int_in_range(1usize..=32) else {
        return;
    };
    let instructions: Vec<Instruction> = match (0..n_instr)
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
    let cells: Vec<Cell> = match (0..n_cells)
        .map(|_| arb_cell(&mut u))
        .collect::<arbitrary::Result<_>>()
    {
        Ok(v) => v,
        Err(_) => return,
    };

    let program = BytecodeProgram::new(instructions);
    let row = RowImage {
        cells: Arc::from(cells),
    };

    let mut vm = Vm::new();
    let _ = vm.eval(&program, &row);
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
    let pg = PostgreSqlDialect {};

    for dialect in [&pg as &dyn sqlparser::dialect::Dialect] {
        if let Ok(normalized) = normalize_sql(sql, dialect) {
            let h1 = hash_sql(&normalized);
            let h2 = hash_sql(&normalized);
            assert_eq!(h1, h2, "hash_sql is not deterministic");
        }
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
/// relation-cache mismatches, JSON-value-to-Cell coercion, sparse-old-row
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

    let catalog = fuzz_catalog();
    match parser_kind {
        0 => {
            let _ = Wal2JsonV1Parser.parse_wal_message(&bytes, &catalog);
        }
        1 => {
            let _ = Wal2JsonV2Parser.parse_wal_message(&bytes, &catalog);
        }
        _ => {
            let _ = MaxwellParser.parse_wal_message(&bytes, &catalog);
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate-consistency harness
// ---------------------------------------------------------------------------

/// Mutation operation against the virtual `orders` table used by
/// [`harness_aggregate_consistency`]. `Truncate` is intentionally absent:
/// subql's `aggregate_deltas` semantics on Truncate would require the
/// engine to know per-consumer running state to negate, which is not
/// part of the documented API.
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

/// Build a 3-cell `RowImage` (id, amount, status) matching the
/// `agg_catalog()` schema.
fn agg_row_image(id: i64, row: &VirtRow) -> RowImage {
    let cells = [
        Cell::Int(id),
        row.amount.map_or(Cell::Null, Cell::Int),
        row.status
            .as_deref()
            .map_or(Cell::Null, |s| Cell::String(s.into())),
    ];
    RowImage {
        cells: Arc::from(cells.as_slice()),
    }
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
    engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>,
    table_id: crate::TableId,
    pk_col: crate::ColumnId,
}

impl AggEngineCell {
    fn new() -> Self {
        let database = agg_catalog();
        let table_id = catalog_helpers::table_id(&database, "orders")
            .expect("agg_catalog must expose an `orders` table");
        let pk_col = catalog_helpers::column_id(&database, table_id, "id")
            .expect("agg_catalog `orders` must expose an `id` column");
        let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
            SubscriptionEngine::new(database, PostgreSqlDialect {});
        engine
            .register(SubscriptionRequest::<DefaultIds>::new(
                1,
                "SELECT COUNT(*) FROM orders",
            ))
            .expect("registering COUNT(*) consumer should succeed against agg_catalog");
        engine
            .register(SubscriptionRequest::<DefaultIds>::new(
                2,
                "SELECT SUM(amount) FROM orders",
            ))
            .expect("registering SUM(amount) consumer should succeed against agg_catalog");
        // Register one consumer per VAR/STDDEV flavor. All four share the
        // same kernel and should receive byte-identical `AggDelta::Stats`
        // deltas, so registering all four catches per-variant routing or
        // hash-collision bugs as well as kernel correctness.
        for (cid, sql) in [
            (3_u64, "SELECT VAR_POP(amount) FROM orders"),
            (4_u64, "SELECT VAR_SAMP(amount) FROM orders"),
            (5_u64, "SELECT STDDEV_POP(amount) FROM orders"),
            (6_u64, "SELECT STDDEV_SAMP(amount) FROM orders"),
        ] {
            engine
                .register(SubscriptionRequest::<DefaultIds>::new(cid, sql))
                .expect("registering VAR/STDDEV consumer should succeed against agg_catalog");
        }
        engine
            .register(SubscriptionRequest::<DefaultIds>::new(
                7,
                "SELECT AVG(amount) FROM orders",
            ))
            .expect("registering AVG(amount) consumer should succeed against agg_catalog");
        Self {
            engine,
            table_id,
            pk_col,
        }
    }
}

std::thread_local! {
    static AGG_ENGINE: std::cell::RefCell<AggEngineCell> =
        std::cell::RefCell::new(AggEngineCell::new());
}

/// Drive an arbitrary sequence of insert/update/delete operations against
/// a fixed agg-only consumer set and assert that the engine's incremental
/// `aggregate_deltas` output matches a from-scratch oracle.
///
/// Catches drift in:
/// - `COUNT(*)` (`AggDelta::Count`). Should equal current virtual-table size.
/// - `SUM(amount)` (`AggDelta::Sum`). Should equal sum of non-NULL amounts.
/// - `AVG(amount)` (`AggDelta::Avg`). Running `(sum, count)` tuple should
///   equal the oracle tuple recomputed from the virtual table.
/// - `VAR_POP/VAR_SAMP/STDDEV_POP/STDDEV_SAMP(amount)` (`AggDelta::Stats`).
///   Each of the four consumers' running `(sum, sum_sq, count)` tuple
///   should equal the oracle tuple recomputed from the virtual table.
///
/// Contract: panics are bugs. Assertion failures are bugs.
#[allow(clippy::too_many_lines)]
pub fn harness_aggregate_consistency(data: &[u8]) {
    let mut u = Unstructured::new(data);
    let Ok(ops): arbitrary::Result<Vec<AggOp>> = (|| {
        let n = u.int_in_range(0usize..=64)?;
        (0..n).map(|_| AggOp::arbitrary(&mut u)).collect()
    })() else {
        return;
    };

    AGG_ENGINE.with(|cell| {
        let mut cell = cell.borrow_mut();
        let table_id = cell.table_id;
        let pk_col = cell.pk_col;

        // Engine-side running state per consumer (fresh each iteration,
        // because the oracle restarts from an empty virtual table).
        let mut engine_count: i64 = 0;
        let mut engine_sum: f64 = 0.0;
        // AVG(amount): running (sum, count) tuple for consumer cid=7.
        let mut engine_avg: (f64, i64) = (0.0, 0);
        // Four running `(sum, sum_sq, count)` tuples, indexed by consumer
        // id - 3 (so cid 3..=6 maps to slots 0..=3).
        let mut engine_stats: [(f64, f64, i64); 4] = [(0.0, 0.0, 0); 4];

        // Virtual table (id -> row), the source of truth for the oracle.
        let mut virt: BTreeMap<i64, VirtRow> = BTreeMap::new();

        for op in ops {
            let (event, mutated): (Option<WalEvent>, bool) = match op {
                AggOp::Insert { id, amount, status } => {
                    let id = i64::from(id);
                    if virt.contains_key(&id) {
                        (None, false)
                    } else {
                        let row = VirtRow::from_op(amount, status);
                        let image = agg_row_image(id, &row);
                        virt.insert(id, row);
                        let pk = match PrimaryKey::new(
                            Arc::from([pk_col].as_slice()),
                            Arc::from([Cell::Int(id)].as_slice()),
                        ) {
                            Ok(pk) => pk,
                            Err(_) => return,
                        };
                        let event = WalEvent::builder(table_id)
                            .insert()
                            .pk(pk)
                            .new_row(image)
                            .build();
                        match event {
                            Ok(e) => (Some(e), true),
                            Err(_) => return,
                        }
                    }
                }
                AggOp::Update { id, amount, status } => {
                    let id = i64::from(id);
                    let Some(old) = virt.get(&id).cloned() else {
                        continue;
                    };
                    let new_row = VirtRow::from_op(amount, status);
                    let old_image = agg_row_image(id, &old);
                    let new_image = agg_row_image(id, &new_row);
                    virt.insert(id, new_row);
                    let pk = match PrimaryKey::new(
                        Arc::from([pk_col].as_slice()),
                        Arc::from([Cell::Int(id)].as_slice()),
                    ) {
                        Ok(pk) => pk,
                        Err(_) => return,
                    };
                    let event = WalEvent::builder(table_id)
                        .update()
                        .pk(pk)
                        .old_row(old_image)
                        .new_row(new_image)
                        .build();
                    match event {
                        Ok(e) => (Some(e), true),
                        Err(_) => return,
                    }
                }
                AggOp::Delete { id } => {
                    let id = i64::from(id);
                    let Some(old) = virt.remove(&id) else {
                        continue;
                    };
                    let old_image = agg_row_image(id, &old);
                    let pk = match PrimaryKey::new(
                        Arc::from([pk_col].as_slice()),
                        Arc::from([Cell::Int(id)].as_slice()),
                    ) {
                        Ok(pk) => pk,
                        Err(_) => return,
                    };
                    let event = WalEvent::builder(table_id)
                        .delete()
                        .pk(pk)
                        .old_row(old_image)
                        .build();
                    match event {
                        Ok(e) => (Some(e), true),
                        Err(_) => return,
                    }
                }
            };

            if !mutated {
                continue;
            }
            let Some(event) = event else { continue };

            let deltas = match cell.engine.aggregate_deltas(&event) {
                Ok(d) => d,
                Err(_) => return,
            };
            for (cid, delta) in deltas {
                match (cid, delta) {
                    (1, AggDelta::Count(d)) => engine_count += d,
                    (2, AggDelta::Sum(d)) => engine_sum += d,
                    (
                        cid @ 3..=6,
                        AggDelta::Stats {
                            sum_delta,
                            sum_sq_delta,
                            count_delta,
                        },
                    ) => {
                        // cid is bounded to 3..=6 by the match guard, so the
                        // subtraction and downcast cannot truncate.
                        #[allow(clippy::cast_possible_truncation)]
                        let idx = (cid - 3) as usize;
                        let slot = &mut engine_stats[idx];
                        slot.0 += sum_delta;
                        slot.1 += sum_sq_delta;
                        slot.2 += count_delta;
                    }
                    (
                        7,
                        AggDelta::Avg {
                            sum_delta,
                            count_delta,
                        },
                    ) => {
                        engine_avg.0 += sum_delta;
                        engine_avg.1 += count_delta;
                    }
                    _ => {}
                }
            }

            // Oracle: COUNT(*) is virtual-table size, SUM(amount) sums
            // non-NULL amounts, VAR/STDDEV consumers share a single
            // (sum, sum_sq, count) ground truth.
            let oracle_count = i64::try_from(virt.len()).unwrap_or(i64::MAX);
            // Amounts originate from `AggOp::Insert/Update.amount: i16`, so
            // `v: i64` always fits exactly in f64.
            #[allow(clippy::cast_precision_loss)]
            let oracle_sum: f64 = virt
                .values()
                .filter_map(|r| r.amount)
                .map(|v| v as f64)
                .sum();
            let oracle_stats_sum: f64 = oracle_sum;
            // Amounts originate from `AggOp::Insert/Update.amount: i16`, so
            // `v: i64` always fits exactly in f64. The precision-loss lint
            // is theoretically true for arbitrary i64 but not for our range.
            #[allow(clippy::cast_precision_loss)]
            let oracle_stats_sum_sq: f64 = virt
                .values()
                .filter_map(|r| r.amount)
                .map(|v| {
                    let f = v as f64;
                    f * f
                })
                .sum();
            let oracle_stats_count: i64 =
                i64::try_from(virt.values().filter(|r| r.amount.is_some()).count())
                    .unwrap_or(i64::MAX);

            assert_eq!(
                engine_count, oracle_count,
                "COUNT(*) drift: engine={engine_count} oracle={oracle_count}"
            );
            let tolerance = 1e-9_f64.max(oracle_sum.abs() * 1e-12);
            assert!(
                (engine_sum - oracle_sum).abs() <= tolerance,
                "SUM(amount) drift: engine={engine_sum} oracle={oracle_sum}"
            );

            // AVG(amount): oracle reuses oracle_sum (sum of non-NULL amounts)
            // and oracle_stats_count (count of non-NULL amounts).
            assert_eq!(
                engine_avg.1, oracle_stats_count,
                "AVG(amount) count drift: engine={} oracle={oracle_stats_count}",
                engine_avg.1,
            );
            assert!(
                (engine_avg.0 - oracle_sum).abs() <= tolerance,
                "AVG(amount) sum drift: engine={} oracle={oracle_sum}",
                engine_avg.0,
            );

            // VAR/STDDEV: all four consumers share the same kernel, so
            // each running tuple must match the oracle independently.
            // Float tolerance scales with magnitude: sum_sq grows as
            // amount^2 * count, so the same relative tolerance suffices.
            let tol_sum_sq = 1e-6_f64.max(oracle_stats_sum_sq.abs() * 1e-10);
            for (slot_idx, slot) in engine_stats.iter().enumerate() {
                let cid = slot_idx as u64 + 3;
                let (s, sq, n) = *slot;
                assert_eq!(
                    n, oracle_stats_count,
                    "consumer {cid} stats count drift: engine={n} oracle={oracle_stats_count}",
                );
                assert!(
                    (s - oracle_stats_sum).abs() <= tolerance,
                    "consumer {cid} stats sum drift: engine={s} oracle={oracle_stats_sum}",
                );
                assert!(
                    (sq - oracle_stats_sum_sq).abs() <= tol_sum_sq,
                    "consumer {cid} stats sum_sq drift: \
                     engine={sq} oracle={oracle_stats_sum_sq}",
                );
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

fn snap_event_to_walevent(
    op: SnapEvent,
    table_id: crate::TableId,
    pk_col: crate::ColumnId,
    virt: &mut BTreeMap<i64, VirtRow>,
) -> Option<WalEvent> {
    match op {
        SnapEvent::Insert { id, amount, status } => {
            let id = i64::from(id);
            if virt.contains_key(&id) {
                return None;
            }
            let row = VirtRow::from_op(amount, status);
            let image = agg_row_image(id, &row);
            virt.insert(id, row);
            let pk = PrimaryKey::new(
                Arc::from([pk_col].as_slice()),
                Arc::from([Cell::Int(id)].as_slice()),
            )
            .ok()?;
            WalEvent::builder(table_id)
                .insert()
                .pk(pk)
                .new_row(image)
                .build()
                .ok()
        }
        SnapEvent::Update { id, amount, status } => {
            let id = i64::from(id);
            let old = virt.get(&id).cloned()?;
            let new_row = VirtRow::from_op(amount, status);
            let old_image = agg_row_image(id, &old);
            let new_image = agg_row_image(id, &new_row);
            virt.insert(id, new_row);
            let pk = PrimaryKey::new(
                Arc::from([pk_col].as_slice()),
                Arc::from([Cell::Int(id)].as_slice()),
            )
            .ok()?;
            WalEvent::builder(table_id)
                .update()
                .pk(pk)
                .old_row(old_image)
                .new_row(new_image)
                .build()
                .ok()
        }
        SnapEvent::Delete { id } => {
            let id = i64::from(id);
            let old = virt.remove(&id)?;
            let old_image = agg_row_image(id, &old);
            let pk = PrimaryKey::new(
                Arc::from([pk_col].as_slice()),
                Arc::from([Cell::Int(id)].as_slice()),
            )
            .ok()?;
            WalEvent::builder(table_id)
                .delete()
                .pk(pk)
                .old_row(old_image)
                .build()
                .ok()
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

    let mut engine_a: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        match SubscriptionEngine::with_storage(agg_catalog(), PostgreSqlDialect {}, workdir.clone())
        {
            Ok(e) => e,
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

    let mut engine_b: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        match SubscriptionEngine::with_storage(database, PostgreSqlDialect {}, workdir) {
            Ok(e) => e,
            Err(_) => return,
        };

    let mut virt: BTreeMap<i64, VirtRow> = BTreeMap::new();
    for op in events {
        let Some(event) = snap_event_to_walevent(op, table_id, pk_col, &mut virt) else {
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
            "snapshot/restore drift: A={:?} B={:?} event={:?}",
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
/// Contract: panics are bugs. Any `Err(WalParseError)` is fine.
pub fn harness_pgoutput(data: &[u8]) {
    let catalog = fuzz_catalog();

    // Single-message mode: the whole input is one pgoutput message.
    // Exercises the message-type dispatch and cursor parsing of every
    // tuple-bearing branch (I/U/D/R/T).
    {
        let parser = PgOutputParser::new();
        let _ = parser.parse_wal_message(data, &catalog);
    }

    // Sequenced mode: up to 8 length-prefixed chunks fed through the
    // same parser instance. Lets the mutator populate the relation
    // cache with one chunk and reference it from a later chunk,
    // surfacing cache-mismatch and replica-identity bugs that a
    // single-message harness cannot reach.
    {
        let parser = PgOutputParser::new();
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
            let _ = parser.parse_wal_message(chunk, &catalog);
        }
    }
}
/// pgoutput round-trip full end-to-end fuzz harness.
///
/// Drives an arbitrary DML stream through [`crate::SqliteCdcSource`],
/// routes each emitted [`WalEvent`] through [`crate::PgOutputBridge`]
/// and [`PgOutputParser`], and dispatches the parser's output through
/// a populated [`SubscriptionEngine`]. Exercises the whole pipeline
/// (catalog plus triggers plus shadow log plus IR translation plus
/// `pgoutput` encode plus `pgoutput` decode plus VM dispatch) on every
/// input, using the in-process `SqliteCdcSource` as a stand-in for a
/// real PostgreSQL source.
///
/// # Invariants enforced at fixture build time
///
/// Several seams of the harness use fixed inputs (the DDL string, the
/// connection target, the subscription SQL). A regression that breaks
/// any of them should crash the very first fuzz iteration, never report
/// "green" while silently fuzzing nothing. The init path asserts:
///
/// * the fixed PG DDL parses into a [`ParserDB`],
/// * the in-memory SQLite connection opens,
/// * the SQLite-side schema applies through `pg2sqlite`,
/// * every fixed [`SubscriptionRequest`] compiles and registers.
///
/// # Per-iteration contract
///
/// Panics inside `bridge.encode_event`, `parser.parse_wal_message`,
/// `engine.consumers`, or any of the Diesel paths are bugs. Errors at
/// those seams are fine because adversarial DML can legitimately
/// produce them (constraint violations, malformed WAL bytes the parser
/// rejects, dispatch errors when an UPDATE arrives without the old
/// row), but the result is fed to [`core::hint::black_box`] so the
/// optimizer cannot dead-code-eliminate the dispatch.
#[cfg(feature = "sqlite-cdc")]
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
            // SQLite source has no TRUNCATE analog, so this is the only
            // way the bridge's Truncate arm and the engine's Truncate
            // dispatch fire in this harness.
            if u.int_in_range(0u8..=24).unwrap_or(0) == 0 {
                let event = WalEvent::builder(fixture.table_id).truncate().build();
                if let Ok(event) = event {
                    fixture.route_event(&event);
                }
                continue;
            }

            let Some(sql) = next_dml(&mut u, fixture.table_id) else {
                return;
            };
            // Execute the DML against SQLite. Errors here are
            // adversarial-input territory (constraint violations,
            // syntax we did not anticipate) and skipped.
            if fixture.source.execute(&sql).is_err() {
                continue;
            }
            fixture.drain_and_dispatch(&mut |ev| {
                let _ = black_box(ev);
            });
        }
    });
}

#[cfg(feature = "sqlite-cdc")]
struct E2eFixture {
    catalog: ParserDB,
    table_id: crate::TableId,
    engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB>,
    source: crate::SqliteCdcSource,
    bridge: crate::PgOutputBridge,
    parser: PgOutputParser,
}

#[cfg(feature = "sqlite-cdc")]
impl E2eFixture {
    /// Fixed PG-dialect DDL for the `orders` table. Single-column INT
    /// primary key, one nullable INT, one nullable TEXT. Composite PK
    /// and REPLICA IDENTITY DEFAULT (key-only old tuples) belong in a
    /// separate harness with its own fixture.
    const PG_DDL: &'static str =
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);";

    /// Subscriptions the engine dispatches against. Mirrors the fixed
    /// query set in `tests/proptest_sqlite_dispatch.rs`.
    const SUBSCRIPTIONS: &'static [(u64, &'static str)] = &[
        (1, "SELECT * FROM orders WHERE amount > 100"),
        (2, "SELECT * FROM orders WHERE status = 'paid'"),
        (3, "SELECT * FROM orders WHERE amount < 50"),
        (4, "SELECT * FROM orders WHERE id = 5"),
        (5, "SELECT * FROM orders WHERE amount IS NULL"),
    ];

    fn init() -> Self {
        use diesel::{Connection, SqliteConnection};

        use crate::{PgOutputBridge, SqliteCdcConfig, SqliteCdcSource};

        let catalog_db = ParserDB::parse::<PostgreSqlDialect>(Self::PG_DDL)
            .expect("fuzz fixture DDL must parse (regression: invariant broken)");
        let table_id = catalog_helpers::table_id(&catalog_db, "orders")
            .expect("fuzz fixture orders table must resolve");

        let mut engine = SubscriptionEngine::<PostgreSqlDialect, DefaultIds, ParserDB>::new(
            ParserDB::parse::<PostgreSqlDialect>(Self::PG_DDL)
                .expect("fuzz fixture DDL must parse"),
            PostgreSqlDialect {},
        );
        for (consumer_id, sql) in Self::SUBSCRIPTIONS {
            engine
                .register(SubscriptionRequest::new(*consumer_id, *sql))
                .expect("fuzz fixture subscription must register");
        }

        let conn =
            SqliteConnection::establish(":memory:").expect("in-memory SQLite connection must open");
        let source = SqliteCdcSource::with_pg_ddl(conn, Self::PG_DDL, SqliteCdcConfig::default())
            .expect("SqliteCdcSource must construct from fixed PG DDL");

        Self {
            catalog: catalog_db,
            table_id,
            engine,
            source,
            bridge: PgOutputBridge::new(),
            parser: PgOutputParser::new(),
        }
    }

    /// Clear shadow + table state between iterations. The connection,
    /// triggers, engine, bridge, and parser all survive.
    fn reset(&mut self) {
        // The bulk DELETE fires one trigger per row, leaving DELETE
        // entries on the shadow log we need to drain so the next
        // iter's events arrive in deterministic order.
        let _ = self.source.execute("DELETE FROM orders");
        while let Ok(Some(_)) = self.source.poll_next_event() {
            // Discard residual shadow-log entries from the bulk DELETE.
        }
    }

    /// Drain every shadow-log entry into the dispatch chain, calling
    /// `sink` with whatever the engine returned for each parsed event.
    fn drain_and_dispatch<F>(&mut self, sink: &mut F)
    where
        F: FnMut(
            &Result<
                crate::ConsumerNotifications<DefaultIds, crate::NoCheckpoint>,
                crate::DispatchError,
            >,
        ),
    {
        loop {
            let event = match self.source.poll_next_event() {
                Ok(Some(ev)) => ev,
                Ok(None) | Err(_) => break,
            };
            self.route_event_inner(&event, sink);
        }
    }

    /// Encode -> parse -> dispatch a single `WalEvent`. Used by both the
    /// SQLite-sourced events and the synthetic Truncate-injection path.
    fn route_event(&mut self, event: &WalEvent) {
        self.route_event_inner(event, &mut |result| {
            let _ = core::hint::black_box(result);
        });
    }

    fn route_event_inner<F>(&mut self, event: &WalEvent, sink: &mut F)
    where
        F: FnMut(
            &Result<
                crate::ConsumerNotifications<DefaultIds, crate::NoCheckpoint>,
                crate::DispatchError,
            >,
        ),
    {
        let frames = match self.bridge.encode_event(event, &self.catalog) {
            Ok(frames) => frames,
            Err(_) => return,
        };
        for frame in frames {
            let Ok(parsed_events) = self.parser.parse_wal_message(&frame, &self.catalog) else {
                continue;
            };
            for parsed in parsed_events {
                let result = self.engine.consumers(&parsed);
                sink(&result);
            }
        }
    }
}

#[cfg(feature = "sqlite-cdc")]
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
            // Cell::Null branch end-to-end, including the
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

#[cfg(feature = "sqlite-cdc")]
fn pick_status(u: &mut Unstructured<'_>) -> &'static str {
    const STATUSES: &[&str] = &["paid", "open", "closed", "pending"];
    let idx = u.int_in_range(0usize..=STATUSES.len() - 1).unwrap_or(0);
    STATUSES[idx]
}

#[cfg(feature = "sqlite-cdc")]
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

    fn cell_kind(cell: &Cell) -> u8 {
        match cell {
            Cell::Null => 0,
            Cell::Missing => 1,
            Cell::Bool(_) => 2,
            Cell::Int(_) => 3,
            Cell::Float(_) => 4,
            Cell::String(_) => 5,
        }
    }

    fn instruction_kind(instr: &Instruction) -> u8 {
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
    fn test_arb_cell_covers_all_variants() {
        let mut seen = BTreeSet::new();
        for seed in u8::MIN..=u8::MAX {
            let mut data = vec![0u8; 1024];
            data[0] = seed;
            let mut u = Unstructured::new(&data);
            if let Ok(cell) = arb_cell(&mut u) {
                seen.insert(cell_kind(&cell));
            }
        }

        assert_eq!(seen.len(), 6, "expected all Cell variants, saw {seen:?}");
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
