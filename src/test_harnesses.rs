//! Shared fuzz harness functions.
//!
//! Each `harness_*` function takes raw bytes and exercises a library subsystem.
//! The contract: errors are fine, **panics are bugs**.
//!
//! This module is only compiled under `#[cfg(any(feature = "testing", test))]`.

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
/// Declares an `orders` table with a wide selection of column names that
/// are commonly produced by the SQL fuzzer (`amount`, `status`, `id`, plus
/// a generous bank of generic `c0`-`c15` columns). The fuzzer may still
/// feed SQL that references columns/tables not in this fixture — those
/// inputs simply fail SQL resolution, which is fine for crash testing.
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

/// Parse SQL using PostgreSqlDialect — the dialect subql's documented
/// CDC pipeline targets.
///
/// We deliberately do NOT also fuzz with `GenericDialect`: it accepts
/// many more parse paths (PG/MySQL/MSSQL/etc. tokens all valid), which
/// makes it prone to deep backtracking on adversarial input. That's an
/// upstream sqlparser perf characteristic, not subql code we'd want to
/// surface, and previous fuzz timeouts traced back to it.
pub fn harness_parse_sql(data: &[u8]) {
    // Real callers pass valid UTF-8 SQL. `from_utf8_lossy` would
    // introduce U+FFFD bytes that drive sqlparser into pathological
    // backtracking — code paths unreachable in production.
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

/// Normalize and hash SQL, asserting determinism. PostgreSqlDialect only —
/// see `harness_parse_sql` for the rationale.
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
#[derive(Debug, Arbitrary)]
enum AggOp {
    Insert {
        id: u8,
        amount: Option<i32>,
        status: Option<u8>,
    },
    Update {
        id: u8,
        amount: Option<i32>,
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
    fn from_op(amount: Option<i32>, status: Option<u8>) -> Self {
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

/// Build the `agg_catalog()` `ParserDB` once — three columns, single-
/// column INT PK. Distinct from `fuzz_catalog()` so the column-id
/// mapping is predictable (id=0, amount=1, status=2).
fn agg_catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
    )
    .expect("agg fuzz fixture DDL parses")
}

/// Drive an arbitrary sequence of insert/update/delete operations against
/// a fixed agg-only consumer set and assert that the engine's incremental
/// `aggregate_deltas` output matches a from-scratch oracle.
///
/// Catches drift in:
/// - `COUNT(*)` (`AggDelta::Count`) — should equal current virtual-table size.
/// - `SUM(amount)` (`AggDelta::Sum`) — should equal sum of non-NULL amounts.
///
/// Contract: panics are bugs. Assertion failures are bugs.
pub fn harness_aggregate_consistency(data: &[u8]) {
    let mut u = Unstructured::new(data);
    let Ok(ops): arbitrary::Result<Vec<AggOp>> = (|| {
        let n = u.int_in_range(0usize..=64)?;
        (0..n).map(|_| AggOp::arbitrary(&mut u)).collect()
    })() else {
        return;
    };

    let database = Arc::new(agg_catalog());
    let Some(table_id) = catalog_helpers::table_id(database.as_ref(), "orders") else {
        return;
    };
    let pk_col = match catalog_helpers::column_id(database.as_ref(), table_id, "id") {
        Some(c) => c,
        None => return,
    };

    let mut engine: SubscriptionEngine<PostgreSqlDialect, DefaultIds, ParserDB> =
        SubscriptionEngine::new(Arc::clone(&database), PostgreSqlDialect {});

    // Register one COUNT(*) consumer (cid=1) and one SUM(amount) consumer
    // (cid=2). Both registrations may fail under odd dialect quirks; bail
    // cleanly rather than asserting.
    if engine
        .register(SubscriptionRequest::<DefaultIds>::new(
            1,
            "SELECT COUNT(*) FROM orders",
        ))
        .is_err()
    {
        return;
    }
    if engine
        .register(SubscriptionRequest::<DefaultIds>::new(
            2,
            "SELECT SUM(amount) FROM orders",
        ))
        .is_err()
    {
        return;
    }

    // Engine-side running state per consumer.
    let mut engine_count: i64 = 0;
    let mut engine_sum: f64 = 0.0;

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

        let deltas = match engine.aggregate_deltas(&event) {
            Ok(d) => d,
            Err(_) => return,
        };
        for (cid, delta) in deltas {
            match (cid, delta) {
                (1, AggDelta::Count(d)) => engine_count += d,
                (2, AggDelta::Sum(d)) => engine_sum += d,
                _ => {}
            }
        }

        // Oracle: COUNT(*) is virtual-table size; SUM(amount) sums
        // non-NULL amounts.
        let oracle_count = i64::try_from(virt.len()).unwrap_or(i64::MAX);
        let oracle_sum: f64 = virt
            .values()
            .filter_map(|r| r.amount)
            .map(|v| v as f64)
            .sum();

        assert_eq!(
            engine_count, oracle_count,
            "COUNT(*) drift: engine={engine_count} oracle={oracle_count}"
        );
        let tolerance = 1e-9_f64.max(oracle_sum.abs() * 1e-12);
        assert!(
            (engine_sum - oracle_sum).abs() <= tolerance,
            "SUM(amount) drift: engine={engine_sum} oracle={oracle_sum}"
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
// Regression tests — replay crash files from tests/crashes/{harness_name}/
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
            Err(_) => return, // directory missing — nothing to replay
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
