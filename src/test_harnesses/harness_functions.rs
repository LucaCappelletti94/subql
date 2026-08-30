#![allow(
    clippy::manual_let_else,
    clippy::too_long_first_doc_paragraph,
    clippy::items_after_statements,
    clippy::needless_pass_by_value,
    clippy::map_entry,
    clippy::match_same_arms
)]

use arbitrary::{Arbitrary, Unstructured};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::{Postgres, RowKind, Value};
use crate::compiler::bytecode::{BytecodeProgram, Instruction};
use crate::compiler::canonicalize::{hash_sql, normalize_sql};
use crate::compiler::parser::parse_and_compile;
use crate::compiler::vm::Vm;
use crate::persistence::codec;
use crate::persistence::shard::{deserialize_shard, ShardPayload};
use crate::testing::TestEvent;
use crate::wal::{parse_maxwell, parse_wal2json_v1, parse_wal2json_v2};
use crate::DefaultIds;

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
pub fn arb_value(u: &mut Unstructured<'_>) -> arbitrary::Result<Value<Postgres>> {
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
pub fn arb_instruction(u: &mut Unstructured<'_>) -> arbitrary::Result<Instruction<Postgres>> {
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
