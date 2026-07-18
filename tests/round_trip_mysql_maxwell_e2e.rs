//! Full CDC round-trip integration test on the Maxwell (MySQL) vehicle.
//!
//! Mirrors the Postgres round trips (`common::dispatch`) from the MySQL
//! angle. A MySQL source feeds a Maxwell daemon (file producer), subql
//! folds the Maxwell events into a patchset, applies it to a SQLite
//! replica through [`SqliteAdapter`], captures the SQLite session
//! patchset, and re-applies it to a fresh MySQL through [`MysqlAdapter`],
//! asserting row parity at every step.
//!
//! Two phases so update and delete are real patchset ops: a seed phase
//! inserts three rows, then a mutate phase updates one and deletes another,
//! matched on a `BINARY(16)` UUID primary key.
//!
//! Dispatched columns exercise the MySQL type surface: `active BOOLEAN`
//! (native bool), `id`/`token BINARY(16)` (UUID as a compact 16-byte
//! blob, no `Uuid` wire type and no SQL cast), and `feeling ENUM(...)`
//! (an inline enum that flows as text, the MySQL analog of the Postgres
//! domain and enum). No SQL casts anywhere: native diesel binds only.
//!
//! Docker-backed. Run with:
//!
//! ```sh
//! cargo test --test round_trip_mysql_maxwell_e2e \
//!     --features "apply-patchset-mysql apply-patchset-sqlite sqlite-cdc" \
//!     -- --ignored --nocapture
//! ```

#![cfg(all(
    feature = "apply-patchset-mysql",
    feature = "apply-patchset-sqlite",
    feature = "sqlite-cdc"
))]
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use diesel::sql_types::{BigInt, Binary, Bool, Integer, Nullable, Text};
use diesel::{
    sql_query, Connection, MysqlConnection, QueryableByName, RunQueryDsl, SqliteConnection,
};
use diesel_sqlite_session::SqliteSessionExt;
use sql_traits::structs::ParserDB;
use sqlite_diff_rs::PatchSet;
use sqlparser::dialect::{MySqlDialect, SQLiteDialect};
use uuid::Uuid;

use subql::backend::SQLite as SqliteBackend;
use subql::emit::{maxwell_patchset_builder, WireTable};
use subql::patchset::{MysqlAdapter, SqliteAdapter};
use subql::testing::TestEvent;
use subql::{parse_maxwell, DefaultIds, MaxwellMessage, SubscriptionEngine};

// UUID stored as BINARY(16). MySQL has no native UUID type, so the column
// classifies as bytes and the 16-byte blob rides WireType::Bytes.
const MYSQL_DDL: &str = "CREATE TABLE orders (id BINARY(16) PRIMARY KEY, amount INT, status VARCHAR(255), active BOOLEAN, token BINARY(16), feeling ENUM('happy','sad','neutral'), note VARCHAR(255))";
// The subql catalog only needs the table shape. `feeling` is an unknown
// scalar (the enum), so subql treats it as text on the wire.
const SUBQL_MYSQL_DDL: &str = "CREATE TABLE orders (id BINARY(16) PRIMARY KEY, amount INT, status VARCHAR(255), active BOOLEAN, token BINARY(16), feeling ENUM('happy','sad','neutral'), note VARCHAR(255));";
const SQLITE_DDL: &str = "CREATE TABLE orders (id BLOB PRIMARY KEY, amount INTEGER, status TEXT, active INTEGER, token BLOB, feeling TEXT, note TEXT);";

const ID_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ID_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const ID_C: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const TOKEN_A: &str = "11111111-1111-1111-1111-111111111111";
const TOKEN_B: &str = "22222222-2222-2222-2222-222222222222";
const TOKEN_C: &str = "33333333-3333-3333-3333-333333333333";

#[derive(QueryableByName, Debug, PartialEq)]
struct MyOrder {
    #[diesel(sql_type = Binary)]
    id: Vec<u8>,
    #[diesel(sql_type = Integer)]
    amount: i32,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Bool)]
    active: bool,
    #[diesel(sql_type = Binary)]
    token: Vec<u8>,
    #[diesel(sql_type = Text)]
    feeling: String,
    #[diesel(sql_type = Nullable<Text>)]
    note: Option<String>,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct SqliteOrder {
    #[diesel(sql_type = Binary)]
    id: Vec<u8>,
    #[diesel(sql_type = BigInt)]
    amount: i64,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = BigInt)]
    active: i64,
    #[diesel(sql_type = Binary)]
    token: Vec<u8>,
    #[diesel(sql_type = Text)]
    feeling: String,
    #[diesel(sql_type = Nullable<Text>)]
    note: Option<String>,
}

/// The 16 UUID bytes as a 32-char lowercase hex string for a MySQL
/// `x'...'` binary literal (no cast, no `UNHEX`).
fn hex16(uuid_str: &str) -> String {
    Uuid::parse_str(uuid_str).unwrap().simple().to_string()
}

fn uuid_bytes(uuid_str: &str) -> Vec<u8> {
    Uuid::parse_str(uuid_str).unwrap().as_bytes().to_vec()
}

fn subql_catalog() -> ParserDB {
    ParserDB::parse::<MySqlDialect>(SUBQL_MYSQL_DDL).unwrap()
}

fn load_mysql(my: &mut MysqlConnection) -> Vec<MyOrder> {
    sql_query("SELECT id, amount, status, active, token, feeling, note FROM orders ORDER BY id")
        .load(my)
        .unwrap()
}

fn load_sqlite(sqlite: &mut SqliteConnection) -> Vec<SqliteOrder> {
    sql_query("SELECT id, amount, status, active, token, feeling, note FROM orders ORDER BY id")
        .load(sqlite)
        .unwrap()
}

fn seed_dml(my: &mut MysqlConnection) {
    for (id, amount, status, active, token, feeling, note) in [
        (ID_A, 100, "new", "true", TOKEN_A, "happy", "NULL"),
        (ID_B, 200, "new", "false", TOKEN_B, "sad", "'packed'"),
        (ID_C, 300, "new", "true", TOKEN_C, "neutral", "'void'"),
    ] {
        let stmt = format!(
            "INSERT INTO orders (id, amount, status, active, token, feeling, note) VALUES (x'{}', {amount}, '{status}', {active}, x'{}', '{feeling}', {note})",
            hex16(id),
            hex16(token)
        );
        sql_query(&stmt).execute(my).unwrap();
    }
}

fn mutate_dml(my: &mut MysqlConnection) {
    sql_query(format!(
        "UPDATE orders SET amount = 250, status = 'shipped', active = true, feeling = 'neutral', note = NULL WHERE id = x'{}'",
        hex16(ID_B)
    ))
    .execute(my)
    .unwrap();
    sql_query(format!("DELETE FROM orders WHERE id = x'{}'", hex16(ID_C)))
        .execute(my)
        .unwrap();
}

fn seed_mysql_rows() -> Vec<MyOrder> {
    vec![
        MyOrder {
            id: uuid_bytes(ID_A),
            amount: 100,
            status: "new".to_owned(),
            active: true,
            token: uuid_bytes(TOKEN_A),
            feeling: "happy".to_owned(),
            note: None,
        },
        MyOrder {
            id: uuid_bytes(ID_B),
            amount: 200,
            status: "new".to_owned(),
            active: false,
            token: uuid_bytes(TOKEN_B),
            feeling: "sad".to_owned(),
            note: Some("packed".to_owned()),
        },
        MyOrder {
            id: uuid_bytes(ID_C),
            amount: 300,
            status: "new".to_owned(),
            active: true,
            token: uuid_bytes(TOKEN_C),
            feeling: "neutral".to_owned(),
            note: Some("void".to_owned()),
        },
    ]
}

// Net state after the mutate: id_b updated (amount, status, active,
// feeling, and note cleared to NULL; token unchanged), id_c deleted. id_a
// keeps its NULL note from the seed, so a NULL survives both an insert and
// an update across the whole loop.
fn final_mysql_rows() -> Vec<MyOrder> {
    vec![
        MyOrder {
            id: uuid_bytes(ID_A),
            amount: 100,
            status: "new".to_owned(),
            active: true,
            token: uuid_bytes(TOKEN_A),
            feeling: "happy".to_owned(),
            note: None,
        },
        MyOrder {
            id: uuid_bytes(ID_B),
            amount: 250,
            status: "shipped".to_owned(),
            active: true,
            token: uuid_bytes(TOKEN_B),
            feeling: "neutral".to_owned(),
            note: None,
        },
    ]
}

fn sqlite_view(row: &MyOrder) -> SqliteOrder {
    SqliteOrder {
        id: row.id.clone(),
        amount: i64::from(row.amount),
        status: row.status.clone(),
        active: i64::from(row.active),
        token: row.token.clone(),
        feeling: row.feeling.clone(),
        note: row.note.clone(),
    }
}

fn seed_sqlite_rows() -> Vec<SqliteOrder> {
    seed_mysql_rows().iter().map(sqlite_view).collect()
}

fn final_sqlite_rows() -> Vec<SqliteOrder> {
    final_mysql_rows().iter().map(sqlite_view).collect()
}

fn parse_lines(lines: &[String]) -> Vec<MaxwellMessage> {
    let mut events = Vec::new();
    for line in lines {
        events.extend(parse_maxwell(line.as_bytes()).unwrap());
    }
    events
}

/// Apply the seed patchset to a fresh SQLite replica, record the mutate in
/// a session, capture that session patchset, and re-apply it to a
/// re-seeded MySQL through [`MysqlAdapter`], asserting parity at each step.
fn finish_loop(
    my: &mut MysqlConnection,
    seed: &PatchSet<WireTable, String, Vec<u8>>,
    mutate: &PatchSet<WireTable, String, Vec<u8>>,
) {
    // MySQL net state after the mutate, captured before the round trip.
    let source_net = load_mysql(my);
    assert_eq!(source_net, final_mysql_rows(), "source net state");

    // ---- Outgoing: seed then mutate the SQLite replica ----
    let mut sqlite = SqliteConnection::establish(":memory:").unwrap();
    sql_query(SQLITE_DDL).execute(&mut sqlite).unwrap();
    let sqlite_catalog = ParserDB::parse::<SQLiteDialect>(SQLITE_DDL).unwrap();
    let sqlite_engine: SubscriptionEngine<TestEvent<SqliteBackend>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(sqlite_catalog, SQLiteDialect {});
    let sqlite_adapter = SqliteAdapter::new(sqlite_engine.database());

    sqlite_engine
        .apply_patchset(seed, &mut sqlite, &sqlite_adapter)
        .unwrap();
    assert_eq!(
        load_sqlite(&mut sqlite),
        seed_sqlite_rows(),
        "SQLite replica after seed"
    );

    let mut session = sqlite.create_session().unwrap();
    session.attach_all().unwrap();

    sqlite_engine
        .apply_patchset(mutate, &mut sqlite, &sqlite_adapter)
        .unwrap();
    assert_eq!(
        load_sqlite(&mut sqlite),
        final_sqlite_rows(),
        "SQLite replica after mutate (update and delete applied)"
    );

    // ---- Incoming: capture the session patchset and re-apply to MySQL ----
    let session_patchset = session.patchset().unwrap();
    assert!(!session_patchset.is_empty(), "session recorded no changes");

    // Re-seed a fresh MySQL, then re-apply the captured session patchset
    // (a genuine update and delete) through the production inbound API,
    // which reconstructs the ops from the raw bytes.
    sql_query("TRUNCATE orders").execute(my).unwrap();

    let my_engine: SubscriptionEngine<MaxwellMessage, DefaultIds, ParserDB> =
        SubscriptionEngine::new(subql_catalog(), MySqlDialect {});
    let my_adapter = MysqlAdapter::new(my_engine.database());

    my_engine.apply_patchset(seed, my, &my_adapter).unwrap();
    assert_eq!(load_mysql(my), seed_mysql_rows(), "MySQL after re-seed");

    my_engine
        .apply_patchset_bytes(&session_patchset, my, &my_adapter)
        .unwrap();
    assert_eq!(
        load_mysql(my),
        source_net,
        "round-tripped MySQL rows equal the source net state"
    );
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn round_trip_maxwell_dispatches_bool_uuid_enum() {
    common::assert_docker_available();

    let pid = std::process::id();
    let network = format!("subql-rt-mx-{pid}");
    let mysql_name = format!("subql-rt-mysql-{pid}");

    // Maxwell bind-mounts this dir; it must be world-writable so the
    // in-container process can write into it.
    let maxwell_dir = tempfile::tempdir().unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(maxwell_dir.path(), std::fs::Permissions::from_mode(0o777))
            .unwrap();
    }
    let out = maxwell_dir.path().to_str().unwrap().to_owned();

    let mysql = common::mysql_networked(&network, &mysql_name);
    // Give MySQL a moment to settle before Maxwell connects.
    std::thread::sleep(Duration::from_secs(2));
    let _maxwell = common::start_maxwell(&network, &mysql_name, &out);

    let port = common::mysql_port(&mysql);
    let mut my = common::mysql_connect(port);

    sql_query(MYSQL_DDL).execute(&mut my).unwrap();
    // Let Maxwell observe the DDL before any DML.
    std::thread::sleep(Duration::from_secs(3));

    let catalog = subql_catalog();

    // Seed phase: three inserts.
    seed_dml(&mut my);
    let seed_lines = common::maxwell_collect(&out, "orders", 3);
    let seed_events = parse_lines(&seed_lines[..3]);
    let seed_builder = maxwell_patchset_builder(&catalog, &seed_events).unwrap();
    assert!(!seed_events.is_empty(), "seed drain yielded no events");

    // Mutate phase: one update and one delete on top of the seeded rows.
    mutate_dml(&mut my);
    let all_lines = common::maxwell_collect(&out, "orders", 5);
    let mutate_events = parse_lines(&all_lines[3..]);
    let mutate_builder = maxwell_patchset_builder(&catalog, &mutate_events).unwrap();
    assert!(!mutate_events.is_empty(), "mutate drain yielded no events");

    finish_loop(&mut my, &seed_builder, &mutate_builder);
}
