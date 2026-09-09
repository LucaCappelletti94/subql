//! Parked reads, for the re-execution connector tests.

use super::mysql::MysqlDatabase;
use super::pg::PgDatabase;
use diesel::prelude::*;
use std::time::{Duration, Instant};

/// Advisory-lock key the parked-read helper gates on. Advisory locks are
/// scoped to the database, so the constant is safe on a shared server.
const PARK_KEY: i64 = 4242;

/// Cross join that parks a read until [`park_a_read`] releases its gate.
///
/// Postgres fixes a statement's snapshot before it executes, so a read that
/// blocks here is already holding the snapshot it will answer from, and a
/// commit landing while it waits is invisible to it. Append it to the read's
/// SQL, after any `FROM`.
pub const PARK: &str = "CROSS JOIN pg_advisory_xact_lock(4242)";

/// The current WAL position, read from an ordinary connection.
fn current_wal_lsn(conn: &mut PgConnection) -> subql::PgLsn {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        v: String,
    }
    // Administrative function, which the query DSL does not express.
    let rows: Vec<Row> = diesel::sql_query("SELECT pg_current_wal_lsn()::text AS v")
        .load(conn)
        .expect("read the current WAL position");
    subql::PgLsn::parse(&rows[0].v).expect("parse the current WAL position")
}

diesel::table! {
    pg_catalog.pg_locks (pid) {
        pid -> Nullable<Integer>,
        locktype -> Text,
        database -> Nullable<Oid>,
        granted -> Bool,
    }
}

diesel::table! {
    pg_catalog.pg_database (oid) {
        oid -> Oid,
        datname -> Text,
    }
}

/// Backends of `database` blocked on an advisory lock, which is what a
/// parked read looks like from another connection.
fn parked_count(conn: &mut PgConnection, database: &str) -> i64 {
    let oid: u32 = pg_database::table
        .filter(pg_database::datname.eq(database))
        .select(pg_database::oid)
        .first(conn)
        .expect("resolve the database oid");
    pg_locks::table
        .filter(pg_locks::locktype.eq("advisory"))
        .filter(pg_locks::granted.eq(false))
        .filter(pg_locks::database.eq(oid))
        .count()
        .get_result(conn)
        .expect("read pg_locks")
}

/// Run `read` on another thread, commit `dml` while it is parked, and report
/// what it returned together with the position that commit landed at.
///
/// The read's SQL must carry [`PARK`], which holds it inside its own snapshot
/// until the commit is done. A position taken before the snapshot therefore
/// sits behind the returned one, and a position taken after sits at or ahead
/// of it, so the two orderings are told apart from outside the call.
pub fn park_a_read<T, F>(db: &PgDatabase, dml: &str, read: F) -> (T, subql::PgLsn)
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let mut gate = db.connect();
    let mut observer = db.connect();
    // Advisory lock function, which the query DSL does not express.
    diesel::sql_query(format!("SELECT pg_advisory_lock({PARK_KEY})"))
        .execute(&mut gate)
        .expect("take the gate");

    let reader = std::thread::spawn(read);
    let deadline = Instant::now() + Duration::from_secs(30);
    while parked_count(&mut observer, &db.name) == 0 {
        assert!(
            Instant::now() < deadline,
            "the read never parked on the gate, so its snapshot was never pinned"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    diesel::sql_query(dml)
        .execute(&mut observer)
        .expect("commit while the read is parked");
    let after_commit = current_wal_lsn(&mut observer);
    diesel::sql_query(format!("SELECT pg_advisory_unlock({PARK_KEY})"))
        .execute(&mut gate)
        .expect("release the gate");

    (reader.join().expect("parked read"), after_commit)
}

/// The current binlog coordinate, read from an ordinary connection.
fn current_binlog_pos(conn: &mut MysqlConnection) -> subql::MysqlBinlogPos {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        file: String,
        #[diesel(sql_type = diesel::sql_types::Unsigned<diesel::sql_types::BigInt>)]
        pos: u64,
    }
    // JSON extraction from a performance schema table, which the query DSL
    // does not express.
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT JSON_UNQUOTE(JSON_EXTRACT(LOCAL, '$.binary_log_file')) AS file, \
     CAST(JSON_EXTRACT(LOCAL, '$.binary_log_position') AS UNSIGNED) AS pos \
     FROM performance_schema.log_status",
    )
    .load(conn)
    .expect("read log_status");
    let file = rows[0]
        .file
        .rsplit('.')
        .next()
        .and_then(|s| s.parse().ok())
        .expect("binlog file suffix");
    subql::MysqlBinlogPos {
        file,
        pos: u32::try_from(rows[0].pos).expect("binlog offset fits u32"),
    }
}

/// MySQL peer of [`park_a_read`], gated on a named user-level lock.
///
/// `lock` must be unique per call: the parked read acquires it and its
/// session keeps it, so a reused name would park the next read on itself.
pub fn park_a_mysql_read<T, F>(
    db: &MysqlDatabase,
    lock: &str,
    dml: &str,
    read: F,
) -> (T, subql::MysqlBinlogPos)
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    // User locks are server wide, so the name carries the database.
    let lock = format!("{}_{lock}", db.name);
    let mut gate = db.connect();
    let mut observer = db.connect();
    // User lock function, which the query DSL does not express.
    diesel::sql_query(format!("SELECT GET_LOCK('{lock}', 60) AS n"))
        .execute(&mut gate)
        .expect("take the gate");

    let reader = std::thread::spawn(read);
    let deadline = Instant::now() + Duration::from_secs(30);
    while mysql_parked_count(&mut observer, &db.name) == 0 {
        assert!(
            Instant::now() < deadline,
            "the read never parked on the gate, so its snapshot was never pinned"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    diesel::sql_query(dml)
        .execute(&mut observer)
        .expect("commit while the read is parked");
    let after_commit = current_binlog_pos(&mut observer);
    diesel::sql_query(format!("SELECT RELEASE_LOCK('{lock}') AS n"))
        .execute(&mut gate)
        .expect("release the gate");

    (reader.join().expect("parked read"), after_commit)
}

diesel::table! {
    information_schema.processlist (id) {
        id -> Unsigned<BigInt>,
        db -> Nullable<Text>,
        state -> Nullable<Text>,
    }
}

/// Sessions of `database` blocked on a user-level lock.
fn mysql_parked_count(conn: &mut MysqlConnection, database: &str) -> i64 {
    processlist::table
        .filter(processlist::state.eq("User lock"))
        .filter(processlist::db.eq(database))
        .count()
        .get_result(conn)
        .expect("read processlist")
}
