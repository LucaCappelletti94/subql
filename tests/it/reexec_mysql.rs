//! Docker-backed integration test for [`AutoResolvingEngine`] +
//! [`MysqlDieselConnector`] against a real MySQL 8.0 with binary logging.
//!
//! Mirrors `tests/it/reexec_postgres.rs` for the MySQL backend. MySQL has no
//! in-tree CDC source (Maxwell lives outside the crate, exercised by
//! `tests/it/cdc_mysql_e2e.rs`), so the displacement test hand-builds the
//! event via [`TestEvent<MySql>`] instead of decoding one from a
//! replication stream. The point is the same: prove the connector
//! re-executes the captured query's SQL against live MySQL and decodes
//! a scalar [`Value`] correctly.
//!
//! Requires Docker. Tests are `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --test it reexec_mysql:: --features executor-diesel-mysql \
//!     -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used)]

use crate::common;

use diesel::{sql_query, MysqlConnection, RunQueryDsl};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::MySqlDialect;
use subql::backend::{MySql, ScalarFamily, Value};
use subql::reexec::{
    AutoResolvingEngine, Connector, MysqlDieselConnector, SessionSetup, SnapshotResult, SyncMode,
};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};

type Engine =
    AutoResolvingEngine<TestEvent<MySql>, DefaultIds, ParserDB, SyncMode<MysqlDieselConnector>>;

/// Catalog DDL shared by the engine + planner. `FLOAT` maps to subql's
/// `Float` scalar kind under the MySQL parser.
const DDL: &str =
    "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);";

/// MySQL-side DDL that actually runs on the server. Uses `DOUBLE` (MySQL's
/// 8-byte float) so the diesel `Nullable<Double>` row type decodes cleanly.
const MYSQL_DDL: &str = "CREATE TABLE orders (
    id INT PRIMARY KEY,
    price DOUBLE,
    quantity INT,
    status TEXT
)";

fn setup_mysql(conn: &mut MysqlConnection, seed: &[(i64, f64)]) {
    sql_query(MYSQL_DDL).execute(conn).expect("CREATE TABLE");
    for (id, price) in seed {
        sql_query(format!(
            "INSERT INTO orders (id, price, quantity, status) \
             VALUES ({id}, {price}, 1, 'paid')"
        ))
        .execute(conn)
        .expect("seed insert");
    }
}

/// One more row, for a commit that lands while a read is parked.
fn insert(id: i64) -> String {
    format!("INSERT INTO orders (id, price, quantity, status) VALUES ({id}, 7.0, 1, 'paid')")
}

fn catalog() -> ParserDB {
    ParserDB::parse::<MySqlDialect>(DDL).expect("parse DDL")
}

fn build_engine(catalog: ParserDB, conn_exec: MysqlConnection) -> Engine {
    let inner =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, ParserDB>::new(catalog, MySqlDialect {});
    AutoResolvingEngine::new(inner, SyncMode(MysqlDieselConnector::new(conn_exec)))
}

/// Build the full `orders` row image for the hand-built delete event.
/// Column order matches the catalog: id=0, price=1, quantity=2, status=3.
fn orders_row(id: i64, price: f64) -> Vec<Value<MySql>> {
    vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(1),
        Value::String("paid".into()),
    ]
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn scaffold_registers_both_and_executes_scalar() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let conn_exec = common::mysql_connect(port);

    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

    let engine_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(
                1u64,
                "SELECT * FROM orders WHERE price > 8.0",
            ),
            (),
        )
        .expect("engine-supported registration");
    match engine_reg {
        Registered {
            tier: Tier::InProcess(_),
            ..
        } => {}
        other => panic!("expected the Engine variant, got {other:?}"),
    }

    let captured_reg = engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(2u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration");
    match captured_reg {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => {
            assert!(subql::Install::install(
                &mut engine,
                subscription_id,
                subql::ScalarInstall {
                    value: Value::Float(5.0),
                    checkpoint: None::<subql::NoCheckpoint>
                }
            )
            .is_ok());
        }
        other => panic!("expected ReExec variant, got {other:?}"),
    }

    let (value, _checkpoint) = engine
        .connector()
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds("SELECT MIN(price) AS v FROM orders"),
            ScalarFamily::Float,
            &(),
        )
        .expect("connector executes");
    assert_eq!(value, Value::Float(5.0), "live MIN(price) decode");
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn snapshot_reads_value_and_binlog_pos_from_mysql() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let conn_exec = common::mysql_connect(port);

    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let mut engine = build_engine(catalog(), conn_exec);

    let captured_qid = match engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };

    let snap = engine
        .snapshot(captured_qid)
        .expect("snapshot")
        .expect("subscription_id exists");
    let (value, checkpoint) = match snap {
        SnapshotResult::Scalar(value, checkpoint) => (value, checkpoint),
        other => panic!("unexpected snapshot variant: {other:?}"),
    };
    assert_eq!(value, Value::Float(5.0), "MIN(price) snapshot value");

    let pos = checkpoint.expect("MysqlDieselConnector must report a binlog checkpoint");
    assert!(
        pos.pos > 0,
        "SHOW MASTER STATUS byte offset should be non-zero on a live server, got {pos:?}"
    );
}

#[test]
#[ignore = "requires Docker; run with --ignored"]
fn delete_displacing_extreme_resolves_via_mysql_connector() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut conn_setup = common::mysql_connect(port);
    let mut conn_dml = common::mysql_connect(port);
    let conn_exec = common::mysql_connect(port);

    setup_mysql(&mut conn_setup, &[(1, 5.0), (2, 9.0)]);

    let cat = catalog();
    let table_id: TableId = catalog_helpers::table_id(&cat, "orders").expect("resolve orders");

    let mut engine = build_engine(cat, conn_exec);

    let captured_qid = match engine
        .register(
            SubscriptionRequest::<DefaultIds, MySql>::new(1u64, "SELECT MIN(price) FROM orders"),
            (),
        )
        .expect("captured registration")
    {
        Registered {
            subscription_id,
            tier: Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected ReExec, got {other:?}"),
    };
    assert!(subql::Install::install(
        &mut engine,
        captured_qid,
        subql::ScalarInstall {
            value: Value::Float(5.0),
            checkpoint: None::<subql::NoCheckpoint>
        }
    )
    .is_ok());

    sql_query("DELETE FROM orders WHERE id = 1")
        .execute(&mut conn_dml)
        .expect("delete id=1");

    let event = TestEvent::<MySql>::delete(table_id, orders_row(1, 5.0)).with_pk_columns([0u16]);

    engine.apply(&event).expect("apply");
    let notifs = engine.resolve_collect().expect("consumers dispatch");
    assert_eq!(
        notifs.scalar_updates.len(),
        1,
        "expected exactly one ScalarUpdate, got {}",
        notifs.scalar_updates.len()
    );
    assert_eq!(notifs.scalar_updates[0].subscription_id, captured_qid);
    assert_eq!(
        notifs.scalar_updates[0].value,
        Value::Float(9.0),
        "connector re-queried live MySQL for the new MIN"
    );
    assert!(
        notifs.transitions.is_empty(),
        "a plain scalar re-read changes no tier"
    );
}

/// The multi-column aggregate seed decodes correctly through the real MySQL
/// connector. MySQL promotes `SUM` over an integer column to `DECIMAL`, so
/// the connector's `CAST(... AS DOUBLE)` must still decode the sum and
/// sum-of-squares as `f64` (the case in-memory SQLite cannot exercise).
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn execute_scalar_row_decodes_integer_aggregate_seed() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let mut setup = common::mysql_connect(port);
    sql_query("CREATE TABLE nums (id INT PRIMARY KEY, amount INT)")
        .execute(&mut setup)
        .expect("CREATE TABLE nums");
    for (id, amount) in [(1, 2), (2, 4), (3, 6)] {
        sql_query(format!(
            "INSERT INTO nums (id, amount) VALUES ({id}, {amount})"
        ))
        .execute(&mut setup)
        .expect("seed insert");
    }

    let db = ParserDB::parse::<MySqlDialect>("CREATE TABLE nums (id INT PRIMARY KEY, amount INT);")
        .expect("parse nums DDL");
    let mut engine =
        SubscriptionEngine::<TestEvent<MySql>, DefaultIds, ParserDB>::new(db, MySqlDialect {});
    let bundle = engine
        .register(SubscriptionRequest::<DefaultIds, MySql>::new(
            1u64,
            "SELECT VAR_POP(amount) FROM nums",
        ))
        .expect("register aggregate")
        .served()
        .expect("the engine maintains this one in process")
        .aggregate_bootstrap
        .clone()
        .expect("aggregate carries a bootstrap");

    // sum=12, squared deviations=8, count=3, decoded into
    // (Float, Float, Int).
    //
    // Eight, not fifty-six. The seed's middle component is the sum of
    // squared deviations, read as `VAR_POP(amount) * COUNT(amount)`,
    // because PostgreSQL and MySQL both have `VAR_POP` and answering
    // from their own variance is what keeps the seed agreeing with them
    // to the last digit. A sum of squares would be 56 and is what
    // SQLite's seed carries instead, since it has no variance function
    // at all.
    let connector = MysqlDieselConnector::new(common::mysql_connect(port));
    let (row, _checkpoint) = connector
        .execute_scalar_row(&bundle.query.as_read_query(), &bundle.kinds, &())
        .expect("execute_scalar_row");
    assert_eq!(
        row,
        vec![
            Value::Decimal(bigdecimal::BigDecimal::from(12)),
            Value::Float(8.0),
            Value::Int(3)
        ]
    );
}

/// Every read reports a position taken before its own snapshot opened.
///
/// The contract a caller replays from: a position at or behind the snapshot
/// re-delivers changes the snapshot already holds, while a position ahead of
/// it silently drops a transaction the snapshot never saw. A user-level lock
/// parks each read inside its own snapshot, a commit lands while it waits, and
/// the returned coordinate has to sit behind that commit. The row count is
/// asserted too, because a read that somehow saw the commit would make the
/// coordinate comparison meaningless.
///
/// A connector each, because this one owns its connection outright and a
/// parked read holds it for the length of the park.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn every_read_reports_a_position_taken_before_its_snapshot() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);
    let mut conn = common::mysql_connect(port);
    setup_mysql(&mut conn, &[(1, 5.0)]);

    // Each lock name carries a column, so MySQL cannot fold the condition to a
    // constant and take the lock before its read view exists. The gate holds
    // the name the lowest id builds, which a clustered-index scan reaches
    // first.
    let sql = "SELECT count(*) AS v FROM orders WHERE GET_LOCK(CONCAT('park_scalar_', id), 60) = 1";
    let ((value, position), after_commit) =
        common::park_a_mysql_read(port, "park_scalar_1", &insert(2), move || {
            MysqlDieselConnector::new(common::mysql_connect(port))
                .execute_scalar(
                    &subql::reexec::ReadQuery::without_binds(sql),
                    ScalarFamily::Int,
                    &(),
                )
                .expect("scalar read")
        });
    assert_eq!(
        value,
        Value::Int(1),
        "the scalar read's snapshot holds one row"
    );
    assert!(
        position.expect("binary logging is on, so a coordinate is reported") < after_commit,
        "the scalar read's position must sit behind the commit at {after_commit:?}"
    );

    let sql = "SELECT id FROM orders WHERE GET_LOCK(CONCAT('park_page_', id), 60) = 1 ORDER BY id";
    let (page, after_commit) =
        common::park_a_mysql_read(port, "park_page_1", &insert(3), move || {
            MysqlDieselConnector::new(common::mysql_connect(port))
                .read_page(&subql::reexec::ReadQuery::without_binds(sql), 1 << 20, &())
                .expect("page read")
        });
    assert_eq!(
        page.value.rows.len(),
        2,
        "the page's snapshot holds two rows"
    );
    assert!(
        page.checkpoint
            .expect("binary logging is on, so a coordinate is reported")
            < after_commit,
        "the page read's position must sit behind the commit at {after_commit:?}"
    );

    let sql = "SELECT count(*) AS c0 FROM orders WHERE GET_LOCK(CONCAT('park_seed_', id), 60) = 1";
    let ((values, position), after_commit) =
        common::park_a_mysql_read(port, "park_seed_1", &insert(4), move || {
            MysqlDieselConnector::new(common::mysql_connect(port))
                .execute_scalar_row(
                    &subql::reexec::ReadQuery::without_binds(sql),
                    &[ScalarFamily::Int],
                    &(),
                )
                .expect("seed read")
        });
    assert_eq!(
        values,
        vec![Value::Int(3)],
        "the seed read's snapshot holds three rows"
    );
    assert!(
        position.expect("binary logging is on, so a coordinate is reported") < after_commit,
        "the seed read's position must sit behind the commit at {after_commit:?}"
    );
}

/// A borrowed-list session setup, mirroring what a caller builds per read.
struct MarkerSetup(Vec<String>);

impl SessionSetup for MarkerSetup {
    fn setup_statements(&self) -> &[String] {
        &self.0
    }
}

/// The session-setup seam runs its statements inside each read's transaction on
/// the single-connection sync `MysqlDieselConnector`. Observed through a
/// session variable the read reads back: a non-empty setup runs before the
/// caller's SQL, an empty one on a fresh connection runs nothing.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn session_setup_runs_on_each_read_sync_mysql() {
    common::assert_docker_available();
    let container = common::mysql_8();
    let port = common::mysql_port(&container);

    let read_marker = "SELECT @@max_sort_length";
    let setup = MarkerSetup(vec!["SET SESSION max_sort_length = 1234".to_string()]);
    let connector: MysqlDieselConnector<MarkerSetup> =
        MysqlDieselConnector::with_session_setup(common::mysql_connect(port));

    let (value, _) = connector
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            ScalarFamily::Int,
            &setup,
        )
        .expect("scalar read");
    assert_eq!(
        value,
        Value::Int(1234),
        "execute_scalar ran the setup first"
    );

    let page = connector
        .read_page(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            4096,
            &setup,
        )
        .expect("page read");
    assert_eq!(
        page.value.rows[0][0],
        Value::Int(1234),
        "read_page ran the setup first"
    );

    let plain = MysqlDieselConnector::new(common::mysql_connect(port));
    let (value, _) = plain
        .execute_scalar(
            &subql::reexec::ReadQuery::without_binds(read_marker),
            ScalarFamily::Int,
            &(),
        )
        .expect("scalar read");
    assert_ne!(value, Value::Int(1234), "an empty setup runs nothing");
}
