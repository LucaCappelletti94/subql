//! Generated filters against the engines themselves.
//!
//! The grammar in `subql::test_harnesses::predicate_grammar` draws the filter
//! as well as the row, so a shape nobody listed in [`super::generators`] is
//! compared too. The libFuzzer target `fuzz_predicate_verdict_sqlite` drives
//! the same grammar against SQLite at full speed, and a failing byte string
//! here replays there unchanged.

use core::cell::{Cell, RefCell};

use diesel::{QueryDsl as _, RunQueryDsl as _};
use proptest::collection::vec;
use proptest::prelude::any;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};
use subql::test_harnesses::predicate_grammar::{self, subql_selects, t, Case, Engine};

/// Where a failing byte string is recorded, apart from the shipped sweep's
/// file so neither replays the other's seeds.
const REGRESSIONS: &str = "tests/it/differential/generated.proptest-regressions";

/// Cases per run: `SUBQL_SWEEP_ROWS` times fifty, so the pull-request depth
/// is 2400 and the weekly one 30000.
fn cases() -> u32 {
    std::env::var("SUBQL_SWEEP_ROWS")
        .ok()
        .and_then(|rows| rows.parse::<u32>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(48)
        .saturating_mul(50)
}

/// Store the row and count what the engine keeps. `None` when it refuses.
macro_rules! engine_selects {
    ($connection:expr, $case:expr, $engine:expr) => {{
        let connection = &mut *$connection;
        (|| {
            diesel::delete(t::table).execute(connection).ok()?;
            diesel::insert_into(t::table)
                .values($case.row.values())
                .execute(connection)
                .ok()?;
            let kept: i64 = t::table
                .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
                    &$case.filter.render($engine),
                ))
                .count()
                .get_result(connection)
                .ok()?;
            Some(kept == 1)
        })()
    }};
}

/// Run the generated cases against one engine and report how many subql
/// served and compared.
macro_rules! generated_filters_agree {
    ($connection:expr, $engine:expr, $backend:ty, $dialect:ty) => {{
        let connection = &mut $connection;
        // DDL, which the typed DSL does not build.
        diesel::sql_query("DROP TABLE IF EXISTS t")
            .execute(connection)
            .expect("the case table drops");
        diesel::sql_query(predicate_grammar::ddl($engine))
            .execute(connection)
            .expect("the case table is created");
        let catalog = predicate_grammar::catalog::<$dialect>($engine);
        let mut runner = TestRunner::new(Config {
            cases: cases(),
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(REGRESSIONS))),
            ..Config::default()
        });
        let connection = RefCell::new(connection);
        let compared = Cell::new(0u32);
        let outcome = runner.run(&vec(any::<u8>(), 64..512), |bytes| {
            let Ok(case) = Case::arbitrary(&mut arbitrary::Unstructured::new(&bytes)) else {
                return Ok(());
            };
            let Some(served) = subql_selects::<$backend>(&case, $engine, catalog.clone()) else {
                return Ok(());
            };
            let Some(kept) = engine_selects!(*connection.borrow_mut(), case, $engine) else {
                return Ok(());
            };
            compared.set(compared.get() + 1);
            if served == kept {
                Ok(())
            } else {
                Err(TestCaseError::fail(format!(
                    "subql serves this filter with a different answer: served {served}, \
                     the engine keeps {kept}\n{}",
                    case.reproduction($engine)
                )))
            }
        });
        if let Err(failure) = outcome {
            panic!("{failure}");
        }
        compared.get()
    }};
}

/// SQLite, in memory, so every run of the suite compares generated filters.
#[test]
fn generated_filters_agree_with_sqlite() {
    use diesel::Connection as _;

    let mut connection = diesel::SqliteConnection::establish(":memory:")
        .expect("an in-memory SQLite database opens");
    let compared = generated_filters_agree!(
        connection,
        Engine::Sqlite,
        subql::backend::SQLite,
        sqlparser::dialect::SQLiteDialect
    );
    assert!(compared > 0, "no generated filter was served and compared");
}

/// PostgreSQL, which refuses most coercions MySQL and SQLite perform.
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn generated_filters_agree_with_postgres() {
    let db = crate::common::pg_database();
    let mut connection = db.connect();
    let compared = generated_filters_agree!(
        connection,
        Engine::Postgres,
        subql::backend::Postgres,
        sqlparser::dialect::PostgreSqlDialect
    );
    assert!(compared > 0, "no generated filter was served and compared");
}

/// MySQL, whose coercions and default collation no other engine shares.
#[cfg(any(
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "executor-diesel-postgres-r2d2",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql",
    feature = "diesel-typed-mysql",
    feature = "apply-patchset-mysql",
    feature = "apply-patchset-mysql-async",
))]
#[test]
#[ignore = "requires Docker; run with --ignored"]
fn generated_filters_agree_with_mysql() {
    let db = crate::common::mysql_database();
    let mut connection = db.connect();
    let compared = generated_filters_agree!(
        connection,
        Engine::MySql,
        subql::backend::MySql,
        sqlparser::dialect::MySqlDialect
    );
    assert!(compared > 0, "no generated filter was served and compared");
}
