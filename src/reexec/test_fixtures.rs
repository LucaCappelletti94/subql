//! The catalog and events both re-execution test suites are written
//! against.
//!
//! Shared so that changing a fixture makes both suites answer for it. The
//! two suites were written from the same fixtures and had drifted to
//! holding byte-identical copies of five of them, which meant a fixture
//! could be corrected on one side and left wrong on the other, and
//! nothing would say so.
//!
//! Only the pure part lives here. The connector mocks stay with their own
//! suites and are not a duplication to remove: the asynchronous one is
//! `parking_lot::Mutex`-backed *because its futures have to be `Send`*,
//! and the synchronous one is `RefCell` and `Rc` backed because it has no
//! such constraint and should not pay for one. Sharing those would mean
//! giving the synchronous suite locks it does not need in order to delete
//! lines, which is the wrong trade.

use crate::backend::{Postgres, Value};
use crate::testing::TestEvent;
use crate::TableId;
use alloc::vec::Vec;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

/// Register `sql` for `consumer` and assert it landed on the scalar tier.
///
/// Both suites open a test this way, 35 times between them, and the
/// scaffold is mode-independent: `register` is synchronous in both modes,
/// and every mock in both suites uses `AuthContext = ()`. `M` is therefore
/// passive here, not a generic over sync-versus-async execution.
///
/// The copies spelled the panic three ways, and one of them, "expected
/// ReExec for MIN", distinguished which of a test's two registrations
/// failed. That distinction is kept rather than normalised away: the
/// message names the query, which identifies the site at every call and
/// says more than "for MIN" did.
pub(super) fn register_scalar_query<M>(
    engine: &mut crate::reexec::AutoResolvingEngine<
        TestEvent<Postgres>,
        crate::DefaultIds,
        ParserDB,
        M,
    >,
    consumer: u64,
    sql: &str,
) -> crate::SubscriptionId
where
    M: crate::reexec::ResolverMode<Postgres, AuthContext = ()>,
{
    match engine
        .register(crate::SubscriptionRequest::new(consumer, sql), ())
        .expect("registration succeeds")
    {
        crate::Registered {
            subscription_id,
            tier: crate::Tier::Scalar { .. },
            ..
        } => subscription_id,
        other => panic!("expected the scalar re-execution tier for {sql}, got {other:?}"),
    }
}

/// [`register_scalar_query`] followed by seeding the captured extremum.
///
/// The seed is what makes a later delete of the matching row trigger a
/// re-execution, so a test that forgets it measures nothing.
pub(super) fn bootstrap_scalar_query<M>(
    engine: &mut crate::reexec::AutoResolvingEngine<
        TestEvent<Postgres>,
        crate::DefaultIds,
        ParserDB,
        M,
    >,
    consumer: u64,
    sql: &str,
    seed: f64,
) -> crate::SubscriptionId
where
    M: crate::reexec::ResolverMode<Postgres, AuthContext = ()>,
{
    let subscription_id = register_scalar_query(engine, consumer, sql);
    crate::Install::install(
        engine,
        subscription_id,
        crate::ScalarInstall {
            value: Value::Float(seed),
            checkpoint: None::<crate::NoCheckpoint>,
        },
    )
    .expect("the bootstrap seed installs");
    subscription_id
}

/// The one table both suites subscribe to.
///
/// Four columns, chosen so that a test can change a column the predicate
/// reads (`status`, position 3) or one it does not (`price`, position 1),
/// and so that one column is absent from every projection.
pub(super) fn catalog() -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, quantity INT, status TEXT);",
    )
    .expect("the fixture catalog parses")
}

/// One row of that table, with `quantity` and `status` fixed.
///
/// Column positions: `id` 0, `price` 1, `quantity` 2, `status` 3. Tests
/// name them by index when declaring which columns an event changed.
pub(super) fn row(id: i64, price: f64) -> Vec<Value<Postgres>> {
    alloc::vec![
        Value::Int(id),
        Value::Float(price),
        Value::Int(1),
        Value::String("paid".into()),
    ]
}

/// That row arriving as an insert.
pub(super) fn insert_event(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::insert(table_id, row(id, price)).with_pk_columns([0u16])
}

/// That row leaving as a delete.
pub(super) fn delete_event(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::delete(table_id, row(id, price)).with_pk_columns([0u16])
}

/// An update that changes nothing the row images differ on, and declares
/// only `status` changed.
///
/// The images being equal is deliberate: it is how a test asks what the
/// tier does with a change it was told about but cannot see.
pub(super) fn update_status_only(table_id: TableId, id: i64, price: f64) -> TestEvent<Postgres> {
    TestEvent::<Postgres>::update(table_id, row(id, price), row(id, price))
        .with_pk_columns([0u16])
        .with_changed_columns([3u16])
}
