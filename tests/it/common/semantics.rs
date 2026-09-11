//! Register-and-dispatch scaffold shared by the `semantics_*` suites.
//!
//! Five suites carried the same six statements and two carried the same
//! registration, and the `not_served_because` guard was present in only
//! two of them. Without it a predicate the engine demoted to a database
//! read dispatches nothing, `inserted()` comes back empty, and a negative
//! assertion passes for the wrong reason. That hid a stale claim in
//! `semantics_bpchar.rs`.
//!
//! These are functions, not macros. Nothing here needed one, and writing
//! it as a function is what revealed that the dialect the five copies
//! passed alongside the backend is redundant: `SubscriptionEngine::new`
//! takes `B::Dialect`, so the two could never disagree.

use sql_traits::structs::ParserDB;
use sqlparser::dialect::Dialect;
use subql::backend::{Backend, Value};
use subql::compiler::SqlLiteralParse;
use subql::testing::TestEvent;
use subql::{catalog_helpers, DefaultIds, Registered, SubscriptionEngine, SubscriptionRequest};

/// Register `predicate`, assert it is answered in process, and report
/// whether an insert of `cells` into `table` reaches a consumer.
pub fn notifies<B>(ddl: &str, table: &str, predicate: &str, cells: Vec<Value<B>>) -> bool
where
    B: Backend + SqlLiteralParse + core::fmt::Debug,
    B::Dialect: Dialect + Default + 'static,
{
    let db = ParserDB::parse::<B::Dialect>(ddl).expect("DDL parses");
    // Every copy resolved the table through `Postgres` whatever the backend.
    let table_id = catalog_helpers::table_id::<subql::backend::Postgres, _>(&db, table)
        .expect("the table is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, <B::Dialect as Default>::default());
    let registered = engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers");
    assert!(
        registered.not_served_because.is_none(),
        "{predicate} is meant to be answered in process, and it was refused: {:?}",
        registered.not_served_because
    );
    let notifications = engine
        .consumers(&TestEvent::insert(table_id, cells))
        .expect("dispatch succeeds");
    !notifications.inserted().is_empty()
}

/// The registration for `predicate`, for the tests that assert a tier or a
/// refusal cause rather than an answer.
pub fn register<B>(ddl: &str, predicate: &str) -> Registered<B>
where
    B: Backend + SqlLiteralParse,
    B::Dialect: Dialect + Default + 'static,
{
    let db = ParserDB::parse::<B::Dialect>(ddl).expect("DDL parses");
    let mut engine: SubscriptionEngine<TestEvent<B>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, <B::Dialect as Default>::default());
    engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("registration succeeds, in process or as a read")
}
