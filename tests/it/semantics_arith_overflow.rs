//! Integer overflow in a predicate's arithmetic, measured.
//!
//! `qty + 9223372036854775807` with `qty` 1 overflows `i64`. Today that
//! panics in a debug build, at the `x + y` in the VM's add instruction, and
//! wraps silently in a release build, which answers the predicate from a
//! number no engine ever produced.
//!
//! The engines do not agree on the answer, so neither can the fix.
//! Measured 2026-09-04 on PostgreSQL 16.11, MySQL 8.4.11 and SQLite 3.51.1:
//!
//! ```text
//! operation                         pg                  mysql               sqlite
//! bigint max + 1                    ERROR out of range  ERROR out of range  9.22e18, real
//! bigint min * -1                   ERROR out of range  ERROR out of range  9.22e18, real
//! negate bigint min                 ERROR out of range  9223372036854775808 9.22e18, real
//! ```
//!
//! So PostgreSQL and MySQL raise, which is a per-subscription evaluation
//! failure reported alongside the notifications, and SQLite promotes the
//! result to a real, which is an answer. One row that fails one
//! subscription leaves every other subscription on the same event alone.
//!
//! The one measured disagreement inside the raising pair is MySQL's unary
//! minus on the smallest integer, which promotes past `i64` instead of
//! raising. subql reports a failure there, which is visible rather than
//! silent, and is recorded in the plan rather than papered over.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use subql::backend::{MySql, Postgres, SQLite, Value};
use subql::compiler::vm::refusal::{ArithmeticOp, EvaluationRefusal};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, EvaluationFailure, SubscriptionEngine, SubscriptionRequest,
};

const DDL: &str = "CREATE TABLE t (id INT PRIMARY KEY, qty BIGINT)";
const SQLITE_DDL: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER)";
const OVERFLOWS: &str = "SELECT * FROM t WHERE qty + 9223372036854775807 > 0";

/// Dispatch one row of `qty` against `predicate` and hand back the whole
/// notification set, so a test can assert both what fired and what failed.
macro_rules! dispatch {
    ($backend:ty, $dialect:ty, $ddl:expr, $predicate:expr, $qty:expr) => {{
        let db = ParserDB::parse::<$dialect>($ddl).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<$backend>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, <$dialect>::default());
        engine
            .register(SubscriptionRequest::new(1u64, $predicate))
            .expect("the predicate registers");
        let row = vec![Value::Int(1), Value::Int($qty)];
        engine
            .consumers(&TestEvent::insert(table, row))
            .expect("dispatch succeeds")
    }};
}

/// The first requirement: an overflowing predicate must not take the
/// process down. This is the debug-build panic.
#[test]
fn overflow_does_not_panic() {
    let notifications = dispatch!(Postgres, PostgreSqlDialect, DDL, OVERFLOWS, 1);
    assert!(
        notifications.inserted().is_empty(),
        "an overflow is not a match"
    );
}

/// PostgreSQL raises, so the subscription's evaluation fails and the
/// failure is reported with the consumer it belongs to.
#[test]
fn pg_overflow_fails_the_subscription() {
    let notifications = dispatch!(Postgres, PostgreSqlDialect, DDL, OVERFLOWS, 1);
    assert_eq!(
        notifications.evaluation_failures(),
        &[EvaluationFailure {
            subscription_id: notifications.evaluation_failures()[0].subscription_id,
            consumer_id: 1u64,
            refusal: EvaluationRefusal::IntegerOverflow {
                operation: ArithmeticOp::Add,
            },
        }],
        "the report names the subscription, its consumer and what failed"
    );
    assert!(
        notifications.inserted().is_empty(),
        "and the subscription is not notified"
    );
}

/// MySQL raises on the same operation, measured, so it answers the same way.
#[test]
fn mysql_overflow_fails_the_subscription() {
    let notifications = dispatch!(MySql, MySqlDialect, DDL, OVERFLOWS, 1);
    assert_eq!(
        notifications.evaluation_failures(),
        &[EvaluationFailure {
            subscription_id: notifications.evaluation_failures()[0].subscription_id,
            consumer_id: 1u64,
            refusal: EvaluationRefusal::IntegerOverflow {
                operation: ArithmeticOp::Add,
            },
        }]
    );
}

/// SQLite promotes the result to a real instead of raising, so the
/// predicate has an answer and the subscription is notified.
#[test]
fn sqlite_overflow_promotes_to_float() {
    let notifications = dispatch!(SQLite, SQLiteDialect, SQLITE_DDL, OVERFLOWS, 1);
    assert!(
        notifications.evaluation_failures().is_empty(),
        "promotion is an answer, not a failure"
    );
    assert_eq!(
        notifications.inserted(),
        &[1],
        "9.22e18 is above zero, so the row matches"
    );
}

/// A failure is per subscription: another subscription reading the same
/// event still gets its answer.
#[test]
fn overflow_fails_only_its_own_subscription() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    engine
        .register(SubscriptionRequest::new(1u64, OVERFLOWS))
        .expect("the overflowing predicate registers");
    engine
        .register(SubscriptionRequest::new(
            2u64,
            "SELECT * FROM t WHERE qty > 0",
        ))
        .expect("the sound predicate registers");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Int(1)],
        ))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications.inserted(),
        &[2],
        "the sound subscription is answered"
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.consumer_id)
            .collect::<Vec<_>>(),
        vec![1u64],
        "and only the overflowing one failed"
    );
}

/// The control: arithmetic that does not overflow is unaffected.
#[test]
fn sound_arithmetic_still_answers() {
    let notifications = dispatch!(
        Postgres,
        PostgreSqlDialect,
        DDL,
        "SELECT * FROM t WHERE qty + 10 > 12",
        5
    );
    assert!(notifications.evaluation_failures().is_empty());
    assert_eq!(notifications.inserted(), &[1], "15 is above 12");
}

/// The granularity is the subscription, not the consumer: one consumer
/// holding two subscriptions, one of which overflows, is answered for the
/// sound one and told which of the two failed.
#[test]
fn one_consumer_with_two_subscriptions_fails_only_the_overflowing_one() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});

    let overflowing = engine
        .register(SubscriptionRequest::new(7u64, OVERFLOWS))
        .expect("the overflowing predicate registers")
        .subscription_id;
    let sound = engine
        .register(SubscriptionRequest::new(
            7u64,
            "SELECT * FROM t WHERE qty > 0",
        ))
        .expect("the sound predicate registers")
        .subscription_id;
    assert_ne!(overflowing, sound, "two subscriptions, one consumer");

    let notifications = engine
        .consumers(&TestEvent::insert(
            table,
            vec![Value::Int(1), Value::Int(1)],
        ))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications.inserted(),
        &[7],
        "the consumer is notified for the sound subscription"
    );
    assert_eq!(
        notifications.evaluation_failures(),
        &[EvaluationFailure {
            subscription_id: overflowing,
            consumer_id: 7u64,
            refusal: EvaluationRefusal::IntegerOverflow {
                operation: ArithmeticOp::Add,
            },
        }],
        "and the failure names the one subscription that could not be evaluated"
    );
}

/// An update whose new row overflows: the subscription is told the
/// evaluation failed and gets no transition. Reporting a deletion as well
/// would say both that the row left the answer and that the answer could
/// not be computed.
#[test]
fn an_update_refused_on_one_version_reports_no_transition() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    // The first disjunct short-circuits for a negative `qty`, so only a
    // positive one reaches the arithmetic, where it overflows.
    let predicate = "SELECT * FROM t WHERE qty < 0 OR qty + 9223372036854775807 > 0";
    let subscription = engine
        .register(SubscriptionRequest::new(1u64, predicate))
        .expect("the predicate registers")
        .subscription_id;

    let row = |qty: i64| vec![Value::Int(1), Value::Int(qty)];

    // Old row matches through the short circuit, new row overflows.
    let notifications = engine
        .consumers(&TestEvent::update(table, row(-1), row(1)))
        .expect("dispatch succeeds");
    assert!(
        notifications.deleted().is_empty() && notifications.updated().is_empty(),
        "a refused evaluation is not a transition: {notifications:?}"
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.subscription_id)
            .collect::<Vec<_>>(),
        vec![subscription],
    );

    // And the mirror: old row overflows, new row matches.
    let notifications = engine
        .consumers(&TestEvent::update(table, row(1), row(-1)))
        .expect("dispatch succeeds");
    assert!(
        notifications.inserted().is_empty() && notifications.updated().is_empty(),
        "nor in the other direction: {notifications:?}"
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.subscription_id)
            .collect::<Vec<_>>(),
        vec![subscription],
    );
}

/// Failure isolation inside one predicate, which is the case a whole-bitmap
/// refusal gets wrong.
///
/// Behind the membership-term feature, since a shared predicate whose
/// subscribers differ is exactly what a term produces.
///
/// Two subscribers share one compiled predicate carrying a membership term
/// or-ed with overflowing arithmetic. For the subscriber the term admits,
/// the `OR` short-circuits and the arithmetic is never reached, so that
/// subscription is answered. For the other the term is false, the
/// arithmetic runs, and only that subscription fails.
#[cfg(feature = "membership-term")]
#[test]
fn a_term_that_short_circuits_the_overflow_keeps_its_subscriber_answered() {
    use rls2fga::translator::TranslatorBuilder;
    use rls2fga::types::ConfidenceLevel;

    const DDL_TERM: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), \
         user_id TEXT, PRIMARY KEY(project_id, user_id));
         CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, qty BIGINT);";
    const PREDICATE: &str = "SELECT * FROM docs WHERE project_id IN \
         (SELECT project_id FROM project_members \
          WHERE user_id = current_setting('app.user_id', true)) \
         OR qty + 9223372036854775807 > 0";

    let db = ParserDB::parse::<PostgreSqlDialect>(DDL_TERM).expect("DDL parses");
    let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(
            TranslatorBuilder::new()
                .with_min_confidence(ConfidenceLevel::B)
                .build(),
        );

    let subscribe = |consumer: u64, projects: &[i64]| {
        SubscriptionRequest::new(consumer, PREDICATE)
            .subscriber(Value::String(format!("user{consumer}")))
            .term_values(
                vec!["project_id"],
                projects
                    .iter()
                    .copied()
                    .map(|project| vec![Value::Int(project)])
                    .collect(),
            )
    };

    let admitted = engine
        .register(subscribe(1, &[7]))
        .expect("the term registers")
        .subscription_id;
    let refused = engine
        .register(subscribe(2, &[9]))
        .expect("the term registers")
        .subscription_id;
    assert_ne!(admitted, refused, "one predicate, two subscriptions");

    // The row belongs to project 7, so the term admits consumer 1 only.
    let notifications = engine
        .consumers(&TestEvent::insert(
            docs,
            vec![Value::Int(1), Value::Int(7), Value::Int(1)],
        ))
        .expect("dispatch succeeds");

    assert_eq!(
        notifications.inserted(),
        &[1],
        "the admitted subscriber's OR short-circuits before the arithmetic"
    );
    assert_eq!(
        notifications
            .evaluation_failures()
            .iter()
            .map(|failure| failure.subscription_id)
            .collect::<Vec<_>>(),
        vec![refused],
        "and only the subscriber whose term is false reached the overflow"
    );
}

/// An aggregate whose filter overflows reports the failure and nothing
/// else: no fold, no tier change, and no read trigger, because a database
/// read would raise the same error. Discarding it would be the silent drop
/// this whole correction exists to remove.
#[test]
fn an_overflowing_aggregate_filter_stops_maintenance() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
    let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
        SubscriptionEngine::new(db, PostgreSqlDialect {});
    let subscription = engine
        .register(SubscriptionRequest::new(
            9u64,
            "SELECT COUNT(*) FROM t WHERE qty + 9223372036854775807 > 0",
        ))
        .expect("the aggregate registers")
        .subscription_id;

    let event =
        TestEvent::insert(table, vec![Value::Int(1), Value::Int(1)]).with_pk_columns([0u16]);
    let output = engine.aggregate_updates(&event).expect("maintenance runs");

    assert!(
        output.updates.is_empty(),
        "nothing folded: the filter has no answer"
    );
    assert_eq!(
        output.evaluation_failures,
        vec![(
            subscription,
            EvaluationRefusal::IntegerOverflow {
                operation: ArithmeticOp::Add,
            },
        )],
        "the failure is reported against the subscription"
    );
    assert!(
        output.transitions.is_empty() && output.triggers.is_empty(),
        "a read would raise the same error, so nothing is triggered: {:?}",
        output.transitions
    );
}

/// The update case, which an insert-only test misses: one row version is
/// served and the other refused, so a delta exists for the served half.
/// Folding it would move the total by a row the filter never judged, so the
/// subscription must get the stop and no update at all. Both directions.
#[test]
fn an_aggregate_update_refused_on_one_version_folds_nothing() {
    // The first disjunct short-circuits for a negative `qty`; a positive one
    // reaches the arithmetic and overflows.
    const FILTER: &str = "SELECT COUNT(*) FROM t WHERE qty < 0 OR qty + 9223372036854775807 > 0";

    for (old, new, label) in [
        (-1i64, 1i64, "old served, new refused"),
        (1, -1, "the mirror"),
    ] {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
        let table = catalog_helpers::table_id(&db, "t").expect("t is in the catalog");
        let mut engine: SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB> =
            SubscriptionEngine::new(db, PostgreSqlDialect {});
        let subscription = engine
            .register(SubscriptionRequest::new(9u64, FILTER))
            .expect("the aggregate registers")
            .subscription_id;

        // Seeded, or the fold reports nothing whatever the deltas say and
        // the assertion below would hold vacuously.
        subql::Install::install(
            &mut engine,
            subscription,
            subql::AggregateSeedInstall {
                rows: vec![vec![Value::Int(4)]],
                read_at: None,
            },
        )
        .expect("the seed installs");

        let row = |qty: i64| vec![Value::Int(1), Value::Int(qty)];
        let event = TestEvent::update(table, row(old), row(new)).with_pk_columns([0u16]);
        let output = engine.aggregate_updates(&event).expect("maintenance runs");

        assert!(
            output.updates.is_empty(),
            "{label}: nothing may fold, got {:?}",
            output.updates
        );
        assert_eq!(
            output
                .evaluation_failures
                .iter()
                .map(|(subscription, _)| *subscription)
                .collect::<Vec<_>>(),
            vec![subscription],
            "{label}: the failure names the subscription"
        );
        assert!(
            output.transitions.is_empty() && output.triggers.is_empty(),
            "{label}: no read is triggered, it would raise the same error"
        );
    }
}
