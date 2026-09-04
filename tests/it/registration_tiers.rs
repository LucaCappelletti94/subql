//! One registration answer: every shape reports its identity and its tier the
//! same way, whichever tier maintains it.
#![allow(clippy::unwrap_used)]

use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    DefaultIds, QueryProjection, RegistrationRequest, SubscriptionEngine, SubscriptionId,
    SubscriptionRequest, Tier,
};

const DDL: &str = "CREATE TABLE orders (id INT PRIMARY KEY, price FLOAT, status TEXT);\
                   CREATE TABLE managers (id INT PRIMARY KEY);";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

struct DownstreamRequest(SubscriptionRequest<DefaultIds, Postgres>);

impl RegistrationRequest<DefaultIds, Postgres> for DownstreamRequest {
    const DATABASE_READS_PER_CONSUMER: bool = false;

    fn into_request(self) -> SubscriptionRequest<DefaultIds, Postgres> {
        self.0
    }
}

fn engine() -> Engine {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    SubscriptionEngine::new(catalog, PostgreSqlDialect {})
}

#[test]
fn downstream_registration_request_needs_only_consumed_conversion() {
    let registered = engine()
        .register(DownstreamRequest(SubscriptionRequest::new(
            1u64,
            "SELECT * FROM orders",
        )))
        .expect("custom request registers");
    assert!(matches!(registered.tier, Tier::InProcess(_)));
}

/// Register `sql` and report what came back.
fn register(engine: &mut Engine, consumer: u64, sql: &str) -> (SubscriptionId, Tier) {
    let registered = engine
        .register(SubscriptionRequest::new(consumer, sql))
        .unwrap_or_else(|e| panic!("`{sql}` should register, got {e:?}"));
    (registered.subscription_id, registered.tier)
}

/// One statement per tier, and each names its own tier.
#[test]
fn every_shape_reports_the_tier_that_maintains_it() {
    let mut engine = engine();

    let (_, rows) = register(&mut engine, 1, "SELECT * FROM orders WHERE status = 'paid'");
    match rows {
        Tier::InProcess(served) => assert_eq!(
            served.projection,
            QueryProjection::Rows,
            "a plain filter is matched in process, row by row"
        ),
        other => panic!("expected an in-process match, got {other:?}"),
    }

    let (_, fold) = register(&mut engine, 2, "SELECT COUNT(*) FROM orders");
    match fold {
        Tier::InProcess(served) => assert!(
            served.aggregate_bootstrap.is_some(),
            "a fold the engine maintains reports the query that seeds it"
        ),
        other => panic!("expected an in-process fold, got {other:?}"),
    }

    let (_, scalar) = register(&mut engine, 3, "SELECT MIN(price) FROM orders");
    match scalar {
        Tier::Scalar { column_kind, .. } => assert_eq!(
            column_kind,
            subql::backend::BuiltinKind::Float,
            "the extreme is re-read and decoded as its column's kind"
        ),
        other => panic!("expected a scalar re-read, got {other:?}"),
    }

    let (_, grouped) = register(
        &mut engine,
        6,
        "SELECT status, MIN(price) FROM orders GROUP BY status",
    );
    match grouped {
        Tier::GroupedScalar { bootstrap } => assert_eq!(
            bootstrap.group_columns, 1,
            "a grouped extreme seeds one row per group and re-reads one group at a time"
        ),
        other => panic!("expected a grouped scalar re-read, got {other:?}"),
    }

    let (_, keyed) = register(
        &mut engine,
        4,
        "SELECT * FROM orders WHERE lower(status) = 'paid'",
    );
    match keyed {
        Tier::KeyedRows { table_id, .. } => assert_eq!(
            table_id,
            subql::catalog_helpers::table_id(engine.database(), "orders").expect("orders resolves"),
            "a keyed re-read reads exactly one table"
        ),
        other => panic!("expected a keyed re-read, got {other:?}"),
    }

    let (_, whole) = register(
        &mut engine,
        5,
        "SELECT * FROM orders WHERE lower(status) = 'paid' \
         AND id IN (SELECT id FROM managers)",
    );
    match whole {
        Tier::WholeRows { tables, .. } => assert_eq!(
            tables.len(),
            2,
            "a whole re-read is triggered by every table it reads"
        ),
        other => panic!("expected a whole re-read, got {other:?}"),
    }
}

#[test]
fn fixed_tiers_report_exact_executable_queries() {
    let mut engine = engine();
    let bind = Value::String("paid".into());

    let scalar = engine
        .register(
            SubscriptionRequest::new(7u64, "SELECT MIN(price) FROM orders WHERE status = $1")
                .binds(vec![bind.clone()]),
        )
        .expect("scalar registers");
    let Tier::Scalar { query, .. } = scalar.tier else {
        panic!("expected scalar tier")
    };
    assert_eq!(
        query.sql(),
        "SELECT MIN(price) AS v FROM orders WHERE status = $1"
    );
    assert_eq!(query.binds(), std::slice::from_ref(&bind));

    let keyed_sql = "SELECT * FROM orders WHERE lower(status) = $1";
    let keyed = engine
        .register(SubscriptionRequest::new(8u64, keyed_sql).binds(vec![bind.clone()]))
        .expect("keyed read registers");
    let Tier::KeyedRows { query, .. } = keyed.tier else {
        panic!("expected keyed tier")
    };
    assert_eq!(query.sql(), keyed_sql);
    assert_eq!(query.binds(), std::slice::from_ref(&bind));

    let whole_sql = "SELECT * FROM orders WHERE lower(status) = $1 \
                     AND id IN (SELECT id FROM managers)";
    let whole = engine
        .register(SubscriptionRequest::new(9u64, whole_sql).binds(vec![bind.clone()]))
        .expect("whole read registers");
    let Tier::WholeRows { query, .. } = whole.tier else {
        panic!("expected whole tier")
    };
    assert_eq!(query.sql(), whole_sql);
    assert_eq!(query.binds(), &[bind]);
}

#[test]
fn current_fixed_read_is_available_by_subscription_identity() {
    let mut engine = engine();
    let (scalar, _) = register(&mut engine, 10, "SELECT MIN(price) FROM orders");
    let (keyed, _) = register(
        &mut engine,
        11,
        "SELECT * FROM orders WHERE lower(status) = 'paid'",
    );
    let whole_sql = "SELECT * FROM orders WHERE lower(status) = 'paid' \
                     AND id IN (SELECT id FROM managers)";
    let (whole, _) = register(&mut engine, 12, whole_sql);
    for (subscription_id, sql) in [
        (scalar, "SELECT MIN(price) AS v FROM orders"),
        (keyed, "SELECT * FROM orders WHERE lower(status) = 'paid'"),
        (whole, whole_sql),
    ] {
        let query = engine
            .read_query(subscription_id)
            .expect("fixed read has a query");
        assert_eq!(query.sql(), sql);
        assert!(query.binds().is_empty());
    }

    let (in_process, _) = register(&mut engine, 13, "SELECT * FROM orders");
    assert!(engine.read_query(in_process).is_none());
    let (grouped, _) = register(
        &mut engine,
        14,
        "SELECT status, MIN(price) FROM orders GROUP BY status",
    );
    assert!(engine.read_query(grouped).is_none());
}

/// The identity is one field on the answer, whatever the tier, and no two
/// registrations share it.
#[test]
fn every_tier_draws_its_identity_from_one_counter() {
    let mut engine = engine();
    let ids: Vec<SubscriptionId> = [
        "SELECT * FROM orders WHERE status = 'paid'",
        "SELECT COUNT(*) FROM orders",
        "SELECT MIN(price) FROM orders",
        "SELECT status, MIN(price) FROM orders GROUP BY status",
        "SELECT * FROM orders WHERE lower(status) = 'paid'",
        "SELECT * FROM orders WHERE lower(status) = 'paid' AND id IN (SELECT id FROM managers)",
    ]
    .iter()
    .enumerate()
    .map(|(i, sql)| register(&mut engine, i as u64 + 1, sql).0)
    .collect();

    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        ids.len(),
        "six registrations, six identities, got {ids:?}"
    );
}

/// A cause a caller can branch on, rather than prose it must parse.
#[test]
fn not_served_because_carries_structured_operands() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("parse DDL");
    let orders = subql::catalog_helpers::table_id(&catalog, "orders").expect("orders is known");
    let status =
        subql::catalog_helpers::column_id(&catalog, orders, "status").expect("status column");
    let mut engine: Engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT SUM(status) FROM orders WHERE id > 0",
        ))
        .expect("the aggregate registers on a read tier");

    let reason = registered
        .not_served_because
        .as_ref()
        .expect("a read tier reports why it is not served in process");
    assert_eq!(
        reason,
        &subql::NotServed::UnfoldableAggregate {
            column: status,
            kind: subql::backend::BuiltinKind::String.into(),
            function: "SUM".to_string(),
        },
        "the cause names its column and that column's kind"
    );
    assert_eq!(
        reason.to_string(),
        format!(
            "SUM requires a numeric column (Int, Float, or Decimal), \
             but column {status} has type String"
        ),
        "Display keeps rendering the sentence callers log today"
    );
}

/// Row security forces a per-consumer read, and the cause names the table.
#[test]
fn a_row_security_read_names_its_table() {
    let catalog = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE guarded (id INT PRIMARY KEY, amount INT);\
         ALTER TABLE guarded ENABLE ROW LEVEL SECURITY;",
    )
    .expect("parse DDL");
    let table = subql::catalog_helpers::table_id(&catalog, "guarded").expect("guarded is known");
    let mut engine: Engine = SubscriptionEngine::new(catalog, PostgreSqlDialect {});
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, "SELECT SUM(amount) FROM guarded WHERE id > 0")
                .database_reads_per_consumer(),
        )
        .expect("a per-consumer read registers");

    assert_eq!(
        registered.not_served_because,
        Some(subql::NotServed::RowSecurityNeedsPerConsumerRead { table }),
        "the engine routes on this cause, so it must be typed"
    );
    // The cause is what selects the tier: this one reads the whole answer
    // rather than folding or reading by key.
    assert!(
        matches!(&registered.tier, Tier::WholeRows { tables, .. } if tables == &vec![table]),
        "the row-security cause routes to a whole-rows read, got {:?}",
        registered.tier
    );
}

/// A cause with no structured operands still reports its own words.
#[test]
fn an_unsupported_form_reports_its_own_words() {
    let mut engine = engine();
    let registered = engine
        .register(SubscriptionRequest::new(
            1u64,
            "SELECT MIN(price) FROM orders WHERE id > 0",
        ))
        .expect("the aggregate registers on a read tier");
    let reason = registered
        .not_served_because
        .as_ref()
        .expect("a read tier reports a reason");
    assert!(
        matches!(reason, subql::NotServed::UnsupportedSql(_)),
        "an unsupported form carries its prose, got {reason:?}"
    );
    assert!(
        reason.to_string().contains("MIN"),
        "the prose still names the function, got {reason}"
    );
}
