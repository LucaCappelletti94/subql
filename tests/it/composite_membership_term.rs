//! A membership whose linking key spans two columns, spelled as the bounded
//! `EXISTS`, end to end: the subscriber states value pairs, a changed row is
//! matched on the pair rather than on each column alone, and changed
//! membership rows move the pair set.
#![allow(clippy::unwrap_used)]

use rls2fga::translator::{Translator, TranslatorBuilder};
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{BuiltinKind, Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, TableId,
};

// `docs` is keyed on (tenant_id, id) together: upstream serves a multi-pair
// membership exactly when the pairs cover the filtered table's composite key,
// since that is what lets one share row name one document.
const DDL: &str = "CREATE TABLE tenants(id INTEGER PRIMARY KEY);
     CREATE TABLE shares(tenant_id INTEGER NOT NULL, doc_id INTEGER NOT NULL, viewer TEXT NOT NULL, PRIMARY KEY(tenant_id, doc_id, viewer));
     CREATE TABLE docs(tenant_id INTEGER NOT NULL, id INTEGER NOT NULL, title TEXT, PRIMARY KEY(tenant_id, id));";

/// The filter under test: a document is shared with the caller under its
/// tenant, and the share names the document by (tenant, document) together.
const TERM: &str = "SELECT * FROM docs WHERE EXISTS (SELECT 1 FROM shares s \
     WHERE s.tenant_id = docs.tenant_id AND s.doc_id = docs.id \
       AND s.viewer = current_setting('app.user_id', true))";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn translator() -> Translator {
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
}

fn engine() -> (Engine, TableId, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
    let shares = catalog_helpers::table_id(&db, "shares").expect("shares is in the catalog");
    let engine = SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());
    (engine, docs, shares)
}

/// One subscription for `consumer`, filtering for `user`, stating the
/// (tenant, document) pairs shared with it today.
fn subscribe(
    consumer: u64,
    user: &str,
    pairs: &[(i64, i64)],
) -> SubscriptionRequest<DefaultIds, Postgres> {
    SubscriptionRequest::new(consumer, TERM)
        .subscriber(Value::String(user.into()))
        .term_values(
            vec!["tenant_id", "id"],
            pairs
                .iter()
                .map(|&(tenant, document)| vec![Value::Int(tenant), Value::Int(document)])
                .collect(),
        )
}

/// A `docs` row: `(tenant_id, id, title)`.
fn doc(id: i64, tenant: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(tenant),
        Value::Int(id),
        Value::String("d".into()),
    ]
}

/// A `shares` row: `(tenant_id, doc_id, viewer)`.
fn share(tenant: i64, document: i64, viewer: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(tenant),
        Value::Int(document),
        Value::String(viewer.into()),
    ]
}

/// The point of the pair set: values match together or not at all. A
/// subscriber holding (1, 5) and (2, 6) is not reached through (1, 6), which
/// per-column sets would admit.
#[test]
fn a_pair_matches_together_or_not_at_all() {
    let (mut engine, docs, _) = engine();
    engine
        .register(subscribe(1, "alice", &[(1, 5), (2, 6)]))
        .unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "(1, 5) is stated");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(6, 2)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "(2, 6) is stated");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(6, 1)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "(1, 6) crosses the two stated pairs and must not match"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(5, 2)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "(2, 5) crosses them the other way"
    );
}

/// The stated column order is the caller's own: each row follows it, and the
/// engine matches by name rather than by position in the filter.
#[test]
fn stated_columns_may_come_in_any_order() {
    let (mut engine, docs, _) = engine();
    engine
        .register(
            SubscriptionRequest::new(1u64, TERM)
                .subscriber(Value::String("alice".into()))
                .term_values(
                    vec!["id", "tenant_id"],
                    vec![vec![Value::Int(5), Value::Int(1)]],
                ),
        )
        .unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the row said id 5 under tenant 1, in its own column order"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 5)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the reversed reading would deliver this row, and must not"
    );
}

/// Two subscribers, one compiled predicate, distinct pair sets.
#[test]
fn two_subscribers_share_one_predicate_and_keep_their_own_pairs() {
    let (mut engine, docs, _) = engine();
    engine.register(subscribe(1, "alice", &[(1, 5)])).unwrap();
    engine.register(subscribe(2, "bob", &[(1, 6)])).unwrap();
    assert_eq!(engine.predicate_count(docs), 1);

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1]);
    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(6, 1)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[2]);
}

/// A NULL in either compared cell admits nobody: SQL never matches through a
/// NULL, whatever the other cell holds.
#[test]
fn a_null_compared_cell_admits_nobody() {
    let (mut engine, docs, _) = engine();
    engine.register(subscribe(1, "alice", &[(1, 5)])).unwrap();

    let row = vec![Value::Null, Value::Int(5), Value::String("d".into())];
    let notifs = engine.consumers(&TestEvent::insert(docs, row)).unwrap();
    assert!(notifs.inserted().is_empty(), "nobody holds that pair");
}

/// A new membership row moves the pair set and reports the narrowing with the
/// whole pair, in the filter's column order.
#[test]
fn a_new_membership_row_moves_the_pair_set() {
    let (mut engine, docs, shares) = engine();
    let alice = engine.register(subscribe(1, "alice", &[(1, 5)])).unwrap();

    let before = engine
        .consumers(&TestEvent::insert(docs, doc(6, 1)))
        .unwrap();
    assert!(before.inserted().is_empty(), "not shared yet");

    let granted = TestEvent::insert(shares, share(1, 6, "alice"));
    let notifs = engine.consumers(&granted).unwrap();
    let narrowings = notifs.narrowings();
    assert_eq!(narrowings.len(), 1, "one subscription, one narrowing");
    assert_eq!(narrowings[0].subscription, alice.subscription_id);
    assert_eq!(narrowings[0].table, docs);
    assert_eq!(
        narrowings[0].values,
        vec![Value::Int(1), Value::Int(6)],
        "the pair arrives whole, tenant first as the filter wrote it"
    );
    assert!(narrowings[0].entered);

    let after = engine
        .consumers(&TestEvent::insert(docs, doc(6, 1)))
        .unwrap();
    assert_eq!(after.inserted(), &[1], "the new share now admits the row");
}

/// A deleted membership row takes the pair away.
#[test]
fn a_removed_membership_row_takes_the_pair_away() {
    let (mut engine, docs, shares) = engine();
    engine.register(subscribe(1, "alice", &[(1, 5)])).unwrap();

    let revoked = TestEvent::delete(shares, share(1, 5, "alice"));
    let notifs = engine.consumers(&revoked).unwrap();
    assert_eq!(notifs.narrowings().len(), 1);
    assert_eq!(
        notifs.narrowings()[0].values,
        vec![Value::Int(1), Value::Int(5)]
    );
    assert!(!notifs.narrowings()[0].entered);

    let after = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert!(after.inserted().is_empty(), "the share is gone");
}

/// An updated membership row reports both halves of the move.
#[test]
fn an_updated_membership_row_reports_both_halves() {
    let (mut engine, _, shares) = engine();
    engine.register(subscribe(1, "alice", &[(1, 5)])).unwrap();

    let moved = TestEvent::update(shares, share(1, 5, "alice"), share(2, 9, "alice"))
        .with_changed_columns([0u16, 1u16]);
    let notifs = engine.consumers(&moved).unwrap();
    let mut narrowings: Vec<(Vec<Value<Postgres>>, bool)> = notifs
        .narrowings()
        .iter()
        .map(|n| (n.values.clone(), n.entered))
        .collect();
    narrowings.sort_by_key(|(_, entered)| *entered);
    assert_eq!(
        narrowings,
        vec![
            (vec![Value::Int(1), Value::Int(5)], false),
            (vec![Value::Int(2), Value::Int(9)], true),
        ]
    );
}

/// Truncating the membership table withdraws every pair.
#[test]
fn truncating_the_membership_table_withdraws_every_pair() {
    let (mut engine, docs, shares) = engine();
    engine.register(subscribe(1, "alice", &[(1, 5)])).unwrap();

    let notifs = engine.consumers(&TestEvent::truncate(shares)).unwrap();
    assert_eq!(notifs.narrowings().len(), 1);
    assert_eq!(
        notifs.narrowings()[0].values,
        vec![Value::Int(1), Value::Int(5)]
    );
    assert!(!notifs.narrowings()[0].entered);

    let after = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert!(after.inserted().is_empty(), "the share is gone");
}

/// The one-pair `EXISTS` says exactly what the accepted `IN` form says, and
/// registers the same way.
#[test]
fn a_one_pair_exists_is_the_in_spelling_in_other_clothes() {
    const ONE_PAIR: &str = "SELECT * FROM docs WHERE EXISTS (SELECT 1 FROM shares s \
         WHERE s.doc_id = docs.id AND s.viewer = current_setting('app.user_id', true))";
    let (mut engine, docs, _) = engine();
    engine
        .register(
            SubscriptionRequest::new(1u64, ONE_PAIR)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["id"], vec![vec![Value::Int(5)]]),
        )
        .expect("the one-pair EXISTS registers");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1]);
}

/// `describe_terms` names both pairs, in the filter's order, and hands over a
/// runnable seed read projecting the membership columns in the same order.
#[test]
fn describe_terms_names_both_pairs_and_the_seed_read() {
    let (engine, _, _) = engine();
    let described = engine
        .describe_terms(&SubscriptionRequest::new(1u64, TERM))
        .expect("the composite term is describable");
    let [subql::term::TermDescription::Membership(term)] = described.as_slice() else {
        panic!("one membership subquery, got {described:?}");
    };
    assert_eq!(term.member_table, "shares");
    assert_eq!(term.member_subject, "viewer");
    assert_eq!(term.subject_kind, BuiltinKind::String);
    let pairs: Vec<(&str, &str)> = term
        .pairs
        .iter()
        .map(|pair| (pair.column.as_str(), pair.member_key.as_str()))
        .collect();
    assert_eq!(
        pairs,
        vec![("tenant_id", "tenant_id"), ("id", "doc_id")],
        "compared and membership columns pair up in written order"
    );
    assert!(
        term.pairs.iter().all(|pair| pair.kind == BuiltinKind::Int),
        "both pairs decode as integers"
    );
    assert_eq!(
        term.seed_sql,
        "SELECT s.tenant_id, s.doc_id FROM shares s \
         WHERE s.viewer = current_setting('app.user_id', true)",
        "the seed projects the membership columns in pair order for the caller's rows"
    );
}

// ---- refusals ----

/// Register a filter the bounded form rejects, expecting the reread tier to
/// serve it, and return the reason the in-process path gave up.
fn reread_reason(engine: &mut Engine, sql: &str) -> String {
    let registered = engine
        .register(SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())))
        .expect("the reread tier serves what the bounded form refuses");
    assert!(
        matches!(registered.tier, subql::Tier::WholeRows { .. }),
        "expected the reread tier, got {:?}",
        registered.tier
    );
    registered
        .not_served_because
        .expect("the fallback names why the fold refused")
}

/// The refusal a filter carrying a term is met with, or a panic naming what
/// happened instead.
fn refusal(engine: &mut Engine, spec: SubscriptionRequest<DefaultIds, Postgres>) -> String {
    match engine.register(spec) {
        Err(
            RegisterError::MembershipTermRefused(reason) | RegisterError::UnsupportedSql(reason),
        ) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// The tuple-IN spelling is refused by name, telling the writer the EXISTS
/// respelling that works.
#[test]
fn a_row_value_in_is_told_to_respell_as_exists() {
    let (mut engine, _, _) = engine();
    let tuple_in = "SELECT * FROM docs WHERE (tenant_id, id) IN \
         (SELECT tenant_id, doc_id FROM shares WHERE viewer = current_setting('app.user_id', true))";
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, tuple_in).subscriber(Value::String("alice".into())),
    );
    assert!(
        reason.contains("EXISTS"),
        "the refusal names the spelling that works: {reason}"
    );
}

/// `NOT EXISTS` is subtraction: the in-process path refuses it by name and
/// the registration falls to the whole-reread tier, like `NOT IN`.
#[test]
fn a_not_exists_falls_to_the_reread_tier_as_subtraction() {
    let (mut engine, _, _) = engine();
    let negated = "SELECT * FROM docs WHERE NOT EXISTS (SELECT 1 FROM shares s \
         WHERE s.tenant_id = docs.tenant_id AND s.doc_id = docs.id \
           AND s.viewer = current_setting('app.user_id', true))";
    let registered = engine
        .register(SubscriptionRequest::new(1u64, negated).subscriber(Value::String("alice".into())))
        .expect("the reread tier serves subtraction");
    assert!(
        matches!(registered.tier, subql::Tier::WholeRows { .. }),
        "subtraction is served by rereading, got {:?}",
        registered.tier
    );
    let reason = registered
        .not_served_because
        .expect("the fallback names why the fold refused");
    assert!(
        reason.contains("subtraction"),
        "the refusal names the reason: {reason}"
    );
}

/// A bare outer column inside the subquery resolves to the membership table
/// under SQL's own rules, so the pair must spell the subscribed side
/// qualified. The filter leaves the bounded form, lands on the reread tier,
/// and the reason names the qualification.
#[test]
fn an_unqualified_subscribed_column_falls_out_of_the_bounded_form() {
    let (mut engine, _, _) = engine();
    let bare = "SELECT * FROM docs WHERE EXISTS (SELECT 1 FROM shares s \
         WHERE s.tenant_id = tenant_id AND s.doc_id = id \
           AND s.viewer = current_setting('app.user_id', true))";
    let reason = reread_reason(&mut engine, bare);
    assert!(
        reason.contains("qualified"),
        "the refusal names the qualification: {reason}"
    );
}

/// A conjunct that is neither a pair equality nor a caller comparison leaves
/// the bounded form: the filter lands on the reread tier, and the reason
/// names the form, so the two executors never answer differently in silence.
#[test]
fn a_residual_conjunct_falls_out_of_the_bounded_form() {
    let (mut engine, _, _) = engine();
    let residual = "SELECT * FROM docs WHERE EXISTS (SELECT 1 FROM shares s \
         WHERE s.tenant_id = docs.tenant_id AND s.doc_id = docs.id \
           AND s.viewer = current_setting('app.user_id', true) AND s.doc_id > 3)";
    let reason = reread_reason(&mut engine, residual);
    assert!(
        reason.contains("pair equality"),
        "the refusal names the bounded form: {reason}"
    );
}

/// A stated row must carry one value per named column.
#[test]
fn a_stated_row_of_the_wrong_width_is_refused() {
    let (mut engine, _, _) = engine();
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, TERM)
            .subscriber(Value::String("alice".into()))
            .term_values(vec!["tenant_id", "id"], vec![vec![Value::Int(1)]]),
    );
    assert!(
        reason.contains("carries 1 values where 2 columns were named"),
        "the refusal counts both sides: {reason}"
    );
}

/// Values stated for a partial column set name no term: the pair is the unit.
#[test]
fn values_stated_for_half_the_pair_are_refused() {
    let (mut engine, _, _) = engine();
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, TERM)
            .subscriber(Value::String("alice".into()))
            .term_values(vec!["tenant_id"], vec![vec![Value::Int(1)]]),
    );
    assert!(
        reason.contains("compares together"),
        "the refusal says the set does not match: {reason}"
    );
}

/// The filter may write its pairs in any order. The membership row is read by
/// the written pairing, not by the key order `rls2fga` reports, which sorts a
/// composite key by the object it names.
#[test]
fn a_reversed_pair_order_still_pairs_by_name() {
    const REVERSED: &str = "SELECT * FROM docs WHERE EXISTS (SELECT 1 FROM shares s \
         WHERE s.doc_id = docs.id AND s.tenant_id = docs.tenant_id \
           AND s.viewer = current_setting('app.user_id', true))";
    let (mut engine, docs, shares) = engine();
    engine
        .register(
            SubscriptionRequest::new(1u64, REVERSED)
                .subscriber(Value::String("alice".into()))
                .term_values(
                    vec!["id", "tenant_id"],
                    vec![vec![Value::Int(5), Value::Int(1)]],
                ),
        )
        .unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(5, 1)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "document 5 under tenant 1 is stated"
    );
    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 5)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "document 1 under tenant 5 crosses the pair"
    );

    let granted = TestEvent::insert(shares, share(1, 6, "alice"));
    let narrowings = engine.consumers(&granted).unwrap();
    assert_eq!(
        narrowings.narrowings()[0].values,
        vec![Value::Int(6), Value::Int(1)],
        "the narrowing follows the filter's own order, document first"
    );
    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(6, 1)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the share row was read by the written pairing"
    );
}
