//! A membership subquery, end to end: two subscribers share one filter and one
//! compiled predicate, and each changed row reaches only the subscribers the
//! relationship admits.
//!
//! The filter under test is the shape the whole feature exists for: `docs`
//! filtered on membership of the project the document belongs to, where the
//! discriminating value lives on neither the subscribed row nor the subscriber.
#![cfg(feature = "membership-term")]
#![allow(clippy::unwrap_used)]

use rls2fga::classifier::patterns::ConfidenceLevel;
use rls2fga::translator::{Translator, TranslatorBuilder};
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, ScalarKind, Value};
use subql::compiler::MAX_TERMS_PER_FILTER;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, TableId,
    Tier,
};

/// One-wide value rows, the shape the tuple-stating API takes for the
/// ordinary single-column term.
fn rows_of(values: Vec<Value<Postgres>>) -> Vec<Vec<Value<Postgres>>> {
    values.into_iter().map(|value| vec![value]).collect()
}

const DDL: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
     CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
     CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER, title TEXT, score DOUBLE PRECISION, a INTEGER, b INTEGER, c INTEGER, d INTEGER);";

/// The filter the whole feature exists for: which documents belong to a project
/// the subscriber is a member of.
const TERM: &str = "SELECT * FROM docs WHERE project_id IN \
     (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn translator() -> Translator {
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
}

fn engine() -> (Engine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let docs = catalog_helpers::table_id(&db, "docs").expect("docs is in the catalog");
    let engine = SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());
    (engine, docs)
}

/// One subscription for `consumer`, filtering for `user`, stating the projects
/// it currently belongs to.
fn subscribe(
    consumer: u64,
    user: &str,
    projects: &[i64],
) -> SubscriptionRequest<DefaultIds, Postgres> {
    SubscriptionRequest::new(consumer, TERM)
        .subscriber(Value::String(user.into()))
        .term_values(
            vec!["project_id"],
            rows_of(projects.iter().copied().map(Value::Int).collect()),
        )
}

/// A `docs` row: `(id, project_id, title, score)`.
fn doc(id: i64, project: i64, title: &str) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::Int(project),
        Value::String(title.into()),
        Value::Float(1.0),
    ]
}

/// The refusal a filter carrying a term is met with, or a panic naming what
/// happened instead.
fn refusal(engine: &mut Engine, spec: SubscriptionRequest<DefaultIds, Postgres>) -> String {
    match engine.register(spec) {
        Err(RegisterError::MembershipTermRefused(reason)) => reason,
        other => panic!("expected a term refusal, got {other:?}"),
    }
}

/// The point of the whole feature: one filter, one compiled predicate, and a
/// changed row that reaches only the subscribers the relationship admits.
#[test]
fn a_changed_row_reaches_only_the_subscribers_the_relationship_admits() {
    let (mut engine, docs) = engine();
    engine.register(subscribe(1, "alice", &[7])).unwrap();
    engine.register(subscribe(2, "bob", &[9])).unwrap();

    assert_eq!(
        engine.predicate_count(docs),
        1,
        "identical filter text still collapses to one compiled predicate"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "spec")))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the document belongs to project 7, which only alice is a member of"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 9, "plan")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[2], "project 9 is bob's");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(3, 11, "other")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "project 11 is nobody's, and the filter is not simply ignored"
    );
}

/// A delete narrows the same way an insert does. Reached through a different
/// dispatch path, which is why it is asserted rather than assumed.
#[test]
fn a_deleted_row_leaves_only_the_subscribers_it_reached() {
    let (mut engine, docs) = engine();
    engine.register(subscribe(1, "alice", &[7])).unwrap();
    engine.register(subscribe(2, "bob", &[9])).unwrap();

    let notifs = engine
        .consumers(&TestEvent::delete(docs, doc(1, 7, "spec")))
        .unwrap();
    assert_eq!(notifs.deleted(), &[1]);
    assert!(
        notifs.inserted().is_empty(),
        "a delete event fires no insertions"
    );
}

/// One base UPDATE, three different verdicts, out of one evaluation of one
/// shared predicate. This is the case a boolean predicate cannot express.
#[test]
fn one_update_is_an_insert_for_one_subscriber_and_a_delete_for_another() {
    let (mut engine, docs) = engine();
    engine.register(subscribe(1, "alice", &[7])).unwrap();
    engine.register(subscribe(2, "bob", &[9])).unwrap();
    engine.register(subscribe(3, "carol", &[7, 9])).unwrap();

    let moved =
        TestEvent::update(docs, doc(1, 7, "spec"), doc(1, 9, "spec")).with_changed_columns([1u16]);
    let notifs = engine.consumers(&moved).unwrap();

    assert_eq!(
        notifs.inserted(),
        &[2],
        "the document moved into bob's project"
    );
    assert_eq!(
        notifs.deleted(),
        &[1],
        "and out of alice's, which she is told about"
    );
    assert_eq!(
        notifs.updated(),
        &[3],
        "carol belongs to both, so for her the row only changed"
    );
}

/// A row test and a term compose: the row test can still refuse a row the term
/// admits, and the term can still refuse one the row test admits.
#[test]
fn a_row_test_and_a_term_both_have_to_hold() {
    let (mut engine, docs) = engine();
    let and_filter = "SELECT * FROM docs WHERE title = 'keep' AND project_id IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";
    engine
        .register(
            SubscriptionRequest::new(1u64, and_filter)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["project_id"], rows_of(vec![Value::Int(7)])),
        )
        .unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "keep")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "both halves hold");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 7, "drop")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the term admits alice and the row test refuses the row"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(3, 11, "keep")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the row test holds and the term admits nobody"
    );
}

/// A term under `OR` is the case the "intersect the bitmap" reading cannot
/// serve: a row failing the term still reaches everybody when the other side
/// holds, which is the complement of the term's set rather than its intersection.
#[test]
fn a_term_under_or_admits_everybody_the_other_side_admits() {
    let (mut engine, docs) = engine();
    let or_filter = "SELECT * FROM docs WHERE title = 'public' OR project_id IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";
    let subscribe_or = |consumer: u64, user: &str, project: i64| {
        SubscriptionRequest::new(consumer, or_filter)
            .subscriber(Value::String(user.into()))
            .term_values(vec!["project_id"], rows_of(vec![Value::Int(project)]))
    };
    engine.register(subscribe_or(1, "alice", 7)).unwrap();
    engine.register(subscribe_or(2, "bob", 9)).unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 11, "public")))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "nobody is a member of project 11, and the row is public, so both see it"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 7, "private")))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "not public, so only the member of project 7 sees it"
    );
}

/// A subscription that states no starting values admits nobody, rather than
/// everybody. A client may legitimately send a partial list, and the direction
/// an absent one degrades in is the one requirement 6 tolerates.
#[test]
fn a_subscription_stating_no_values_admits_nobody() {
    let (mut engine, docs) = engine();
    engine
        .register(SubscriptionRequest::new(1u64, TERM).subscriber(Value::String("alice".into())))
        .unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "spec")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "no stated membership means no row, never every row"
    );
}

/// A row that does not carry the compared column cannot say, and "cannot say" is
/// not "nobody": it propagates as unknown and the row simply does not match,
/// without the absence being read as a decision.
#[test]
fn a_row_missing_the_compared_column_matches_nobody() {
    let (mut engine, docs) = engine();
    engine.register(subscribe(1, "alice", &[7])).unwrap();

    // A row image carrying only the key: `project_id` is absent, not null.
    let partial = TestEvent::insert(docs, vec![Value::Int(1)]);
    let notifs = engine.consumers(&partial).unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "project_id absent so no subscriber can be admitted"
    );
}

/// A null compared value admits nobody, because `NULL IN (SELECT ...)` is never
/// true in SQL.
#[test]
fn a_null_compared_value_admits_nobody() {
    let (mut engine, docs) = engine();
    engine.register(subscribe(1, "alice", &[7])).unwrap();

    let null_project = TestEvent::insert(
        docs,
        vec![
            Value::Int(1),
            Value::Null,
            Value::String("spec".into()),
            Value::Float(1.0),
        ],
    );
    assert!(
        engine
            .consumers(&null_project)
            .unwrap()
            .inserted()
            .is_empty(),
        "null cannot be IN any set so no subscriber is admitted"
    );
}

/// Unregistering takes the subscriber out of the term's set. Asserted by
/// re-registering the same consumer with different membership rather than by
/// checking that a removed subscription receives nothing: the predicate's own
/// bitmap already excludes a removed binding, so that weaker assertion holds
/// whether or not the term set was cleaned up, and would be vacuous.
#[test]
fn unregistering_takes_the_subscriber_out_of_the_set() {
    let (mut engine, docs) = engine();
    let alice = engine.register(subscribe(1, "alice", &[7])).unwrap();
    engine.register(subscribe(2, "bob", &[9])).unwrap();

    assert!(engine.unregister_subscription(alice.subscription_id));
    // Same consumer, same filter, different membership. Her ordinal is the one
    // she had, so a set that kept the old value admits her to project 7 still.
    engine.register(subscribe(1, "alice", &[11])).unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "spec")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "alice left project 7, so the row that project admitted reaches nobody"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 11, "spec")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "and her new membership admits her");
}

/// Registering the same filter through the batch path seeds the terms too. The
/// batch path assigns its predicate ids in a later phase than the single path,
/// so seeding it is a separate piece of wiring and a separate way to get it
/// wrong.
#[test]
fn the_batch_path_seeds_the_terms_as_the_single_path_does() {
    let (mut engine, docs) = engine();
    let results =
        engine.register_batch(vec![subscribe(1, "alice", &[7]), subscribe(2, "bob", &[9])]);
    for result in &results {
        assert!(result.is_ok(), "batch registration failed: {result:?}");
    }
    assert_eq!(engine.predicate_count(docs), 1);

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "spec")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1]);
}

/// Two spellings of one two-term filter share a predicate, because the
/// normalized text sorts conjuncts. The slots a subscriber's values land in
/// must therefore not depend on the order its own source text named the
/// terms, or one column's values are stored where dispatch reads another's:
/// rows the subscriber should get never arrive, and rows whose columns hold
/// each other's values arrive wrongly.
#[test]
fn a_reversed_spelling_binds_its_values_to_the_columns_they_name() {
    let (mut engine, docs) = engine();
    let subquery = "(SELECT project_id FROM project_members \
         WHERE user_id = current_setting('app.user_id', true))";
    let forward = format!("SELECT * FROM docs WHERE project_id IN {subquery} AND a IN {subquery}");
    let reversed = format!("SELECT * FROM docs WHERE a IN {subquery} AND project_id IN {subquery}");
    engine
        .register(
            SubscriptionRequest::new(1u64, forward)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["project_id"], rows_of(vec![Value::Int(7)]))
                .term_values(vec!["a"], rows_of(vec![Value::Int(70)])),
        )
        .unwrap();
    engine
        .register(
            SubscriptionRequest::new(2u64, reversed)
                .subscriber(Value::String("bob".into()))
                .term_values(vec!["project_id"], rows_of(vec![Value::Int(9)]))
                .term_values(vec!["a"], rows_of(vec![Value::Int(90)])),
        )
        .unwrap();

    assert_eq!(
        engine.predicate_count(docs),
        1,
        "the premise: both spellings share one compiled predicate"
    );

    // A `docs` row: `(id, project_id, title, score, a)`.
    let row = |id: i64, project: i64, a: i64| {
        vec![
            Value::<Postgres>::Int(id),
            Value::Int(project),
            Value::String("spec".into()),
            Value::Float(1.0),
            Value::Int(a),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(docs, row(1, 7, 70)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the spelling that created the predicate is bound straight"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, row(2, 9, 90)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[2],
        "bob stated project 9 and a 90, and this row is exactly that"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, row(3, 90, 9)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "nobody stated project 90, so a row whose columns hold each other's \
         values reaches nobody"
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// Without translation settings the engine cannot tell whether a relationship
/// can be served, and it says so rather than defaulting the setting that names
/// the caller.
#[test]
fn a_term_is_refused_when_the_engine_has_no_translator() {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
    let reason = refusal(&mut engine, subscribe(1, "alice", &[7]));
    assert!(
        reason.contains("with_translator"),
        "the refusal should name the missing configuration, got {reason:?}"
    );
}

/// A term needs to know who it filters for, because that identity is what a
/// changed membership row is matched against.
#[test]
fn a_term_is_refused_without_a_subscriber() {
    let (mut engine, _) = engine();
    let reason = refusal(&mut engine, SubscriptionRequest::new(1u64, TERM));
    assert!(
        reason.contains("which subscriber"),
        "the refusal should ask for the subscriber, got {reason:?}"
    );
}

/// One accumulator is shared between an aggregate's consumers, and a term gives
/// each of them a different set of rows to aggregate.
#[test]
fn a_term_is_refused_on_an_aggregate() {
    let (mut engine, _) = engine();
    let sql = "SELECT COUNT(*) FROM docs WHERE project_id IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())),
    );
    assert!(
        reason.contains("accumulator"),
        "the refusal should say why an aggregate cannot carry one, got {reason:?}"
    );
}

/// A float is not equal to itself, so a lookup keyed on one could not find what
/// it stored.
#[test]
fn a_term_comparing_an_unkeyable_column_is_refused() {
    let (mut engine, _) = engine();
    let sql = "SELECT * FROM docs WHERE score IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())),
    );
    assert!(
        reason.contains("not equal to itself"),
        "the refusal should say why the column cannot be a key, got {reason:?}"
    );
}

/// Two terms comparing one column make that column's name ambiguous in the
/// values a subscription states, and the name is the only thing both sides share.
#[test]
fn two_terms_comparing_the_same_column_are_refused() {
    let (mut engine, _) = engine();
    let sql = "SELECT * FROM docs WHERE project_id IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true)) \
         AND project_id IN (SELECT project_id FROM project_members WHERE user_id = 'x')";
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())),
    );
    assert!(
        reason.contains("two membership terms"),
        "the refusal should name the ambiguity, got {reason:?}"
    );
}

/// Values stated for a column no subquery compares would be stored where
/// nothing reads them, which reads to the client as a filter that admits nobody
/// for no visible reason.
#[test]
fn values_stated_for_an_uncompared_column_are_refused() {
    let (mut engine, _) = engine();
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, TERM)
            .subscriber(Value::String("alice".into()))
            .term_values(vec!["title"], rows_of(vec![Value::String("x".into())])),
    );
    assert!(
        reason.contains("no membership subquery"),
        "the refusal should name the column nothing compares, got {reason:?}"
    );
}

/// A relationship `rls2fga` will not compile is refused in its own wording, so
/// an operator reads why rather than reading that SubQL dislikes subqueries.
#[test]
fn a_relationship_that_does_not_compile_is_refused_in_rls2fga_wording() {
    let (mut engine, _) = engine();
    // Nothing about a `docs` row decides this: the inner filter names no caller.
    let sql = "SELECT * FROM docs WHERE project_id IN \
         (SELECT project_id FROM project_members WHERE user_id = 'someone-else')";
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())),
    );
    assert!(
        !reason.contains("SubQL"),
        "the reason should be the one rls2fga gave, got {reason:?}"
    );
    assert!(
        !reason.is_empty(),
        "a refused relationship must carry a non-empty reason"
    );
}
/// A term comparing a column of a different kind than the relationship is keyed
/// on is refused. The classifier reads the inner half of the correlation and can
/// produce a shape keyed on a column the filter never named, so this is the one
/// check standing between that and a lookup that stores under one key and reads
/// under another.
#[test]
fn a_term_comparing_a_differently_typed_column_is_refused() {
    let (mut engine, _) = engine();
    let sql = "SELECT * FROM docs WHERE title IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";
    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())),
    );
    assert!(
        !reason.is_empty(),
        "a filter comparing text against an integer key must not be served"
    );
}

// ---------------------------------------------------------------------------
// A changed membership row moves the set, and says so
// ---------------------------------------------------------------------------

/// The membership table, and a row of it: `(project_id, user_id)`.
fn members_table(engine: &Engine) -> TableId {
    catalog_helpers::table_id(engine.database(), "project_members")
        .expect("project_members is in the catalog")
}

fn membership(project: i64, user: &str) -> Vec<Value<Postgres>> {
    vec![Value::Int(project), Value::String(user.into())]
}

/// A membership appearing moves the subscriber set, and the subscription is told
/// that its answer changed. The message carries no row, because the rows now
/// qualifying were never in this event.
#[test]
fn a_new_membership_row_moves_the_set_and_reports_the_narrowing() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    let alice = engine.register(subscribe(1, "alice", &[7])).unwrap();

    // Before: project 11 admits nobody.
    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
            .unwrap()
            .inserted()
            .is_empty(),
        "project 11 is not yet in alice's admitted set"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(members, membership(11, "alice")))
        .unwrap();

    assert!(
        notifs.inserted().is_empty() && notifs.updated().is_empty(),
        "a membership row is not a row of the subscription, so no row is delivered"
    );
    let narrowings = notifs.narrowings();
    assert_eq!(narrowings.len(), 1, "one subscription, one narrowing");
    assert_eq!(narrowings[0].subscription, alice.subscription_id);
    assert_eq!(narrowings[0].table, docs, "it names the subscribed table");
    assert_eq!(narrowings[0].values, vec![Value::Int(11)]);
    assert!(narrowings[0].entered, "the value entered her set");

    // After: the same row now reaches her.
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(docs, doc(2, 11, "spec")))
            .unwrap()
            .inserted(),
        &[1],
        "the set moved, so the filter now admits the project"
    );
}

/// A membership disappearing takes the value away, and the subscription is told.
/// The row it loses is one the consumer already holds, which is why no row
/// travels with the message.
#[test]
fn a_removed_membership_row_takes_the_value_away() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    let alice = engine.register(subscribe(1, "alice", &[7])).unwrap();

    let notifs = engine
        .consumers(&TestEvent::delete(members, membership(7, "alice")))
        .unwrap();

    let narrowings = notifs.narrowings();
    assert_eq!(narrowings.len(), 1);
    assert_eq!(narrowings[0].subscription, alice.subscription_id);
    assert_eq!(narrowings[0].values, vec![Value::Int(7)]);
    assert!(!narrowings[0].entered, "the value left her set");

    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(1, 7, "spec")))
            .unwrap()
            .inserted()
            .is_empty(),
        "she is no longer a member, so project 7 admits her no longer"
    );
}

/// The direction stated as carefully as the absent one: a value the subscriber
/// does not match is trusted too, and only a membership row naming that same
/// pair ever takes it away. A membership that never existed has no row to send,
/// so the widening lasts as long as the subscription.
#[test]
fn a_value_the_subscriber_never_matched_keeps_admitting_it() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    // Alice is a member of project 7. Project 11 is the stale value: she is not
    // a member, so the membership table holds no row naming the pair.
    engine.register(subscribe(1, "alice", &[7, 11])).unwrap();

    assert_eq!(
        engine
            .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
            .unwrap()
            .inserted(),
        &[1],
        "a stated value admits whether or not a membership row backs it"
    );

    // Membership traffic naming another project, or another person in this one,
    // moves nothing: no pass reconciles the set against the table.
    engine
        .consumers(&TestEvent::delete(members, membership(7, "alice")))
        .unwrap();
    engine
        .consumers(&TestEvent::delete(members, membership(11, "bob")))
        .unwrap();

    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(2, 7, "gone")))
            .unwrap()
            .inserted()
            .is_empty(),
        "the pair that did change was withdrawn, so what survives is not a dead dispatch path"
    );
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(docs, doc(3, 11, "plan")))
            .unwrap()
            .inserted(),
        &[1],
        "neither event named (11, alice), so the value she never matched still admits her"
    );

    // Only that pair's own row withdraws it, which is the row a membership that
    // never existed cannot produce.
    let notifs = engine
        .consumers(&TestEvent::delete(members, membership(11, "alice")))
        .unwrap();
    let narrowings = notifs.narrowings();
    assert_eq!(narrowings.len(), 1, "one subscription, one withdrawal");
    assert_eq!(narrowings[0].values, vec![Value::Int(11)]);
    assert!(!narrowings[0].entered, "the value left her set");
    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(4, 11, "note")))
            .unwrap()
            .inserted()
            .is_empty(),
        "and once that row arrives the value stops admitting, as any other would"
    );
}

/// A membership moving from one project to another is one departure and one
/// arrival, reported as both.
#[test]
fn an_updated_membership_row_reports_both_halves() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    engine.register(subscribe(1, "alice", &[7])).unwrap();

    let moved = TestEvent::update(members, membership(7, "alice"), membership(11, "alice"))
        .with_changed_columns([0u16]);
    let notifs = engine.consumers(&moved).unwrap();

    let mut narrowings: Vec<(Vec<Value<Postgres>>, bool)> = notifs
        .narrowings()
        .iter()
        .map(|n| (n.values.clone(), n.entered))
        .collect();
    narrowings.sort_by_key(|(_, entered)| *entered);
    assert_eq!(
        narrowings,
        vec![(vec![Value::Int(7)], false), (vec![Value::Int(11)], true)],
        "one row moved, so one value left and one arrived"
    );

    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(1, 7, "old")))
            .unwrap()
            .inserted()
            .is_empty(),
        "alice left project 7 so that document no longer reaches her"
    );
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(docs, doc(2, 11, "new")))
            .unwrap()
            .inserted(),
        &[1]
    );
}

/// A membership naming somebody who subscribes to nothing moves nothing, and
/// says nothing. Otherwise every membership row in the database would produce a
/// message.
#[test]
fn a_membership_naming_another_person_moves_nobody() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    engine.register(subscribe(1, "alice", &[7])).unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(members, membership(11, "stranger")))
        .unwrap();
    assert!(
        notifs.narrowings().is_empty(),
        "nobody here filters for that identity"
    );
    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
            .unwrap()
            .inserted()
            .is_empty(),
        "stranger is not any subscriber so project 11 admits nobody"
    );
}

/// Truncating the membership table withdraws every value it admitted. Leaving
/// the sets alone would keep admitting rows through memberships that no longer
/// exist, which is the one error direction the design refuses.
#[test]
fn truncating_the_membership_table_withdraws_every_value() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    engine.register(subscribe(1, "alice", &[7, 11])).unwrap();

    let notifs = engine.consumers(&TestEvent::truncate(members)).unwrap();
    let mut values: Vec<Value<Postgres>> = notifs
        .narrowings()
        .iter()
        .map(|narrowing| {
            assert!(!narrowing.entered);
            assert_eq!(
                narrowing.values.len(),
                1,
                "a one-column term withdraws one-wide rows"
            );
            narrowing.values[0].clone()
        })
        .collect();
    values.sort_by_key(|value| match value {
        Value::Int(v) => *v,
        other => panic!("expected an int, got {other:?}"),
    });
    assert_eq!(values, vec![Value::Int(7), Value::Int(11)]);

    for project in [7, 11] {
        assert!(
            engine
                .consumers(&TestEvent::insert(docs, doc(1, project, "spec")))
                .unwrap()
                .inserted()
                .is_empty(),
            "every membership is gone, so no project admits anybody"
        );
    }
}

/// A membership row missing the column naming the subscriber moves nothing. A
/// half-read row would otherwise move whichever subscribers happened to key on
/// the absent value.
#[test]
fn a_membership_row_missing_the_subscriber_moves_nobody() {
    let (mut engine, docs) = engine();
    let members = members_table(&engine);
    engine.register(subscribe(1, "alice", &[7])).unwrap();

    // Only the project half of the row.
    let partial = TestEvent::insert(members, vec![Value::Int(11)]);
    assert!(
        engine.consumers(&partial).unwrap().narrowings().is_empty(),
        "a membership row without user_id names no subscriber"
    );
    assert!(
        engine
            .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
            .unwrap()
            .inserted()
            .is_empty(),
        "the partial row never opened project 11 to any subscriber"
    );
}

/// An event on a table no membership subquery reads through produces no
/// narrowing at all, which is what keeps the whole mechanism absent from every
/// filter that names no term.
#[test]
fn an_ordinary_event_reports_no_narrowing() {
    let (mut engine, docs) = engine();
    engine.register(subscribe(1, "alice", &[7])).unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "spec")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1]);
    assert!(
        notifs.narrowings().is_empty(),
        "a docs event never touches the membership set"
    );
}

/// A filter naming more membership subqueries than the cap is refused. The cost
/// of a term is one evaluation per combination of the terms, so the cap is what
/// keeps that from doubling per subquery without bound.
#[test]
fn a_filter_past_the_term_cap_is_refused() {
    let (mut engine, _) = engine();
    let term_on = |column: &str| {
        format!(
            "{column} IN (SELECT project_id FROM project_members WHERE user_id = \
             current_setting('app.user_id', true))"
        )
    };
    let sql = format!(
        "SELECT * FROM docs WHERE {}",
        ["project_id", "a", "b", "c", "d"]
            .map(term_on)
            .join(" AND ")
    );

    let reason = refusal(
        &mut engine,
        SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into())),
    );
    assert!(
        reason.contains(&format!("at most {MAX_TERMS_PER_FILTER}")),
        "the refusal should name the cap, got {reason:?}"
    );

    // One below the cap still registers, so the cap is a boundary rather than a
    // blanket refusal of several terms.
    let under = format!(
        "SELECT * FROM docs WHERE {}",
        ["project_id", "a", "b", "c"].map(term_on).join(" AND ")
    );
    assert!(engine
        .register(SubscriptionRequest::new(2u64, under).subscriber(Value::String("alice".into())))
        .is_ok());
}

// ---------------------------------------------------------------------------
// What a caller has to read before it can register
// ---------------------------------------------------------------------------

/// Registration consumes the seed, so a caller that cannot ask what to seed can
/// only guess. This is that question, and the answer names the read in the
/// catalog's own words.
#[test]
fn describe_terms_names_what_a_seed_read_needs() {
    let (engine, _) = engine();
    let described = engine
        .describe_terms(&SubscriptionRequest::new(1u64, TERM))
        .expect("the canonical term is describable");
    let [term] = described.as_slice() else {
        panic!("one membership subquery, got {described:?}");
    };

    let [pair] = term.pairs.as_slice() else {
        panic!("one compared column, got {:?}", term.pairs);
    };
    assert_eq!(pair.column, "project_id", "the term_values key");
    assert_eq!(term.member_table, "project_members");
    assert_eq!(pair.member_key, "project_id");
    assert_eq!(term.member_subject, "user_id");
    assert_eq!(
        term.subject_kind,
        ScalarKind::String,
        "user_id is TEXT, and a subscriber built at another kind admits nobody"
    );
    assert_eq!(
        pair.kind,
        ScalarKind::Int,
        "project_id is INTEGER, which is what the seed rows decode as"
    );
    assert_eq!(
        term.seed_sql,
        "SELECT project_id FROM project_members WHERE user_id = \
         current_setting('app.user_id', true)",
        "the seed read is the subquery itself, so it cannot disagree with the filter"
    );
}

/// The ordering the whole entry point exists for: describing needs no seed,
/// because the seed is what the answer is for.
#[test]
fn describe_terms_answers_before_a_subscriber_is_known() {
    let (mut engine, _) = engine();
    let unseeded = SubscriptionRequest::new(1u64, TERM);
    assert_eq!(
        engine.describe_terms(&unseeded).map(|d| d.len()).ok(),
        Some(1),
        "an unseeded request must still be describable"
    );
    assert!(
        matches!(
            engine.register(SubscriptionRequest::new(1u64, TERM)),
            Err(RegisterError::MembershipTermRefused(_))
        ),
        "registering the same unseeded request is what is refused"
    );
}

/// Describing is not registering. A caller describes a filter it may then
/// refuse to register, and nothing may be left behind.
#[test]
fn describe_terms_registers_nothing() {
    let (mut engine, docs) = engine();
    engine
        .describe_terms(&SubscriptionRequest::new(1u64, TERM))
        .expect("describable");

    assert_eq!(engine.predicate_count(docs), 0, "no predicate was compiled");
    assert_eq!(engine.subscription_count(), 0, "nothing was bound");

    // And the same filter still registers afterwards, so describing did not
    // consume anything either.
    engine.register(subscribe(1, "alice", &[7])).unwrap();
    assert_eq!(engine.predicate_count(docs), 1);
}

/// Every filter names no term until one is written, so the answer for one is
/// empty rather than an error the caller has to recognise as harmless.
#[test]
fn describe_terms_is_empty_for_a_filter_naming_no_term() {
    let (engine, _) = engine();
    assert!(engine
        .describe_terms(&SubscriptionRequest::new(
            1u64,
            "SELECT * FROM docs WHERE project_id = 7"
        ))
        .expect("a plain filter describes")
        .is_empty());
}

/// Describing and registering the same request agree, or a panic naming which
/// of the two broke ranks. Registering no longer turns these away: a shape the
/// in-process evaluator refuses lands on a tier that re-reads it, carrying the
/// same words describing refused with. Both serving it in process is a failure
/// too: a parity assertion over two successes proves nothing.
fn refuses_alike(sql: &str) {
    let (mut engine, _) = engine();
    let request = || SubscriptionRequest::new(1u64, sql).subscriber(Value::String("alice".into()));
    let described = engine
        .describe_terms(&request())
        .err()
        .map(|error| error.to_string());
    assert!(described.is_some(), "{sql} must be refused at all");
    let described = described.expect("checked just above");

    match engine.register(request()) {
        Ok(registered) => {
            assert!(
                !matches!(registered.tier, Tier::InProcess(_)),
                "{sql} must not be served in process, got {:?}",
                registered.tier
            );
            let reason = registered
                .not_served_because
                .expect("a tier that needs a read says why");
            assert!(
                described.contains(&reason),
                "describing said `{described}`, registering said `{reason}`, for {sql}"
            );
        }
        Err(refused) => assert_eq!(
            described,
            refused.to_string(),
            "describing must refuse exactly as registering does, for {sql}"
        ),
    }
}

/// The point of sharing one classification: a filter the caller was told to seed
/// is a filter registration accepts, and one it refuses is refused there for the
/// same stated reason.
#[test]
fn describe_terms_refuses_exactly_what_register_refuses() {
    let member = "(SELECT project_id FROM project_members WHERE user_id = \
                  current_setting('app.user_id', true))";
    for sql in [
        // An aggregate cannot carry a term.
        format!("SELECT COUNT(*) FROM docs WHERE project_id IN {member}"),
        // A float column cannot key the lookup.
        format!("SELECT * FROM docs WHERE score IN {member}"),
        // Text compared against an integer key.
        format!("SELECT * FROM docs WHERE title IN {member}"),
        // Two distinct subqueries comparing one column. Identical text collapses
        // to one slot instead, so the two have to differ to reach the refusal.
        format!(
            "SELECT * FROM docs WHERE project_id IN {member} AND project_id IN (SELECT \
             project_id FROM project_members WHERE user_id = 'x')"
        ),
        // A relationship rls2fga will not compile: the inner filter names nobody.
        "SELECT * FROM docs WHERE project_id IN (SELECT project_id FROM project_members WHERE \
         user_id = 'someone-else')"
            .to_string(),
        // Outside SubQL's statement shape, refused before any term is seen.
        format!("SELECT * FROM docs d JOIN projects p ON p.id = d.project_id WHERE d.project_id IN {member}"),
        // A joined membership subquery, refused by the bounded form before the
        // relationship is ever classified. Same rule as the outer statement, so
        // no caller can be told to seed a shape registration will not take.
        "SELECT * FROM docs WHERE project_id IN (SELECT pm.project_id FROM project_members pm \
         JOIN projects p ON p.id = pm.project_id WHERE pm.user_id = \
         current_setting('app.user_id', true))"
            .to_string(),
    ] {
        refuses_alike(&sql);
    }
}

/// Two subqueries on different columns are two descriptions, each keyed by the
/// column it compares, because that key is the only thing the caller and the
/// engine share. The two subqueries differ in text so that pairing a description
/// with the wrong one is visible. The order is slot order, which follows the
/// normalized text rather than the source text (`a` sorts before `project_id`),
/// since a shared predicate's slots cannot depend on how one spelling ordered
/// its conjuncts.
#[test]
fn describe_terms_answers_once_per_subquery() {
    let (engine, _) = engine();
    let sql = "SELECT * FROM docs WHERE project_id IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true)) \
         AND a IN (SELECT pm.project_id FROM project_members AS pm WHERE pm.user_id = current_setting('app.user_id', true))";
    let described = engine
        .describe_terms(&SubscriptionRequest::new(1u64, sql))
        .expect("two terms on two columns are describable");
    let pairs: Vec<(&str, bool)> = described
        .iter()
        .map(|term| {
            (
                term.pairs[0].column.as_str(),
                term.seed_sql.contains("AS pm"),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        [("a", true), ("project_id", false)],
        "each description carries the subquery of its own compared column"
    );
}

/// The compared column and the inner projection are spelled differently, and
/// the guarded table also carries a decoy column named after the inner one.
/// The filter must move by the column it compares, never by the name twin.
/// Pins the wrong-allow shape rls2fga once had, where the bridge read the
/// guarded table's column named after the inner projection.
#[test]
fn a_cross_named_correlation_moves_by_the_compared_column() {
    const CROSS_DDL: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
         CREATE TABLE reports(id INTEGER PRIMARY KEY, proj INTEGER, project_id INTEGER, title TEXT);";
    const CROSS_TERM: &str = "SELECT * FROM reports WHERE proj IN \
         (SELECT project_id FROM project_members WHERE user_id = current_setting('app.user_id', true))";

    let db = ParserDB::parse::<PostgreSqlDialect>(CROSS_DDL).expect("DDL parses");
    let reports = catalog_helpers::table_id(&db, "reports").expect("reports is in the catalog");
    let mut engine: Engine =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());

    engine
        .register(
            SubscriptionRequest::new(1u64, CROSS_TERM)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["proj"], rows_of(vec![Value::Int(7)])),
        )
        .expect("a cross-named correlation registers");

    let row = |id: i64, proj: i64, decoy: i64| {
        vec![
            Value::Int(id),
            Value::Int(proj),
            Value::Int(decoy),
            Value::String("r".into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(reports, row(1, 7, 999)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the compared column holds alice's project, the decoy does not"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(reports, row(2, 999, 7)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "only the decoy column holds alice's project, so the row must not reach her"
    );
}
