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
use subql::backend::{Postgres, Value};
use subql::compiler::MAX_TERMS_PER_FILTER;
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, TableId,
};

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
            "project_id",
            projects.iter().copied().map(Value::Int).collect(),
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
    assert!(notifs.inserted().is_empty());
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
                .term_values("project_id", vec![Value::Int(7)]),
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
            .term_values("project_id", vec![Value::Int(project)])
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
    assert!(notifs.inserted().is_empty());
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
    assert!(engine
        .consumers(&null_project)
        .unwrap()
        .inserted()
        .is_empty());
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
        reason.contains("two membership subqueries"),
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
            .term_values("title", vec![Value::String("x".into())]),
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
    assert!(!reason.is_empty());
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
    assert!(engine
        .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
        .unwrap()
        .inserted()
        .is_empty());

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
    assert_eq!(narrowings[0].value, Value::Int(11));
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
    assert_eq!(narrowings[0].value, Value::Int(7));
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

    let mut narrowings: Vec<(Value<Postgres>, bool)> = notifs
        .narrowings()
        .iter()
        .map(|n| (n.value.clone(), n.entered))
        .collect();
    narrowings.sort_by_key(|(_, entered)| *entered);
    assert_eq!(
        narrowings,
        vec![(Value::Int(7), false), (Value::Int(11), true)],
        "one row moved, so one value left and one arrived"
    );

    assert!(engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "old")))
        .unwrap()
        .inserted()
        .is_empty());
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
    assert!(engine
        .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
        .unwrap()
        .inserted()
        .is_empty());
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
            narrowing.value.clone()
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
    assert!(engine.consumers(&partial).unwrap().narrowings().is_empty());
    assert!(engine
        .consumers(&TestEvent::insert(docs, doc(1, 11, "spec")))
        .unwrap()
        .inserted()
        .is_empty());
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
    assert!(notifs.narrowings().is_empty());
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
