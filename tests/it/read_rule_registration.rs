//! Caller-bound query interest intersected with the table's read rule.

use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
use rls2fga::translator::{Translator, TranslatorBuilder};
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use std::time::{Duration, Instant};
use subql::backend::{Postgres, ScalarFamily, Value};
use subql::term::{TermCaller, TermDescription};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, QueryProjection, SubscriptionEngine, SubscriptionRequest, TableId,
    TierKind,
};

use crate::common::store::{StoredEngine, TempStore};

const DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (
       owner = current_setting('app.user_id', true)
       OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));";

const UNION_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (
       owner = current_setting('app.user_id', true)
       OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
     CREATE POLICY items_public ON items FOR ALL USING (title = 'public');";

const RESTRICTIVE_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (
       owner = current_setting('app.user_id', true)
       OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
     CREATE POLICY items_shared ON items AS RESTRICTIVE FOR ALL USING (title = 'shared');";

const UNSUPPORTED_DDL: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (owner = current_setting('app.tenant_id', true));";

const ROLE_DDL: &str = "CREATE ROLE auditor;
     CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR SELECT TO auditor USING (
       owner = current_setting('app.user_id', true)
       OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));";

const MEMBERSHIP_DDL: &str =
    "CREATE TABLE team_members(team_id INTEGER, user_id TEXT, PRIMARY KEY(team_id, user_id));
     CREATE TABLE docs(id INTEGER PRIMARY KEY, team_id INTEGER, title TEXT);
     ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
     CREATE POLICY docs_p ON docs FOR ALL USING (
       EXISTS (SELECT 1 FROM team_members
               WHERE team_members.team_id = docs.team_id
                 AND team_members.user_id = current_setting('app.user_id', true)));";

const NO_READ_POLICY_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;";

const WRITE_ONLY_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_ins ON items FOR INSERT WITH CHECK (
       owner = current_setting('app.user_id', true));";

const RESTRICTIVE_ONLY_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_shared ON items AS RESTRICTIVE FOR ALL USING (title = 'shared');";

const NO_RLS_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     CREATE POLICY items_p ON items FOR ALL USING (
       owner = current_setting('app.user_id', true)
       OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));";

const NO_USING_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_open ON items FOR ALL;";

const PARENT_DDL: &str =
    "CREATE TABLE projects(id INTEGER PRIMARY KEY, owner TEXT);
     CREATE POLICY projects_p ON projects FOR ALL USING (
       owner = current_setting('app.user_id', true));
     CREATE TABLE docs(id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id), title TEXT);
     ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
     CREATE POLICY docs_p ON docs FOR ALL USING (
       EXISTS (SELECT 1 FROM projects
               WHERE projects.id = docs.project_id
                 AND projects.owner = current_setting('app.user_id', true)));";

const FUNCTION_DDL: &str =
    "CREATE TABLE items(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE FUNCTION item_is_mine(owner TEXT) RETURNS boolean LANGUAGE sql AS $$
       SELECT owner = current_setting('app.user_id', true)
     $$;
     CREATE POLICY items_p ON items FOR ALL USING (item_is_mine(owner));";

const ABAC_DDL: &str =
    "CREATE TABLE team_members(team_id INTEGER, user_id TEXT, PRIMARY KEY(team_id, user_id));
     CREATE TABLE docs(id INTEGER PRIMARY KEY, team_id INTEGER, status TEXT);
     ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
     CREATE POLICY docs_p ON docs FOR ALL USING (
       EXISTS (SELECT 1 FROM team_members
               WHERE team_members.team_id = docs.team_id
                 AND team_members.user_id = current_setting('app.user_id', true))
       AND docs.status IN ('active', 'pending'));";

const FLOAT_RULE_DDL: &str = "CREATE TABLE items(id INTEGER PRIMARY KEY, score REAL);
     ALTER TABLE items ENABLE ROW LEVEL SECURITY;
     CREATE POLICY items_p ON items FOR ALL USING (
       score = current_setting('app.score', true)::float8);";

const QUERY: &str = "SELECT * FROM items";

const SUBJECT_ONLY: &str = "SELECT * FROM items \
     WHERE owner = ANY(string_to_array(current_setting('app.subjects', true), ','))";

const BOUND: &str = "SELECT id, title FROM items WHERE title = $1";

const MEMBERS_QUERY: &str = "SELECT * FROM docs";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn translator_at(min: ConfidenceLevel) -> Translator {
    TranslatorBuilder::new()
        .with_min_confidence(min)
        .with_session_attributes([SessionAttribute::setting(
            "app.subjects",
            SessionAttributeKind::SetAttribute,
        )])
        .build()
}

fn translator() -> Translator {
    translator_at(ConfidenceLevel::B)
}

fn parse(ddl: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(ddl).expect("DDL parses")
}

fn table_id(db: &ParserDB, name: &str) -> TableId {
    catalog_helpers::table_id::<Postgres, _>(db, name).expect("the table is in the catalog")
}

fn engine_with(ddl: &str) -> (Engine, TableId) {
    let db = parse(ddl);
    let items = table_id(&db, "items");
    let engine = SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());
    (engine, items)
}

fn stored_with(path: std::path::PathBuf, db: ParserDB) -> Engine {
    StoredEngine::with_storage(db, PostgreSqlDialect {}, path)
        .expect("the store opens")
        .into_parts()
        .0
        .with_translator(translator())
}

fn for_user(
    consumer: u64,
    user: &str,
    subjects: &[&str],
) -> SubscriptionRequest<DefaultIds, Postgres> {
    SubscriptionRequest::new(consumer, QUERY)
        .subscriber(Value::String(user.into()))
        .subjects(
            subjects
                .iter()
                .map(|subject| Value::String((*subject).into())),
        )
}

fn rows_of(
    subject: &str,
    values: Vec<Value<Postgres>>,
) -> Vec<(Value<Postgres>, Vec<Value<Postgres>>)> {
    values
        .into_iter()
        .map(|value| (Value::String(subject.into()), vec![value]))
        .collect()
}

/// An `items` row as the change stream carries it, `(id, owner, project_id, title)`.
fn item(id: i64, owner: &str, project: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(owner.into()),
        Value::Int(project),
        Value::String("title".into()),
    ]
}

#[test]
fn the_rule_admits_the_row_to_only_the_callers_it_names() {
    let (mut engine, items) = engine_with(DDL);
    let registered = engine
        .register(for_user(1, "alice", &["team:a"]))
        .expect("the rule folds into the registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the rule is in the registered predicate"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    assert_eq!(
        engine.predicate_count(items),
        1,
        "the rule is caller-independent, so every caller shares the one predicate"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[1], "the row names alice's identity");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "team:a", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the row names a subject only alice's session holds"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(3, "bob", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[2], "the row names bob's identity");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(4, "carol", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the row admits no registered caller and reaches nobody"
    );
}

#[test]
fn empty_subjects_leave_only_the_identity_to_admit() {
    let (mut engine, items) = engine_with(DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("an empty subject set is legal SQL");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the rule is in the registered predicate"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[1], "the identity admits its own row");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "bob", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the empty set admits nobody and the row names no identity"
    );
}

#[test]
fn the_subject_only_interest_keeps_the_identity_out() {
    let (mut engine, items) = engine_with(DDL);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, SUBJECT_ONLY)
                .subscriber(Value::String("alice".into()))
                .subjects([Value::String("bob".into())]),
        )
        .expect("the interest registers");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the rule is in the registered predicate"
    );
    engine
        .register(
            SubscriptionRequest::new(2u64, SUBJECT_ONLY)
                .subscriber(Value::String("alice".into()))
                .subjects([Value::String("alice".into())]),
        )
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "bob", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the interest names bob as the subject"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[2],
        "the identity admits its row only where it is stated as a subject"
    );
}

#[test]
fn a_callerless_registration_keeps_query_only_matching() {
    let (mut engine, items) = engine_with(DDL);
    let registered = engine
        .register(SubscriptionRequest::new(1u64, QUERY))
        .expect("the plain interest registers");
    assert_eq!(
        registered.tier.kind(),
        TierKind::InProcess,
        "a plain interest is maintained in process"
    );
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "nothing the caller cannot state is folded in"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[1], "the interest admits the row");
}

#[test]
fn an_update_moving_the_owner_reports_both_sides_of_the_transfer() {
    let (mut engine, items) = engine_with(DDL);
    engine
        .register(for_user(1, "alice", &[]))
        .expect("alice registers");
    engine
        .register(for_user(2, "bob", &[]))
        .expect("bob registers");

    let moved = TestEvent::update(items, item(1, "alice", 7), item(1, "bob", 7))
        .with_changed_columns([1u16]);
    let notifs = engine.consumers(&moved).expect("dispatch runs");

    assert_eq!(notifs.inserted(), &[2], "the row moved into bob");
    assert_eq!(notifs.deleted(), &[1], "and out of alice, who is told");
    assert!(notifs.updated().is_empty(), "the row is nobody's update");
}

#[test]
fn a_key_only_transfer_leaves_every_caller_unanswered() {
    let (mut engine, items) = engine_with(DDL);
    engine
        .register(for_user(1, "alice", &[]))
        .expect("alice registers");
    engine
        .register(for_user(2, "bob", &[]))
        .expect("bob registers");

    let old = vec![
        Value::Int(1),
        Value::Missing,
        Value::Missing,
        Value::Missing,
    ];
    let moved = TestEvent::update(items, old, item(1, "bob", 7)).with_changed_columns([1u16]);
    let notifs = engine.consumers(&moved).expect("dispatch runs");

    assert!(
        notifs.inserted().is_empty(),
        "a caller may hold the row it cannot be told it lost"
    );
    assert!(
        notifs.deleted().is_empty(),
        "the old image cannot show the row leaving anyone"
    );
    assert!(
        notifs.updated().is_empty(),
        "an undecidable side is no transition"
    );

    let mut unanswered = notifs
        .unanswered()
        .iter()
        .map(|cell| (cell.consumer_id, cell.column))
        .collect::<Vec<_>>();
    unanswered.sort_unstable();
    assert_eq!(
        unanswered,
        vec![(1u64, 1u16), (2u64, 1u16)],
        "every caller read the owner the event does not carry"
    );
}

#[test]
fn a_bound_scalar_interest_intersects_the_rule() {
    let (mut engine, items) = engine_with(DDL);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, BOUND)
                .binds(vec![Value::String("keep".into())])
                .subscriber(Value::String("alice".into())),
        )
        .expect("the bound interest registers");
    let served = registered.served().expect("an in-process answer");
    assert!(
        served.read_rule_folded,
        "the rule is in the registered predicate"
    );
    assert!(
        matches!(&served.projection, QueryProjection::Columns { columns, .. } if columns.as_slice() == [0u16, 3u16]),
        "the interest's projection is preserved, got {served:?}"
    );

    let titled = |id: i64, owner: &str, title: &str| {
        vec![
            Value::Int(id),
            Value::String(owner.into()),
            Value::Int(7),
            Value::String(title.into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(1, "alice", "keep")))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the interest and the rule both admit the row"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(2, "alice", "drop")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the rule admits the caller and the interest refuses the row"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(3, "bob", "keep")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the interest admits the row and the rule names bob"
    );
}

#[test]
fn permissive_policies_compose_as_a_union() {
    let (mut engine, items) = engine_with(UNION_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the rule folds into the registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "both permissive arms are in the registered predicate"
    );

    let titled = |id: i64, owner: &str, title: &str| {
        vec![
            Value::Int(id),
            Value::String(owner.into()),
            Value::Int(7),
            Value::String(title.into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(1, "alice", "x")))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the deployment's arm admits the row"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(2, "bob", "public")))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the public arm admits the row nobody owns"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(3, "bob", "x")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "no arm admits the row and it reaches nobody"
    );
}

#[test]
fn a_restrictive_policy_intersects_the_union() {
    let (mut engine, items) = engine_with(RESTRICTIVE_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the rule folds into the registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the barrier is in the registered predicate"
    );

    let titled = |id: i64, owner: &str, title: &str| {
        vec![
            Value::Int(id),
            Value::String(owner.into()),
            Value::Int(7),
            Value::String(title.into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(1, "alice", "shared")))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the deployment's arm and the barrier both admit the row"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(2, "alice", "x")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the deployment's arm admits the row and the barrier refuses it"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(3, "bob", "shared")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the barrier admits the row and the deployment's arm names bob"
    );
}

#[test]
fn an_unclassifiable_rule_keeps_query_only_matching() {
    let (mut engine, items) = engine_with(UNSUPPORTED_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "nothing the translator cannot classify is folded in"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
}

#[test]
fn a_role_scoped_rule_keeps_query_only_matching() {
    let (mut engine, items) = engine_with(ROLE_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a rule no public caller may read is not folded in"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
}

#[test]
fn a_below_threshold_rule_keeps_query_only_matching() {
    let db = parse(DDL);
    let items = table_id(&db, "items");
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {})
        .with_translator(translator_at(ConfidenceLevel::A));
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a rule below the threshold is not folded in"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
}

#[test]
fn an_rls_table_with_no_read_policy_admits_nobody() {
    let (mut engine, items) = engine_with(NO_READ_POLICY_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the deny-by-default rule folds into the registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the closed read rule is in the registered predicate"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "row security with no permissive read admits nobody, even the owner"
    );
}

#[test]
fn a_write_only_policy_admits_nobody() {
    let (mut engine, items) = engine_with(WRITE_ONLY_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the deny-by-default rule folds into the registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a write-only policy grants no read and the closed rule folds in"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the write policy admits no read and the row reaches no caller"
    );
}

#[test]
fn a_restrictive_only_read_policy_admits_nobody() {
    let (mut engine, items) = engine_with(RESTRICTIVE_ONLY_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the closed rule folds into the registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a barrier with no permissive grant is in the registered predicate"
    );

    let titled = |id: i64, owner: &str, title: &str| {
        vec![
            Value::Int(id),
            Value::String(owner.into()),
            Value::Int(7),
            Value::String(title.into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(items, titled(1, "alice", "shared")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the barrier admits the row but no permissive grant admits it"
    );
}

#[test]
fn a_read_policy_on_an_rls_disabled_table_keeps_query_only_matching() {
    let (mut engine, items) = engine_with(NO_RLS_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the plain interest registers");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a policy on a table RLS does not guard folds nothing"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
}

#[test]
fn a_membership_rule_describes_its_seed_and_moves_its_admissions() {
    let db = parse(MEMBERSHIP_DDL);
    let docs = table_id(&db, "docs");
    let members = table_id(&db, "team_members");
    let engine: Engine =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());

    let described = engine
        .describe_terms(
            &SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into())),
        )
        .expect("the plain query describes the rule's term");
    let [TermDescription::Membership(membership)] = described.as_slice() else {
        panic!("one membership seed read, got {described:?}")
    };
    assert_eq!(membership.pairs[0].column, "team_id");
    assert_eq!(membership.member_table, "team_members");
    assert_eq!(membership.member_subject, "user_id");
    assert_eq!(membership.caller, TermCaller::Identity);
    assert_eq!(membership.subject_kind, ScalarFamily::String);

    let mut engine = engine;
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["team_id"], rows_of("alice", vec![Value::Int(7)])),
        )
        .expect("the seeded registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the membership rule is in the registered predicate"
    );

    let doc = |id: i64, team: i64| {
        vec![
            Value::Int(id),
            Value::Int(team),
            Value::String("title".into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[1], "the row is in a team alice holds");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 9)))
        .expect("dispatch runs");
    assert!(notifs.inserted().is_empty(), "team 9 is not alice's yet");

    let notifs = engine
        .consumers(&TestEvent::insert(
            members,
            vec![Value::Int(9), Value::String("alice".into())],
        ))
        .expect("dispatch runs");
    let [narrowing] = notifs.narrowings() else {
        panic!("the membership change narrows, got {notifs:?}")
    };
    assert_eq!(narrowing.subscription, registered.subscription_id);
    assert_eq!(narrowing.table, docs);
    assert_eq!(narrowing.columns, [1u16]);
    assert_eq!(narrowing.values, [Value::Int(9)]);
    assert!(narrowing.entered, "team 9 entered alice's set");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(3, 9)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the moved admission admits the row"
    );

    let notifs = engine
        .consumers(&TestEvent::delete(
            members,
            vec![Value::Int(9), Value::String("alice".into())],
        ))
        .expect("dispatch runs");
    let [narrowing] = notifs.narrowings() else {
        panic!("the withdrawal narrows, got {notifs:?}")
    };
    assert!(!narrowing.entered, "team 9 left alice's set");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(4, 9)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the withdrawn admission admits nobody"
    );
}

#[test]
fn the_batch_path_folds_each_registration() {
    let (mut engine, items) = engine_with(DDL);
    let results = engine.register_batch(vec![
        for_user(1, "alice", &["team:a"]),
        for_user(2, "bob", &[]),
    ]);
    for result in &results {
        assert!(
            result
                .as_ref()
                .expect("the batch registers")
                .served()
                .expect("an in-process answer")
                .read_rule_folded,
            "each batch registration carries the rule"
        );
    }

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[1], "the row names alice's identity");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "team:a", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the row names a subject only alice's session holds"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(3, "bob", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[2], "the row names bob's identity");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(4, "carol", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the row admits nobody registered"
    );
}

#[test]
fn unregistering_by_the_original_sql_removes_the_fold() {
    let (mut engine, items) = engine_with(DDL);
    engine
        .register(for_user(1, "alice", &["team:a"]))
        .expect("the rule folds into the registration");

    let report = engine
        .unregister_query(1, QUERY)
        .expect("the original SQL reaches the fold");
    assert_eq!(report.removed_bindings, 1, "the registration is removed");
    assert_eq!(engine.subscription_count(), 0, "nothing of it remains");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the removed registration receives nothing"
    );
}

#[test]
fn a_fold_comes_back_from_its_shard() {
    let store = TempStore::new();
    let db = parse(DDL);
    let items = table_id(&db, "items");
    let mut engine = stored_with(store.path(), db);
    engine
        .register(for_user(1, "alice", &["team:a"]))
        .expect("alice registers");
    engine
        .register(for_user(2, "bob", &[]))
        .expect("bob registers");
    engine
        .snapshot_table(items)
        .expect("the snapshot writes the shard");
    drop(engine);

    let mut restored = store.open(parse(DDL));
    assert_eq!(
        restored.subscription_count(),
        2,
        "both folds come back without a translator attached"
    );

    let notifs = restored
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the persisted identity admission survives the restore"
    );

    let notifs = restored
        .consumers(&TestEvent::insert(items, item(2, "team:a", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the persisted subject admission survives the restore"
    );

    let notifs = restored
        .consumers(&TestEvent::insert(items, item(3, "bob", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[2],
        "the other caller's audience comes back too"
    );

    let notifs = restored
        .consumers(&TestEvent::insert(items, item(4, "carol", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "a row no admission names reaches nobody"
    );
}

#[test]
fn unregistering_by_the_original_sql_after_a_restore() {
    let store = TempStore::new();
    let db = parse(DDL);
    let items = table_id(&db, "items");
    let mut engine = stored_with(store.path(), db);
    engine
        .register(for_user(1, "alice", &["team:a"]))
        .expect("alice registers");
    engine
        .register(for_user(2, "bob", &[]))
        .expect("bob registers");
    engine
        .snapshot_table(items)
        .expect("the snapshot writes the shard");
    drop(engine);

    let mut restored = store.open(parse(DDL));
    let notifs = restored
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the restored fold admits its audience before the removal"
    );

    let report = restored
        .unregister_query(1, QUERY)
        .expect("the original SQL reaches the restored fold");
    assert_eq!(
        report.removed_bindings, 1,
        "the restored registration is removed"
    );
    assert_eq!(restored.subscription_count(), 1, "the other caller stays");

    let notifs = restored
        .consumers(&TestEvent::insert(items, item(2, "alice", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the removed caller receives nothing"
    );

    let notifs = restored
        .consumers(&TestEvent::insert(items, item(3, "bob", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[2],
        "the caller that stays keeps its audience"
    );
}

#[test]
fn a_merged_shard_keeps_the_folded_audience() {
    let donor_store = TempStore::new();
    let db = parse(DDL);
    let items = table_id(&db, "items");
    let mut donor = stored_with(donor_store.path(), db);
    donor
        .register(for_user(1, "alice", &["team:a"]))
        .expect("alice registers");
    donor
        .snapshot_table(items)
        .expect("the snapshot writes the shard");
    let donor_shard = donor_store.path().join(format!("table_{items}.shard"));
    drop(donor);

    let live_store = TempStore::new();
    let mut live = live_store.open(parse(DDL));
    assert_eq!(
        live.subscription_count(),
        0,
        "the live partition starts empty"
    );
    let _ = live
        .merge_shards_background(items, &[donor_shard])
        .expect("the merge starts");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut reports = Vec::new();
    while Instant::now() < deadline {
        reports = live.complete_ready_merges().expect("the drain runs");
        if !reports.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(reports.len(), 1, "the merge is swapped in");
    assert_eq!(live.subscription_count(), 1, "the fold's answer is live");

    let notifs = live
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the merged shard carries the identity admission"
    );

    let notifs = live
        .consumers(&TestEvent::insert(items, item(2, "team:a", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the merged shard carries the subject admission"
    );

    let notifs = live
        .consumers(&TestEvent::insert(items, item(3, "bob", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the merged audience admits nobody else"
    );
}

#[test]
fn an_empty_subject_set_is_legal_sql_and_admits_nobody() {
    let (mut engine, items) = engine_with(DDL);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, SUBJECT_ONLY)
                .subscriber(Value::String("alice".into()))
                .subjects([]),
        )
        .expect("an empty subject set is legal SQL");
    assert_eq!(
        registered.tier.kind(),
        TierKind::InProcess,
        "the empty set is maintained in process"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the identity is a separate slot and the empty set admits nobody"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "bob", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "a subject nobody states admits no row"
    );
}

#[test]
fn restored_membership_watches_keep_current_grants_and_empty_claims() {
    let store = TempStore::new();
    let db = parse(MEMBERSHIP_DDL);
    let docs = table_id(&db, "docs");
    let members = table_id(&db, "team_members");
    let mut engine = stored_with(store.path(), db);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into()))
                .term_values(
                    vec!["team_id"],
                    rows_of("alice", vec![Value::Int(7), Value::Int(9)]),
                ),
        )
        .expect("membership registration folds");
    assert!(
        registered
            .served()
            .expect("in-process registration")
            .read_rule_folded
    );
    let membership = |team| vec![Value::Int(team), Value::String("alice".into())];
    let doc = |team| {
        vec![
            Value::Int(1),
            Value::Int(team),
            Value::String("title".into()),
        ]
    };
    let withdrawn = engine
        .consumers(&TestEvent::delete(members, membership(7)))
        .expect("membership withdrawal dispatches");
    assert_eq!(withdrawn.narrowings().len(), 1);
    assert!(!withdrawn.narrowings()[0].entered);
    engine.snapshot_table(docs).expect("current grants persist");
    drop(engine);

    let mut restored = store.open(parse(MEMBERSHIP_DDL));
    assert_eq!(
        restored
            .consumers(&TestEvent::insert(docs, doc(7)))
            .expect("withdrawn team dispatches")
            .inserted(),
        &[] as &[u64]
    );
    assert_eq!(
        restored
            .consumers(&TestEvent::insert(docs, doc(9)))
            .expect("retained team dispatches")
            .inserted(),
        &[1]
    );
    let withdrawn = restored
        .consumers(&TestEvent::delete(members, membership(9)))
        .expect("restored watch withdraws the final grant");
    assert_eq!(withdrawn.narrowings().len(), 1);
    assert_eq!(
        withdrawn.narrowings()[0].subscription,
        registered.subscription_id
    );
    assert!(!withdrawn.narrowings()[0].entered);
    assert_eq!(
        restored
            .consumers(&TestEvent::insert(docs, doc(9)))
            .expect("final withdrawal dispatches")
            .inserted(),
        &[] as &[u64]
    );
    restored
        .snapshot_table(docs)
        .expect("empty admissions persist");
    drop(restored);

    let mut restored = store.open(parse(MEMBERSHIP_DDL));
    assert_eq!(
        restored
            .consumers(&TestEvent::insert(docs, doc(9)))
            .expect("empty state dispatches")
            .inserted(),
        &[] as &[u64]
    );
    let granted = restored
        .consumers(&TestEvent::insert(members, membership(11)))
        .expect("restored empty claim receives a new grant");
    assert_eq!(granted.narrowings().len(), 1);
    assert_eq!(
        granted.narrowings()[0].subscription,
        registered.subscription_id
    );
    assert!(granted.narrowings()[0].entered);
    assert_eq!(
        restored
            .consumers(&TestEvent::insert(docs, doc(11)))
            .expect("new grant dispatches")
            .inserted(),
        &[1]
    );
}

#[test]
fn a_rule_requiring_an_unstated_identity_keeps_query_interest() {
    let (mut engine, items) = engine_with(DDL);
    for (consumer, sql) in [(1u64, QUERY), (2u64, SUBJECT_ONLY)] {
        let registered = engine
            .register(
                SubscriptionRequest::new(consumer, sql).subjects([Value::String("team:a".into())]),
            )
            .expect("unservable policy fold keeps the original interest");
        assert!(
            !registered
                .served()
                .expect("query is in process")
                .read_rule_folded
        );
    }
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
            .expect("identity row dispatches")
            .inserted(),
        &[1],
    );
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(items, item(2, "team:a", 7)))
            .expect("subject row dispatches")
            .inserted(),
        &[1, 2],
    );
}

#[test]
fn a_policy_without_a_using_clause_keeps_query_only_matching() {
    let (mut engine, items) = engine_with(NO_USING_DDL);
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a clause with no classification is not folded in"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the database grants the row to every caller and the interest names it"
    );
}

#[test]
fn an_unclassifiable_rule_refuses_even_at_the_lowest_confidence() {
    let db = parse(UNSUPPORTED_DDL);
    let items = table_id(&db, "items");
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {})
        .with_translator(translator_at(ConfidenceLevel::D));
    let registered = engine
        .register(for_user(1, "alice", &[]))
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "an unclassifiable rule is never folded in, even at the floor"
    );
    engine
        .register(for_user(2, "bob", &[]))
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
}

#[test]
fn a_parent_inheritance_rule_folds_its_parent_gate() {
    let db = parse(PARENT_DDL);
    let docs = table_id(&db, "docs");
    let projects = table_id(&db, "projects");
    let engine: Engine =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());

    let described = engine
        .describe_terms(
            &SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into())),
        )
        .expect("the plain query describes the rule's term");
    let [TermDescription::Membership(membership)] = described.as_slice() else {
        panic!("one membership seed read, got {described:?}")
    };
    assert_eq!(
        membership.pairs[0].column, "project_id",
        "the term compares the child's foreign key"
    );
    assert_eq!(
        membership.member_table, "projects",
        "the admission moves through the parent table"
    );
    assert_eq!(
        membership.member_subject, "owner",
        "the parent row names the subscriber through its owner"
    );
    assert_eq!(membership.caller, TermCaller::Identity);
    assert_eq!(membership.subject_kind, ScalarFamily::String);

    let mut engine = engine;
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["project_id"], rows_of("alice", vec![Value::Int(7)])),
        )
        .expect("the seeded registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "the parent inheritance rule is in the registered predicate"
    );

    let doc = |id: i64, project: i64| {
        vec![
            Value::Int(id),
            Value::Int(project),
            Value::String("title".into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the row sits under a project alice owns"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 9)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "project 9 is not behind alice's gate yet"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(
            projects,
            vec![Value::Int(9), Value::String("alice".into())],
        ))
        .expect("dispatch runs");
    let [narrowing] = notifs.narrowings() else {
        panic!("the parent change narrows, got {notifs:?}")
    };
    assert_eq!(narrowing.subscription, registered.subscription_id);
    assert_eq!(narrowing.table, docs);
    assert_eq!(narrowing.columns, [1u16]);
    assert_eq!(narrowing.values, [Value::Int(9)]);
    assert!(narrowing.entered, "project 9 entered alice's gate");

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(3, 9)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the moved admission admits the row"
    );
}

#[test]
fn a_row_secured_parent_keeps_the_child_query_only() {
    const INTEREST: &str = "SELECT * FROM docs WHERE project_id = $1";
    let db = parse(&format!(
        "{PARENT_DDL} ALTER TABLE projects ENABLE ROW LEVEL SECURITY;"
    ));
    let docs = table_id(&db, "docs");
    let mut engine: Engine =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());
    for (consumer, owner) in [(1u64, "alice"), (2, "bob")] {
        let registered = engine
            .register(
                SubscriptionRequest::new(consumer, INTEREST)
                    .subscriber(Value::String(owner.into()))
                    .binds(vec![Value::Int(7)]),
            )
            .expect("the original interest remains serveable");
        assert!(
            !registered
                .served()
                .expect("the query is served in process")
                .read_rule_folded
        );
    }
    let doc = |project| {
        vec![
            Value::Int(101),
            Value::Int(project),
            Value::String("title".into()),
        ]
    };
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(docs, doc(7)))
            .expect("the original interest admits its project")
            .inserted(),
        &[1, 2]
    );
    assert_eq!(
        engine
            .consumers(&TestEvent::insert(docs, doc(9)))
            .expect("the original interest rejects another project")
            .inserted(),
        &[] as &[u64]
    );
}

#[test]
fn a_function_rule_the_compiler_cannot_serve_keeps_query_only_matching() {
    const INTEREST: &str = "SELECT * FROM items WHERE project_id = $1";
    let (mut engine, items) = engine_with(FUNCTION_DDL);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, INTEREST)
                .subscriber(Value::String("alice".into()))
                .binds(vec![Value::Int(7)]),
        )
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a clause the indexed compiler cannot serve is not folded in"
    );
    engine
        .register(
            SubscriptionRequest::new(2u64, INTEREST)
                .subscriber(Value::String("bob".into()))
                .binds(vec![Value::Int(7)]),
        )
        .expect("the second caller registers");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "alice", 8)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the original bound interest excludes another project"
    );
}

#[test]
fn an_abac_rule_below_the_threshold_keeps_query_only_matching() {
    let db = parse(ABAC_DDL);
    let docs = table_id(&db, "docs");
    let mut engine: Engine =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, MEMBERS_QUERY).subscriber(Value::String("alice".into())),
        )
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a partial-grade rule is not folded in at the threshold"
    );
    engine
        .register(
            SubscriptionRequest::new(2u64, MEMBERS_QUERY).subscriber(Value::String("bob".into())),
        )
        .expect("the second caller registers");

    let doc = |id: i64, team: i64, status: &str| {
        vec![
            Value::Int(id),
            Value::Int(team),
            Value::String(status.into()),
        ]
    };
    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "archived")))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the refused rule guards nothing and the interest names the row"
    );
}

#[test]
fn a_partial_grade_abac_rule_folds_when_the_floor_allows_it() {
    let db = parse(ABAC_DDL);
    let docs = table_id(&db, "docs");
    let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {})
        .with_translator(translator_at(ConfidenceLevel::C));

    let described = engine
        .describe_terms(
            &SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into())),
        )
        .expect("the plain query describes the rule's term");
    let [TermDescription::Membership(membership)] = described.as_slice() else {
        panic!("one membership seed read, got {described:?}")
    };
    assert_eq!(membership.pairs[0].column, "team_id");
    assert_eq!(membership.member_table, "team_members");
    assert_eq!(membership.member_subject, "user_id");
    assert_eq!(membership.caller, TermCaller::Identity);

    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, MEMBERS_QUERY)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["team_id"], rows_of("alice", vec![Value::Int(7)])),
        )
        .expect("the seeded registration");
    assert!(
        registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a rule whose relationship half clears the floor folds in"
    );

    let doc = |id: i64, team: i64, status: &str| {
        vec![
            Value::Int(id),
            Value::Int(team),
            Value::String(status.into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(1, 7, "active")))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1],
        "the membership and the guard both hold"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(2, 7, "archived")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the attribute guard keeps its hold through the fold"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(docs, doc(3, 9, "active")))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the relationship half keeps its hold through the fold"
    );
}

#[test]
fn a_rule_over_a_column_the_planner_cannot_key_keeps_query_only_matching() {
    const INTEREST: &str = "SELECT * FROM items WHERE id = $1";
    let db = parse(FLOAT_RULE_DDL);
    let items = table_id(&db, "items");
    let translator = TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .with_session_attributes([SessionAttribute::setting(
            "app.score",
            SessionAttributeKind::ScalarAttribute,
        )])
        .build();
    let mut engine: Engine =
        SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator);
    let registered = engine
        .register(
            SubscriptionRequest::new(1u64, INTEREST)
                .subscriber(Value::String("alice".into()))
                .binds(vec![Value::Int(1)]),
        )
        .expect("the registration falls back to the query");
    assert!(
        !registered
            .served()
            .expect("an in-process answer")
            .read_rule_folded,
        "a rule the planner cannot key a lookup on is not folded in"
    );
    engine
        .register(
            SubscriptionRequest::new(2u64, INTEREST)
                .subscriber(Value::String("bob".into()))
                .binds(vec![Value::Int(1)]),
        )
        .expect("the second caller registers");

    let row = |id: i64, score: f64| vec![Value::Int(id), Value::Float(score)];
    let notifs = engine
        .consumers(&TestEvent::insert(items, row(1, 3.5)))
        .expect("dispatch runs");
    assert_eq!(
        notifs.inserted(),
        &[1, 2],
        "the query-only predicate admits the row to every caller"
    );
    let notifs = engine
        .consumers(&TestEvent::insert(items, row(2, 7.25)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the original bound interest excludes another primary key"
    );
}

#[test]
fn a_batch_reregistration_of_a_live_caller_keeps_its_binding() {
    let (mut engine, items) = engine_with(DDL);
    let first = engine
        .register(for_user(1, "alice", &[]))
        .expect("alice registers");
    let first_served = first.served().expect("an in-process answer");
    assert!(
        first_served.read_rule_folded,
        "the fold reports on the first registration"
    );

    let results = engine.register_batch(vec![for_user(1, "alice", &[]), for_user(2, "bob", &[])]);
    let repeat = results[0].as_ref().expect("the repeat registers");
    assert_eq!(
        repeat.subscription_id, first.subscription_id,
        "the repeat keeps the live subscription's identity"
    );
    let repeat_served = repeat.served().expect("an in-process answer");
    assert!(
        !repeat_served.created_new_predicate,
        "the repeat creates no second predicate"
    );
    assert!(
        repeat_served.read_rule_folded,
        "the repeat still reports the fold"
    );
    assert_eq!(
        repeat_served.predicate_hash, first_served.predicate_hash,
        "the repeat binds the same predicate"
    );

    let bob = results[1].as_ref().expect("bob registers");
    assert_ne!(
        bob.subscription_id, first.subscription_id,
        "bob is a new subscription"
    );
    assert!(
        !bob.served()
            .expect("an in-process answer")
            .created_new_predicate,
        "bob shares the folded predicate rather than duplicating it"
    );
    assert_eq!(
        engine.predicate_count(items),
        1,
        "one predicate serves both callers"
    );
    assert_eq!(
        engine.subscription_count(),
        2,
        "the repeat adds no subscription"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(1, "alice", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[1], "admission stays bound to alice");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(2, "bob", 7)))
        .expect("dispatch runs");
    assert_eq!(notifs.inserted(), &[2], "and to bob, not to anyone else");

    let notifs = engine
        .consumers(&TestEvent::insert(items, item(3, "carol", 7)))
        .expect("dispatch runs");
    assert!(
        notifs.inserted().is_empty(),
        "the row admits nobody registered"
    );
}
