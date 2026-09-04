//! A filter comparing a column to the caller directly, end to end: the
//! comparison compiles against the stated subscriber, so one predicate text
//! serves every subscriber and each changed row reaches exactly the one the
//! row names.
//!
//! The shape under test is `WHERE owner = current_setting('app.user_id', true)`,
//! the spelling Postgres row-level security resolves per connection. SubQL has
//! neither a connection nor a session, so the subscription states the identity
//! and the term seeds itself from it: no membership table, no seed read, and
//! nothing ever moves the set, because an identity does not change.
#![allow(clippy::unwrap_used)]

use rls2fga::translator::{Translator, TranslatorBuilder};
use rls2fga::types::ConfidenceLevel;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::{
    catalog_helpers, DefaultIds, RegisterError, SubscriptionEngine, SubscriptionRequest, TableId,
};

/// One-wide value rows, the shape the tuple-stating API takes for the
/// ordinary single-column term.
fn rows_of(values: Vec<Value<Postgres>>) -> Vec<Vec<Value<Postgres>>> {
    values.into_iter().map(|value| vec![value]).collect()
}

const DDL: &str = "CREATE TABLE projects(id INTEGER PRIMARY KEY, name TEXT);
     CREATE TABLE project_members(project_id INTEGER REFERENCES projects(id), user_id TEXT, PRIMARY KEY(project_id, user_id));
     CREATE TABLE notes(id INTEGER PRIMARY KEY, owner TEXT, project_id INTEGER, title TEXT);";

/// The caller comparison in the column-first spelling.
const CALLER: &str = "SELECT * FROM notes WHERE owner = current_setting('app.user_id', true)";

type Engine = SubscriptionEngine<TestEvent<Postgres>, DefaultIds, ParserDB>;

fn translator() -> Translator {
    TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build()
}

fn engine() -> (Engine, TableId) {
    let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
    let notes = catalog_helpers::table_id(&db, "notes").expect("notes is in the catalog");
    let engine = SubscriptionEngine::new(db, PostgreSqlDialect {}).with_translator(translator());
    (engine, notes)
}

/// One subscription for `consumer`, filtering for `user`.
fn subscribe(consumer: u64, user: &str) -> SubscriptionRequest<DefaultIds, Postgres> {
    SubscriptionRequest::new(consumer, CALLER).subscriber(Value::String(user.into()))
}

/// A `notes` row: `(id, owner, project_id, title)`.
fn note(id: i64, owner: &str, project: i64) -> Vec<Value<Postgres>> {
    vec![
        Value::Int(id),
        Value::String(owner.into()),
        Value::Int(project),
        Value::String("title".into()),
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

/// The reproduction from the finding that motivated the feature: the filter is
/// registered against the membership table itself, with the subscriber stated,
/// and a changed row reaches exactly the subscriber it names.
#[test]
fn the_finding_reproduction_registers_and_filters() {
    let (mut engine, _) = engine();
    let members =
        catalog_helpers::table_id(engine.database(), "project_members").expect("in the catalog");
    engine
        .register(
            SubscriptionRequest::new(
                1u64,
                "SELECT * FROM project_members WHERE user_id = current_setting('app.user_id', true)",
            )
            .subscriber(Value::String("alice".into())),
        )
        .unwrap();

    let row = |user: &str| vec![Value::Int(2), Value::String(user.into())];
    let notifs = engine
        .consumers(&TestEvent::insert(members, row("alice")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "the row names alice");

    let notifs = engine
        .consumers(&TestEvent::insert(members, row("bob")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the row names bob, and alice's subscription does not deliver it"
    );
}

/// The property the term route exists for: both spellings of one comparison
/// collapse to one compiled predicate, and each subscriber still receives only
/// the rows naming it.
#[test]
fn every_subscriber_shares_one_predicate() {
    let (mut engine, notes) = engine();
    engine.register(subscribe(1, "alice")).unwrap();
    engine.register(subscribe(2, "bob")).unwrap();
    engine
        .register(
            SubscriptionRequest::new(
                3u64,
                "SELECT * FROM notes WHERE current_setting('app.user_id', true) = owner",
            )
            .subscriber(Value::String("carol".into())),
        )
        .unwrap();

    assert_eq!(
        engine.predicate_count(notes),
        1,
        "the reversed spelling normalizes to the same predicate text"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(1, "alice", 7)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "the row names alice alone");

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(2, "carol", 7)))
        .unwrap();
    assert_eq!(
        notifs.inserted(),
        &[3],
        "the reversed spelling filters exactly as the column-first one"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(3, "dave", 7)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "a row naming nobody subscribed reaches nobody"
    );
}

/// An update that moves a row between owners is a delete for one subscriber and
/// an insert for the other, out of one evaluation of one shared predicate.
#[test]
fn an_update_moving_the_owner_reports_both_halves() {
    let (mut engine, notes) = engine();
    engine.register(subscribe(1, "alice")).unwrap();
    engine.register(subscribe(2, "bob")).unwrap();

    let moved = TestEvent::update(notes, note(1, "alice", 7), note(1, "bob", 7))
        .with_changed_columns([1u16]);
    let notifs = engine.consumers(&moved).unwrap();

    assert_eq!(notifs.inserted(), &[2], "the row moved to bob");
    assert_eq!(notifs.deleted(), &[1], "and away from alice, who is told");
    assert!(notifs.updated().is_empty(), "it changed for nobody else");
}

/// A delete reaches only the subscriber the deleted row named.
#[test]
fn a_deleted_row_reaches_only_its_owner() {
    let (mut engine, notes) = engine();
    engine.register(subscribe(1, "alice")).unwrap();
    engine.register(subscribe(2, "bob")).unwrap();

    let notifs = engine
        .consumers(&TestEvent::delete(notes, note(1, "alice", 7)))
        .unwrap();
    assert_eq!(notifs.deleted(), &[1]);
    assert!(notifs.inserted().is_empty(), "a delete fires no insertions");
}

/// A null compared value admits nobody, mirroring SQL: `NULL = caller` is
/// never true.
#[test]
fn a_null_owner_admits_nobody() {
    let (mut engine, notes) = engine();
    engine.register(subscribe(1, "alice")).unwrap();

    let row = vec![
        Value::Int(1),
        Value::Null,
        Value::Int(7),
        Value::String("title".into()),
    ];
    let notifs = engine.consumers(&TestEvent::insert(notes, row)).unwrap();
    assert!(notifs.inserted().is_empty(), "NULL names nobody");
}

/// A row test and the caller comparison compose: each can still refuse a row
/// the other admits.
#[test]
fn a_row_test_and_the_caller_comparison_both_have_to_hold() {
    let (mut engine, notes) = engine();
    let and_filter = "SELECT * FROM notes WHERE title = 'keep' \
         AND owner = current_setting('app.user_id', true)";
    engine
        .register(
            SubscriptionRequest::new(1u64, and_filter).subscriber(Value::String("alice".into())),
        )
        .unwrap();

    let with_title = |id: i64, owner: &str, title: &str| {
        vec![
            Value::Int(id),
            Value::String(owner.into()),
            Value::Int(7),
            Value::String(title.into()),
        ]
    };

    let notifs = engine
        .consumers(&TestEvent::insert(notes, with_title(1, "alice", "keep")))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "both halves hold");

    let notifs = engine
        .consumers(&TestEvent::insert(notes, with_title(2, "alice", "drop")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the comparison admits alice and the row test refuses the row"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(notes, with_title(3, "bob", "keep")))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the row test holds and the comparison names bob"
    );
}

/// A caller comparison and a membership subquery compose in one filter, each
/// keeping its own semantics.
#[test]
fn a_caller_comparison_composes_with_a_membership_subquery() {
    let (mut engine, notes) = engine();
    let mixed = "SELECT * FROM notes WHERE owner = current_setting('app.user_id', true) \
         AND project_id IN (SELECT project_id FROM project_members \
         WHERE user_id = current_setting('app.user_id', true))";
    engine
        .register(
            SubscriptionRequest::new(1u64, mixed)
                .subscriber(Value::String("alice".into()))
                .term_values(vec!["project_id"], rows_of(vec![Value::Int(7)])),
        )
        .unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(1, "alice", 7)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "owned by alice, in her project");

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(2, "alice", 9)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "owned by alice, but project 9 is not hers"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(3, "bob", 7)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "in her project, but owned by bob"
    );
}

/// Unregistering takes the identity out of the set: re-registering the same
/// consumer under another identity delivers the new identity's rows and not
/// the old one's.
#[test]
fn unregistering_takes_the_identity_out_of_the_set() {
    let (mut engine, notes) = engine();
    let registered = engine.register(subscribe(1, "alice")).unwrap();
    assert!(engine.unregister_subscription(registered.subscription_id));
    engine.register(subscribe(1, "bob")).unwrap();

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(1, "alice", 7)))
        .unwrap();
    assert!(
        notifs.inserted().is_empty(),
        "the consumer no longer claims alice"
    );

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(2, "bob", 7)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[1], "and now claims bob");
}

/// The batch path seeds the identity exactly as the single path does.
#[test]
fn the_batch_path_seeds_the_identity_as_the_single_path_does() {
    let (mut engine, notes) = engine();
    for outcome in engine.register_batch(vec![subscribe(1, "alice"), subscribe(2, "bob")]) {
        outcome.unwrap();
    }

    let notifs = engine
        .consumers(&TestEvent::insert(notes, note(1, "bob", 7)))
        .unwrap();
    assert_eq!(notifs.inserted(), &[2], "the batch-registered term filters");
}

mod refusals {
    use super::{engine, refusal, rows_of, subscribe, Engine, CALLER, DDL};
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::PostgreSqlDialect;
    use subql::backend::{Postgres, Value};
    use subql::{DefaultIds, SubscriptionEngine, SubscriptionRequest, Tier};

    /// The refusal the finding asked for by name: without a subscriber the
    /// registration is refused with a message naming the missing subscriber,
    /// rather than as unsupported SQL.
    #[test]
    fn without_a_subscriber_the_refusal_names_the_subscriber() {
        let (mut engine, _) = engine();
        let reason = refusal(&mut engine, SubscriptionRequest::new(1u64, CALLER));
        assert!(
            reason.contains("subscriber"),
            "the refusal names what is missing: {reason}"
        );
    }

    /// A setting key nothing declares as the caller is refused rather than read as
    /// the subscriber. `app.user_id` answers through the registry's defaults, and
    /// `app.tenant_id` is exactly the key those defaults do not name.
    #[test]
    fn an_undeclared_setting_key_is_refused() {
        let (mut engine, _) = engine();
        let reason = refusal(
            &mut engine,
            SubscriptionRequest::new(
                1u64,
                "SELECT * FROM notes WHERE owner = current_setting('app.tenant_id', true)",
            )
            .subscriber(Value::String("alice".into())),
        );
        assert!(!reason.is_empty(), "the refusal carries rls2fga's reason");
    }

    /// A subscriber of another kind than the compared column would match no row,
    /// ever, so it is refused at registration rather than served in silence.
    #[test]
    fn a_subscriber_of_the_wrong_kind_is_refused() {
        let (mut engine, _) = engine();
        let reason = refusal(
            &mut engine,
            SubscriptionRequest::new(1u64, CALLER).subscriber(Value::Int(7)),
        );
        assert!(
            reason.contains("Int") && reason.contains("String"),
            "the refusal names both kinds: {reason}"
        );
    }

    /// Values stated for the compared column would admit rows the filter's text
    /// never names, so they are refused: the comparison seeds itself.
    #[test]
    fn values_stated_for_the_caller_comparison_are_refused() {
        let (mut engine, _) = engine();
        let reason = refusal(
            &mut engine,
            subscribe(1, "alice")
                .term_values(vec!["owner"], rows_of(vec![Value::String("bob".into())])),
        );
        assert!(
            reason.contains("owner"),
            "the refusal names the column: {reason}"
        );
    }

    /// `NOT` over a caller comparison admits everybody but the caller, including
    /// rows the caller cannot see, so it is not served in process and the reason
    /// says so rather than the engine quietly serving different semantics.
    #[test]
    fn a_negated_caller_comparison_is_not_served_in_process() {
        let (mut engine, _) = engine();
        let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(
            1u64,
            "SELECT * FROM notes WHERE NOT (owner = current_setting('app.user_id', true))",
        )
        .subscriber(Value::String("alice".into()));
        let registered = engine
            .register(spec)
            .expect("a shape the evaluator refuses is re-read, not turned away");
        assert!(
            !matches!(registered.tier, Tier::InProcess(_)),
            "got {:?}",
            registered.tier
        );
        let reason = registered
            .not_served_because
            .map(|reason| reason.to_string())
            .expect("a read tier says why it is one");
        assert!(
            reason.contains("caller"),
            "the reason names the shape: {reason}"
        );
    }

    /// A function reading a column is not a caller comparison, so it too lands on
    /// a tier that re-reads rather than being maintained in process.
    #[test]
    fn a_function_of_a_column_is_not_served_in_process() {
        let (mut engine, _) = engine();
        let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(
            1u64,
            "SELECT * FROM notes WHERE owner = lower(title)",
        );
        let registered = engine.register(spec).expect("re-read, not refused");
        assert!(
            !matches!(registered.tier, Tier::InProcess(_)),
            "a column-reading function is outside the lifted shape, got {:?}",
            registered.tier
        );
    }

    /// Without translation settings the engine cannot tell whether the comparison
    /// names the caller, and says so.
    #[test]
    fn a_caller_comparison_is_refused_without_a_translator() {
        let db = ParserDB::parse::<PostgreSqlDialect>(DDL).expect("DDL parses");
        let mut engine: Engine = SubscriptionEngine::new(db, PostgreSqlDialect {});
        let reason = refusal(&mut engine, subscribe(1, "alice"));
        assert!(
            reason.contains("translator") || reason.contains("translation"),
            "the refusal names what is missing: {reason}"
        );
    }

    /// An aggregate cannot carry a caller comparison for the same reason it cannot
    /// carry a membership subquery: one shared accumulator cannot answer per
    /// subscriber.
    #[test]
    fn an_aggregate_with_a_caller_comparison_is_refused() {
        let (mut engine, _) = engine();
        let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(
            1u64,
            "SELECT COUNT(*) FROM notes WHERE owner = current_setting('app.user_id', true)",
        )
        .subscriber(Value::String("alice".into()));
        let reason = refusal(&mut engine, spec);
        assert!(
            reason.contains("aggregate"),
            "the refusal names the aggregate: {reason}"
        );
    }
}

mod describe_terms {
    use super::{engine, refusal, subscribe, CALLER};
    use subql::backend::{BuiltinKind, Postgres, Value};
    use subql::{DefaultIds, SubscriptionRequest};

    /// A caller comparison needs nothing seeded, but the subscriber has to be
    /// built at the compared column's kind, so the description says which.
    #[test]
    fn describe_terms_names_the_kind_for_a_caller_comparison() {
        let (describing, _) = engine();
        let described = describing
            .describe_terms(&SubscriptionRequest::new(1u64, CALLER))
            .unwrap();
        let [subql::term::TermDescription::Caller(caller)] = described.as_slice() else {
            panic!("one caller comparison, got {described:?}");
        };
        assert_eq!(caller.column, "owner", "the compared column");
        assert_eq!(
            caller.kind,
            BuiltinKind::String,
            "owner is TEXT, the kind the subscriber must be built at"
        );
        assert_eq!(caller.custom, None);

        // The description and registration agree: the named kind registers, and
        // another kind is refused with the message that already exists.
        let (mut registering, _) = engine();
        registering
            .register(subscribe(1, "alice"))
            .expect("a subscriber built at the described kind registers");
        let reason = refusal(
            &mut registering,
            SubscriptionRequest::new(2u64, CALLER).subscriber(Value::Int(7)),
        );
        assert!(
            reason.contains("kind"),
            "a subscriber built at another kind is refused: {reason}"
        );
    }

    /// A mixed filter describes both halves: the membership subquery with its
    /// seed read, and the caller comparison with the kind to build the
    /// subscriber at.
    #[test]
    fn describe_terms_names_both_halves_of_a_mixed_filter() {
        let (engine, _) = engine();
        let mixed = "SELECT * FROM notes WHERE owner = current_setting('app.user_id', true) \
         AND project_id IN (SELECT project_id FROM project_members \
         WHERE user_id = current_setting('app.user_id', true))";
        let described = engine
            .describe_terms(&SubscriptionRequest::new(1u64, mixed))
            .unwrap();
        assert_eq!(described.len(), 2, "one entry per term");
        let membership = described
            .iter()
            .find_map(|term| match term {
                subql::term::TermDescription::Membership(membership) => Some(membership),
                subql::term::TermDescription::Caller(_) => None,
            })
            .expect("the subquery is described with its seed read");
        assert_eq!(
            membership.pairs[0].column, "project_id",
            "the subquery's compared column"
        );
        let caller = described
            .iter()
            .find_map(|term| match term {
                subql::term::TermDescription::Caller(caller) => Some(caller),
                subql::term::TermDescription::Membership(_) => None,
            })
            .expect("the caller comparison is described with its kind");
        assert_eq!(caller.column, "owner");
        assert_eq!(caller.kind, BuiltinKind::String);
    }

    /// Describing and registering the same request refuse alike for the caller
    /// shapes, mirroring the membership-subquery parity assertion.
    #[test]
    fn describe_terms_refuses_the_caller_shapes_register_refuses() {
        for sql in [
            "SELECT * FROM notes WHERE owner = current_setting('app.tenant_id', true)",
            "SELECT COUNT(*) FROM notes WHERE owner = current_setting('app.user_id', true)",
        ] {
            let (mut engine, _) = engine();
            let spec = SubscriptionRequest::<DefaultIds, Postgres>::new(1u64, sql)
                .subscriber(Value::String("alice".into()));
            let described = engine.describe_terms(&spec);
            let registered = engine.register(spec);
            match (described, registered) {
                (Err(d), Err(r)) => assert_eq!(
                    format!("{d:?}"),
                    format!("{r:?}"),
                    "the two entry points refuse for the same stated reason"
                ),
                (d, r) => {
                    panic!("expected both to refuse for {sql}: describe {d:?}, register {r:?}")
                }
            }
        }
    }
}
