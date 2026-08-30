//! The delegated half, against a real OpenFGA server.
//!
//! Everything else about row visibility is proven against a counting stand-in,
//! which is why the delegate went unwritten while every other criterion passed.
//! This is the criterion that was missing: a question the changed row does not
//! settle is answered by the authorization service, and the answer is right.
//!
//! The fixture is a tuple-to-userset, `can_select: member from teams`, which no
//! single row can ever decide. So the local half provably cannot answer it and
//! every verdict here came back over the wire.
//!
//! Run with:
//!   cargo +1.88 test --release --test it visibility_openfga_e2e:: \
//!       --features "visibility-openfga testing" -- --ignored --nocapture

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openfga_client::client::{
    ConsistencyPreference, CreateStoreRequest, ListStoresRequest, OpenFgaServiceClient,
    ReadRequest, ReadRequestTupleKey, TupleKey, WriteRequest, WriteRequestWrites,
};
use openfga_client::tonic::transport::Channel;
use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
use rls2fga::generator::well_known::member_relation;
use rls2fga::translator::TranslatorBuilder;
use rls2fga::types::ActionStatement;
use rls2fga::types::ConfidenceLevel;
use rls2fga::types::{Record, RecordContextValue};
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::visibility::openfga::OpenFgaPolicy;
use subql::visibility::policy::{RequestValues, RowPolicy, Subject};
use subql::visibility::shapes::Shapes;
use subql::visibility::{EventRow, Verdict, VisibilityPolicy};
use subql::{catalog_helpers, ParserDB};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Everything a policy over one schema needs, from one translation.
///
/// Every test here repeats this, and one of them outgrew the line limit doing
/// so. Sharing it also means each test reads one translation, which is what says
/// the recipes, the naming, the answers and the model cannot disagree.
struct Wiring {
    db: ParserDB,
    relations: Vec<rls2fga::types::RelationShapes>,
    naming: Vec<rls2fga::types::RowNaming>,
    notes: Vec<rls2fga::types::TranslationNote>,
    answers: Vec<rls2fga::types::ActionRelations>,
    model: rls2fga::generator::json_model::AuthorizationModel,
}

/// The request-scoped values are declared for every schema here, because
/// without them rls2fga refuses a held-keys arm rather than modelling it, and a
/// schema that has no such arm is unaffected by the declaration.
fn wiring(sql: &str) -> Wiring {
    let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
    let translator = TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .with_session_attributes([
            SessionAttribute::setting("app.user_id", SessionAttributeKind::CallerId),
            SessionAttribute::setting("app.subjects", SessionAttributeKind::SetAttribute),
        ])
        .build();
    let (relations, naming, notes, answers) = {
        let translation = translator
            .translate(&db)
            .expect("the visibility schema translates");
        (
            translation.relations().to_vec(),
            translation.row_naming(),
            translation.notes().to_vec(),
            translation.action_relations(),
        )
    };
    let model = translator
        .translate(&db)
        .expect("the visibility schema translates")
        .outputs_accepting_gaps()
        .json_model();
    Wiring {
        db,
        relations,
        naming,
        notes,
        answers,
        model,
    }
}

impl Wiring {
    /// The index every reader shares.
    fn shapes(self) -> Arc<Shapes<ParserDB>> {
        Arc::new(
            Shapes::new::<Postgres>(self.db, &self.relations)
                .with_row_naming(&self.naming)
                .with_action_relations(&self.answers)
                .with_required_parameters(&self.notes),
        )
    }
}

/// Write the model rls2fga emitted and return the id the server stored it under.
///
/// rls2fga's own writer, rather than a conversion spelled here: the model and the
/// request are different Rust types over one wire format, and the crate that
/// emits the model is where knowing that belongs.
async fn write_model(
    client: &mut OpenFgaServiceClient<Channel>,
    store_id: &str,
    model: &rls2fga::generator::json_model::AuthorizationModel,
) -> String {
    rls2fga::client::write_authorization_model(client, store_id, model)
        .await
        .expect("write the model")
}

/// `can_select: member from teams`, which one row never decides: whether a
/// watcher is a member of the team the row names is not in the row.
const SCHEMA: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          PRIMARY KEY(team_id, user_id));
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user));
";

/// Pinned rather than `latest`, and handed over only once the gRPC surface has
/// answered a call.
///
/// The line the service logs first is not readiness: it precedes binding the
/// gRPC listener, and a published port with nothing behind it yet resets the
/// connection rather than refusing it, so a client built on that line can fail
/// its first call. A call that succeeded is the only condition worth waiting on.
///
/// Started through the async runner, not the blocking one: the blocking runner
/// parks the thread it is called on, and this test is already being driven by a
/// runtime, so parking it deadlocks rather than waits.
async fn openfga() -> (ContainerAsync<GenericImage>, OpenFgaServiceClient<Channel>) {
    let container = GenericImage::new("openfga/openfga", "v1.8.13")
        .with_wait_for(WaitFor::message_on_stdout("starting openfga service"))
        // The gRPC port, which the image does not declare, so it has to be
        // mapped explicitly before it can be reached.
        .with_exposed_port(8081.tcp())
        .with_cmd(["run"])
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("start openfga");
    let port = container
        .get_host_port_ipv4(8081.tcp())
        .await
        .expect("grpc port");
    let client = serving_client(port).await;
    (container, client)
}

/// A client whose first call has already come back, or a panic carrying the
/// last refusal rather than an unexplained reset inside a test body.
async fn serving_client(port: u16) -> OpenFgaServiceClient<Channel> {
    let endpoint = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = "never attempted".to_owned();
    while Instant::now() < deadline {
        match OpenFgaServiceClient::connect(endpoint.clone()).await {
            Ok(mut client) => match client.list_stores(ListStoresRequest::default()).await {
                Ok(_) => return client,
                Err(status) => last = status.to_string(),
            },
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("openfga on {endpoint} never answered a call: {last}");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_question_the_row_does_not_settle_is_answered_by_the_service() {
    let (_container, mut client) = openfga().await;

    // What rls2fga makes of the schema: the model, the descriptions subql reads,
    // and which relations answer which statement. All from one translation, so
    // they cannot disagree.
    let wired = wiring(SCHEMA);
    let docs = catalog_helpers::table_id(&wired.db, "docs").expect("docs is in the catalog");
    let model = wired.model.clone();

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;

    let model_id = write_model(&mut client, &store, &model).await;

    // The facts the loader would have written: doc 4 belongs to team 1, and
    // alice is a member of it. bob is a member of nothing.
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: vec![
                    TupleKey {
                        user: "teams:1".to_owned(),
                        relation: "teams".to_owned(),
                        object: "docs:4".to_owned(),
                        condition: None,
                    },
                    TupleKey {
                        user: "user:alice".to_owned(),
                        relation: "member".to_owned(),
                        object: "teams:1".to_owned(),
                        condition: None,
                    },
                ],
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: model_id.clone(),
        })
        .await
        .expect("write the tuples");

    let shapes = wired.shapes();

    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model_id);
    let policy = RowPolicy::new(Arc::clone(&shapes), backend);

    assert!(
        !policy.answers_locally(docs, ActionStatement::Select),
        "a tuple-to-userset is never decidable from one row, so every answer \
         below had to come from the service"
    );

    let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(1)])
        .with_pk_columns([0u16]);
    let row = EventRow::current(&event, policy.catalog()).expect("an insert carries a post-image");

    let watchers = ["user:alice".to_owned(), "user:bob".to_owned()];
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    policy
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");

    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "alice is a member of the team the row names and bob is not"
    );
}

/// A batch larger than the configured cap is split, and every watcher still gets
/// its own answer in its own place.
///
/// The audience is deliberately larger than OpenFGA's own default limit of 50
/// questions per call, so the server itself refuses an unsplit batch. That is
/// what makes this a test of the splitting rather than of the answers: without
/// the chunking the call comes back rejected, not merely slower.
///
/// The cap is a setting because the server's limit is not discoverable through
/// any of its calls, so the two ends are configured to agree.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_batch_over_the_cap_is_split_and_stays_positional() {
    let (_container, mut client) = openfga().await;

    let wired = wiring(SCHEMA);
    let docs = catalog_helpers::table_id(&wired.db, "docs").expect("docs is in the catalog");
    let model = wired.model.clone();

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility-cap".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &model).await;

    // Doc 4 belongs to team 1, and every third watcher is a member of it.
    let audience: Vec<String> = (0..120).map(|i| format!("user:u{i}")).collect();
    let mut tuples = vec![TupleKey {
        user: "teams:1".to_owned(),
        relation: "teams".to_owned(),
        object: "docs:4".to_owned(),
        condition: None,
    }];
    tuples.extend(audience.iter().step_by(3).map(|name| TupleKey {
        user: name.clone(),
        relation: "member".to_owned(),
        object: "teams:1".to_owned(),
        condition: None,
    }));
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: tuples,
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: model_id.clone(),
        })
        .await
        .expect("write the tuples");

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model_id)
    .max_checks_per_batch(20);
    let policy = RowPolicy::new(Arc::clone(&shapes), backend);

    let event = TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::Int(1)])
        .with_pk_columns([0u16]);
    let row = EventRow::current(&event, policy.catalog()).expect("an insert carries a post-image");

    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, audience.len());
    policy
        .may_see(&row, &audience, &mut verdicts)
        .await
        .expect("the service answered every chunk");

    let expected: Vec<Verdict> = (0..audience.len())
        .map(|i| {
            if i % 3 == 0 {
                Verdict::Allow
            } else {
                Verdict::Deny
            }
        })
        .collect();
    assert_eq!(
        verdicts, expected,
        "120 questions at a cap of 20 is six calls, none of them over the \
         server's own limit, and each answer landed on the watcher it was \
         asked about"
    );
}

/// Requirement 6, closed against the service: the difference one changed row
/// reports is written through the client, and the next question answers from it.
///
/// This is the ordering the design put in subql rather than in the caller. Only
/// subql sees both the change stream and the questions, so only subql can write
/// before it answers. A store written by one component and read by another has
/// no ordering, and a question landing in the gap answers from facts that have
/// already moved.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_row_changing_hands_moves_the_store_and_the_next_answer() {
    // Ownership, which one row does decide, so the local half would answer it.
    // The point here is the store rather than the answering, so the questions go
    // to the service directly.
    const OWNED: &str = "
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, owner_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);
";

    let (_container, mut client) = openfga().await;

    let wired = wiring(OWNED);
    let docs = catalog_helpers::table_id(&wired.db, "docs").expect("docs is in the catalog");
    let model = wired.model.clone();

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility-store".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &model).await;

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model_id);

    // Row 4 is created owned by alice.
    let created =
        TestEvent::<Postgres>::insert(docs, vec![Value::Int(4), Value::String("alice".into())])
            .with_pk_columns([0u16]);
    backend
        .apply(&shapes.diff(&created).expect("an insert is all additions"))
        .await
        .expect("write the additions");

    let watchers = ["user:alice".to_owned(), "user:bob".to_owned()];
    let row = EventRow::current(&created, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "the store says alice owns it"
    );

    // It changes hands. The difference is one removal and one addition, and
    // writing the addition without the removal would leave alice holding it.
    let moved = TestEvent::<Postgres>::update(
        docs,
        vec![Value::Int(4), Value::String("alice".into())],
        vec![Value::Int(4), Value::String("bob".into())],
    )
    .with_pk_columns([0u16]);
    let diff = shapes.diff(&moved).expect("both images are complete");
    assert_eq!(diff.added.len(), 1, "one fact stated");
    assert_eq!(diff.removed.len(), 1, "one fact withdrawn");
    backend.apply(&diff).await.expect("write the difference");

    let row = EventRow::current(&moved, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Deny, Verdict::Allow],
        "and now the store says bob does, so alice lost it rather than both holding it"
    );
}

/// The two-table removal, closed against the service store: a membership only a
/// replay reaches is withdrawn, the replay returns the slice's remaining
/// truth, and reconciling it takes the stale fact out. This is the gap where a
/// caller following the old contract, replay and write back, was told nothing
/// was stale and kept granting for ever.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
#[allow(
    clippy::too_many_lines,
    reason = "one scenario walks grant, withdrawal, empty reconcile and refreshed reconcile \
              in order, and each step's report is asserted where it happens"
)]
async fn a_withdrawn_grant_only_a_replay_reaches_is_reconciled_out() {
    // Nothing keys (team, user) uniquely, so several rows can carry one
    // grant and the latest deadline wins, which no single row image can say:
    // the shape hands over a bound query instead of settling, and the
    // reconciliation is the only remover.
    const EXPIRING: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user
            AND team_members.expires_at > now()));
";

    let (_container, mut client) = openfga().await;

    let wired = wiring(EXPIRING);
    let members =
        catalog_helpers::table_id(&wired.db, "team_members").expect("members is in the catalog");
    let model = wired.model.clone();

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-reconcile-store".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &model).await;

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, String, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model_id);

    let withdrawn = TestEvent::<Postgres>::delete(
        members,
        vec![
            Value::Int(3),
            Value::String("alice".into()),
            Value::String("2027-01-01T00:00:00Z".into()),
        ],
    );
    let diff = shapes
        .diff(&withdrawn)
        .expect("the previous image is whole");
    let [requery] = diff.requeries.as_slice() else {
        panic!("one replay, one table the change arrived on: {diff:?}");
    };
    let condition = requery
        .query
        .condition
        .as_deref()
        .expect("the membership is conditional");
    backend
        .write_records(&[membership(
            "user:alice",
            condition,
            "2027-01-01T00:00:00+00:00",
        )])
        .await
        .expect("seed the membership");

    assert_eq!(
        stored_members(&mut client, &store).await,
        ["user:alice".to_owned()]
    );
    assert!(
        diff.removed.is_empty(),
        "the row alone cannot say what the slice still holds"
    );

    let emptied = backend
        .reconcile_records(requery, &[])
        .await
        .expect("reconcile the emptied slice");
    assert!(
        emptied.added.is_empty(),
        "an empty replay writes nothing: {emptied:?}"
    );
    let [withdrawn_fact] = emptied.removed.as_slice() else {
        panic!("one stale fact deleted, got {emptied:?}");
    };
    assert_eq!(withdrawn_fact.subject, "user:alice");
    assert_eq!(withdrawn_fact.object, "teams:3");
    assert_eq!(withdrawn_fact.relation, member_relation().to_string());

    assert_eq!(
        stored_members(&mut client, &store).await,
        Vec::<String>::new()
    );

    let refreshed = backend
        .reconcile_records(
            requery,
            &[membership(
                "user:bob",
                condition,
                "2027-01-01T00:00:00+00:00",
            )],
        )
        .await
        .expect("reconcile the replayed truth");
    assert_eq!(
        refreshed.added,
        vec![membership(
            "user:bob",
            condition,
            "2027-01-01T00:00:00+00:00",
        )],
        "the report names the fresh fact it wrote"
    );
    assert!(refreshed.removed.is_empty(), "nothing left to withdraw");

    assert_eq!(
        stored_members(&mut client, &store).await,
        ["user:bob".to_owned()]
    );
}

fn membership(subject: &str, condition: &str, expires_at: &str) -> Record {
    Record {
        object: "teams:3".to_owned(),
        relation: member_relation(),
        subject: subject.to_owned(),
        context: Some(RecordContextValue {
            condition: condition.to_owned(),
            values: BTreeMap::from([("expires_at".to_owned(), expires_at.to_owned())]),
        }),
    }
}

async fn stored_members(client: &mut OpenFgaServiceClient<Channel>, store: &str) -> Vec<String> {
    let response = client
        .read(ReadRequest {
            store_id: store.to_owned(),
            tuple_key: Some(ReadRequestTupleKey {
                user: String::new(),
                relation: member_relation().to_string(),
                object: "teams:3".to_owned(),
            }),
            page_size: None,
            continuation_token: String::new(),
            consistency: ConsistencyPreference::HigherConsistency as i32,
        })
        .await
        .expect("read memberships")
        .into_inner();
    response
        .tuples
        .into_iter()
        .filter_map(|tuple| tuple.key)
        .map(|key| key.user)
        .collect()
}

/// A watcher that names itself and says which keys its request carried.
///
/// The held-keys arm is a condition the server completes from the question, so
/// a watcher that cannot state its keys is refused rather than answered.
struct Principal {
    name: String,
    keys: Vec<String>,
}

impl Principal {
    fn new(name: &str, keys: &[&str]) -> Self {
        Self {
            name: name.to_owned(),
            keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        }
    }
}

impl Subject for Principal {
    fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
        core::iter::once(Cow::Borrowed(self.name.as_str()))
    }

    fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
        if parameter != "app_subjects" {
            return false;
        }
        for key in &self.keys {
            out.push(key);
        }
        true
    }
}

/// A change that moves only a conditional tuple's context is still applied.
///
/// This is the shape connetto writes on every table: an identity arm and a
/// held-keys arm in one policy. rls2fga models the held-keys arm as a condition
/// over the wildcard rather than as a subject, so every owner shares the subject
/// `user:*` and the owner lives in the condition's context. An owner change then
/// produces an addition and a removal carrying one tuple key, differing in a
/// field the server does not treat as part of the key, and it refuses a single
/// call holding both.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_change_of_only_a_conditional_context_is_applied() {
    const HELD_KEYS: &str = "
CREATE TABLE public.notes(id INTEGER PRIMARY KEY, owner TEXT NOT NULL);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes FOR ALL USING (
  owner = current_setting('app.user_id', true)
  OR owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
";

    let (_container, mut client) = openfga().await;

    let wired = wiring(HELD_KEYS);
    let notes = catalog_helpers::table_id(&wired.db, "notes").expect("notes is in the catalog");
    let model = wired.model.clone();

    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-visibility-store".to_owned(),
        })
        .await
        .expect("create store")
        .into_inner()
        .id;
    let model_id = write_model(&mut client, &store, &model).await;

    let shapes = wired.shapes();
    let backend = OpenFgaPolicy::<_, _, Principal, Postgres>::new(
        Arc::clone(&shapes),
        client.clone(),
        store.clone(),
    )
    .expect("the index carries what the questions need")
    .authorization_model_id(model_id);

    let created =
        TestEvent::<Postgres>::insert(notes, vec![Value::Int(1), Value::String("alice".into())])
            .with_pk_columns([0u16]);
    backend
        .apply(&shapes.diff(&created).expect("an insert is all additions"))
        .await
        .expect("write the additions");

    // Each watcher holds its own name as a key, which is how the held-keys arm
    // grants at all, and what puts the owner into the condition's context.
    let watchers = [
        Principal::new("user:alice", &["alice"]),
        Principal::new("user:carol", &["carol"]),
    ];
    let row = EventRow::current(&created, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Allow, Verdict::Deny],
        "the store says alice owns it"
    );

    let moved = TestEvent::<Postgres>::update(
        notes,
        vec![Value::Int(1), Value::String("alice".into())],
        vec![Value::Int(1), Value::String("carol".into())],
    )
    .with_pk_columns([0u16]);
    let diff = shapes.diff(&moved).expect("both images are complete");
    let conditional = |record: &rls2fga::types::Record| record.context.is_some();
    assert!(
        diff.added.iter().any(conditional) && diff.removed.iter().any(conditional),
        "the held-keys arm states a conditional fact on both sides, \
         which is what collides: {diff:?}"
    );
    backend
        .apply(&diff)
        .await
        .expect("the difference is written even though one key is on both sides");

    let row = EventRow::current(&moved, shapes.catalog()).expect("post-image");
    let mut verdicts = Vec::new();
    Verdict::reset(&mut verdicts, watchers.len());
    backend
        .may_see(&row, &watchers, &mut verdicts)
        .await
        .expect("the service answered");
    assert_eq!(
        verdicts,
        [Verdict::Deny, Verdict::Allow],
        "and the condition now carries carol rather than alice"
    );
}
