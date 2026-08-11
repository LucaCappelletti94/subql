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
//!   cargo +1.88 test --release --test visibility_openfga_e2e \
//!       --features "visibility-openfga testing" -- --ignored --nocapture

#![cfg(all(feature = "visibility-openfga", feature = "testing"))]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use openfga_client::client::{
    CreateStoreRequest, OpenFgaServiceClient, TupleKey, WriteAuthorizationModelRequest,
    WriteRequest, WriteRequestWrites,
};
use rls2fga::classifier::patterns::ConfidenceLevel;
use rls2fga::generator::action_relations::ActionStatement;
use rls2fga::translator::TranslatorBuilder;
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::visibility::openfga::OpenFgaPolicy;
use subql::visibility::policy::RowPolicy;
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
    relations: Vec<rls2fga::generator::relations::RelationShapes>,
    naming: Vec<rls2fga::generator::row_naming::RowNaming>,
    notes: Vec<rls2fga::generator::notes::TranslationNote>,
    answers: Vec<rls2fga::generator::action_relations::ActionRelations>,
    model: rls2fga::generator::json_model::AuthorizationModel,
}

fn wiring(sql: &str) -> Wiring {
    let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
    let translator = TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .build();
    let (relations, naming, notes, answers) = {
        let translation = translator.translate(&db);
        (
            translation.relations(),
            translation.row_naming(),
            translation.notes().to_vec(),
            translation.action_relations(),
        )
    };
    let model = translator
        .translate(&db)
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
            Shapes::new(self.db, &self.relations)
                .with_row_naming(&self.naming)
                .with_action_relations(&self.answers)
                .with_required_parameters(&self.notes)
                .with_condition_names(&self.model),
        )
    }
}

/// rls2fga's model as the client's protobuf types.
///
/// The two are different Rust types over one wire format, and JSON is the shape
/// both agree on, so this converts rather than re-deriving anything: a model
/// spelled twice would let the tuples subql writes and the rules the server
/// applies drift apart.
fn model_request(
    store_id: &str,
    model: &rls2fga::generator::json_model::AuthorizationModel,
) -> WriteAuthorizationModelRequest {
    let json = serde_json::to_value(model).expect("the model serializes");
    let mut request: WriteAuthorizationModelRequest =
        serde_json::from_value(json).expect("the client reads the same json");
    store_id.clone_into(&mut request.store_id);
    request
}

/// `can_select: member from teams`, which one row never decides: whether a
/// watcher is a member of the team the row names is not in the row.
const SCHEMA: &str = "
CREATE TABLE teams(id INTEGER PRIMARY KEY);
CREATE TABLE team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT,
                          PRIMARY KEY(team_id, user_id));
CREATE TABLE docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user));
";

/// Pinned rather than `latest`, and ready on the line the server logs once it
/// is listening.
///
/// Started through the async runner, not the blocking one: the blocking runner
/// parks the thread it is called on, and this test is already being driven by a
/// runtime, so parking it deadlocks rather than waits.
async fn openfga() -> ContainerAsync<GenericImage> {
    GenericImage::new("openfga/openfga", "v1.8.13")
        .with_wait_for(WaitFor::message_on_stdout("starting openfga service"))
        // The gRPC port, which the image does not declare, so it has to be
        // mapped explicitly before it can be reached.
        .with_exposed_port(8081.tcp())
        .with_cmd(["run"])
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("start openfga")
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_question_the_row_does_not_settle_is_answered_by_the_service() {
    let container = openfga().await;
    let port = container
        .get_host_port_ipv4(8081.tcp())
        .await
        .expect("grpc port");
    let mut client = OpenFgaServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("connect to openfga");

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

    let model_id = client
        .write_authorization_model(model_request(&store, &model))
        .await
        .expect("write the model")
        .into_inner()
        .authorization_model_id;

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
    let container = openfga().await;
    let port = container
        .get_host_port_ipv4(8081.tcp())
        .await
        .expect("grpc port");
    let mut client = OpenFgaServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("connect to openfga");

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
    let model_id = client
        .write_authorization_model(model_request(&store, &model))
        .await
        .expect("write the model")
        .into_inner()
        .authorization_model_id;

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
CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);
";

    let container = openfga().await;
    let port = container
        .get_host_port_ipv4(8081.tcp())
        .await
        .expect("grpc port");
    let mut client = OpenFgaServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("connect to openfga");

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
    let model_id = client
        .write_authorization_model(model_request(&store, &model))
        .await
        .expect("write the model")
        .into_inner()
        .authorization_model_id;

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
