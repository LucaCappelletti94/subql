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
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openfga_client::client::{
    ConsistencyPreference, CreateStoreRequest, ListStoresRequest, OpenFgaServiceClient,
    ReadRequest, ReadRequestTupleKey, TupleKey, WriteRequest, WriteRequestWrites,
};
use openfga_client::tonic::transport::Channel;
use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
use rls2fga::generator::well_known::member_relation;
use rls2fga::translator::{Outputs, TranslatorBuilder};
use rls2fga::types::ActionStatement;
use rls2fga::types::ConfidenceLevel;
use rls2fga::types::{Record, RecordContextValue};
use sqlparser::dialect::PostgreSqlDialect;
use subql::backend::{Postgres, Value};
use subql::testing::TestEvent;
use subql::visibility::openfga::{MaterialiseError, OpenFgaPolicy};
use subql::visibility::policy::{RequestValues, RowPolicy, Subject};
use subql::visibility::shapes::Shapes;
use subql::visibility::store::{Enumeration, Replay, Replayer, Requery};
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
    outputs: Outputs,
    model: rls2fga::generator::json_model::AuthorizationModel,
}

/// The request-scoped values are declared for every schema here, because
/// without them rls2fga refuses a held-keys arm rather than modelling it, and a
/// schema that has no such arm is unaffected by the declaration.
fn wiring(sql: &str) -> Wiring {
    let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
    let outputs = TranslatorBuilder::new()
        .with_min_confidence(ConfidenceLevel::B)
        .with_session_attributes([
            SessionAttribute::setting("app.user_id", SessionAttributeKind::CallerId),
            SessionAttribute::setting("app.subjects", SessionAttributeKind::SetAttribute),
        ])
        .build()
        .translate(&db)
        .expect("the visibility schema translates")
        .outputs_accepting_gaps();
    let model = outputs.json_model();
    Wiring { db, outputs, model }
}

impl Wiring {
    /// The index every reader shares.
    fn shapes(self) -> Arc<Shapes<ParserDB>> {
        let Self { db, outputs, .. } = self;
        let translation = outputs.translation();
        // A skipped query carries no description, so `filter_map` drops exactly
        // the entries that enumerate nothing.
        let enumerations: Vec<Enumeration<'_>> = outputs
            .tuple_queries()
            .iter()
            .filter_map(|query| {
                query.description.as_ref().map(|description| Enumeration {
                    description,
                    sql: &query.sql,
                    condition: query.condition.as_deref(),
                })
            })
            .collect();
        Arc::new(
            Shapes::new::<Postgres>(db, translation.relations(), &enumerations)
                .with_row_naming(translation.row_naming())
                .with_action_relations(translation.action_relations())
                .with_required_parameters(translation.notes()),
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
    let [Requery::Keyed(requery)] = diff.requeries.as_slice() else {
        panic!("one keyed replay, one table the change arrived on: {diff:?}");
    };
    let condition = requery
        .query
        .condition()
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

/// Two membership sources feeding one relation, one settled by the row and one
/// only a replay reaches. Their regions overlap, so they form one group.
const SHARED_REGION: &str = "
CREATE TABLE public.teams(id INTEGER PRIMARY KEY);
CREATE TABLE public.team_members(team_id INTEGER REFERENCES teams(id), user_id TEXT);
CREATE TABLE public.team_guests(team_id INTEGER REFERENCES teams(id), user_id TEXT,
    expires_at TIMESTAMPTZ);
CREATE TABLE public.docs(id INTEGER PRIMARY KEY, team_id INTEGER REFERENCES teams(id));
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs FOR SELECT USING (
  EXISTS (SELECT 1 FROM team_members
          WHERE team_members.team_id = docs.team_id AND team_members.user_id = current_user)
  OR EXISTS (SELECT 1 FROM team_guests
          WHERE team_guests.team_id = docs.team_id AND team_guests.user_id = current_user
            AND team_guests.expires_at > now()));
";

/// A membership whose residual compares one row against an aggregate over the
/// whole table. No key narrows it, so the shape carries the unnarrowed query
/// and one changed share can move a grant on any paper at all.
const WHOLE_SHAPE: &str = "
CREATE TABLE public.papers(id INTEGER PRIMARY KEY, owner TEXT);
CREATE TABLE public.paper_shares(paper_id INTEGER REFERENCES papers(id), viewer TEXT,
    weight NUMERIC, PRIMARY KEY(paper_id, viewer));
ALTER TABLE papers ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON papers FOR SELECT USING (
  EXISTS (SELECT 1 FROM paper_shares s
          WHERE s.paper_id = papers.id AND s.viewer = current_user
            AND s.weight > (SELECT avg(weight) FROM paper_shares)));
";

/// The caller's half of a group reconcile: run one member's query and hand
/// back its rows.
///
/// Canned rather than run against a database, because what is under test is
/// the reconciliation of the union, not the SQL. Each member is recognised by
/// the table its query reads, which is what distinguishes the two producers.
struct CannedReplay {
    rows: Vec<(&'static str, Vec<Record>)>,
}

impl Replayer for CannedReplay {
    type Error = core::convert::Infallible;

    fn replay(
        &self,
        member: &Replay,
    ) -> impl Future<Output = Result<Vec<Record>, Self::Error>> + Send {
        let matched = self
            .rows
            .iter()
            .find(|(table, _)| member.sql().contains(table))
            .unwrap_or_else(|| panic!("no canned rows for {}", member.sql()));
        core::future::ready(Ok(matched.1.clone()))
    }
}

fn plain_membership(object: &str, subject: &str) -> Record {
    Record {
        object: object.to_owned(),
        relation: member_relation(),
        subject: subject.to_owned(),
        context: None,
    }
}

/// Both producers' facts survive one reconcile of the region they share, which
/// the old code refused to attempt at all rather than delete one with the
/// other.
///
/// Deliberately not a pair of already-stored facts asserted to still be there,
/// which a materialiser that did nothing would also pass. One expected fact is
/// seeded and one is not, and a third is stored that no member returns, so the
/// report has to name one addition and one removal.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_group_keeps_every_members_facts_in_one_reconcile() {
    let (_container, mut client) = openfga().await;

    let wired = wiring(SHARED_REGION);
    let model = wired.model.clone();
    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-group-union".to_owned(),
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

    // carol is stored and still granted, dave is stored and granted by nobody,
    // and alice is granted by the settled producer but not yet stored.
    backend
        .write_records(&[
            plain_membership("teams:3", "user:carol"),
            plain_membership("teams:3", "user:dave"),
        ])
        .await
        .expect("seed the store");
    // Outside the region entirely: another relation on another type, which no
    // member states and this reconcile has no authority over. Written through
    // the raw client because a `Record` carries a relation name only rls2fga
    // can mint.
    client
        .write(WriteRequest {
            store_id: store.clone(),
            writes: Some(WriteRequestWrites {
                tuple_keys: vec![TupleKey {
                    user: "teams:1".to_owned(),
                    relation: "teams".to_owned(),
                    object: "docs:4".to_owned(),
                    condition: None,
                }],
                on_duplicate: String::new(),
            }),
            deletes: None,
            authorization_model_id: String::new(),
        })
        .await
        .expect("seed the out-of-region tuple");

    let replay = CannedReplay {
        rows: vec![
            (
                "team_members",
                vec![plain_membership("teams:3", "user:alice")],
            ),
            (
                "team_guests",
                vec![plain_membership("teams:3", "user:carol")],
            ),
        ],
    };

    let [group] = shapes.materialisations() else {
        panic!(
            "one group over the shared region: {:?}",
            shapes.materialisations()
        );
    };
    let reports = backend
        .materialise([group], &replay)
        .await
        .expect("reconcile the group");
    let [report] = reports.as_slice() else {
        panic!("one report per group: {reports:?}");
    };

    assert_eq!(
        report.added,
        [plain_membership("teams:3", "user:alice")],
        "the settled producer's fact was missing and is written"
    );
    let [withdrawn] = report.removed.as_slice() else {
        panic!("one stale fact, got {:?}", report.removed);
    };
    assert_eq!(withdrawn.subject, "user:dave");

    let mut stored = stored_members(&mut client, &store).await;
    stored.sort();
    assert_eq!(
        stored,
        ["user:alice".to_owned(), "user:carol".to_owned()],
        "both producers' facts stand, so neither member deleted the other's"
    );
    assert!(
        stored_relation(&mut client, &store, "teams", "docs:4")
            .await
            .contains(&"teams:1".to_owned()),
        "a fact outside the region is not this reconcile's to delete"
    );
}

/// Two members stating one fact under different conditions is refused, and
/// refused before anything is written.
///
/// A tuple carries one condition. Collapsing to whichever member came first
/// would hand somebody access on terms the other producer did not state, and
/// reading the last for the removal while writing the first would delete the
/// tuple and put nothing back.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn two_members_contradicting_one_fact_are_refused_before_any_write() {
    let (_container, mut client) = openfga().await;

    let wired = wiring(SHARED_REGION);
    let model = wired.model.clone();
    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-group-contradiction".to_owned(),
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

    backend
        .write_records(&[plain_membership("teams:3", "user:carol")])
        .await
        .expect("seed the store");

    // Both members name carol on team 3. One grants outright, the other only
    // under a condition, and the store can hold one of the two.
    let conditional = Record {
        context: Some(RecordContextValue {
            condition: "when_team_guests_expires_at".to_owned(),
            values: BTreeMap::from([(
                "expires_at".to_owned(),
                "2027-01-01T00:00:00+00:00".to_owned(),
            )]),
        }),
        ..plain_membership("teams:3", "user:carol")
    };
    let replay = CannedReplay {
        rows: vec![
            (
                "team_members",
                vec![plain_membership("teams:3", "user:carol")],
            ),
            ("team_guests", vec![conditional]),
        ],
    };

    let [group] = shapes.materialisations() else {
        panic!(
            "one group over the shared region: {:?}",
            shapes.materialisations()
        );
    };
    let refused = backend
        .materialise([group], &replay)
        .await
        .expect_err("a contradiction is refused");
    assert!(
        matches!(&refused, MaterialiseError::Contradiction(fact) if fact.contains("user:carol")),
        "named as a contradiction rather than as a store failure, which a \
         caller would retry for ever: {refused:?}"
    );

    assert_eq!(
        stored_members(&mut client, &store).await,
        ["user:carol".to_owned()],
        "and the store is untouched, so nothing was written before the refusal"
    );
}

/// The point of the whole change: a fact the replay stopped returning is
/// removed even for an object the event never named.
///
/// A whole-shape grant moves with rows the changed row does not name, so the
/// object whose grant is withdrawn need have nothing to do with the event. A
/// keyed replay cannot reach it, which is why attaching one would have left it
/// standing with nothing saying so.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires docker"]
async fn a_reconcile_removes_a_fact_for_an_object_the_event_never_named() {
    let (_container, mut client) = openfga().await;

    let wired = wiring(WHOLE_SHAPE);
    let share_table =
        catalog_helpers::table_id(&wired.db, "paper_shares").expect("shares are in the catalog");
    let model = wired.model.clone();
    let store = client
        .create_store(CreateStoreRequest {
            name: "subql-group-unnamed".to_owned(),
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

    backend
        .write_records(&[
            plain_membership("papers:1", "user:alice"),
            plain_membership("papers:2", "user:bob"),
        ])
        .await
        .expect("seed the store");

    // The event names paper 1 only. Paper 2 is not in it anywhere.
    let event = TestEvent::<Postgres>::delete(
        share_table,
        vec![
            Value::Int(1),
            Value::String("carol".into()),
            Value::String("7".into()),
        ],
    );
    let diff = shapes.diff(&event).expect("the previous image is whole");
    let [Requery::Whole(group)] = diff.requeries.as_slice() else {
        panic!("the change schedules its whole group: {diff:?}");
    };

    // One share leaving moved the average, so bob's own share fell below it and
    // the replay stops returning his grant on a paper the event never named.
    let replay = CannedReplay {
        rows: vec![(
            "paper_shares",
            vec![plain_membership("papers:1", "user:alice")],
        )],
    };

    let reports = backend
        .materialise([*group], &replay)
        .await
        .expect("reconcile the group");
    let [report] = reports.as_slice() else {
        panic!("one report per group: {reports:?}");
    };

    let [withdrawn] = report.removed.as_slice() else {
        panic!("one stale fact, got {:?}", report.removed);
    };
    assert_eq!(withdrawn.subject, "user:bob");
    assert_eq!(
        withdrawn.object, "papers:2",
        "the object the event never named is exactly the one that had to move"
    );
    assert!(
        report.added.is_empty(),
        "alice was already stored: {report:?}"
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
    stored_relation(client, store, &member_relation().to_string(), "teams:3").await
}

/// Every subject the store holds for one relation on one object.
async fn stored_relation(
    client: &mut OpenFgaServiceClient<Channel>,
    store: &str,
    relation: &str,
    object: &str,
) -> Vec<String> {
    let response = client
        .read(ReadRequest {
            store_id: store.to_owned(),
            tuple_key: Some(ReadRequestTupleKey {
                user: String::new(),
                relation: relation.to_owned(),
                object: object.to_owned(),
            }),
            page_size: None,
            continuation_token: String::new(),
            consistency: ConsistencyPreference::HigherConsistency as i32,
        })
        .await
        .expect("read the relation")
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
