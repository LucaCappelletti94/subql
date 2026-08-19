//! The terminal answer: ask an OpenFGA server whatever the row does not settle.
//!
//! [`RowPolicy`](crate::visibility::policy::RowPolicy) answers from the changed
//! row wherever the schema decides the relation and delegates the rest. This is
//! what the rest goes to, so the composition terminates in an answer rather
//! than in another delegation.
//!
//! # One question per watcher, batched
//!
//! Every answer here is exact, so there is nothing to detect: a question is
//! asked about one watcher and one row and comes back allowed or not. They are
//! packed into `BatchCheck` calls of at most
//! [`max_checks_per_batch`](OpenFgaPolicy::max_checks_per_batch), which the
//! server also caps and does not report, so the two ends are configured to
//! agree and the default matches OpenFGA's own.
//!
//! Asking instead who may see the row, with one flat `ListUsers` call, was
//! rejected because it can be silently wrong: its result limit and its deadline
//! are server-side, and neither the request nor the reply carries a page token,
//! so a truncated answer is indistinguishable from a complete one and reads as
//! a wrong refusal.
//!
//! So calls per changed row are the watcher count over the batch size, which is
//! linear in the audience. Whether a shape bounded by the row instead exists was
//! asked and answered against OpenFGA rather than assumed, and it does, with a
//! constraint that rules it out here for now. `Expand` on the row names the
//! usersets granting the relation without enumerating their members, and it is
//! the one enumerating call that cannot be silently truncated. One paginated
//! `Read` per named userset then lists that userset's members with a real
//! continuation token, so a caller always knows it saw all of them, and the
//! watcher list is intersected locally. Calls are then bounded by how many
//! usersets the row grants to, which is a property of the row.
//!
//! The constraint is that `Read` returns only directly stored tuples and does
//! not evaluate the model, while the models this reads are generated from
//! row-level security policies and put conditions on membership. On such a model
//! `Read` omits members it cannot see it is omitting, which is the silent
//! wrongness the flat call was rejected for, arriving by another door.
//! Substituting `ListUsers` per userset evaluates the model correctly and brings
//! back the truncation. So the linear shape is what is correct today, and this
//! paragraph is here so the next reader inherits the finding rather than the
//! search.
//!
//! # Nothing here caches, and that is deliberate
//!
//! There is no cache and no seam for one. The server has its own check-result
//! cache, and which of the two paths a question takes is chosen per statement
//! rather than configured: a read may be served from that cache, and a write
//! asks authoritatively so it cannot be. A cache in front of this would be a
//! second staleness window, invalidated by nothing, sitting in front of a
//! deliberate decision about the first one. Tune the server's cache, and raise
//! [`read_consistency`](OpenFgaPolicy::read_consistency) when a deployment wants
//! reads authoritative too.
//!
//! # The caller's own values travel with the question
//!
//! A grant the caller's request completes is a condition the server evaluates
//! when asked, not a fact it stores, so a question that omits the value is
//! **refused** rather than answered no. Each question therefore carries what
//! its watcher sent, read out of the parameters rls2fga says are required.
//!
//! # A failure is never a refusal
//!
//! [`VisibilityPolicy::Error`] is failure to reach an answer, never an answer
//! of denied, so a transport failure is reported and nothing is invented. A
//! fabricated refusal would make clients delete rows they still hold.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::marker::PhantomData;
use std::collections::HashMap as StructFields;

use openfga_client::client::batch_check_single_result::CheckResult;
use openfga_client::client::{
    BatchCheckItem, BatchCheckRequest, CheckRequestTupleKey, ConsistencyPreference,
    ContextualTupleKeys, OpenFgaServiceClient, ReadRequest, ReadRequestTupleKey,
    RelationshipCondition, TupleKey, TupleKeyWithoutCondition, WriteRequest, WriteRequestDeletes,
    WriteRequestWrites,
};
use openfga_client::prost_wkt_types::{value::Kind, ListValue, Struct, Value as ProstValue};
use openfga_client::tonic::body::Body;
use openfga_client::tonic::client::GrpcService;
use openfga_client::tonic::codegen::{Bytes, StdError};
use openfga_client::tonic::Code;
use rls2fga::generator::records::{Record, RecordContextValue, ReplayScope};
use rls2fga::parser::identifiers::RelationName;
use sql_traits::prelude::DatabaseLike;

use crate::backend::Backend;
use crate::visibility::policy::{image_of, statement_of, table_of, RequestValues, Subject};
use crate::TableId;
use core::ops::Not;

use rls2fga::generator::action_relations::{ActionAnswer, ActionStatement, RowVersion};

use crate::visibility::records::render_text;
use crate::visibility::shapes::{RequiredParameter, Shapes, SharedShapes};
use crate::visibility::store::{Requery, StoreDiff};
use crate::visibility::{RowView, RowWrite, Verdict, VisibilityPolicy};

/// OpenFGA's own default for `MaxChecksPerBatchCheck`.
///
/// A constructor parameter rather than a constant because it cannot be
/// discovered: it is server configuration, and none of the service's twenty
/// calls reports the server's limits.
const DEFAULT_MAX_CHECKS_PER_BATCH: usize = 50;

/// How many times a transport failure is retried before giving up.
const DEFAULT_CONNECT_RETRIES: u32 = 2;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why no answer could be reached.
///
/// Every variant means exactly that. None of them is a denial, and a caller
/// that reads one as a denial withdraws rows its clients may still see.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OpenFgaError {
    /// The server could not be reached, after the configured retries.
    #[error("could not reach the authorization service after {attempts} attempts: {message}")]
    Transport {
        /// How many times it was tried.
        attempts: u32,
        /// What the transport reported last.
        message: String,
    },
    /// The server answered, and rejected the question.
    #[error("the authorization service rejected the question: {message}")]
    Rejected {
        /// What it said.
        message: String,
    },
    /// The reply left a question unanswered, so nothing can be said about it.
    #[error("the authorization service answered nothing for {questions} of the questions asked")]
    Incomplete {
        /// How many went unanswered.
        questions: usize,
    },
    /// The model names no object for the changed row's table, so the question
    /// cannot be put at all.
    ///
    /// Reached when the index was built without
    /// [`Shapes::with_row_naming`](crate::visibility::shapes::Shapes::with_row_naming),
    /// or for a table the model gives no type.
    #[error("the model names no object for rows of the changed table")]
    RowCannotBeNamed,
    /// The model says nothing this can ask about the statement, so no question
    /// can be put.
    ///
    /// Reached when the index was built without
    /// [`Shapes::with_action_relations`](crate::visibility::shapes::Shapes::with_action_relations),
    /// when the model names no type for the changed table, and when one relation
    /// fuses both row versions, which no single image answers.
    #[error("the model says nothing this can ask about a {statement:?} on the changed row")]
    StatementNotAnswered {
        /// The statement that went unanswered.
        statement: ActionStatement,
    },
    /// The replayed key cannot name the slice its result determines, so
    /// nothing can be reconciled against the store.
    ///
    /// Reached when a key value has no text form, when the encoded name is
    /// longer than the server accepts, and when a replayed record lies outside
    /// the declared slice, which means the declaration and the query disagree.
    #[error("the replayed key cannot name its slice: {message}")]
    SliceCannotBeNamed {
        /// What could not be named, and why.
        message: String,
    },
    /// A recipe grants through a comparison the caller's request completes and
    /// nothing said which parameters carry it, so every such question would be
    /// refused by the server.
    ///
    /// Reached when the index was built without
    /// [`Shapes::with_required_parameters`](crate::visibility::shapes::Shapes::with_required_parameters).
    #[error("the index carries a request-gated recipe and no required parameters were reported")]
    MissingRequiredParameters,
}

impl OpenFgaError {
    /// Whether `status` is worth trying again.
    ///
    /// Only a failure to reach the server is. A question it answered, or
    /// rejected, gives the same answer however often it is asked, and retrying
    /// it holds the change stream for nothing.
    const fn is_transport(code: Code) -> bool {
        matches!(
            code,
            Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
        )
    }
}

// ---------------------------------------------------------------------------
// OpenFgaPolicy
// ---------------------------------------------------------------------------

/// Answers by asking an OpenFGA server.
///
/// `T` is the transport the caller built, so TLS, bearer tokens and
/// interceptors stay its business: it hands over whatever
/// [`OpenFgaServiceClient`] it assembled.
#[derive(Debug, Clone)]
pub struct OpenFgaPolicy<DB, T, W, B> {
    shapes: SharedShapes<DB>,
    client: OpenFgaServiceClient<T>,
    store_id: String,
    authorization_model_id: String,
    max_checks_per_batch: usize,
    connect_retries: u32,
    read_consistency: ConsistencyPreference,
    watcher: PhantomData<fn(W)>,
    backend: PhantomData<fn(B)>,
}

impl<DB, T, W, B> OpenFgaPolicy<DB, T, W, B>
where
    DB: DatabaseLike,
{
    /// Ask `client` about the store `store_id`, reading `shapes` for what the
    /// questions have to name.
    ///
    /// The index is shared with the wrapper in front of this one, so both read
    /// one catalog and one set of descriptions.
    ///
    /// # Errors
    ///
    /// [`OpenFgaError::MissingRequiredParameters`] when a recipe grants through
    /// a comparison the caller's request completes and the index carries no
    /// required parameters. That combination means every such question would be
    /// refused by the server, and refusing to construct says so once rather
    /// than once per event.
    ///
    /// The check exists because the parameters arrive in a report whose type is
    /// `#[non_exhaustive]`, so a variant renamed upstream would be read as "no
    /// parameters required" rather than failing to compile.
    pub fn new(
        shapes: SharedShapes<DB>,
        client: OpenFgaServiceClient<T>,
        store_id: impl Into<String>,
    ) -> Result<Self, OpenFgaError> {
        usable_index(&shapes)?;
        Ok(Self {
            shapes,
            client,
            store_id: store_id.into(),
            authorization_model_id: String::new(),
            max_checks_per_batch: DEFAULT_MAX_CHECKS_PER_BATCH,
            connect_retries: DEFAULT_CONNECT_RETRIES,
            read_consistency: ConsistencyPreference::MinimizeLatency,
            watcher: PhantomData,
            backend: PhantomData,
        })
    }

    /// Pin the authorization model to ask against. Unset asks the store's
    /// latest, which is what the server does with an empty id.
    #[must_use]
    pub fn authorization_model_id(mut self, id: impl Into<String>) -> Self {
        self.authorization_model_id = id.into();
        self
    }

    /// How many questions travel in one call, defaulting to OpenFGA's own
    /// `MaxChecksPerBatchCheck`.
    ///
    /// Set it to match the server: it caps this too, reports the limit through
    /// no call, and rejects a batch over it.
    #[must_use]
    pub const fn max_checks_per_batch(mut self, checks: usize) -> Self {
        self.max_checks_per_batch = if checks == 0 { 1 } else { checks };
        self
    }

    /// Ask read questions on `preference` rather than the cheap path.
    ///
    /// Only the read path is settable. A write always asks authoritatively,
    /// because a write accepted against a permission already withdrawn is not
    /// undone afterwards, so offering to lower it would offer to reintroduce
    /// exactly that.
    #[must_use]
    pub const fn read_consistency(mut self, preference: ConsistencyPreference) -> Self {
        self.read_consistency = preference;
        self
    }

    /// How many times a transport failure is retried.
    ///
    /// Bounded on purpose. A retry runs while the change stream is held, so it
    /// is backpressure on every subscriber including those answered from the
    /// row, and an unbounded one stalls delivery for everybody.
    #[must_use]
    pub const fn connect_retries(mut self, retries: u32) -> Self {
        self.connect_retries = retries;
        self
    }

    /// The index, shared with the wrapper in front of this one.
    #[must_use]
    pub const fn shapes(&self) -> &SharedShapes<DB> {
        &self.shapes
    }

    /// Name `row` as the model names it.
    fn object_of<R>(&self, row: &R) -> Result<String, OpenFgaError>
    where
        R: RowView + ?Sized,
    {
        let naming = self
            .shapes
            .naming(row.table_id())
            .ok_or(OpenFgaError::RowCannotBeNamed)?;
        let values = crate::visibility::records::row_values(row, self.shapes.catalog());
        naming
            .key
            .render(&naming.type_name, &values)
            .ok()
            .flatten()
            .ok_or(OpenFgaError::RowCannotBeNamed)
    }
}

/// Whether `shapes` carries what the questions will need.
///
/// A recipe granting through a comparison the caller's request completes needs
/// the parameters that comparison reads, and a question missing one is refused
/// by the server rather than answered. Saying so once at construction beats
/// discovering it per event, and it is the guard against the report's type being
/// `#[non_exhaustive]`: a variant renamed upstream reads as "none required"
/// rather than failing to compile.
fn usable_index<DB: DatabaseLike>(shapes: &Shapes<DB>) -> Result<(), OpenFgaError> {
    if shapes.has_request_gated_recipe() && shapes.required_parameters().is_empty() {
        return Err(OpenFgaError::MissingRequiredParameters);
    }
    Ok(())
}

/// The context one watcher's question carries: every required parameter it can
/// answer, rendered as the condition expects.
///
/// A parameter no watcher supplies is left out, and so is one this watcher
/// cannot answer. Leaving it out is what makes the server refuse the question
/// rather than answer it wrongly, which is the right direction: the alternative
/// is inventing a value the caller never sent.
fn context_for<W: Subject + ?Sized>(
    required: &[RequiredParameter],
    watcher: &W,
    values: &mut RequestValues,
) -> Option<Struct> {
    let mut fields = StructFields::new();
    for parameter in required {
        if !parameter.watcher_supplied() {
            continue;
        }
        values.reset();
        if !watcher.request_value(&parameter.parameter, values) {
            continue;
        }
        let value = if parameter.list {
            ProstValue {
                kind: Some(Kind::ListValue(ListValue {
                    values: values
                        .values()
                        .map(|held| ProstValue {
                            kind: Some(Kind::StringValue(held.to_string())),
                        })
                        .collect(),
                })),
            }
        } else {
            // One value, and a watcher that sent several has not answered a
            // parameter declared to hold one. Picking the first would put a
            // value the policy never compared against into the context.
            let mut held = values.values();
            let (Some(one), None) = (held.next(), held.next()) else {
                continue;
            };
            ProstValue {
                kind: Some(Kind::StringValue(one.to_string())),
            }
        };
        fields.insert(parameter.parameter.clone(), value);
    }
    if fields.is_empty() {
        None
    } else {
        Some(Struct { fields })
    }
}

/// One question, and which watcher it belongs to.
struct Question {
    /// Where the answer goes.
    place: usize,
    item: BatchCheckItem,
}

/// What answering a statement takes, where the model states a refusal instead of
/// naming anything to ask.
///
/// A refusal is an answer, and the cheapest one there is, so it travels as a
/// state rather than as [`OpenFgaError::StatementNotAnswered`], which means no
/// answer was reached and tells a caller that fails closed to hold the event and
/// try again.
#[derive(Debug, PartialEq, Eq)]
enum Asked<T> {
    /// The model grants nobody, so nobody is asked.
    Refused,
    /// What has to be asked.
    Ask(T),
}

/// Which consistency preference a question about `statement` needs, where `read`
/// is what the caller chose for reads.
///
/// The server may answer a check from its own cache unless the question says
/// otherwise, and `ConsistencyPreference::Unspecified` is documented as
/// behaving like the cheap path, so leaving it unset asks for the cached answer
/// rather than for no preference.
///
/// A write therefore asks authoritatively: it is authorised once, and a write
/// accepted against a permission the cache has not caught up to is not undone
/// later. A read may take the cheap path, because the change path asks again on
/// the next event, so a stale answer costs freshness rather than disclosure.
///
/// The wildcard is the safe direction rather than laziness: [`ActionStatement`]
/// is `#[non_exhaustive]`, and a statement added upstream is answered
/// authoritatively until someone decides it is a read.
const fn consistency_for(
    statement: ActionStatement,
    read: ConsistencyPreference,
) -> ConsistencyPreference {
    match statement {
        ActionStatement::Select => read,
        _ => ConsistencyPreference::HigherConsistency,
    }
}

/// One call's worth of questions, asked at `consistency`.
fn batch_request(
    store_id: &str,
    authorization_model_id: &str,
    chunk: &[Question],
    consistency: ConsistencyPreference,
) -> BatchCheckRequest {
    BatchCheckRequest {
        store_id: store_id.to_string(),
        checks: chunk.iter().map(|question| question.item.clone()).collect(),
        authorization_model_id: authorization_model_id.to_string(),
        // Discriminant extraction: the generated field is a bare `i32`.
        consistency: consistency as i32,
    }
}

impl<DB, T, W, B> OpenFgaPolicy<DB, T, W, B>
where
    DB: DatabaseLike + Send + Sync,
    T: GrpcService<Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError>,
    T::ResponseBody: openfga_client::tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as openfga_client::tonic::codegen::Body>::Error: Into<StdError> + Send,
    T::Future: Send,
    W: Subject + Send + Sync,
    B: Backend,
{
    /// Ask `questions` at `consistency`, in calls of at most the configured cap,
    /// writing each answer where its question says.
    async fn ask(
        &self,
        questions: Vec<Question>,
        verdicts: &mut [Verdict],
        consistency: ConsistencyPreference,
    ) -> Result<(), OpenFgaError> {
        for chunk in questions.chunks(self.max_checks_per_batch) {
            let request = batch_request(
                &self.store_id,
                &self.authorization_model_id,
                chunk,
                consistency,
            );
            let answers = self.batch_check(request).await?;

            let mut unanswered = 0;
            for question in chunk {
                let Some(result) = answers.get(&question.item.correlation_id) else {
                    unanswered += 1;
                    continue;
                };
                if *result {
                    if let Some(verdict) = verdicts.get_mut(question.place) {
                        *verdict = Verdict::Allow;
                    }
                }
            }
            if unanswered > 0 {
                return Err(OpenFgaError::Incomplete {
                    questions: unanswered,
                });
            }
        }
        Ok(())
    }

    /// One call, retried while the failure is the transport's.
    async fn batch_check(
        &self,
        request: BatchCheckRequest,
    ) -> Result<BTreeMap<String, bool>, OpenFgaError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let mut client = self.client.clone();
            match client.batch_check(request.clone()).await {
                Ok(response) => {
                    let mut out = BTreeMap::new();
                    for (correlation, result) in response.into_inner().result {
                        // An error on one question is not an answer to it, so
                        // it is left out and reported as unanswered rather than
                        // read as a refusal.
                        let Some(CheckResult::Allowed(allowed)) = result.check_result else {
                            continue;
                        };
                        out.insert(correlation, allowed);
                    }
                    return Ok(out);
                }
                Err(status) if OpenFgaError::is_transport(status.code()) => {
                    if attempts > self.connect_retries {
                        return Err(OpenFgaError::Transport {
                            attempts,
                            message: status.message().to_string(),
                        });
                    }
                }
                Err(status) => {
                    return Err(OpenFgaError::Rejected {
                        message: status.message().to_string(),
                    })
                }
            }
        }
    }

    /// Every question one relation on one row raises for `watchers`.
    fn questions<R>(
        &self,
        row: &R,
        relation: &str,
        watchers: &[W],
        contextual: &[TupleKey],
    ) -> Result<Vec<Question>, OpenFgaError>
    where
        R: RowView + ?Sized,
    {
        let object = self.object_of(row)?;
        let mut values = RequestValues::new();
        let mut questions = Vec::new();
        for (place, watcher) in watchers.iter().enumerate() {
            let context = context_for(self.shapes.required_parameters(), watcher, &mut values);
            // A watcher may authenticate as several names, and any of them
            // granting grants the watcher, so each is its own question.
            for (nth, name) in watcher.subjects().enumerate() {
                questions.push(Question {
                    place,
                    item: BatchCheckItem {
                        tuple_key: Some(CheckRequestTupleKey {
                            user: name.into_owned(),
                            relation: relation.to_string(),
                            object: object.clone(),
                        }),
                        contextual_tuples: (!contextual.is_empty()).then(|| ContextualTupleKeys {
                            tuple_keys: contextual.to_vec(),
                        }),
                        context: context.clone(),
                        correlation_id: format!("w{place}n{nth}"),
                    },
                });
            }
        }
        Ok(questions)
    }

    /// The questions `write` raises, one group per rule that has to grant, or
    /// [`Asked::Refused`] where the model grants nobody and nothing is worth
    /// asking.
    ///
    /// A replacement is two rules across two versions. The one choosing which
    /// rows may be touched reads the stored row, and the one admitting the
    /// result reads a row the store has never seen, so that group carries the
    /// facts the new row implies.
    fn write_plan<R>(
        &self,
        write: RowWrite<'_, R>,
        watcher: &W,
    ) -> Result<Asked<Vec<Vec<Question>>>, OpenFgaError>
    where
        R: RowView + ?Sized,
    {
        let statement = statement_of(&write);
        let table = table_of(&write);
        let answer = self
            .shapes
            .answer(table, statement)
            .ok_or(OpenFgaError::StatementNotAnswered { statement })?;
        let judges = match answer {
            // The database restricts nothing here, so there is nothing to ask
            // and no group has to grant.
            ActionAnswer::Unrestricted => return Ok(Asked::Ask(Vec::new())),
            // The model grants nobody, so the write is refused without asking.
            ActionAnswer::Denied => return Ok(Asked::Refused),
            ActionAnswer::Judged(judges) if judges.is_empty().not() => judges,
            // One relation fusing both versions cannot be asked against either
            // image: judging the check clause on the row as it is grants a
            // change that clause was written to refuse.
            _ => return Err(OpenFgaError::StatementNotAnswered { statement }),
        };

        let only = core::slice::from_ref(watcher);
        let mut plan = Vec::with_capacity(judges.len());
        for judge in judges {
            let row = image_of(&write, judge.version)
                .ok_or(OpenFgaError::StatementNotAnswered { statement })?;
            // A row the store has never seen carries the facts it implies, so the
            // rule admitting it has something to read. The stored row needs none.
            let contextual = match judge.version {
                RowVersion::Resulting => self.contextual_of(row),
                _ => Vec::new(),
            };
            plan.push(self.questions(row, judge.relation.as_str(), only, &contextual)?);
        }
        Ok(Asked::Ask(plan))
    }

    /// The relation that answers a read of `table`, as the model reports it, or
    /// [`Asked::Refused`] where the model grants nobody.
    ///
    /// A read judges the row as it is, so its answer is one relation against one
    /// version. Anything else, including a table the model names no type for,
    /// leaves the question unputtable rather than guessed at.
    ///
    /// # Errors
    ///
    /// [`OpenFgaError::StatementNotAnswered`] when the model says nothing this
    /// can ask. A refusal is not one of those cases: it is an answer, and it
    /// comes back as [`Asked::Refused`].
    fn read_relation(&self, table: TableId) -> Result<Asked<RelationName>, OpenFgaError> {
        let statement = ActionStatement::Select;
        let unanswered = || OpenFgaError::StatementNotAnswered { statement };
        match self
            .shapes
            .answer(table, statement)
            .ok_or_else(unanswered)?
        {
            // The model grants nobody, so the read is refused without asking.
            ActionAnswer::Denied => Ok(Asked::Refused),
            ActionAnswer::Judged(judges) => match judges.as_slice() {
                [judge] if judge.version == RowVersion::Existing => {
                    Ok(Asked::Ask(judge.relation.clone()))
                }
                _ => Err(unanswered()),
            },
            // An unrestricted table has nothing to ask, and this path exists to
            // ask, so the caller answers it locally or not at all.
            _ => Err(unanswered()),
        }
    }

    /// The facts the row being written implies, as tuples the question carries.
    ///
    /// A row that does not exist yet has nothing stored about it, so the rule
    /// admitting it can only be evaluated against facts supplied with the
    /// question.
    fn contextual_of<R>(&self, row: &R) -> Vec<TupleKey>
    where
        R: RowView + ?Sized,
    {
        let Some(shapes) = self.shapes.table_records(row.table_id()) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for shape in shapes {
            let Ok(records) = crate::visibility::records::records_from_row_view(
                shape,
                row,
                self.shapes.catalog(),
            ) else {
                continue;
            };
            for record in records {
                let condition = record.context.as_ref().map(condition_of);
                out.push(TupleKey {
                    user: record.subject,
                    relation: record.relation.to_string(),
                    object: record.object,
                    condition,
                });
            }
        }
        out
    }
}

/// OpenFGA's own default limit on tuples per write.
///
/// Server configuration again, and again not reported by any call, so the
/// default matches the server's and a difference larger than one write is split.
const MAX_TUPLES_PER_WRITE: usize = 100;

/// Whether a difference's two halves can travel in one call.
///
/// Two conditions, and the second is not a size. The server's tuple key is the
/// subject, the relation and the object, and a conditional tuple's context is
/// not part of it, so a change that moves only a context states one key on both
/// sides. A call carrying a key twice is refused outright, whatever the sizes,
/// and the held-key arm rls2fga models as a condition over the wildcard reaches
/// this on the most ordinary event there is: an owner change, where every owner
/// shares the subject `user:*`.
///
/// Scanning is quadratic and guarded by the size test, so it runs only over
/// lists already known to be small, and it allocates nothing.
fn fits_one_call(writes: &[TupleKey], deletes: &[TupleKeyWithoutCondition]) -> bool {
    writes.len() + deletes.len() <= MAX_TUPLES_PER_WRITE
        && !deletes.iter().any(|delete| {
            writes.iter().any(|write| {
                write.user == delete.user
                    && write.relation == delete.relation
                    && write.object == delete.object
            })
        })
}

impl<DB, T, W, B> OpenFgaPolicy<DB, T, W, B>
where
    DB: DatabaseLike + Send + Sync,
    T: GrpcService<Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError>,
    T::ResponseBody: openfga_client::tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as openfga_client::tonic::codegen::Body>::Error: Into<StdError> + Send,
    T::Future: Send,
    W: Subject + Send + Sync,
    B: Backend,
{
    /// Write what one changed row moved.
    ///
    /// Only subql sees both the change stream and the questions, so only subql
    /// can order a write against the question that reads it. A store written by
    /// one component and read by another has no ordering between them, and a
    /// question landing in the gap answers from facts that have already moved.
    ///
    /// A difference that fits one call is applied atomically, so a row changing
    /// hands never has a moment where both subjects hold it. One too large for a
    /// single call, or one stating a key on both sides, cannot be, and then
    /// removals go first, which is the fail-closed order.
    ///
    /// [`StoreDiff::requeries`] is **not** covered here. Those are the facts no
    /// single row settles, and running their SQL belongs to the caller, which
    /// then hands the rows back through
    /// [`reconcile_records`](Self::reconcile_records). A caller that ignores
    /// them leaves every two-table fact stale.
    ///
    /// # Errors
    ///
    /// [`OpenFgaError::Transport`] when the server could not be reached, and
    /// [`OpenFgaError::Rejected`] when it refused the write.
    pub async fn apply(&self, diff: &StoreDiff<'_, B>) -> Result<(), OpenFgaError> {
        let mut writes = Vec::with_capacity(diff.added.len());
        for record in &diff.added {
            writes.push(tuple_of(record));
        }
        let deletes: Vec<TupleKeyWithoutCondition> = diff
            .removed
            .iter()
            .map(|record| TupleKeyWithoutCondition {
                user: record.subject.clone(),
                relation: record.relation.clone().to_string(),
                object: record.object.clone(),
            })
            .collect();
        self.write_difference(writes, deletes).await
    }

    /// Replace what the store holds for the slice `requery` determines with
    /// `records`, which is what replaying it returned.
    ///
    /// The replay's result is the whole truth for its slice, so a stored fact
    /// in the slice the result no longer states is stale and is deleted, which
    /// is what [`write_records`](Self::write_records) can never do. This is
    /// where a caller puts what replaying a
    /// [`Requery`](crate::visibility::store::Requery) returned, due before the
    /// event is delivered, exactly as the store module's contract says.
    ///
    /// The read asks authoritatively whatever the configured read preference
    /// says, because it reads this policy's own writes: a replica missing the
    /// newest one would resurrect what was just deleted.
    ///
    /// # Errors
    ///
    /// As [`apply`](Self::apply), and
    /// [`OpenFgaError::SliceCannotBeNamed`] when the replayed key cannot name
    /// the slice, or a record lies outside it, which means the query and its
    /// declaration disagree and nothing here can say which one is right.
    pub async fn reconcile_records(
        &self,
        requery: &Requery<'_, B>,
        records: &[Record],
    ) -> Result<(), OpenFgaError> {
        let scope = &requery.query.scope;
        let mut rendered = Vec::with_capacity(requery.key.len());
        for value in &requery.key {
            let Some(text) = render_text(value) else {
                return Err(OpenFgaError::SliceCannotBeNamed {
                    message: "a bound key value has no text form".to_string(),
                });
            };
            rendered.push(text);
        }
        let slice = scope
            .rendered_key(&rendered.iter().map(String::as_str).collect::<Vec<_>>())
            .map_err(|error| OpenFgaError::SliceCannotBeNamed {
                message: error.to_string(),
            })?;
        for record in records {
            if !in_slice(scope, &slice, record) {
                return Err(OpenFgaError::SliceCannotBeNamed {
                    message: format!(
                        "the replayed record ({}, {}, {}) lies outside the slice {slice}",
                        record.subject, record.relation, record.object
                    ),
                });
            }
        }

        let desired: BTreeMap<Triple, Option<RelationshipCondition>> = records
            .iter()
            .map(|record| (triple_of(record), record.context.as_ref().map(condition_of)))
            .collect();
        let mut stored: BTreeMap<Triple, Option<RelationshipCondition>> = BTreeMap::new();
        for tuple in self.read_slice(scope, &slice).await? {
            // An object slice reads every relation on the object, and the ones
            // outside the declared set are other shapes' facts.
            if let ReplayScope::Object { relations, .. } = scope {
                if !relations.iter().any(|known| *known == tuple.relation) {
                    continue;
                }
            }
            stored.insert(
                (
                    tuple.user.clone(),
                    tuple.relation.clone(),
                    tuple.object.clone(),
                ),
                tuple.condition,
            );
        }

        // A context that moved is a delete and a write of the same key, which
        // `write_difference` already refuses to send as one call and orders
        // removals first.
        let deletes: Vec<TupleKeyWithoutCondition> = stored
            .iter()
            .filter(|(key, condition)| desired.get(*key) != Some(condition))
            .map(|((user, relation, object), _)| TupleKeyWithoutCondition {
                user: user.clone(),
                relation: relation.clone(),
                object: object.clone(),
            })
            .collect();
        let writes: Vec<TupleKey> = desired
            .into_iter()
            .filter(|(key, condition)| stored.get(key) != Some(condition))
            .map(|((user, relation, object), condition)| TupleKey {
                user,
                relation,
                object,
                condition,
            })
            .collect();
        self.write_difference(writes, deletes).await
    }

    /// Every tuple the store holds in `slice`, read page by page.
    async fn read_slice(
        &self,
        scope: &ReplayScope,
        slice: &str,
    ) -> Result<Vec<TupleKey>, OpenFgaError> {
        let filter = match scope {
            ReplayScope::Object { .. } => ReadRequestTupleKey {
                user: String::new(),
                relation: String::new(),
                object: slice.to_string(),
            },
            ReplayScope::Subject {
                relation,
                object_type,
                ..
            } => ReadRequestTupleKey {
                user: slice.to_string(),
                relation: relation.as_str().to_string(),
                object: format!("{object_type}:"),
            },
        };
        let mut out = Vec::new();
        let mut continuation_token = String::new();
        loop {
            let request = ReadRequest {
                store_id: self.store_id.clone(),
                tuple_key: Some(filter.clone()),
                page_size: None,
                continuation_token: continuation_token.clone(),
                // Discriminant extraction: the generated field is a bare `i32`.
                // Authoritative regardless of the configured read preference,
                // since this read precedes a write built from it.
                consistency: ConsistencyPreference::HigherConsistency as i32,
            };
            let mut attempts = 0;
            let response = loop {
                attempts += 1;
                let mut client = self.client.clone();
                match client.read(request.clone()).await {
                    Ok(response) => break response.into_inner(),
                    Err(status) if OpenFgaError::is_transport(status.code()) => {
                        if attempts > self.connect_retries {
                            return Err(OpenFgaError::Transport {
                                attempts,
                                message: status.message().to_string(),
                            });
                        }
                    }
                    Err(status) => {
                        return Err(OpenFgaError::Rejected {
                            message: status.message().to_string(),
                        })
                    }
                }
            };
            out.extend(response.tuples.into_iter().filter_map(|tuple| tuple.key));
            if response.continuation_token.is_empty() {
                return Ok(out);
            }
            continuation_token = response.continuation_token;
        }
    }

    /// Apply `writes` and `deletes` as [`apply`](Self::apply) documents:
    /// atomically when the server accepts one call, and removals first
    /// otherwise, which is the fail-closed order.
    async fn write_difference(
        &self,
        writes: Vec<TupleKey>,
        deletes: Vec<TupleKeyWithoutCondition>,
    ) -> Result<(), OpenFgaError> {
        // One call whenever the server accepts one, because it applies a write
        // atomically: a row changing hands then has no moment where both the old
        // and the new subject hold it, and none where neither does.
        if fits_one_call(&writes, &deletes) {
            return self
                .write(
                    (!writes.is_empty()).then(|| WriteRequestWrites {
                        tuple_keys: writes,
                        on_duplicate: String::new(),
                    }),
                    (!deletes.is_empty()).then(|| WriteRequestDeletes {
                        tuple_keys: deletes,
                        on_missing: String::new(),
                    }),
                )
                .await;
        }

        // Not sendable as one call, so the atomicity is gone and the order is
        // what is left to choose. Removals go first: between the calls the row
        // then reaches nobody rather than everybody, and a client told too late
        // that it may see a row loses nothing it was entitled to. Splitting only
        // the colliding key and keeping the rest atomic was rejected for that
        // reason: it leaves the subject losing the row holding it through its
        // other, uncollided facts for the length of the gap.
        for chunk in deletes.chunks(MAX_TUPLES_PER_WRITE) {
            self.write(
                None,
                Some(WriteRequestDeletes {
                    tuple_keys: chunk.to_vec(),
                    on_missing: String::new(),
                }),
            )
            .await?;
        }
        for chunk in writes.chunks(MAX_TUPLES_PER_WRITE) {
            self.write(
                Some(WriteRequestWrites {
                    tuple_keys: chunk.to_vec(),
                    on_duplicate: String::new(),
                }),
                None,
            )
            .await?;
        }
        Ok(())
    }

    /// Write `records` as tuples, splitting them across calls the server accepts.
    ///
    /// This is where the initial load's rows go: it only ever writes, so what a
    /// replayed [`Requery`](crate::visibility::store::Requery) returned goes
    /// through [`reconcile_records`](Self::reconcile_records) instead, which
    /// also takes out what the replay stopped returning.
    ///
    /// # Errors
    ///
    /// As [`apply`](Self::apply).
    pub async fn write_records(&self, records: &[Record]) -> Result<(), OpenFgaError> {
        for chunk in records.chunks(MAX_TUPLES_PER_WRITE) {
            let mut tuple_keys = Vec::with_capacity(chunk.len());
            for record in chunk {
                tuple_keys.push(tuple_of(record));
            }
            self.write(
                Some(WriteRequestWrites {
                    tuple_keys,
                    on_duplicate: String::new(),
                }),
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn write(
        &self,
        writes: Option<WriteRequestWrites>,
        deletes: Option<WriteRequestDeletes>,
    ) -> Result<(), OpenFgaError> {
        if writes.as_ref().is_none_or(|w| w.tuple_keys.is_empty())
            && deletes.as_ref().is_none_or(|d| d.tuple_keys.is_empty())
        {
            return Ok(());
        }
        let request = WriteRequest {
            store_id: self.store_id.clone(),
            writes,
            deletes,
            authorization_model_id: self.authorization_model_id.clone(),
        };
        let mut attempts = 0;
        loop {
            attempts += 1;
            let mut client = self.client.clone();
            match client.write(request.clone()).await {
                Ok(_) => return Ok(()),
                Err(status) if OpenFgaError::is_transport(status.code()) => {
                    if attempts > self.connect_retries {
                        return Err(OpenFgaError::Transport {
                            attempts,
                            message: status.message().to_string(),
                        });
                    }
                }
                Err(status) => {
                    return Err(OpenFgaError::Rejected {
                        message: status.message().to_string(),
                    })
                }
            }
        }
    }
}

/// One tuple's identity to the server: subject, relation, object. A condition's
/// context is not part of it, so a context that moved compares as the same key
/// with a different value.
type Triple = (String, String, String);

/// The key `record` states, in the server's own terms.
fn triple_of(record: &Record) -> Triple {
    (
        record.subject.clone(),
        record.relation.clone().to_string(),
        record.object.clone(),
    )
}

/// Whether `record` lies in the slice `scope` names as `slice`.
fn in_slice(scope: &ReplayScope, slice: &str, record: &Record) -> bool {
    match scope {
        ReplayScope::Object { relations, .. } => {
            record.object == slice && relations.contains(&record.relation)
        }
        ReplayScope::Subject {
            relation,
            object_type,
            ..
        } => {
            record.subject == slice
                && record.relation == *relation
                && record
                    .object
                    .strip_prefix(object_type.as_str())
                    .is_some_and(|rest| rest.starts_with(':'))
        }
    }
}

/// One record as a tuple, carrying its condition when it has one.
///
/// Free rather than a method, and infallible, because the condition name travels
/// on the record. Looking it up by relation, which is what this did before, was a
/// second copy of something rls2fga already states.
fn tuple_of(record: &Record) -> TupleKey {
    TupleKey {
        user: record.subject.clone(),
        relation: record.relation.clone().to_string(),
        object: record.object.clone(),
        condition: record.context.as_ref().map(condition_of),
    }
}

/// The condition a record's context asks the server to complete.
fn condition_of(context: &RecordContextValue) -> RelationshipCondition {
    RelationshipCondition {
        name: context.condition.clone(),
        context: Some(Struct {
            fields: context
                .values
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        ProstValue {
                            kind: Some(Kind::StringValue(value.clone())),
                        },
                    )
                })
                .collect(),
        }),
    }
}

impl<DB, T, W, B> VisibilityPolicy for OpenFgaPolicy<DB, T, W, B>
where
    DB: DatabaseLike + Send + Sync,
    T: GrpcService<Body> + Clone + Send + Sync + 'static,
    T::Error: Into<StdError>,
    T::ResponseBody: openfga_client::tonic::codegen::Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as openfga_client::tonic::codegen::Body>::Error: Into<StdError> + Send,
    T::Future: Send,
    W: Subject + Send + Sync,
    B: Backend,
{
    type Watcher = W;
    type Error = OpenFgaError;
    type Backend = B;

    fn may_see<R>(
        &self,
        row: &R,
        watchers: &[Self::Watcher],
        verdicts: &mut [Verdict],
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized,
    {
        let questions = match self.read_relation(row.table_id()) {
            // Nobody is granted, and the caller pre-filled a denial for every
            // watcher, so there is nothing to ask and nothing to write.
            Ok(Asked::Refused) => Ok(Vec::new()),
            Ok(Asked::Ask(relation)) => self.questions(row, relation.as_str(), watchers, &[]),
            Err(err) => Err(err),
        };
        let consistency = consistency_for(ActionStatement::Select, self.read_consistency);
        async move { self.ask(questions?, verdicts, consistency).await }
    }

    fn may_write<R>(
        &self,
        write: RowWrite<'_, R>,
        watcher: &Self::Watcher,
    ) -> impl core::future::Future<Output = Result<Verdict, Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized,
    {
        let statement = statement_of(&write);
        let consistency = consistency_for(statement, self.read_consistency);
        let plan = self.write_plan(write, watcher);
        async move {
            let groups = match plan? {
                Asked::Refused => return Ok(Verdict::Deny),
                Asked::Ask(groups) => groups,
            };
            // Every half must grant, so the first refusal is the answer and the
            // rest are never asked.
            for questions in groups {
                let mut verdict = [Verdict::Deny];
                self.ask(questions, &mut verdict, consistency).await?;
                if verdict[0] == Verdict::Deny {
                    return Ok(Verdict::Deny);
                }
            }
            Ok(Verdict::Allow)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::borrow::Cow;
    use alloc::collections::BTreeMap;
    use alloc::vec;

    use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
    use rls2fga::classifier::patterns::ConfidenceLevel;
    use rls2fga::translator::TranslatorBuilder;
    use sqlparser::dialect::PostgreSqlDialect;

    use super::{
        batch_request, consistency_for, context_for, fits_one_call, tuple_of, usable_index,
        ActionStatement, Asked, BatchCheckItem, CheckRequestTupleKey, Code, ConsistencyPreference,
        Kind, OpenFgaError, OpenFgaPolicy, OpenFgaServiceClient, Question, Record,
        RecordContextValue, RequestValues, RequiredParameter, RowWrite, Subject,
        TupleKeyWithoutCondition, MAX_TUPLES_PER_WRITE,
    };
    use crate::backend::{Postgres, Value};
    use crate::testing::TestEvent;
    use crate::visibility::shapes::Shapes;
    use crate::visibility::{test_names, EventRow, Verdict, VisibilityPolicy};
    use crate::{catalog_helpers, ParserDB};
    use alloc::string::String;
    use alloc::sync::Arc;
    use core::task::{Context as CoreContext, Poll};
    use openfga_client::tonic::client::GrpcService;
    use openfga_client::tonic::codegen::http::{Request, Response};
    use openfga_client::tonic::{body::Body, Status};

    /// The four statements a write can be, which is what `statement_of` reports
    /// for the four [`RowWrite`](crate::visibility::RowWrite) shapes.
    const WRITE_STATEMENTS: [ActionStatement; 4] = [
        ActionStatement::Insert,
        ActionStatement::Update,
        ActionStatement::SelectForUpdate,
        ActionStatement::Delete,
    ];

    /// One question, enough to read the request built around it.
    fn a_question() -> Question {
        Question {
            place: 0,
            item: BatchCheckItem {
                tuple_key: Some(CheckRequestTupleKey {
                    user: "user:one".to_string(),
                    relation: "can_select".to_string(),
                    object: "docs:1".to_string(),
                }),
                contextual_tuples: None,
                context: None,
                correlation_id: "w0n0".to_string(),
            },
        }
    }

    /// A write is authorised once and never revisited, so it asks the
    /// authoritative path even where the caller chose the cheap one for reads.
    /// Answering a write from the server's cache accepts it against a permission
    /// that may already be gone.
    #[test]
    fn a_write_asks_authoritatively() {
        for statement in WRITE_STATEMENTS {
            assert_eq!(
                consistency_for(statement, ConsistencyPreference::MinimizeLatency),
                ConsistencyPreference::HigherConsistency,
                "{statement:?} is a write and must not be answered from cache"
            );
        }
    }

    /// The condition a tuple names comes off the record, which is the only thing
    /// that knows it. Looking it up by relation, as this used to, is a second
    /// copy of something rls2fga already states, and two copies can disagree.
    #[test]
    fn a_tuple_names_the_condition_its_record_carries() {
        let record = Record {
            object: "docs:1".to_string(),
            relation: test_names::relation("owner"),
            subject: "user:*".to_string(),
            context: Some(RecordContextValue {
                condition: "when_row_owner".to_string(),
                values: BTreeMap::from([("row_owner".to_string(), "alice".to_string())]),
            }),
        };

        let condition = tuple_of(&record)
            .condition
            .expect("a record carrying a context yields a conditional tuple");
        assert_eq!(condition.name, "when_row_owner");
        let fields = condition
            .context
            .expect("the condition carries its context");
        assert_eq!(
            fields
                .fields
                .get("row_owner")
                .and_then(|value| match &value.kind {
                    Some(Kind::StringValue(text)) => Some(text.as_str()),
                    _ => None,
                }),
            Some("alice")
        );
    }

    /// An unconditional record yields an unconditional tuple. A condition
    /// invented for it would be refused by the server for naming nothing the
    /// model declares.
    #[test]
    fn a_record_without_a_context_yields_an_unconditional_tuple() {
        let record = Record {
            object: "docs:1".to_string(),
            relation: test_names::relation("owner"),
            subject: "user:alice".to_string(),
            context: None,
        };
        assert!(tuple_of(&record).condition.is_none());
    }

    /// The record shapes an owner change produces under the held-key arm: a
    /// conditional fact whose subject is the wildcard, and an unconditional one
    /// whose subject is the owner.
    fn owner_records(owner: &str) -> [Record; 2] {
        let (gate, condition) = test_names::gated_relation();
        [
            Record {
                object: "notes:1".to_string(),
                relation: gate,
                subject: "user:*".to_string(),
                context: Some(RecordContextValue {
                    condition,
                    values: BTreeMap::from([("owner".to_string(), owner.to_string())]),
                }),
            },
            Record {
                object: "notes:1".to_string(),
                relation: test_names::relation("owner"),
                subject: format!("user:{owner}"),
                context: None,
            },
        ]
    }

    fn without_condition(record: &Record) -> TupleKeyWithoutCondition {
        TupleKeyWithoutCondition {
            user: record.subject.clone(),
            relation: record.relation.clone().to_string(),
            object: record.object.clone(),
        }
    }

    /// A context is not part of the server's tuple key, so a change that moves
    /// only a context states one key on both sides, and the call carrying both
    /// is refused however small it is. The pair differing in subject alongside
    /// it is what says the test is about the key rather than about the sizes.
    #[test]
    fn a_change_of_only_a_context_cannot_travel_in_one_call() {
        let after = owner_records("carol");
        let before = owner_records("alice");
        let writes: Vec<_> = after.iter().map(tuple_of).collect();
        let deletes: Vec<_> = before.iter().map(without_condition).collect();

        assert!(
            !fits_one_call(&writes, &deletes),
            "the wildcard gate is one key on both sides"
        );
        assert!(
            fits_one_call(&writes[1..], &deletes[1..]),
            "and the owner records, which differ in their subject, are two keys"
        );
    }

    /// The size test still holds, and it is checked first so the scan for a
    /// shared key never runs over a list that is already too long.
    #[test]
    fn a_difference_larger_than_one_call_is_split() {
        let writes: Vec<_> = (0..=MAX_TUPLES_PER_WRITE)
            .map(|nth| Record {
                object: format!("notes:{nth}"),
                relation: test_names::relation("owner"),
                subject: "user:alice".to_string(),
                context: None,
            })
            .map(|record| tuple_of(&record))
            .collect();
        assert!(!fits_one_call(&writes, &[]));
        assert!(fits_one_call(&writes[1..], &[]));
    }

    /// A read takes the cheap path by default, which is what makes asking per
    /// watcher affordable. A stale answer there is re-asked on the next event.
    #[test]
    fn a_read_asks_cheaply_by_default() {
        assert_eq!(
            consistency_for(
                ActionStatement::Select,
                ConsistencyPreference::MinimizeLatency
            ),
            ConsistencyPreference::MinimizeLatency
        );
    }

    /// A deployment wanting every answer authoritative raises the read path, and
    /// the choice reaches the question rather than being overridden.
    #[test]
    fn a_raised_read_preference_reaches_the_question() {
        assert_eq!(
            consistency_for(
                ActionStatement::Select,
                ConsistencyPreference::HigherConsistency
            ),
            ConsistencyPreference::HigherConsistency
        );
    }

    /// The preference travels as itself. `Unspecified` is documented as behaving
    /// like the cheap path, so sending it asks for a cached answer while looking
    /// like no request at all, which is the one value a reader misreads.
    #[test]
    fn the_request_names_the_preference_it_was_given() {
        let questions = [a_question()];
        for preference in [
            ConsistencyPreference::MinimizeLatency,
            ConsistencyPreference::HigherConsistency,
        ] {
            let request = batch_request("store", "", &questions, preference);
            assert_eq!(request.consistency, preference as i32);
            assert_ne!(
                request.consistency,
                ConsistencyPreference::Unspecified as i32,
                "an unset preference is the cached path wearing a neutral name"
            );
        }
    }

    /// A watcher answering one parameter with whatever it was given.
    struct Holder {
        values: Vec<&'static str>,
    }

    impl Subject for Holder {
        fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
            core::iter::once(Cow::Borrowed("user:one"))
        }

        fn request_value(&self, parameter: &str, out: &mut RequestValues) -> bool {
            if parameter != "app_subjects" {
                return false;
            }
            for value in &self.values {
                out.push(value);
            }
            true
        }
    }

    fn required(list: bool) -> Vec<RequiredParameter> {
        vec![RequiredParameter {
            parameter: "app_subjects".to_string(),
            setting_key: Some("app.subjects".to_string()),
            list,
        }]
    }

    /// A set parameter travels as a list, so a condition testing membership sees
    /// every value the caller sent.
    #[test]
    fn a_set_parameter_travels_as_a_list() {
        let mut values = RequestValues::new();
        let context = context_for(
            &required(true),
            &Holder {
                values: vec!["a", "b"],
            },
            &mut values,
        )
        .expect("the watcher answered");

        let held = context.fields.get("app_subjects").expect("the parameter");
        let Some(Kind::ListValue(list)) = held.kind.as_ref() else {
            panic!("expected a list, got {held:?}");
        };
        assert_eq!(list.values.len(), 2);
    }

    /// A single-value parameter travels as one string, and a watcher that sent
    /// several has not answered it.
    ///
    /// Sending the first would put a value the policy never compared against
    /// into the context, which the server would then evaluate as a match.
    #[test]
    fn a_single_value_parameter_refuses_a_watcher_that_sent_several() {
        let mut values = RequestValues::new();
        let one = context_for(&required(false), &Holder { values: vec!["a"] }, &mut values)
            .expect("one value answers");
        assert!(matches!(
            one.fields.get("app_subjects").unwrap().kind.as_ref(),
            Some(Kind::StringValue(value)) if value == "a"
        ));

        let many = context_for(
            &required(false),
            &Holder {
                values: vec!["a", "b"],
            },
            &mut values,
        );
        assert!(many.is_none(), "several values answer nothing here");
    }

    /// A watcher answering whatever it is asked, which is what makes the test
    /// below about the skip rather than about the watcher refusing.
    struct Eager;

    impl Subject for Eager {
        fn subjects(&self) -> impl Iterator<Item = Cow<'_, str>> {
            core::iter::once(Cow::Borrowed("user:one"))
        }

        fn request_value(&self, _parameter: &str, out: &mut RequestValues) -> bool {
            out.push("whatever-it-was-asked");
            true
        }
    }

    /// A parameter no watcher owns is never asked of one, even when the watcher
    /// would happily answer.
    ///
    /// The clock is such a parameter: the report names it with no session
    /// setting behind it. Filling it from a watcher would put the watcher's own
    /// value where the condition expects a time, and the server would then
    /// compare against it.
    #[test]
    fn a_parameter_no_watcher_owns_is_not_asked_of_one() {
        let mut values = RequestValues::new();
        let context = context_for(
            &[RequiredParameter {
                parameter: "request_time".to_string(),
                setting_key: None,
                list: false,
            }],
            &Eager,
            &mut values,
        );
        assert!(
            context.is_none(),
            "the eager watcher was never asked, so nothing was filled"
        );
    }

    /// Only a failure to reach the server is worth trying again. A question it
    /// answered or rejected gives the same answer however often it is asked, and
    /// retrying holds the change stream for nothing.
    #[test]
    fn only_transport_failures_are_retried() {
        assert!(OpenFgaError::is_transport(Code::Unavailable));
        assert!(OpenFgaError::is_transport(Code::DeadlineExceeded));
        assert!(!OpenFgaError::is_transport(Code::InvalidArgument));
        assert!(!OpenFgaError::is_transport(Code::PermissionDenied));
        assert!(!OpenFgaError::is_transport(Code::NotFound));
    }

    /// An index carrying a grant the caller's request completes, and no report of
    /// which parameters carry it, refuses to build.
    ///
    /// Every such question would be refused by the server, so saying so once at
    /// construction beats discovering it per event. The parameters arrive in a
    /// report whose type is `#[non_exhaustive]`, so a variant renamed upstream
    /// reads as "none required" rather than failing to compile, and this is the
    /// guard against that.
    #[test]
    fn a_request_gated_index_without_its_parameters_refuses_to_build() {
        let sql = "
CREATE TABLE notes(id INTEGER PRIMARY KEY, owner TEXT);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_p ON notes USING (
  owner = ANY(string_to_array(current_setting('app.subjects', true), ',')));
";
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let translation = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .with_session_attributes([SessionAttribute::setting(
                "app.subjects",
                SessionAttributeKind::SetAttribute,
            )])
            .build()
            .translate(&db);
        let relations = translation.relations();
        let notes = translation.notes().to_vec();

        let bare = Shapes::new(db, &relations);
        assert!(
            bare.has_request_gated_recipe(),
            "the recipe grants through the caller's own values"
        );
        assert!(
            bare.required_parameters().is_empty(),
            "built without notes the index reports no required parameters"
        );

        assert_eq!(
            usable_index(&bare),
            Err(OpenFgaError::MissingRequiredParameters),
            "so a policy over it refuses to build"
        );

        let told = Shapes::new(
            ParserDB::parse::<PostgreSqlDialect>(sql).unwrap(),
            &relations,
        )
        .with_required_parameters(&notes);
        assert_eq!(told.required_parameters().len(), 1);
        assert_eq!(
            usable_index(&told),
            Ok(()),
            "and one told which parameters carry it builds"
        );
    }

    /// A transport that panics if anything is asked of it.
    ///
    /// The tests below are about questions that are never sent, so a real
    /// channel would prove less: this one turns a regression into a failure
    /// rather than into a call nobody notices.
    #[derive(Clone)]
    struct NeverAsked;

    impl GrpcService<Body> for NeverAsked {
        type ResponseBody = Body;
        type Error = Status;
        type Future = core::future::Ready<Result<Response<Body>, Status>>;

        fn poll_ready(&mut self, _: &mut CoreContext<'_>) -> Poll<Result<(), Status>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: Request<Body>) -> Self::Future {
            panic!("the model refused this statement, so nothing may be asked")
        }
    }

    fn policy_over(sql: &str) -> OpenFgaPolicy<ParserDB, NeverAsked, String, Postgres> {
        let db = ParserDB::parse::<PostgreSqlDialect>(sql).unwrap();
        let translation = TranslatorBuilder::new()
            .with_min_confidence(ConfidenceLevel::B)
            .build()
            .translate(&db);
        let (relations, naming, answers) = (
            translation.relations(),
            translation.row_naming(),
            translation.action_relations(),
        );
        let shapes = Arc::new(
            Shapes::new(db, &relations)
                .with_row_naming(&naming)
                .with_action_relations(&answers),
        );
        OpenFgaPolicy::new(shapes, OpenFgaServiceClient::new(NeverAsked), "store").unwrap()
    }

    /// Row-level security on with no policy: the model refuses every statement.
    const CLOSED: &str = "
CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER);
ALTER TABLE ledger ENABLE ROW LEVEL SECURITY;
";

    /// Spin rather than schedule: every future below answers on its first poll,
    /// because a refused statement asks nothing, and a regression that asks
    /// panics in [`NeverAsked`] rather than hanging here.
    fn block_on<F: core::future::Future>(future: F) -> F::Output {
        let mut context = CoreContext::from_waker(core::task::Waker::noop());
        let mut pinned = core::pin::pin!(future);
        loop {
            if let Poll::Ready(value) = pinned.as_mut().poll(&mut context) {
                return value;
            }
        }
    }

    /// A refusal the model states is an answer, so it comes back as a denial
    /// rather than as [`OpenFgaError::StatementNotAnswered`]. The difference
    /// decides what a caller does with it: an unanswered statement is held and
    /// retried, and the retry fails identically, while a denial is final.
    #[test]
    fn a_write_the_model_refuses_is_denied_without_asking() {
        let policy = policy_over(CLOSED);
        let ledger = catalog_helpers::table_id(policy.shapes().catalog(), "ledger").unwrap();
        let event = TestEvent::<Postgres>::insert(ledger, vec![Value::Int(4), Value::Int(7)])
            .with_pk_columns([0u16]);
        let view = EventRow::current(&event, policy.shapes().catalog()).unwrap();

        let got =
            block_on(policy.may_write(RowWrite::Insert { new: &view }, &"user:alice".to_string()));

        assert_eq!(got, Ok(Verdict::Deny), "nobody may write this table");
    }

    /// The same for a read, which is a different path with the same rule. Every
    /// watcher is denied, and the buffer the caller pre-filled is what says so.
    #[test]
    fn a_read_the_model_refuses_denies_every_watcher_without_asking() {
        let policy = policy_over(CLOSED);
        let ledger = catalog_helpers::table_id(policy.shapes().catalog(), "ledger").unwrap();
        let event = TestEvent::<Postgres>::insert(ledger, vec![Value::Int(4), Value::Int(7)])
            .with_pk_columns([0u16]);
        let view = EventRow::current(&event, policy.shapes().catalog()).unwrap();
        let watchers = ["user:alice".to_string(), "user:bob".to_string()];
        let mut verdicts = vec![Verdict::Deny; 2];

        block_on(policy.may_see(&view, &watchers, &mut verdicts)).unwrap();

        assert_eq!(
            verdicts,
            [Verdict::Deny, Verdict::Deny],
            "nobody reads this table"
        );
    }

    /// And a table the model does grant reads on still names the relation to
    /// ask, so the refusal above is read off the report rather than standing in
    /// for every table.
    #[test]
    fn a_read_the_model_grants_still_names_its_relation() {
        let policy = policy_over(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, owner_id TEXT);
             ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
             CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);",
        );
        let docs = catalog_helpers::table_id(policy.shapes().catalog(), "docs").unwrap();

        let asked = policy.read_relation(docs).unwrap();
        let Asked::Ask(relation) = asked else {
            panic!("a granted read names the relation that answers it");
        };
        assert_eq!(relation.as_str(), "can_select");
    }
}
