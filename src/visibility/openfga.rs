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
    BatchCheckItem, BatchCheckRequest, CheckRequestTupleKey, ContextualTupleKeys,
    OpenFgaServiceClient, RelationshipCondition, TupleKey, TupleKeyWithoutCondition, WriteRequest,
    WriteRequestDeletes, WriteRequestWrites,
};
use openfga_client::prost_wkt_types::{value::Kind, ListValue, Struct, Value as ProstValue};
use openfga_client::tonic::body::Body;
use openfga_client::tonic::client::GrpcService;
use openfga_client::tonic::codegen::{Bytes, StdError};
use openfga_client::tonic::Code;
use rls2fga::generator::records::Record;
use rls2fga::parser::identifiers::RelationName;
use sql_traits::prelude::DatabaseLike;

use crate::backend::Backend;
use crate::visibility::policy::{image_of, statement_of, table_of, RequestValues, Subject};
use crate::TableId;
use core::ops::Not;

use rls2fga::generator::action_relations::{ActionAnswer, ActionStatement, RowVersion};

use crate::visibility::shapes::{RequiredParameter, Shapes, SharedShapes};
use crate::visibility::store::StoreDiff;
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
    /// A record carries a condition context and the model names no condition
    /// for the relation it is written on, so the tuple cannot be sent.
    ///
    /// Reached when the index was built without
    /// [`Shapes::with_condition_names`](crate::visibility::shapes::Shapes::with_condition_names),
    /// or for a relation the model declares more than one condition on.
    #[error("the model names no single condition for the relation {relation}")]
    ConditionNotNamed {
        /// The relation the tuple is written on.
        relation: String,
    },
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
    /// Ask `questions`, in calls of at most the configured cap, writing each
    /// answer where its question says.
    async fn ask(
        &self,
        questions: Vec<Question>,
        verdicts: &mut [Verdict],
    ) -> Result<(), OpenFgaError> {
        for chunk in questions.chunks(self.max_checks_per_batch) {
            let request = BatchCheckRequest {
                store_id: self.store_id.clone(),
                checks: chunk.iter().map(|question| question.item.clone()).collect(),
                authorization_model_id: self.authorization_model_id.clone(),
                consistency: 0,
            };
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

    /// The questions `write` raises, one group per rule that has to grant.
    ///
    /// A replacement is two rules across two versions. The one choosing which
    /// rows may be touched reads the stored row, and the one admitting the
    /// result reads a row the store has never seen, so that group carries the
    /// facts the new row implies.
    fn write_plan<R>(
        &self,
        write: RowWrite<'_, R>,
        watcher: &W,
    ) -> Result<Vec<Vec<Question>>, OpenFgaError>
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
            ActionAnswer::Unrestricted => return Ok(Vec::new()),
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
                RowVersion::Resulting => self.contextual_of(row)?,
                _ => Vec::new(),
            };
            plan.push(self.questions(row, judge.relation.as_str(), only, &contextual)?);
        }
        Ok(plan)
    }

    /// The relation that answers a read of `table`, as the model reports it.
    ///
    /// A read judges the row as it is, so its answer is one relation against one
    /// version. Anything else, including a table the model names no type for,
    /// leaves the question unputtable rather than guessed at.
    ///
    /// # Errors
    ///
    /// [`OpenFgaError::StatementNotAnswered`] when the model says nothing this
    /// can ask.
    fn read_relation(&self, table: TableId) -> Result<RelationName, OpenFgaError> {
        let statement = ActionStatement::Select;
        let unanswered = || OpenFgaError::StatementNotAnswered { statement };
        let ActionAnswer::Judged(judges) = self
            .shapes
            .answer(table, statement)
            .ok_or_else(unanswered)?
        else {
            // An unrestricted table has nothing to ask, and this path exists to
            // ask, so the caller answers it locally or not at all.
            return Err(unanswered());
        };
        match judges.as_slice() {
            [judge] if judge.version == RowVersion::Existing => Ok(judge.relation.clone()),
            _ => Err(unanswered()),
        }
    }

    /// The facts the row being written implies, as tuples the question carries.
    ///
    /// A row that does not exist yet has nothing stored about it, so the rule
    /// admitting it can only be evaluated against facts supplied with the
    /// question.
    ///
    /// # Errors
    ///
    /// [`OpenFgaError::ConditionNotNamed`] for a record carrying a condition
    /// context whose condition the model does not name. Sending it unnamed
    /// would have the server evaluate the tuple by no condition at all, which
    /// grants whatever the condition was there to restrict.
    fn contextual_of<R>(&self, row: &R) -> Result<Vec<TupleKey>, OpenFgaError>
    where
        R: RowView + ?Sized,
    {
        let Some(shapes) = self.shapes.table_records(row.table_id()) else {
            return Ok(Vec::new());
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
                let condition = match record.context {
                    None => None,
                    Some(context) => {
                        let object_type = record
                            .object
                            .split_once(':')
                            .map_or(record.object.as_str(), |(kind, _)| kind);
                        let name = self
                            .shapes
                            .condition_name(object_type, record.relation.as_str())
                            .ok_or_else(|| OpenFgaError::ConditionNotNamed {
                                relation: record.relation.clone().to_string(),
                            })?;
                        Some(RelationshipCondition {
                            name: name.to_string(),
                            context: Some(Struct {
                                fields: StructFields::from_iter([(
                                    context.key,
                                    ProstValue {
                                        kind: Some(Kind::StringValue(context.value)),
                                    },
                                )]),
                            }),
                        })
                    }
                };
                out.push(TupleKey {
                    user: record.subject,
                    relation: record.relation.to_string(),
                    object: record.object,
                    condition,
                });
            }
        }
        Ok(out)
    }
}

/// OpenFGA's own default limit on tuples per write.
///
/// Server configuration again, and again not reported by any call, so the
/// default matches the server's and a difference larger than one write is split.
const MAX_TUPLES_PER_WRITE: usize = 100;

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
    /// hands never has a moment where both subjects hold it. A larger one cannot
    /// be, and then removals go first, which is the fail-closed order.
    ///
    /// [`StoreDiff::requeries`] is **not** covered here. Those are the facts no
    /// single row settles, and running their SQL belongs to the caller, which
    /// then hands the records back through [`write_records`](Self::write_records).
    /// A caller that ignores them leaves every two-table fact stale.
    ///
    /// # Errors
    ///
    /// [`OpenFgaError::Transport`] when the server could not be reached,
    /// [`OpenFgaError::Rejected`] when it refused the write, and
    /// [`OpenFgaError::ConditionNotNamed`] for an added record whose condition
    /// the model does not name.
    pub async fn apply(&self, diff: &StoreDiff<'_, B>) -> Result<(), OpenFgaError> {
        let mut writes = Vec::with_capacity(diff.added.len());
        for record in &diff.added {
            writes.push(self.tuple_of(record)?);
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

        // One call while both halves fit, because the server applies a write
        // atomically: a row changing hands then has no moment where both the old
        // and the new subject hold it, and none where neither does.
        if writes.len() + deletes.len() <= MAX_TUPLES_PER_WRITE {
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

        // Too large for one call, so the atomicity is gone and the order is
        // what is left to choose. Removals go first: between the calls the row
        // then reaches nobody rather than everybody, and a client told too late
        // that it may see a row loses nothing it was entitled to.
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
    /// This is also where a caller puts what replaying a
    /// [`Requery`](crate::visibility::store::Requery) returned, so the facts no
    /// one row settles reach the store through the same writer as the rest.
    ///
    /// # Errors
    ///
    /// As [`apply`](Self::apply).
    pub async fn write_records(&self, records: &[Record]) -> Result<(), OpenFgaError> {
        for chunk in records.chunks(MAX_TUPLES_PER_WRITE) {
            let mut tuple_keys = Vec::with_capacity(chunk.len());
            for record in chunk {
                tuple_keys.push(self.tuple_of(record)?);
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

    /// One record as a tuple, carrying its condition when it has one.
    fn tuple_of(&self, record: &Record) -> Result<TupleKey, OpenFgaError> {
        let condition = match record.context.as_ref() {
            None => None,
            Some(context) => Some(RelationshipCondition {
                name: self.condition_for(record)?,
                context: Some(Struct {
                    fields: StructFields::from_iter([(
                        context.key.clone(),
                        ProstValue {
                            kind: Some(Kind::StringValue(context.value.clone())),
                        },
                    )]),
                }),
            }),
        };
        Ok(TupleKey {
            user: record.subject.clone(),
            relation: record.relation.clone().to_string(),
            object: record.object.clone(),
            condition,
        })
    }

    /// The condition the model names for `record`'s relation.
    fn condition_for(&self, record: &Record) -> Result<String, OpenFgaError> {
        let object_type = record
            .object
            .split_once(':')
            .map_or(record.object.as_str(), |(kind, _)| kind);
        self.shapes
            .condition_name(object_type, record.relation.as_str())
            .map(ToString::to_string)
            .ok_or_else(|| OpenFgaError::ConditionNotNamed {
                relation: record.relation.clone().to_string(),
            })
    }

    /// One write, retried while the failure is the transport's.
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
        let questions = self
            .read_relation(row.table_id())
            .and_then(|relation| self.questions(row, relation.as_str(), watchers, &[]));
        async move { self.ask(questions?, verdicts).await }
    }

    fn may_write<R>(
        &self,
        write: RowWrite<'_, R>,
        watcher: &Self::Watcher,
    ) -> impl core::future::Future<Output = Result<Verdict, Self::Error>> + Send
    where
        R: RowView<Backend = Self::Backend> + Sync + ?Sized,
    {
        let plan = self.write_plan(write, watcher);
        async move {
            // Every half must grant, so the first refusal is the answer and the
            // rest are never asked.
            for questions in plan? {
                let mut verdict = [Verdict::Deny];
                self.ask(questions, &mut verdict).await?;
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
    use alloc::vec;

    use rls2fga::classifier::function_registry::{SessionAttribute, SessionAttributeKind};
    use rls2fga::classifier::patterns::ConfidenceLevel;
    use rls2fga::translator::TranslatorBuilder;
    use sqlparser::dialect::PostgreSqlDialect;

    use super::{
        context_for, usable_index, Code, Kind, OpenFgaError, RequestValues, RequiredParameter,
        Subject,
    };
    use crate::visibility::shapes::Shapes;
    use crate::ParserDB;

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
        assert!(bare.required_parameters().is_empty());

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
}
