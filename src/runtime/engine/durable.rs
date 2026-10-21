#![doc = include_str!("../../../DURABLE_REPLAY.md")]

use super::replay::{ReplayAnswer, ReplayDetector, ReplayError, ReplayTable};
use super::state_codec;
use super::{CdcEvent, DatabaseLike, IdTypes, SqlLiteralParse, SubscriptionEngine};
use crate::backend::Backend;
use crate::{Checkpoint, SubscriptionRequest, SubscriptionScope};
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use rpds::RedBlackTreeMapSync;
use serde::{Deserialize, Serialize};

const VERSION: u32 = 1;

/// The source generation and authority revision required for recovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableContext {
    pub source: Vec<u8>,
    pub authority: Vec<u8>,
}

/// A caller-owned authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DurableAuthorizationError {
    #[error("authorization denied")]
    Denied,
    #[error("authorization service unavailable")]
    Unavailable,
}

/// A rejected durable operation leaves the published checkpoint unchanged.
#[derive(Debug, thiserror::Error)]
pub enum DurableError {
    #[error(transparent)]
    Replay(#[from] ReplayError),
    #[error(transparent)]
    Codec(#[from] postcard::Error),
    #[error("durable declaration codec failed: {0}")]
    Declaration(String),
    #[error(transparent)]
    Authorization(#[from] DurableAuthorizationError),
    #[error("durable recovery context is missing or incompatible")]
    Context,
    #[error("durable recovery state is missing or incompatible")]
    State,
    #[error("the durable compiler must have an empty registry")]
    Registry,
    #[error("instance {0} already exists")]
    AlreadyExists(u64),
    #[error("instance {0} does not exist")]
    UnknownInstance(u64),
    #[error("durable instances require durable scope")]
    Scope,
    #[error("bootstrap does not match the acknowledged source boundary")]
    BootstrapBoundary,
    #[error("source history does not continue the acknowledged boundary")]
    HistoryGap,
    #[error("source events do not belong to the declared commit")]
    EventBoundary,
    #[error("acknowledgement exceeds the acknowledged source boundary")]
    FutureAcknowledgement,
    #[error("instance state requires at least {required} bytes with a quota of {quota}")]
    Capacity { required: usize, quota: usize },
    #[error("durable storage revision exhausted")]
    RevisionExhausted,
}

/// A persistence failure retains the unchanged preparation for retry.
#[derive(Debug, thiserror::Error)]
pub enum DurablePersistError<T, P> {
    #[error("durable persistence callback failed with {error:?}")]
    Callback { error: P, preparation: T },
}

fn admission_replay_error(error: ReplayError, quota: usize) -> DurableError {
    match error {
        ReplayError::Capacity(_) => DurableError::Capacity {
            required: quota.saturating_add(1),
            quota,
        },
        error => error.into(),
    }
}

/// A selected-result change at a committed source boundary.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct DurableChange<I: IdTypes, C: Checkpoint> {
    pub instance: u64,
    pub consumer: I::ConsumerId,
    pub at: C,
}

/// A terminal detector-capacity failure preserves the last valid boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct DurableFailure<C: Checkpoint> {
    pub instance: u64,
    pub at: C,
    pub last_valid: C,
    pub quota: usize,
}

/// The storage-side effect accompanying a checkpoint revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum DurableOperation<C: Checkpoint> {
    Source,
    Register { instance: u64 },
    Acknowledge { instance: u64, unread: Option<C> },
    Remove { instance: u64 },
}

/// Persist every field atomically with a compare-and-swap of the storage revision.
pub struct DurableCommit<I: IdTypes, C: Checkpoint> {
    pub expected_revision: u64,
    pub revision: u64,
    pub previous: C,
    pub at: C,
    pub checkpoint: Vec<u8>,
    pub changed: Vec<DurableChange<I, C>>,
    pub failures: Vec<DurableFailure<C>>,
    pub operation: DurableOperation<C>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct Instance<I: IdTypes, B: Backend, C: Checkpoint> {
    declaration: Arc<Vec<u8>>,
    operator: Arc<Vec<u8>>,
    quota: usize,
    unread: Option<C>,
    last_valid: C,
    failure: Option<DurableFailure<C>>,
    #[serde(skip)]
    detector: Option<Arc<ReplayDetector<B>>>,
    #[serde(skip)]
    request: Option<Arc<SubscriptionRequest<I, B>>>,
}

impl<I: IdTypes, B: Backend, C: Checkpoint> Clone for Instance<I, B, C> {
    fn clone(&self) -> Self {
        Self {
            declaration: Arc::clone(&self.declaration),
            operator: Arc::clone(&self.operator),
            quota: self.quota,
            unread: self.unread.clone(),
            last_valid: self.last_valid.clone(),
            failure: self.failure.clone(),
            detector: self.detector.as_ref().map(Arc::clone),
            request: self.request.as_ref().map(Arc::clone),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct Snapshot<I: IdTypes, B: Backend, C: Checkpoint> {
    version: u32,
    backend: Arc<str>,
    checkpoint_type: Arc<str>,
    context: Arc<DurableContext>,
    revision: u64,
    at: C,
    instances: RedBlackTreeMapSync<u64, Arc<Instance<I, B, C>>>,
}

impl<I: IdTypes, B: Backend, C: Checkpoint> Clone for Snapshot<I, B, C> {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            backend: Arc::clone(&self.backend),
            checkpoint_type: Arc::clone(&self.checkpoint_type),
            context: Arc::clone(&self.context),
            revision: self.revision,
            at: self.at.clone(),
            instances: self.instances.clone(),
        }
    }
}

/// An exact replay engine whose state publishes only after durable persistence.
pub struct DurableEngine<E: CdcEvent, I: IdTypes, DB: DatabaseLike>
where
    E::Backend: SqlLiteralParse,
{
    compiler: SubscriptionEngine<E, I, DB>,
    state: Snapshot<I, E::Backend, E::Checkpoint>,
}

/// An unpublished operation holds exclusive access until persisted or dropped.
#[must_use]
pub struct PreparedDurable<'a, E: CdcEvent, I: IdTypes, DB: DatabaseLike>
where
    E::Backend: SqlLiteralParse,
{
    engine: &'a mut DurableEngine<E, I, DB>,
    state: Snapshot<I, E::Backend, E::Checkpoint>,
    commit: DurableCommit<I, E::Checkpoint>,
}

impl<E: CdcEvent, I: IdTypes, DB: DatabaseLike> core::fmt::Debug for PreparedDurable<'_, E, I, DB>
where
    E::Backend: SqlLiteralParse,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PreparedDurable")
            .field("revision", &self.commit.revision)
            .field("at", &self.commit.at)
            .finish_non_exhaustive()
    }
}

fn instance_bytes<I: IdTypes, B: Backend, C: Checkpoint>(
    instance: &Instance<I, B, C>,
) -> Result<usize, DurableError> {
    let metadata = postcard::experimental::serialized_size(&(
        instance.quota,
        &instance.unread,
        &instance.last_valid,
        &instance.failure,
    ))?;
    Ok(
        postcard::experimental::serialized_size(&instance.declaration.len())?
            + instance.declaration.len()
            + metadata
            + postcard::experimental::serialized_size(&instance.operator.len())?
            + instance.operator.len(),
    )
}

impl<E: CdcEvent, I: IdTypes, DB: DatabaseLike + 'static> DurableEngine<E, I, DB>
where
    E::Backend: SqlLiteralParse,
{
    /// Creates an empty durable registry at a consistent source boundary.
    ///
    /// # Errors
    /// Rejects empty context identities or a compiler with registered subscriptions.
    pub fn new(
        compiler: SubscriptionEngine<E, I, DB>,
        context: DurableContext,
        at: E::Checkpoint,
    ) -> Result<Self, DurableError> {
        if context.source.is_empty() || context.authority.is_empty() {
            return Err(DurableError::Context);
        }
        if at.transaction_boundary().as_ref() != Some(&at) {
            return Err(DurableError::EventBoundary);
        }
        if compiler.subscription_count() != 0 || compiler.reread_count() != 0 {
            return Err(DurableError::Registry);
        }
        Ok(Self {
            compiler,
            state: Snapshot {
                version: VERSION,
                backend: core::any::type_name::<E::Backend>().into(),
                checkpoint_type: core::any::type_name::<E::Checkpoint>().into(),
                context: Arc::new(context),
                revision: 0,
                at,
                instances: RedBlackTreeMapSync::new_sync(),
            },
        })
    }

    /// Restores a checkpoint only after every declaration is freshly authorized.
    ///
    /// # Errors
    /// Rejects incompatible context, receipts, catalogs, plans or operator state.
    pub fn restore(
        compiler: SubscriptionEngine<E, I, DB>,
        context: DurableContext,
        revision: u64,
        at: E::Checkpoint,
        bytes: &[u8],
        mut authorize: impl FnMut(
            &SubscriptionRequest<I, E::Backend>,
        ) -> Result<(), DurableAuthorizationError>,
    ) -> Result<Self, DurableError> {
        let mut engine = Self::new(compiler, context, at)?;
        let (mut state, remaining): (Snapshot<I, E::Backend, E::Checkpoint>, _) =
            postcard::take_from_bytes(bytes)?;
        if !remaining.is_empty()
            || state.version != VERSION
            || state.backend != engine.state.backend
            || state.checkpoint_type != engine.state.checkpoint_type
            || state.context != engine.state.context
            || state.revision != revision
            || state.at != engine.state.at
        {
            return Err(DurableError::State);
        }
        let ids = state.instances.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let instance = state.instances.get_mut(&id).ok_or(DurableError::State)?;
            let record = Arc::get_mut(instance).ok_or(DurableError::State)?;
            let request: SubscriptionRequest<I, E::Backend> =
                state_codec::decode(&record.declaration)
                    .map_err(|error| DurableError::Declaration(error.to_string()))?;
            if request.scope != SubscriptionScope::Durable
                || record.quota == 0
                || record.last_valid > state.at
                || record.last_valid.transaction_boundary().as_ref() != Some(&record.last_valid)
                || (record.failure.is_none() && record.last_valid != state.at)
                || record.unread.as_ref().is_some_and(|at| {
                    at > &record.last_valid || at.transaction_boundary().as_ref() != Some(at)
                })
                || record.failure.as_ref().is_some_and(|failure| {
                    failure.instance != id
                        || failure.quota != record.quota
                        || failure.last_valid != record.last_valid
                        || failure.at <= record.last_valid
                        || failure.at > state.at
                        || failure.at.transaction_boundary().as_ref() != Some(&failure.at)
                })
            {
                return Err(DurableError::State);
            }
            authorize(&request)?;
            let request = Arc::new(request);
            let detector = engine
                .compiler
                .restore_replay_detector(request.as_ref(), record.operator.as_slice())?;
            if record.failure.is_none() && instance_bytes(record)? > record.quota {
                return Err(DurableError::State);
            }
            record.detector = Some(Arc::new(detector));
            record.request = Some(request);
        }
        engine.state = state;
        Ok(engine)
    }

    /// Prepares admission from complete dependency inputs at the acknowledged boundary.
    ///
    /// # Errors
    /// Rejects unauthorized, duplicate, unsupported, incomplete or over-quota admissions.
    pub fn prepare_register(
        &mut self,
        instance: u64,
        spec: SubscriptionRequest<I, E::Backend>,
        at: E::Checkpoint,
        tables: Vec<ReplayTable<E::Backend>>,
        quota: usize,
        authorize: impl FnOnce(
            &SubscriptionRequest<I, E::Backend>,
        ) -> Result<(), DurableAuthorizationError>,
    ) -> Result<PreparedDurable<'_, E, I, DB>, DurableError> {
        if self.state.instances.contains_key(&instance) {
            return Err(DurableError::AlreadyExists(instance));
        }
        if spec.scope != SubscriptionScope::Durable {
            return Err(DurableError::Scope);
        }
        if at != self.state.at {
            return Err(DurableError::BootstrapBoundary);
        }
        authorize(&spec)?;
        let declaration =
            Arc::new(
                state_codec::encode_limited(&spec, quota).map_err(|error| match error {
                    state_codec::StateCodecError::OverBudget(_) => DurableError::Capacity {
                        required: quota.saturating_add(1),
                        quota,
                    },
                    error => DurableError::Declaration(error.to_string()),
                })?,
            );
        let request = Arc::new(spec);
        let detector = self
            .compiler
            .replay_detector_bounded(&request, tables, quota)
            .map_err(|error| admission_replay_error(error, quota))?;
        let operator = Arc::new(
            detector
                .checkpoint_limited(quota)
                .map_err(|error| admission_replay_error(error, quota))?,
        );
        let request = Some(request);
        let record = Instance {
            declaration,
            request,
            operator,
            quota,
            unread: None,
            last_valid: at,
            failure: None,
            detector: Some(Arc::new(detector)),
        };
        let required = instance_bytes(&record)?;
        if quota == 0 || required > quota {
            return Err(DurableError::Capacity { required, quota });
        }
        let mut state = self.state.clone();
        state.instances.insert_mut(instance, Arc::new(record));
        self.prepare(
            state,
            Vec::new(),
            Vec::new(),
            DurableOperation::Register { instance },
        )
    }

    /// Stages one complete source transaction, returning nothing for a duplicate.
    ///
    /// # Errors
    /// Rejects source/history mismatches, incomplete events or failed operator evaluation.
    #[expect(
        clippy::too_many_lines,
        reason = "one pass settles each instance's operator, evidence and failure at a source boundary"
    )]
    pub fn prepare_transaction(
        &mut self,
        source: &[u8],
        after: &E::Checkpoint,
        at: E::Checkpoint,
        events: &[E],
    ) -> Result<Option<PreparedDurable<'_, E, I, DB>>, DurableError> {
        if source != self.state.context.source {
            return Err(DurableError::Context);
        }
        if at <= self.state.at {
            return Ok(None);
        }
        if after != &self.state.at || &at <= after {
            return Err(DurableError::HistoryGap);
        }
        if at.transaction_boundary().as_ref() != Some(&at)
            || events.iter().any(|event| {
                event
                    .checkpoint()
                    .and_then(|position| position.transaction_boundary())
                    .as_ref()
                    != Some(&at)
            })
        {
            return Err(DurableError::EventBoundary);
        }
        let mut state = self.state.clone();
        let mut changed = Vec::new();
        let mut failures = Vec::new();
        for (&id, published) in &self.state.instances {
            if published.failure.is_some() {
                continue;
            }
            let detector = published.detector.as_ref().ok_or(DurableError::State)?;
            let relevant = events.iter().any(|event| {
                detector
                    .dependencies()
                    .contains(&event.table_id(self.compiler.database()))
            });
            let mut staged = published.as_ref().clone();
            staged.last_valid = at.clone();
            let mut over_capacity = false;
            let result_changed = if relevant {
                staged.unread = None;
                let metadata = postcard::experimental::serialized_size(&staged.declaration.len())?
                    + staged.declaration.len()
                    + postcard::experimental::serialized_size(&(
                        staged.quota,
                        &staged.unread,
                        &staged.last_valid,
                        &staged.failure,
                    ))?;
                let available = staged.quota.saturating_sub(metadata);
                let mut budget = available.saturating_sub(1);
                while budget > 0
                    && budget.saturating_add(postcard::experimental::serialized_size(&budget)?)
                        > available
                {
                    budget -= 1;
                }
                let mut detector = detector.as_ref().clone();
                match detector.apply_transaction_limited(events, self.compiler.database(), budget) {
                    Ok((changed, checkpoint)) => {
                        staged.operator = Arc::new(checkpoint);
                        staged.detector = Some(Arc::new(detector));
                        changed
                    }
                    Err(ReplayError::Capacity(_)) => {
                        over_capacity = true;
                        false
                    }
                    Err(error) => return Err(error.into()),
                }
            } else {
                false
            };
            staged.unread = if result_changed {
                Some(at.clone())
            } else {
                published.unread.clone()
            };
            if over_capacity || instance_bytes(&staged)? > staged.quota {
                let failure = DurableFailure {
                    instance: id,
                    at: at.clone(),
                    last_valid: published.last_valid.clone(),
                    quota: published.quota,
                };
                let mut stopped = published.as_ref().clone();
                stopped.failure = Some(failure.clone());
                failures.push(failure);
                state.instances.insert_mut(id, Arc::new(stopped));
            } else {
                if result_changed {
                    changed.push(DurableChange {
                        instance: id,
                        consumer: published
                            .request
                            .as_ref()
                            .ok_or(DurableError::State)?
                            .consumer_id,
                        at: at.clone(),
                    });
                }
                state.instances.insert_mut(id, Arc::new(staged));
            }
        }
        state.at = at;
        self.prepare(state, changed, failures, DurableOperation::Source)
            .map(Some)
    }

    /// Clears only evidence covered by a freshly authorized catch-up boundary.
    ///
    /// # Errors
    /// Rejects unknown instances, authorization failures and future boundaries.
    pub fn prepare_acknowledge(
        &mut self,
        instance: u64,
        through: &E::Checkpoint,
        authorize: impl FnOnce(
            &SubscriptionRequest<I, E::Backend>,
        ) -> Result<(), DurableAuthorizationError>,
    ) -> Result<PreparedDurable<'_, E, I, DB>, DurableError> {
        let published = self
            .state
            .instances
            .get(&instance)
            .ok_or(DurableError::UnknownInstance(instance))?;
        authorize(published.request.as_deref().ok_or(DurableError::State)?)?;
        if through > &self.state.at {
            return Err(DurableError::FutureAcknowledgement);
        }
        let mut staged = published.as_ref().clone();
        if staged.unread.as_ref().is_some_and(|at| at <= through) {
            staged.unread = None;
        }
        let operation = DurableOperation::Acknowledge {
            instance,
            unread: staged.unread.clone(),
        };
        let mut state = self.state.clone();
        state.instances.insert_mut(instance, Arc::new(staged));
        self.prepare(state, Vec::new(), Vec::new(), operation)
    }

    /// Prepares an authorized removal without moving source progress.
    ///
    /// # Errors
    /// Rejects unknown instances and authorization failures.
    pub fn prepare_remove(
        &mut self,
        instance: u64,
        authorize: impl FnOnce(
            &SubscriptionRequest<I, E::Backend>,
        ) -> Result<(), DurableAuthorizationError>,
    ) -> Result<PreparedDurable<'_, E, I, DB>, DurableError> {
        let published = self
            .state
            .instances
            .get(&instance)
            .ok_or(DurableError::UnknownInstance(instance))?;
        authorize(published.request.as_deref().ok_or(DurableError::State)?)?;
        let mut state = self.state.clone();
        state.instances.remove_mut(&instance);
        self.prepare(
            state,
            Vec::new(),
            Vec::new(),
            DurableOperation::Remove { instance },
        )
    }

    fn prepare(
        &mut self,
        mut state: Snapshot<I, E::Backend, E::Checkpoint>,
        changed: Vec<DurableChange<I, E::Checkpoint>>,
        failures: Vec<DurableFailure<E::Checkpoint>>,
        operation: DurableOperation<E::Checkpoint>,
    ) -> Result<PreparedDurable<'_, E, I, DB>, DurableError> {
        state.revision = self
            .state
            .revision
            .checked_add(1)
            .ok_or(DurableError::RevisionExhausted)?;
        let commit = DurableCommit {
            expected_revision: self.state.revision,
            revision: state.revision,
            previous: self.state.at.clone(),
            at: state.at.clone(),
            checkpoint: postcard::to_allocvec(&state)?,
            changed,
            failures,
            operation,
        };
        Ok(PreparedDurable {
            engine: self,
            state,
            commit,
        })
    }

    /// Encodes complete published state for the caller's atomic storage transaction.
    ///
    /// # Errors
    /// Returns an encoding error if any backend scalar cannot be serialized.
    pub fn checkpoint(&self) -> Result<Vec<u8>, DurableError> {
        Ok(postcard::to_allocvec(&self.state)?)
    }

    /// The complete source boundary successfully published after persistence.
    #[must_use]
    pub const fn checkpoint_position(&self) -> &E::Checkpoint {
        &self.state.at
    }

    /// The storage revision required by the next atomic compare-and-swap.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.state.revision
    }

    /// The newest selected-result change not covered by acknowledgement.
    #[must_use]
    pub fn unread(&self, instance: u64) -> Option<&E::Checkpoint> {
        self.state.instances.get(&instance)?.unread.as_ref()
    }

    /// The terminal capacity failure, when this instance's wake guarantee has ended.
    #[must_use]
    pub fn failure(&self, instance: u64) -> Option<&DurableFailure<E::Checkpoint>> {
        self.state.instances.get(&instance)?.failure.as_ref()
    }

    /// The last boundary whose selected result this instance maintains exactly.
    #[must_use]
    pub fn last_valid(&self, instance: u64) -> Option<&E::Checkpoint> {
        Some(&self.state.instances.get(&instance)?.last_valid)
    }

    /// The selected result at the instance's last valid boundary.
    #[must_use]
    pub fn answer(&self, instance: u64) -> Option<&ReplayAnswer<E::Backend>> {
        Some(
            self.state
                .instances
                .get(&instance)?
                .detector
                .as_ref()?
                .answer(),
        )
    }

    /// The complete retained encoded instance size used for admission charging.
    ///
    /// # Errors
    /// Returns an encoding error if a backend scalar cannot be serialized.
    pub fn state_bytes(&self, instance: u64) -> Result<Option<usize>, DurableError> {
        self.state
            .instances
            .get(&instance)
            .map(|record| instance_bytes(record.as_ref()))
            .transpose()
    }
}

#[expect(
    clippy::result_large_err,
    reason = "returning the preparation preserves retryable ownership without allocation"
)]
impl<E: CdcEvent, I: IdTypes, DB: DatabaseLike> PreparedDurable<'_, E, I, DB>
where
    E::Backend: SqlLiteralParse,
{
    /// The immutable receipt to persist before publishing this preparation.
    #[must_use]
    pub const fn commit(&self) -> &DurableCommit<I, E::Checkpoint> {
        &self.commit
    }

    /// Publishes only after the callback atomically persists the complete receipt.
    ///
    /// # Errors
    /// Returns the storage error and unchanged preparation without publishing state.
    pub fn persist<P>(
        self,
        persist: impl FnOnce(&DurableCommit<I, E::Checkpoint>) -> Result<(), P>,
    ) -> Result<(), DurablePersistError<Self, P>> {
        if let Err(error) = persist(&self.commit) {
            return Err(DurablePersistError::Callback {
                error,
                preparation: self,
            });
        }
        self.engine.state = self.state;
        Ok(())
    }

    /// Publishes only after asynchronous persistence succeeds.
    ///
    /// # Errors
    /// Returns the storage error and unchanged preparation without publishing state.
    pub async fn persist_async<P>(
        self,
        persist: impl for<'a> core::ops::AsyncFnOnce(
            &'a DurableCommit<I, E::Checkpoint>,
        ) -> Result<(), P>,
    ) -> Result<(), DurablePersistError<Self, P>> {
        if let Err(error) = persist(&self.commit).await {
            return Err(DurablePersistError::Callback {
                error,
                preparation: self,
            });
        }
        self.engine.state = self.state;
        Ok(())
    }
}
