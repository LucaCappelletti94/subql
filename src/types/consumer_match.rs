use super::{EvaluationFailure, IdTypes, UnansweredCell};
use crate::checkpoint::{Checkpoint, NoCheckpoint};
use alloc::vec::Vec;

/// One consumer's view-relative row transitions, per-subscription diagnostics and event checkpoint.
pub struct ConsumerMatch<I: IdTypes, C: Checkpoint = NoCheckpoint> {
    pub(crate) inserted: bool,
    pub(crate) deleted: bool,
    pub(crate) updated: bool,
    checkpoint: Option<C>,
    pub(crate) evaluation_failures: Vec<EvaluationFailure<I>>,
    pub(crate) unanswered: Vec<UnansweredCell<I>>,
}

impl<I: IdTypes, C: Checkpoint> ConsumerMatch<I, C> {
    pub(crate) const fn empty() -> Self {
        Self {
            inserted: false,
            deleted: false,
            updated: false,
            checkpoint: None,
            evaluation_failures: Vec::new(),
            unanswered: Vec::new(),
        }
    }

    pub(crate) fn with_checkpoint(mut self, checkpoint: Option<C>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Whether the row entered any of the consumer's subscribed views.
    #[must_use]
    pub const fn inserted(&self) -> bool {
        self.inserted
    }

    /// Whether the row left any of the consumer's subscribed views.
    #[must_use]
    pub const fn deleted(&self) -> bool {
        self.deleted
    }

    /// Whether the row changed within any of the consumer's subscribed views.
    #[must_use]
    pub const fn updated(&self) -> bool {
        self.updated
    }

    /// The originating event's position, when known.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&C> {
        self.checkpoint.as_ref()
    }

    /// The consumer's subscriptions whose predicates could not be evaluated.
    #[must_use]
    pub fn evaluation_failures(&self) -> &[EvaluationFailure<I>] {
        &self.evaluation_failures
    }

    /// The consumer's subscriptions whose answers need cells absent from the event.
    #[must_use]
    pub fn unanswered(&self) -> &[UnansweredCell<I>] {
        &self.unanswered
    }
}

impl<I: IdTypes, C: Checkpoint> core::fmt::Debug for ConsumerMatch<I, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConsumerMatch")
            .field("inserted", &self.inserted)
            .field("deleted", &self.deleted)
            .field("updated", &self.updated)
            .field("checkpoint", &self.checkpoint)
            .field("evaluation_failure_count", &self.evaluation_failures.len())
            .field("unanswered_count", &self.unanswered.len())
            .finish()
    }
}
