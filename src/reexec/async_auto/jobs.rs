//! What one triggered query needs from the database, and what came back.
//!
//! The async resolve runs in three phases, and these are the types that cross
//! the boundaries between them with every borrow of the engine resolved.

use super::{Arc, Backend, ReExecError, Value, Vec};

/// What one triggered query needs from the database, with every borrow of the
/// engine already resolved.
///
/// The async resolve runs in three phases: plan against a snapshot (needs
/// `&mut self`), read concurrently (needs only shared borrows), install and
/// deliver (needs `&mut self` again, between awaits). This type is what
/// crosses the first boundary, so anything the read needs from engine state
/// is owned by the time it is built. Pending keys are copied, never taken,
/// so a dropped or failed read loses nothing.
pub type KeyedRows<B> = Vec<(Vec<Value<B>>, Vec<Value<B>>)>;

/// One queued read paired with the job phase one planned for it.
pub type PlannedJob<I, C, B> = (crate::reexec::ReExecutionTrigger<I, C, B>, ResolveJob<B>);

/// One concurrent read's outcome, entering phase three. The trigger travels
/// with the failure too, so phase three can drop a read whose failure is
/// not retryable.
pub type ReadOutcome<I, C, B, E> = Result<
    (crate::reexec::ReExecutionTrigger<I, C, B>, Resolved<B>),
    (crate::reexec::ReExecutionTrigger<I, C, B>, ReExecError<E>),
>;

/// Every outcome of one concurrent drain iteration, in completion order.
pub type ReadOutcomes<I, C, B, E> = Vec<ReadOutcome<I, C, B, E>>;

pub enum ResolveJob<B: Backend> {
    /// A scalar the connector reads in one call.
    Scalar {
        query: crate::reexec::BoundQuery<B>,
        column_kind: crate::backend::ScalarFamily,
    },
    /// One grouped extreme and its source-row count.
    GroupedScalar {
        group: Vec<u8>,
        query: crate::reexec::BoundQuery<B>,
    },
    /// Rows for the keys that changed, read scoped to those keys. Boxed: this
    /// variant carries a parsed statement, and the others carry a string.
    Keyed(alloc::boxed::Box<KeyedJob<B>>),
    /// The whole result, paged. The generation is taken when the job is built,
    /// so a read that fails part way cannot let a later one reuse it.
    Whole {
        query: crate::reexec::BoundQuery<B>,
        generation: u64,
    },
}

/// The keyed tier's read, as planned.
pub struct KeyedJob<B: Backend> {
    pub plan: Arc<crate::reexec::plan::KeyedPlan>,
    pub keys: Vec<Vec<Value<B>>>,
    pub query: crate::reexec::BoundQuery<B>,
    /// Keys one statement may name, carried so the read needs nothing from the
    /// engine once it is planned.
    pub max_keys: usize,
}

/// What the database answered, still owned, ready to install.
pub enum Resolved<B: Backend> {
    Scalar(Value<B>),
    GroupedScalar {
        group: Vec<u8>,
        row: Vec<Value<B>>,
    },
    Keyed {
        keys: Vec<Vec<Value<B>>>,
        columns: Vec<alloc::string::String>,
        present: KeyedRows<B>,
    },

    /// A whole re-read whose pages already streamed to the sink from the
    /// concurrent phase. Nothing is installed for it.
    WholeStreamed,
}
/// One page of a whole re-read buffered for a snapshot answer, which returns
/// the whole result by contract.
pub struct ReadPage<B: Backend> {
    pub columns: Vec<alloc::string::String>,
    pub rows: Vec<Vec<Value<B>>>,
}
