use alloc::string::String;

use openfga_client::tonic::Code;
use rls2fga_types::ActionStatement;

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
    pub(super) const fn is_transport(code: Code) -> bool {
        matches!(
            code,
            Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
        )
    }
}
