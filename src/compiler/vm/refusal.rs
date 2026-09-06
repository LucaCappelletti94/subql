//! What an evaluation refuses, and the per-backend rules that decide it.
//!
//! Split out of `arithmetic` once a `LIKE` pattern could refuse too: the
//! cause a subscription is handed back is not an arithmetic fact.

/// Which arithmetic operation failed, as the statement spelled it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithmeticOp {
    /// `+`
    Add,
    /// `-`, binary
    Subtract,
    /// `*`
    Multiply,
    /// `-`, unary
    Negate,
    /// `/`
    Divide,
    /// `%`
    Modulo,
}

/// An evaluation the target engine refuses to answer.
///
/// Not a `Value::Null`: null composes through `OR` and would turn a refused
/// evaluation into a silent no-match, which is the divergence this removes.
/// Reported per subscription alongside the notifications instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvaluationRefusal {
    /// The result does not fit the integer type. Measured: PostgreSQL and
    /// MySQL raise `out of range`, while SQLite promotes the result to a
    /// real, which is an answer rather than a failure.
    IntegerOverflow {
        /// The operation whose result did not fit.
        operation: ArithmeticOp,
    },
    /// The divisor is zero. Measured: PostgreSQL raises `division by zero`
    /// for `/` and `%` alike and for every numeric type, while MySQL and
    /// SQLite answer `NULL`, which is unknown rather than a failure.
    DivisionByZero {
        /// The operator whose divisor was zero.
        operation: ArithmeticOp,
    },
    /// A `LIKE` pattern's last character is the escape character, so it
    /// escapes nothing, and the matcher reached it with input still to
    /// read. Measured: PostgreSQL raises exactly there, and answers false
    /// when the input ran out first, which is a no-match rather than this.
    LikePatternEndsWithEscape,
    /// An exact operand has to be cast to `double precision` to be
    /// compared against a float, and does not fit. Measured on PostgreSQL
    /// 16: `1e309::numeric > 1.5::float8` raises `out of range for type
    /// double precision` at either sign, while `1e300` is answered
    /// normally. PostgreSQL only, since MySQL's `DECIMAL` holds at most 65
    /// digits and SQLite compares through the exact rule instead.
    DecimalOutsideFloatRange,
}

/// What a backend answers when a divisor is zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DivisionByZero {
    /// Raise, which becomes a per-subscription evaluation failure.
    Fails,
    /// Answer SQL `NULL`, which composes to `Tri::Unknown`.
    IsNull,
}

/// What a backend answers when an integer operation overflows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntegerOverflow {
    /// Raise, which becomes a per-subscription evaluation failure.
    Fails,
    /// Carry the result as a float, which is SQLite's answer.
    PromotesToFloat,
}

/// What a backend answers for a `LIKE` pattern whose last character is the
/// escape character, so that it escapes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DanglingEscape {
    /// Raise once the matcher reaches it with input still to read, which is
    /// PostgreSQL's `LIKE pattern must not end with escape character`.
    Fails,
    /// Answer no-match, which is what MySQL answers whether or not input
    /// remains.
    NoMatch,
}

/// One engine's default `LIKE` escape, and what a dangling one does.
///
/// Together rather than as two constants: an engine with no default escape
/// cannot have a dangling one, and this makes that a type-level fact rather
/// than an unused second answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LikeEscape {
    /// The character that escapes the next character in a pattern.
    pub character: char,
    /// What a pattern ending with that character answers.
    pub dangling: DanglingEscape,
}
