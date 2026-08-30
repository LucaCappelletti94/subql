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
//! [`crate::visibility::VisibilityPolicy::Error`] is failure to reach an answer, never an answer
//! of denied, so a transport failure is reported and nothing is invented. A
//! fabricated refusal would make clients delete rows they still hold.

pub(crate) mod errors;
pub(crate) mod open_fga_policy;

pub use errors::OpenFgaError;
pub use open_fga_policy::{OpenFgaPolicy, Reconciled, WithdrawnFact};
