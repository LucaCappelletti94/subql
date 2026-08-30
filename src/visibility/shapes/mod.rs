//! Everything one translation says about rows, resolved against one catalog
//! and built once.
//!
//! Three things read it. [`RowPolicy`](crate::visibility::policy::RowPolicy)
//! answers from the changed row where the schema decides the relation, a
//! terminal policy asks the authorization service about everything else, and
//! [`Shapes::diff`] reports what each changed row moved in that service's
//! store. All three need the same descriptions resolved against the same
//! catalog.
//!
//! # Why one index rather than three
//!
//! Each reader taking the catalog and the descriptions for itself lets a caller
//! hand one reader a catalog that disagrees with another's, and every record
//! then names rows that do not exist. Building once removes the argument rather
//! than documenting the hazard.
//!
//! # What the naming is for
//!
//! Asking the service anything about a row means naming that row as the model
//! names it, which is neither the table's name nor a function of it: the model
//! assigns a type, appending a suffix where two tables canonicalise alike, and
//! builds the object from the row's key. Only rls2fga knows that, and it
//! reports it through
//! [`Translation::row_naming`](rls2fga::translator::Translation::row_naming).
//! Reading it off a fact-shape instead is ambiguous, because a table whose
//! whole key is a foreign key is keyed identically by its own shape and by a
//! shape describing its parent.

mod required_parameter;
mod resolution;

pub use required_parameter::RequiredParameter;
pub(crate) use required_parameter::TableShapes;
pub use resolution::{Shapes, SharedShapes};
