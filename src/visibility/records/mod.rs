//! Which authorization records one changed row implies.
//!
//! `rls2fga-types` describes a relation's records as structure: a template
//! naming where the object key and the subject key come from, plus guards
//! the row must satisfy. Given a row's column values it evaluates that
//! description with no database. This module is the adapter that lets a
//! subql [`crate::visibility::RowView`] be those column values.
//!
//! # Refusing is part of the contract
//!
//! [`rls2fga_types::RowValues`] answers `None` for anything
//! it cannot read, and `records_from_row` turns a `None` object key into
//! an empty record set. That is correct when the row genuinely says
//! nothing, and wrong when this adapter simply cannot read the shape,
//! because "no records" reads as "this row grants nobody" and silently
//! withdraws access.
//!
//! So the adapter never guesses. [`records_from_row_view`] checks the
//! description against what a [`crate::visibility::RowView`] can answer and returns
//! [`RowRecordError::UnsupportedValueSource`] rather than an empty set.

use alloc::string::String;

use rls2fga_types::RecordError;

use crate::ValueError;

pub(crate) mod adapter;
pub(crate) mod entry_point;

pub(crate) use adapter::render_text;
pub use adapter::row_values;
pub use entry_point::{is_evaluable, records_from_row_view};

// Errors

/// Why a description could not be evaluated against one row view.
///
/// Every variant means "ask somebody else", never "this row grants
/// nobody". Collapsing any of them into an empty record set is a silent
/// withdrawal of access.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RowRecordError {
    /// `rls2fga-types` refused to produce records for this row.
    ///
    /// Wrapped whole rather than mapped arm by arm, because
    /// [`RecordError`] is `#[non_exhaustive]` and every arm of it is a
    /// refusal. A mapping would have to grow with each new reason, and
    /// the reason is the producer's to word.
    #[error(transparent)]
    Refused(#[from] RecordError),
    /// The description reads a column shape a [`crate::visibility::RowView`] cannot answer.
    ///
    /// Today that is only a list column: [`crate::backend::Value`] has no array variant,
    /// so an `= ANY(members)` shape cannot be expanded from a row image.
    #[error("a row view cannot answer a {0} column")]
    UnsupportedValueSource(&'static str),
    /// A cell the row carried could not be decoded, so what the row
    /// implies is not knowable.
    ///
    /// Distinct from a cell the source never carried, which is
    /// [`crate::backend::Value::Missing`] and genuinely says nothing. Reading a corrupt
    /// cell as no records would withdraw access the row may still grant.
    #[error(transparent)]
    Undecodable(#[from] ValueError),
    /// The description reads a column the row side cannot answer the way
    /// the loading SQL does.
    ///
    /// The loading SQL spells every kind through `::text`, while the row
    /// side spells only the kinds whose text form provably matches it, so
    /// serving such a shape would load records no changed row could ever
    /// produce or withdraw. The kind is the catalog's, not any row's, so
    /// this is refused once at setup through [`is_evaluable`] rather than
    /// rediscovered per row.
    #[error("a row view cannot read {0}")]
    UnreadableColumn(String),
}
