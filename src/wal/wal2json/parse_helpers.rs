use alloc::string::ToString;
use alloc::vec::Vec;
use wal2json_events::{Action, ChangeV1, MessageV2};

use crate::types::EventKind;
use crate::wal::WalParseError;

pub(super) const fn v2_row_kind(action: Action) -> Option<EventKind> {
    match action {
        Action::Insert => Some(EventKind::Insert),
        Action::Update => Some(EventKind::Update),
        Action::Delete => Some(EventKind::Delete),
        Action::Truncate => Some(EventKind::Truncate),
        // Begin, Commit, and Message are transaction boundaries, not rows.
        Action::Begin | Action::Commit | Action::Message => None,
    }
}

pub(super) const fn v1_row_kind(change: &ChangeV1) -> Option<EventKind> {
    match change {
        ChangeV1::Insert { .. } => Some(EventKind::Insert),
        ChangeV1::Update { .. } => Some(EventKind::Update),
        ChangeV1::Delete { .. } => Some(EventKind::Delete),
        // Not a row change.
        ChangeV1::Message { .. } => None,
    }
}

/// Parse one wal2json v2 line into the row events subql dispatches.
///
/// Returns an empty vector for a transaction boundary (`B`, `C`, `M`) and a
/// single [`MessageV2`] for a row action (`I`, `U`, `D`, `T`).
///
/// # Errors
///
/// [`WalParseError::InvalidUtf8`] for non-UTF-8 input and
/// [`WalParseError::JsonError`] for malformed JSON.
pub fn parse_wal2json_v2(bytes: &[u8]) -> Result<Vec<MessageV2>, WalParseError> {
    let text =
        core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    let msg =
        wal2json_events::parse_v2(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
    Ok(if v2_row_kind(msg.action()).is_some() {
        alloc::vec![msg]
    } else {
        Vec::new()
    })
}

/// Parse a wal2json v1 transaction into one [`ChangeV1`] per row change.
///
/// Non-row changes are dropped.
///
/// # Errors
///
/// [`WalParseError::InvalidUtf8`] for non-UTF-8 input and
/// [`WalParseError::JsonError`] for malformed JSON.
pub fn parse_wal2json_v1(bytes: &[u8]) -> Result<Vec<ChangeV1>, WalParseError> {
    let text =
        core::str::from_utf8(bytes).map_err(|e| WalParseError::InvalidUtf8(e.to_string()))?;
    let txn =
        wal2json_events::parse_v1(text).map_err(|e| WalParseError::JsonError(e.to_string()))?;
    Ok(txn
        .change
        .into_iter()
        .filter(|c| v1_row_kind(c).is_some())
        .collect())
}
