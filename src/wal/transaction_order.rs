//! The place of each row event inside the transaction that carries it.
//!
//! Logical decoding sends whole transactions in commit order, each framed by
//! a begin that names its commit position and a commit. [`TransactionOrder`]
//! is the frame both Postgres readers keep, pgoutput and wal2json v2, and it
//! refuses a stream whose frames are out of place, since a row it cannot
//! place in a transaction has no position.

use alloc::string::String;

use crate::PgLsn;

/// A logical decoding stream whose transaction frames are out of place.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransactionOrderError {
    /// A transaction began before the open one committed.
    #[error("a transaction began before the open one committed")]
    NestedBegin,
    /// A row change arrived with no transaction open.
    #[error("a row change arrived outside a transaction")]
    RowOutsideTransaction,
    /// A commit arrived with no transaction open.
    #[error("a commit arrived outside a transaction")]
    CommitOutsideTransaction,
    /// A commit named a commit position other than the one its begin named.
    #[error("a transaction that began to commit at {began:?} committed at {committed:?}")]
    CommitMismatch {
        /// The commit position the begin named.
        began: PgLsn,
        /// The commit position the commit named.
        committed: PgLsn,
    },
    /// A message a stream decoded without streaming or two-phase commit never
    /// carries, so its rows could not be placed in commit order.
    #[error("unexpected `{0}` message in a stream without streaming or two-phase commit")]
    UnexpectedMessage(String),
}

/// The open transaction, if any, and how many row events it has placed.
///
/// `K` is what the begin says about the commit: a [`PgLsn`] for pgoutput, and
/// an optional one for wal2json, whose begin names it only under
/// `include-lsn`.
pub struct TransactionOrder<K> {
    open: Option<(K, u64)>,
}

impl<K: Copy> TransactionOrder<K> {
    pub const fn new() -> Self {
        Self { open: None }
    }

    /// Open the transaction whose begin named `commit`.
    pub const fn begin(&mut self, commit: K) -> Result<(), TransactionOrderError> {
        if self.open.is_some() {
            return Err(TransactionOrderError::NestedBegin);
        }
        self.open = Some((commit, 0));
        Ok(())
    }

    /// Place the next row event: the open transaction's commit and the event's
    /// ordinal in it, counted from 1.
    pub fn next_row(&mut self) -> Result<(K, u64), TransactionOrderError> {
        let (commit, rows) = self
            .open
            .as_mut()
            .ok_or(TransactionOrderError::RowOutsideTransaction)?;
        *rows += 1;
        Ok((*commit, *rows))
    }

    /// Close the open transaction: its commit and how many row events it
    /// placed.
    pub fn commit(&mut self) -> Result<(K, u64), TransactionOrderError> {
        self.open
            .take()
            .ok_or(TransactionOrderError::CommitOutsideTransaction)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{TransactionOrder, TransactionOrderError};

    #[test]
    fn rows_count_from_one_within_each_transaction() {
        let mut order = TransactionOrder::new();
        order.begin(10).unwrap();
        assert_eq!(order.next_row(), Ok((10, 1)));
        assert_eq!(order.next_row(), Ok((10, 2)));
        assert_eq!(order.commit(), Ok((10, 2)));
        order.begin(20).unwrap();
        assert_eq!(order.next_row(), Ok((20, 1)));
        assert_eq!(order.commit(), Ok((20, 1)));
    }

    #[test]
    fn a_transaction_without_rows_commits_with_none_placed() {
        let mut order = TransactionOrder::new();
        order.begin(10).unwrap();
        assert_eq!(order.commit(), Ok((10, 0)));
    }

    #[test]
    fn frames_out_of_place_are_refused() {
        let mut order = TransactionOrder::<u64>::new();
        assert_eq!(
            order.next_row(),
            Err(TransactionOrderError::RowOutsideTransaction)
        );
        assert_eq!(
            order.commit(),
            Err(TransactionOrderError::CommitOutsideTransaction)
        );
        order.begin(10).unwrap();
        assert_eq!(order.begin(20), Err(TransactionOrderError::NestedBegin));
        // The refused begin left the open transaction as it was.
        assert_eq!(order.next_row(), Ok((10, 1)));
    }
}
