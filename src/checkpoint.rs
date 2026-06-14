//! Checkpoint trait and concrete impls.
//!
//! A [`Checkpoint`] is an opaque, ordered token that anchors a point in a
//! CDC stream. PostgreSQL uses an 8-byte LSN ([`PgLsn`]); MySQL uses a
//! `(file, position)` pair ([`MysqlBinlogPos`]); custom or unknown sources
//! use [`OpaqueCheckpoint`]. Engines pin one `C: Checkpoint` per
//! construction so events from a parser pinned to a different checkpoint
//! type are a compile-time error rather than a runtime mismatch.
//!
//! The [`NoCheckpoint`] marker exists for synthetic tests and contexts that
//! genuinely have no notion of position; production CDC code should use a
//! real impl.

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::fmt::Debug;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Marker trait satisfied by any opaque, ordered, serializable token that
/// labels a position in a CDC stream.
///
/// All required bounds:
/// * `Ord` so engines and oplogs can compare positions.
/// * `Clone`/`Debug` so checkpoints can flow through notifications.
/// * `Serialize` + `DeserializeOwned` so checkpoints can be persisted (a
///   client cursor on disk, an oplog table, an audit log) and restored.
/// * `Send + Sync + 'static` so checkpoints can cross threads and outlive
///   any specific scope.
///
/// No methods. Backends differ in **shape**, not in what subql does with
/// checkpoints; the trait is purely a marker.
pub trait Checkpoint:
    Ord + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

/// PostgreSQL Log Sequence Number. 64-bit, strictly increasing per WAL
/// record.
///
/// Wire formatted by PostgreSQL as `0/3A29C8`. The numeric value is the
/// 64-bit absolute byte position in the WAL stream; ordering is the
/// natural integer ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgLsn(pub u64);

impl Checkpoint for PgLsn {}

impl PgLsn {
    /// Parse a PostgreSQL hex LSN string like `"0/3A29C8"` into a [`PgLsn`].
    /// Returns `None` if the string does not match the expected shape.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let (hi, lo) = s.split_once('/')?;
        let hi = u32::from_str_radix(hi, 16).ok()?;
        let lo = u32::from_str_radix(lo, 16).ok()?;
        Some(Self((u64::from(hi) << 32) | u64::from(lo)))
    }
}

/// MySQL binary-log position. A `(file_id, position)` pair where `file_id`
/// is the numeric suffix of the binlog file name (e.g. `mysql-bin.000042`
/// has file id `42`).
///
/// Ordering is lexicographic on `(file, pos)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MysqlBinlogPos {
    /// Numeric suffix of the binlog file name.
    pub file: u32,
    /// Byte offset within the file.
    pub pos: u32,
}

impl Checkpoint for MysqlBinlogPos {}

/// Opaque escape hatch.
///
/// A backend-defined byte sequence with lexicographic ordering. Use this
/// when a backend's checkpoint does not fit the typed variants above
/// (custom WAL formats, in-tree experiments, third-party CDC sources).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpaqueCheckpoint(pub Vec<u8>);

impl PartialOrd for OpaqueCheckpoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OpaqueCheckpoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Checkpoint for OpaqueCheckpoint {}

/// Marker for "no checkpoint is meaningful in this context."
///
/// Use for synthetic unit tests that construct [`WalEvent`](crate::WalEvent)s
/// directly, or for in-memory pipelines that do not need replay / resume
/// semantics. Production CDC code should choose a real impl
/// ([`PgLsn`], [`MysqlBinlogPos`], or [`OpaqueCheckpoint`]).
///
/// All instances compare equal under `Ord` since there is no position to
/// order by. Treat this as a zero-information marker.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NoCheckpoint;

impl Checkpoint for NoCheckpoint {}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn pg_lsn_order_is_numeric() {
        assert!(PgLsn(5) < PgLsn(6));
        assert_eq!(PgLsn(7).cmp(&PgLsn(7)), Ordering::Equal);
    }

    #[test]
    fn pg_lsn_parses_hex_pair() {
        assert_eq!(PgLsn::parse("0/3A29C8"), Some(PgLsn(0x3A_29C8)));
        assert_eq!(
            PgLsn::parse("16/12345678"),
            Some(PgLsn((0x16 << 32) | 0x1234_5678))
        );
        assert!(PgLsn::parse("not-an-lsn").is_none());
        assert!(PgLsn::parse("0/").is_none());
    }

    #[test]
    fn mysql_binlog_pos_orders_by_file_then_pos() {
        let a = MysqlBinlogPos { file: 1, pos: 10 };
        let b = MysqlBinlogPos { file: 1, pos: 20 };
        let c = MysqlBinlogPos { file: 2, pos: 1 };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn opaque_checkpoint_orders_lexicographically() {
        let a = OpaqueCheckpoint(vec![1, 2, 3]);
        let b = OpaqueCheckpoint(vec![1, 2, 4]);
        let c = OpaqueCheckpoint(vec![2]);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn no_checkpoint_is_always_equal() {
        assert_eq!(NoCheckpoint.cmp(&NoCheckpoint), Ordering::Equal);
    }

    /// Compile-time assertion: every concrete impl satisfies the trait
    /// bound. The function body exists only to instantiate the bound.
    #[test]
    fn concrete_impls_satisfy_trait_bound() {
        fn assert_checkpoint<C: Checkpoint>() {}
        assert_checkpoint::<PgLsn>();
        assert_checkpoint::<MysqlBinlogPos>();
        assert_checkpoint::<OpaqueCheckpoint>();
        assert_checkpoint::<NoCheckpoint>();
    }
}
