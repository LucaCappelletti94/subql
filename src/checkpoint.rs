//! Checkpoint trait and concrete impls.
//!
//! A [`Checkpoint`] is an opaque, ordered token that anchors a point in a
//! CDC stream. PostgreSQL events and reads use a [`PgCommitPosition`], which
//! orders by commit. MySQL uses a `(file, position)` pair ([`MysqlBinlogPos`]).
//! Custom or unknown sources use [`OpaqueCheckpoint`]. Engines pin one
//! `C: Checkpoint` per construction so events from a parser pinned to a
//! different checkpoint type are a compile-time error rather than a runtime
//! mismatch.
//!
//! The [`NoCheckpoint`] marker exists for synthetic tests and contexts that
//! genuinely have no notion of position. Production CDC code should use a
//! real impl.

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::fmt::Debug;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// An opaque, ordered, serializable token that labels a position in a CDC
/// stream.
///
/// All required bounds:
/// * `Ord` so engines and oplogs can compare positions.
/// * `Clone`/`Debug` so checkpoints can flow through notifications.
/// * `Serialize` + `DeserializeOwned` so checkpoints can be persisted (a
///   client cursor on disk, an oplog table, an audit log) and restored.
/// * `Send + Sync + 'static` so checkpoints can cross threads and outlive
///   any specific scope.
///
/// Backends differ in **shape**, not in what subql does with checkpoints.
/// The one thing a checkpoint spells itself is its byte form, which is what
/// [`SubscriptionEngine::advance_cursor`](crate::SubscriptionEngine::advance_cursor)
/// compares. A serde format with variable-width integers does not sort in
/// value order as bytes, so a cursor is installed through
/// [`Self::to_opaque`] and read back through [`Self::from_opaque`].
pub trait Checkpoint:
    Ord + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The checkpoint as bytes that sort as the checkpoint does, so
    /// `a.cmp(&b) == a.to_opaque().cmp(&b.to_opaque())` for any two checkpoints.
    #[must_use]
    fn to_opaque(&self) -> OpaqueCheckpoint;

    /// The checkpoint [`Self::to_opaque`] encoded, or `None` for bytes it
    /// never produces.
    #[must_use]
    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self>;
}

/// The bytes of `opaque` when there are exactly `N` of them.
fn exact<const N: usize>(opaque: &OpaqueCheckpoint) -> Option<[u8; N]> {
    opaque.0.as_slice().try_into().ok()
}

/// PostgreSQL Log Sequence Number. 64-bit, strictly increasing per WAL
/// record.
///
/// Wire formatted by PostgreSQL as `0/3A29C8`. The numeric value is the
/// 64-bit absolute byte position in the WAL stream. Ordering is the
/// natural integer ordering.
///
/// A WAL address, which is what a replication slot's flush position and a
/// source's acknowledged position are. A row change's own record position does
/// not follow commit order, so Postgres events and reads carry a
/// [`PgCommitPosition`] instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgLsn(pub u64);

impl Checkpoint for PgLsn {
    fn to_opaque(&self) -> OpaqueCheckpoint {
        OpaqueCheckpoint(self.0.to_be_bytes().to_vec())
    }

    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self> {
        exact(opaque).map(|bytes| Self(u64::from_be_bytes(bytes)))
    }
}

impl PgLsn {
    /// Parse a PostgreSQL hex LSN string like `"0/3A29C8"` into a [`PgLsn`].
    /// Returns `None` if the string does not match the expected shape.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::PgLsn;
    ///
    /// let lsn = PgLsn::parse("0/3A29C8").unwrap();
    /// assert_eq!(lsn, PgLsn(0x003A_29C8));
    /// assert!(PgLsn::parse("not-an-lsn").is_none());
    /// ```
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let (hi, lo) = s.split_once('/')?;
        let hi = u32::from_str_radix(hi, 16).ok()?;
        let lo = u32::from_str_radix(lo, 16).ok()?;
        Some(Self((u64::from(hi) << 32) | u64::from(lo)))
    }
}

/// Where a Postgres row change falls in commit order.
///
/// Ordered by the commit position of the change's transaction, the start of
/// its commit record, and then by the change's ordinal within that
/// transaction. Logical decoding delivers whole transactions in commit order,
/// so the positions of delivered changes strictly increase, including when an
/// older transaction commits after a newer one.
///
/// The ordinal counts a transaction's row events from 1 in message order.
/// Ordinal 0 is the position of a read, see [`Self::before_commit`].
///
/// # Examples
///
/// ```
/// use subql::{PgCommitPosition, PgLsn};
///
/// // T1 wrote at 1000 and committed at 1500, T2 wrote at 1200 and committed at 1300.
/// let t2_row = PgCommitPosition::new(PgLsn(1300), 1);
/// let t1_first = PgCommitPosition::new(PgLsn(1500), 1);
/// let t1_second = PgCommitPosition::new(PgLsn(1500), 2);
/// assert!(t2_row < t1_first && t1_first < t1_second);
///
/// // A read at 1400 reflects T2 and not T1.
/// let read = PgCommitPosition::before_commit(PgLsn(1400));
/// assert!(t2_row < read && read < t1_first);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgCommitPosition {
    commit_lsn: PgLsn,
    ordinal: u64,
}

impl Checkpoint for PgCommitPosition {
    fn to_opaque(&self) -> OpaqueCheckpoint {
        OpaqueCheckpoint([self.commit_lsn.0.to_be_bytes(), self.ordinal.to_be_bytes()].concat())
    }

    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self> {
        let bytes: [u8; 16] = exact(opaque)?;
        let (commit_lsn, ordinal) = bytes.split_at(8);
        Some(Self::new(
            PgLsn(u64::from_be_bytes(commit_lsn.try_into().ok()?)),
            u64::from_be_bytes(ordinal.try_into().ok()?),
        ))
    }
}

impl PgCommitPosition {
    /// The `ordinal`-th row event of the transaction whose commit record
    /// starts at `commit_lsn`.
    #[must_use]
    pub const fn new(commit_lsn: PgLsn, ordinal: u64) -> Self {
        Self {
            commit_lsn,
            ordinal,
        }
    }

    /// The position of a read taken at WAL position `lsn`, as
    /// `pg_current_wal_lsn()` reports it.
    ///
    /// Orders after every change of a transaction that committed before `lsn`
    /// and before every change of one committing at `lsn` or later.
    #[must_use]
    pub const fn before_commit(lsn: PgLsn) -> Self {
        Self::new(lsn, 0)
    }

    /// Start of the commit record of the change's transaction.
    #[must_use]
    pub const fn commit_lsn(self) -> PgLsn {
        self.commit_lsn
    }

    /// The change's place in its transaction, from 1, or 0 for a read.
    #[must_use]
    pub const fn ordinal(self) -> u64 {
        self.ordinal
    }
}

/// MySQL binary-log position. A `(file_id, position)` pair where `file_id`
/// is the numeric suffix of the binlog file name (e.g. `mysql-bin.000042`
/// has file id `42`).
///
/// Ordering is lexicographic on `(file, pos)`.
///
/// # Examples
///
/// ```
/// use subql::MysqlBinlogPos;
///
/// let a = MysqlBinlogPos { file: 42, pos: 100 };
/// let b = MysqlBinlogPos { file: 42, pos: 200 };
/// let c = MysqlBinlogPos { file: 43, pos: 50 };
/// assert!(a < b);
/// assert!(b < c);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MysqlBinlogPos {
    /// Numeric suffix of the binlog file name.
    pub file: u32,
    /// Byte offset within the file.
    pub pos: u32,
}

impl Checkpoint for MysqlBinlogPos {
    fn to_opaque(&self) -> OpaqueCheckpoint {
        OpaqueCheckpoint([self.file.to_be_bytes(), self.pos.to_be_bytes()].concat())
    }

    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self> {
        let bytes: [u8; 8] = exact(opaque)?;
        let (file, pos) = bytes.split_at(4);
        Some(Self {
            file: u32::from_be_bytes(file.try_into().ok()?),
            pos: u32::from_be_bytes(pos.try_into().ok()?),
        })
    }
}

/// Opaque escape hatch.
///
/// A backend-defined byte sequence with lexicographic ordering. Use this
/// when a backend's checkpoint does not fit the typed variants above
/// (custom WAL formats, in-tree experiments, third-party CDC sources).
///
/// # Examples
///
/// ```
/// use subql::OpaqueCheckpoint;
///
/// let early = OpaqueCheckpoint(vec![0x00, 0x10]);
/// let later = OpaqueCheckpoint(vec![0x00, 0x20]);
/// assert!(early < later);
/// ```
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

impl Checkpoint for OpaqueCheckpoint {
    fn to_opaque(&self) -> OpaqueCheckpoint {
        self.clone()
    }

    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self> {
        Some(opaque.clone())
    }
}

/// Marker for "no checkpoint is meaningful in this context."
///
/// Use for synthetic unit tests that construct events with no source
/// position, or for in-memory pipelines that do not need replay /
/// resume semantics. Production CDC code should choose a real impl
/// ([`PgCommitPosition`], [`MysqlBinlogPos`], or [`OpaqueCheckpoint`]).
///
/// All instances compare equal under `Ord` since there is no position to
/// order by. Treat this as a zero-information marker.
///
/// # Examples
///
/// ```
/// use subql::NoCheckpoint;
///
/// assert_eq!(NoCheckpoint, NoCheckpoint);
/// let _: NoCheckpoint = NoCheckpoint::default();
/// ```
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NoCheckpoint;

impl Checkpoint for NoCheckpoint {
    fn to_opaque(&self) -> OpaqueCheckpoint {
        OpaqueCheckpoint(Vec::new())
    }

    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self> {
        opaque.0.is_empty().then_some(Self)
    }
}

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

    /// Every pair in `sorted` compares as bytes as it does as values, and
    /// every value reads back from its bytes.
    fn assert_bytes_sort_as_values<C: Checkpoint>(sorted: &[C]) {
        for (i, a) in sorted.iter().enumerate() {
            assert_eq!(C::from_opaque(&a.to_opaque()).as_ref(), Some(a));
            for b in &sorted[i..] {
                assert_eq!(
                    a.to_opaque().cmp(&b.to_opaque()),
                    a.cmp(b),
                    "{a:?} against {b:?}"
                );
            }
        }
    }

    /// The pairs a variable-width integer encoding sorts backwards as bytes,
    /// such as 255 against 256, each side of every field.
    #[test]
    fn byte_form_sorts_as_the_checkpoint_does() {
        let edges = [0u64, 1, 127, 128, 255, 256, 16_383, 16_384, u64::MAX];
        let lsns: Vec<PgLsn> = edges.iter().map(|&e| PgLsn(e)).collect();
        assert_bytes_sort_as_values(&lsns);

        let mut positions: Vec<PgCommitPosition> = edges
            .iter()
            .flat_map(|&c| {
                edges
                    .iter()
                    .map(move |&o| PgCommitPosition::new(PgLsn(c), o))
            })
            .collect();
        positions.sort_unstable();
        assert_bytes_sort_as_values(&positions);

        let narrow = [0u32, 1, 127, 128, 255, 256, u32::MAX];
        let mut binlog: Vec<MysqlBinlogPos> = narrow
            .iter()
            .flat_map(|&file| narrow.iter().map(move |&pos| MysqlBinlogPos { file, pos }))
            .collect();
        binlog.sort_unstable();
        assert_bytes_sort_as_values(&binlog);

        assert_bytes_sort_as_values(&[
            OpaqueCheckpoint(vec![]),
            OpaqueCheckpoint(vec![0]),
            OpaqueCheckpoint(vec![0, 255]),
            OpaqueCheckpoint(vec![1]),
        ]);
        assert_bytes_sort_as_values(&[NoCheckpoint]);
    }

    /// Bytes of another width decode to nothing.
    #[test]
    fn byte_form_of_another_width_is_refused() {
        let lsn = PgLsn(7).to_opaque();
        let position = PgCommitPosition::new(PgLsn(7), 1).to_opaque();
        assert_eq!(PgCommitPosition::from_opaque(&lsn), None);
        assert_eq!(PgLsn::from_opaque(&position), None);
        assert_eq!(MysqlBinlogPos::from_opaque(&position), None);
        assert_eq!(NoCheckpoint::from_opaque(&lsn), None);
    }
}
