//! Checkpoint trait and concrete impls.
//!
//! A [`Checkpoint`] is an opaque, ordered token that anchors a point in a
//! CDC stream. PostgreSQL events use a [`PgCommitPosition`], which orders by
//! commit and names the transaction. MySQL uses a `(file, position)` pair
//! ([`MysqlBinlogPos`]). Custom or unknown sources use [`OpaqueCheckpoint`].
//! Engines pin one `C: Checkpoint` per construction so events from a parser
//! pinned to a different checkpoint type are a compile-time error rather than
//! a runtime mismatch.
//!
//! A database read reports a [`Checkpoint::Fence`] instead of a position, and
//! every change is judged against it with [`Checkpoint::seen_by`]. On
//! PostgreSQL the fence is the read's own snapshot ([`PgSnapshotFence`]),
//! since no stream position taken beside a snapshot says which commits it
//! sees.
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
    /// What a database read reports about the stream, so every change can be
    /// judged as inside or outside the rows it returned.
    type Fence: Clone + Debug + PartialEq + Send + Sync + 'static;

    /// Whether the read behind `fence` already holds the change at this
    /// position.
    #[must_use]
    fn seen_by(&self, fence: &Self::Fence) -> Seen;

    /// The checkpoint as bytes that sort as the checkpoint does, so
    /// `a.cmp(&b) == a.to_opaque().cmp(&b.to_opaque())` for any two checkpoints.
    #[must_use]
    fn to_opaque(&self) -> OpaqueCheckpoint;

    /// The checkpoint [`Self::to_opaque`] encoded, or `None` for bytes it
    /// never produces.
    #[must_use]
    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self>;
}

/// How a change stands against the fence of a database read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Seen {
    /// The read's rows already reflect the change, so applying it again
    /// would count it twice.
    Held,
    /// The read's rows lack the change, and a later change may still be one
    /// the read holds.
    Missed,
    /// The read's rows lack the change and every change after it in the
    /// stream, so the fence has nothing left to decide.
    Beyond,
}

/// The fence of the read a value was last set from, while it still decides
/// anything.
pub(crate) struct ReadFence<C: Checkpoint>(Option<C::Fence>);

impl<C: Checkpoint> ReadFence<C> {
    pub(crate) const fn new(fence: Option<C::Fence>) -> Self {
        Self(fence)
    }

    pub(crate) const fn none() -> Self {
        Self(None)
    }

    /// Whether the change at `at` belongs on top of the read's numbers.
    ///
    /// A change past the fence retires it. So does one without a position,
    /// since a stream that names no positions cannot be fenced.
    pub(crate) fn admits(&mut self, at: Option<&C>) -> bool {
        let Some(fence) = &self.0 else {
            return true;
        };
        match at.map(|at| at.seen_by(fence)) {
            Some(Seen::Held) => false,
            Some(Seen::Missed) => true,
            Some(Seen::Beyond) | None => {
                self.0 = None;
                true
            }
        }
    }
}

/// Changes applied to a value that no fence of its own reads has shown the
/// database holds yet, so a later read that still misses one can have it
/// applied on top of its answer.
///
/// Visibility only grows, so an entry a fence holds is held by every later
/// snapshot and leaves for good.
pub(crate) struct UnseenLog<C: Checkpoint, T> {
    pub(crate) entries: Vec<(C, T)>,
    /// Set when entries had to be forgotten, after which no read of the value
    /// can be completed exactly and the next install asks again.
    pub(crate) overflowed: bool,
}

impl<C: Checkpoint, T> UnseenLog<C, T> {
    pub(crate) const fn new() -> Self {
        Self {
            entries: Vec::new(),
            overflowed: false,
        }
    }

    pub(crate) fn push(&mut self, at: C, entry: T, cap: usize) {
        if self.overflowed {
            return;
        }
        if self.entries.len() >= cap {
            self.overflowed = true;
            self.entries = Vec::new();
            return;
        }
        self.entries.push((at, entry));
    }

    pub(crate) fn forget_held(&mut self, fence: &C::Fence) {
        self.entries
            .retain(|(at, _)| at.seen_by(fence) != Seen::Held);
    }

    /// Past half the cap, so a fence-only read can trim it before it
    /// overflows.
    pub(crate) const fn wants_fence(&self, cap: usize) -> bool {
        !self.overflowed && self.entries.len() >= cap.div_ceil(2)
    }

    pub(crate) const fn is_empty(&self) -> bool {
        !self.overflowed && self.entries.is_empty()
    }
}

impl<C: Checkpoint, T> Default for UnseenLog<C, T> {
    fn default() -> Self {
        Self::new()
    }
}

/// `Held` at or before a position fence, `Beyond` after it.
fn by_position<C: Ord>(at: &C, fence: &C) -> Seen {
    if at <= fence {
        Seen::Held
    } else {
        Seen::Beyond
    }
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
/// not follow commit order, so Postgres events carry a [`PgCommitPosition`]
/// instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgLsn(pub u64);

impl Checkpoint for PgLsn {
    type Fence = Self;

    fn seen_by(&self, fence: &Self) -> Seen {
        by_position(self, fence)
    }

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

/// A PostgreSQL transaction id as logical decoding reports it, 32 bits and
/// without the epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgXid(pub u32);

impl PgXid {
    /// `InvalidTransactionId`, which names no transaction.
    pub const INVALID: Self = Self(0);
}

/// Where a Postgres row change, or the commit that ends its transaction,
/// falls in commit order, and which transaction made it.
///
/// Ordered by the commit position of the change's transaction, the start of
/// its commit record, then by its transaction id, then by the change's
/// ordinal within that transaction. One commit record ends one transaction,
/// so the transaction id never reorders two changes. Logical decoding
/// delivers whole transactions in commit order, so the positions of delivered
/// changes strictly increase, including when an older transaction commits
/// after a newer one.
///
/// The ordinal counts a transaction's row events from 1 in message order.
/// Ordinal 0 with [`PgXid::INVALID`] is a resume point ahead of a commit, see
/// [`Self::before_commit`], and `u64::MAX` is the position of the commit
/// itself, see [`Self::at_commit`].
///
/// # Examples
///
/// ```
/// use subql::{PgCommitPosition, PgLsn, PgXid};
///
/// // T1 wrote at 1000 and committed at 1500, T2 wrote at 1200 and committed at 1300.
/// let (t1, t2) = (PgXid(740), PgXid(741));
/// let t2_row = PgCommitPosition::new(PgLsn(1300), t2, 1);
/// let t1_first = PgCommitPosition::new(PgLsn(1500), t1, 1);
/// let t1_second = PgCommitPosition::new(PgLsn(1500), t1, 2);
/// assert!(t2_row < t1_first && t1_first < t1_second);
///
/// // Resuming at 1400 replays T1 and not T2.
/// let resume = PgCommitPosition::before_commit(PgLsn(1400));
/// assert!(t2_row < resume && resume < t1_first);
///
/// // T1's commit follows its rows, and a transaction committing later follows it.
/// let t1_commit = PgCommitPosition::at_commit(PgLsn(1500), t1);
/// assert!(t1_second < t1_commit && t1_commit < PgCommitPosition::before_commit(PgLsn(1501)));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgCommitPosition {
    commit_lsn: PgLsn,
    xid: PgXid,
    ordinal: u64,
}

impl Checkpoint for PgCommitPosition {
    type Fence = PgSnapshotFence;

    fn seen_by(&self, fence: &PgSnapshotFence) -> Seen {
        if self.commit_lsn >= fence.insert_lsn {
            Seen::Beyond
        } else if fence.sees(self.xid) {
            Seen::Held
        } else {
            Seen::Missed
        }
    }

    fn to_opaque(&self) -> OpaqueCheckpoint {
        let mut bytes = Vec::with_capacity(20);
        bytes.extend_from_slice(&self.commit_lsn.0.to_be_bytes());
        bytes.extend_from_slice(&self.xid.0.to_be_bytes());
        bytes.extend_from_slice(&self.ordinal.to_be_bytes());
        OpaqueCheckpoint(bytes)
    }

    fn from_opaque(opaque: &OpaqueCheckpoint) -> Option<Self> {
        let bytes: [u8; 20] = exact(opaque)?;
        let (commit_lsn, rest) = bytes.split_at(8);
        let (xid, ordinal) = rest.split_at(4);
        Some(Self::new(
            PgLsn(u64::from_be_bytes(commit_lsn.try_into().ok()?)),
            PgXid(u32::from_be_bytes(xid.try_into().ok()?)),
            u64::from_be_bytes(ordinal.try_into().ok()?),
        ))
    }
}

impl PgCommitPosition {
    /// The `ordinal`-th row event of transaction `xid`, whose commit record
    /// starts at `commit_lsn`.
    #[must_use]
    pub const fn new(commit_lsn: PgLsn, xid: PgXid, ordinal: u64) -> Self {
        Self {
            commit_lsn,
            xid,
            ordinal,
        }
    }

    /// A resume point at WAL position `lsn`.
    ///
    /// Orders after every change of a transaction that committed before `lsn`
    /// and before every change of one committing at `lsn` or later.
    #[must_use]
    pub const fn before_commit(lsn: PgLsn) -> Self {
        Self::new(lsn, PgXid::INVALID, 0)
    }

    /// The commit of transaction `xid`, whose commit record starts at
    /// `commit_lsn`, after every row of it and before every later transaction.
    #[must_use]
    pub const fn at_commit(commit_lsn: PgLsn, xid: PgXid) -> Self {
        Self::new(commit_lsn, xid, u64::MAX)
    }

    /// Start of the commit record of the change's transaction.
    #[must_use]
    pub const fn commit_lsn(self) -> PgLsn {
        self.commit_lsn
    }

    /// The transaction the change belongs to, or [`PgXid::INVALID`] for a
    /// resume point.
    #[must_use]
    pub const fn xid(self) -> PgXid {
        self.xid
    }

    /// The change's place in its transaction, from 1, or 0 for a resume point
    /// and `u64::MAX` for the commit.
    #[must_use]
    pub const fn ordinal(self) -> u64 {
        self.ordinal
    }
}

/// What a PostgreSQL read saw: its snapshot, and the WAL insert position
/// read after the snapshot was taken.
///
/// A change is held by the read exactly when the snapshot sees its
/// transaction. A position taken beside the snapshot cannot say that. A
/// commit landing between the position and the snapshot is seen though it
/// comes after the position, and a commit whose record is flushed but which
/// still waits for a synchronous standby is unseen though it comes before.
///
/// The insert position bounds the fence. A transaction the snapshot sees left
/// the running set before the snapshot was taken, after inserting its commit
/// record, so every such record starts before `insert_lsn`. A change
/// committing at or past it is therefore [`Seen::Beyond`], and so is every
/// change after it.
///
/// # Examples
///
/// ```
/// use subql::{Checkpoint, PgCommitPosition, PgLsn, PgSnapshotFence, PgXid, Seen};
///
/// // Transaction 742 was still running when the read took its snapshot.
/// let fence = PgSnapshotFence::parse("740:745:742", PgLsn(2000)).unwrap();
/// let seen = |xid, lsn| PgCommitPosition::new(PgLsn(lsn), PgXid(xid), 1).seen_by(&fence);
///
/// assert_eq!(seen(741, 1500), Seen::Held);
/// assert_eq!(seen(742, 1500), Seen::Missed);
/// assert_eq!(seen(745, 1900), Seen::Missed);
/// assert_eq!(seen(742, 2000), Seen::Beyond);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PgSnapshotFence {
    xmin: u64,
    xmax: u64,
    /// Sorted, each in `xmin..xmax`.
    running: Vec<u64>,
    insert_lsn: PgLsn,
}

impl PgSnapshotFence {
    /// The fence of a snapshot in `pg_snapshot` text form, `xmin:xmax:xip,…`,
    /// as `pg_current_snapshot()::text` prints it, with the insert position
    /// read after it. `None` for text of another shape.
    #[must_use]
    pub fn parse(snapshot: &str, insert_lsn: PgLsn) -> Option<Self> {
        let mut parts = snapshot.split(':');
        let xmin = parts.next()?.parse().ok()?;
        let xmax = parts.next()?.parse().ok()?;
        let list = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        let mut running = if list.is_empty() {
            Vec::new()
        } else {
            list.split(',')
                .map(|xid| xid.parse().ok())
                .collect::<Option<Vec<u64>>>()?
        };
        running.sort_unstable();
        let bounded = xmin <= xmax && running.iter().all(|xid| (xmin..xmax).contains(xid));
        bounded.then_some(Self {
            xmin,
            xmax,
            running,
            insert_lsn,
        })
    }

    /// The WAL insert position read after the snapshot.
    #[must_use]
    pub const fn insert_lsn(&self) -> PgLsn {
        self.insert_lsn
    }

    /// Whether the snapshot sees the committed transaction `xid`.
    ///
    /// The 32-bit id is placed in the epoch that puts it nearest `xmax`.
    /// Only a change committing before `insert_lsn` is asked about, and
    /// Postgres keeps every such transaction within 2^31 ids of the next one.
    fn sees(&self, xid: PgXid) -> bool {
        if xid == PgXid::INVALID {
            return false;
        }
        let epochless = u32::try_from(self.xmax & u64::from(u32::MAX))
            .expect("masking to 32 bits leaves an id without its epoch");
        // The wrapped distance read as signed is the nearest one, in either direction.
        let distance = xid.0.wrapping_sub(epochless).cast_signed();
        let Ok(full) = u64::try_from(i128::from(self.xmax) + i128::from(distance)) else {
            // Older than the first epoch, so older than any snapshot.
            return true;
        };
        full < self.xmin || (full < self.xmax && self.running.binary_search(&full).is_err())
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

/// The fence is the binlog position read just before the read's transaction
/// opens. MySQL offers no per-transaction visibility test against a
/// snapshot, so a commit landing between that position and the snapshot is
/// held by the read yet judged [`Seen::Beyond`], and is applied a second time.
impl Checkpoint for MysqlBinlogPos {
    type Fence = Self;

    fn seen_by(&self, fence: &Self) -> Seen {
        by_position(self, fence)
    }

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
    type Fence = Self;

    fn seen_by(&self, fence: &Self) -> Seen {
        by_position(self, fence)
    }

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
/// order by. Treat this as a zero-information marker. No read can report a
/// fence for it, so its fence type has no values.
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
    type Fence = core::convert::Infallible;

    /// Never called, since no fence of this type exists.
    fn seen_by(&self, _: &core::convert::Infallible) -> Seen {
        Seen::Beyond
    }

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

        let xids = [0u32, 1, 255, 256, u32::MAX];
        let mut positions: Vec<PgCommitPosition> = Vec::new();
        for c in edges {
            for x in xids {
                for o in edges {
                    positions.push(PgCommitPosition::new(PgLsn(c), PgXid(x), o));
                }
            }
        }
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
        let position = PgCommitPosition::new(PgLsn(7), PgXid(740), 1).to_opaque();
        assert_eq!(PgCommitPosition::from_opaque(&lsn), None);
        assert_eq!(PgLsn::from_opaque(&position), None);
        assert_eq!(MysqlBinlogPos::from_opaque(&position), None);
        assert_eq!(NoCheckpoint::from_opaque(&lsn), None);
    }

    fn judged(fence: &PgSnapshotFence, xid: u32, commit_lsn: u64) -> Seen {
        PgCommitPosition::new(PgLsn(commit_lsn), PgXid(xid), 1).seen_by(fence)
    }

    /// A snapshot taken across an epoch boundary still places each 32-bit id
    /// on the right side of it.
    #[test]
    fn a_snapshot_across_an_epoch_boundary_judges_each_side() {
        let epoch = 1u64 << 32;
        let (xmin, xmax) = (epoch - 8, epoch + 5);
        let running = [epoch - 6, epoch + 2];
        let fence = PgSnapshotFence::parse(
            &alloc::format!("{xmin}:{xmax}:{},{}", running[0], running[1]),
            PgLsn(100),
        )
        .unwrap();

        assert_eq!(judged(&fence, u32::MAX - 20, 50), Seen::Held, "before xmin");
        assert_eq!(
            judged(&fence, u32::MAX - 5, 50),
            Seen::Missed,
            "running, old epoch"
        );
        assert_eq!(
            judged(&fence, u32::MAX - 4, 50),
            Seen::Held,
            "done, old epoch"
        );
        assert_eq!(judged(&fence, 1, 50), Seen::Held, "done, new epoch");
        assert_eq!(judged(&fence, 2, 50), Seen::Missed, "running, new epoch");
        assert_eq!(judged(&fence, 5, 50), Seen::Missed, "at xmax");
        assert_eq!(judged(&fence, 9, 50), Seen::Missed, "after xmax");

        let first_epoch = PgSnapshotFence::parse("3:5:", PgLsn(100)).unwrap();
        assert_eq!(
            judged(&first_epoch, u32::MAX - 1, 50),
            Seen::Held,
            "nearest to xmax lies before the first epoch"
        );
    }

    /// The insert position decides before the snapshot does, and a position
    /// naming no transaction is never held.
    #[test]
    fn the_insert_position_and_an_invalid_xid_are_never_held() {
        let fence = PgSnapshotFence::parse("740:745:", PgLsn(2000)).unwrap();
        assert_eq!(judged(&fence, 741, 1999), Seen::Held);
        assert_eq!(judged(&fence, 741, 2000), Seen::Beyond);
        assert_eq!(
            PgCommitPosition::before_commit(PgLsn(1000)).seen_by(&fence),
            Seen::Missed
        );
    }

    #[test]
    fn a_snapshot_of_another_shape_is_refused() {
        let parse = |text| PgSnapshotFence::parse(text, PgLsn(1));
        assert!(parse("740:745:741,743").is_some());
        assert!(parse("740:745").is_none(), "no running list");
        assert!(parse("745:740:").is_none(), "xmin after xmax");
        assert!(parse("740:745:746").is_none(), "running past xmax");
        assert!(parse("740:745:739").is_none(), "running before xmin");
        assert!(parse("740:745:741:").is_none(), "a fourth field");
        assert!(parse("x:745:").is_none());
        assert!(parse("740:x:").is_none());
        assert!(parse("740:745:741,x").is_none());
    }

    #[test]
    fn a_position_fence_holds_what_lies_at_or_before_it() {
        assert_eq!(PgLsn(20).seen_by(&PgLsn(20)), Seen::Held);
        assert_eq!(PgLsn(21).seen_by(&PgLsn(20)), Seen::Beyond);
        let binlog = MysqlBinlogPos { file: 3, pos: 40 };
        assert_eq!(
            MysqlBinlogPos { file: 3, pos: 40 }.seen_by(&binlog),
            Seen::Held
        );
        assert_eq!(
            MysqlBinlogPos { file: 4, pos: 1 }.seen_by(&binlog),
            Seen::Beyond
        );
        let opaque = OpaqueCheckpoint(vec![1, 2]);
        assert_eq!(OpaqueCheckpoint(vec![1]).seen_by(&opaque), Seen::Held);
        assert_eq!(OpaqueCheckpoint(vec![1, 3]).seen_by(&opaque), Seen::Beyond);
    }
}
