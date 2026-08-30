use alloc::vec::Vec;

/// One authorization answer.
///
/// Two states only. "Could not determine" is not a verdict: it is a
/// returned error, which on the read path leaves the unreached watchers
/// at their pre-filled [`Verdict::Deny`] and on the write path is the
/// whole answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Verdict {
    /// The watcher may not see the row, or may not perform the write.
    ///
    /// The default, so a buffer that an implementation never touched
    /// fails closed.
    #[default]
    Deny,
    /// The watcher may see the row, or may perform the write.
    Allow,
}

impl Verdict {
    /// Whether this verdict permits the action.
    #[must_use]
    pub const fn allowed(self) -> bool {
        matches!(self, Self::Allow)
    }

    /// Prepare `buffer` to answer `watchers` questions, fail-closed.
    ///
    /// Sizes the buffer to exactly `watchers` entries and sets every one
    /// to [`Verdict::Deny`]. Call this on a buffer kept across events
    /// rather than allocating a fresh one, which is the whole reason
    /// [`VisibilityPolicy::may_see`](crate::visibility::VisibilityPolicy::may_see) writes into a caller-owned slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use subql::visibility::Verdict;
    ///
    /// let mut buffer = Vec::new();
    /// Verdict::reset(&mut buffer, 3);
    /// assert_eq!(buffer, [Verdict::Deny; 3]);
    ///
    /// // A stale Allow from a previous event never survives the reset.
    /// buffer[0] = Verdict::Allow;
    /// Verdict::reset(&mut buffer, 2);
    /// assert_eq!(buffer, [Verdict::Deny; 2]);
    /// ```
    pub fn reset(buffer: &mut Vec<Self>, watchers: usize) {
        buffer.clear();
        buffer.resize(watchers, Self::Deny);
    }
}

/// The verb a write authorization question is about.
///
/// Distinct from [`EventKind`](crate::EventKind), which describes an
/// observed change and includes `Truncate`. A write question is always
/// about one row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteOp {
    /// Create the row.
    Insert,
    /// Replace the row's values.
    Update,
    /// Remove the row.
    Delete,
}
