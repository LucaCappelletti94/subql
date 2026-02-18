//! ID types for runtime predicate and user management

use std::num::NonZeroU32;

/// Predicate identifier (slab index)
///
/// Uses NonZeroU32 for Option<PredicateId> optimization (no extra byte).
/// Slab allocates stable IDs that don't change during predicate lifetime.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PredicateId(NonZeroU32);

impl PredicateId {
    /// Create PredicateId from slab index
    ///
    /// # Panics
    /// Panics if index is `usize::MAX` (reserved for None representation)
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn from_slab_index(index: usize) -> Self {
        assert!(index < u32::MAX as usize, "Slab index too large");
        // slab indices start at 0, but NonZeroU32 requires non-zero
        // So we use index + 1 for storage
        // Safety: the assert above guarantees index < u32::MAX, so index+1 is non-zero
        #[allow(clippy::unwrap_used)]
        let inner = NonZeroU32::new((index as u32) + 1).unwrap();
        Self(inner)
    }

    /// Convert back to slab index
    #[must_use]
    pub const fn to_slab_index(self) -> usize {
        (self.0.get() - 1) as usize
    }

    /// Get raw u32 representation
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0.get()
    }

    /// Reconstruct PredicateId from raw u32 (inverse of as_u32)
    ///
    /// # Panics
    /// Panics if `raw` is 0.
    #[must_use]
    pub fn from_u32(raw: u32) -> Self {
        Self(NonZeroU32::new(raw).expect("PredicateId cannot be zero"))
    }
}

/// User ordinal within table partition (for bitmap indexing)
///
/// Dense, 0-based ordinal assigned within each table partition.
/// Used as bitmap index in `RoaringBitmap` for fast set operations.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UserOrdinal(u32);

impl UserOrdinal {
    /// Create UserOrdinal from u32
    #[must_use]
    pub const fn new(ordinal: u32) -> Self {
        Self(ordinal)
    }

    /// Get raw u32 value
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Increment ordinal (for dictionary allocation)
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// Predicate hash for deduplication
///
/// 128-bit hash computed via seahash. Collision probability is negligible
/// (< 2^-64 for millions of predicates).
pub type PredicateHash = u128;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_id_roundtrip() {
        for i in 0..1000 {
            let pid = PredicateId::from_slab_index(i);
            assert_eq!(pid.to_slab_index(), i);
        }
    }

    #[test]
    fn test_predicate_id_nonzero_optimization() {
        // Option<PredicateId> should be same size as PredicateId
        assert_eq!(
            std::mem::size_of::<Option<PredicateId>>(),
            std::mem::size_of::<PredicateId>()
        );
    }

    #[test]
    fn test_user_ordinal() {
        let ord = UserOrdinal::new(42);
        assert_eq!(ord.get(), 42);

        let next = ord.next();
        assert_eq!(next.get(), 43);
    }

    #[test]
    fn test_user_ordinal_ordering() {
        let ord1 = UserOrdinal::new(10);
        let ord2 = UserOrdinal::new(20);

        assert!(ord1 < ord2);
        assert!(ord2 > ord1);
    }

    #[test]
    #[should_panic]
    fn test_predicate_id_max_index_panics() {
        let _ = PredicateId::from_slab_index(u32::MAX as usize);
    }
}
