//! ID types for runtime predicate and consumer management

use std::num::NonZeroU32;

/// Predicate identifier (slab index)
///
/// Uses NonZeroU32 for `Option<PredicateId>` optimization (no extra byte).
/// Slab allocates stable IDs that don't change during predicate lifetime.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PredicateId(NonZeroU32);

impl PredicateId {
    /// Try to create `PredicateId` from slab index.
    ///
    /// Returns error when index would overflow the internal non-zero u32 layout.
    pub const fn try_from_slab_index(index: usize) -> Result<Self, &'static str> {
        if index >= u32::MAX as usize {
            return Err("Slab index too large");
        }

        #[allow(clippy::cast_possible_truncation)]
        let raw = (index as u32) + 1;
        match NonZeroU32::new(raw) {
            Some(inner) => Ok(Self(inner)),
            None => Err("Slab index too large"),
        }
    }

    /// Create PredicateId from slab index
    ///
    /// # Panics
    /// Panics if index is `usize::MAX` (reserved for None representation)
    #[must_use]
    pub fn from_slab_index(index: usize) -> Self {
        Self::try_from_slab_index(index).unwrap_or_else(|msg| panic!("{msg}"))
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

    /// Try to reconstruct PredicateId from raw u32 (inverse of as_u32).
    ///
    /// Returns `None` if `raw` is 0.
    #[must_use]
    pub const fn try_from_u32(raw: u32) -> Option<Self> {
        match NonZeroU32::new(raw) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Reconstruct PredicateId from raw u32 (inverse of as_u32)
    ///
    /// # Panics
    /// Panics if `raw` is 0.
    #[must_use]
    pub const fn from_u32(raw: u32) -> Self {
        match Self::try_from_u32(raw) {
            Some(value) => value,
            None => panic!("PredicateId cannot be zero"),
        }
    }
}

impl TryFrom<u32> for PredicateId {
    type Error = &'static str;

    fn try_from(raw: u32) -> Result<Self, Self::Error> {
        Self::try_from_u32(raw).ok_or("PredicateId cannot be zero")
    }
}

impl TryFrom<usize> for PredicateId {
    type Error = &'static str;

    fn try_from(index: usize) -> Result<Self, Self::Error> {
        Self::try_from_slab_index(index)
    }
}

/// Consumer ordinal within table partition (for bitmap indexing)
///
/// Dense, 0-based ordinal assigned within each table partition.
/// Used as bitmap index in `RoaringBitmap` for fast set operations.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerOrdinal(u32);

impl ConsumerOrdinal {
    /// Create ConsumerOrdinal from u32
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
    fn test_predicate_id_try_from_u32() {
        assert!(PredicateId::try_from_u32(0).is_none());
        assert_eq!(
            PredicateId::try_from_u32(7)
                .expect("non-zero raw id should construct")
                .as_u32(),
            7
        );
    }

    #[test]
    fn test_predicate_id_try_from_slab_index_success() {
        let pid = PredicateId::try_from_slab_index(7).expect("small slab index should construct");
        assert_eq!(pid.as_u32(), 8);
        assert_eq!(pid.to_slab_index(), 7);
    }

    #[test]
    fn test_predicate_id_try_from_slab_index_overflow_errors() {
        assert!(PredicateId::try_from_slab_index(u32::MAX as usize).is_err());
    }

    #[test]
    fn test_predicate_id_try_from_trait() {
        let pid = PredicateId::try_from(7_u32).expect("non-zero raw id should construct");
        assert_eq!(pid.as_u32(), 7);
    }

    #[test]
    fn test_predicate_id_try_from_trait_zero_errors() {
        assert!(PredicateId::try_from(0_u32).is_err());
    }

    #[test]
    fn test_predicate_id_from_u32_roundtrip() {
        let pid = PredicateId::from_u32(9);
        assert_eq!(pid.as_u32(), 9);
        assert_eq!(pid.to_slab_index(), 8);
    }

    #[test]
    fn test_consumer_ordinal() {
        let ord = ConsumerOrdinal::new(42);
        assert_eq!(ord.get(), 42);

        let next = ord.next();
        assert_eq!(next.get(), 43);
    }

    #[test]
    fn test_consumer_ordinal_ordering() {
        let ord1 = ConsumerOrdinal::new(10);
        let ord2 = ConsumerOrdinal::new(20);

        assert!(ord1 < ord2);
        assert!(ord2 > ord1);
    }

    #[test]
    #[should_panic(expected = "Slab index too large")]
    fn test_predicate_id_max_index_panics() {
        let _ = PredicateId::from_slab_index(u32::MAX as usize);
    }

    #[test]
    #[should_panic(expected = "PredicateId cannot be zero")]
    fn test_predicate_id_from_u32_zero_panics() {
        let _ = PredicateId::from_u32(0);
    }
}
