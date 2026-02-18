//! SQL three-valued logic (TRUE/FALSE/UNKNOWN)
//!
//! Implements truth tables from SQL standard for NULL propagation.

/// SQL tri-state value
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Tri {
    True,
    False,
    Unknown,
}

impl Tri {
    /// AND operation with short-circuit semantics
    ///
    /// Truth table:
    /// ```text
    /// | A       | B       | A AND B |
    /// |---------|---------|---------|
    /// | True    | True    | True    |
    /// | True    | False   | False   |
    /// | True    | Unknown | Unknown |
    /// | False   | True    | False   |
    /// | False   | False   | False   |
    /// | False   | Unknown | False   | ← short-circuit
    /// | Unknown | True    | Unknown |
    /// | Unknown | False   | False   | ← short-circuit
    /// | Unknown | Unknown | Unknown |
    /// ```
    #[must_use]
    pub const fn and(self, other: Self) -> Self {
        match (self, other) {
            // Short-circuit: False AND _ = False
            (Self::False, _) | (_, Self::False) => Self::False,
            // Both true
            (Self::True, Self::True) => Self::True,
            // Everything else is Unknown
            _ => Self::Unknown,
        }
    }

    /// OR operation with short-circuit semantics
    ///
    /// Truth table:
    /// ```text
    /// | A       | B       | A OR B  |
    /// |---------|---------|---------|
    /// | True    | True    | True    |
    /// | True    | False   | True    |
    /// | True    | Unknown | True    | ← short-circuit
    /// | False   | True    | True    |
    /// | False   | False   | False   |
    /// | False   | Unknown | Unknown |
    /// | Unknown | True    | True    | ← short-circuit
    /// | Unknown | False   | Unknown |
    /// | Unknown | Unknown | Unknown |
    /// ```
    #[must_use]
    pub const fn or(self, other: Self) -> Self {
        match (self, other) {
            // Short-circuit: True OR _ = True
            (Self::True, _) | (_, Self::True) => Self::True,
            // Both false
            (Self::False, Self::False) => Self::False,
            // Everything else is Unknown
            _ => Self::Unknown,
        }
    }

    /// NOT operation
    ///
    /// Truth table:
    /// ```text
    /// | Input   | Output  |
    /// |---------|---------|
    /// | True    | False   |
    /// | False   | True    |
    /// | Unknown | Unknown |
    /// ```
    #[must_use]
    pub const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    /// Converts to Option<bool> (Unknown → None)
    #[must_use]
    pub const fn to_option(self) -> Option<bool> {
        match self {
            Self::True => Some(true),
            Self::False => Some(false),
            Self::Unknown => None,
        }
    }

    /// Converts from Option<bool> (None → Unknown)
    #[must_use]
    pub const fn from_option(opt: Option<bool>) -> Self {
        match opt {
            Some(true) => Self::True,
            Some(false) => Self::False,
            None => Self::Unknown,
        }
    }
}

impl std::ops::Not for Tri {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self::not(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_truth_table() {
        assert_eq!(!Tri::True, Tri::False);
        assert_eq!(!Tri::False, Tri::True);
        assert_eq!(!Tri::Unknown, Tri::Unknown);
    }

    #[test]
    fn test_and_truth_table_exhaustive() {
        // True AND x
        assert_eq!(Tri::True.and(Tri::True), Tri::True);
        assert_eq!(Tri::True.and(Tri::False), Tri::False);
        assert_eq!(Tri::True.and(Tri::Unknown), Tri::Unknown);

        // False AND x (short-circuit)
        assert_eq!(Tri::False.and(Tri::True), Tri::False);
        assert_eq!(Tri::False.and(Tri::False), Tri::False);
        assert_eq!(Tri::False.and(Tri::Unknown), Tri::False);

        // Unknown AND x
        assert_eq!(Tri::Unknown.and(Tri::True), Tri::Unknown);
        assert_eq!(Tri::Unknown.and(Tri::False), Tri::False);
        assert_eq!(Tri::Unknown.and(Tri::Unknown), Tri::Unknown);
    }

    #[test]
    fn test_or_truth_table_exhaustive() {
        // True OR x (short-circuit)
        assert_eq!(Tri::True.or(Tri::True), Tri::True);
        assert_eq!(Tri::True.or(Tri::False), Tri::True);
        assert_eq!(Tri::True.or(Tri::Unknown), Tri::True);

        // False OR x
        assert_eq!(Tri::False.or(Tri::True), Tri::True);
        assert_eq!(Tri::False.or(Tri::False), Tri::False);
        assert_eq!(Tri::False.or(Tri::Unknown), Tri::Unknown);

        // Unknown OR x
        assert_eq!(Tri::Unknown.or(Tri::True), Tri::True);
        assert_eq!(Tri::Unknown.or(Tri::False), Tri::Unknown);
        assert_eq!(Tri::Unknown.or(Tri::Unknown), Tri::Unknown);
    }

    #[test]
    fn test_and_associativity() {
        for a in [Tri::True, Tri::False, Tri::Unknown] {
            for b in [Tri::True, Tri::False, Tri::Unknown] {
                for c in [Tri::True, Tri::False, Tri::Unknown] {
                    assert_eq!(
                        a.and(b).and(c),
                        a.and(b.and(c)),
                        "AND associativity failed for {:?}, {:?}, {:?}",
                        a,
                        b,
                        c
                    );
                }
            }
        }
    }

    #[test]
    fn test_or_associativity() {
        for a in [Tri::True, Tri::False, Tri::Unknown] {
            for b in [Tri::True, Tri::False, Tri::Unknown] {
                for c in [Tri::True, Tri::False, Tri::Unknown] {
                    assert_eq!(
                        a.or(b).or(c),
                        a.or(b.or(c)),
                        "OR associativity failed for {:?}, {:?}, {:?}",
                        a,
                        b,
                        c
                    );
                }
            }
        }
    }

    #[test]
    fn test_demorgan_laws() {
        for a in [Tri::True, Tri::False, Tri::Unknown] {
            for b in [Tri::True, Tri::False, Tri::Unknown] {
                // NOT (a AND b) = (NOT a) OR (NOT b)
                assert_eq!(
                    a.and(b).not(),
                    a.not().or(b.not()),
                    "De Morgan's first law failed for {:?}, {:?}",
                    a,
                    b
                );

                // NOT (a OR b) = (NOT a) AND (NOT b)
                assert_eq!(
                    a.or(b).not(),
                    a.not().and(b.not()),
                    "De Morgan's second law failed for {:?}, {:?}",
                    a,
                    b
                );
            }
        }
    }

    #[test]
    fn test_option_conversion() {
        assert_eq!(Tri::from_option(Some(true)), Tri::True);
        assert_eq!(Tri::from_option(Some(false)), Tri::False);
        assert_eq!(Tri::from_option(None), Tri::Unknown);

        assert_eq!(Tri::True.to_option(), Some(true));
        assert_eq!(Tri::False.to_option(), Some(false));
        assert_eq!(Tri::Unknown.to_option(), None);
    }
}
