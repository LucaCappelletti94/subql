//! The truths a condition may still take once every cell the event omitted is known.

use crate::compiler::Tri;

const TRUE: u8 = 1;
const FALSE: u8 = 2;
const UNKNOWN: u8 = 4;

/// A set of truths, one bit each.
///
/// A condition over carried cells has exactly one. One that read an omitted
/// cell may have several, and combining sets truth by truth keeps a `NULL`
/// that settles the answer apart from an omitted cell that might not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Possible(u8);

impl Possible {
    /// Every truth, for a condition the event's omitted cells leave open.
    pub(super) const ANY: Self = Self(TRUE | FALSE | UNKNOWN);

    /// The one truth a condition over carried cells has.
    pub(super) const fn of(truth: Tri) -> Self {
        Self(match truth {
            Tri::True => TRUE,
            Tri::False => FALSE,
            Tri::Unknown => UNKNOWN,
        })
    }

    /// The truth evaluation reports: the only one possible, or unknown.
    pub(super) const fn tri(self) -> Tri {
        match self.0 {
            TRUE => Tri::True,
            FALSE => Tri::False,
            _ => Tri::Unknown,
        }
    }

    /// Whether the omitted cells decide between a match and none.
    pub(super) const fn undecided(self) -> bool {
        self.0 & TRUE != 0 && self.0 != TRUE
    }

    /// `self AND other`, for every pair of truths the two may take.
    pub(super) const fn and(self, other: Self) -> Self {
        let (a, b) = (self.0, other.0);
        let unknown = (a & UNKNOWN != 0 && b & (TRUE | UNKNOWN) != 0)
            || (b & UNKNOWN != 0 && a & (TRUE | UNKNOWN) != 0);
        Self((a & b & TRUE) | ((a | b) & FALSE) | if unknown { UNKNOWN } else { 0 })
    }

    /// `self OR other`, for every pair of truths the two may take.
    pub(super) const fn or(self, other: Self) -> Self {
        let (a, b) = (self.0, other.0);
        let unknown = (a & UNKNOWN != 0 && b & (FALSE | UNKNOWN) != 0)
            || (b & UNKNOWN != 0 && a & (FALSE | UNKNOWN) != 0);
        Self(((a | b) & TRUE) | (a & b & FALSE) | if unknown { UNKNOWN } else { 0 })
    }

    /// `NOT self`.
    pub(super) const fn not(self) -> Self {
        let a = self.0;
        Self(((a & TRUE) << 1) | ((a & FALSE) >> 1) | (a & UNKNOWN))
    }

    /// `self IS [NOT] value`, which is never unknown.
    pub(super) const fn is(self, value: Tri, negated: bool) -> Self {
        let wanted = Self::of(value).0;
        let (hit, miss) = if negated {
            (FALSE, TRUE)
        } else {
            (TRUE, FALSE)
        };
        let hits = if self.0 & wanted != 0 { hit } else { 0 };
        let misses = if self.0 & !wanted != 0 { miss } else { 0 };
        Self(hits | misses)
    }
}

#[cfg(test)]
mod tests {
    use super::{Possible, Tri, FALSE, TRUE, UNKNOWN};

    const TRUTHS: [Tri; 3] = [Tri::True, Tri::False, Tri::Unknown];

    fn sets() -> impl Iterator<Item = Possible> {
        (1..=TRUE | FALSE | UNKNOWN).map(Possible)
    }

    fn members(set: Possible) -> impl Iterator<Item = Tri> {
        TRUTHS
            .into_iter()
            .filter(move |truth| set.0 & Possible::of(*truth).0 != 0)
    }

    fn collect(truths: impl Iterator<Item = Tri>) -> Possible {
        Possible(truths.fold(0, |set, truth| set | Possible::of(truth).0))
    }

    /// Each operation is the three-valued one applied to every pair of truths the operands may take.
    #[test]
    fn every_operation_lifts_three_valued_logic_truth_by_truth() {
        for a in sets() {
            assert_eq!(a.not(), collect(members(a).map(Tri::not)), "NOT {a:?}");
            for value in TRUTHS {
                for negated in [false, true] {
                    let expected = collect(
                        members(a).map(|truth| Tri::from_option(Some((truth == value) != negated))),
                    );
                    assert_eq!(a.is(value, negated), expected, "{a:?} IS {value:?}");
                }
            }
            for b in sets() {
                let pairs = || members(a).flat_map(move |x| members(b).map(move |y| (x, y)));
                assert_eq!(
                    a.and(b),
                    collect(pairs().map(|(x, y)| x.and(y))),
                    "{a:?} AND {b:?}"
                );
                assert_eq!(
                    a.or(b),
                    collect(pairs().map(|(x, y)| x.or(y))),
                    "{a:?} OR {b:?}"
                );
            }
        }
    }
}
