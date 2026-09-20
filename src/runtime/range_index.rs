//! One column's range entries, as an interval tree.
//!
//! A probe asks which intervals contain a value. An ordered map answers it by
//! walking every entry whose lower bound the value clears, which is most of
//! the column once the column is large, even when almost none of those
//! entries reach the value from above. Each node here also carries the widest
//! upper bound in its subtree, so a subtree that cannot reach the value is
//! skipped whole and the walk costs the answer rather than the column.
//!
//! The tree is immutable and shares structure. Registration clones the index
//! to patch it under copy-on-write, so an insert copies the path it descends
//! and leaves every other node shared.

use super::ids::PredicateId;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use roaring::RoaringBitmap;

/// Range index key, ordered by lower bound.
///
/// An unbounded lower bound sorts first and the rest sort along the numeric
/// line. The predicate id only breaks ties between equal bounds.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RangeKey {
    pub lower: Option<i64>,
    pub predicate_id: PredicateId,
}

/// What a probe asks of a bound. `None` on either side is unbounded and
/// always answers yes.
pub trait RangeProbe {
    /// Whether the probed value sits at or above `lower`.
    fn reaches(&self, lower: i64) -> bool;
    /// Whether the probed value sits at or below `upper`.
    fn within(&self, upper: i64) -> bool;
}

/// The upper bound that admits every value either bound admits. Unbounded
/// wins, otherwise the larger one does.
const fn widest_upper(lhs: Option<i64>, rhs: Option<i64>) -> Option<i64> {
    match (lhs, rhs) {
        (Some(left), Some(right)) => Some(if left >= right { left } else { right }),
        _ => None,
    }
}

#[derive(Debug)]
struct Node {
    key: RangeKey,
    upper: Option<i64>,
    /// The widest upper bound in this subtree, this node included.
    max_upper: Option<i64>,
    height: u32,
    left: Link,
    right: Link,
}

type Link = Option<Arc<Node>>;

fn height(link: &Link) -> u32 {
    link.as_ref().map_or(0, |node| node.height)
}

fn max_upper(link: &Link) -> Option<i64> {
    // An absent child bounds nothing, so it must not widen the subtree.
    // `Some(i64::MIN)` is the neutral element `widest_upper` accepts.
    link.as_ref().map_or(Some(i64::MIN), |node| node.max_upper)
}

fn make(key: RangeKey, upper: Option<i64>, left: Link, right: Link) -> Arc<Node> {
    let height = 1 + height(&left).max(height(&right));
    let max_upper = widest_upper(upper, widest_upper(max_upper(&left), max_upper(&right)));
    Arc::new(Node {
        key,
        upper,
        max_upper,
        height,
        left,
        right,
    })
}

fn balance_factor(left: &Link, right: &Link) -> i64 {
    i64::from(height(left)) - i64::from(height(right))
}

fn rotate_right(key: RangeKey, upper: Option<i64>, left: Link, right: Link) -> Arc<Node> {
    let pivot = left.expect("a right rotation has a left child");
    make(
        pivot.key,
        pivot.upper,
        pivot.left.clone(),
        Some(make(key, upper, pivot.right.clone(), right)),
    )
}

fn rotate_left(key: RangeKey, upper: Option<i64>, left: Link, right: Link) -> Arc<Node> {
    let pivot = right.expect("a left rotation has a right child");
    make(
        pivot.key,
        pivot.upper,
        Some(make(key, upper, left, pivot.left.clone())),
        pivot.right.clone(),
    )
}

/// Rebuild one node, rotating when its children differ in height by more
/// than one.
fn balanced(key: RangeKey, upper: Option<i64>, left: Link, right: Link) -> Arc<Node> {
    match balance_factor(&left, &right) {
        2 => {
            let heavy = left.expect("a left-heavy node has a left child");
            if balance_factor(&heavy.left, &heavy.right) < 0 {
                let rotated = rotate_left(
                    heavy.key,
                    heavy.upper,
                    heavy.left.clone(),
                    heavy.right.clone(),
                );
                rotate_right(key, upper, Some(rotated), right)
            } else {
                rotate_right(
                    key,
                    upper,
                    Some(make(
                        heavy.key,
                        heavy.upper,
                        heavy.left.clone(),
                        heavy.right.clone(),
                    )),
                    right,
                )
            }
        }
        -2 => {
            let heavy = right.expect("a right-heavy node has a right child");
            if balance_factor(&heavy.left, &heavy.right) > 0 {
                let rotated = rotate_right(
                    heavy.key,
                    heavy.upper,
                    heavy.left.clone(),
                    heavy.right.clone(),
                );
                rotate_left(key, upper, left, Some(rotated))
            } else {
                rotate_left(
                    key,
                    upper,
                    left,
                    Some(make(
                        heavy.key,
                        heavy.upper,
                        heavy.left.clone(),
                        heavy.right.clone(),
                    )),
                )
            }
        }
        _ => make(key, upper, left, right),
    }
}

/// Insert or widen, returning the new subtree and whether the entry is new.
fn insert(link: &Link, key: RangeKey, upper: Option<i64>) -> (Arc<Node>, bool) {
    let Some(node) = link else {
        return (make(key, upper, None, None), true);
    };
    match key.cmp(&node.key) {
        // One predicate can file two ranges on a column sharing a lower
        // bound, as `a BETWEEN 5 AND 10 OR a BETWEEN 5 AND 20` does. The key
        // holds one of them, so it holds the wider upper bound, which keeps
        // the index a superset and leaves the verdict to the comparator.
        Ordering::Equal => (
            make(
                node.key,
                widest_upper(node.upper, upper),
                node.left.clone(),
                node.right.clone(),
            ),
            false,
        ),
        Ordering::Less => {
            let (left, added) = insert(&node.left, key, upper);
            (
                balanced(node.key, node.upper, Some(left), node.right.clone()),
                added,
            )
        }
        Ordering::Greater => {
            let (right, added) = insert(&node.right, key, upper);
            (
                balanced(node.key, node.upper, node.left.clone(), Some(right)),
                added,
            )
        }
    }
}

/// Detach the leftmost entry, returning it and the subtree without it.
fn take_min(node: &Arc<Node>) -> (RangeKey, Option<i64>, Link) {
    node.left.as_ref().map_or_else(
        || (node.key, node.upper, node.right.clone()),
        |left| {
            let (key, upper, rest) = take_min(left);
            (
                key,
                upper,
                Some(balanced(node.key, node.upper, rest, node.right.clone())),
            )
        },
    )
}

fn remove(link: &Link, key: &RangeKey) -> (Link, bool) {
    let Some(node) = link else {
        return (None, false);
    };
    match key.cmp(&node.key) {
        Ordering::Less => {
            let (left, removed) = remove(&node.left, key);
            (
                Some(balanced(node.key, node.upper, left, node.right.clone())),
                removed,
            )
        }
        Ordering::Greater => {
            let (right, removed) = remove(&node.right, key);
            (
                Some(balanced(node.key, node.upper, node.left.clone(), right)),
                removed,
            )
        }
        Ordering::Equal => match (&node.left, &node.right) {
            (None, right) => (right.clone(), true),
            (left, None) => (left.clone(), true),
            (left, Some(right)) => {
                let (next_key, next_upper, rest) = take_min(right);
                (
                    Some(balanced(next_key, next_upper, left.clone(), rest)),
                    true,
                )
            }
        },
    }
}

/// One column's range entries, keyed by lower bound and valued by upper
/// bound.
#[derive(Clone, Debug, Default)]
pub struct RangeIndex {
    root: Link,
    len: usize,
}

impl RangeIndex {
    #[must_use]
    pub const fn new() -> Self {
        Self { root: None, len: 0 }
    }

    /// Add an interval, widening the entry already under `key` if there is
    /// one.
    pub fn insert(&mut self, key: RangeKey, upper: Option<i64>) {
        let (root, added) = insert(&self.root, key, upper);
        self.root = Some(root);
        if added {
            self.len += 1;
        }
    }

    /// Drop the interval under `key`, reporting whether there was one.
    pub fn remove(&mut self, key: &RangeKey) -> bool {
        let (root, removed) = remove(&self.root, key);
        self.root = root;
        if removed {
            self.len -= 1;
        }
        removed
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Every predicate whose interval contains the probed value.
    ///
    /// Costs the height of the tree plus the answer, because a subtree whose
    /// widest upper bound falls short of the value is skipped whole, and so
    /// is one whose lower bounds all overshoot it.
    pub fn stab_into(&self, probe: &impl RangeProbe, out: &mut RoaringBitmap) {
        stab(&self.root, probe, out);
    }

    /// Every predicate on the column, in no particular order.
    pub fn insert_all_into(&self, out: &mut RoaringBitmap) {
        out.extend(self.iter().map(|(key, _)| key.predicate_id.as_u32()));
    }

    /// Entries in lower-bound order.
    #[must_use]
    pub fn iter(&self) -> RangeIter<'_> {
        let mut iter = RangeIter {
            stack: Vec::with_capacity(self.len.next_power_of_two().trailing_zeros() as usize + 1),
        };
        iter.descend(&self.root);
        iter
    }
}

fn stab(link: &Link, probe: &impl RangeProbe, out: &mut RoaringBitmap) {
    let Some(node) = link else {
        return;
    };
    // Nothing under here reaches the value from above.
    if node.max_upper.is_some_and(|widest| !probe.within(widest)) {
        return;
    }
    stab(&node.left, probe, out);
    // This node and everything to its right start above the value.
    if node.key.lower.is_some_and(|lower| !probe.reaches(lower)) {
        return;
    }
    if node.upper.is_none_or(|upper| probe.within(upper)) {
        out.insert(node.key.predicate_id.as_u32());
    }
    stab(&node.right, probe, out);
}

impl<'a> IntoIterator for &'a RangeIndex {
    type Item = (RangeKey, Option<i64>);
    type IntoIter = RangeIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Entries in lower-bound order.
pub struct RangeIter<'a> {
    stack: Vec<&'a Arc<Node>>,
}

impl<'a> RangeIter<'a> {
    fn descend(&mut self, mut link: &'a Link) {
        while let Some(node) = link {
            self.stack.push(node);
            link = &node.left;
        }
    }
}

impl Iterator for RangeIter<'_> {
    type Item = (RangeKey, Option<i64>);

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        self.descend(&node.right);
        Some((node.key, node.upper))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloc::vec;

    /// The probe the runtime uses, over integers only, which is all these
    /// tests need.
    struct At(i64);

    impl RangeProbe for At {
        fn reaches(&self, lower: i64) -> bool {
            self.0 >= lower
        }
        fn within(&self, upper: i64) -> bool {
            self.0 <= upper
        }
    }

    fn key(lower: Option<i64>, id: usize) -> RangeKey {
        RangeKey {
            lower,
            predicate_id: PredicateId::from_slab_index(id),
        }
    }

    fn id(slab: usize) -> u32 {
        PredicateId::from_slab_index(slab).as_u32()
    }

    fn stab(index: &RangeIndex, value: i64) -> Vec<u32> {
        let mut out = RoaringBitmap::new();
        index.stab_into(&At(value), &mut out);
        out.into_iter().collect()
    }

    /// Entries that would answer `value` if every one of them were tested.
    fn brute_force(entries: &[(RangeKey, Option<i64>)], value: i64) -> Vec<u32> {
        let mut found: Vec<u32> = entries
            .iter()
            .filter(|(key, upper)| {
                key.lower.is_none_or(|lower| value >= lower)
                    && upper.is_none_or(|upper| value <= upper)
            })
            .map(|(key, _)| key.predicate_id.as_u32())
            .collect();
        found.sort_unstable();
        found.dedup();
        found
    }

    /// Height stays logarithmic and every node's `max_upper` covers its
    /// subtree, which is what the probe prunes on. A wrong `max_upper` skips
    /// a subtree that holds an answer.
    fn check_invariants(link: &Link) -> u32 {
        let Some(node) = link else {
            return 0;
        };
        let left = check_invariants(&node.left);
        let right = check_invariants(&node.right);
        assert!(
            left.abs_diff(right) <= 1,
            "heights {left} and {right} differ by more than one"
        );
        assert_eq!(node.height, 1 + left.max(right), "stale height");
        let expected = widest_upper(
            node.upper,
            widest_upper(max_upper(&node.left), max_upper(&node.right)),
        );
        assert_eq!(node.max_upper, expected, "stale widest upper bound");
        if let Some(left) = node.left.as_ref() {
            assert!(left.key < node.key, "left child out of order");
        }
        if let Some(right) = node.right.as_ref() {
            assert!(right.key > node.key, "right child out of order");
        }
        node.height
    }

    /// A predicate whose interval ends below the probe must not be reported,
    /// and pruning must not lose one that spans it.
    #[test]
    fn a_probe_reports_exactly_the_intervals_that_contain_it() {
        let mut index = RangeIndex::new();
        index.insert(key(Some(0), 0), Some(10));
        index.insert(key(Some(5), 1), Some(6));
        index.insert(key(Some(5), 2), None);
        index.insert(key(None, 3), Some(4));

        assert_eq!(stab(&index, 3), vec![id(0), id(3)]);
        assert_eq!(stab(&index, 6), vec![id(0), id(1), id(2)]);
        assert_eq!(stab(&index, 11), vec![id(2)]);
    }

    /// Inserting in bound order is the shape a subscription stream produces,
    /// and it is the one that degenerates an unbalanced tree into a list.
    #[test]
    fn ordered_inserts_stay_balanced_and_answer_the_same() {
        let mut index = RangeIndex::new();
        let entries: Vec<(RangeKey, Option<i64>)> = (0..1_000usize)
            .map(|i| {
                let lower = i64::try_from(i).expect("a thousand fits");
                (key(Some(lower), i), Some(lower + 50))
            })
            .collect();
        for (key, upper) in &entries {
            index.insert(*key, *upper);
        }

        assert_eq!(index.len(), 1_000);
        let height = check_invariants(&index.root);
        assert!(height <= 15, "a thousand ordered inserts reached {height}");
        for value in [0, 1, 25, 500, 999, 1_048] {
            assert_eq!(stab(&index, value), brute_force(&entries, value));
        }
    }

    /// Insert and remove in a scattered order, checking the answer against a
    /// full scan at every step, which is the only way a pruning bug shows.
    #[test]
    fn pruning_agrees_with_a_full_scan_through_inserts_and_removals() {
        let mut index = RangeIndex::new();
        let mut live: Vec<(RangeKey, Option<i64>)> = Vec::new();
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for round in 0..400usize {
            let lower = i64::try_from(next() % 200).expect("bounded") - 100;
            let width = i64::try_from(next() % 40).expect("bounded");
            let entry = (
                key(
                    if next().is_multiple_of(11) {
                        None
                    } else {
                        Some(lower)
                    },
                    round,
                ),
                if next().is_multiple_of(7) {
                    None
                } else {
                    Some(lower + width)
                },
            );
            index.insert(entry.0, entry.1);
            live.push(entry);

            if round.is_multiple_of(3) && !live.is_empty() {
                let index_of_victim = usize::try_from(next()).unwrap_or(usize::MAX) % live.len();
                let victim = live.remove(index_of_victim);
                assert!(index.remove(&victim.0), "the entry was there");
            }

            check_invariants(&index.root);
            assert_eq!(index.len(), live.len());
            for value in [-120, -50, 0, 17, 60, 140] {
                assert_eq!(
                    stab(&index, value),
                    brute_force(&live, value),
                    "round {round} probing {value}"
                );
            }
        }
    }

    /// Two intervals under one key merge to the wider, and removing one
    /// entry leaves a twin that shares its bounds.
    #[test]
    fn a_shared_key_widens_and_a_twin_survives_removal() {
        let mut index = RangeIndex::new();
        index.insert(key(Some(5), 0), Some(20));
        index.insert(key(Some(5), 0), Some(10));
        assert_eq!(index.len(), 1, "one key, one entry");
        assert_eq!(stab(&index, 15), vec![id(0)], "widened, not narrowed");

        index.insert(key(Some(5), 1), Some(10));
        assert!(index.remove(&key(Some(5), 1)));
        assert_eq!(
            stab(&index, 7),
            vec![id(0)],
            "the twin went, the entry stayed"
        );
        assert!(!index.remove(&key(Some(5), 1)), "and it is gone for good");
    }

    /// The order the export and the index comparison rely on.
    #[test]
    fn iteration_follows_the_lower_bound() {
        let mut index = RangeIndex::new();
        index.insert(key(Some(7), 1), None);
        index.insert(key(None, 2), Some(3));
        index.insert(key(Some(-4), 0), Some(0));

        let seen: Vec<Option<i64>> = index.iter().map(|(key, _)| key.lower).collect();
        assert_eq!(seen, vec![None, Some(-4), Some(7)]);
    }
}
