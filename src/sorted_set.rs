//! The sorted set data type: unique members, each with a floating-point
//! score, kept ordered by `(score, member)`.
//!
//! Like Redis (hash table + skip list) it is stored twice over: a hash map
//! answers "what is this member's score?" in O(1), and an ordered set keeps
//! the members sorted for range queries. Every mutation updates both.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

/// A member together with its score: what ZADD takes
/// (`ZADD key <score> <member> ...`) and what a sorted set keeps in order.
#[derive(Debug, Clone)]
pub struct ScoredMember {
    pub score: f64,
    pub member: String,
}

impl ScoredMember {
    pub fn new(score: f64, member: impl Into<String>) -> ScoredMember {
        ScoredMember { score, member: member.into() }
    }
}

/// Ascending by score, then by member name for equal scores.
///
/// `f64` is only `PartialOrd` (NaN compares with nothing), so `total_cmp`
/// supplies the total order a `BTreeSet` needs. Scores are never NaN - input
/// and the RDB loader reject it - where `total_cmp` is plain numeric order,
/// except that it puts `-0.0` before `0.0`.
impl Ord for ScoredMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for ScoredMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Equality has to agree with `cmp`, which a derived `PartialEq` (using f64's
// `==`, where NaN != NaN and -0.0 == 0.0) would not.
impl PartialEq for ScoredMember {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ScoredMember {}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SortedSet {
    /// member -> score.
    scores: HashMap<String, f64>,
    /// Members in ascending `(score, member)` order.
    ordered: BTreeSet<ScoredMember>,
}

impl SortedSet {
    pub fn new() -> SortedSet {
        SortedSet::default()
    }

    /// Sets `member`'s score, inserting the member if it is new.
    ///
    /// Returns `true` when the member was added, `false` when it already
    /// existed (its score is updated and it moves to its new position).
    pub fn add(&mut self, member: &str, score: f64) -> bool {
        match self.scores.insert(member.to_owned(), score) {
            Some(old_score) => {
                self.ordered.remove(&ScoredMember::new(old_score, member));
                self.ordered.insert(ScoredMember::new(score, member));
                false
            }
            None => {
                self.ordered.insert(ScoredMember::new(score, member));
                true
            }
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// `member`'s 0-based position in ascending `(score, member)` order, or
    /// `None` if it is not in the set.
    ///
    /// Counts the members ordered before it, so it is O(rank): `BTreeSet`
    /// keeps no subtree sizes to answer this in O(log n) the way Redis's
    /// skip list does.
    pub fn rank(&self, member: &str) -> Option<usize> {
        self.ordered.iter().position(|other| other.member == member)
    }

    /// Members at positions `start..=stop` (both inclusive) in ascending
    /// `(score, member)` order, the way ZRANGE indexes them.
    ///
    /// A negative index counts from the end, `-1` being the last member. A
    /// `start` before the first member and a `stop` past the last one are
    /// clamped to the set; nothing is returned when `start > stop` or `start`
    /// is past the last member.
    ///
    /// Skipping to `start` is O(start): like [`rank`](Self::rank), `BTreeSet`
    /// keeps no subtree sizes to seek by position in O(log n).
    pub fn range(&self, start: i64, stop: i64) -> impl Iterator<Item = &str> {
        // A set can never hold anywhere near i64::MAX members.
        let len = self.len() as i64;
        let from_end = |index: i64| if index < 0 { index + len } else { index };
        let start = from_end(start).max(0);
        let stop = from_end(stop).min(len - 1);
        // Checked before subtracting: `stop - start` could overflow when
        // `stop` is far negative and `start` far positive.
        let (skip, count) = if start > stop {
            (0, 0)
        } else {
            // Both lie in 0..len here, so they fit a usize.
            (start as usize, (stop - start + 1) as usize)
        };
        self.ordered.iter().skip(skip).take(count).map(|scored| scored.member.as_str())
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Members with their scores, in ascending `(score, member)` order.
    pub fn iter(&self) -> impl Iterator<Item = &ScoredMember> {
        self.ordered.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(set: &SortedSet) -> Vec<ScoredMember> {
        set.iter().cloned().collect()
    }

    #[test]
    fn test_add_new_member_reports_added() {
        let mut set = SortedSet::new();
        assert!(set.add("a", 1.0));
        assert_eq!(set.len(), 1);
        assert_eq!(set.score("a"), Some(1.0));
    }

    #[test]
    fn test_add_existing_member_updates_score_and_position() {
        let mut set = SortedSet::new();
        set.add("a", 1.0);
        set.add("b", 2.0);

        assert!(!set.add("a", 3.0));
        assert_eq!(set.len(), 2);
        assert_eq!(set.score("a"), Some(3.0));
        assert_eq!(members(&set), vec![ScoredMember::new(2.0, "b"), ScoredMember::new(3.0, "a")]);
    }

    #[test]
    fn test_members_ordered_by_score_then_name() {
        let mut set = SortedSet::new();
        set.add("Prickett", 14.5);
        set.add("Sam-Bodden", 8.2);
        set.add("Ford", 6.1);
        set.add("Royce", 8.2);

        assert_eq!(
            members(&set),
            vec![
                ScoredMember::new(6.1, "Ford"),
                ScoredMember::new(8.2, "Royce"),
                ScoredMember::new(8.2, "Sam-Bodden"),
                ScoredMember::new(14.5, "Prickett"),
            ]
        );
    }

    #[test]
    fn test_rank_orders_by_score_then_name() {
        let mut set = SortedSet::new();
        set.add("foo", 100.0);
        set.add("bar", 100.0);
        set.add("baz", 20.0);
        set.add("caz", 30.1);
        set.add("paz", 40.2);

        assert_eq!(set.rank("baz"), Some(0));
        assert_eq!(set.rank("caz"), Some(1));
        assert_eq!(set.rank("paz"), Some(2));
        assert_eq!(set.rank("bar"), Some(3));
        assert_eq!(set.rank("foo"), Some(4));
    }

    #[test]
    fn test_rank_follows_score_updates() {
        let mut set = SortedSet::new();
        set.add("a", 1.0);
        set.add("b", 2.0);
        set.add("a", 3.0);

        assert_eq!(set.rank("b"), Some(0));
        assert_eq!(set.rank("a"), Some(1));
    }

    #[test]
    fn test_rank_of_missing_member_is_none() {
        let set = SortedSet::new();
        assert_eq!(set.rank("a"), None);
    }

    #[test]
    fn test_infinite_scores_order_at_the_ends() {
        let mut set = SortedSet::new();
        set.add("top", f64::INFINITY);
        set.add("middle", 0.0);
        set.add("bottom", f64::NEG_INFINITY);

        let names: Vec<&str> = set.iter().map(|scored| scored.member.as_str()).collect();
        assert_eq!(names, vec!["bottom", "middle", "top"]);
    }

    /// A set holding "a".."e" with scores 1.0..5.0, so position i holds the
    /// i-th letter.
    fn abcde() -> SortedSet {
        let mut set = SortedSet::new();
        for (score, member) in [(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")] {
            set.add(member, score);
        }
        set
    }

    fn range(set: &SortedSet, start: i64, stop: i64) -> Vec<&str> {
        set.range(start, stop).collect()
    }

    #[test]
    fn test_range_is_inclusive_on_both_ends() {
        assert_eq!(range(&abcde(), 1, 3), vec!["b", "c", "d"]);
    }

    #[test]
    fn test_range_with_equal_start_and_stop_returns_one_member() {
        let set = abcde();
        assert_eq!(range(&set, 0, 0), vec!["a"]);
        assert_eq!(range(&set, 2, 2), vec!["c"]);
        assert_eq!(range(&set, 4, 4), vec!["e"]);
    }

    #[test]
    fn test_range_with_start_after_stop_is_empty() {
        assert!(range(&abcde(), 1, 0).is_empty());
        assert!(range(&abcde(), -1, -2).is_empty());
    }

    #[test]
    fn test_range_clamps_stop_past_the_end() {
        assert_eq!(range(&abcde(), 3, 100), vec!["d", "e"]);
        assert_eq!(range(&abcde(), 0, i64::MAX), vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn test_range_with_start_past_the_end_is_empty() {
        assert!(range(&abcde(), 5, 10).is_empty());
        assert!(range(&abcde(), i64::MAX, i64::MAX).is_empty());
    }

    #[test]
    fn test_range_negative_indexes_count_from_the_end() {
        let set = abcde();
        assert_eq!(range(&set, -2, -1), vec!["d", "e"]);
        assert_eq!(range(&set, 0, -1), vec!["a", "b", "c", "d", "e"]);
        assert_eq!(range(&set, -3, 3), vec!["c", "d"]);
    }

    #[test]
    fn test_range_clamps_negative_indexes_past_the_start() {
        let set = abcde();
        assert_eq!(range(&set, -100, 1), vec!["a", "b"]);
        assert!(range(&set, 0, -100).is_empty());
        assert!(range(&set, i64::MAX, i64::MIN).is_empty());
        assert_eq!(range(&set, i64::MIN, 0), vec!["a"]);
    }

    #[test]
    fn test_range_of_empty_set_is_empty() {
        let set = SortedSet::new();
        assert!(range(&set, 0, -1).is_empty());
        assert!(range(&set, 0, 0).is_empty());
    }
}
