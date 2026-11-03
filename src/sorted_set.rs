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
    fn test_infinite_scores_order_at_the_ends() {
        let mut set = SortedSet::new();
        set.add("top", f64::INFINITY);
        set.add("middle", 0.0);
        set.add("bottom", f64::NEG_INFINITY);

        let names: Vec<&str> = set.iter().map(|scored| scored.member.as_str()).collect();
        assert_eq!(names, vec!["bottom", "middle", "top"]);
    }
}
