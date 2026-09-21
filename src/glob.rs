//! Glob-style pattern matching, the kind Redis matches keys against.
//!
//! Hand-written rather than translated to a regex because keys are opaque
//! bytes, NUL included, while the POSIX regex and `fnmatch` interfaces take
//! NUL-terminated strings. Used by `KEYS`; `SCAN MATCH` and `PSUBSCRIBE`
//! want the same matcher.
//!
//! [`Pattern`] is the whole of it: build one from the pattern bytes, then ask
//! it about each key. How it goes about answering - what it works out once up
//! front, what it walks per key - is its own business.

use std::ops::RangeInclusive;

/// A glob pattern, ready to be asked about keys.
///
/// Built once per command rather than per key, so that anything worth
/// knowing about the pattern is worked out once: see [`matches_every_key`].
pub struct Pattern<'a> {
    text: &'a [u8],
    /// Answers every key without walking the pattern at all.
    matches_every_key: bool,
}

impl<'a> Pattern<'a> {
    pub fn new(text: &'a [u8]) -> Pattern<'a> {
        Pattern { text, matches_every_key: matches_every_key(text) }
    }

    /// Whether `key` matches this pattern.
    pub fn matches(&self, key: &[u8]) -> bool {
        self.matches_every_key || matches_pattern(self.text, key)
    }
}

/// A pattern of nothing but `*` matches every key - and `KEYS *` is the call
/// that actually gets made, so it is worth recognising once instead of
/// walking per key. Redis hoists the same question out of its loop, where it
/// is called `allkeys`.
///
/// An empty pattern matches only the empty key, so it is not one.
fn matches_every_key(pattern: &[u8]) -> bool {
    !pattern.is_empty() && pattern.iter().all(|&byte| byte == b'*')
}

/// Walks `pattern` against `key`, a byte at a time. The syntax:
///
/// - `*` matches any run of bytes, the empty one included.
/// - `?` matches exactly one byte.
/// - `[...]` matches one byte out of a set of bytes and ranges - `[abc]`,
///   `[a-z]`, `[a-cx]`. A leading `^` inverts it.
/// - `\` makes the next byte literal; a trailing `\` is a literal backslash.
/// - Anything else stands for itself.
///
/// Bytes, not characters: `?` matches one byte of a multi-byte character.
///
/// Backtracking to only the most recent `*` keeps this linear, where the
/// recursive form Redis uses is exponential on `*a*a*a*b`.
fn matches_pattern(pattern: &[u8], key: &[u8]) -> bool {
    let mut pattern_position = 0;
    let mut key_position = 0;
    let mut star: Option<Star> = None;

    loop {
        let rest_of_pattern = &pattern[pattern_position..];

        // A `*` swallows nothing until something after it fails to match.
        if rest_of_pattern.first() == Some(&b'*') {
            star = Some(Star { position: pattern_position, swallowed: key_position });
            pattern_position += 1;
            continue;
        }

        // Key exhausted. Backtracking could only leave the tail less key to
        // work with, so the pattern has to be exhausted too.
        let Some(&byte) = key.get(key_position) else {
            return rest_of_pattern.is_empty();
        };

        if let Some(matched_pattern_length) = match_token(rest_of_pattern, byte) {
            pattern_position += matched_pattern_length;
            key_position += 1;
            continue;
        }

        // Give the last `*` one more byte, and retry what follows it.
        match star {
            Some(Star { position, swallowed }) if swallowed < key.len() => {
                pattern_position = position + 1;
                key_position = swallowed + 1;
                star = Some(Star { position, swallowed: key_position });
            }
            _ => return false,
        }
    }
}

/// The `*` to fall back to, and how much of the key it has swallowed.
struct Star {
    position: usize,
    swallowed: usize,
}

/// Matches the one-byte token at the front of `pattern` against `byte`,
/// answering with the length of that token, or `None` if it did not match.
fn match_token(pattern: &[u8], byte: u8) -> Option<usize> {
    match pattern {
        [] => None,
        [b'?', ..] => Some(1),
        [b'[', ..] => {
            let set = read_set(pattern, byte);
            (set.holds != set.negated).then_some(set.length)
        }
        &[b'\\', quoted, ..] => (quoted == byte).then_some(2),
        // A trailing `\` has nothing to quote and lands here, as a literal.
        &[literal, ..] => (literal == byte).then_some(1),
    }
}

/// A `[...]` set, read against the one byte it was asked about.
struct Set {
    /// Written inverted, as `[^abc]`.
    negated: bool,
    /// Holds that byte.
    holds: bool,
    /// Length in the pattern, brackets included.
    length: usize,
}

/// Reads the set at the front of `pattern`, which must start with `[`. One
/// that is never closed runs to the end of the pattern, as it does in Redis.
fn read_set(pattern: &[u8], byte: u8) -> Set {
    let mut position = 1; // past the '['
    let negated = pattern.get(position) == Some(&b'^');
    if negated {
        position += 1;
    }

    let mut holds = false;
    while let Some(member) = next_set_member(pattern, &mut position) {
        holds |= member.contains(&byte);
    }

    Set { negated, holds, length: position }
}

/// The next member of a set as the span of bytes it stands for, advancing
/// `position` past it. `None` at the closing `]` or the end of the pattern.
fn next_set_member(pattern: &[u8], position: &mut usize) -> Option<RangeInclusive<u8>> {
    match &pattern[*position..] {
        [] => None,
        [b']', ..] => {
            *position += 1;
            None
        }
        &[b'\\', quoted, ..] => {
            *position += 2;
            Some(quoted..=quoted)
        }
        // A range, `[z-a]` meaning the same span as `[a-z]`.
        &[low, b'-', high, ..] => {
            *position += 3;
            Some(low.min(high)..=low.max(high))
        }
        &[literal, ..] => {
            *position += 1;
            Some(literal..=literal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The public path, as a function, so the cases below read as
    /// pattern-against-key. Every one of them goes through `Pattern`, so the
    /// shortcut it takes is under test too.
    fn matches(pattern: &[u8], key: &[u8]) -> bool {
        Pattern::new(pattern).matches(key)
    }

    fn m(pattern: &str, key: &str) -> bool {
        matches(pattern.as_bytes(), key.as_bytes())
    }

    #[test]
    fn should_match_a_pattern_that_is_all_literal() {
        assert!(m("foo", "foo"));
        assert!(!m("foo", "bar"));
        assert!(!m("foo", "foobar"));
        assert!(!m("foobar", "foo"));
        assert!(m("", ""));
        assert!(!m("", "foo"));
    }

    #[test]
    fn should_match_case_sensitively() {
        assert!(!m("foo", "FOO"));
        assert!(!m("[a-z]", "A"));
    }

    #[test]
    fn should_let_a_star_stand_for_any_run_of_bytes() {
        assert!(m("*", ""));
        assert!(m("*", "anything at all"));
        assert!(m("foo*", "foo"));
        assert!(m("foo*", "foobar"));
        assert!(m("*bar", "bar"));
        assert!(m("*bar", "foobar"));
        assert!(m("f*r", "foobar"));
        assert!(m("*o*", "foo"));
        assert!(!m("foo*", "barfoo"));
        assert!(!m("*bar", "barfoo"));
    }

    #[test]
    fn should_treat_a_run_of_stars_as_one() {
        assert!(m("**", "foo"));
        assert!(m("f**o", "foo"));
        assert!(m("***", ""));
    }

    #[test]
    fn should_backtrack_over_a_star_that_swallowed_too_much() {
        assert!(m("*abc", "xxabcyyabc"));
        assert!(m("*a*b", "aab"));
        assert!(m("a*a*a*b", "aaaaaaaaaab"));
        assert!(!m("a*a*a*b", "aaaaaaaaaac"));
    }

    #[test]
    fn should_let_a_question_mark_stand_for_exactly_one_byte() {
        assert!(m("?", "a"));
        assert!(!m("?", ""));
        assert!(!m("?", "ab"));
        assert!(m("f?o", "foo"));
        assert!(!m("f?o", "fooo"));
        assert!(m("??o", "foo"));
    }

    #[test]
    fn should_match_one_byte_out_of_a_set() {
        assert!(m("[abc]", "a"));
        assert!(m("[abc]", "c"));
        assert!(!m("[abc]", "d"));
        assert!(!m("[abc]", "ab"));
        assert!(m("h[ae]llo", "hello"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
    }

    #[test]
    fn should_match_one_byte_out_of_a_range() {
        assert!(m("[a-z]", "q"));
        assert!(m("[a-z]", "a"));
        assert!(m("[a-z]", "z"));
        assert!(!m("[a-z]", "A"));
        assert!(m("[0-9][0-9]", "42"));
        assert!(m("[z-a]", "q"));
    }

    #[test]
    fn should_mix_ranges_and_single_bytes_in_one_set() {
        assert!(m("[a-cx]", "b"));
        assert!(m("[a-cx]", "x"));
        assert!(!m("[a-cx]", "d"));
        assert!(m("[a-c0-9]", "7"));
    }

    #[test]
    fn should_invert_a_set_that_starts_with_a_caret() {
        assert!(m("[^abc]", "d"));
        assert!(!m("[^abc]", "a"));
        assert!(m("[^a-z]", "A"));
        assert!(!m("[^a-z]", "q"));
        // Still exactly one byte, inverted or not.
        assert!(!m("[^abc]", ""));
        assert!(!m("[^abc]", "de"));
    }

    #[test]
    fn should_make_the_byte_after_a_backslash_literal() {
        assert!(m("\\*", "*"));
        assert!(!m("\\*", "anything"));
        assert!(m("\\?", "?"));
        assert!(!m("\\?", "a"));
        assert!(m("\\[abc]", "[abc]"));
        assert!(!m("\\[abc]", "b"));
        assert!(m("foo\\*bar", "foo*bar"));
        assert!(!m("foo\\*bar", "fooXbar"));
        // Inside a set too.
        assert!(m("[\\]]", "]"));
        assert!(m("[a\\-c]", "-"));
        assert!(!m("[a\\-c]", "b"));
    }

    #[test]
    fn should_treat_a_trailing_backslash_as_a_literal_one() {
        assert!(m("\\", "\\"));
        assert!(!m("\\", "a"));
        assert!(m("foo\\", "foo\\"));
    }

    #[test]
    fn should_run_an_unclosed_set_to_the_end_of_the_pattern() {
        assert!(m("[abc", "a"));
        assert!(!m("[abc", "d"));
        assert!(m("[^abc", "d"));
    }

    #[test]
    fn should_match_a_key_that_is_not_valid_text_byte_for_byte() {
        assert!(matches(b"?", &[0xFF]));
        assert!(matches(b"*", &[0xFF, 0x00, 0xFE]));
        // Two bytes to a two-byte character, so `??` matches it and `?` does not.
        assert!(!matches(b"??", "\u{00e9}?".as_bytes()));
        assert!(matches(b"??", "\u{00e9}".as_bytes()));
    }

    #[test]
    fn should_know_which_patterns_match_every_key() {
        assert!(matches_every_key(b"*"));
        assert!(matches_every_key(b"**"));
        assert!(!matches_every_key(b""));
        assert!(!matches_every_key(b"a"));
        assert!(!matches_every_key(b"*a"));
        assert!(!matches_every_key(b"?"));
    }

    #[test]
    fn should_only_call_a_pattern_all_matching_when_it_really_matches_all() {
        // The shortcut has to agree with the matcher it skips.
        for pattern in ["*", "**", "***", "", "a", "*a", "a*", "?", "[a]", "\\*"] {
            let keys = ["", "a", "foo", "*", "\\", "a*b"];

            if matches_every_key(pattern.as_bytes()) {
                assert!(
                    keys.iter().all(|key| m(pattern, key)),
                    "{:?} was called all-matching but is not",
                    pattern
                );
            }
        }
    }

    /// The obvious, exponential formulation: `*` tried against every tail of
    /// the key. Correct by inspection, and far too slow to ship.
    fn reference_matches(pattern: &[u8], key: &[u8]) -> bool {
        match pattern.first() {
            None => key.is_empty(),
            Some(b'*') => (0..=key.len())
                .any(|swallowed| reference_matches(&pattern[1..], &key[swallowed..])),
            Some(_) => match key.first() {
                Some(&byte) => match_token(pattern, byte)
                    .is_some_and(|length| reference_matches(&pattern[length..], &key[1..])),
                None => false,
            },
        }
    }

    /// Every string of up to `length` bytes drawn from `alphabet`.
    fn all_strings(alphabet: &[u8], length: usize) -> Vec<Vec<u8>> {
        let mut strings = vec![Vec::new()];
        let mut longest = vec![Vec::new()];
        for _ in 0..length {
            longest = longest
                .iter()
                .flat_map(|prefix| {
                    alphabet.iter().map(move |&byte| {
                        let mut longer = prefix.clone();
                        longer.push(byte);
                        longer
                    })
                })
                .collect();
            strings.extend(longest.iter().cloned());
        }
        strings
    }

    #[test]
    fn should_agree_with_the_obvious_recursive_matcher() {
        // What says the two shortcuts - backtrack to the newest `*` only, and
        // give up once the key runs out - lose nothing.
        let patterns = all_strings(b"ab*?[]^-\\", 4);
        let keys = all_strings(b"ab", 3);

        for pattern in &patterns {
            for key in &keys {
                assert_eq!(
                    matches(pattern, key),
                    reference_matches(pattern, key),
                    "disagreed on pattern {:?} and key {:?}",
                    String::from_utf8_lossy(pattern),
                    String::from_utf8_lossy(key),
                );
            }
        }
    }

    #[test]
    fn should_not_blow_up_on_a_pattern_built_to_make_it() {
        // Nothing asserts on the clock: were the loop to start backtracking
        // over every `*`, this would simply never finish.
        let pattern = "a*".repeat(24) + "b";
        let key = "a".repeat(64);

        assert!(!matches(pattern.as_bytes(), key.as_bytes()));
    }
}
