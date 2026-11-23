//! Converting sorted set scores to and from the text clients send and
//! receive.

use crate::commands::try_parsing;
use crate::error::RedisError;

/// Reads a score argument the way Redis does: any float, including `inf`,
/// `+inf` and `-inf`, but never NaN, which has no place in an ordering.
pub(crate) fn parse_score(argument: &str) -> Result<f64, anyhow::Error> {
    let not_a_float = || RedisError::new("ERR value is not a valid float");
    let score: f64 = try_parsing(argument, not_a_float())?;
    if score.is_nan() {
        return Err(not_a_float().into());
    }
    Ok(score)
}

/// Writes a score the way Redis replies with one: the shortest decimal that
/// reads back as the same number, so `20.0` becomes `20` and `30.1` stays
/// `30.1`. Infinities come out as `inf` and `-inf`.
///
/// Redis switches to exponent notation for very large or very small scores
/// (`1e+21`, `1e-7`) where this writes every digit out. Both parse back to
/// the same score.
pub(crate) fn format_score(score: f64) -> String {
    // Rust writes negative zero as `-0`; Redis replies `0`.
    if score == 0.0 {
        return "0".to_owned();
    }
    score.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::client_error_message;

    #[test]
    fn test_parse_score_accepts_floats_and_infinities() {
        assert_eq!(parse_score("10.0").unwrap(), 10.0);
        assert_eq!(parse_score("-1e3").unwrap(), -1000.0);
        assert_eq!(parse_score("inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score("+inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score("-inf").unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn test_parse_score_rejects_nan_and_text() {
        for bad in ["nan", "NaN", "abc", ""] {
            let error = parse_score(bad).unwrap_err();
            assert_eq!(client_error_message(error), "ERR value is not a valid float");
        }
    }

    #[test]
    fn test_format_score_uses_shortest_decimal() {
        assert_eq!(format_score(30.1), "30.1");
        assert_eq!(format_score(100.99), "100.99");
        assert_eq!(format_score(-2.5), "-2.5");
        assert_eq!(format_score(0.1 + 0.2), "0.30000000000000004");
    }

    #[test]
    fn test_format_score_drops_fraction_of_whole_numbers() {
        assert_eq!(format_score(20.0), "20");
        assert_eq!(format_score(-4.0), "-4");
    }

    #[test]
    fn test_format_score_negative_zero_is_zero() {
        assert_eq!(format_score(-0.0), "0");
        assert_eq!(format_score(0.0), "0");
    }

    #[test]
    fn test_format_score_infinities() {
        assert_eq!(format_score(f64::INFINITY), "inf");
        assert_eq!(format_score(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn test_format_score_round_trips_extreme_scores() {
        for score in [1e21, -1e21, 1e-7, 5e-324, f64::MAX, f64::MIN, f64::MIN_POSITIVE] {
            assert_eq!(parse_score(&format_score(score)).unwrap(), score, "round-tripping {}", score);
        }
    }
}
