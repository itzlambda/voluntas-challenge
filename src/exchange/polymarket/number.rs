//! Exact decimal and timestamp parsing. Never goes through floats.

use chrono::{DateTime, Utc};

const SCALE_DIGITS: usize = 6;

/// Parses a non-negative decimal string into fixed point with six fractional
/// digits: `"0.022"` → `22_000`. More than six fractional digits, ignoring
/// trailing zeros, is an error.
pub fn parse_fixed6(text: &str) -> Result<i64, String> {
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (text, ""),
    };
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) || text.ends_with('.') {
        return Err(format!("invalid decimal {text:?}"));
    }
    let fraction = fraction.trim_end_matches('0');
    if fraction.len() > SCALE_DIGITS {
        return Err(format!(
            "more than {SCALE_DIGITS} fractional digits in {text:?}"
        ));
    }

    let overflow = || format!("decimal out of range {text:?}");
    let mut value: i64 = 0;
    for byte in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(byte - b'0')))
            .ok_or_else(overflow)?;
    }
    let mut frac_value: i64 = 0;
    for position in 0..SCALE_DIGITS {
        let digit = fraction.as_bytes().get(position).map_or(0, |b| b - b'0');
        frac_value = frac_value * 10 + i64::from(digit);
    }
    value
        .checked_mul(1_000_000)
        .and_then(|v| v.checked_add(frac_value))
        .ok_or_else(overflow)
}

/// Parses a Unix-milliseconds string into a timestamp.
pub fn parse_millis(text: &str) -> Result<DateTime<Utc>, String> {
    text.parse::<i64>()
        .ok()
        .filter(|&millis| millis >= 0)
        .and_then(DateTime::from_timestamp_millis)
        .ok_or_else(|| format!("invalid millisecond timestamp {text:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exactly() {
        assert_eq!(parse_fixed6("0.022"), Ok(22_000));
        assert_eq!(parse_fixed6("52438.01"), Ok(52_438_010_000));
        assert_eq!(parse_fixed6("886"), Ok(886_000_000));
        assert_eq!(parse_fixed6("0"), Ok(0));
        assert_eq!(parse_fixed6("0.000001"), Ok(1));
        assert_eq!(parse_fixed6("1.1234560000"), Ok(1_123_456));
    }

    #[test]
    fn rejects_bad_input() {
        for text in [
            "",
            ".5",
            "5.",
            "-1",
            "1e3",
            "0.0000001",
            "1.2.3",
            " 1",
            "1,5",
            "9223372036855",
        ] {
            assert!(parse_fixed6(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn parses_millis() {
        let ts = parse_millis("1792777248021").unwrap();
        assert_eq!(ts.timestamp_micros(), 1_792_777_248_021_000);
        assert!(parse_millis("-1").is_err());
        assert!(parse_millis("12.5").is_err());
    }
}
