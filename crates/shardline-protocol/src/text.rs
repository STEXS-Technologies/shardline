/// Parses a boolean value from common operator-friendly strings.
///
/// Matching is case-insensitive and tolerant of leading/trailing ASCII
/// whitespace.
///
/// # Examples
///
/// ```
/// use shardline_protocol::parse_bool;
///
/// assert_eq!(parse_bool("true"), Some(true));
/// assert_eq!(parse_bool("1"), Some(true));
/// assert_eq!(parse_bool(" yes "), Some(true));
/// assert_eq!(parse_bool("OFF"), Some(false));
/// assert_eq!(parse_bool("maybe"), None);
/// ```
#[must_use]
pub fn parse_bool(value: &str) -> Option<bool> {
    match value.trim() {
        v if v.eq_ignore_ascii_case("true")
            || v.eq_ignore_ascii_case("1")
            || v.eq_ignore_ascii_case("yes")
            || v.eq_ignore_ascii_case("on") =>
        {
            Some(true)
        }
        v if v.eq_ignore_ascii_case("false")
            || v.eq_ignore_ascii_case("0")
            || v.eq_ignore_ascii_case("no")
            || v.eq_ignore_ascii_case("off") =>
        {
            Some(false)
        }
        _other => None,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn boolean_spellings_case_whitespace_and_invalid_values() {
        for (value, expected) in [
            ("true", Some(true)),
            ("1", Some(true)),
            ("yes", Some(true)),
            ("on", Some(true)),
            ("false", Some(false)),
            ("0", Some(false)),
            ("no", Some(false)),
            ("off", Some(false)),
            ("True", Some(true)),
            ("YES", Some(true)),
            ("On", Some(true)),
            ("False", Some(false)),
            ("NO", Some(false)),
            ("Off", Some(false)),
            ("", None),
            ("invalid", None),
            (" yes ", Some(true)),
            ("\ttrue\n", Some(true)),
            (" off ", Some(false)),
        ] {
            assert_eq!(super::parse_bool(value), expected, "{value:?}");
        }
    }
}
