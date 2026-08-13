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
    fn parse_true_strings() {
        assert_eq!(super::parse_bool("true"), Some(true));
        assert_eq!(super::parse_bool("1"), Some(true));
        assert_eq!(super::parse_bool("yes"), Some(true));
        assert_eq!(super::parse_bool("on"), Some(true));
    }

    #[test]
    fn parse_false_strings() {
        assert_eq!(super::parse_bool("false"), Some(false));
        assert_eq!(super::parse_bool("0"), Some(false));
        assert_eq!(super::parse_bool("no"), Some(false));
        assert_eq!(super::parse_bool("off"), Some(false));
    }

    #[test]
    fn case_insensitive_true() {
        assert_eq!(super::parse_bool("True"), Some(true));
        assert_eq!(super::parse_bool("YES"), Some(true));
        assert_eq!(super::parse_bool("On"), Some(true));
    }

    #[test]
    fn case_insensitive_false() {
        assert_eq!(super::parse_bool("False"), Some(false));
        assert_eq!(super::parse_bool("NO"), Some(false));
        assert_eq!(super::parse_bool("Off"), Some(false));
    }

    #[test]
    fn parse_empty() {
        assert_eq!(super::parse_bool(""), None);
    }

    #[test]
    fn parse_invalid() {
        assert_eq!(super::parse_bool("invalid"), None);
    }

    #[test]
    fn parse_surrounding_whitespace() {
        assert_eq!(super::parse_bool(" yes "), Some(true));
        assert_eq!(super::parse_bool("\ttrue\n"), Some(true));
        assert_eq!(super::parse_bool(" off "), Some(false));
    }
}
