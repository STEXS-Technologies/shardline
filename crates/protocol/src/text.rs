/// Parses a boolean value from common operator-friendly strings.
#[must_use]
pub fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
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
    fn case_sensitive_true() {
        assert_eq!(super::parse_bool("True"), None);
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
    fn parse_trailing_whitespace() {
        assert_eq!(super::parse_bool("yes "), None);
    }
}
