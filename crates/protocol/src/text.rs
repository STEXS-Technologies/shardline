/// Parses a boolean value from common operator-friendly strings.
#[must_use]
pub fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _other => None,
    }
}
