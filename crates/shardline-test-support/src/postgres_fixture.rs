//! PostgreSQL qualification must not pass by silently skipping its fixture.

/// Return an optional local fixture URL, or fail mandatory qualification.
///
/// Set `SHARDLINE_REQUIRE_POSTGRES_TESTS=1` in a provisioned PostgreSQL gate.
/// Values and connection credentials are never included in diagnostics.
///
/// # Panics
/// Panics for empty/non-Unicode URLs, or a missing URL in mandatory mode.
#[track_caller]
#[must_use]
pub fn postgres_fixture_url(variable: &str) -> Option<String> {
    resolve_fixture(
        variable,
        std::env::var(variable),
        std::env::var_os("SHARDLINE_REQUIRE_POSTGRES_TESTS").is_some_and(|value| value == "1"),
    )
}

#[track_caller]
fn resolve_fixture(
    variable: &str,
    value: Result<String, std::env::VarError>,
    required: bool,
) -> Option<String> {
    let optional_missing = !required && matches!(&value, Err(std::env::VarError::NotPresent));
    assert!(
        optional_missing || value.as_ref().is_ok_and(|url| !url.trim().is_empty()),
        "PostgreSQL fixture {variable} is missing or invalid"
    );
    match value {
        Ok(url) => Some(url),
        Err(_error) => {
            eprintln!("SKIPPED: optional PostgreSQL fixture {variable} is not configured");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_fixture;
    use std::env::VarError;

    #[test]
    fn optional_missing_fixture_is_explicitly_unavailable() {
        assert!(resolve_fixture("TEST_URL", Err(VarError::NotPresent), false).is_none());
    }

    #[test]
    #[should_panic(expected = "PostgreSQL fixture TEST_URL is missing or invalid")]
    fn required_missing_fixture_fails() {
        let _url = resolve_fixture("TEST_URL", Err(VarError::NotPresent), true);
    }

    #[test]
    #[should_panic(expected = "PostgreSQL fixture TEST_URL is missing or invalid")]
    fn configured_empty_fixture_never_skips() {
        let _url = resolve_fixture("TEST_URL", Ok("  ".to_owned()), false);
    }

    #[test]
    fn configured_fixture_is_preserved_exactly() {
        let url = "postgres://user:secret@localhost/db";
        assert_eq!(
            resolve_fixture("TEST_URL", Ok(url.to_owned()), true).as_deref(),
            Some(url)
        );
    }

    #[test]
    #[should_panic(expected = "PostgreSQL fixture TEST_URL is missing or invalid")]
    fn invalid_unicode_fixture_is_not_treated_as_optional() {
        let _url = resolve_fixture(
            "TEST_URL",
            Err(VarError::NotUnicode(std::ffi::OsString::from(
                "withheld-secret",
            ))),
            false,
        );
    }
}
