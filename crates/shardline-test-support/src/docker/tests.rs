#![allow(clippy::expect_used)]

use super::*;

#[test]
fn mandatory_fixture_diagnostic_preserves_capacity_guidance_without_error_contents() {
    let secret = "https://access:private-token@example.invalid/?signature=private";
    let full = IoError::new(ErrorKind::StorageFull, secret);
    let diagnostic = fixture_requirement(Some(&full));
    assert!(diagnostic.contains("restore free disk space"));
    assert!(!diagnostic.contains(secret));
    let other = IoError::other(secret);
    assert_eq!(
        fixture_requirement(Some(&other)),
        "disposable Docker service fixture"
    );
    assert_eq!(
        fixture_requirement(None),
        "disposable Docker service fixture"
    );
}

#[cfg(unix)]
#[test]
fn failed_fixture_command_does_not_disclose_client_output() {
    let error = run_command_checked(
        Command::new("sh").args([
            "-c",
            "printf private-fixture-output; printf private-fixture-diagnostic >&2; exit 7",
        ]),
        "fixture control",
    )
    .expect_err("failed command must be rejected");
    let diagnostic = error.to_string();
    assert!(diagnostic.contains("fixture control"));
    assert!(diagnostic.contains('7'));
    assert!(!diagnostic.contains("private-fixture-output"));
    assert!(!diagnostic.contains("private-fixture-diagnostic"));
}

#[cfg(unix)]
#[test]
fn successful_fixture_command_preserves_captured_output() {
    let output = run_command_checked(
        Command::new("sh").args(["-c", "printf fixture-result; printf fixture-note >&2"]),
        "fixture control",
    )
    .expect("successful command must preserve its output");
    assert_eq!(output.stdout, b"fixture-result");
    assert_eq!(output.stderr, b"fixture-note");
}
