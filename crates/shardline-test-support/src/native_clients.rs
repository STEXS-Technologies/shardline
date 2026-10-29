//! Optional developer tools must become mandatory in release qualification.

/// Whether this process is running mandatory native-client qualification.
#[must_use]
pub fn native_clients_required() -> bool {
    std::env::var_os("SHARDLINE_REQUIRE_NATIVE_CLIENTS").is_some_and(|value| value == "1")
}

/// Returns availability for optional local tests; fails required qualification
/// when a client or its runtime dependencies cannot execute successfully.
///
/// # Panics
/// Panics when the tool is unavailable and native clients are required.
#[track_caller]
#[must_use]
pub fn require_native_client(program: &str, available: bool) -> bool {
    assert!(
        available || !native_clients_required(),
        "required native client {program} is missing or cannot run"
    );
    available
}
