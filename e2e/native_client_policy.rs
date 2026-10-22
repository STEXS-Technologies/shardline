//! Shared policy for optional developer tools and required CI qualification.
pub fn required() -> bool {
    std::env::var_os("SHARDLINE_REQUIRE_NATIVE_CLIENTS").is_some_and(|value| value == "1")
}

pub fn require_available(program: &str, available: bool) -> bool {
    assert!(
        available || !required(),
        "required native client {program} is missing or cannot run"
    );
    available
}
