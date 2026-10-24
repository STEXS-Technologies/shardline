//! Shared policy for optional developer tools and required CI qualification.
pub fn require_available(program: &str, available: bool) -> bool {
    shardline_test_support::require_native_client(program, available)
}
