use super::ServerFrontend;

#[test]
fn parses_supported_frontends() {
    assert_eq!(ServerFrontend::parse("xet"), Ok(ServerFrontend::Xet));
    assert!(ServerFrontend::parse("lfs").is_err());
}

#[test]
fn frontend_tokens_are_stable() {
    assert_eq!(ServerFrontend::Xet.as_str(), "xet");
}
