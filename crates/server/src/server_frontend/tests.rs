use super::ServerFrontend;

#[test]
fn parses_supported_frontends() {
    assert_eq!(ServerFrontend::parse("xet"), Ok(ServerFrontend::Xet));
    assert_eq!(ServerFrontend::parse("lfs"), Ok(ServerFrontend::Lfs));
    assert_eq!(
        ServerFrontend::parse("bazel-http"),
        Ok(ServerFrontend::BazelHttp)
    );
    assert_eq!(ServerFrontend::parse("oci"), Ok(ServerFrontend::Oci));
    assert!(ServerFrontend::parse("nope").is_err());
}

#[test]
fn frontend_tokens_are_stable() {
    assert_eq!(ServerFrontend::Xet.as_str(), "xet");
    assert_eq!(ServerFrontend::Lfs.as_str(), "lfs");
    assert_eq!(ServerFrontend::BazelHttp.as_str(), "bazel-http");
    assert_eq!(ServerFrontend::Oci.as_str(), "oci");
}
