//! Explicit Xet file-information media negotiation.

use super::*;

#[test]
fn xet_metadata_requires_explicit_acceptable_media_type() {
    for (accept, expected) in [
        ("*/*", false),
        ("application/octet-stream", false),
        ("application/vnd.xet-fileinfo+json, */*", true),
        ("application/vnd.xet-fileinfo+json;q=0, */*", false),
        ("application/vnd.xet-fileinfo+json;q=invalid", false),
        ("application/vnd.xet-fileinfo+json;q=0.5", true),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("accept", accept.parse().unwrap());
        assert_eq!(accepts_xet_fileinfo(&headers), expected, "{accept}");
    }
}
