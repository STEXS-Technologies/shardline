#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_server::fuzz_oci_frontend_summary;

fuzz_target!(|data: &[u8]| {
    let Ok(raw) = from_utf8(data) else {
        return;
    };

    let mut parts = raw.split('\0');
    let repository = parts.next().unwrap_or_default();
    let reference = parts.next().unwrap_or_default();
    let digest = parts.next().unwrap_or_default();
    let session_id = parts.next().unwrap_or_default();
    let content_range = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    let Ok(summary) = fuzz_oci_frontend_summary(
        repository,
        reference,
        digest,
        session_id,
        content_range,
        path,
    ) else {
        return;
    };

    if summary.blob_accepts || summary.manifest_accepts {
        assert!(summary.repository_accepts);
        assert!(summary.digest_accepts);
    }

    if summary.path_accepts && path.contains("/blobs/sha256:") {
        assert!(summary.digest_accepts || path.contains("sha256:"));
    }
});
