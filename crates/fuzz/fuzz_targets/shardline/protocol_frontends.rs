#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_server::fuzz_protocol_frontend_summary;

fuzz_target!(|data: &[u8]| {
    let Ok(raw) = from_utf8(data) else {
        return;
    };

    let mut parts = raw.split('\0');
    let frontend = parts.next().unwrap_or_default();
    let oid = parts.next().unwrap_or_default();
    let digest = parts.next().unwrap_or_default();
    let repository = parts.next().unwrap_or_default();
    let reference = parts.next().unwrap_or_default();

    let summary = fuzz_protocol_frontend_summary(frontend, oid, digest, repository, reference);
    let Ok(summary) = summary else {
        return;
    };

    if summary.oci_blob_accepts || summary.oci_manifest_accepts {
        assert!(summary.digest_accepts);
        assert!(summary.oci_repository_accepts);
    }

    if summary.lfs_accepts || summary.bazel_accepts {
        assert_eq!(oid.len(), 64);
        assert!(oid.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    if summary.frontend_accepts {
        assert!(matches!(frontend, "xet" | "lfs" | "bazel-http" | "oci"));
    }
});
