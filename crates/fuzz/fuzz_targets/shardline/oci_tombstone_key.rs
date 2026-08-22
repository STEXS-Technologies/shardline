#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_oci_adapter::{
    oci_blob_key_from_namespace, oci_manifest_key_from_namespace,
    oci_manifest_media_type_key_from_namespace,
};

fuzz_target!(|data: (&str, &str, &str)| {
    let (repository, digest_hex, scope_namespace) = data;
    let blob = oci_blob_key_from_namespace(repository, digest_hex, scope_namespace);
    let manifest = oci_manifest_key_from_namespace(repository, digest_hex, scope_namespace);
    let media_type =
        oci_manifest_media_type_key_from_namespace(repository, digest_hex, scope_namespace);

    assert_eq!(
        blob.is_ok(),
        manifest.is_ok(),
        "all tombstone key namespaces must accept the same identity"
    );
    assert_eq!(
        blob.is_ok(),
        media_type.is_ok(),
        "all tombstone sidecars must accept the same identity"
    );

    if let (Ok(blob), Ok(manifest), Ok(media_type)) = (blob, manifest, media_type) {
        assert_ne!(blob, manifest);
        assert_ne!(manifest, media_type);
        let repeated = oci_blob_key_from_namespace(repository, digest_hex, scope_namespace);
        assert!(
            matches!(repeated, Ok(ref key) if key == &blob),
            "an accepted tombstone identity must remain accepted and deterministic"
        );
    }
});
