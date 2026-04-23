use std::{fs::read_to_string, path::Path};

#[test]
fn production_api_manifest_pins_secret_volume_permissions() {
    let manifest = read_manifest("docs/k8s/production-scaled/api-deployment.yaml");

    assert!(manifest.contains("fsGroup: 999"));
    assert!(manifest.contains("fsGroupChangePolicy: OnRootMismatch"));
    assert!(manifest.contains("secretName: shardline-runtime"));
    assert!(manifest.contains("defaultMode: 0440"));
    assert!(manifest.contains("secretName: shardline-provider-catalog"));
    assert!(manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID_FILE"));
    assert!(manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY_FILE"));
    assert!(!manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID\n"));
    assert!(!manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY\n"));
}

#[test]
fn production_transfer_manifest_pins_secret_volume_permissions() {
    let manifest = read_manifest("docs/k8s/production-scaled/transfer-deployment.yaml");

    assert!(manifest.contains("fsGroup: 999"));
    assert!(manifest.contains("fsGroupChangePolicy: OnRootMismatch"));
    assert!(manifest.contains("secretName: shardline-runtime"));
    assert!(manifest.contains("defaultMode: 0440"));
    assert!(manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID_FILE"));
    assert!(manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY_FILE"));
    assert!(!manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID\n"));
    assert!(!manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY\n"));
}

#[test]
fn production_gc_manifest_pins_secret_volume_permissions() {
    let manifest = read_manifest("docs/k8s/production-scaled/gc-cronjob.yaml");

    assert!(manifest.contains("fsGroup: 999"));
    assert!(manifest.contains("fsGroupChangePolicy: OnRootMismatch"));
    assert!(manifest.contains("secretName: shardline-runtime"));
    assert!(manifest.contains("defaultMode: 0440"));
    assert!(manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID_FILE"));
    assert!(manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY_FILE"));
    assert!(!manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID\n"));
    assert!(!manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY\n"));
}

fn read_manifest(path: &str) -> String {
    let result = read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(path),
    );
    assert!(result.is_ok());
    result.unwrap_or_default()
}
