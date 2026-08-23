use std::{fs::read_to_string, path::Path};

#[test]
fn production_api_manifest_pins_secret_volume_permissions() {
    let manifest = read_manifest("docs/k8s/production-scaled/api-deployment.yaml");

    assert!(manifest.contains("fsGroup: 1000"));
    assert!(manifest.contains("fsGroupChangePolicy: OnRootMismatch"));
    assert!(manifest.contains("secretName: shardline-runtime"));
    assert!(manifest.contains("defaultMode: 0440"));
    assert!(manifest.contains("secretName: shardline-provider-catalog"));
    assert!(manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID_FILE"));
    assert!(manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY_FILE"));
    assert!(!manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID\n"));
    assert!(!manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY\n"));
    assert!(manifest.contains("- name: root\n          emptyDir: {}"));
    assert!(!manifest.contains("persistentVolumeClaim:"));
}

#[test]
fn production_transfer_manifest_pins_secret_volume_permissions() {
    let manifest = read_manifest("docs/k8s/production-scaled/transfer-deployment.yaml");

    assert!(manifest.contains("fsGroup: 1000"));
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

    assert!(manifest.contains("fsGroup: 1000"));
    assert!(manifest.contains("fsGroupChangePolicy: OnRootMismatch"));
    assert!(manifest.contains("secretName: shardline-runtime"));
    assert!(manifest.contains("defaultMode: 0440"));
    assert!(manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID_FILE"));
    assert!(manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY_FILE"));
    assert!(!manifest.contains("SHARDLINE_S3_ACCESS_KEY_ID\n"));
    assert!(!manifest.contains("SHARDLINE_S3_SECRET_ACCESS_KEY\n"));
}

#[test]
fn production_scaled_profile_keeps_role_disruption_budgets_and_autoscalers() {
    for (role, deployment) in [("api", "shardline-api"), ("transfer", "shardline-transfer")] {
        let hpa = read_manifest(&format!("docs/k8s/production-scaled/{role}-hpa.yaml"));
        assert!(hpa.contains("apiVersion: autoscaling/v2"));
        assert!(hpa.contains("kind: HorizontalPodAutoscaler"));
        assert!(hpa.contains(&format!("name: {deployment}")));
        assert!(hpa.contains("minReplicas:"));
        assert!(hpa.contains("maxReplicas:"));
        assert!(hpa.contains("type: Resource"));

        let pdb = read_manifest(&format!("docs/k8s/production-scaled/{role}-pdb.yaml"));
        assert!(pdb.contains("apiVersion: policy/v1"));
        assert!(pdb.contains("kind: PodDisruptionBudget"));
        assert!(pdb.contains("minAvailable: 1"));
        assert!(pdb.contains(&format!("component: {role}")));
    }
}

#[test]
fn production_scaled_profile_keeps_default_deny_and_explicit_ingress_routes() {
    let kustomization = read_manifest("docs/k8s/production-scaled/kustomization.yaml");
    assert!(!kustomization.contains("api-staging-pvc.yaml"));
    for resource in [
        "networkpolicy-default-deny-ingress.yaml",
        "networkpolicy-allow-ingress-nginx.yaml",
        "networkpolicy-allow-monitoring.yaml",
        "ingress-nginx.yaml",
    ] {
        assert!(
            kustomization.contains(resource),
            "production profile must include {resource}"
        );
    }

    let deny = read_manifest("docs/k8s/production-scaled/networkpolicy-default-deny-ingress.yaml");
    assert!(deny.contains("policyTypes:"));
    assert!(deny.contains("- Ingress"));
    assert!(deny.contains("podSelector: {}"));

    let ingress = read_manifest("docs/k8s/production-scaled/ingress-nginx.yaml");
    assert!(ingress.contains("path: /transfer/xorb"));
    assert!(ingress.contains("name: shardline-transfer"));
    assert!(ingress.contains("path: /"));
    assert!(ingress.contains("name: shardline-api"));
}

#[test]
fn production_workloads_enforce_restricted_runtime_defaults() {
    for manifest_path in [
        "docs/k8s/production-scaled/api-deployment.yaml",
        "docs/k8s/production-scaled/transfer-deployment.yaml",
        "docs/k8s/production-scaled/gc-cronjob.yaml",
    ] {
        let manifest = read_manifest(manifest_path);
        assert!(manifest.contains("runAsNonRoot: true"));
        assert!(manifest.contains("readOnlyRootFilesystem: true"));
        assert!(manifest.contains("allowPrivilegeEscalation: false"));
        assert!(manifest.contains("drop: [\"ALL\"]"));
        assert!(manifest.contains("seccompProfile:"));
        assert!(manifest.contains("type: RuntimeDefault"));
    }
}

#[test]
fn production_image_does_not_bake_in_a_loopback_public_url() {
    let dockerfile = read_manifest("Dockerfile");
    assert!(!dockerfile.contains("ENV SHARDLINE_PUBLIC_BASE_URL="));
}

#[test]
fn production_and_kind_configs_use_a_supported_auth_provider() {
    for manifest_path in [
        "docs/k8s/production-scaled/configmap.yaml",
        "tests/k8s/kind/configmap.patch.yaml",
    ] {
        let manifest = read_manifest(manifest_path);
        assert!(manifest.contains("provider = \"local\""));
        assert!(!manifest.contains("provider = \"local-hmac\""));
    }
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
