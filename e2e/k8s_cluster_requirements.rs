//! An incomplete external cluster must never qualify a replacement local stack.
use super::ClusterConfig;

const REQUIRED: [&str; 4] = [
    "SHARDLINE_K8S_E2E_BASE_URL",
    "SHARDLINE_K8S_E2E_S3_BUCKET",
    "SHARDLINE_K8S_E2E_POSTGRES_URL",
    "SHARDLINE_K8S_E2E_REDIS_URL",
];

fn configured_value(name: &str) -> Option<String> {
    match name {
        "SHARDLINE_K8S_E2E_BASE_URL" => Some("http://cluster.invalid".to_owned()),
        "SHARDLINE_K8S_E2E_S3_BUCKET" => Some("qualification".to_owned()),
        "SHARDLINE_K8S_E2E_POSTGRES_URL" => Some("postgres://private-fixture".to_owned()),
        "SHARDLINE_K8S_E2E_REDIS_URL" => Some("redis://private-fixture".to_owned()),
        _ => None,
    }
}

#[test]
fn unconfigured_cluster_allows_explicit_local_fixture() {
    assert!(
        ClusterConfig::from_values(false, |_| None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn partial_cluster_configuration_never_falls_back_to_local_services() {
    for missing in REQUIRED {
        let error = ClusterConfig::from_values(true, |name| {
            configured_value(name).filter(|_| name != missing)
        })
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains(missing));
        assert!(!message.contains("private-fixture"));
        assert!(!message.contains("cluster.invalid"));
    }
    assert!(ClusterConfig::from_values(true, |_| None).is_err());
}

#[test]
fn empty_required_cluster_values_are_errors() {
    for empty in REQUIRED {
        for contents in ["", " \t\n"] {
            assert!(
                ClusterConfig::from_values(true, |name| {
                    if name == empty {
                        Some(contents.to_owned())
                    } else {
                        configured_value(name)
                    }
                })
                .is_err()
            );
        }
    }
}

#[test]
fn complete_cluster_configuration_preserves_default_routes() {
    let config = ClusterConfig::from_values(true, configured_value)
        .unwrap()
        .unwrap();
    assert_eq!(config.base_url, "http://cluster.invalid");
    assert_eq!(config.transfer_url, config.base_url);
    assert_eq!(config.owner, "team");
    assert_eq!(config.repo, "assets");
    assert_eq!(config.provider, "generic");
    assert!(config.metrics_url.is_none());
}
