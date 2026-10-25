//! Explicitly selected deployment qualification must exercise a real endpoint.
use super::KindDeploymentConfig;

const API: &str = "SHARDLINE_KIND_SMOKE_API_URL";
const TRANSFER: &str = "SHARDLINE_KIND_SMOKE_TRANSFER_URL";

fn endpoint(name: &str) -> Option<String> {
    match name {
        API => Some("http://api.invalid".to_owned()),
        TRANSFER => Some("http://transfer.invalid".to_owned()),
        _ => None,
    }
}

#[test]
fn missing_deployment_configuration_cannot_pass_qualification() {
    assert!(KindDeploymentConfig::from_values(|_| None).is_err());
    for missing in [API, TRANSFER] {
        let error =
            KindDeploymentConfig::from_values(|name| endpoint(name).filter(|_| name != missing))
                .unwrap_err();
        let message = error.to_string();
        assert!(message.contains(missing));
        assert!(!message.contains(".invalid"));
    }
}

#[test]
fn blank_deployment_endpoints_cannot_pass_qualification() {
    for empty in [API, TRANSFER] {
        for contents in ["", " \t\n"] {
            assert!(
                KindDeploymentConfig::from_values(|name| {
                    if name == empty {
                        Some(contents.to_owned())
                    } else {
                        endpoint(name)
                    }
                })
                .is_err()
            );
        }
    }
}

#[test]
fn complete_deployment_configuration_preserves_manual_defaults() {
    let config = KindDeploymentConfig::from_values(endpoint).unwrap();
    assert_eq!(config.api_url, "http://api.invalid");
    assert_eq!(config.transfer_url, "http://transfer.invalid");
    assert_eq!(config.run_id, "manual");
    assert!(config.previous_run_id.is_none());
}
