#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    num::NonZeroU64,
};

use axum::http::{HeaderMap, HeaderValue};
use hmac::Mac;
use shardline_protocol::{SecretBytes, SecretString, TokenScope};
use shardline_vcs::{
    BuiltInProviderCatalog, BuiltInProviderError, GitHubAdapter, ProviderKind,
    ProviderRepositoryPolicy, ProviderSubject, ProviderTokenIssuer, RepositoryRef,
    RepositoryVisibility, RepositoryWebhookEventKind, configured_metadata,
};

use super::{
    BuiltInProvider, GITHUB_DELIVERY_HEADER, GITHUB_EVENT_HEADER, GITHUB_SIGNATURE_HEADER,
    MAX_PROVIDER_API_KEY_HEADER_BYTES, MAX_PROVIDER_CONFIG_BYTES,
    MAX_PROVIDER_WEBHOOK_AUTH_HEADER_BYTES, MAX_PROVIDER_WEBHOOK_DELIVERY_HEADER_BYTES,
    ProviderConfig, ProviderConfigDocument, ProviderRegistry, ProviderServiceError,
    ProviderTokenService, RepositoryPolicyConfig, parse_provider_config_document,
    set_before_provider_config_read_hook,
};
use crate::model::ProviderTokenIssueRequest;

#[test]
fn provider_service_rejects_missing_bootstrap_key() {
    let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
    assert!(issuer.is_ok());
    let Ok(issuer) = issuer else {
        return;
    };
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let result = service.issue_token(
        &HeaderMap::new(),
        "github",
        &ProviderTokenIssueRequest {
            subject: "github-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: Some("refs/heads/main".to_owned()),
            scope: TokenScope::Read,
        },
    );

    assert!(matches!(result, Err(ProviderServiceError::MissingApiKey)));
}

#[test]
fn provider_config_parse_zeroizes_raw_buffer_after_success() {
    let mut bytes = br#"{
            "providers": [{
                "kind": "github",
                "integration_subject": "github-app",
                "webhook_secret": "super-secret",
                "repositories": [{
                    "owner": "team",
                    "name": "assets",
                    "visibility": "private",
                    "default_revision": "main",
                    "clone_url": "https://github.example/team/assets.git",
                    "read_subjects": ["github-user-1"],
                    "write_subjects": ["github-user-1"]
                }]
            }]
        }"#
    .to_vec();

    let parsed = parse_provider_config_document(&mut bytes);

    assert!(parsed.is_ok());
    assert!(bytes.iter().all(|byte| *byte == 0));
}

#[test]
fn provider_config_parse_zeroizes_raw_buffer_after_failure() {
    let mut bytes = br#"{"providers":[{"webhook_secret":"super-secret"}]}"#.to_vec();

    let parsed = parse_provider_config_document(&mut bytes);

    assert!(parsed.is_err());
    assert!(bytes.iter().all(|byte| *byte == 0));
}

#[test]
fn provider_service_rejects_oversized_bootstrap_key_header() {
    let mut headers = HeaderMap::new();
    let oversized = "k".repeat(MAX_PROVIDER_API_KEY_HEADER_BYTES + 1);
    let header = HeaderValue::from_str(&oversized);
    assert!(header.is_ok());
    let Ok(header) = header else {
        return;
    };
    headers.insert("x-shardline-provider-key", header);
    let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
    assert!(issuer.is_ok());
    let Ok(issuer) = issuer else {
        return;
    };
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let result = service.issue_token(
        &headers,
        "github",
        &ProviderTokenIssueRequest {
            subject: "github-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: Some("refs/heads/main".to_owned()),
            scope: TokenScope::Read,
        },
    );

    assert!(matches!(result, Err(ProviderServiceError::InvalidApiKey)));
}

#[test]
fn provider_service_rejects_mismatched_bootstrap_key_length_before_lookup() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shardline-provider-key",
        HeaderValue::from_static("short"),
    );
    let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
    assert!(issuer.is_ok());
    let Ok(issuer) = issuer else {
        return;
    };
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let result = service.issue_token(
        &headers,
        "github",
        &ProviderTokenIssueRequest {
            subject: "github-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: Some("refs/heads/main".to_owned()),
            scope: TokenScope::Read,
        },
    );

    assert!(matches!(result, Err(ProviderServiceError::InvalidApiKey)));
}

#[test]
fn provider_token_service_debug_redacts_secret_material() {
    let issuer = ProviderTokenIssuer::new("issuer", &[5, 6, 7, 8], NonZeroU64::MIN);
    assert!(issuer.is_ok());
    let Ok(issuer) = issuer else {
        return;
    };
    let service = ProviderTokenService {
        api_key: SecretBytes::new(vec![1, 2, 3, 4]),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let rendered = format!("{service:?}");

    assert!(!rendered.contains("[1, 2, 3, 4]"));
    assert!(!rendered.contains("[5, 6, 7, 8]"));
    assert!(rendered.contains("***"));
}

#[test]
fn provider_service_issues_token_for_allowed_subject() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shardline-provider-key",
        HeaderValue::from_static("bootstrap"),
    );
    let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
    assert!(issuer.is_ok());
    let Ok(issuer) = issuer else {
        return;
    };
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: {
            let provider = github_provider();
            assert!(provider.is_ok());
            let Ok(provider) = provider else {
                return;
            };
            ProviderRegistry {
                providers: HashMap::from([("github".to_owned(), provider)]),
            }
        },
    };

    let response = service.issue_token(
        &headers,
        "github",
        &ProviderTokenIssueRequest {
            subject: "github-user-1".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Write,
        },
    );

    assert!(response.is_ok());
    let Ok(response) = response else {
        return;
    };
    assert_eq!(response.issuer, "issuer");
    assert_eq!(response.owner, "team");
    assert_eq!(response.repo, "assets");
    assert_eq!(response.revision.as_deref(), Some("refs/heads/main"));
    assert_eq!(response.scope, TokenScope::Write);
}

#[test]
fn provider_service_parses_github_repository_deleted_webhook() {
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer: {
            let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
            assert!(issuer.is_ok());
            let Ok(issuer) = issuer else {
                return;
            };
            issuer
        },
        registry: {
            let provider = github_provider();
            assert!(provider.is_ok());
            let Ok(provider) = provider else {
                return;
            };
            ProviderRegistry {
                providers: HashMap::from([("github".to_owned(), provider)]),
            }
        },
    };
    let body = br#"{
            "action":"deleted",
            "repository":{"full_name":"team/assets"}
        }"#;
    let signature = github_webhook_signature(body);
    assert!(signature.is_some());
    let Some(signature) = signature else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert(GITHUB_EVENT_HEADER, HeaderValue::from_static("repository"));
    headers.insert(
        GITHUB_DELIVERY_HEADER,
        HeaderValue::from_static("delivery-1"),
    );
    let signature_value = HeaderValue::from_str(&signature);
    assert!(signature_value.is_ok());
    let Ok(signature_value) = signature_value else {
        return;
    };
    headers.insert(GITHUB_SIGNATURE_HEADER, signature_value);

    let event = service.parse_webhook(&headers, "github", body);

    assert!(event.is_ok());
    let Ok(event) = event else {
        return;
    };
    let Some(event) = event else {
        return;
    };
    assert_eq!(event.repository().owner(), "team");
    assert_eq!(event.repository().name(), "assets");
    assert_eq!(event.kind(), &RepositoryWebhookEventKind::RepositoryDeleted);
}

#[test]
fn provider_service_rejects_oversized_webhook_delivery_header_before_adapter_parsing() {
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer: {
            let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
            assert!(issuer.is_ok());
            let Ok(issuer) = issuer else {
                return;
            };
            issuer
        },
        registry: {
            let provider = github_provider();
            assert!(provider.is_ok());
            let Ok(provider) = provider else {
                return;
            };
            ProviderRegistry {
                providers: HashMap::from([("github".to_owned(), provider)]),
            }
        },
    };
    let body = br#"{"action":"deleted","repository":{"full_name":"team/assets"}}"#;
    let signature = github_webhook_signature(body);
    assert!(signature.is_some());
    let Some(signature) = signature else {
        return;
    };
    let oversized_delivery = "d".repeat(MAX_PROVIDER_WEBHOOK_DELIVERY_HEADER_BYTES + 1);
    let mut headers = HeaderMap::new();
    headers.insert(GITHUB_EVENT_HEADER, HeaderValue::from_static("repository"));
    let delivery_value = HeaderValue::from_str(&oversized_delivery);
    assert!(delivery_value.is_ok());
    let Ok(delivery_value) = delivery_value else {
        return;
    };
    headers.insert(GITHUB_DELIVERY_HEADER, delivery_value);
    let signature_value = HeaderValue::from_str(&signature);
    assert!(signature_value.is_ok());
    let Ok(signature_value) = signature_value else {
        return;
    };
    headers.insert(GITHUB_SIGNATURE_HEADER, signature_value);

    let event = service.parse_webhook(&headers, "github", body);

    assert!(matches!(
        event,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidWebhookPayload
        ))
    ));
}

#[test]
fn provider_service_rejects_oversized_webhook_auth_header_before_adapter_parsing() {
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer: {
            let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
            assert!(issuer.is_ok());
            let Ok(issuer) = issuer else {
                return;
            };
            issuer
        },
        registry: {
            let provider = github_provider();
            assert!(provider.is_ok());
            let Ok(provider) = provider else {
                return;
            };
            ProviderRegistry {
                providers: HashMap::from([("github".to_owned(), provider)]),
            }
        },
    };
    let body = br#"{"action":"deleted","repository":{"full_name":"team/assets"}}"#;
    let oversized_signature = "s".repeat(MAX_PROVIDER_WEBHOOK_AUTH_HEADER_BYTES + 1);
    let mut headers = HeaderMap::new();
    headers.insert(GITHUB_EVENT_HEADER, HeaderValue::from_static("repository"));
    headers.insert(
        GITHUB_DELIVERY_HEADER,
        HeaderValue::from_static("delivery-1"),
    );
    let signature_value = HeaderValue::from_str(&oversized_signature);
    assert!(signature_value.is_ok());
    let Ok(signature_value) = signature_value else {
        return;
    };
    headers.insert(GITHUB_SIGNATURE_HEADER, signature_value);

    let event = service.parse_webhook(&headers, "github", body);

    assert!(matches!(
        event,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidWebhookAuthentication
        ))
    ));
}

#[test]
fn provider_service_rejects_non_utf8_webhook_auth_header_before_adapter_parsing() {
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer: {
            let issuer = ProviderTokenIssuer::new("issuer", b"signing-key", NonZeroU64::MIN);
            assert!(issuer.is_ok());
            let Ok(issuer) = issuer else {
                return;
            };
            issuer
        },
        registry: {
            let provider = github_provider();
            assert!(provider.is_ok());
            let Ok(provider) = provider else {
                return;
            };
            ProviderRegistry {
                providers: HashMap::from([("github".to_owned(), provider)]),
            }
        },
    };
    let body = br#"{"action":"deleted","repository":{"full_name":"team/assets"}}"#;
    let invalid_signature = HeaderValue::from_bytes(b"\xff\xfe\xfd");
    assert!(invalid_signature.is_ok());
    let Ok(invalid_signature) = invalid_signature else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert(GITHUB_EVENT_HEADER, HeaderValue::from_static("repository"));
    headers.insert(
        GITHUB_DELIVERY_HEADER,
        HeaderValue::from_static("delivery-1"),
    );
    headers.insert(GITHUB_SIGNATURE_HEADER, invalid_signature);

    let event = service.parse_webhook(&headers, "github", body);

    assert!(matches!(
        event,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidWebhookAuthentication
        ))
    ));
}

#[test]
fn provider_registry_rejects_missing_webhook_secret() {
    let registry = ProviderRegistry::from_document(ProviderConfigDocument {
        providers: vec![ProviderConfig {
            kind: "github".to_owned(),
            integration_subject: "github-app".to_owned(),
            webhook_secret: None,
            repositories: vec![RepositoryPolicyConfig {
                owner: "team".to_owned(),
                name: "assets".to_owned(),
                visibility: "private".to_owned(),
                default_revision: "main".to_owned(),
                clone_url: "https://github.example/team/assets.git".to_owned(),
                read_subjects: vec!["github-user-1".to_owned()],
                write_subjects: vec!["github-user-1".to_owned()],
            }],
        }],
    });

    assert!(matches!(
        registry,
        Err(ProviderServiceError::MissingWebhookSecret)
    ));
}

#[test]
fn provider_registry_rejects_empty_webhook_secret() {
    let registry = ProviderRegistry::from_document(ProviderConfigDocument {
        providers: vec![ProviderConfig {
            kind: "github".to_owned(),
            integration_subject: "github-app".to_owned(),
            webhook_secret: Some(SecretString::from_secret("   ")),
            repositories: vec![RepositoryPolicyConfig {
                owner: "team".to_owned(),
                name: "assets".to_owned(),
                visibility: "private".to_owned(),
                default_revision: "main".to_owned(),
                clone_url: "https://github.example/team/assets.git".to_owned(),
                read_subjects: vec!["github-user-1".to_owned()],
                write_subjects: vec!["github-user-1".to_owned()],
            }],
        }],
    });

    assert!(matches!(
        registry,
        Err(ProviderServiceError::EmptyWebhookSecret)
    ));
}

#[test]
fn provider_service_rejects_oversized_configuration_before_json_parsing() {
    let config = tempfile::NamedTempFile::new();
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let resized = config.as_file().set_len(MAX_PROVIDER_CONFIG_BYTES + 1);
    assert!(resized.is_ok());

    let service = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"signing-key",
    );

    assert!(matches!(
        service,
        Err(ProviderServiceError::ConfigTooLarge {
            maximum_bytes: MAX_PROVIDER_CONFIG_BYTES,
            ..
        })
    ));
}

#[test]
fn provider_service_rejects_configuration_growth_after_validation() {
    let config = tempfile::NamedTempFile::new();
    assert!(config.is_ok());
    let Ok(mut config) = config else {
        return;
    };
    let initial_write = config.write_all(br#"{"providers":[]}"#);
    assert!(initial_write.is_ok());
    let config_sync = config.as_file().sync_all();
    assert!(config_sync.is_ok());

    let writer = OpenOptions::new().append(true).open(config.path());
    assert!(writer.is_ok());
    let Ok(mut writer) = writer else {
        return;
    };
    set_before_provider_config_read_hook(config.path().to_path_buf(), move || {
        let appended = writer.write_all(b" ");
        assert!(appended.is_ok());
        let writer_sync = writer.sync_all();
        assert!(writer_sync.is_ok());
    });

    let service = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"signing-key",
    );

    assert!(matches!(
        service,
        Err(ProviderServiceError::ConfigLengthMismatch)
    ));
}

#[cfg(unix)]
#[test]
fn provider_service_accepts_projected_secret_symlink_configuration_path() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let data_dir = temp.path().join("..data");
    let created = fs::create_dir(&data_dir);
    assert!(created.is_ok());
    let target = data_dir.join("target-providers.json");
    let link = temp.path().join("linked-providers.json");
    let write = fs::write(&target, br#"{"providers":[]}"#);
    assert!(write.is_ok());
    let linked = symlink(
        std::path::Path::new("..data").join("target-providers.json"),
        &link,
    );
    assert!(linked.is_ok());

    let service = ProviderTokenService::from_file(
        &link,
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"signing-key",
    );

    assert!(service.is_ok());
}

#[cfg(unix)]
#[test]
fn provider_service_rejects_symlinked_configuration_path_outside_directory() {
    let temp = tempfile::tempdir();
    assert!(temp.is_ok());
    let Ok(temp) = temp else {
        return;
    };
    let outside = tempfile::NamedTempFile::new();
    assert!(outside.is_ok());
    let Ok(outside) = outside else {
        return;
    };
    let write = fs::write(outside.path(), br#"{"providers":[]}"#);
    assert!(write.is_ok());
    let link = temp.path().join("linked-providers.json");
    let linked = symlink(outside.path(), &link);
    assert!(linked.is_ok());

    let service = ProviderTokenService::from_file(
        &link,
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"signing-key",
    );

    assert!(matches!(service, Err(ProviderServiceError::Io(_))));
}

fn github_provider() -> Result<BuiltInProvider, ProviderServiceError> {
    let mut catalog = BuiltInProviderCatalog::new("github-app")?;
    let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?;
    let subject = ProviderSubject::new("github-user-1")?;
    let metadata = configured_metadata(
        repository,
        RepositoryVisibility::Private,
        "main",
        "https://github.example/team/assets.git",
    )?;
    catalog.register(ProviderRepositoryPolicy::new(
        metadata,
        HashSet::from([subject.clone()]),
        HashSet::from([subject]),
    ))?;

    Ok(BuiltInProvider::GitHub(GitHubAdapter::new(
        catalog,
        Some(SecretString::from_secret("secret")),
    )))
}

fn github_webhook_signature(body: &[u8]) -> Option<String> {
    let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
    assert!(mac.is_ok());
    let Ok(mut mac) = mac else {
        return None;
    };
    mac.update(body);
    Some(format!(
        "sha256={}",
        hex::encode(mac.finalize().into_bytes())
    ))
}
