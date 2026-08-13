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
use serde_json::json;
use serial_test::serial;
use shardline_protocol::{SecretBytes, SecretString, TokenScope};
use shardline_server_core::at_rest::AtRestCipher;
use shardline_vcs::{
    AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError, GitHubAdapter,
    ProviderBoundaryError, ProviderKind, ProviderRepositoryPolicy, ProviderSubject,
    ProviderTokenIssuer, RepositoryAccess, RepositoryRef, RepositoryVisibility,
    RepositoryWebhookEventKind, RevisionRef, configured_metadata,
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
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    );
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
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    );
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
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    );
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
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        &[
            5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
            28, 29, 30, 31, 32, 33, 34, 35, 36,
        ],
        NonZeroU64::MIN,
    );
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
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    );
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
            let issuer = ProviderTokenIssuer::new(
                "issuer",
                b"a]32-byte-signing-key-for-testing!",
                NonZeroU64::MIN,
            );
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
            let issuer = ProviderTokenIssuer::new(
                "issuer",
                b"a]32-byte-signing-key-for-testing!",
                NonZeroU64::MIN,
            );
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
            let issuer = ProviderTokenIssuer::new(
                "issuer",
                b"a]32-byte-signing-key-for-testing!",
                NonZeroU64::MIN,
            );
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
            let issuer = ProviderTokenIssuer::new(
                "issuer",
                b"a]32-byte-signing-key-for-testing!",
                NonZeroU64::MIN,
            );
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
    let registry = ProviderRegistry::from_document(
        ProviderConfigDocument {
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
        },
        None,
    );

    assert!(matches!(
        registry,
        Err(ProviderServiceError::MissingWebhookSecret)
    ));
}

#[test]
fn provider_registry_rejects_empty_webhook_secret() {
    let registry = ProviderRegistry::from_document(
        ProviderConfigDocument {
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
        },
        None,
    );

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
        b"a]32-byte-signing-key-for-testing!",
        None,
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
#[serial(provider_config_hook)]
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
        b"a]32-byte-signing-key-for-testing!",
        None,
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
        b"a]32-byte-signing-key-for-testing!",
        None,
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
        b"a]32-byte-signing-key-for-testing!",
        None,
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

// ── visibility ──────────────────────────────────────────────────────────

#[test]
fn visibility_private() {
    assert_eq!(super::visibility("private"), RepositoryVisibility::Private);
}

#[test]
fn visibility_internal() {
    assert_eq!(
        super::visibility("internal"),
        RepositoryVisibility::Internal
    );
}

#[test]
fn visibility_public() {
    assert_eq!(super::visibility("public"), RepositoryVisibility::Public);
}

#[test]
fn visibility_unknown_defaults_to_public() {
    assert_eq!(
        super::visibility("unknown-value"),
        RepositoryVisibility::Public
    );
}

#[test]
fn visibility_empty_defaults_to_public() {
    assert_eq!(super::visibility(""), RepositoryVisibility::Public);
}

// ── parse_provider_kind ────────────────────────────────────────────────

#[test]
fn parse_provider_kind_github() {
    let result = super::parse_provider_kind("github");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProviderKind::GitHub);
}

#[test]
fn parse_provider_kind_gitea() {
    let result = super::parse_provider_kind("gitea");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProviderKind::Gitea);
}

#[test]
fn parse_provider_kind_gitlab() {
    let result = super::parse_provider_kind("gitlab");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProviderKind::GitLab);
}

#[test]
fn parse_provider_kind_codeberg() {
    let result = super::parse_provider_kind("codeberg");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProviderKind::Codeberg);
}

#[test]
fn parse_provider_kind_generic() {
    let result = super::parse_provider_kind("generic");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProviderKind::Generic);
}

#[test]
fn parse_provider_kind_unknown() {
    let result = super::parse_provider_kind("unknown");
    assert!(matches!(result, Err(ProviderServiceError::UnknownProvider)));
}

#[test]
fn parse_provider_kind_empty_string() {
    let result = super::parse_provider_kind("");
    assert!(matches!(result, Err(ProviderServiceError::UnknownProvider)));
}

// ── webhook_request ─────────────────────────────────────────────────────

#[test]
fn webhook_request_returns_ok_with_valid_headers() {
    use axum::http::HeaderValue;
    let mut headers = HeaderMap::new();
    headers.insert("x-github-event", HeaderValue::from_static("push"));
    headers.insert("x-github-delivery", HeaderValue::from_static("abc-123"));
    headers.insert(
        "x-hub-signature-256",
        HeaderValue::from_static("sha256=abc"),
    );
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::GitHub, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.event_name(), "push");
    assert_eq!(request.delivery_id(), "abc-123");
    assert!(request.signature().is_some());
}

#[test]
fn webhook_request_missing_headers_uses_defaults() {
    let headers = HeaderMap::new();
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::GitHub, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    // Missing event header defaults to empty string
    assert_eq!(request.event_name(), "");
    assert_eq!(request.delivery_id(), "");
    assert!(request.signature().is_none());
}

#[test]
fn webhook_request_rejects_oversized_event_header() {
    let mut headers = HeaderMap::new();
    let oversized = "x".repeat(600);
    headers.insert("x-github-event", HeaderValue::from_str(&oversized).unwrap());
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::GitHub, &headers, body);
    assert!(matches!(
        result,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidWebhookPayload
        ))
    ));
}

#[test]
fn webhook_request_rejects_oversized_signature_header() {
    let mut headers = HeaderMap::new();
    let oversized = "x".repeat(5000);
    headers.insert(
        "x-hub-signature-256",
        HeaderValue::from_str(&oversized).unwrap(),
    );
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::GitHub, &headers, body);
    assert!(matches!(
        result,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidWebhookAuthentication
        ))
    ));
}

// ── ProviderServiceError Display tests ─────────────────────────────────

#[test]
fn provider_service_error_display_empty_api_key() {
    let err = ProviderServiceError::EmptyApiKey;
    assert_eq!(err.to_string(), "provider bootstrap key must not be empty");
}

#[test]
fn provider_service_error_display_api_key_too_large() {
    let err = ProviderServiceError::ApiKeyTooLarge;
    assert_eq!(
        err.to_string(),
        "provider bootstrap key exceeded the supported metadata size"
    );
}

#[test]
fn provider_service_error_display_missing_api_key() {
    let err = ProviderServiceError::MissingApiKey;
    assert_eq!(err.to_string(), "provider bootstrap key is missing");
}

#[test]
fn provider_service_error_display_invalid_api_key() {
    let err = ProviderServiceError::InvalidApiKey;
    assert_eq!(err.to_string(), "provider bootstrap key is invalid");
}

#[test]
fn provider_service_error_display_config_too_large() {
    let err = ProviderServiceError::ConfigTooLarge {
        observed_bytes: 100,
        maximum_bytes: 50,
    };
    let display = err.to_string();
    assert!(display.contains("provider configuration exceeded the bounded parser ceiling"));
}

#[test]
fn provider_service_error_display_config_length_mismatch() {
    let err = ProviderServiceError::ConfigLengthMismatch;
    assert_eq!(
        err.to_string(),
        "provider configuration length did not match the validated length"
    );
}

#[test]
fn provider_service_error_display_duplicate_provider() {
    let err = ProviderServiceError::DuplicateProvider;
    assert_eq!(err.to_string(), "provider is configured more than once");
}

#[test]
fn provider_service_error_display_missing_webhook_secret() {
    let err = ProviderServiceError::MissingWebhookSecret;
    assert_eq!(
        err.to_string(),
        "provider webhook secret must be configured"
    );
}

#[test]
fn provider_service_error_display_empty_webhook_secret() {
    let err = ProviderServiceError::EmptyWebhookSecret;
    assert_eq!(err.to_string(), "provider webhook secret must not be empty");
}

#[test]
fn provider_service_error_display_unknown_provider() {
    let err = ProviderServiceError::UnknownProvider;
    assert_eq!(err.to_string(), "provider is not configured");
}

#[test]
fn provider_service_error_display_denied() {
    let err = ProviderServiceError::Denied;
    assert_eq!(
        err.to_string(),
        "provider denied requested repository access"
    );
}

#[test]
fn provider_service_error_display_debug_empty_api_key() {
    let err = ProviderServiceError::EmptyApiKey;
    let debug = format!("{err:?}");
    assert!(debug.contains("EmptyApiKey"));
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

// ── optional_bounded_header_value ────────────────────────────────────────

#[test]
fn optional_bounded_header_value_returns_none_when_header_missing() {
    let headers = HeaderMap::new();
    let result = super::optional_bounded_header_value(
        &headers,
        "x-nonexistent",
        100,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookPayload),
    );
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn optional_bounded_header_value_returns_value_when_within_bounds() {
    let mut headers = HeaderMap::new();
    headers.insert("x-my-header", HeaderValue::from_static("hello"));
    let result = super::optional_bounded_header_value(
        &headers,
        "x-my-header",
        100,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookPayload),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("hello"));
}

#[test]
fn optional_bounded_header_value_rejects_oversized_header() {
    let mut headers = HeaderMap::new();
    let oversized = "x".repeat(200);
    let value = HeaderValue::from_str(&oversized).unwrap();
    headers.insert("x-oversized", value);
    let result = super::optional_bounded_header_value(
        &headers,
        "x-oversized",
        100,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookAuthentication),
    );
    assert!(result.is_err());
}

#[test]
fn optional_bounded_header_value_with_valid_ascii_header_returns_ok() {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_static("simple-ascii-value");
    headers.insert("x-ascii-header", value);
    let result = super::optional_bounded_header_value(
        &headers,
        "x-ascii-header",
        100,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookAuthentication),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some("simple-ascii-value"));
}

#[test]
fn optional_bounded_header_value_exact_boundary_is_ok() {
    let mut headers = HeaderMap::new();
    let exact = "a".repeat(50);
    let value = HeaderValue::from_str(&exact).unwrap();
    headers.insert("x-exact", value);
    let result = super::optional_bounded_header_value(
        &headers,
        "x-exact",
        50,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookPayload),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(exact.as_str()));
}

// ── ProviderTokenService::from_file — API key validation ────────────────

#[test]
fn provider_service_from_file_rejects_empty_api_key() {
    let mut config = tempfile::NamedTempFile::new().unwrap();
    config.write_all(br#"{"providers":[]}"#).unwrap();
    config.as_file().sync_all().unwrap();

    let result = ProviderTokenService::from_file(
        config.path(),
        vec![],
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        None,
    );

    assert!(matches!(result, Err(ProviderServiceError::EmptyApiKey)));
}

#[test]
fn provider_service_from_file_rejects_oversized_api_key() {
    let mut config = tempfile::NamedTempFile::new().unwrap();
    config.write_all(br#"{"providers":[]}"#).unwrap();
    config.as_file().sync_all().unwrap();

    let oversized = vec![0u8; MAX_PROVIDER_API_KEY_HEADER_BYTES + 1];
    let result = ProviderTokenService::from_file(
        config.path(),
        oversized,
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        None,
    );

    assert!(matches!(result, Err(ProviderServiceError::ApiKeyTooLarge)));
}

// ── ProviderRegistry duplicate provider ──────────────────────────────────

#[test]
fn provider_registry_rejects_duplicate_provider() {
    let document = ProviderConfigDocument {
        providers: vec![
            ProviderConfig {
                kind: "github".to_owned(),
                integration_subject: "github-app".to_owned(),
                webhook_secret: Some(SecretString::from_secret("secret")),
                repositories: vec![RepositoryPolicyConfig {
                    owner: "team".to_owned(),
                    name: "assets".to_owned(),
                    visibility: "private".to_owned(),
                    default_revision: "main".to_owned(),
                    clone_url: "https://github.example/team/assets.git".to_owned(),
                    read_subjects: vec!["user-1".to_owned()],
                    write_subjects: vec!["user-1".to_owned()],
                }],
            },
            ProviderConfig {
                kind: "github".to_owned(),
                integration_subject: "github-app-2".to_owned(),
                webhook_secret: Some(SecretString::from_secret("secret-2")),
                repositories: vec![],
            },
        ],
    };

    let result = ProviderRegistry::from_document(document, None);
    assert!(matches!(
        result,
        Err(ProviderServiceError::DuplicateProvider)
    ));
}

#[test]
fn provider_registry_accepts_different_provider_kinds() {
    let document = ProviderConfigDocument {
        providers: vec![
            ProviderConfig {
                kind: "github".to_owned(),
                integration_subject: "github-app".to_owned(),
                webhook_secret: Some(SecretString::from_secret("secret")),
                repositories: vec![],
            },
            ProviderConfig {
                kind: "gitlab".to_owned(),
                integration_subject: "gitlab-app".to_owned(),
                webhook_secret: Some(SecretString::from_secret("secret-2")),
                repositories: vec![],
            },
        ],
    };

    let result = ProviderRegistry::from_document(document, None);
    assert!(result.is_ok());
}

// ── BuiltInProvider::from_config — all provider kinds ────────────────────

#[test]
fn built_in_provider_from_config_gitea() {
    let config = ProviderConfig {
        kind: "gitea".to_owned(),
        integration_subject: "gitea-app".to_owned(),
        webhook_secret: Some(SecretString::from_secret("gitea-secret")),
        repositories: vec![RepositoryPolicyConfig {
            owner: "team".to_owned(),
            name: "assets".to_owned(),
            visibility: "public".to_owned(),
            default_revision: "main".to_owned(),
            clone_url: "https://gitea.example/team/assets.git".to_owned(),
            read_subjects: vec!["user-1".to_owned()],
            write_subjects: vec!["user-1".to_owned()],
        }],
    };
    let result = BuiltInProvider::from_config(config, None);
    assert!(result.is_ok());
    let provider = result.unwrap();
    assert_eq!(provider.kind(), ProviderKind::Gitea);
}

#[test]
fn built_in_provider_from_config_gitlab() {
    let config = ProviderConfig {
        kind: "gitlab".to_owned(),
        integration_subject: "gitlab-app".to_owned(),
        webhook_secret: Some(SecretString::from_secret("gitlab-secret")),
        repositories: vec![RepositoryPolicyConfig {
            owner: "team".to_owned(),
            name: "assets".to_owned(),
            visibility: "private".to_owned(),
            default_revision: "main".to_owned(),
            clone_url: "https://gitlab.example/team/assets.git".to_owned(),
            read_subjects: vec!["user-1".to_owned()],
            write_subjects: vec!["user-1".to_owned()],
        }],
    };
    let result = BuiltInProvider::from_config(config, None);
    assert!(result.is_ok());
    let provider = result.unwrap();
    assert_eq!(provider.kind(), ProviderKind::GitLab);
}

#[test]
fn built_in_provider_from_config_codeberg() {
    let config = ProviderConfig {
        kind: "codeberg".to_owned(),
        integration_subject: "codeberg-app".to_owned(),
        webhook_secret: Some(SecretString::from_secret("codeberg-secret")),
        repositories: vec![RepositoryPolicyConfig {
            owner: "team".to_owned(),
            name: "assets".to_owned(),
            visibility: "internal".to_owned(),
            default_revision: "main".to_owned(),
            clone_url: "https://codeberg.example/team/assets.git".to_owned(),
            read_subjects: vec!["user-1".to_owned()],
            write_subjects: vec!["user-1".to_owned()],
        }],
    };
    let result = BuiltInProvider::from_config(config, None);
    assert!(result.is_ok());
    let provider = result.unwrap();
    assert_eq!(provider.kind(), ProviderKind::Codeberg);
}

#[test]
fn built_in_provider_from_config_generic() {
    let config = ProviderConfig {
        kind: "generic".to_owned(),
        integration_subject: "generic-app".to_owned(),
        webhook_secret: Some(SecretString::from_secret("generic-secret")),
        repositories: vec![RepositoryPolicyConfig {
            owner: "team".to_owned(),
            name: "assets".to_owned(),
            visibility: "public".to_owned(),
            default_revision: "main".to_owned(),
            clone_url: "https://generic.example/team/assets.git".to_owned(),
            read_subjects: vec!["user-1".to_owned()],
            write_subjects: vec!["user-1".to_owned()],
        }],
    };
    let result = BuiltInProvider::from_config(config, None);
    assert!(result.is_ok());
    let provider = result.unwrap();
    assert_eq!(provider.kind(), ProviderKind::Generic);
}

// ── webhook_request — non-GitHub provider kinds ─────────────────────────

#[test]
fn webhook_request_gitea_provider() {
    let mut headers = HeaderMap::new();
    headers.insert("x-gitea-event", HeaderValue::from_static("push"));
    headers.insert("x-gitea-delivery", HeaderValue::from_static("delivery-1"));
    headers.insert("x-gitea-signature", HeaderValue::from_static("sha256=abc"));
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::Gitea, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.event_name(), "push");
    assert_eq!(request.delivery_id(), "delivery-1");
    assert!(request.signature().is_some());
}

#[test]
fn webhook_request_gitlab_provider() {
    let mut headers = HeaderMap::new();
    headers.insert("x-gitlab-event", HeaderValue::from_static("push"));
    headers.insert(
        "x-gitlab-webhook-uuid",
        HeaderValue::from_static("delivery-1"),
    );
    headers.insert("x-gitlab-token", HeaderValue::from_static("token-abc"));
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::GitLab, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.event_name(), "push");
    assert_eq!(request.delivery_id(), "delivery-1");
    assert!(request.signature().is_some());
}

#[test]
fn webhook_request_codeberg_provider() {
    let mut headers = HeaderMap::new();
    headers.insert("x-codeberg-event", HeaderValue::from_static("push"));
    headers.insert(
        "x-codeberg-delivery",
        HeaderValue::from_static("delivery-1"),
    );
    headers.insert(
        "x-codeberg-signature",
        HeaderValue::from_static("sha256=abc"),
    );
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::Codeberg, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.event_name(), "push");
    assert_eq!(request.delivery_id(), "delivery-1");
    assert!(request.signature().is_some());
}

#[test]
fn webhook_request_generic_provider() {
    let mut headers = HeaderMap::new();
    headers.insert("x-shardline-event", HeaderValue::from_static("push"));
    headers.insert(
        "x-shardline-delivery",
        HeaderValue::from_static("delivery-1"),
    );
    headers.insert(
        "x-shardline-signature",
        HeaderValue::from_static("sha256=abc"),
    );
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::Generic, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert_eq!(request.event_name(), "push");
    assert_eq!(request.delivery_id(), "delivery-1");
    assert!(request.signature().is_some());
}

#[test]
fn webhook_request_generic_missing_signature() {
    let mut headers = HeaderMap::new();
    headers.insert("x-shardline-event", HeaderValue::from_static("push"));
    headers.insert(
        "x-shardline-delivery",
        HeaderValue::from_static("delivery-1"),
    );
    // No signature header — optional, should be None
    let body = b"{}";
    let result = super::webhook_request(ProviderKind::Generic, &headers, body);
    assert!(result.is_ok());
    let request = result.unwrap();
    assert!(request.signature().is_none());
}

// ── repository_access ───────────────────────────────────────────────────

#[test]
fn repository_access_read_scope() {
    assert_eq!(
        super::repository_access(TokenScope::Read),
        RepositoryAccess::Read
    );
}

#[test]
fn repository_access_write_scope() {
    assert_eq!(
        super::repository_access(TokenScope::Write),
        RepositoryAccess::Write
    );
}

// ── authorize_bootstrap_key ────────────────────────────────────────────

#[test]
fn provider_authorize_bootstrap_key_validates_api_key() {
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    )
    .unwrap();
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shardline-provider-key",
        HeaderValue::from_static("bootstrap"),
    );
    let result = service.authorize_bootstrap_key(&headers);
    assert!(result.is_ok());
}

#[test]
fn provider_authorize_bootstrap_key_rejects_missing_key() {
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    )
    .unwrap();
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let result = service.authorize_bootstrap_key(&HeaderMap::new());
    assert!(matches!(result, Err(ProviderServiceError::MissingApiKey)));
}

// ── ProviderServiceError Debug ─────────────────────────────────────────

#[test]
fn provider_service_error_debug_all_variants() {
    let variants: Vec<ProviderServiceError> = vec![
        ProviderServiceError::EmptyApiKey,
        ProviderServiceError::ApiKeyTooLarge,
        ProviderServiceError::MissingApiKey,
        ProviderServiceError::InvalidApiKey,
        ProviderServiceError::DuplicateProvider,
        ProviderServiceError::MissingWebhookSecret,
        ProviderServiceError::EmptyWebhookSecret,
        ProviderServiceError::UnknownProvider,
        ProviderServiceError::Denied,
    ];
    for variant in variants {
        let debug = format!("{variant:?}");
        assert!(!debug.is_empty());
    }
}

// ── BuiltInProvider kind() for all variants ────────────────────────────

#[test]
fn built_in_provider_kind_returns_correct_kind() {
    let catalog = BuiltInProviderCatalog::new("test-app").unwrap();
    let generic = super::BuiltInProvider::Generic(shardline_vcs::GenericAdapter::new(
        catalog,
        Some(SecretString::from_secret("secret")),
    ));
    assert_eq!(generic.kind(), ProviderKind::Generic);
}

// ── ProviderTokenService parse_webhook with unknown provider ───────────

#[test]
fn provider_parse_webhook_unknown_provider_returns_error() {
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    )
    .unwrap();
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let result = service.parse_webhook(&HeaderMap::new(), "unknown-provider", b"{}");
    assert!(matches!(result, Err(ProviderServiceError::UnknownProvider)));
}

// ── ProviderServiceError Debug for config and mismatch variants ────────

#[test]
fn provider_service_error_config_variants_debug() {
    let too_large = ProviderServiceError::ConfigTooLarge {
        observed_bytes: 100,
        maximum_bytes: 50,
    };
    let debug = format!("{too_large:?}");
    assert!(debug.contains("observed_bytes") || debug.contains("ConfigTooLarge"));

    let mismatch = ProviderServiceError::ConfigLengthMismatch;
    let debug = format!("{mismatch:?}");
    assert!(!debug.is_empty());
}

// ── authorize_bootstrap_key — invalid key ─────────────────────────────

#[test]
fn provider_authorize_bootstrap_key_rejects_invalid_key() {
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    )
    .unwrap();
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer,
        registry: ProviderRegistry {
            providers: HashMap::new(),
        },
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shardline-provider-key",
        HeaderValue::from_static("wrong-key"),
    );
    let result = service.authorize_bootstrap_key(&headers);
    assert!(matches!(result, Err(ProviderServiceError::InvalidApiKey)));
}

// ── issue_token — subject validation ──────────────────────────────────

#[test]
fn provider_issue_token_rejects_empty_subject() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shardline-provider-key",
        HeaderValue::from_static("bootstrap"),
    );
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    )
    .unwrap();
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

    let result = service.issue_token(
        &headers,
        "github",
        &ProviderTokenIssueRequest {
            subject: String::new(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: Some("refs/heads/main".to_owned()),
            scope: TokenScope::Read,
        },
    );

    assert!(matches!(
        result,
        Err(ProviderServiceError::Subject(ProviderBoundaryError::Empty))
    ));
}

#[test]
fn provider_issue_token_denies_unauthorized_subject() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-shardline-provider-key",
        HeaderValue::from_static("bootstrap"),
    );
    let issuer = ProviderTokenIssuer::new(
        "issuer",
        b"a]32-byte-signing-key-for-testing!",
        NonZeroU64::MIN,
    )
    .unwrap();
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

    let result = service.issue_token(
        &headers,
        "github",
        &ProviderTokenIssueRequest {
            subject: "unauthorized-user".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            scope: TokenScope::Read,
        },
    );

    assert!(matches!(result, Err(ProviderServiceError::Denied)));
}

// ── parse_webhook — payload validation ────────────────────────────────

#[test]
fn provider_parse_webhook_malformed_json_body() {
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer: {
            let issuer = ProviderTokenIssuer::new(
                "issuer",
                b"a]32-byte-signing-key-for-testing!",
                NonZeroU64::MIN,
            );
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
    let body = b"not valid json";
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

    let result = service.parse_webhook(&headers, "github", body);

    assert!(matches!(
        result,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidWebhookPayload
        ))
    ));
}

#[test]
fn provider_parse_webhook_missing_required_fields() {
    let service = ProviderTokenService {
        api_key: SecretBytes::from_slice(b"bootstrap"),
        issuer: {
            let issuer = ProviderTokenIssuer::new(
                "issuer",
                b"a]32-byte-signing-key-for-testing!",
                NonZeroU64::MIN,
            );
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
    let body = br#"{"action":"deleted"}"#;
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

    let result = service.parse_webhook(&headers, "github", body);

    assert!(matches!(
        result,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidRepositoryPayload
        ))
    ));
}

// ── ProviderTokenService::from_file — config path and content errors ──

#[test]
fn provider_service_from_file_rejects_missing_config_path() {
    let result = ProviderTokenService::from_file(
        std::path::Path::new("/nonexistent/path/providers.json"),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        None,
    );

    assert!(matches!(result, Err(ProviderServiceError::Io(_))));
}

#[test]
fn provider_service_from_file_rejects_malformed_json() {
    let mut config = tempfile::NamedTempFile::new().unwrap();
    config.write_all(b"this is not valid json").unwrap();
    config.as_file().sync_all().unwrap();

    let result = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        None,
    );

    assert!(matches!(result, Err(ProviderServiceError::Json(_))));
}

// ── provider-config at-rest encryption (SHARDLINE_CONFIG_SECRET_KEY) ─────

fn at_rest_test_key() -> SecretBytes {
    SecretBytes::new(b"0123456789abcdef0123456789abcdef".to_vec())
}

fn at_rest_wrong_key() -> SecretBytes {
    SecretBytes::new(b"abcdef0123456789abcdef0123456789".to_vec())
}

fn provider_config_with_webhook_secret(webhook_secret: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "providers": [{
            "kind": "github",
            "integration_subject": "github-app",
            "webhook_secret": webhook_secret,
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
    }))
    .expect("serialize provider config")
}

fn provider_config_secret_identity(kind: &str, field: &str) -> String {
    super::provider_config_secret_identity(kind, field)
}

fn write_provider_config_bytes(contents: &[u8]) -> tempfile::NamedTempFile {
    let mut config = tempfile::NamedTempFile::new().unwrap();
    config.write_all(contents).unwrap();
    config.as_file().sync_all().unwrap();
    config
}

#[test]
fn provider_config_encrypted_write_read_round_trips_in_memory() {
    let cipher = AtRestCipher::new(at_rest_test_key()).unwrap();
    let identity = provider_config_secret_identity("github", "webhook_secret");
    let encrypted = cipher.encrypt(&identity, "s3cr3t-value").unwrap();

    let config = write_provider_config_bytes(&provider_config_with_webhook_secret(&encrypted));

    // At rest the file must carry ciphertext, never the plaintext secret.
    let on_disk = fs::read(config.path()).unwrap();
    let on_disk = String::from_utf8(on_disk).unwrap();
    assert!(
        on_disk.contains("sse1:"),
        "ciphertext must be `sse1:`-prefixed"
    );
    assert!(
        !on_disk.contains("s3cr3t-value"),
        "plaintext secret must not appear at rest"
    );

    // Reading with the key decrypts the secret in memory and builds the registry.
    let service = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        Some(&cipher),
    );
    let service = service.expect("provider token service with decrypted secret");
    assert!(service.registry.provider("github").is_ok());
}

#[test]
fn provider_config_wrong_key_fails_loud() {
    let cipher = AtRestCipher::new(at_rest_test_key()).unwrap();
    let identity = provider_config_secret_identity("github", "webhook_secret");
    let encrypted = cipher.encrypt(&identity, "s3cr3t-value").unwrap();
    let config = write_provider_config_bytes(&provider_config_with_webhook_secret(&encrypted));

    let wrong = AtRestCipher::new(at_rest_wrong_key()).unwrap();
    let result = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        Some(&wrong),
    );
    assert!(matches!(
        result,
        Err(ProviderServiceError::SecretDecrypt(_))
    ));
}

#[test]
fn provider_config_encrypted_without_key_fails_loud() {
    let cipher = AtRestCipher::new(at_rest_test_key()).unwrap();
    let identity = provider_config_secret_identity("github", "webhook_secret");
    let encrypted = cipher.encrypt(&identity, "s3cr3t-value").unwrap();
    let config = write_provider_config_bytes(&provider_config_with_webhook_secret(&encrypted));

    let result = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        None,
    );
    assert!(matches!(
        result,
        Err(ProviderServiceError::EncryptedSecretWithoutKey)
    ));
}

#[test]
fn provider_config_aad_binds_secret_to_provider_identity() {
    let cipher = AtRestCipher::new(at_rest_test_key()).unwrap();
    // Encrypt bound to a different provider than the config declares.
    let identity = provider_config_secret_identity("gitea", "webhook_secret");
    let encrypted = cipher.encrypt(&identity, "s3cr3t-value").unwrap();
    let config = write_provider_config_bytes(&provider_config_with_webhook_secret(&encrypted));

    let result = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        Some(&cipher),
    );
    assert!(matches!(
        result,
        Err(ProviderServiceError::SecretDecrypt(_))
    ));
}

#[test]
fn provider_config_legacy_plaintext_parses_with_and_without_key() {
    let config = write_provider_config_bytes(&provider_config_with_webhook_secret("s3cr3t-value"));

    // Without a key: legacy plaintext is used as-is.
    let service = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        None,
    );
    let service = service.expect("legacy config without key");
    assert!(service.registry.provider("github").is_ok());

    // With a key: legacy plaintext passes through unchanged.
    let cipher = AtRestCipher::new(at_rest_test_key()).unwrap();
    let service = ProviderTokenService::from_file(
        config.path(),
        b"bootstrap".to_vec(),
        "issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
        Some(&cipher),
    );
    let service = service.expect("legacy config with key");
    assert!(service.registry.provider("github").is_ok());
}

// ── from_config — repository metadata error paths ───────────────────────

#[test]
fn provider_config_rejects_invalid_clone_url() {
    // `configured_metadata` fails on an empty clone URL, surfacing the error
    // from `BuiltInProvider::from_config`.
    let config = ProviderConfig {
        kind: "github".to_owned(),
        integration_subject: "github-app".to_owned(),
        webhook_secret: Some(SecretString::from_secret("secret")),
        repositories: vec![RepositoryPolicyConfig {
            owner: "team".to_owned(),
            name: "assets".to_owned(),
            visibility: "private".to_owned(),
            default_revision: "main".to_owned(),
            clone_url: String::new(),
            read_subjects: vec!["github-user-1".to_owned()],
            write_subjects: vec!["github-user-1".to_owned()],
        }],
    };

    let result = BuiltInProvider::from_config(config, None);
    assert!(matches!(
        result,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::InvalidCloneUrl
        ))
    ));
}

#[test]
fn provider_config_rejects_duplicate_repository() {
    // Registering the same repository twice fails inside the catalog, surfacing
    // the error from `BuiltInProvider::from_config`.
    let repository = || RepositoryPolicyConfig {
        owner: "team".to_owned(),
        name: "assets".to_owned(),
        visibility: "private".to_owned(),
        default_revision: "main".to_owned(),
        clone_url: "https://github.example/team/assets.git".to_owned(),
        read_subjects: vec!["github-user-1".to_owned()],
        write_subjects: vec!["github-user-1".to_owned()],
    };
    let config = ProviderConfig {
        kind: "github".to_owned(),
        integration_subject: "github-app".to_owned(),
        webhook_secret: Some(SecretString::from_secret("secret")),
        repositories: vec![repository(), repository()],
    };

    let result = BuiltInProvider::from_config(config, None);
    assert!(matches!(
        result,
        Err(ProviderServiceError::BuiltIn(
            BuiltInProviderError::DuplicateRepository
        ))
    ));
}

// ── BuiltInProvider adapter dispatch — non-GitHub provider kinds ────────

fn provider_of_kind(kind: &str) -> BuiltInProvider {
    let config = ProviderConfig {
        kind: kind.to_owned(),
        integration_subject: format!("{kind}-app"),
        webhook_secret: Some(SecretString::from_secret(format!("{kind}-secret").as_str())),
        repositories: vec![RepositoryPolicyConfig {
            owner: "team".to_owned(),
            name: "assets".to_owned(),
            visibility: "private".to_owned(),
            default_revision: "main".to_owned(),
            clone_url: format!("https://{kind}.example/team/assets.git"),
            read_subjects: vec!["github-user-1".to_owned()],
            write_subjects: vec!["github-user-1".to_owned()],
        }],
    };
    BuiltInProvider::from_config(config, None).expect("provider config builds")
}

fn provider_kind_from_str(kind: &str) -> ProviderKind {
    match kind {
        "gitea" => ProviderKind::Gitea,
        "gitlab" => ProviderKind::GitLab,
        "codeberg" => ProviderKind::Codeberg,
        _ => ProviderKind::Generic,
    }
}

#[test]
fn provider_default_revision_resolves_for_each_kind() {
    for kind in ["gitea", "gitlab", "codeberg", "generic"] {
        let provider = provider_of_kind(kind);
        let repository =
            RepositoryRef::new(provider_kind_from_str(kind), "team", "assets").unwrap();
        let revision = provider
            .default_revision(&repository)
            .expect("default revision resolves");
        assert_eq!(revision.as_str(), "refs/heads/main");
    }
}

#[test]
fn provider_authorize_grants_allowed_subject_for_each_kind() {
    for kind in ["gitea", "gitlab", "codeberg", "generic"] {
        let provider = provider_of_kind(kind);
        let repository =
            RepositoryRef::new(provider_kind_from_str(kind), "team", "assets").unwrap();
        let subject = ProviderSubject::new("github-user-1").unwrap();
        let request = AuthorizationRequest::new(
            subject,
            repository,
            RevisionRef::new("refs/heads/main").unwrap(),
            RepositoryAccess::Read,
        );
        let grant = provider
            .authorize(&request)
            .expect("authorization evaluates");
        assert!(grant.is_some(), "{kind} should grant the allowed subject");
    }
}

fn hex_hmac_sha256(secret: &[u8], body: &[u8]) -> String {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).unwrap();
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[test]
fn provider_parse_webhook_handles_each_kind() {
    // Gitea and Codeberg: hex HMAC-SHA256 signature, `ping` event.
    for kind in ["gitea", "codeberg"] {
        let provider = provider_of_kind(kind);
        let body = br#"{"action":"ping"}"#;
        let signature = hex_hmac_sha256(format!("{kind}-secret").as_bytes(), body);
        let (event_header, delivery_header, signature_header) = match kind {
            "gitea" => (
                super::GITEA_EVENT_HEADER,
                super::GITEA_DELIVERY_HEADER,
                super::GITEA_SIGNATURE_HEADER,
            ),
            _ => (
                super::CODEBERG_EVENT_HEADER,
                super::CODEBERG_DELIVERY_HEADER,
                super::CODEBERG_SIGNATURE_HEADER,
            ),
        };
        let mut headers = HeaderMap::new();
        headers.insert(event_header, HeaderValue::from_static("ping"));
        headers.insert(delivery_header, HeaderValue::from_static("delivery-1"));
        headers.insert(signature_header, HeaderValue::from_str(&signature).unwrap());
        let request = super::webhook_request(provider_kind_from_str(kind), &headers, body).unwrap();
        let event = provider.parse_webhook(request).expect("webhook parses");
        assert!(
            event.is_none(),
            "{kind} ping events yield no repository event"
        );
    }

    // Generic: `sha256=`-prefixed HMAC-SHA256 signature, `ping` event.
    let generic = provider_of_kind("generic");
    let body = br#"{"action":"ping"}"#;
    let signature = format!("sha256={}", hex_hmac_sha256(b"generic-secret", body));
    let mut headers = HeaderMap::new();
    headers.insert(
        super::GENERIC_EVENT_HEADER,
        HeaderValue::from_static("ping"),
    );
    headers.insert(
        super::GENERIC_DELIVERY_HEADER,
        HeaderValue::from_static("delivery-1"),
    );
    headers.insert(
        super::GENERIC_SIGNATURE_HEADER,
        HeaderValue::from_str(&signature).unwrap(),
    );
    let request = super::webhook_request(ProviderKind::Generic, &headers, body).unwrap();
    let event = generic.parse_webhook(request).expect("webhook parses");
    assert!(event.is_none());

    // GitLab: the configured secret IS the constant-time webhook token, and a
    // `Push Hook` payload yields a revision-pushed event.
    let gitlab = provider_of_kind("gitlab");
    let body = br#"{"project":{"path_with_namespace":"team/assets"},"ref":"refs/heads/main"}"#;
    let mut headers = HeaderMap::new();
    headers.insert(
        super::GITLAB_EVENT_HEADER,
        HeaderValue::from_static("Push Hook"),
    );
    headers.insert(
        super::GITLAB_DELIVERY_HEADER,
        HeaderValue::from_static("delivery-1"),
    );
    headers.insert(
        super::GITLAB_SIGNATURE_HEADER,
        HeaderValue::from_static("gitlab-secret"),
    );
    let request = super::webhook_request(ProviderKind::GitLab, &headers, body).unwrap();
    let event = gitlab
        .parse_webhook(request)
        .expect("gitlab webhook parses")
        .expect("push hook yields an event");
    assert_eq!(event.repository().owner(), "team");
    assert_eq!(event.repository().name(), "assets");
}
