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
    ProviderRepositoryPolicy, ProviderSubject, ProviderTokenIssuer, RepositoryAccess,
    RepositoryRef, RepositoryVisibility, RepositoryWebhookEventKind, configured_metadata,
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
        b"a]32-byte-signing-key-for-testing!",
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
        b"a]32-byte-signing-key-for-testing!",
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

    let result = ProviderRegistry::from_document(document);
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

    let result = ProviderRegistry::from_document(document);
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
    let result = BuiltInProvider::from_config(config);
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
    let result = BuiltInProvider::from_config(config);
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
    let result = BuiltInProvider::from_config(config);
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
    let result = BuiltInProvider::from_config(config);
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
