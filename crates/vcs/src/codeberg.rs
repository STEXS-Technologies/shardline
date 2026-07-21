use serde_json::Value;
use shardline_protocol::SecretString;

use crate::{
    AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
    ProviderAdapter, ProviderKind, RepositoryMetadata, RepositoryRef, RepositoryWebhookEvent,
    RepositoryWebhookEventKind, WebhookRequest,
    builtin::{
        parse_delivery_id, parse_repository_from_full_name, parse_revision, parse_webhook_json,
        value_str, verify_hex_hmac_sha256,
    },
};

/// Built-in Codeberg (Gitea-based) provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodebergAdapter {
    catalog: BuiltInProviderCatalog,
    webhook_secret: Option<SecretString>,
}

impl CodebergAdapter {
    /// Creates a Codeberg adapter.
    #[must_use]
    pub const fn new(
        catalog: BuiltInProviderCatalog,
        webhook_secret: Option<SecretString>,
    ) -> Self {
        Self {
            catalog,
            webhook_secret,
        }
    }
}

impl ProviderAdapter for CodebergAdapter {
    type Error = BuiltInProviderError;

    fn kind(&self) -> ProviderKind {
        ProviderKind::Codeberg
    }

    fn check_access(
        &self,
        request: &AuthorizationRequest,
    ) -> Result<AuthorizationDecision, Self::Error> {
        self.catalog
            .check_access(request.repository(), request.subject(), request.access())
    }

    fn repository_metadata(
        &self,
        repository: &RepositoryRef,
    ) -> Result<RepositoryMetadata, Self::Error> {
        Ok(self.catalog.repository(repository)?.metadata().clone())
    }

    fn parse_webhook(
        &self,
        request: WebhookRequest<'_>,
    ) -> Result<Option<RepositoryWebhookEvent>, Self::Error> {
        if let Some(secret) = &self.webhook_secret {
            let Some(signature) = request.signature() else {
                return Err(BuiltInProviderError::MissingWebhookAuthentication);
            };
            verify_hex_hmac_sha256(secret.expose_secret(), signature, request.body())?;
        }

        match request.event_name() {
            "push" => parse_push_event(request),
            "repository" => parse_repository_event(request),
            "ping" => Ok(None),
            _other => Ok(None),
        }
    }
}

fn parse_push_event(
    request: WebhookRequest<'_>,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let payload = parse_webhook_json(request.body())?;
    let repository = codeberg_repository(&payload)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let revision = parse_revision(value_str(&payload, &["ref"]).unwrap_or_default())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RevisionPushed { revision },
    )))
}

fn parse_repository_event(
    request: WebhookRequest<'_>,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let payload = parse_webhook_json(request.body())?;
    let action = value_str(&payload, &["action"]).unwrap_or_default();
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let repository = codeberg_repository(&payload)?;

    match action {
        "deleted" => Ok(Some(RepositoryWebhookEvent::new(
            repository,
            delivery_id,
            RepositoryWebhookEventKind::RepositoryDeleted,
        ))),
        "renamed" => {
            let Some(previous_name) = codeberg_previous_name(&payload) else {
                return Ok(None);
            };
            let previous =
                RepositoryRef::new(ProviderKind::Codeberg, repository.owner(), previous_name)
                    .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;

            Ok(Some(RepositoryWebhookEvent::new(
                previous,
                delivery_id,
                RepositoryWebhookEventKind::RepositoryRenamed {
                    new_repository: repository,
                },
            )))
        }
        "edited" | "privatized" | "publicized" | "transferred" | "archived" | "unarchived" => {
            Ok(Some(RepositoryWebhookEvent::new(
                repository,
                delivery_id,
                RepositoryWebhookEventKind::AccessChanged,
            )))
        }
        _other => Ok(None),
    }
}

fn codeberg_repository(payload: &Value) -> Result<RepositoryRef, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["repository", "full_name"])
        .or_else(|| value_str(payload, &["repository", "name"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    if full_name.contains('/') {
        return parse_repository_from_full_name(ProviderKind::Codeberg, full_name);
    }

    let Some(owner) = value_str(payload, &["repository", "owner", "username"])
        .or_else(|| value_str(payload, &["repository", "owner", "login"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    RepositoryRef::new(ProviderKind::Codeberg, owner, full_name)
        .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)
}

fn codeberg_previous_name(payload: &Value) -> Option<&str> {
    value_str(payload, &["changes", "repository", "name", "from"])
        .or_else(|| value_str(payload, &["changes", "name", "from"]))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hmac::Mac;
    use shardline_protocol::SecretString;

    use super::CodebergAdapter;
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        ProviderAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RepositoryWebhookEventKind, RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<CodebergAdapter, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new("codeberg-system")?;
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let subject = ProviderSubject::new("codeberg-user-1")
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://codeberg.org/team/assets.git",
        )?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::from([subject]),
        ))?;

        Ok(CodebergAdapter::new(
            catalog,
            Some(SecretString::from_secret("secret")),
        ))
    }

    fn signature(body: &[u8]) -> Option<String> {
        let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
        assert!(mac.is_ok());
        let Ok(mut mac) = mac else {
            return None;
        };
        mac.update(body);
        Some(hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn codeberg_adapter_parses_push_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{
                "full_name":"team/assets"
            }
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-1", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let revision = RevisionRef::new("refs/heads/main");
        assert!(revision.is_ok());
        let Ok(revision) = revision else {
            return;
        };
        assert_eq!(event.repository().provider(), ProviderKind::Codeberg);
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn codeberg_adapter_parses_repository_deleted_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "action":"deleted",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-2", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::RepositoryDeleted);
    }

    #[test]
    fn codeberg_adapter_parses_repository_renamed_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/new-assets"},
            "changes":{"repository":{"name":{"from":"assets"}}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-3", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let new_repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "new-assets");
        assert!(new_repository.is_ok());
        let Ok(new_repository) = new_repository else {
            return;
        };
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
        );
    }

    #[test]
    fn codeberg_adapter_parses_repository_access_change_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "action":"transferred",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-4", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn codeberg_adapter_rejects_invalid_signature() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let request = WebhookRequest::new("push", "delivery-1", Some("deadbeef"), br#"{}"#);

        let event = adapter.parse_webhook(request);

        assert_eq!(
            event,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn codeberg_adapter_no_webhook_secret_skips_verification() {
        let mut catalog = BuiltInProviderCatalog::new("codeberg-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://codeberg.org/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = CodebergAdapter::new(catalog, None);

        let body = br#"{"ref":"refs/heads/main","repository":{"full_name":"team/assets"}}"#;
        let request = WebhookRequest::new("push", "delivery-1", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_some());
    }

    #[test]
    fn codeberg_adapter_ping_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{}"#;
        let signature = signature(body);
        let request = WebhookRequest::new("ping", "delivery-ping", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn codeberg_adapter_unknown_event_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{}"#;
        let request = WebhookRequest::new("unknown", "delivery-unk", None, body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::MissingWebhookAuthentication)
        ));
    }

    #[test]
    fn codeberg_adapter_push_without_ref_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{"repository":{"full_name":"team/assets"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-noref", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRevisionPayload)
        ));
    }

    #[test]
    fn codeberg_adapter_repository_edited_as_access_change() {
        let adapter = adapter().unwrap();
        let body = br#"{"action":"edited","repository":{"full_name":"team/assets"}}"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("repository", "delivery-edit", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn codeberg_adapter_repository_archived_as_access_change() {
        let adapter = adapter().unwrap();
        let body = br#"{"action":"archived","repository":{"full_name":"team/assets"}}"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("repository", "delivery-arch", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn codeberg_adapter_repository_unknown_action_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{"action":"unknown","repository":{"full_name":"team/assets"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-unk", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn codeberg_repository_uses_name_without_full_name_and_owner_from_username() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{"name":"assets","owner":{"username":"team"}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-name", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn codeberg_repository_name_only_missing_owner_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{"ref":"refs/heads/main","repository":{"name":"assets"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-noowner", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn codeberg_secret_required_when_configured() {
        let adapter = adapter().unwrap();
        let request = WebhookRequest::new("push", "delivery-1", None, br#"{}"#);
        let event = adapter.parse_webhook(request);
        assert_eq!(
            event,
            Err(BuiltInProviderError::MissingWebhookAuthentication)
        );
    }

    #[test]
    fn codeberg_repository_rename_with_changes_name_fallback() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/new-assets"},
            "changes":{"name":{"from":"assets"}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-alt", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn codeberg_repository_missing_payload_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{}"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-empty", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn codeberg_adapter_kind_returns_codeberg() {
        let adapter = adapter().unwrap();
        assert_eq!(adapter.kind(), ProviderKind::Codeberg);
    }

    #[test]
    fn codeberg_adapter_check_access_allows_authorized_subject() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("codeberg-user-1").unwrap();
        let request = AuthorizationRequest::new(
            subject.clone(),
            repository,
            revision,
            RepositoryAccess::Read,
        );
        let decision = adapter.check_access(&request).unwrap();
        assert_eq!(decision, AuthorizationDecision::Allow(subject));
    }

    #[test]
    fn codeberg_adapter_check_access_denies_unauthorized_subject() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("unknown-user").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let decision = adapter.check_access(&request).unwrap();
        assert_eq!(decision, AuthorizationDecision::Deny);
    }

    #[test]
    fn codeberg_adapter_check_access_rejects_unknown_repository() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "unknown", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("codeberg-user-1").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let result = adapter.check_access(&request);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn codeberg_adapter_repository_metadata_returns_metadata() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let metadata = adapter.repository_metadata(&repository).unwrap();
        assert_eq!(metadata.repository(), &repository);
        assert_eq!(metadata.visibility(), RepositoryVisibility::Private);
    }

    #[test]
    fn codeberg_adapter_repository_metadata_unknown_repo_errors() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "unknown", "repo").unwrap();
        let result = adapter.repository_metadata(&repository);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn codeberg_adapter_repository_privatized_as_access_change() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"privatized",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("repository", "delivery-priv", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn codeberg_adapter_repository_publicized_as_access_change() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"publicized",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-pub", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn codeberg_adapter_repository_unarchived_as_access_change() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"unarchived",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("repository", "delivery-unarch", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn codeberg_repository_uses_owner_login_fallback() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{
                "name":"assets",
                "owner":{"login":"team"}
            }
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-login", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.repository().owner(), "team");
    }

    #[test]
    fn codeberg_repository_missing_full_name_and_name_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{"owner":{"username":"team"}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-noname", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn codeberg_push_event_no_ref_falls_back_to_empty_and_fails_validation() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-noref2", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRevisionPayload)
        ));
    }

    #[test]
    fn codeberg_repository_renamed_without_changes_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/new-assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-ren", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn codeberg_adapter_repository_missing_owner_fields_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{"name":"assets","owner":{}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-nofields", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn codeberg_adapter_check_allows_write_access() {
        let mut catalog = BuiltInProviderCatalog::new("codeberg-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let subject = ProviderSubject::new("codeberg-writer").unwrap();
        let metadata = configured_metadata(
            repository.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://codeberg.org/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::new(),
                HashSet::from([subject.clone()]),
            ))
            .unwrap();
        let adapter = CodebergAdapter::new(catalog, None);
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let request = AuthorizationRequest::new(
            subject.clone(),
            repository,
            revision,
            RepositoryAccess::Write,
        );
        let decision = adapter.check_access(&request).unwrap();
        assert_eq!(decision, AuthorizationDecision::Allow(subject));
    }

    #[test]
    fn codeberg_adapter_new_is_const() {
        let catalog = BuiltInProviderCatalog::new("codeberg-system").unwrap();
        let adapter = CodebergAdapter::new(catalog, None);
        assert_eq!(adapter.kind(), ProviderKind::Codeberg);
    }

    #[test]
    fn codeberg_repository_full_name_with_extra_dot() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{"full_name":"team/a.ssets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-dot", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.repository().name(), "a.ssets");
    }

    #[test]
    fn codeberg_adapter_push_with_ref_but_no_repository_name_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{"owner":{"username":"team"}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-norepo", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn codeberg_adapter_invalid_json_body_errors() {
        let mut catalog = BuiltInProviderCatalog::new("codeberg-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://codeberg.org/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        // No webhook secret => no HMAC verification
        let adapter = CodebergAdapter::new(catalog, None);
        let body = b"not valid json";
        let request = WebhookRequest::new("push", "delivery-badjson", None, body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidWebhookPayload)
        ));
    }

    #[test]
    fn codeberg_adapter_debug_format() {
        let catalog = BuiltInProviderCatalog::new("codeberg-system").unwrap();
        let adapter = CodebergAdapter::new(catalog, None);
        let debug = format!("{adapter:?}");
        assert!(debug.contains("CodebergAdapter"));
    }

    #[test]
    fn codeberg_adapter_clone_eq() {
        let adapter = adapter().unwrap();
        let cloned = adapter.clone();
        assert_eq!(adapter, cloned);
    }

    #[test]
    fn codeberg_unknown_event_with_valid_signature_reaches_match_arm() {
        let adapter = adapter().unwrap();
        let body = b"{}";
        let sig = signature(body);
        let request =
            WebhookRequest::new("unknown_event_name", "delivery-unk", sig.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn codeberg_repository_event_with_unknown_action_and_no_signature_check() {
        let mut catalog = BuiltInProviderCatalog::new("codeberg-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Codeberg, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://codeberg.org/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = CodebergAdapter::new(catalog, None);
        let body = br#"{"action":"completely_unknown","repository":{"full_name":"team/assets"}}"#;
        let request = WebhookRequest::new("repository", "delivery-unk2", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }
}
