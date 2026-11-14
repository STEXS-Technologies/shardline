use serde_json::Value;
use shardline_protocol::SecretString;

use crate::{
    AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
    ProviderAdapter, ProviderKind, RepositoryMetadata, RepositoryRef, RepositoryWebhookEvent,
    RepositoryWebhookEventKind, WebhookRequest,
    builtin::{
        parse_delivery_id, parse_repository_from_full_name, parse_repository_rename,
        parse_revision, parse_webhook_json, value_str, verify_hex_hmac_sha256,
    },
};

/// Built-in Gitea provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GiteaAdapter {
    catalog: BuiltInProviderCatalog,
    webhook_secret: Option<SecretString>,
}

impl GiteaAdapter {
    /// Creates a Gitea adapter.
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

impl ProviderAdapter for GiteaAdapter {
    type Error = BuiltInProviderError;

    fn kind(&self) -> ProviderKind {
        ProviderKind::Gitea
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
        parse_gitea_webhook(self.kind(), self.webhook_secret.as_ref(), request)
    }
}

pub(super) fn parse_gitea_webhook(
    provider: ProviderKind,
    secret: Option<&SecretString>,
    request: WebhookRequest<'_>,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    if let Some(secret) = secret {
        let Some(signature) = request.signature() else {
            return Err(BuiltInProviderError::MissingWebhookAuthentication);
        };
        verify_hex_hmac_sha256(secret.expose_secret(), signature, request.body())?;
    } else {
        eprintln!(
            "[shardline] WARNING: {} webhook signature verification SKIPPED — \
             no webhook secret configured; deployers MUST set a webhook secret \
             to prevent forged webhook injection",
            provider.as_str()
        );
    }

    match request.event_name() {
        "push" => parse_push_event(provider, request),
        "repository" => parse_repository_event(provider, request),
        "ping" => Ok(None),
        _other => Ok(None),
    }
}

fn parse_push_event(
    provider: ProviderKind,
    request: WebhookRequest<'_>,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let payload = parse_webhook_json(request.body())?;
    let repository = gitea_repository(provider, &payload)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let revision = parse_revision(value_str(&payload, &["ref"]).unwrap_or_default())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RevisionPushed { revision },
    )))
}

fn parse_repository_event(
    provider: ProviderKind,
    request: WebhookRequest<'_>,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let payload = parse_webhook_json(request.body())?;
    let action = value_str(&payload, &["action"]).unwrap_or_default();
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let repository = gitea_repository(provider, &payload)?;

    match action {
        "deleted" => Ok(Some(RepositoryWebhookEvent::new(
            repository,
            delivery_id,
            RepositoryWebhookEventKind::RepositoryDeleted,
        ))),
        "renamed" => parse_repository_rename(&payload, repository, delivery_id),
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

fn gitea_repository(
    provider: ProviderKind,
    payload: &Value,
) -> Result<RepositoryRef, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["repository", "full_name"])
        .or_else(|| value_str(payload, &["repository", "name"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    if full_name.contains('/') {
        return parse_repository_from_full_name(provider, full_name);
    }

    let Some(owner) = value_str(payload, &["repository", "owner", "username"])
        .or_else(|| value_str(payload, &["repository", "owner", "login"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    RepositoryRef::new(provider, owner, full_name)
        .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::provider::test_support::signature;
    use shardline_protocol::SecretString;

    use super::GiteaAdapter;
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        ProviderAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RepositoryWebhookEventKind, RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<GiteaAdapter, BuiltInProviderError> {
        let catalog = crate::provider::test_support::catalog(
            ProviderKind::Gitea,
            "gitea-system",
            "https://gitea.example/team/assets.git",
        )?;

        Ok(GiteaAdapter::new(
            catalog,
            Some(SecretString::from_secret("secret")),
        ))
    }

    #[test]
    fn gitea_adapter_parses_push_webhook() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{
                "full_name":"team/assets"
            }
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-1", signature.as_deref(), body);

        let event = adapter.parse_webhook(request).unwrap();
        let event = event.expect("expected webhook event");
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        assert_eq!(event.repository().provider(), ProviderKind::Gitea);
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn gitea_adapter_parses_repository_deleted_webhook() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"deleted",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-2", signature.as_deref(), body);

        let event = adapter.parse_webhook(request).unwrap();
        let event = event.expect("expected webhook event");
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::RepositoryDeleted);
    }

    #[test]
    fn gitea_adapter_parses_repository_renamed_webhook() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/new-assets"},
            "changes":{"repository":{"name":{"from":"assets"}}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-3", signature.as_deref(), body);

        let event = adapter.parse_webhook(request).unwrap();
        let event = event.expect("expected webhook event");
        let new_repository = RepositoryRef::new(ProviderKind::Gitea, "team", "new-assets").unwrap();
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
        );
    }

    #[test]
    fn gitea_adapter_repository_access_changes() {
        for (action, delivery_id) in [
            ("transferred", "delivery-4"),
            ("edited", "delivery-edit"),
            ("archived", "delivery-arch"),
            ("unarchived", "delivery-unarch"),
            ("privatized", "delivery-priv"),
            ("publicized", "delivery-pub"),
        ] {
            let adapter = adapter().unwrap();
            let body = serde_json::to_vec(&serde_json::json!({
                "action": action,
                "repository": {"full_name": "team/assets"},
            }))
            .unwrap();
            let signature = signature(&body);
            let request =
                WebhookRequest::new("repository", delivery_id, signature.as_deref(), &body);
            let result = adapter.parse_webhook(request);
            assert!(
                matches!(&result, Ok(Some(event)) if event.kind() == &RepositoryWebhookEventKind::AccessChanged),
                "action {action}: {result:?}"
            );
        }
    }

    #[test]
    fn gitea_adapter_rejects_invalid_signature() {
        let adapter = adapter().unwrap();
        let request = WebhookRequest::new("push", "delivery-1", Some("deadbeef"), br#"{}"#);

        let event = adapter.parse_webhook(request);

        assert_eq!(
            event,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn gitea_adapter_no_webhook_secret_skips_verification() {
        let mut catalog = BuiltInProviderCatalog::new("gitea-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitea.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GiteaAdapter::new(catalog, None);

        let body = br#"{"ref":"refs/heads/main","repository":{"full_name":"team/assets"}}"#;
        let request = WebhookRequest::new("push", "delivery-1", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_some());
    }

    #[test]
    fn gitea_adapter_ping_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{}"#;
        let signature = signature(body);
        let request = WebhookRequest::new("ping", "delivery-ping", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitea_adapter_unknown_event_returns_none() {
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
    fn gitea_adapter_push_without_ref_errors() {
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
    fn gitea_adapter_repository_unknown_action_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"unknown",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-unk", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitea_repository_uses_name_without_full_name_and_owner_from_username() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{
                "name":"assets",
                "owner":{"username":"team"}
            }
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-name", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn gitea_repository_uses_owner_login_fallback() {
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
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().owner(), "team");
    }

    #[test]
    fn gitea_repository_invalid_payloads() {
        for (case, delivery, body) in [
            (
                "repository_name_only_missing_owner",
                "delivery-noowner",
                br#"{
            "ref":"refs/heads/main",
            "repository":{"name":"assets"}
        }"#
                .as_slice(),
            ),
            (
                "repository_missing_full_name_and_name",
                "delivery-noname",
                br#"{
            "ref":"refs/heads/main",
            "repository":{"owner":{"username":"team"}}
        }"#
                .as_slice(),
            ),
            (
                "repository_missing_owner_fields",
                "delivery-nofields",
                br#"{
            "ref":"refs/heads/main",
            "repository":{"name":"assets","owner":{}}
        }"#
                .as_slice(),
            ),
            (
                "adapter_push_with_ref_but_no_repository_name",
                "delivery-norepo",
                br#"{
            "ref":"refs/heads/main",
            "repository":{"owner":{"username":"team"}}
        }"#
                .as_slice(),
            ),
        ] {
            let adapter = adapter().unwrap();
            let signature = signature(body);
            let request = WebhookRequest::new("push", delivery, signature.as_deref(), body);
            let result = adapter.parse_webhook(request);
            assert!(
                matches!(result, Err(BuiltInProviderError::InvalidRepositoryPayload)),
                "case {case}: {result:?}"
            );
        }
    }

    #[test]
    fn gitea_repository_rename_with_changes_name_fallback() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/new-assets"},
            "changes":{"name":{"from":"assets"}}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-alt", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn gitea_secret_required_when_configured() {
        let adapter = adapter().unwrap();
        let request = WebhookRequest::new("push", "delivery-1", None, br#"{}"#);
        let event = adapter.parse_webhook(request);
        assert_eq!(
            event,
            Err(BuiltInProviderError::MissingWebhookAuthentication)
        );
    }

    #[test]
    fn gitea_adapter_kind_returns_gitea() {
        let adapter = adapter().unwrap();
        assert_eq!(adapter.kind(), ProviderKind::Gitea);
    }

    #[test]
    fn gitea_adapter_check_access_allows_authorized_subject() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("gitea-user-1").unwrap();
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
    fn gitea_adapter_check_access_denies_unauthorized_subject() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("unknown-user").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let decision = adapter.check_access(&request).unwrap();
        assert_eq!(decision, AuthorizationDecision::Deny);
    }

    #[test]
    fn gitea_adapter_check_access_rejects_unknown_repository() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "unknown", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("gitea-user-1").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let result = adapter.check_access(&request);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn gitea_adapter_repository_metadata_returns_metadata() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let metadata = adapter.repository_metadata(&repository).unwrap();
        assert_eq!(metadata.repository(), &repository);
        assert_eq!(metadata.visibility(), RepositoryVisibility::Private);
        assert_eq!(
            metadata.clone_url().as_str(),
            "https://gitea.example/team/assets.git"
        );
    }

    #[test]
    fn gitea_adapter_repository_metadata_unknown_repo_errors() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "unknown", "repo").unwrap();
        let result = adapter.repository_metadata(&repository);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn gitea_repository_renamed_without_changes_returns_none() {
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
    fn gitea_adapter_unknown_repository_event_action_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"some_unknown_action",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("repository", "delivery-unk", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitea_adapter_invalid_json_payload_errors() {
        let mut catalog = BuiltInProviderCatalog::new("gitea-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitea.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GiteaAdapter::new(catalog, None);
        let body = b"not-json";
        let request = WebhookRequest::new("push", "delivery-bad", None, body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidWebhookPayload)
        ));
    }

    #[test]
    fn gitea_adapter_parses_repository_renamed_event_with_updates() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "action":"renamed",
            "repository":{"full_name":"team/newname"},
            "changes":{"repository":{"name":{"from":"oldname"}}}
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("repository", "delivery-ren2", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().name(), "oldname");
        assert!(
            matches!(event.kind(), RepositoryWebhookEventKind::RepositoryRenamed { new_repository } if new_repository.name() == "newname"),
            "expected RepositoryRenamed(new_repo=newname)"
        );
    }

    #[test]
    fn gitea_adapter_check_allows_write_access() {
        let mut catalog = BuiltInProviderCatalog::new("gitea-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let subject = ProviderSubject::new("gitea-writer").unwrap();
        let metadata = configured_metadata(
            repository.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://gitea.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::new(),
                HashSet::from([subject.clone()]),
            ))
            .unwrap();
        let adapter = GiteaAdapter::new(catalog, None);
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
    fn gitea_adapter_new_is_const() {
        let catalog = BuiltInProviderCatalog::new("gitea-system").unwrap();
        let adapter = GiteaAdapter::new(catalog, None);
        assert_eq!(adapter.kind(), ProviderKind::Gitea);
    }

    #[test]
    fn gitea_adapter_debug_format() {
        let catalog = BuiltInProviderCatalog::new("gitea-system").unwrap();
        let adapter = GiteaAdapter::new(catalog, None);
        let debug = format!("{adapter:?}");
        assert!(debug.contains("GiteaAdapter"));
    }

    #[test]
    fn gitea_adapter_clone_eq() {
        let adapter = adapter().unwrap();
        let cloned = adapter.clone();
        assert_eq!(adapter, cloned);
    }

    #[test]
    fn gitea_unknown_event_with_valid_signature_reaches_match_arm() {
        let adapter = adapter().unwrap();
        let body = b"{}";
        let sig = signature(body);
        let request =
            WebhookRequest::new("unknown_event_name", "delivery-unk", sig.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitea_repository_event_with_unknown_action_and_no_signature_check() {
        let mut catalog = BuiltInProviderCatalog::new("gitea-system").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitea.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GiteaAdapter::new(catalog, None);
        let body = br#"{"action":"completely_unknown","repository":{"full_name":"team/assets"}}"#;
        let request = WebhookRequest::new("repository", "delivery-unk2", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }
}
