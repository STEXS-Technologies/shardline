use shardline_protocol::SecretString;

use crate::{
    AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
    ProviderAdapter, ProviderKind, RepositoryMetadata, RepositoryRef, RepositoryWebhookEvent,
    WebhookRequest,
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
        crate::gitea::parse_gitea_webhook(self.kind(), self.webhook_secret.as_ref(), request)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::provider::test_support::signature;
    use shardline_protocol::SecretString;

    use super::CodebergAdapter;
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        ProviderAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RepositoryWebhookEventKind, RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<CodebergAdapter, BuiltInProviderError> {
        let catalog = crate::provider::test_support::catalog(
            ProviderKind::Codeberg,
            "codeberg-system",
            "https://codeberg.org/team/assets.git",
        )?;

        Ok(CodebergAdapter::new(
            catalog,
            Some(SecretString::from_secret("secret")),
        ))
    }

    #[test]
    fn codeberg_adapter_parses_push_webhook() {
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
        assert_eq!(event.repository().provider(), ProviderKind::Codeberg);
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn codeberg_adapter_parses_repository_deleted_webhook() {
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
    fn codeberg_adapter_parses_repository_renamed_webhook() {
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
        let new_repository =
            RepositoryRef::new(ProviderKind::Codeberg, "team", "new-assets").unwrap();
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
        );
    }

    #[test]
    fn codeberg_adapter_repository_access_changes() {
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
    fn codeberg_adapter_rejects_invalid_signature() {
        let adapter = adapter().unwrap();
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
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn codeberg_repository_invalid_payloads() {
        for (case, delivery, body) in [
            (
                "repository_name_only_missing_owner",
                "delivery-noowner",
                br#"{"ref":"refs/heads/main","repository":{"name":"assets"}}"#.as_slice(),
            ),
            (
                "repository_missing_payload",
                "delivery-empty",
                br#"{}"#.as_slice(),
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
                "adapter_repository_missing_owner_fields",
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
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().name(), "assets");
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
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().owner(), "team");
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
        let event = event.expect("expected webhook event");
        assert_eq!(event.repository().name(), "a.ssets");
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
