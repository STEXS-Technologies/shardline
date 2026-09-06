use serde_json::Value;
use shardline_protocol::SecretString;

use crate::{
    AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
    ProviderAdapter, ProviderKind, RepositoryMetadata, RepositoryRef, RepositoryWebhookEvent,
    RepositoryWebhookEventKind, WebhookRequest,
    builtin::{
        parse_delivery_id, parse_repository_from_full_name, parse_revision, parse_webhook_json,
        value_str, verify_prefixed_hmac_sha256,
    },
};

/// Built-in generic provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericAdapter {
    catalog: BuiltInProviderCatalog,
    webhook_secret: Option<SecretString>,
}

impl GenericAdapter {
    /// Creates a generic provider adapter.
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

impl ProviderAdapter for GenericAdapter {
    type Error = BuiltInProviderError;

    fn kind(&self) -> ProviderKind {
        ProviderKind::Generic
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
            verify_prefixed_hmac_sha256(
                secret.expose_secret(),
                request.signature(),
                "sha256=",
                request.body(),
            )?;
        } else {
            eprintln!(
                "[shardline] WARNING: generic webhook signature verification SKIPPED — \
                 no webhook secret configured; deployers MUST set a webhook secret \
                 to prevent forged webhook injection"
            );
        }

        if request.event_name() == "ping" {
            return Ok(None);
        }

        let payload = parse_webhook_json(request.body())?;
        match normalized_kind(request.event_name(), &payload) {
            Some("revision_pushed") => parse_revision_pushed(request, &payload),
            Some("repository_deleted") => parse_repository_deleted(request, &payload),
            Some("repository_renamed") => parse_repository_renamed(request, &payload),
            Some("access_changed") => parse_access_changed(request, &payload),
            Some(_) | None => Ok(None),
        }
    }
}

fn normalized_kind<'payload>(
    event_name: &'payload str,
    payload: &'payload Value,
) -> Option<&'payload str> {
    if !event_name.is_empty() {
        return match event_name {
            "push" => Some("revision_pushed"),
            "repository_deleted" | "repository_renamed" | "access_changed" => Some(event_name),
            _other => {
                value_str(payload, &["kind"]).or_else(|| value_str(payload, &["event", "kind"]))
            }
        };
    }

    value_str(payload, &["kind"]).or_else(|| value_str(payload, &["event", "kind"]))
}

fn parse_revision_pushed(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let repository = generic_repository(payload, "repository")?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let revision = parse_revision(
        value_str(payload, &["revision"])
            .or_else(|| value_str(payload, &["ref"]))
            .ok_or(BuiltInProviderError::InvalidRevisionPayload)?,
    )?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RevisionPushed { revision },
    )))
}

fn parse_repository_deleted(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let repository = generic_repository(payload, "repository")?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RepositoryDeleted,
    )))
}

fn parse_repository_renamed(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let repository = generic_repository(payload, "repository")?;
    let new_repository = generic_repository(payload, "new_repository")?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RepositoryRenamed { new_repository },
    )))
}

fn parse_access_changed(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let repository = generic_repository(payload, "repository")?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::AccessChanged,
    )))
}

fn generic_repository(payload: &Value, path: &str) -> Result<RepositoryRef, BuiltInProviderError> {
    if let Some(full_name) = value_str(payload, &[path, "full_name"]) {
        return parse_repository_from_full_name(ProviderKind::Generic, full_name);
    }

    let owner = value_str(payload, &[path, "owner"])
        .or_else(|| value_str(payload, &[path, "namespace"]))
        .or_else(|| value_str(payload, &["owner"]));
    let name = value_str(payload, &[path, "name"]).or_else(|| value_str(payload, &["name"]));
    let Some(owner) = owner else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let Some(name) = name else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    RepositoryRef::new(ProviderKind::Generic, owner, name)
        .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hmac::Mac;
    use shardline_protocol::SecretString;

    use super::GenericAdapter;
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        ProviderAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RepositoryWebhookEventKind, RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<GenericAdapter, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new("generic-bridge")?;
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let subject = ProviderSubject::new("generic-user-1")
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://forge.example/team/assets.git",
        )?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::from([subject]),
        ))?;

        Ok(GenericAdapter::new(
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
        Some(format!(
            "sha256={}",
            hex::encode(mac.finalize().into_bytes())
        ))
    }

    #[test]
    fn generic_adapter_returns_registered_metadata() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets");
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        let metadata = adapter.repository_metadata(&repository);

        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };
        assert_eq!(
            metadata.clone_url().as_str(),
            "https://forge.example/team/assets.git"
        );
        assert_eq!(metadata.default_revision().as_str(), "refs/heads/main");
    }

    #[test]
    fn generic_adapter_allows_registered_write_access() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let subject = ProviderSubject::new("generic-user-1");
        let denied_subject = ProviderSubject::new("generic-user-2");
        assert!(repository.is_ok());
        assert!(revision.is_ok());
        assert!(subject.is_ok());
        assert!(denied_subject.is_ok());
        let (Ok(repository), Ok(revision), Ok(subject), Ok(denied_subject)) =
            (repository, revision, subject, denied_subject)
        else {
            return;
        };
        let request = AuthorizationRequest::new(
            subject.clone(),
            repository.clone(),
            revision.clone(),
            RepositoryAccess::Write,
        );
        let denied_request = AuthorizationRequest::new(
            denied_subject,
            repository,
            revision,
            RepositoryAccess::Write,
        );

        let decision = adapter.check_access(&request);
        let denied_decision = adapter.check_access(&denied_request);

        assert!(decision.is_ok());
        assert!(denied_decision.is_ok());
        let Ok(decision) = decision else {
            return;
        };
        let Ok(denied_decision) = denied_decision else {
            return;
        };
        assert_eq!(decision, AuthorizationDecision::Allow(subject));
        assert_eq!(denied_decision, AuthorizationDecision::Deny);
    }

    #[test]
    fn generic_adapter_parses_revision_pushed_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"revision_pushed",
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("revision_pushed", "delivery-1", signature.as_deref(), body);

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
        assert_eq!(event.delivery_id().as_str(), "delivery-1");
        assert_eq!(event.repository().provider(), ProviderKind::Generic);
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn generic_adapter_parses_repository_rename_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"repository_renamed",
            "repository":{"owner":"team","name":"assets"},
            "new_repository":{"owner":"team","name":"new-assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_renamed",
            "delivery-2",
            signature.as_deref(),
            body,
        );

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let new_repository = RepositoryRef::new(ProviderKind::Generic, "team", "new-assets");
        assert!(new_repository.is_ok());
        let Ok(new_repository) = new_repository else {
            return;
        };
        assert_eq!(event.delivery_id().as_str(), "delivery-2");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
        );
    }

    #[test]
    fn generic_adapter_rejects_invalid_signature() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let request = WebhookRequest::new(
            "repository_deleted",
            "delivery-1",
            Some("sha256=deadbeef"),
            br#"{}"#,
        );

        let event = adapter.parse_webhook(request);

        assert_eq!(
            event,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn generic_adapter_parses_push_event_as_revision_pushed() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "repository":{"owner":"team","name":"assets"},
            "ref":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-push-1", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.delivery_id().as_str(), "delivery-push-1");
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RevisionPushed { .. }
        ));
    }

    #[test]
    fn generic_adapter_parses_repository_deleted_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "repository":{"owner":"team","name":"assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_deleted",
            "delivery-del-1",
            signature.as_deref(),
            body,
        );

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
    fn generic_adapter_parses_access_changed_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "repository":{"owner":"team","name":"assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "access_changed",
            "delivery-ac-1",
            signature.as_deref(),
            body,
        );

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
    fn generic_adapter_ping_returns_none() {
        let mut catalog = BuiltInProviderCatalog::new("generic-bridge").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://forge.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GenericAdapter::new(catalog, None); // No secret
        let body = b"{}";
        let signature = signature(body);
        let request = WebhookRequest::new("ping", "delivery-ping", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn generic_adapter_unknown_event_returns_none() {
        let mut catalog = BuiltInProviderCatalog::new("generic-bridge").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://forge.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GenericAdapter::new(catalog, None); // No secret
        let body = b"{}";
        let request = WebhookRequest::new("unknown_event", "delivery-unk", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn generic_adapter_parses_event_kind_from_payload_when_event_name_empty() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"revision_pushed",
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("", "delivery-kind", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RevisionPushed { .. }
        ));
    }

    #[test]
    fn generic_adapter_parses_event_kind_from_nested_event_kind() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "event":{"kind":"revision_pushed"},
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("custom", "delivery-nested", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RevisionPushed { .. }
        ));
    }

    #[test]
    fn generic_adapter_revision_pushed_uses_ref_fallback() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"revision_pushed",
            "repository":{"full_name":"team/assets"},
            "ref":"refs/heads/fallback"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-ref", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RevisionPushed { .. }
        ));
    }

    #[test]
    fn generic_repository_uses_root_owner_and_name() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"revision_pushed",
            "owner":"team",
            "name":"assets",
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "revision_pushed",
            "delivery-root",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn generic_repository_uses_namespace_fallback() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"revision_pushed",
            "repository":{"namespace":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("revision_pushed", "delivery-ns", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn generic_repository_missing_owner_errors() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{"kind":"access_changed","repository":{"name":"assets"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "access_changed",
            "delivery-no-owner",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_repository_missing_name_errors() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{"kind":"access_changed","repository":{"owner":"team"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "access_changed",
            "delivery-no-name",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_adapter_revision_pushed_missing_revision_errors() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{"kind":"revision_pushed","repository":{"owner":"team","name":"assets"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "revision_pushed",
            "delivery-no-rev",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRevisionPayload)
        ));
    }

    #[test]
    fn generic_repository_uses_full_name() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"repository_deleted",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_deleted",
            "delivery-fullname",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn generic_adapter_parse_repository_renamed_with_new_repository_full_name() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "kind":"repository_renamed",
            "repository":{"full_name":"team/assets"},
            "new_repository":{"full_name":"team/new-assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_renamed",
            "delivery-ren",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else {
            return;
        };
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RepositoryRenamed { .. }
        ));
    }

    #[test]
    fn generic_adapter_kind_returns_generic() {
        let adapter = adapter().unwrap();
        assert_eq!(adapter.kind(), ProviderKind::Generic);
    }

    #[test]
    fn generic_adapter_check_access_denies_unauthorized_subject() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("unknown-user").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let decision = adapter.check_access(&request).unwrap();
        assert_eq!(decision, AuthorizationDecision::Deny);
    }

    #[test]
    fn generic_adapter_check_access_rejects_unknown_repository() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "unknown", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("generic-user-1").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let result = adapter.check_access(&request);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn generic_adapter_repository_metadata_unknown_repo_errors() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "unknown", "repo").unwrap();
        let result = adapter.repository_metadata(&repository);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn generic_adapter_parses_access_changed_with_full_name() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "kind":"access_changed",
            "repository":{"full_name":"team/assets"}
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("access_changed", "delivery-ac2", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
        assert_eq!(event.repository().owner(), "team");
    }

    #[test]
    fn generic_adapter_parses_event_from_event_kind_field_with_unknown_event_name() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event":{"kind":"revision_pushed"},
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request =
            WebhookRequest::new("custom_event", "delivery-ek", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RevisionPushed { .. }
        ));
    }

    #[test]
    fn generic_adapter_parses_event_with_kind_field_and_empty_event_name() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "kind":"revision_pushed",
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("", "delivery-kind2", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert!(matches!(
            event.kind(),
            RepositoryWebhookEventKind::RevisionPushed { .. }
        ));
    }

    #[test]
    fn generic_adapter_parses_event_with_unknown_event_name_and_missing_kind_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "repository":{"owner":"team","name":"assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "unknown_event",
            "delivery-nokind",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn generic_adapter_parses_event_with_empty_event_name_and_missing_kind_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "repository":{"owner":"team","name":"assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("", "delivery-empty", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn generic_adapter_revision_pushed_missing_repository_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "kind":"revision_pushed",
            "revision":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "revision_pushed",
            "delivery-norepo",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_adapter_repository_deleted_missing_repository_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{"kind":"repository_deleted"}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_deleted",
            "delivery-norepo",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_adapter_repository_renamed_missing_repository_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{"kind":"repository_renamed","new_repository":{"full_name":"team/new"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_renamed",
            "delivery-norepo",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_adapter_repository_renamed_missing_new_repository_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{"kind":"repository_renamed","repository":{"full_name":"team/assets"}}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "repository_renamed",
            "delivery-nonew",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_adapter_access_changed_missing_repository_errors() {
        let adapter = adapter().unwrap();
        let body = br#"{"kind":"access_changed"}"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "access_changed",
            "delivery-norepo",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn generic_adapter_no_secret_skips_verification_and_parses() {
        let mut catalog = BuiltInProviderCatalog::new("generic-bridge").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://forge.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GenericAdapter::new(catalog, None);
        let body = br#"{
            "kind":"revision_pushed",
            "repository":{"owner":"team","name":"assets"},
            "revision":"refs/heads/main"
        }"#;
        let request = WebhookRequest::new("revision_pushed", "delivery-nosec", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_some());
    }

    #[test]
    fn generic_adapter_secret_required_when_configured() {
        let adapter = adapter().unwrap();
        let request = WebhookRequest::new("revision_pushed", "delivery-1", None, br#"{}"#);
        let event = adapter.parse_webhook(request);
        assert_eq!(
            event,
            Err(BuiltInProviderError::MissingWebhookAuthentication)
        );
    }

    #[test]
    fn generic_adapter_new_is_const() {
        let catalog = BuiltInProviderCatalog::new("generic-bridge").unwrap();
        let adapter = GenericAdapter::new(catalog, None);
        assert_eq!(adapter.kind(), ProviderKind::Generic);
    }

    #[test]
    fn generic_adapter_debug_format() {
        let catalog = BuiltInProviderCatalog::new("generic-bridge").unwrap();
        let adapter = GenericAdapter::new(catalog, None);
        let debug = format!("{adapter:?}");
        assert!(debug.contains("GenericAdapter"));
    }

    #[test]
    fn generic_adapter_clone_eq() {
        let adapter = adapter().unwrap();
        let cloned = adapter.clone();
        assert_eq!(adapter, cloned);
    }

    #[test]
    fn generic_unknown_kind_value_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "kind":"unknown_event_kind",
            "repository":{"owner":"team","name":"assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new(
            "custom_event",
            "delivery-unk-kind",
            signature.as_deref(),
            body,
        );
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn generic_adapter_invalid_json_payload_errors() {
        let mut catalog = BuiltInProviderCatalog::new("generic-bridge").unwrap();
        let repository = RepositoryRef::new(ProviderKind::Generic, "team", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://forge.example/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GenericAdapter::new(catalog, None);
        let request = WebhookRequest::new("revision_pushed", "delivery-bad", None, b"not json");
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidWebhookPayload)
        ));
    }

    #[test]
    fn generic_adapter_push_event_without_repository_owner_uses_namespace() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "repository":{"namespace":"team","name":"assets"},
            "ref":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-ns2", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.repository().owner(), "team");
    }

    #[test]
    fn generic_adapter_push_event_without_repository_name_uses_root_name() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "repository":{"owner":"team"},
            "name":"assets",
            "ref":"refs/heads/main"
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-root2", signature.as_deref(), body);
        let event = adapter.parse_webhook(request).unwrap();
        let Some(event) = event else { return };
        assert_eq!(event.repository().name(), "assets");
    }

    #[test]
    fn generic_adapter_push_event_no_ref_uses_default_fails() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "repository":{"owner":"team","name":"assets"}
        }"#;
        let signature = signature(body);
        let request = WebhookRequest::new("push", "delivery-noref", signature.as_deref(), body);
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidRevisionPayload)
        ));
    }
}
