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
}
