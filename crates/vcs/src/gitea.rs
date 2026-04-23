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
    let repository = gitea_repository(&payload)?;
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
    let repository = gitea_repository(&payload)?;

    match action {
        "deleted" => Ok(Some(RepositoryWebhookEvent::new(
            repository,
            delivery_id,
            RepositoryWebhookEventKind::RepositoryDeleted,
        ))),
        "renamed" => {
            let Some(previous_name) = gitea_previous_name(&payload) else {
                return Ok(None);
            };
            let previous =
                RepositoryRef::new(ProviderKind::Gitea, repository.owner(), previous_name)
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

fn gitea_repository(payload: &Value) -> Result<RepositoryRef, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["repository", "full_name"])
        .or_else(|| value_str(payload, &["repository", "name"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    if full_name.contains('/') {
        return parse_repository_from_full_name(ProviderKind::Gitea, full_name);
    }

    let Some(owner) = value_str(payload, &["repository", "owner", "username"])
        .or_else(|| value_str(payload, &["repository", "owner", "login"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    RepositoryRef::new(ProviderKind::Gitea, owner, full_name)
        .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)
}

fn gitea_previous_name(payload: &Value) -> Option<&str> {
    value_str(payload, &["changes", "repository", "name", "from"])
        .or_else(|| value_str(payload, &["changes", "name", "from"]))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hmac::Mac;
    use shardline_protocol::SecretString;

    use super::GiteaAdapter;
    use crate::{
        BuiltInProviderCatalog, BuiltInProviderError, ProviderAdapter, ProviderKind,
        ProviderSubject, RepositoryRef, RepositoryVisibility, RepositoryWebhookEventKind,
        RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<GiteaAdapter, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new("gitea-system")?;
        let repository = RepositoryRef::new(ProviderKind::Gitea, "team", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let subject = ProviderSubject::new("gitea-user-1")
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitea.example/team/assets.git",
        )?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::from([subject]),
        ))?;

        Ok(GiteaAdapter::new(
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
    fn gitea_adapter_parses_push_webhook() {
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
        assert_eq!(event.repository().provider(), ProviderKind::Gitea);
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn gitea_adapter_parses_repository_deleted_webhook() {
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
    fn gitea_adapter_parses_repository_renamed_webhook() {
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
        let new_repository = RepositoryRef::new(ProviderKind::Gitea, "team", "new-assets");
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
    fn gitea_adapter_parses_repository_access_change_webhook() {
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
    fn gitea_adapter_rejects_invalid_signature() {
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
}
