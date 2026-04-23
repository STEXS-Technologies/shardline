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

/// Built-in GitHub provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubAdapter {
    catalog: BuiltInProviderCatalog,
    webhook_secret: Option<SecretString>,
}

impl GitHubAdapter {
    /// Creates a GitHub adapter.
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

impl ProviderAdapter for GitHubAdapter {
    type Error = BuiltInProviderError;

    fn kind(&self) -> ProviderKind {
        ProviderKind::GitHub
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

        match request.event_name() {
            "ping" => Ok(None),
            "push" => parse_push_event(request),
            "repository" => parse_repository_event(request),
            _other => Ok(None),
        }
    }
}

fn parse_push_event(
    request: WebhookRequest<'_>,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let payload = parse_webhook_json(request.body())?;
    let repository = github_repository(&payload)?;
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
    let repository = github_repository(&payload)?;

    match action {
        "deleted" => Ok(Some(RepositoryWebhookEvent::new(
            repository,
            delivery_id,
            RepositoryWebhookEventKind::RepositoryDeleted,
        ))),
        "renamed" => {
            let Some(previous_name) = github_previous_name(&payload) else {
                return Ok(None);
            };
            let previous =
                RepositoryRef::new(ProviderKind::GitHub, repository.owner(), previous_name)
                    .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;

            Ok(Some(RepositoryWebhookEvent::new(
                previous,
                delivery_id,
                RepositoryWebhookEventKind::RepositoryRenamed {
                    new_repository: repository,
                },
            )))
        }
        "edited" | "privatized" | "publicized" | "transferred" => {
            Ok(Some(RepositoryWebhookEvent::new(
                repository,
                delivery_id,
                RepositoryWebhookEventKind::AccessChanged,
            )))
        }
        _other => Ok(None),
    }
}

fn github_repository(payload: &Value) -> Result<RepositoryRef, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["repository", "full_name"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    parse_repository_from_full_name(ProviderKind::GitHub, full_name)
}

fn github_previous_name(payload: &Value) -> Option<&str> {
    value_str(payload, &["changes", "repository", "name", "from"])
        .or_else(|| value_str(payload, &["changes", "name", "from"]))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hmac::Mac;
    use shardline_protocol::SecretString;

    use super::GitHubAdapter;
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        ProviderAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RepositoryWebhookEventKind, RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<GitHubAdapter, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new("github-app")?;
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let subject = ProviderSubject::new("github-user-1")
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
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

        Ok(GitHubAdapter::new(
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
    fn github_adapter_returns_registered_metadata() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
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
            "https://github.example/team/assets.git"
        );
        assert_eq!(metadata.default_revision().as_str(), "refs/heads/main");
    }

    #[test]
    fn github_adapter_allows_registered_write_access() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let subject = ProviderSubject::new("github-user-1");
        let denied_subject = ProviderSubject::new("github-user-2");
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
    fn github_adapter_parses_push_webhook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "ref":"refs/heads/main",
            "repository":{"full_name":"team/assets"}
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
        assert_eq!(event.delivery_id().as_str(), "delivery-1");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn github_adapter_parses_repository_rename() {
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
        let request = WebhookRequest::new("repository", "delivery-2", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let new_repository = RepositoryRef::new(ProviderKind::GitHub, "team", "new-assets");
        assert!(new_repository.is_ok());
        let Ok(new_repository) = new_repository else {
            return;
        };
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
        );
    }

    #[test]
    fn github_adapter_parses_repository_transfer_as_access_change() {
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
        let request = WebhookRequest::new("repository", "delivery-3", signature.as_deref(), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.repository().owner(), "team");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn github_adapter_rejects_invalid_signature() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{"ref":"refs/heads/main","repository":{"full_name":"team/assets"}}"#;
        let request = WebhookRequest::new("push", "delivery-1", Some("sha256=deadbeef"), body);

        let event = adapter.parse_webhook(request);

        assert_eq!(
            event,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }
}
