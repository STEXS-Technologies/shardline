use serde_json::Value;
use shardline_protocol::SecretString;

use crate::{
    AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
    ProviderAdapter, ProviderKind, RepositoryMetadata, RepositoryRef, RepositoryWebhookEvent,
    RepositoryWebhookEventKind, WebhookRequest,
    builtin::{
        configured_metadata, parse_delivery_id, parse_gitlab_visibility,
        parse_repository_from_full_name, parse_revision, parse_visibility_name, parse_webhook_json,
        value_str, value_u64, verify_constant_time_secret,
    },
};

/// Built-in GitLab provider adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitLabAdapter {
    catalog: BuiltInProviderCatalog,
    webhook_token: Option<SecretString>,
}

impl GitLabAdapter {
    /// Creates a GitLab adapter.
    #[must_use]
    pub const fn new(catalog: BuiltInProviderCatalog, webhook_token: Option<SecretString>) -> Self {
        Self {
            catalog,
            webhook_token,
        }
    }
}

impl ProviderAdapter for GitLabAdapter {
    type Error = BuiltInProviderError;

    fn kind(&self) -> ProviderKind {
        ProviderKind::GitLab
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
        if let Some(token) = &self.webhook_token {
            verify_constant_time_secret(token.expose_secret(), request.signature())?;
        }

        let payload = parse_webhook_json(request.body())?;
        match request.event_name() {
            "Push Hook" => parse_push_event(request, &payload),
            "Project Hook" => parse_project_event(request, &payload),
            "System Hook" => parse_system_event(request, &payload),
            _other => match value_str(&payload, &["event_name"]).unwrap_or_default() {
                "push" => parse_push_event(request, &payload),
                "project_destroy" => parse_project_event(request, &payload),
                "project_create" => parse_project_event(request, &payload),
                "project_rename" | "project_transfer" | "project_update" | "repository_update" => {
                    parse_system_event(request, &payload)
                }
                _value => Ok(None),
            },
        }
    }
}

fn parse_push_event(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["project", "path_with_namespace"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let repository = parse_repository_from_full_name(ProviderKind::GitLab, full_name)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let revision = parse_revision(value_str(payload, &["ref"]).unwrap_or_default())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RevisionPushed { revision },
    )))
}

fn parse_project_event(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let event_name = value_str(payload, &["event_name"]).unwrap_or_default();
    let Some(full_name) = value_str(payload, &["path_with_namespace"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let repository = parse_repository_from_full_name(ProviderKind::GitLab, full_name)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;

    match event_name {
        "project_destroy" => Ok(Some(RepositoryWebhookEvent::new(
            repository,
            delivery_id,
            RepositoryWebhookEventKind::RepositoryDeleted,
        ))),
        "project_create" => Ok(Some(RepositoryWebhookEvent::new(
            repository,
            delivery_id,
            RepositoryWebhookEventKind::AccessChanged,
        ))),
        _other => Ok(None),
    }
}

fn parse_system_event(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    match value_str(payload, &["event_name"]).unwrap_or_default() {
        "project_destroy" | "project_create" => parse_project_event(request, payload),
        "project_rename" | "project_transfer" => parse_project_rename_event(request, payload),
        "project_update" => parse_project_update_event(request, payload),
        "repository_update" => parse_repository_update_event(request, payload),
        _other => Ok(None),
    }
}

fn parse_project_rename_event(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let Some(previous_full_name) = value_str(payload, &["old_path_with_namespace"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let Some(current_full_name) = value_str(payload, &["path_with_namespace"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let repository = parse_repository_from_full_name(ProviderKind::GitLab, previous_full_name)?;
    let new_repository = parse_repository_from_full_name(ProviderKind::GitLab, current_full_name)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RepositoryRenamed { new_repository },
    )))
}

fn parse_project_update_event(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["path_with_namespace"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let repository = parse_repository_from_full_name(ProviderKind::GitLab, full_name)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::AccessChanged,
    )))
}

fn parse_repository_update_event(
    request: WebhookRequest<'_>,
    payload: &Value,
) -> Result<Option<RepositoryWebhookEvent>, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["project", "path_with_namespace"]) else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let repository = parse_repository_from_full_name(ProviderKind::GitLab, full_name)?;
    let delivery_id = parse_delivery_id(request.delivery_id())?;
    let refs = payload
        .get("refs")
        .and_then(Value::as_array)
        .ok_or(BuiltInProviderError::InvalidRevisionPayload)?;
    if refs.len() != 1 {
        return Ok(None);
    }
    let Some(revision_ref) = refs.first().and_then(Value::as_str) else {
        return Err(BuiltInProviderError::InvalidRevisionPayload);
    };
    let revision = parse_revision(revision_ref)?;

    Ok(Some(RepositoryWebhookEvent::new(
        repository,
        delivery_id,
        RepositoryWebhookEventKind::RevisionPushed { revision },
    )))
}

/// Derives GitLab repository metadata from a webhook payload.
///
/// # Errors
///
/// Returns [`BuiltInProviderError`] when the payload does not describe a valid project.
pub fn metadata_from_project_payload(
    payload: &Value,
) -> Result<RepositoryMetadata, BuiltInProviderError> {
    let Some(full_name) = value_str(payload, &["path_with_namespace"])
        .or_else(|| value_str(payload, &["project", "path_with_namespace"]))
    else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };
    let repository = parse_repository_from_full_name(ProviderKind::GitLab, full_name)?;
    let default_branch = value_str(payload, &["default_branch"])
        .or_else(|| value_str(payload, &["project", "default_branch"]))
        .ok_or(BuiltInProviderError::InvalidRevisionPayload)?;
    let clone_url = value_str(payload, &["http_url"])
        .or_else(|| value_str(payload, &["project", "http_url"]))
        .or_else(|| value_str(payload, &["project", "git_http_url"]))
        .ok_or(BuiltInProviderError::InvalidRepositoryPayload)?;
    let visibility = if let Some(level) = value_u64(payload, &["visibility_level"])
        .or_else(|| value_u64(payload, &["project", "visibility_level"]))
    {
        parse_gitlab_visibility(level)
    } else if let Some(value) = value_str(payload, &["project_visibility"]) {
        parse_visibility_name(value)
    } else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    configured_metadata(repository, visibility, default_branch, clone_url)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use shardline_protocol::SecretString;

    use super::{GitLabAdapter, metadata_from_project_payload};
    use crate::{
        AuthorizationDecision, AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError,
        ProviderAdapter, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility, RepositoryWebhookEventKind, RevisionRef, WebhookRequest,
        builtin::{ProviderRepositoryPolicy, configured_metadata},
    };

    fn adapter() -> Result<GitLabAdapter, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new("gitlab-token")?;
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let read_subject = ProviderSubject::new("gitlab-user-1")
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitlab.example/group/assets.git",
        )?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([read_subject]),
            HashSet::new(),
        ))?;

        Ok(GitLabAdapter::new(
            catalog,
            Some(SecretString::from_secret("token")),
        ))
    }

    #[test]
    fn gitlab_adapter_denies_unconfigured_write_access() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets");
        let revision = RevisionRef::new("refs/heads/main");
        let subject = ProviderSubject::new("gitlab-user-1");
        assert!(repository.is_ok());
        assert!(revision.is_ok());
        assert!(subject.is_ok());
        let (Ok(repository), Ok(revision), Ok(subject)) = (repository, revision, subject) else {
            return;
        };
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Write);

        let decision = adapter.check_access(&request);

        assert_eq!(decision, Ok(AuthorizationDecision::Deny));
    }

    #[test]
    fn gitlab_adapter_parses_push_hook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "event_name":"push",
            "ref":"refs/heads/main",
            "project":{"path_with_namespace":"group/assets"}
        }"#;
        let request = WebhookRequest::new("Push Hook", "delivery-1", Some("token"), body);

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
        assert_eq!(event.repository().owner(), "group");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn gitlab_adapter_parses_project_destroy_hook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "event_name":"project_destroy",
            "path_with_namespace":"group/assets"
        }"#;
        let request = WebhookRequest::new("Project Hook", "delivery-2", Some("token"), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.delivery_id().as_str(), "delivery-2");
        assert_eq!(event.repository().owner(), "group");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::RepositoryDeleted);
    }

    #[test]
    fn gitlab_adapter_parses_project_rename_system_hook() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "event_name":"project_rename",
            "path_with_namespace":"group/new-assets",
            "old_path_with_namespace":"group/assets"
        }"#;
        let request = WebhookRequest::new("System Hook", "delivery-3", Some("token"), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        let new_repository = RepositoryRef::new(ProviderKind::GitLab, "group", "new-assets");
        assert!(new_repository.is_ok());
        let Ok(new_repository) = new_repository else {
            return;
        };
        assert_eq!(event.repository().owner(), "group");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
        );
    }

    #[test]
    fn gitlab_adapter_parses_project_update_system_hook_as_access_change() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "event_name":"project_update",
            "path_with_namespace":"group/assets"
        }"#;
        let request = WebhookRequest::new("System Hook", "delivery-4", Some("token"), body);

        let event = adapter.parse_webhook(request);

        assert!(event.is_ok());
        let Ok(event) = event else {
            return;
        };
        let Some(event) = event else {
            return;
        };
        assert_eq!(event.delivery_id().as_str(), "delivery-4");
        assert_eq!(event.repository().owner(), "group");
        assert_eq!(event.repository().name(), "assets");
        assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
    }

    #[test]
    fn gitlab_adapter_parses_repository_update_system_hook_with_single_ref() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let body = br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"},
            "refs":["refs/heads/main"]
        }"#;
        let request = WebhookRequest::new("System Hook", "delivery-5", Some("token"), body);

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
        assert_eq!(
            event.kind(),
            &RepositoryWebhookEventKind::RevisionPushed { revision }
        );
    }

    #[test]
    fn gitlab_metadata_parser_uses_project_payload() {
        let payload = json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "http_url": "https://gitlab.example/group/assets.git",
                "visibility_level": 10
            }
        });

        let metadata = metadata_from_project_payload(&payload);

        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };
        assert_eq!(metadata.repository().provider(), ProviderKind::GitLab);
        assert_eq!(metadata.default_revision().as_str(), "refs/heads/main");
    }

    #[test]
    fn gitlab_adapter_rejects_invalid_token() {
        let adapter = adapter();
        assert!(adapter.is_ok());
        let Ok(adapter) = adapter else {
            return;
        };
        let request = WebhookRequest::new("Push Hook", "delivery-1", Some("wrong"), br#"{}"#);

        let event = adapter.parse_webhook(request);

        assert_eq!(
            event,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }
}
