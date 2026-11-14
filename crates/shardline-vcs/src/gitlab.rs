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
        } else {
            eprintln!(
                "[shardline] WARNING: gitlab webhook token verification SKIPPED — \
                 no webhook token configured; deployers MUST set a webhook token \
                 to prevent forged webhook injection"
            );
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

    fn parse_event(
        hook: &str,
        delivery: &str,
        payload: &serde_json::Value,
    ) -> crate::RepositoryWebhookEvent {
        let adapter = adapter().unwrap();
        let body = serde_json::to_vec(payload).unwrap();
        let result =
            adapter.parse_webhook(WebhookRequest::new(hook, delivery, Some("token"), &body));
        assert!(
            matches!(&result, Ok(Some(_))),
            "hook {hook}, delivery {delivery}, payload {payload}: {result:?}"
        );
        let event = result.unwrap().unwrap();
        assert_eq!(event.delivery_id().as_str(), delivery);
        assert_eq!(event.repository().provider(), ProviderKind::GitLab);
        assert_eq!(event.repository().owner(), "group");
        assert_eq!(event.repository().name(), "assets");
        event
    }

    #[test]
    fn gitlab_adapter_denies_unconfigured_write_access() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("gitlab-user-1").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Write);

        let decision = adapter.check_access(&request);

        assert_eq!(decision, Ok(AuthorizationDecision::Deny));
    }

    #[test]
    fn gitlab_revision_push_hooks_require_matching_events() {
        for (hook, delivery, event_name, revision) in [
            ("Push Hook", "delivery-1", "push", "refs/heads/main"),
            (
                "System Hook",
                "delivery-5",
                "repository_update",
                "refs/heads/main",
            ),
            ("Push Hook", "delivery-push2", "push", "refs/heads/feature"),
            (
                "Some Unknown Hook",
                "delivery-unk",
                "push",
                "refs/heads/main",
            ),
            (
                "Some Unknown Hook",
                "delivery-repo",
                "repository_update",
                "refs/heads/main",
            ),
        ] {
            let mut payload = serde_json::json!({
                "event_name": event_name,
                "project": {"path_with_namespace": "group/assets"},
            });
            if event_name == "repository_update" {
                payload["refs"] = serde_json::json!([revision]);
            } else {
                payload["ref"] = serde_json::json!(revision);
            }
            let event = parse_event(hook, delivery, &payload);
            let revision = RevisionRef::new(revision).unwrap();
            assert_eq!(
                event.kind(),
                &RepositoryWebhookEventKind::RevisionPushed { revision }
            );
        }
    }

    #[test]
    fn gitlab_delete_hooks_require_matching_events() {
        for (hook, delivery) in [
            ("Project Hook", "delivery-2"),
            ("System Hook", "delivery-destroy"),
            ("System Hook", "delivery-sys3"),
            ("Some Unknown Hook", "delivery-destroy"),
        ] {
            let event = parse_event(
                hook,
                delivery,
                &serde_json::json!({
                    "event_name": "project_destroy",
                    "path_with_namespace": "group/assets",
                }),
            );
            assert_eq!(event.kind(), &RepositoryWebhookEventKind::RepositoryDeleted);
        }
    }

    #[test]
    fn gitlab_rename_hooks_require_matching_events() {
        for (hook, delivery, event_name) in [
            ("System Hook", "delivery-3", "project_rename"),
            ("System Hook", "delivery-transfer", "project_transfer"),
            ("Some Unknown Hook", "delivery-ren", "project_rename"),
            ("Some Unknown Hook", "delivery-trans", "project_transfer"),
        ] {
            let event = parse_event(
                hook,
                delivery,
                &serde_json::json!({
                    "event_name": event_name,
                    "path_with_namespace": "group/new-assets",
                    "old_path_with_namespace": "group/assets",
                }),
            );
            let new_repository =
                RepositoryRef::new(ProviderKind::GitLab, "group", "new-assets").unwrap();
            assert_eq!(
                event.kind(),
                &RepositoryWebhookEventKind::RepositoryRenamed { new_repository }
            );
        }
    }

    #[test]
    fn gitlab_access_change_hooks_require_matching_events() {
        for (hook, event_name, delivery) in [
            ("System Hook", "project_update", "delivery-4"),
            ("Project Hook", "project_create", "delivery-create"),
            ("System Hook", "project_create", "delivery-sys-create"),
            ("System Hook", "project_create", "delivery-sys2"),
            ("Some Unknown Hook", "project_create", "delivery-create"),
            ("Some Unknown Hook", "project_update", "delivery-upd"),
            ("System Hook", "project_update", "delivery-upd2"),
        ] {
            let event = parse_event(
                hook,
                delivery,
                &serde_json::json!({
                    "event_name": event_name,
                    "path_with_namespace": "group/assets",
                }),
            );
            assert_eq!(event.kind(), &RepositoryWebhookEventKind::AccessChanged);
        }
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

        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert_eq!(metadata.repository().provider(), ProviderKind::GitLab);
        assert_eq!(metadata.default_revision().as_str(), "refs/heads/main");
    }

    #[test]
    fn gitlab_adapter_rejects_invalid_token() {
        let adapter = adapter().unwrap();
        let request = WebhookRequest::new("Push Hook", "delivery-1", Some("wrong"), br#"{}"#);

        let event = adapter.parse_webhook(request);

        assert_eq!(
            event,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn gitlab_adapter_no_webhook_token_skips_verification() {
        let mut catalog = BuiltInProviderCatalog::new("gitlab-token").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitlab.example/group/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GitLabAdapter::new(catalog, None);

        let body = br#"{"ref":"refs/heads/main","project":{"path_with_namespace":"group/assets"}}"#;
        let request = WebhookRequest::new("Push Hook", "delivery-1", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_some());
    }

    #[test]
    fn gitlab_adapter_no_webhook_token_skips_verification_with_push_hook_name() {
        let mut catalog = BuiltInProviderCatalog::new("gitlab-token").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://gitlab.example/group/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject]),
                HashSet::new(),
            ))
            .unwrap();
        let adapter = GitLabAdapter::new(catalog, None);

        let body = br#"{
            "event_name":"push",
            "ref":"refs/heads/main",
            "project":{"path_with_namespace":"group/assets"}
        }"#;
        let request = WebhookRequest::new("Push Hook", "delivery-1", None, body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_some());
    }

    #[test]
    fn gitlab_adapter_system_hook_unknown_event_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{"event_name":"unknown_event","path_with_namespace":"group/assets"}"#;
        let request = WebhookRequest::new("System Hook", "delivery-unk", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_adapter_unknown_hook_with_unknown_event_name_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{"event_name":"unknown","path_with_namespace":"group/assets"}"#;
        let request = WebhookRequest::new("Some Random Hook", "delivery-rand", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_webhook_invalid_repository_payloads() {
        for (case, hook, delivery, body) in [
            (
                "adapter_push_hook_missing_project_errors",
                "Push Hook",
                "delivery-no-proj",
                br#"{"ref":"refs/heads/main"}"#.as_slice(),
            ),
            (
                "system_hook_repository_update_missing_project_errors",
                "System Hook",
                "delivery-no-proj",
                br#"{"event_name":"repository_update","refs":["refs/heads/main"]}"#.as_slice(),
            ),
            (
                "push_hook_missing_project_path_with_namespace_errors",
                "Push Hook",
                "delivery-miss",
                br#"{
            "event_name":"push",
            "ref":"refs/heads/main",
            "project":{}
        }"#
                .as_slice(),
            ),
            (
                "project_hook_missing_path_with_namespace_errors",
                "Project Hook",
                "delivery-miss2",
                br#"{
            "event_name":"project_destroy"
        }"#
                .as_slice(),
            ),
            (
                "project_rename_missing_old_path_errors",
                "System Hook",
                "delivery-noold",
                br#"{
            "event_name":"project_rename",
            "path_with_namespace":"group/new-assets"
        }"#
                .as_slice(),
            ),
            (
                "project_rename_missing_new_path_errors",
                "System Hook",
                "delivery-nonew",
                br#"{
            "event_name":"project_rename",
            "old_path_with_namespace":"group/assets"
        }"#
                .as_slice(),
            ),
            (
                "project_update_missing_path_with_namespace_errors",
                "System Hook",
                "delivery-no-path",
                br#"{
            "event_name":"project_update"
        }"#
                .as_slice(),
            ),
        ] {
            let adapter = adapter().unwrap();
            let request = WebhookRequest::new(hook, delivery, Some("token"), body);
            let result = adapter.parse_webhook(request);
            assert!(
                matches!(result, Err(BuiltInProviderError::InvalidRepositoryPayload)),
                "case {case}: {result:?}"
            );
        }
    }

    #[test]
    fn gitlab_adapter_system_hook_repository_update_multiple_refs_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"},
            "refs":["refs/heads/main", "refs/heads/feature"]
        }"#;
        let request = WebhookRequest::new("System Hook", "delivery-multi", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_webhook_invalid_revision_payloads() {
        for (case, hook, delivery, body) in [
            ("system_hook_repository_update_invalid_refs_errors", "System Hook", "delivery-bad-ref", br#"{"event_name":"repository_update","project":{"path_with_namespace":"group/assets"},"refs":[""]}"#.as_slice()),
            ("system_hook_repository_update_missing_refs_errors", "System Hook", "delivery-no-refs", br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"}
        }"#.as_slice()),
            ("system_hook_repository_update_non_array_refs_errors", "System Hook", "delivery-badrefs", br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"},
            "refs":"not_an_array"
        }"#.as_slice()),
            ("system_hook_repository_update_non_string_ref_errors", "System Hook", "delivery-int-ref", br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"},
            "refs":[42]
        }"#.as_slice()),
            ("adapter_push_hook_no_ref_errors", "Push Hook", "delivery-noref", br#"{
            "event_name":"push",
            "project":{"path_with_namespace":"group/assets"}
        }"#.as_slice()),
        ] {
            let adapter = adapter().unwrap();
            let request = WebhookRequest::new(hook, delivery, Some("token"), body);
            let result = adapter.parse_webhook(request);
            assert!(
                matches!(result, Err(BuiltInProviderError::InvalidRevisionPayload)),
                "case {case}: {result:?}"
            );
        }
    }

    #[test]
    fn gitlab_metadata_parser_uses_flat_payload_fields() {
        let payload = serde_json::json!({
            "path_with_namespace": "group/assets",
            "default_branch": "main",
            "http_url": "https://gitlab.example/group/assets.git",
            "visibility_level": 10
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert_eq!(metadata.repository().owner(), "group");
        assert_eq!(metadata.default_revision().as_str(), "refs/heads/main");
    }

    #[test]
    fn gitlab_metadata_parser_falls_back_to_git_http_url() {
        let payload = serde_json::json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "git_http_url": "https://gitlab.example/group/assets.git",
                "visibility_level": 10
            }
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert!(metadata.clone_url().as_str().contains("gitlab.example"));
    }

    #[test]
    fn gitlab_metadata_parser_missing_visibility_errors() {
        let payload = serde_json::json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "http_url": "https://gitlab.example/group/assets.git"
            }
        });
        let result = metadata_from_project_payload(&payload);
        assert!(matches!(
            result,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn gitlab_metadata_parser_missing_clone_url_errors() {
        let payload = serde_json::json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "visibility_level": 10
            }
        });
        let result = metadata_from_project_payload(&payload);
        assert!(matches!(
            result,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn gitlab_metadata_parser_missing_default_branch_errors() {
        let payload = serde_json::json!({
            "project": {
                "path_with_namespace": "group/assets",
                "http_url": "https://gitlab.example/group/assets.git",
                "visibility_level": 10
            }
        });
        let result = metadata_from_project_payload(&payload);
        assert!(matches!(
            result,
            Err(BuiltInProviderError::InvalidRevisionPayload)
        ));
    }

    #[test]
    fn gitlab_adapter_kind_returns_gitlab() {
        let adapter = adapter().unwrap();
        assert_eq!(adapter.kind(), ProviderKind::GitLab);
    }

    #[test]
    fn gitlab_adapter_check_access_allows_authorized_subject_read() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("gitlab-user-1").unwrap();
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
    fn gitlab_adapter_check_access_denies_unauthorized_subject() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("unknown-user").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let decision = adapter.check_access(&request).unwrap();
        assert_eq!(decision, AuthorizationDecision::Deny);
    }

    #[test]
    fn gitlab_adapter_check_access_rejects_unknown_repository() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "unknown", "repo").unwrap();
        let revision = RevisionRef::new("refs/heads/main").unwrap();
        let subject = ProviderSubject::new("gitlab-user-1").unwrap();
        let request =
            AuthorizationRequest::new(subject, repository, revision, RepositoryAccess::Read);
        let result = adapter.check_access(&request);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn gitlab_adapter_repository_metadata_returns_metadata() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let metadata = adapter.repository_metadata(&repository).unwrap();
        assert_eq!(metadata.repository(), &repository);
        assert_eq!(metadata.visibility(), RepositoryVisibility::Private);
        assert_eq!(
            metadata.clone_url().as_str(),
            "https://gitlab.example/group/assets.git"
        );
    }

    #[test]
    fn gitlab_adapter_repository_metadata_unknown_repo_errors() {
        let adapter = adapter().unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "unknown", "repo").unwrap();
        let result = adapter.repository_metadata(&repository);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn gitlab_adapter_parses_unknown_hook_with_unknown_event_name_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event_name":"completely_unknown",
            "path_with_namespace":"group/assets"
        }"#;
        let request =
            WebhookRequest::new("Some Unknown Hook", "delivery-unk2", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_project_hook_unknown_event_name_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event_name":"project_unknown",
            "path_with_namespace":"group/assets"
        }"#;
        let request = WebhookRequest::new("Project Hook", "delivery-unk", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_system_hook_repository_update_empty_refs_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event_name":"repository_update",
            "project":{"path_with_namespace":"group/assets"},
            "refs":[]
        }"#;
        let request =
            WebhookRequest::new("System Hook", "delivery-empty-refs", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_metadata_parser_uses_project_visibility_name() {
        let payload = serde_json::json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "http_url": "https://gitlab.example/group/assets.git"
            },
            "project_visibility": "private"
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert_eq!(metadata.visibility(), RepositoryVisibility::Private);
    }

    #[test]
    fn gitlab_metadata_parser_uses_visibility_level_from_root() {
        let payload = serde_json::json!({
            "path_with_namespace": "group/assets",
            "default_branch": "main",
            "http_url": "https://gitlab.example/group/assets.git",
            "visibility_level": 10
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert_eq!(metadata.visibility(), RepositoryVisibility::Private);
    }

    #[test]
    fn gitlab_metadata_parser_uses_project_visibility_name_internal() {
        let payload = serde_json::json!({
            "path_with_namespace": "group/assets",
            "default_branch": "main",
            "http_url": "https://gitlab.example/group/assets.git",
            "project_visibility": "internal"
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert_eq!(metadata.visibility(), RepositoryVisibility::Internal);
    }

    #[test]
    fn gitlab_metadata_parser_missing_visibility_and_level_errors() {
        let payload = serde_json::json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "http_url": "https://gitlab.example/group/assets.git"
            }
        });
        let result = metadata_from_project_payload(&payload);
        assert!(matches!(
            result,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }

    #[test]
    fn gitlab_adapter_new_is_const() {
        let catalog = BuiltInProviderCatalog::new("gitlab-token").unwrap();
        let adapter = GitLabAdapter::new(catalog, None);
        assert_eq!(adapter.kind(), ProviderKind::GitLab);
    }

    #[test]
    fn gitlab_adapter_debug_format() {
        let catalog = BuiltInProviderCatalog::new("gitlab-token").unwrap();
        let adapter = GitLabAdapter::new(catalog, None);
        let debug = format!("{adapter:?}");
        assert!(debug.contains("GitLabAdapter"));
    }

    #[test]
    fn gitlab_adapter_clone_eq() {
        let adapter = adapter().unwrap();
        let cloned = adapter.clone();
        assert_eq!(adapter, cloned);
    }

    #[test]
    fn gitlab_adapter_check_allows_write_access() {
        let mut catalog = BuiltInProviderCatalog::new("gitlab-token").unwrap();
        let repository = RepositoryRef::new(ProviderKind::GitLab, "group", "assets").unwrap();
        let subject = ProviderSubject::new("gitlab-writer").unwrap();
        let metadata = configured_metadata(
            repository.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://gitlab.example/group/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([subject.clone()]),
                HashSet::from([subject.clone()]),
            ))
            .unwrap();
        let adapter = GitLabAdapter::new(catalog, None);
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
    fn gitlab_system_hook_unknown_event_name_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event_name":"some_unknown_system_event",
            "path_with_namespace":"group/assets"
        }"#;
        let request = WebhookRequest::new("System Hook", "delivery-unk", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_adapter_rejects_invalid_json_body() {
        let adapter = adapter().unwrap();
        let request = WebhookRequest::new("Push Hook", "delivery-bad", Some("token"), b"not json");
        let event = adapter.parse_webhook(request);
        assert!(matches!(
            event,
            Err(BuiltInProviderError::InvalidWebhookPayload)
        ));
    }

    #[test]
    fn gitlab_metadata_from_project_payload_with_visibility_level_in_project() {
        let payload = json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "http_url": "https://gitlab.example/group/assets.git",
                "visibility_level": 20
            }
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert_eq!(metadata.visibility(), RepositoryVisibility::Internal);
    }

    #[test]
    fn gitlab_metadata_from_project_payload_uses_git_http_url() {
        let payload = json!({
            "project": {
                "path_with_namespace": "group/assets",
                "default_branch": "main",
                "git_http_url": "https://gitlab.example/group/assets.git",
                "visibility_level": 10
            }
        });
        let metadata = metadata_from_project_payload(&payload).unwrap();
        assert!(metadata.clone_url().as_str().contains("gitlab.example"));
    }

    #[test]
    fn gitlab_adapter_project_hook_create_unknown_event_name_returns_none() {
        let adapter = adapter().unwrap();
        let body = br#"{
            "event_name":"some_project_event",
            "path_with_namespace":"group/assets"
        }"#;
        let request = WebhookRequest::new("Project Hook", "delivery-unk3", Some("token"), body);
        let event = adapter.parse_webhook(request).unwrap();
        assert!(event.is_none());
    }

    #[test]
    fn gitlab_metadata_parser_missing_path_with_namespace_errors() {
        let payload = serde_json::json!({
            "default_branch": "main",
            "http_url": "https://gitlab.example/group/assets.git",
            "visibility_level": 10
        });
        let result = metadata_from_project_payload(&payload);
        assert!(matches!(
            result,
            Err(BuiltInProviderError::InvalidRepositoryPayload)
        ));
    }
}
