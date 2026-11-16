use shardline_protocol::RepositoryProvider;

/// Supported version-control provider families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    /// GitHub repositories.
    GitHub,
    /// Gitea repositories.
    Gitea,
    /// GitLab repositories.
    GitLab,
    /// Codeberg (Gitea-based) repositories.
    Codeberg,
    /// A provider implemented through the generic integration boundary.
    Generic,
}

impl ProviderKind {
    /// Returns the corresponding repository-scope provider.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::RepositoryProvider;
    /// use shardline_vcs::ProviderKind;
    ///
    /// assert_eq!(
    ///     ProviderKind::GitLab.repository_provider(),
    ///     RepositoryProvider::GitLab
    /// );
    /// ```
    #[must_use]
    pub const fn repository_provider(self) -> RepositoryProvider {
        match self {
            Self::GitHub => RepositoryProvider::GitHub,
            Self::Gitea => RepositoryProvider::Gitea,
            Self::GitLab => RepositoryProvider::GitLab,
            Self::Codeberg => RepositoryProvider::Codeberg,
            Self::Generic => RepositoryProvider::Generic,
        }
    }

    /// Returns the stable lowercase provider name.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_vcs::ProviderKind;
    ///
    /// assert_eq!(ProviderKind::Codeberg.as_str(), "codeberg");
    /// ```
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.repository_provider().as_str()
    }
}

impl From<ProviderKind> for RepositoryProvider {
    fn from(kind: ProviderKind) -> Self {
        kind.repository_provider()
    }
}

impl From<RepositoryProvider> for ProviderKind {
    fn from(provider: RepositoryProvider) -> Self {
        match provider {
            RepositoryProvider::GitHub => Self::GitHub,
            RepositoryProvider::Gitea => Self::Gitea,
            RepositoryProvider::GitLab => Self::GitLab,
            RepositoryProvider::Codeberg => Self::Codeberg,
            RepositoryProvider::Generic => Self::Generic,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashSet;

    use hmac::Mac;

    use crate::{
        BuiltInProviderCatalog, BuiltInProviderError, ProviderAdapter, ProviderKind,
        ProviderRepositoryPolicy, ProviderSubject, RepositoryRef, RepositoryVisibility,
        RepositoryWebhookEventKind, WebhookRequest, configured_metadata,
    };

    pub(crate) fn catalog(
        provider: ProviderKind,
        integration_subject: &str,
        clone_url: &str,
    ) -> Result<BuiltInProviderCatalog, BuiltInProviderError> {
        let mut catalog = BuiltInProviderCatalog::new(integration_subject)?;
        let repository = RepositoryRef::new(provider, "team", "assets")
            .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)?;
        let subject = ProviderSubject::new(&format!("{}-user-1", provider.as_str()))
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;
        let metadata =
            configured_metadata(repository, RepositoryVisibility::Private, "main", clone_url)?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::from([subject]),
        ))?;
        Ok(catalog)
    }

    pub(crate) fn signature(body: &[u8]) -> Option<String> {
        let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
        assert!(mac.is_ok());
        let Ok(mut mac) = mac else {
            return None;
        };
        mac.update(body);
        Some(hex::encode(mac.finalize().into_bytes()))
    }

    pub(crate) fn assert_repository_access_changes(
        adapter: &impl ProviderAdapter<Error = BuiltInProviderError>,
    ) {
        for (action, delivery_id) in [
            ("transferred", "delivery-4"),
            ("edited", "delivery-edit"),
            ("archived", "delivery-arch"),
            ("unarchived", "delivery-unarch"),
            ("privatized", "delivery-priv"),
            ("publicized", "delivery-pub"),
        ] {
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
                matches!(&result, Ok(Some(event)) if event.kind() == &RepositoryWebhookEventKind::AccessChanged
                    && event.repository().provider() == adapter.kind()),
                "action {action}: {result:?}"
            );
        }
    }

    pub(crate) fn assert_invalid_repository_payloads(
        adapter: &impl ProviderAdapter<Error = BuiltInProviderError>,
    ) {
        for (case, body) in [
            (
                "missing_owner",
                r#"{"ref":"refs/heads/main","repository":{"name":"assets"}}"#,
            ),
            ("missing_repository", "{}"),
            (
                "missing_name",
                r#"{"ref":"refs/heads/main","repository":{"owner":{"username":"team"}}}"#,
            ),
            (
                "missing_owner_fields",
                r#"{"ref":"refs/heads/main","repository":{"name":"assets","owner":{}}}"#,
            ),
        ] {
            let body = body.as_bytes();
            let signature = signature(body);
            let request = WebhookRequest::new("push", case, signature.as_deref(), body);
            let result = adapter.parse_webhook(request);
            assert!(
                matches!(result, Err(BuiltInProviderError::InvalidRepositoryPayload)),
                "case {case}: {result:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderKind;
    use shardline_protocol::RepositoryProvider;

    #[test]
    fn provider_names_and_conversions() {
        for (kind, repository_provider, name) in [
            (ProviderKind::GitHub, RepositoryProvider::GitHub, "github"),
            (ProviderKind::Gitea, RepositoryProvider::Gitea, "gitea"),
            (ProviderKind::GitLab, RepositoryProvider::GitLab, "gitlab"),
            (
                ProviderKind::Codeberg,
                RepositoryProvider::Codeberg,
                "codeberg",
            ),
            (
                ProviderKind::Generic,
                RepositoryProvider::Generic,
                "generic",
            ),
        ] {
            assert_eq!(kind.repository_provider(), repository_provider);
            assert_eq!(RepositoryProvider::from(kind), repository_provider);
            assert_eq!(ProviderKind::from(repository_provider), kind);
            assert_eq!(kind.as_str(), name);
        }
    }

    #[test]
    fn provider_kind_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ProviderKind>();
        assert_sync::<ProviderKind>();
    }
}
