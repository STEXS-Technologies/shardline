use std::collections::{HashMap, HashSet};

use hmac::{Hmac, Mac};
use serde_json::{Value, from_slice};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{
    AuthorizationDecision, CanonicalCloneUrl, ProviderKind, ProviderSubject, RepositoryAccess,
    RepositoryMetadata, RepositoryRef, RepositoryVisibility, RevisionRef, VcsReferenceError,
    WebhookDeliveryId,
};

type HmacSha256 = Hmac<Sha256>;
const HMAC_SHA256_HEX_BYTES: usize = 64;

/// Built-in provider adapter error.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum BuiltInProviderError {
    /// The requested repository is not registered in the adapter catalog.
    #[error("repository is not registered")]
    UnknownRepository,
    /// A repository was registered more than once.
    #[error("repository is already registered")]
    DuplicateRepository,
    /// The webhook request body was not valid JSON.
    #[error("webhook payload was not valid json")]
    InvalidWebhookPayload,
    /// The webhook payload did not describe a valid repository reference.
    #[error("webhook payload contained invalid repository fields")]
    InvalidRepositoryPayload,
    /// The webhook payload did not describe a valid revision reference.
    #[error("webhook payload contained invalid revision fields")]
    InvalidRevisionPayload,
    /// The webhook request did not include the required authentication header.
    #[error("webhook authentication header is missing")]
    MissingWebhookAuthentication,
    /// The webhook authentication header did not verify.
    #[error("webhook authentication header is invalid")]
    InvalidWebhookAuthentication,
    /// The configured integration subject was invalid.
    #[error("integration subject was invalid")]
    InvalidIntegrationSubject,
    /// The configured clone URL was invalid.
    #[error("clone url was invalid")]
    InvalidCloneUrl,
    /// The configured default revision was invalid.
    #[error("default revision was invalid")]
    InvalidDefaultRevision,
}

/// Repository access policy for built-in provider adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRepositoryPolicy {
    metadata: RepositoryMetadata,
    read_subjects: HashSet<ProviderSubject>,
    write_subjects: HashSet<ProviderSubject>,
}

impl ProviderRepositoryPolicy {
    /// Creates a repository policy entry.
    #[must_use]
    pub const fn new(
        metadata: RepositoryMetadata,
        read_subjects: HashSet<ProviderSubject>,
        write_subjects: HashSet<ProviderSubject>,
    ) -> Self {
        Self {
            metadata,
            read_subjects,
            write_subjects,
        }
    }

    /// Returns normalized repository metadata.
    #[must_use]
    pub const fn metadata(&self) -> &RepositoryMetadata {
        &self.metadata
    }

    /// Evaluates whether the requested access is allowed.
    #[must_use]
    pub fn allows(&self, subject: &ProviderSubject, access: RepositoryAccess) -> bool {
        match access {
            RepositoryAccess::Read => self.read_subjects.contains(subject),
            RepositoryAccess::Write => self.write_subjects.contains(subject),
        }
    }
}

/// Shared in-memory catalog used by built-in provider adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltInProviderCatalog {
    integration_subject: ProviderSubject,
    repositories: HashMap<RepositoryRef, ProviderRepositoryPolicy>,
}

impl BuiltInProviderCatalog {
    /// Creates a provider catalog for a concrete integration subject.
    ///
    /// # Errors
    ///
    /// Returns [`BuiltInProviderError`] when the integration subject is invalid.
    pub fn new(integration_subject: &str) -> Result<Self, BuiltInProviderError> {
        let integration_subject = ProviderSubject::new(integration_subject)
            .map_err(|_error| BuiltInProviderError::InvalidIntegrationSubject)?;

        Ok(Self {
            integration_subject,
            repositories: HashMap::new(),
        })
    }

    /// Registers a repository policy.
    ///
    /// # Errors
    ///
    /// Returns [`BuiltInProviderError::DuplicateRepository`] when the repository was
    /// already registered.
    pub fn register(
        &mut self,
        policy: ProviderRepositoryPolicy,
    ) -> Result<(), BuiltInProviderError> {
        let repository = policy.metadata().repository().clone();
        if self.repositories.insert(repository, policy).is_some() {
            return Err(BuiltInProviderError::DuplicateRepository);
        }

        Ok(())
    }

    /// Returns the configured integration subject.
    #[must_use]
    pub const fn integration_subject(&self) -> &ProviderSubject {
        &self.integration_subject
    }

    /// Looks up a repository policy.
    ///
    /// # Errors
    ///
    /// Returns [`BuiltInProviderError::UnknownRepository`] when the repository was not
    /// registered in the catalog.
    pub fn repository(
        &self,
        repository: &RepositoryRef,
    ) -> Result<&ProviderRepositoryPolicy, BuiltInProviderError> {
        self.repositories
            .get(repository)
            .ok_or(BuiltInProviderError::UnknownRepository)
    }

    /// Evaluates access using repository policy and returns a normalized decision.
    ///
    /// # Errors
    ///
    /// Returns [`BuiltInProviderError::UnknownRepository`] when the repository was not
    /// registered in the catalog.
    pub fn check_access(
        &self,
        repository: &RepositoryRef,
        subject: &ProviderSubject,
        access: RepositoryAccess,
    ) -> Result<AuthorizationDecision, BuiltInProviderError> {
        let policy = self.repository(repository)?;
        if policy.allows(subject, access) {
            return Ok(AuthorizationDecision::Allow(subject.clone()));
        }

        Ok(AuthorizationDecision::Deny)
    }
}

/// Creates repository metadata from plain configuration values.
///
/// # Errors
///
/// Returns [`BuiltInProviderError`] when any configured value is invalid.
pub fn configured_metadata(
    repository: RepositoryRef,
    visibility: RepositoryVisibility,
    default_revision: &str,
    clone_url: &str,
) -> Result<RepositoryMetadata, BuiltInProviderError> {
    let default_revision = normalize_default_revision(default_revision)
        .map_err(|_error| BuiltInProviderError::InvalidDefaultRevision)?;
    let clone_url = CanonicalCloneUrl::new(clone_url)
        .map_err(|_error| BuiltInProviderError::InvalidCloneUrl)?;

    Ok(RepositoryMetadata::new(
        repository,
        visibility,
        default_revision,
        clone_url,
    ))
}

pub(crate) fn value_at<'value>(value: &'value Value, path: &[&str]) -> Option<&'value Value> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }

    Some(current)
}

pub(crate) fn value_str<'value>(value: &'value Value, path: &[&str]) -> Option<&'value str> {
    value_at(value, path)?.as_str()
}

pub(crate) fn value_u64(value: &Value, path: &[&str]) -> Option<u64> {
    value_at(value, path)?.as_u64()
}

pub(crate) fn parse_webhook_json(body: &[u8]) -> Result<Value, BuiltInProviderError> {
    from_slice(body).map_err(|_error| BuiltInProviderError::InvalidWebhookPayload)
}

pub(crate) fn parse_repository_from_full_name(
    provider: ProviderKind,
    full_name: &str,
) -> Result<RepositoryRef, BuiltInProviderError> {
    let Some((owner, name)) = full_name.split_once('/') else {
        return Err(BuiltInProviderError::InvalidRepositoryPayload);
    };

    RepositoryRef::new(provider, owner, name)
        .map_err(|_error| BuiltInProviderError::InvalidRepositoryPayload)
}

pub(crate) const fn parse_gitlab_visibility(level: u64) -> RepositoryVisibility {
    match level {
        20 => RepositoryVisibility::Internal,
        10 => RepositoryVisibility::Private,
        _value => RepositoryVisibility::Public,
    }
}

pub(crate) fn parse_visibility_name(value: &str) -> RepositoryVisibility {
    match value {
        "private" => RepositoryVisibility::Private,
        "internal" => RepositoryVisibility::Internal,
        _value => RepositoryVisibility::Public,
    }
}

pub(crate) fn normalize_default_revision(value: &str) -> Result<RevisionRef, VcsReferenceError> {
    if value.starts_with("refs/") {
        return RevisionRef::new(value);
    }

    RevisionRef::new(&format!("refs/heads/{value}"))
}

pub(crate) fn parse_delivery_id(value: &str) -> Result<WebhookDeliveryId, BuiltInProviderError> {
    WebhookDeliveryId::new(value).map_err(|_error| BuiltInProviderError::InvalidWebhookPayload)
}

pub(crate) fn parse_revision(value: &str) -> Result<RevisionRef, BuiltInProviderError> {
    RevisionRef::new(value).map_err(|_error| BuiltInProviderError::InvalidRevisionPayload)
}

pub(crate) fn verify_prefixed_hmac_sha256(
    secret: &str,
    header: Option<&str>,
    prefix: &str,
    body: &[u8],
) -> Result<(), BuiltInProviderError> {
    let Some(header) = header else {
        return Err(BuiltInProviderError::MissingWebhookAuthentication);
    };
    let Some(signature_hex) = header.strip_prefix(prefix) else {
        return Err(BuiltInProviderError::InvalidWebhookAuthentication);
    };
    if signature_hex.len() != HMAC_SHA256_HEX_BYTES {
        return Err(BuiltInProviderError::InvalidWebhookAuthentication);
    }
    verify_hex_hmac_sha256(secret, signature_hex, body)
}

pub(crate) fn verify_hex_hmac_sha256(
    secret: &str,
    header_hex: &str,
    body: &[u8],
) -> Result<(), BuiltInProviderError> {
    if header_hex.len() != HMAC_SHA256_HEX_BYTES {
        return Err(BuiltInProviderError::InvalidWebhookAuthentication);
    }
    let signature = hex::decode(header_hex)
        .map_err(|_error| BuiltInProviderError::InvalidWebhookAuthentication)?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_error| BuiltInProviderError::InvalidWebhookAuthentication)?;
    mac.update(body);
    mac.verify_slice(&signature)
        .map_err(|_error| BuiltInProviderError::InvalidWebhookAuthentication)
}

pub(crate) fn verify_constant_time_secret(
    expected: &str,
    actual: Option<&str>,
) -> Result<(), BuiltInProviderError> {
    let Some(actual) = actual else {
        return Err(BuiltInProviderError::MissingWebhookAuthentication);
    };
    if actual.len() != expected.len() {
        return Err(BuiltInProviderError::InvalidWebhookAuthentication);
    }
    if expected.as_bytes().ct_eq(actual.as_bytes()).into() {
        return Ok(());
    }

    Err(BuiltInProviderError::InvalidWebhookAuthentication)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use hmac::Mac;

    use super::{
        BuiltInProviderCatalog, BuiltInProviderError, HMAC_SHA256_HEX_BYTES,
        ProviderRepositoryPolicy, configured_metadata, parse_repository_from_full_name,
        parse_visibility_name, verify_constant_time_secret, verify_hex_hmac_sha256,
    };
    use crate::{
        AuthorizationDecision, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
        RepositoryVisibility,
    };

    #[test]
    fn catalog_rejects_duplicate_repository_registration() {
        let mut catalog = BuiltInProviderCatalog::new("integration");
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let subject = ProviderSubject::new("user-1");

        assert!(catalog.is_ok());
        assert!(repository.is_ok());
        assert!(subject.is_ok());
        let (Ok(catalog), Ok(repository), Ok(subject)) = (&mut catalog, repository, subject) else {
            return;
        };
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://example.invalid/team/assets.git",
        );

        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };
        let first = catalog.register(ProviderRepositoryPolicy::new(
            metadata.clone(),
            HashSet::from([subject.clone()]),
            HashSet::from([subject.clone()]),
        ));
        let second = catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::from([subject]),
        ));

        assert!(first.is_ok());
        assert_eq!(second, Err(BuiltInProviderError::DuplicateRepository));
    }

    #[test]
    fn catalog_returns_allow_and_deny_from_repository_policy() {
        let mut catalog = BuiltInProviderCatalog::new("integration");
        let repository = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let allowed = ProviderSubject::new("user-1");
        let denied = ProviderSubject::new("user-2");

        assert!(catalog.is_ok());
        assert!(repository.is_ok());
        assert!(allowed.is_ok());
        assert!(denied.is_ok());
        let (Ok(catalog), Ok(repository), Ok(allowed), Ok(denied)) =
            (&mut catalog, repository, allowed, denied)
        else {
            return;
        };
        let metadata = configured_metadata(
            repository.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://example.invalid/team/assets.git",
        );

        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };
        let register = catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([allowed.clone()]),
            HashSet::new(),
        ));
        assert!(register.is_ok());

        let read = catalog.check_access(&repository, &allowed, RepositoryAccess::Read);
        let write = catalog.check_access(&repository, &allowed, RepositoryAccess::Write);
        let denied_read = catalog.check_access(&repository, &denied, RepositoryAccess::Read);

        assert!(read.is_ok());
        assert!(write.is_ok());
        assert!(denied_read.is_ok());
        let Ok(read) = read else {
            return;
        };
        let Ok(write) = write else {
            return;
        };
        let Ok(denied_read) = denied_read else {
            return;
        };

        assert_eq!(read, AuthorizationDecision::Allow(allowed));
        assert_eq!(write, AuthorizationDecision::Deny);
        assert_eq!(denied_read, AuthorizationDecision::Deny);
    }

    #[test]
    fn parser_builds_repository_from_full_name() {
        let repository = parse_repository_from_full_name(ProviderKind::GitLab, "team/assets");

        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };

        assert_eq!(repository.owner(), "team");
        assert_eq!(repository.name(), "assets");
    }

    #[test]
    fn visibility_parser_maps_private_and_internal_names() {
        assert_eq!(
            parse_visibility_name("private"),
            RepositoryVisibility::Private
        );
        assert_eq!(
            parse_visibility_name("internal"),
            RepositoryVisibility::Internal
        );
        assert_eq!(
            parse_visibility_name("public"),
            RepositoryVisibility::Public
        );
    }

    #[test]
    fn constant_time_secret_verifier_rejects_mismatch() {
        let result = verify_constant_time_secret("expected", Some("wrong"));

        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn constant_time_secret_verifier_rejects_oversized_input_before_compare() {
        let oversized = "x".repeat(4096);
        let result = verify_constant_time_secret("expected", Some(&oversized));

        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn hex_hmac_verifier_accepts_valid_signature() {
        let body = br#"{"hello":"world"}"#;
        let mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret");
        assert!(mac.is_ok());
        let Ok(mut mac) = mac else {
            return;
        };
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());

        let result = verify_hex_hmac_sha256("secret", &signature, body);

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn hex_hmac_verifier_rejects_oversized_signature_before_decode() {
        let signature = "a".repeat(HMAC_SHA256_HEX_BYTES + 1);
        let result = verify_hex_hmac_sha256("secret", &signature, b"{}");

        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn built_in_provider_error_display_all_variants() {
        let cases: &[(BuiltInProviderError, &str)] = &[
            (BuiltInProviderError::UnknownRepository, "registered"),
            (BuiltInProviderError::DuplicateRepository, "registered"),
            (BuiltInProviderError::InvalidWebhookPayload, "payload"),
            (BuiltInProviderError::InvalidRepositoryPayload, "repository"),
            (BuiltInProviderError::InvalidRevisionPayload, "revision"),
            (BuiltInProviderError::MissingWebhookAuthentication, "missing"),
            (BuiltInProviderError::InvalidWebhookAuthentication, "invalid"),
            (BuiltInProviderError::InvalidIntegrationSubject, "subject"),
            (BuiltInProviderError::InvalidCloneUrl, "url"),
            (BuiltInProviderError::InvalidDefaultRevision, "revision"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn provider_repository_policy_allows_read_subjects() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let subject = ProviderSubject::new("user-1");
        let other = ProviderSubject::new("user-2");
        assert!(repo.is_ok());
        assert!(subject.is_ok());
        assert!(other.is_ok());
        let (Ok(repo), Ok(subject), Ok(other)) = (repo, subject, other) else {
            return;
        };
        let metadata = configured_metadata(
            repo,
            RepositoryVisibility::Private,
            "main",
            "https://example.invalid/team/assets.git",
        );
        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };
        let policy = ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([subject.clone()]),
            HashSet::new(),
        );
        assert!(policy.allows(&subject, RepositoryAccess::Read));
        assert!(!policy.allows(&other, RepositoryAccess::Read));
        assert!(!policy.allows(&subject, RepositoryAccess::Write));
    }

    #[test]
    fn provider_repository_policy_allows_write_subjects() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        let subject = ProviderSubject::new("user-1");
        assert!(repo.is_ok());
        assert!(subject.is_ok());
        let (Ok(repo), Ok(subject)) = (repo, subject) else {
            return;
        };
        let metadata = configured_metadata(
            repo,
            RepositoryVisibility::Private,
            "main",
            "https://example.invalid/team/assets.git",
        );
        assert!(metadata.is_ok());
        let Ok(metadata) = metadata else {
            return;
        };
        let policy = ProviderRepositoryPolicy::new(
            metadata,
            HashSet::new(),
            HashSet::from([subject.clone()]),
        );
        assert!(policy.allows(&subject, RepositoryAccess::Write));
        assert!(!policy.allows(&subject, RepositoryAccess::Read));
    }

    #[test]
    fn parse_repository_from_full_name_rejects_missing_slash() {
        let result = parse_repository_from_full_name(ProviderKind::GitHub, "incomplete");
        assert_eq!(result, Err(BuiltInProviderError::InvalidRepositoryPayload));
    }

    #[test]
    fn parse_gitlab_visibility_maps_correctly() {
        assert_eq!(super::parse_gitlab_visibility(10), RepositoryVisibility::Private);
        assert_eq!(super::parse_gitlab_visibility(20), RepositoryVisibility::Internal);
        assert_eq!(super::parse_gitlab_visibility(0), RepositoryVisibility::Public);
        assert_eq!(super::parse_gitlab_visibility(99), RepositoryVisibility::Public);
    }

    #[test]
    fn normalize_default_revision_prepends_refs_heads() {
        let result = super::normalize_default_revision("main");
        assert!(result.is_ok());
        if let Ok(rev) = result {
            assert_eq!(rev.as_str(), "refs/heads/main");
        }
    }

    #[test]
    fn normalize_default_revision_preserves_refs_prefix() {
        let result = super::normalize_default_revision("refs/tags/v1.0");
        assert!(result.is_ok());
        if let Ok(rev) = result {
            assert_eq!(rev.as_str(), "refs/tags/v1.0");
        }
    }

    #[test]
    fn parse_delivery_id_rejects_empty() {
        let result = super::parse_delivery_id("");
        assert_eq!(result, Err(BuiltInProviderError::InvalidWebhookPayload));
    }

    #[test]
    fn parse_revision_rejects_empty() {
        let result = super::parse_revision("");
        assert_eq!(result, Err(BuiltInProviderError::InvalidRevisionPayload));
    }

    #[test]
    fn catalog_rejects_unknown_repository() {
        let catalog = BuiltInProviderCatalog::new("integration");
        assert!(catalog.is_ok());
        let Ok(catalog) = catalog else {
            return;
        };
        let unknown_repo = RepositoryRef::new(ProviderKind::GitHub, "unknown", "repo");
        assert!(unknown_repo.is_ok());
        let Ok(unknown_repo) = unknown_repo else {
            return;
        };
        assert_eq!(
            catalog.repository(&unknown_repo),
            Err(BuiltInProviderError::UnknownRepository)
        );
    }

    #[test]
    fn parsed_json_value_extraction() {
        let json: serde_json::Value = serde_json::from_str(r#"{"a":{"b":"hello","c":42}}"#).unwrap();
        assert_eq!(super::value_at(&json, &["a", "b"]).and_then(|v| v.as_str()), Some("hello"));
        assert_eq!(super::value_at(&json, &["a", "c"]).and_then(|v| v.as_u64()), Some(42));
        assert_eq!(super::value_at(&json, &["a", "missing"]), None);
        assert_eq!(super::value_at(&json, &["missing"]), None);

        assert_eq!(super::value_str(&json, &["a", "b"]), Some("hello"));
        assert_eq!(super::value_str(&json, &["a", "c"]), None);

        assert_eq!(super::value_u64(&json, &["a", "c"]), Some(42));
        assert_eq!(super::value_u64(&json, &["a", "b"]), None);
    }

    #[test]
    fn parse_webhook_json_rejects_invalid_body() {
        let result = super::parse_webhook_json(b"not json");
        assert_eq!(result, Err(BuiltInProviderError::InvalidWebhookPayload));
    }

    #[test]
    fn verify_prefixed_hmac_rejects_missing_header() {
        let result = super::verify_prefixed_hmac_sha256("secret", None, "sha256=", b"{}");
        assert_eq!(result, Err(BuiltInProviderError::MissingWebhookAuthentication));
    }

    #[test]
    fn verify_prefixed_hmac_rejects_wrong_prefix() {
        let result = super::verify_prefixed_hmac_sha256("secret", Some("sha1=abc"), "sha256=", b"{}");
        assert_eq!(result, Err(BuiltInProviderError::InvalidWebhookAuthentication));
    }

    #[test]
    fn verify_prefixed_hmac_rejects_short_signature() {
        let result = super::verify_prefixed_hmac_sha256("secret", Some("sha256=abc"), "sha256=", b"{}");
        assert_eq!(result, Err(BuiltInProviderError::InvalidWebhookAuthentication));
    }

    #[test]
    fn configured_metadata_accepts_empty_default_revision_as_refs_heads() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        assert!(repo.is_ok());
        let Ok(repo) = repo else {
            return;
        };
        let result = configured_metadata(
            repo,
            RepositoryVisibility::Public,
            "",
            "https://example.invalid/team/assets.git",
        );
        assert!(result.is_ok(), "empty revision becomes 'refs/heads/' which is valid: {result:?}");
    }

    #[test]
    fn configured_metadata_rejects_control_characters_in_clone_url() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets");
        assert!(repo.is_ok());
        let Ok(repo) = repo else {
            return;
        };
        let result = configured_metadata(
            repo,
            RepositoryVisibility::Public,
            "main",
            "https://example.invalid/team/assets.git\n",
        );
        assert_eq!(result, Err(BuiltInProviderError::InvalidCloneUrl));
    }

    #[test]
    fn verify_constant_time_secret_rejects_missing_header() {
        let result = verify_constant_time_secret("expected", None);
        assert_eq!(result, Err(BuiltInProviderError::MissingWebhookAuthentication));
    }

    #[test]
    fn verify_constant_time_secret_accepts_matching() {
        let result = verify_constant_time_secret("expected", Some("expected"));
        assert_eq!(result, Ok(()));
    }
}
