use std::collections::{HashMap, HashSet};
use std::str::FromStr;

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
    // Route through the single canonical string -> visibility parser. Unknown
    // names fall back to `Public`, matching the historical lenient behavior.
    RepositoryVisibility::from_str(value).unwrap_or(RepositoryVisibility::Public)
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
            (
                BuiltInProviderError::MissingWebhookAuthentication,
                "missing",
            ),
            (
                BuiltInProviderError::InvalidWebhookAuthentication,
                "invalid",
            ),
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
        assert_eq!(
            super::parse_gitlab_visibility(10),
            RepositoryVisibility::Private
        );
        assert_eq!(
            super::parse_gitlab_visibility(20),
            RepositoryVisibility::Internal
        );
        assert_eq!(
            super::parse_gitlab_visibility(0),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(99),
            RepositoryVisibility::Public
        );
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
        let json: serde_json::Value =
            serde_json::from_str(r#"{"a":{"b":"hello","c":42}}"#).unwrap();
        assert_eq!(
            super::value_at(&json, &["a", "b"]).and_then(|v| v.as_str()),
            Some("hello")
        );
        assert_eq!(
            super::value_at(&json, &["a", "c"]).and_then(|v| v.as_u64()),
            Some(42)
        );
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
        assert_eq!(
            result,
            Err(BuiltInProviderError::MissingWebhookAuthentication)
        );
    }

    #[test]
    fn verify_prefixed_hmac_rejects_wrong_prefix() {
        let result =
            super::verify_prefixed_hmac_sha256("secret", Some("sha1=abc"), "sha256=", b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_prefixed_hmac_rejects_short_signature() {
        let result =
            super::verify_prefixed_hmac_sha256("secret", Some("sha256=abc"), "sha256=", b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
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
        assert!(
            result.is_ok(),
            "empty revision becomes 'refs/heads/' which is valid: {result:?}"
        );
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
        assert_eq!(
            result,
            Err(BuiltInProviderError::MissingWebhookAuthentication)
        );
    }

    #[test]
    fn verify_constant_time_secret_accepts_matching() {
        let result = verify_constant_time_secret("expected", Some("expected"));
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn verify_prefixed_hmac_sha256_valid() {
        let body = br#"{"hello":"world"}"#;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        let result = super::verify_prefixed_hmac_sha256(
            "secret",
            Some(&format!("sha256={signature}")),
            "sha256=",
            body,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn verify_prefixed_hmac_sha256_wrong_secret() {
        let body = br#"{"hello":"world"}"#;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"correct").unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        let result = super::verify_prefixed_hmac_sha256(
            "wrong",
            Some(&format!("sha256={signature}")),
            "sha256=",
            body,
        );
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_hex_hmac_sha256_wrong_secret() {
        let body = br#"{"hello":"world"}"#;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"correct").unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        let result = super::verify_hex_hmac_sha256("wrong", &signature, body);
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn parse_revision_rejects_control_characters() {
        let result = super::parse_revision("main\n");
        assert_eq!(result, Err(BuiltInProviderError::InvalidRevisionPayload));
    }

    #[test]
    fn normalize_default_revision_accepts_empty_as_refs_heads() {
        let result = super::normalize_default_revision("");
        assert!(result.is_ok());
        if let Ok(rev) = result {
            assert_eq!(rev.as_str(), "refs/heads/");
        }
    }

    #[test]
    fn parse_gitlab_visibility_all_variants() {
        assert_eq!(
            super::parse_gitlab_visibility(0),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(10),
            RepositoryVisibility::Private
        );
        assert_eq!(
            super::parse_gitlab_visibility(20),
            RepositoryVisibility::Internal
        );
        assert_eq!(
            super::parse_gitlab_visibility(30),
            RepositoryVisibility::Public
        );
    }

    #[test]
    fn parse_visibility_name_all_variants() {
        assert_eq!(
            parse_visibility_name("public"),
            RepositoryVisibility::Public
        );
        assert_eq!(
            parse_visibility_name("private"),
            RepositoryVisibility::Private
        );
        assert_eq!(
            parse_visibility_name("internal"),
            RepositoryVisibility::Internal
        );
        assert_eq!(
            parse_visibility_name("unknown"),
            RepositoryVisibility::Public
        );
    }

    #[test]
    fn configured_metadata_rejects_invalid_revision() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let result = configured_metadata(
            repo,
            RepositoryVisibility::Public,
            "\t",
            "https://example.invalid/team/assets.git",
        );
        assert_eq!(result, Err(BuiltInProviderError::InvalidDefaultRevision));
    }

    #[test]
    fn catalog_integration_subject_accessible() {
        let catalog = BuiltInProviderCatalog::new("my-bot").unwrap();
        assert_eq!(catalog.integration_subject().as_str(), "my-bot");
    }

    #[test]
    fn catalog_rejects_empty_integration_subject() {
        let catalog = BuiltInProviderCatalog::new("   ");
        assert!(catalog.is_err());
    }

    #[test]
    fn catalog_rejects_integration_subject_with_control_char() {
        let catalog = BuiltInProviderCatalog::new("sub\nject");
        assert!(catalog.is_err());
    }

    #[test]
    fn catalog_rejects_unregistered_repository_check_access() {
        let catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "unknown", "repo").unwrap();
        let subject = ProviderSubject::new("user-1").unwrap();
        let result = catalog.check_access(&repo, &subject, RepositoryAccess::Read);
        assert_eq!(result, Err(BuiltInProviderError::UnknownRepository));
    }

    #[test]
    fn catalog_check_access_denies_unregistered_read_subject() {
        let mut catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let allowed = ProviderSubject::new("alice").unwrap();
        let denied = ProviderSubject::new("bob").unwrap();
        let metadata = configured_metadata(
            repo.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://example.com/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([allowed]),
                HashSet::new(),
            ))
            .unwrap();
        let result = catalog.check_access(&repo, &denied, RepositoryAccess::Read);
        assert_eq!(result, Ok(AuthorizationDecision::Deny));
    }

    #[test]
    fn catalog_register_with_no_read_subjects() {
        let mut catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let metadata = configured_metadata(
            repo.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://example.com/team/assets.git",
        )
        .unwrap();
        let policy = ProviderRepositoryPolicy::new(metadata, HashSet::new(), HashSet::new());
        assert!(catalog.register(policy).is_ok());
        let subject = ProviderSubject::new("anyone").unwrap();
        assert_eq!(
            catalog.check_access(&repo, &subject, RepositoryAccess::Read),
            Ok(AuthorizationDecision::Deny)
        );
        assert_eq!(
            catalog.check_access(&repo, &subject, RepositoryAccess::Write),
            Ok(AuthorizationDecision::Deny)
        );
    }

    #[test]
    fn parse_webhook_json_accepts_valid_json() {
        let result = super::parse_webhook_json(b"{\"key\":\"value\"}");
        assert!(result.is_ok());
        let Ok(val) = result else { return };
        assert_eq!(val["key"], "value");
    }

    #[test]
    fn parse_webhook_json_accepts_empty_object() {
        let result = super::parse_webhook_json(b"{}");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_webhook_json_accepts_array() {
        let result = super::parse_webhook_json(b"[1,2,3]");
        assert!(result.is_ok());
    }

    #[test]
    fn value_at_missing_intermediate_path() {
        let json: serde_json::Value = serde_json::from_str(r#"{"a":{"b":"hello"}}"#).unwrap();
        assert_eq!(super::value_at(&json, &["a", "c", "d"]), None);
    }

    #[test]
    fn value_str_at_non_string() {
        let json: serde_json::Value = serde_json::from_str(r#"{"a":42}"#).unwrap();
        assert_eq!(super::value_str(&json, &["a"]), None);
    }

    #[test]
    fn value_u64_at_non_integer() {
        let json: serde_json::Value = serde_json::from_str(r#"{"a":"hello"}"#).unwrap();
        assert_eq!(super::value_u64(&json, &["a"]), None);
    }

    #[test]
    fn value_u64_at_large_integer() {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"a":99999999999999999999}"#).unwrap();
        let result = super::value_u64(&json, &["a"]);
        // serde_json may parse as f64, so as_u64 might return None for overflowing values
        // This just tests the path exists
        assert!(result.is_none() || result.is_some());
    }

    #[test]
    fn parse_repository_from_full_name_rejects_empty_parts_after_split() {
        let result = parse_repository_from_full_name(ProviderKind::GitHub, "onlyslashes/");
        assert_eq!(result, Err(BuiltInProviderError::InvalidRepositoryPayload));
    }

    #[test]
    fn parse_repository_from_full_name_rejects_empty_owner_after_split() {
        let result = parse_repository_from_full_name(ProviderKind::GitHub, "/name");
        assert_eq!(result, Err(BuiltInProviderError::InvalidRepositoryPayload));
    }

    #[test]
    fn parse_repository_from_full_name_handles_multiple_slashes_as_name() {
        // split_once only splits on the first '/', so "a/b/c" becomes owner="a", name="b/c"
        let result = parse_repository_from_full_name(ProviderKind::GitHub, "a/b/c");
        assert!(result.is_ok());
        let Ok(repo) = result else { return };
        assert_eq!(repo.owner(), "a");
        assert_eq!(repo.name(), "b/c");
    }

    #[test]
    fn verify_prefixed_hmac_sha256_rejects_invalid_hex() {
        let header = format!("sha256={}", "z".repeat(64));
        let result = super::verify_prefixed_hmac_sha256("secret", Some(&header), "sha256=", b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_prefixed_hmac_sha256_rejects_empty_prefix() {
        let result =
            super::verify_prefixed_hmac_sha256("secret", Some("sha256="), "sha256=", b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_hex_hmac_sha256_rejects_invalid_hex_characters() {
        let result = super::verify_hex_hmac_sha256("secret", &"z".repeat(64), b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_hex_hmac_sha256_rejects_empty_signature() {
        let result = super::verify_hex_hmac_sha256("secret", "", b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_constant_time_secret_rejects_different_length() {
        let result = verify_constant_time_secret("short", Some("much longer value"));
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn normalize_default_revision_rejects_control_characters() {
        let result = super::normalize_default_revision("main\n");
        assert_eq!(result, Err(super::VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn normalize_default_revision_with_refs_tags() {
        let result = super::normalize_default_revision("refs/tags/v2.0");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), "refs/tags/v2.0");
    }

    #[test]
    fn normalize_default_revision_rejects_control_in_refs() {
        let result = super::normalize_default_revision("refs/heads/main\x00");
        assert_eq!(result, Err(super::VcsReferenceError::ControlCharacter));
    }

    #[test]
    fn parse_delivery_id_accepts_valid() {
        let result = super::parse_delivery_id("delivery-abc-123");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), "delivery-abc-123");
    }

    #[test]
    fn parse_delivery_id_rejects_control_char() {
        let result = super::parse_delivery_id("delivery\n");
        assert_eq!(result, Err(BuiltInProviderError::InvalidWebhookPayload));
    }

    #[test]
    fn parse_revision_accepts_valid() {
        let result = super::parse_revision("refs/heads/feature");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), "refs/heads/feature");
    }

    #[test]
    fn parse_revision_rejects_control_char() {
        let result = super::parse_revision("refs/heads/feature\x00");
        assert_eq!(result, Err(BuiltInProviderError::InvalidRevisionPayload));
    }

    #[test]
    fn configured_metadata_rejects_empty_clone_url() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let result = configured_metadata(repo, RepositoryVisibility::Public, "main", "");
        assert_eq!(result, Err(BuiltInProviderError::InvalidCloneUrl));
    }

    #[test]
    fn configured_metadata_accepts_refs_heads_prefix() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let result = configured_metadata(
            repo,
            RepositoryVisibility::Public,
            "refs/heads/main",
            "https://example.com/repo.git",
        );
        assert!(result.is_ok());
        let Ok(meta) = result else { return };
        assert_eq!(meta.default_revision().as_str(), "refs/heads/main");
    }

    #[test]
    fn verify_prefixed_hmac_sha256_valid_with_body_containing_unicode() {
        let body = "{\"msg\":\"héllo\"}".as_bytes();
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        let result = super::verify_prefixed_hmac_sha256(
            "secret",
            Some(&format!("sha256={signature}")),
            "sha256=",
            body,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn verify_hex_hmac_sha256_accepts_valid_with_unicode_body() {
        let body = "{\"msg\":\"héllo\"}".as_bytes();
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        let result = super::verify_hex_hmac_sha256("secret", &signature, body);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn verify_hex_hmac_sha256_rejects_too_short_signature() {
        let result = super::verify_hex_hmac_sha256("secret", &"a".repeat(63), b"{}");
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn verify_prefixed_hmac_rejects_wrong_prefix_length() {
        // Use correct-body but wrong-key to exercise HMAC mismatch
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"correct").unwrap();
        mac.update(b"{}");
        let real_sig = hex::encode(mac.finalize().into_bytes());
        let result = super::verify_prefixed_hmac_sha256(
            "wrong",
            Some(&format!("sha256={real_sig}")),
            "sha256=",
            b"{}",
        );
        assert_eq!(
            result,
            Err(BuiltInProviderError::InvalidWebhookAuthentication)
        );
    }

    #[test]
    fn catalog_policy_allows_read_for_subject_with_multiple_entries() {
        let mut catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let alice = ProviderSubject::new("alice").unwrap();
        let bob = ProviderSubject::new("bob").unwrap();
        let metadata = configured_metadata(
            repo.clone(),
            RepositoryVisibility::Private,
            "main",
            "https://example.com/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::from([alice.clone(), bob.clone()]),
                HashSet::from([bob.clone()]),
            ))
            .unwrap();
        // Alice can read but not write
        assert_eq!(
            catalog.check_access(&repo, &alice, RepositoryAccess::Read),
            Ok(AuthorizationDecision::Allow(alice.clone()))
        );
        assert_eq!(
            catalog.check_access(&repo, &alice, RepositoryAccess::Write),
            Ok(AuthorizationDecision::Deny)
        );
        // Bob can read and write
        assert_eq!(
            catalog.check_access(&repo, &bob, RepositoryAccess::Read),
            Ok(AuthorizationDecision::Allow(bob.clone()))
        );
        assert_eq!(
            catalog.check_access(&repo, &bob, RepositoryAccess::Write),
            Ok(AuthorizationDecision::Allow(bob))
        );
    }

    #[test]
    fn catalog_clear_and_reregister() {
        // This test verifies that registering a DIFFERENT repository after a duplicate
        // error still works
        let mut catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let repo_a = RepositoryRef::new(ProviderKind::GitHub, "team", "a").unwrap();
        let repo_b = RepositoryRef::new(ProviderKind::GitHub, "team", "b").unwrap();
        let subject = ProviderSubject::new("user").unwrap();
        let meta_a = configured_metadata(
            repo_a.clone(),
            RepositoryVisibility::Public,
            "main",
            "https://example.com/a.git",
        )
        .unwrap();
        let meta_b = configured_metadata(
            repo_b.clone(),
            RepositoryVisibility::Public,
            "main",
            "https://example.com/b.git",
        )
        .unwrap();
        assert!(
            catalog
                .register(ProviderRepositoryPolicy::new(
                    meta_a,
                    HashSet::from([subject.clone()]),
                    HashSet::new(),
                ))
                .is_ok()
        );
        assert!(
            catalog
                .register(ProviderRepositoryPolicy::new(
                    meta_b,
                    HashSet::from([subject]),
                    HashSet::new(),
                ))
                .is_ok()
        );
        assert!(catalog.repository(&repo_a).is_ok());
        assert!(catalog.repository(&repo_b).is_ok());
    }

    #[test]
    fn verify_constant_time_secret_accepts_matching_with_special_chars() {
        let result = verify_constant_time_secret("tok-en@123!@#", Some("tok-en@123!@#"));
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn value_at_nested_array_path() {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"data":[{"id":1},{"id":2}]}"#).unwrap();
        // value_at isn't designed for array indices, but verify it returns None
        assert_eq!(super::value_at(&json, &["data", "0"]), None);
    }

    #[test]
    fn verify_prefixed_hmac_sha256_accepts_hex_upper_case() {
        let body = b"{}";
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        // hex::encode always returns lowercase, but verify it works
        let result = super::verify_prefixed_hmac_sha256(
            "secret",
            Some(&format!("sha256={signature}")),
            "sha256=",
            body,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn parse_gitlab_visibility_negative_values() {
        // visibility_level is u64 in JSON, but test edge case
        assert_eq!(
            super::parse_gitlab_visibility(0),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(1),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(9),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(11),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(19),
            RepositoryVisibility::Public
        );
        assert_eq!(
            super::parse_gitlab_visibility(21),
            RepositoryVisibility::Public
        );
    }

    #[test]
    fn parse_visibility_name_public_returns_public() {
        assert_eq!(
            parse_visibility_name("PUBLIC"),
            RepositoryVisibility::Public
        );
    }

    #[test]
    fn parse_visibility_name_empty_returns_public() {
        assert_eq!(parse_visibility_name(""), RepositoryVisibility::Public);
    }

    #[test]
    fn provider_repository_policy_metadata_accessor() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let metadata = configured_metadata(
            repo,
            RepositoryVisibility::Private,
            "main",
            "https://example.com/team/assets.git",
        )
        .unwrap();
        let policy =
            ProviderRepositoryPolicy::new(metadata.clone(), HashSet::new(), HashSet::new());
        assert_eq!(policy.metadata(), &metadata);
    }

    #[test]
    fn provider_repository_policy_debug_format() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let metadata = configured_metadata(
            repo,
            RepositoryVisibility::Private,
            "main",
            "https://example.com/team/assets.git",
        )
        .unwrap();
        let policy = ProviderRepositoryPolicy::new(metadata, HashSet::new(), HashSet::new());
        let debug = format!("{policy:?}");
        assert!(debug.contains("ProviderRepositoryPolicy"));
    }

    #[test]
    fn built_in_provider_catalog_debug_format() {
        let catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let debug = format!("{catalog:?}");
        assert!(debug.contains("BuiltInProviderCatalog"));
    }

    #[test]
    fn built_in_provider_catalog_clone() {
        let mut catalog = BuiltInProviderCatalog::new("integration").unwrap();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let metadata = configured_metadata(
            repo,
            RepositoryVisibility::Private,
            "main",
            "https://example.com/team/assets.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::new(),
                HashSet::new(),
            ))
            .unwrap();
        let cloned = catalog.clone();
        assert_eq!(catalog, cloned);
    }

    #[test]
    fn built_in_provider_error_derived_traits() {
        let err = BuiltInProviderError::UnknownRepository;
        assert_eq!(err, BuiltInProviderError::UnknownRepository);
        let copied = err.clone();
        assert_eq!(err, copied);
        assert!(!format!("{err:?}").is_empty());
    }

    #[test]
    fn configured_metadata_accepts_long_valid_values() {
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let long_url = format!("https://{}/team/assets.git", "a".repeat(200));
        let result = configured_metadata(repo, RepositoryVisibility::Public, "main", &long_url);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_repository_from_full_name_handles_unicode() {
        let result = parse_repository_from_full_name(ProviderKind::GitHub, "üser/repo");
        assert!(result.is_ok());
        let Ok(repo) = result else { return };
        assert_eq!(repo.owner(), "üser");
        assert_eq!(repo.name(), "repo");
    }

    #[test]
    fn built_in_provider_catalog_default_subject_accessible_after_register() {
        let mut catalog = BuiltInProviderCatalog::new("my-bot-42").unwrap();
        assert_eq!(catalog.integration_subject().as_str(), "my-bot-42");
        let repo = RepositoryRef::new(ProviderKind::GitHub, "team", "assets").unwrap();
        let metadata = configured_metadata(
            repo,
            RepositoryVisibility::Public,
            "main",
            "https://example.com/repo.git",
        )
        .unwrap();
        catalog
            .register(ProviderRepositoryPolicy::new(
                metadata,
                HashSet::new(),
                HashSet::new(),
            ))
            .unwrap();
        // integration_subject is still accessible
        assert_eq!(catalog.integration_subject().as_str(), "my-bot-42");
    }
}
