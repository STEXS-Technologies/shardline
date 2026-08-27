#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::Error as IoError,
    num::NonZeroU64,
    path::Path,
    str::FromStr,
};

mod config_io;

use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::Error as SerdeJsonError;
use shardline_protocol::{
    RepositoryProvider, RepositoryProviderParseError, SecretBytes, SecretString, TokenScope,
};
use shardline_vcs::{
    AuthorizationRequest, BuiltInProviderCatalog, BuiltInProviderError, CodebergAdapter,
    GenericAdapter, GitHubAdapter, GitLabAdapter, GiteaAdapter, GrantedRepositoryAccess,
    ProviderAdapter, ProviderBoundaryError, ProviderKind, ProviderRepositoryPolicy,
    ProviderSubject, ProviderTokenIssuanceError, ProviderTokenIssuer, RepositoryAccess,
    RepositoryRef, RepositoryVisibility, RepositoryWebhookEvent, RevisionRef, VcsReferenceError,
    WebhookRequest, configured_metadata,
};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::model::{ProviderTokenIssueRequest, ProviderTokenIssueResponse};
#[cfg(test)]
use config_io::set_before_provider_config_read_hook;
use config_io::{parse_provider_config_document, read_provider_config_bytes};
use shardline_server_core::at_rest::{AtRestCipher, is_ciphertext};

const PROVIDER_API_KEY_HEADER: &str = "x-shardline-provider-key";
const GITHUB_EVENT_HEADER: &str = "x-github-event";
const GITHUB_DELIVERY_HEADER: &str = "x-github-delivery";
const GITHUB_SIGNATURE_HEADER: &str = "x-hub-signature-256";
const GITEA_EVENT_HEADER: &str = "x-gitea-event";
const GITEA_DELIVERY_HEADER: &str = "x-gitea-delivery";
const GITEA_SIGNATURE_HEADER: &str = "x-gitea-signature";
const CODEBERG_EVENT_HEADER: &str = "x-codeberg-event";
const CODEBERG_DELIVERY_HEADER: &str = "x-codeberg-delivery";
const CODEBERG_SIGNATURE_HEADER: &str = "x-codeberg-signature";
const GITLAB_EVENT_HEADER: &str = "x-gitlab-event";
const GITLAB_DELIVERY_HEADER: &str = "x-gitlab-webhook-uuid";
const GITLAB_SIGNATURE_HEADER: &str = "x-gitlab-token";
const GENERIC_EVENT_HEADER: &str = "x-shardline-event";
const GENERIC_DELIVERY_HEADER: &str = "x-shardline-delivery";
const GENERIC_SIGNATURE_HEADER: &str = "x-shardline-signature";
const MAX_PROVIDER_API_KEY_HEADER_BYTES: usize = 4096;
const MAX_PROVIDER_WEBHOOK_EVENT_HEADER_BYTES: usize = 512;
const MAX_PROVIDER_WEBHOOK_DELIVERY_HEADER_BYTES: usize = 512;
const MAX_PROVIDER_WEBHOOK_AUTH_HEADER_BYTES: usize = 4096;
const MAX_PROVIDER_CONFIG_BYTES: u64 = 67_108_864;

#[cfg(test)]
type ProviderConfigReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct ProviderConfigReadHookRegistration {
    path: PathBuf,
    hook: ProviderConfigReadHook,
}

#[cfg(test)]
type ProviderConfigReadHookSlot = Option<ProviderConfigReadHookRegistration>;

#[cfg(test)]
static BEFORE_PROVIDER_CONFIG_READ_HOOK: LazyLock<Mutex<ProviderConfigReadHookSlot>> =
    LazyLock::new(|| Mutex::new(None));

/// Provider token issuance runtime.
#[derive(Clone)]
pub struct ProviderTokenService {
    api_key: SecretBytes,
    issuer: ProviderTokenIssuer,
    registry: ProviderRegistry,
}

impl fmt::Debug for ProviderTokenService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderTokenService")
            .field("api_key", &"***")
            .field("issuer", &self.issuer)
            .field("registry", &self.registry)
            .finish()
    }
}

impl ProviderTokenService {
    /// Builds the provider token issuance runtime from a JSON catalog file.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderServiceError`] when the configuration file cannot be read,
    /// parsed, or converted into provider adapters.
    pub fn from_file(
        config_path: &Path,
        api_key: Vec<u8>,
        issuer_identity: &str,
        ttl_seconds: NonZeroU64,
        signing_key: &[u8],
        config_secret_cipher: Option<&AtRestCipher>,
    ) -> Result<Self, ProviderServiceError> {
        if api_key.is_empty() {
            return Err(ProviderServiceError::EmptyApiKey);
        }
        if api_key.len() < 16 {
            return Err(ProviderServiceError::ApiKeyTooShort);
        }
        if api_key.len() > MAX_PROVIDER_API_KEY_HEADER_BYTES {
            return Err(ProviderServiceError::ApiKeyTooLarge);
        }

        let mut bytes = read_provider_config_bytes(config_path)?;
        let document = parse_provider_config_document(&mut bytes)?;
        let issuer = ProviderTokenIssuer::new(issuer_identity, signing_key, ttl_seconds)?;
        let registry = ProviderRegistry::from_document(document, config_secret_cipher)?;

        Ok(Self {
            api_key: SecretBytes::new(api_key),
            issuer,
            registry,
        })
    }

    /// Issues a repository-scoped CAS token after validating the provider bootstrap
    /// key and evaluating provider authorization.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderServiceError`] when the bootstrap key is invalid, the
    /// provider is unknown, provider authorization fails, or the token cannot be
    /// signed.
    pub fn issue_token(
        &self,
        headers: &HeaderMap,
        provider_name: &str,
        request: &ProviderTokenIssueRequest,
    ) -> Result<ProviderTokenIssueResponse, ProviderServiceError> {
        self.authorize_provider_key(headers)?;
        let provider = self.registry.provider(provider_name)?;
        let access = repository_access(request.scope);
        let repository = RepositoryRef::new(provider.kind(), &request.owner, &request.repo)?;
        let revision = if let Some(revision) = request.revision.as_deref() {
            RevisionRef::new(revision)?
        } else {
            provider.default_revision(&repository)?
        };
        let subject = ProviderSubject::new(&request.subject)?;
        let authorization = AuthorizationRequest::new(subject, repository, revision, access);
        let grant = provider.authorize(&authorization)?;
        let Some(grant) = grant else {
            return Err(ProviderServiceError::Denied);
        };
        let issued = self.issuer.issue(&grant)?;

        Ok(ProviderTokenIssueResponse {
            token: SecretString::from_secret(issued.token()),
            issuer: issued.claims().issuer().to_owned(),
            subject: issued.claims().subject().to_owned(),
            provider: issued.claims().repository().provider(),
            owner: issued.claims().repository().owner().to_owned(),
            repo: issued.claims().repository().name().to_owned(),
            revision: issued
                .claims()
                .repository()
                .revision()
                .map(ToOwned::to_owned),
            scope: issued.claims().scope(),
            expires_at_unix_seconds: issued.claims().expires_at_unix_seconds(),
        })
    }

    /// Validates the provider bootstrap key before endpoint-specific body parsing.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderServiceError`] when the bootstrap key is missing or invalid.
    pub fn authorize_bootstrap_key(&self, headers: &HeaderMap) -> Result<(), ProviderServiceError> {
        self.authorize_provider_key(headers)
    }

    /// Parses a provider webhook into a normalized repository event.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderServiceError`] when the provider is unknown or the webhook
    /// headers, authentication, or payload are invalid.
    pub fn parse_webhook(
        &self,
        headers: &HeaderMap,
        provider_name: &str,
        body: &[u8],
    ) -> Result<Option<RepositoryWebhookEvent>, ProviderServiceError> {
        let provider = self.registry.provider(provider_name)?;
        let request = webhook_request(provider.kind(), headers, body)?;
        provider.parse_webhook(request)
    }

    fn authorize_provider_key(&self, headers: &HeaderMap) -> Result<(), ProviderServiceError> {
        let actual = headers
            .get(PROVIDER_API_KEY_HEADER)
            .ok_or(ProviderServiceError::MissingApiKey)?
            .as_bytes();
        if actual.len() > MAX_PROVIDER_API_KEY_HEADER_BYTES {
            return Err(ProviderServiceError::InvalidApiKey);
        }
        if actual.len() != self.api_key.len() {
            return Err(ProviderServiceError::InvalidApiKey);
        }
        if self.api_key.expose_secret().ct_eq(actual).into() {
            return Ok(());
        }

        Err(ProviderServiceError::InvalidApiKey)
    }
}

#[derive(Debug, Clone)]
struct ProviderRegistry {
    providers: HashMap<String, BuiltInProvider>,
}

impl ProviderRegistry {
    fn from_document(
        document: ProviderConfigDocument,
        config_secret_cipher: Option<&AtRestCipher>,
    ) -> Result<Self, ProviderServiceError> {
        let mut providers = HashMap::new();
        for provider in document.providers {
            let key = provider.kind.clone();
            let built = BuiltInProvider::from_config(provider, config_secret_cipher)?;
            if providers.insert(key, built).is_some() {
                return Err(ProviderServiceError::DuplicateProvider);
            }
        }

        Ok(Self { providers })
    }

    fn provider(&self, provider_name: &str) -> Result<&BuiltInProvider, ProviderServiceError> {
        self.providers
            .get(provider_name)
            .ok_or(ProviderServiceError::UnknownProvider)
    }
}

#[derive(Debug, Clone)]
enum BuiltInProvider {
    GitHub(GitHubAdapter),
    Gitea(GiteaAdapter),
    GitLab(GitLabAdapter),
    Codeberg(CodebergAdapter),
    Generic(GenericAdapter),
}

impl BuiltInProvider {
    fn from_config(
        config: ProviderConfig,
        config_secret_cipher: Option<&AtRestCipher>,
    ) -> Result<Self, ProviderServiceError> {
        let identity_kind = config.kind.clone();
        let ProviderConfig {
            kind,
            integration_subject,
            webhook_secret,
            repositories,
        } = config;
        let kind = parse_provider_kind(&kind)?;
        let webhook_secret = webhook_secret.ok_or(ProviderServiceError::MissingWebhookSecret)?;
        // Decrypt any at-rest-encrypted secret before handing it to the adapter.
        // Legacy plaintext (no `sse1:` prefix) passes through unchanged; a real
        // `sse1:` ciphertext is decrypted with a loud error on wrong key or
        // tampering. Encrypted data without a configured key is a loud error.
        let webhook_secret = match config_secret_cipher {
            Some(cipher) => {
                cipher
                    .decrypt(
                        &provider_config_secret_identity(&identity_kind, "webhook_secret"),
                        webhook_secret.expose_secret(),
                    )?
                    .secret
            }
            None => {
                if is_ciphertext(webhook_secret.expose_secret()) {
                    return Err(ProviderServiceError::EncryptedSecretWithoutKey);
                }
                webhook_secret
            }
        };
        if webhook_secret.expose_secret().trim().is_empty() {
            return Err(ProviderServiceError::EmptyWebhookSecret);
        }
        let mut catalog = BuiltInProviderCatalog::new(&integration_subject)?;
        for repository in repositories {
            let repository_ref = RepositoryRef::new(kind, &repository.owner, &repository.name)?;
            let metadata = configured_metadata(
                repository_ref,
                visibility(&repository.visibility)?,
                &repository.default_revision,
                &repository.clone_url,
            )?;
            let read_subjects = repository
                .read_subjects
                .iter()
                .map(|subject| ProviderSubject::new(subject))
                .collect::<Result<HashSet<_>, _>>()?;
            let write_subjects = repository
                .write_subjects
                .iter()
                .map(|subject| ProviderSubject::new(subject))
                .collect::<Result<HashSet<_>, _>>()?;
            catalog.register(ProviderRepositoryPolicy::new(
                metadata,
                read_subjects,
                write_subjects,
            ))?;
        }

        Ok(match kind {
            ProviderKind::GitHub => Self::GitHub(GitHubAdapter::new(catalog, Some(webhook_secret))),
            ProviderKind::Gitea => Self::Gitea(GiteaAdapter::new(catalog, Some(webhook_secret))),
            ProviderKind::GitLab => Self::GitLab(GitLabAdapter::new(catalog, Some(webhook_secret))),
            ProviderKind::Codeberg => {
                Self::Codeberg(CodebergAdapter::new(catalog, Some(webhook_secret)))
            }
            ProviderKind::Generic => {
                Self::Generic(GenericAdapter::new(catalog, Some(webhook_secret)))
            }
        })
    }

    const fn kind(&self) -> ProviderKind {
        match self {
            Self::GitHub(_) => ProviderKind::GitHub,
            Self::Gitea(_) => ProviderKind::Gitea,
            Self::GitLab(_) => ProviderKind::GitLab,
            Self::Codeberg(_) => ProviderKind::Codeberg,
            Self::Generic(_) => ProviderKind::Generic,
        }
    }

    fn default_revision(
        &self,
        repository: &RepositoryRef,
    ) -> Result<RevisionRef, ProviderServiceError> {
        match self {
            Self::GitHub(adapter) => Ok(adapter
                .repository_metadata(repository)?
                .default_revision()
                .clone()),
            Self::Gitea(adapter) => Ok(adapter
                .repository_metadata(repository)?
                .default_revision()
                .clone()),
            Self::GitLab(adapter) => Ok(adapter
                .repository_metadata(repository)?
                .default_revision()
                .clone()),
            Self::Codeberg(adapter) => Ok(adapter
                .repository_metadata(repository)?
                .default_revision()
                .clone()),
            Self::Generic(adapter) => Ok(adapter
                .repository_metadata(repository)?
                .default_revision()
                .clone()),
        }
    }

    fn authorize(
        &self,
        request: &AuthorizationRequest,
    ) -> Result<Option<GrantedRepositoryAccess>, ProviderServiceError> {
        match self {
            Self::GitHub(adapter) => Ok(GrantedRepositoryAccess::authorize(adapter, request)?),
            Self::Gitea(adapter) => Ok(GrantedRepositoryAccess::authorize(adapter, request)?),
            Self::GitLab(adapter) => Ok(GrantedRepositoryAccess::authorize(adapter, request)?),
            Self::Codeberg(adapter) => Ok(GrantedRepositoryAccess::authorize(adapter, request)?),
            Self::Generic(adapter) => Ok(GrantedRepositoryAccess::authorize(adapter, request)?),
        }
    }

    fn parse_webhook(
        &self,
        request: WebhookRequest<'_>,
    ) -> Result<Option<RepositoryWebhookEvent>, ProviderServiceError> {
        match self {
            Self::GitHub(adapter) => Ok(ProviderAdapter::parse_webhook(adapter, request)?),
            Self::Gitea(adapter) => Ok(ProviderAdapter::parse_webhook(adapter, request)?),
            Self::GitLab(adapter) => Ok(ProviderAdapter::parse_webhook(adapter, request)?),
            Self::Codeberg(adapter) => Ok(ProviderAdapter::parse_webhook(adapter, request)?),
            Self::Generic(adapter) => Ok(ProviderAdapter::parse_webhook(adapter, request)?),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ProviderConfigDocument {
    providers: Vec<ProviderConfig>,
}

#[derive(Debug, Deserialize)]
struct ProviderConfig {
    kind: String,
    integration_subject: String,
    webhook_secret: Option<SecretString>,
    repositories: Vec<RepositoryPolicyConfig>,
}

#[derive(Debug, Deserialize)]
struct RepositoryPolicyConfig {
    owner: String,
    name: String,
    visibility: String,
    default_revision: String,
    clone_url: String,
    read_subjects: Vec<String>,
    write_subjects: Vec<String>,
}

/// Provider runtime errors.
#[derive(Debug, Error)]
pub enum ProviderServiceError {
    /// The provider bootstrap key file was empty.
    #[error("provider bootstrap key must not be empty")]
    EmptyApiKey,
    /// The provider bootstrap key is too short for brute-force resistance.
    #[error("provider bootstrap key must be at least 16 bytes")]
    ApiKeyTooShort,
    /// The provider bootstrap key file exceeded the supported metadata size.
    #[error("provider bootstrap key exceeded the supported metadata size")]
    ApiKeyTooLarge,
    /// The provider bootstrap key header was missing.
    #[error("provider bootstrap key is missing")]
    MissingApiKey,
    /// The provider bootstrap key header was invalid.
    #[error("provider bootstrap key is invalid")]
    InvalidApiKey,
    /// The provider configuration file could not be read.
    #[error("provider configuration could not be read")]
    Io(#[from] IoError),
    /// The provider configuration file exceeded the bounded parser ceiling.
    #[error("provider configuration exceeded the bounded parser ceiling")]
    ConfigTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
    /// The provider configuration changed after bounded validation.
    #[error("provider configuration length did not match the validated length")]
    ConfigLengthMismatch,
    /// The provider configuration file was not valid JSON.
    #[error("provider configuration was not valid json")]
    Json(#[from] SerdeJsonError),
    /// The provider catalog registered the same provider twice.
    #[error("provider is configured more than once")]
    DuplicateProvider,
    /// A repository visibility value was not one of the supported values.
    #[error("provider repository visibility must be public, private, or internal")]
    InvalidRepositoryVisibility,
    /// The provider webhook secret was not configured.
    #[error("provider webhook secret must be configured")]
    MissingWebhookSecret,
    /// The provider webhook secret was configured as empty text.
    #[error("provider webhook secret must not be empty")]
    EmptyWebhookSecret,
    /// A provider-config secret is at-rest encrypted but no encryption key was
    /// configured for this process.
    #[error(
        "provider-config secret is at-rest encrypted but no SHARDLINE_CONFIG_SECRET_KEY is configured"
    )]
    EncryptedSecretWithoutKey,
    /// A provider-config at-rest secret could not be decrypted (wrong key or
    /// tampered data).
    #[error("provider-config secret could not be decrypted (wrong key or tampered data)")]
    SecretDecrypt(#[from] shardline_server_core::at_rest::CipherError),
    /// The requested provider was not configured.
    #[error("provider is not configured")]
    UnknownProvider,
    /// Provider authorization denied the request.
    #[error("provider denied requested repository access")]
    Denied,
    /// The provider catalog was invalid.
    #[error("provider catalog entry was invalid")]
    BuiltIn(#[from] BuiltInProviderError),
    /// The repository or revision references were invalid.
    #[error("provider request contained invalid repository or revision fields")]
    Reference(#[from] VcsReferenceError),
    /// The subject identifier was invalid.
    #[error("provider request contained an invalid subject")]
    Subject(#[from] ProviderBoundaryError),
    /// Token issuance failed.
    #[error("provider token could not be issued")]
    Token(#[from] ProviderTokenIssuanceError),
}

const fn repository_access(scope: TokenScope) -> RepositoryAccess {
    match scope {
        TokenScope::Read => RepositoryAccess::Read,
        TokenScope::Write => RepositoryAccess::Write,
    }
}

/// A provider-string parse failure is surfaced as `UnknownProvider` — there is
/// exactly one reason `RepositoryProvider::from_str` errors (the value is not a
/// known provider name), so the conversion is total and lossless.
impl From<RepositoryProviderParseError> for ProviderServiceError {
    fn from(_: RepositoryProviderParseError) -> Self {
        Self::UnknownProvider
    }
}

fn parse_provider_kind(value: &str) -> Result<ProviderKind, ProviderServiceError> {
    // Single source of truth: delegate to the canonical `RepositoryProvider`
    // parser and convert via the `From<RepositoryProvider> for ProviderKind`
    // impl, instead of re-matching the provider strings here.
    RepositoryProvider::from_str(value)
        .map(ProviderKind::from)
        .map_err(ProviderServiceError::from)
}

fn visibility(value: &str) -> Result<RepositoryVisibility, ProviderServiceError> {
    // Visibility directly affects authorization. Invalid or missing values must
    // fail closed during startup instead of silently making a repository public.
    RepositoryVisibility::from_str(value)
        .map_err(|_error| ProviderServiceError::InvalidRepositoryVisibility)
}

/// Canonical at-rest AAD identity for a provider-config secret field.
///
/// The identity binds a ciphertext to the exact provider kind and field it was
/// created for, so a secret cannot be transplanted between providers (or between
/// a provider's `api_key` and `webhook_secret`). The scheme is:
///
/// ```text
/// provider:<kind>:<field>
/// ```
///
/// where `<kind>` is the raw `kind` string from the provider config (e.g.
/// `github`) and `<field>` is one of `webhook_secret`. This same identity is
/// used both when encrypting on write and when decrypting on read, and must
/// never change for a field without migrating existing ciphertext.
fn provider_config_secret_identity(kind: &str, field: &str) -> String {
    format!("provider:{kind}:{field}")
}

#[cfg(test)]
fn run_before_provider_config_read_hook(path: &Path) {
    let hook = match BEFORE_PROVIDER_CONFIG_READ_HOOK.lock() {
        Ok(mut guard) => take_provider_config_read_hook_for_path(&mut guard, path),
        Err(poisoned) => take_provider_config_read_hook_for_path(&mut poisoned.into_inner(), path),
    };
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_provider_config_read_hook_for_path(
    slot: &mut ProviderConfigReadHookSlot,
    path: &Path,
) -> Option<ProviderConfigReadHook> {
    if !matches!(slot, Some(registration) if registration.path == path) {
        return None;
    }
    slot.take().map(|registration| registration.hook)
}

#[cfg(not(test))]
const fn run_before_provider_config_read_hook(_path: &Path) {}

fn webhook_request<'body>(
    provider_kind: ProviderKind,
    headers: &'body HeaderMap,
    body: &'body [u8],
) -> Result<WebhookRequest<'body>, ProviderServiceError> {
    let (event_header, delivery_header, signature_header) = match provider_kind {
        ProviderKind::GitHub => (
            GITHUB_EVENT_HEADER,
            GITHUB_DELIVERY_HEADER,
            GITHUB_SIGNATURE_HEADER,
        ),
        ProviderKind::Gitea => (
            GITEA_EVENT_HEADER,
            GITEA_DELIVERY_HEADER,
            GITEA_SIGNATURE_HEADER,
        ),
        ProviderKind::GitLab => (
            GITLAB_EVENT_HEADER,
            GITLAB_DELIVERY_HEADER,
            GITLAB_SIGNATURE_HEADER,
        ),
        ProviderKind::Codeberg => (
            CODEBERG_EVENT_HEADER,
            CODEBERG_DELIVERY_HEADER,
            CODEBERG_SIGNATURE_HEADER,
        ),
        ProviderKind::Generic => (
            GENERIC_EVENT_HEADER,
            GENERIC_DELIVERY_HEADER,
            GENERIC_SIGNATURE_HEADER,
        ),
    };

    let event_name = optional_bounded_header_value(
        headers,
        event_header,
        MAX_PROVIDER_WEBHOOK_EVENT_HEADER_BYTES,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookPayload),
    )?
    .unwrap_or_default();
    let delivery_id = optional_bounded_header_value(
        headers,
        delivery_header,
        MAX_PROVIDER_WEBHOOK_DELIVERY_HEADER_BYTES,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookPayload),
    )?
    .unwrap_or_default();
    let signature = optional_bounded_header_value(
        headers,
        signature_header,
        MAX_PROVIDER_WEBHOOK_AUTH_HEADER_BYTES,
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookAuthentication),
    )?;

    Ok(WebhookRequest::new(
        event_name,
        delivery_id,
        signature,
        body,
    ))
}

fn optional_bounded_header_value<'header>(
    headers: &'header HeaderMap,
    name: &str,
    maximum_bytes: usize,
    error: ProviderServiceError,
) -> Result<Option<&'header str>, ProviderServiceError> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    if value.as_bytes().len() > maximum_bytes {
        return Err(error);
    }

    value.to_str().map(Some).map_err(|_error| error)
}

#[cfg(test)]
mod tests;
