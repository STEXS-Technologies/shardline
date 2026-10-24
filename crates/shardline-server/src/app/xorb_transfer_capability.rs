//! Short-lived, range-scoped capabilities for reconstruction-provided Xorb URLs.
//!
//! These capabilities use a key derived from the configured token-signing key,
//! so they cannot be replayed as ordinary bearer tokens accepted by the auth
//! provider. The signed claims still carry the repository identity, while the
//! subject binds one file, Xorb, and exact serialized byte range.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use shardline_protocol::{TokenClaims, TokenScope, TokenSigner, unix_now_seconds_lossy};
use shardline_server_core::AuthorizedRepository;

use crate::{
    ServerError,
    config::ServerConfig,
    xet_adapter::{FileReconstructionResponse, FileReconstructionV2Response, validate_hash_path},
};

const CAPABILITY_ISSUER: &str = "shardline-xorb-range-v1";
const HUB_CAPABILITY_ISSUER: &str = "shardline-hub-xorb-range-v1";
const CAPABILITY_KEY_CONTEXT: &[u8] = b"shardline:xorb-range-read-capability:v1";
pub(super) const CAPABILITY_TTL_SECONDS: u64 = 900;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct XorbReadGrant {
    pub(super) hub_commit: bool,
    pub(super) file_id: String,
    pub(super) xorb_hash: String,
    pub(super) range_start: u64,
    pub(super) range_end: u64,
}

#[derive(Clone)]
pub(super) struct XorbReadCapabilitySigner {
    signer: TokenSigner,
}

impl std::fmt::Debug for XorbReadCapabilitySigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("XorbReadCapabilitySigner")
            .field("signer", &"<redacted>")
            .finish()
    }
}

impl XorbReadCapabilitySigner {
    /// Creates a capability signer using a key isolated from ordinary tokens.
    pub(super) fn new(signing_key: &[u8]) -> Result<Self, ServerError> {
        let mut derivation = HmacSha256::new_from_slice(signing_key)
            .map_err(|error| ServerError::SigningKeyError(error.to_string()))?;
        derivation.update(CAPABILITY_KEY_CONTEXT);
        let derived_key = derivation.finalize().into_bytes();
        let signer = TokenSigner::new(&derived_key)
            .map_err(|error| ServerError::SigningKeyError(error.to_string()))?;
        Ok(Self { signer })
    }

    /// Creates a signer only when authenticated reconstruction claims exist
    /// and this deployment has the shared signing secret needed by all nodes.
    pub(super) fn for_authorized_repository(
        config: &ServerConfig,
        repository: &AuthorizedRepository,
    ) -> Result<Option<Self>, ServerError> {
        if repository.claims().is_none() {
            return Ok(None);
        }
        config.token_signing_key().map(Self::new).transpose()
    }

    /// Signs every Xorb range advertised for one authorized file.
    pub(super) fn sign_file_response(
        &self,
        claims: &TokenClaims,
        file_id: &str,
        response: &mut FileReconstructionResponse,
    ) -> Result<(), ServerError> {
        for (xorb_hash, entries) in &mut response.fetch_info {
            for entry in entries {
                let capability = self.issue(
                    claims,
                    file_id,
                    xorb_hash,
                    entry.url_range.start,
                    entry.url_range.end,
                )?;
                append_capability(&mut entry.url, &capability);
            }
        }
        Ok(())
    }

    /// Signs the exact byte range in each V2 fetch descriptor.
    pub(super) fn sign_file_v2_response(
        &self,
        claims: &TokenClaims,
        file_id: &str,
        response: &mut FileReconstructionV2Response,
    ) -> Result<(), ServerError> {
        for (xorb_hash, entries) in &mut response.xorbs {
            for entry in entries {
                if entry.ranges.len() != 1 {
                    return Err(ServerError::NotFound);
                }
                let Some(range) = entry.ranges.first() else {
                    return Err(ServerError::NotFound);
                };
                let capability = self.issue(
                    claims,
                    file_id,
                    xorb_hash,
                    range.bytes.start,
                    range.bytes.end,
                )?;
                append_capability(&mut entry.url, &capability);
            }
        }
        Ok(())
    }

    /// Issues a capability for one range already authorized by reconstruction.
    pub(super) fn issue(
        &self,
        authorized_claims: &TokenClaims,
        file_id: &str,
        xorb_hash: &str,
        range_start: u64,
        range_end: u64,
    ) -> Result<String, ServerError> {
        validate_hash_path(file_id)?;
        validate_hash_path(xorb_hash)?;
        if range_start > range_end {
            return Err(ServerError::NotFound);
        }
        let subject = format!("{file_id}:{xorb_hash}:{range_start}:{range_end}");
        let expires_at = unix_now_seconds_lossy()
            .checked_add(CAPABILITY_TTL_SECONDS)
            .ok_or(ServerError::Overflow)?;
        let claims = TokenClaims::new(
            if authorized_claims.issuer() == shardline_protocol::HUB_XET_READ_TOKEN_ISSUER {
                HUB_CAPABILITY_ISSUER
            } else {
                CAPABILITY_ISSUER
            },
            &subject,
            TokenScope::Read,
            authorized_claims.repository().clone(),
            expires_at,
        )
        .map_err(|error| ServerError::SigningKeyError(error.to_string()))?;
        self.signer.sign(&claims).map_err(ServerError::InvalidToken)
    }

    /// Verifies signature, expiry, capability domain, scope, and bound objects.
    pub(super) fn verify(&self, token: &str) -> Result<(TokenClaims, XorbReadGrant), ServerError> {
        self.verify_at(token, unix_now_seconds_lossy())
    }

    /// Verifies the signature and payload without enforcing expiry. Callers
    /// must compare expiry and require current repository auth before accepting
    /// an expired capability.
    pub(super) fn verify_signed(
        &self,
        token: &str,
    ) -> Result<(TokenClaims, XorbReadGrant), ServerError> {
        self.verify_at(token, 0)
    }

    fn verify_at(
        &self,
        token: &str,
        now_unix_seconds: u64,
    ) -> Result<(TokenClaims, XorbReadGrant), ServerError> {
        let claims = self
            .signer
            .verify_at(token, now_unix_seconds)
            .map_err(ServerError::InvalidToken)?;
        if ![CAPABILITY_ISSUER, HUB_CAPABILITY_ISSUER].contains(&claims.issuer())
            || claims.scope() != TokenScope::Read
        {
            return Err(ServerError::NotFound);
        }
        let mut fields = claims.subject().split(':');
        let Some(file_id) = fields.next() else {
            return Err(ServerError::NotFound);
        };
        let Some(xorb_hash) = fields.next() else {
            return Err(ServerError::NotFound);
        };
        let Some(range_start) = fields.next().and_then(|value| value.parse().ok()) else {
            return Err(ServerError::NotFound);
        };
        let Some(range_end) = fields.next().and_then(|value| value.parse().ok()) else {
            return Err(ServerError::NotFound);
        };
        if fields.next().is_some()
            || range_start > range_end
            || validate_hash_path(file_id).is_err()
            || validate_hash_path(xorb_hash).is_err()
        {
            return Err(ServerError::NotFound);
        }
        let grant = XorbReadGrant {
            hub_commit: claims.issuer() == HUB_CAPABILITY_ISSUER,
            file_id: file_id.to_owned(),
            xorb_hash: xorb_hash.to_owned(),
            range_start,
            range_end,
        };
        Ok((claims, grant))
    }
}

fn append_capability(url: &mut String, capability: &str) {
    url.push(if url.contains('?') { '&' } else { '?' });
    url.push_str("xorb_read_capability=");
    url.push_str(capability);
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};

    use super::*;

    const KEY: &[u8] = b"xorb-capability-test-signing-key-32-bytes";
    const OTHER_KEY: &[u8] = b"different-test-signing-key-is-32-bytes";

    #[test]
    fn hub_commit_grant_retains_its_authorization_domain() {
        let ordinary = authorized_claims();
        let hub = TokenClaims::new(
            shardline_protocol::HUB_XET_READ_TOKEN_ISSUER,
            ordinary.subject(),
            TokenScope::Read,
            ordinary.repository().clone(),
            u64::MAX,
        )
        .unwrap();
        let signer = XorbReadCapabilitySigner::new(KEY).unwrap();
        for (claims, expected) in [(ordinary, false), (hub, true)] {
            let token = signer
                .issue(&claims, &"a".repeat(64), &"b".repeat(64), 10, 20)
                .unwrap();
            let (_, grant) = signer.verify(&token).unwrap();
            assert_eq!(grant.hub_commit, expected);
        }
    }
    fn authorized_claims() -> TokenClaims {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "models", Some("main"))
                .expect("test repository scope is valid");
        TokenClaims::new(
            "test-issuer",
            "alice",
            TokenScope::Read,
            repository,
            u64::MAX,
        )
        .expect("test claims are valid")
    }

    #[test]
    fn capability_binds_exact_file_xorb_repository_and_range() {
        let signer = XorbReadCapabilitySigner::new(KEY).expect("signer is valid");
        let file_id = "a".repeat(64);
        let xorb_hash = "b".repeat(64);
        let token = signer
            .issue(&authorized_claims(), &file_id, &xorb_hash, 10, 20)
            .expect("capability should issue");
        let (claims, grant) = signer.verify(&token).expect("capability should verify");
        assert_eq!(claims.repository().owner(), "team");
        assert_eq!(claims.repository().name(), "models");
        assert_eq!(claims.scope(), TokenScope::Read);
        assert_eq!(grant.file_id, file_id);
        assert_eq!(grant.xorb_hash, xorb_hash);
        assert_eq!((grant.range_start, grant.range_end), (10, 20));
    }

    #[test]
    fn capability_is_not_an_ordinary_bearer_token() {
        let capability_signer = XorbReadCapabilitySigner::new(KEY).expect("signer is valid");
        let file_id = "a".repeat(64);
        let xorb_hash = "b".repeat(64);
        let token = capability_signer
            .issue(&authorized_claims(), &file_id, &xorb_hash, 10, 20)
            .expect("capability should issue");
        let ordinary_signer = TokenSigner::new(KEY).expect("ordinary signer is valid");
        assert!(ordinary_signer.verify_now(&token).is_err());
    }

    #[test]
    fn capability_rejects_tampering_and_other_server_keys() {
        let signer = XorbReadCapabilitySigner::new(KEY).expect("signer is valid");
        let file_id = "a".repeat(64);
        let xorb_hash = "b".repeat(64);
        let token = signer
            .issue(&authorized_claims(), &file_id, &xorb_hash, 10, 20)
            .expect("capability should issue");
        let mut changed = token.clone().into_bytes();
        if let Some(last) = changed.last_mut() {
            *last = if *last == b'0' { b'1' } else { b'0' };
        }
        let changed = String::from_utf8(changed).expect("token remains utf-8");
        assert!(signer.verify(&changed).is_err());
        let other = XorbReadCapabilitySigner::new(OTHER_KEY).expect("signer is valid");
        assert!(other.verify(&token).is_err());
    }

    #[test]
    fn capability_expires_after_its_short_ttl() {
        let signer = XorbReadCapabilitySigner::new(KEY).expect("signer is valid");
        let file_id = "a".repeat(64);
        let xorb_hash = "b".repeat(64);
        let issued_at = unix_now_seconds_lossy();
        let token = signer
            .issue(&authorized_claims(), &file_id, &xorb_hash, 10, 20)
            .expect("capability should issue");
        let expired_at = issued_at + CAPABILITY_TTL_SECONDS + 2;
        assert!(signer.verify_at(&token, expired_at).is_err());
    }
}
