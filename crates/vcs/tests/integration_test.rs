#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::NonZeroU64;

use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use shardline_vcs::{
    GrantedRepositoryAccess, ProviderKind, ProviderSubject, RepositoryAccess, RepositoryRef,
    RevisionRef,
};

const SIGNING_KEY: &[u8] = b"test-integration-signing-key-32bytes!!";

fn make_grant(
    provider: ProviderKind,
    owner: &str,
    name: &str,
    revision: &str,
    subject: &str,
    access: RepositoryAccess,
) -> GrantedRepositoryAccess {
    let repository = RepositoryRef::new(provider, owner, name).expect("valid repository ref");
    let rev = RevisionRef::new(revision).expect("valid revision ref");
    let sub = ProviderSubject::new(subject).expect("valid subject");
    GrantedRepositoryAccess::from_decision(
        &shardline_vcs::AuthorizationRequest::new(sub.clone(), repository, rev, access),
        shardline_vcs::AuthorizationDecision::Allow(sub),
    )
    .expect("allow decision should produce a grant")
}

#[test]
fn token_issuance_roundtrip_verifies_with_same_key() {
    let issuer = shardline_vcs::ProviderTokenIssuer::new(
        "test-issuer",
        SIGNING_KEY,
        NonZeroU64::new(300).expect("nonzero"),
    )
    .expect("valid issuer");
    let grant = make_grant(
        ProviderKind::GitHub,
        "acme",
        "assets",
        "refs/heads/main",
        "user-1",
        RepositoryAccess::Read,
    );

    let issued = issuer
        .issue_at(&grant, 1_000)
        .expect("issuance should succeed");

    assert_eq!(issued.claims().issuer(), "test-issuer");
    assert_eq!(issued.claims().subject(), "user-1");
    assert_eq!(issued.claims().expires_at_unix_seconds(), 1_300);

    let signer = TokenSigner::new(SIGNING_KEY).expect("valid signer");
    let verified = signer
        .verify_at(issued.token(), 1_000)
        .expect("verification should succeed");

    assert_eq!(verified.issuer(), "test-issuer");
    assert_eq!(verified.subject(), "user-1");
    assert_eq!(verified.scope(), TokenScope::Read);
    assert_eq!(verified.repository().owner(), "acme");
    assert_eq!(verified.repository().name(), "assets");
    assert_eq!(verified.repository().revision(), Some("refs/heads/main"));
}

#[test]
fn token_verification_rejects_wrong_key() {
    let issuer = shardline_vcs::ProviderTokenIssuer::new(
        "test-issuer",
        SIGNING_KEY,
        NonZeroU64::new(60).expect("nonzero"),
    )
    .expect("valid issuer");
    let grant = make_grant(
        ProviderKind::GitLab,
        "team",
        "repo",
        "refs/tags/v1.0",
        "user-2",
        RepositoryAccess::Write,
    );

    let issued = issuer
        .issue_at(&grant, 500)
        .expect("issuance should succeed");

    let wrong_key = b"wrong-signing-key-different-32bytes!!";
    let wrong_signer = TokenSigner::new(wrong_key).expect("valid wrong signer");
    let result = wrong_signer.verify_at(issued.token(), 500);

    assert!(result.is_err(), "verification with wrong key should fail");
}

#[test]
fn token_verification_rejects_expired_token() {
    let issuer = shardline_vcs::ProviderTokenIssuer::new(
        "test-issuer",
        SIGNING_KEY,
        NonZeroU64::new(10).expect("nonzero"),
    )
    .expect("valid issuer");
    let grant = make_grant(
        ProviderKind::Generic,
        "org",
        "project",
        "main",
        "user-3",
        RepositoryAccess::Read,
    );

    let issued = issuer
        .issue_at(&grant, 100)
        .expect("issuance should succeed");

    let signer = TokenSigner::new(SIGNING_KEY).expect("valid signer");
    let result = signer.verify_at(issued.token(), 200);

    assert!(result.is_err(), "expired token should be rejected");
}

#[test]
fn hmac_signature_is_deterministic() {
    let signer = TokenSigner::new(SIGNING_KEY).expect("valid signer");
    let repository = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main"))
        .expect("valid scope");
    let claims_a = TokenClaims::new(
        "issuer",
        "subject",
        TokenScope::Read,
        repository.clone(),
        42,
    )
    .expect("valid claims");
    let claims_b = TokenClaims::new("issuer", "subject", TokenScope::Read, repository, 42)
        .expect("valid claims");

    let token_a = signer.sign(&claims_a).expect("signing should succeed");
    let token_b = signer.sign(&claims_b).expect("signing should succeed");

    assert_eq!(token_a, token_b, "HMAC signatures should be deterministic");
}

#[test]
fn repository_scope_roundtrips_through_token_claims() {
    let signer = TokenSigner::new(SIGNING_KEY).expect("valid signer");
    let scope = RepositoryScope::new(
        RepositoryProvider::GitLab,
        "group",
        "project",
        Some("refs/heads/dev"),
    )
    .expect("valid scope");
    let claims =
        TokenClaims::new("issuer", "user", TokenScope::Write, scope, 999).expect("valid claims");

    let token = signer.sign(&claims).expect("signing should succeed");
    let verified = signer
        .verify_at(&token, 0)
        .expect("verification should succeed");

    assert_eq!(verified.repository().provider(), RepositoryProvider::GitLab);
    assert_eq!(verified.repository().owner(), "group");
    assert_eq!(verified.repository().name(), "project");
    assert_eq!(verified.repository().revision(), Some("refs/heads/dev"));
    assert_eq!(verified.scope(), TokenScope::Write);
}
