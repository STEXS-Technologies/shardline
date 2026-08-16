//! Regression coverage for the `shardline admin token` CLI-mint path vs the
//! server's Local auth provider.
//!
//! A deployed server configured with `SHARDLINE_TOKEN_SIGNING_KEY` must accept
//! tokens minted by `shardline admin token` using the SAME environment key.
//! This file reproduces the exact CLI mint (`mint_admin_token_from_sources`
//! reading the env var) and verifies it through the provider the server uses
//! (`shardline_server_core::auth::LocalHmacProvider`, which backs
//! `ServerAuth`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::arithmetic_side_effects,
    clippy::option_if_let_else,
    clippy::unreachable,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use
)]

use shardline::mint_admin_token_from_sources;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope};
use shardline_server_core::auth::{AuthProvider, LocalHmacProvider};

/// The exact signing key from the reported deployment
/// (`SHARDLINE_TOKEN_SIGNING_KEY=0123456789abcdef0123456789abcdef`).
const DEPLOYED_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const KEY_ENV_VAR: &str = "SHARDLINE_TOKEN_SIGNING_KEY";

/// The claims a deployed operator mints with:
/// `shardline admin token --issuer shardline --subject s3test --scope write
///  --provider generic --owner ac --repo assets --ttl-seconds 3600`.
fn cli_claims() -> shardline_protocol::TokenClaims {
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "ac", "assets", None).unwrap();
    let expires_at_unix_seconds = shardline_protocol::unix_now_seconds_lossy()
        .checked_add(3600)
        .unwrap();
    shardline_protocol::TokenClaims::new(
        "shardline",
        "s3test",
        TokenScope::Write,
        repository,
        expires_at_unix_seconds,
    )
    .unwrap()
}

#[test]
fn cli_minted_token_from_env_key_verifies_against_local_provider() {
    // Safety: this test is single-threaded with respect to the env var and
    // restores the prior value; the CLI itself sets no global state.
    let previous = std::env::var(KEY_ENV_VAR).ok();
    // SAFETY: this test is the only writer of the env var in this process and
    // restores the prior value on exit; the CLI itself sets no global state.
    unsafe {
        std::env::set_var(KEY_ENV_VAR, "0123456789abcdef0123456789abcdef");
    }

    // The exact CLI code path: read the env var, sign with TokenSigner.
    let token = mint_admin_token_from_sources(
        None,
        Some(KEY_ENV_VAR),
        "shardline",
        "s3test",
        TokenScope::Write,
        RepositoryScope::new(RepositoryProvider::Generic, "ac", "assets", None).unwrap(),
        3600,
    )
    .expect("CLI mint must succeed");

    // The exact server verify path: the Local provider backing ServerAuth.
    let provider = LocalHmacProvider::new(DEPLOYED_KEY).expect("provider from env key");
    let verified = provider.verify_token(&token);
    match verified {
        Ok(claims) => {
            assert_eq!(claims.issuer(), "shardline");
            assert_eq!(claims.subject(), "s3test");
            assert_eq!(claims.scope(), TokenScope::Write);
            assert_eq!(claims.repository().owner(), "ac");
            assert_eq!(claims.repository().name(), "assets");
        }
        Err(error) => panic!("CLI-minted token failed server verification: {error:?}"),
    }

    // Restore the prior env value.
    match previous {
        // SAFETY: restoring the value this test changed; no other thread in
        // this test process reads the variable concurrently.
        Some(value) => unsafe { std::env::set_var(KEY_ENV_VAR, value) },
        // SAFETY: see above.
        None => unsafe { std::env::remove_var(KEY_ENV_VAR) },
    }
}

#[test]
fn cli_claims_without_env_key_source_requires_explicit_source() {
    // `mint_admin_token_from_sources` with neither a file nor an env source is
    // an error (the CLI enforces `--key-file` xor `--key-env`).
    let result = mint_admin_token_from_sources(
        None,
        None,
        "shardline",
        "s3test",
        TokenScope::Write,
        RepositoryScope::new(RepositoryProvider::Generic, "ac", "assets", None).unwrap(),
        3600,
    );
    assert!(matches!(
        result,
        Err(shardline::AdminTokenError::MissingSigningKeySource)
    ));
}

#[test]
fn cli_key_file_with_trailing_newline_verifies_against_server_key() {
    // The standard `echo $KEY > file` artifact: a trailing newline in the key
    // file. The CLI's `--key-file` path strips one trailing line terminator
    // (`shardline::admin::read_signing_key_bytes`), so the effective key is the
    // 32-byte value the server is configured with; without the strip the
    // minted token is rejected with a signature mismatch.
    let tmp = tempfile::tempdir().unwrap();
    let key_path = tmp.path().join("signing.key");
    std::fs::write(&key_path, b"0123456789abcdef0123456789abcdef\n").unwrap();

    let token = mint_admin_token_from_sources(
        Some(&key_path),
        None,
        "shardline",
        "s3test",
        TokenScope::Write,
        RepositoryScope::new(RepositoryProvider::Generic, "ac", "assets", None).unwrap(),
        3600,
    )
    .expect("CLI file-key mint must succeed");

    // The server resolves the SAME key file (via SHARDLINE_TOKEN_SIGNING_KEY_FILE)
    // and strips the trailing newline → 32-byte key.
    let provider = LocalHmacProvider::new(b"0123456789abcdef0123456789abcdef").unwrap();
    let verified = provider.verify_token(&token);
    assert!(
        verified.is_ok(),
        "CLI --key-file minted with a trailing-newline key file must verify against \
         the server's stripped key (got {verified:?})"
    );

    // Same for a CRLF-terminated key file (e.g. edited on Windows).
    let crlf_path = tmp.path().join("signing-crlf.key");
    std::fs::write(&crlf_path, b"0123456789abcdef0123456789abcdef\r\n").unwrap();
    let token = mint_admin_token_from_sources(
        Some(&crlf_path),
        None,
        "shardline",
        "s3test",
        TokenScope::Write,
        RepositoryScope::new(RepositoryProvider::Generic, "ac", "assets", None).unwrap(),
        3600,
    )
    .expect("CLI CRLF file-key mint must succeed");
    assert!(
        provider.verify_token(&token).is_ok(),
        "CRLF-terminated key file must verify against the stripped server key"
    );
}

#[test]
fn cli_claims_verify_with_the_same_signature_check_as_the_server() {
    // Direct signer-level check: mint the CLI claims with the deployed key and
    // verify with the same key — this is the signature/expiry core of the
    // server's authorize path.
    let signer = shardline_protocol::TokenSigner::new(DEPLOYED_KEY).unwrap();
    let token = signer.sign(&cli_claims()).unwrap();
    let verified = signer.verify_now(&token).expect("must verify");
    assert_eq!(verified.subject(), "s3test");
}
