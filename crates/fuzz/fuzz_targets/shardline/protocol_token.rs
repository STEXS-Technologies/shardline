#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenCodecError, TokenScope, TokenSigner,
};

type ProtocolTokenInput = (
    Vec<u8>,
    String,
    u64,
    String,
    String,
    String,
    String,
    Option<String>,
    u64,
    bool,
    u8,
);

fuzz_target!(|data: ProtocolTokenInput| {
    let (
        signing_key,
        token,
        current_unix_seconds,
        issuer,
        subject,
        owner,
        name,
        revision,
        expires_at_unix_seconds,
        write_scope,
        provider_tag,
    ) = data;

    let signer = TokenSigner::new(&signing_key);
    let repeated_signer = TokenSigner::new(&signing_key);
    assert_eq!(signer.is_ok(), repeated_signer.is_ok());
    match (&signer, &repeated_signer) {
        (Err(left), Err(right)) => {
            assert_eq!(left.to_string(), right.to_string());
            return;
        }
        (Ok(_), Ok(_)) => {}
        _ => return,
    }

    let Ok(signer) = signer else {
        return;
    };

    let first_verify = signer.verify_at(&token, current_unix_seconds);
    let second_verify = signer.verify_at(&token, current_unix_seconds);
    assert_eq!(first_verify.is_ok(), second_verify.is_ok());
    match (&first_verify, &second_verify) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
        _ => return,
    }

    let provider = match provider_tag % 4 {
        0 => RepositoryProvider::GitHub,
        1 => RepositoryProvider::Gitea,
        2 => RepositoryProvider::GitLab,
        _ => RepositoryProvider::Generic,
    };
    let scope = if write_scope {
        TokenScope::Write
    } else {
        TokenScope::Read
    };
    let repository = RepositoryScope::new(provider, &owner, &name, revision.as_deref());
    let Ok(repository) = repository else {
        return;
    };
    let claims = TokenClaims::new(
        &issuer,
        &subject,
        scope,
        repository.clone(),
        expires_at_unix_seconds,
    );
    let Ok(claims) = claims else {
        return;
    };

    let signed = signer.sign(&claims);
    assert!(signed.is_ok());
    let Ok(signed) = signed else {
        return;
    };
    let separator_count = signed.matches('.').count();
    assert_eq!(separator_count, 1);

    let verified = signer.verify_at(&signed, current_unix_seconds.min(expires_at_unix_seconds));
    assert!(verified.is_ok());
    let Ok(verified) = verified else {
        return;
    };
    assert_eq!(verified, claims);
    assert_eq!(verified.repository(), &repository);
    assert_eq!(verified.scope(), scope);
    assert_eq!(verified.scope().allows_write(), write_scope);
    assert!(verified.scope().allows_read());

    let after_expiry = expires_at_unix_seconds.saturating_add(1);
    if after_expiry > expires_at_unix_seconds {
        let expired = signer.verify_at(&signed, after_expiry);
        assert!(matches!(expired, Err(TokenCodecError::Expired)));
    }

    let wrong_signer = TokenSigner::new(b"wrong-signing-key");
    assert!(wrong_signer.is_ok());
    let Ok(wrong_signer) = wrong_signer else {
        return;
    };
    let wrong_verify = wrong_signer.verify_at(&signed, current_unix_seconds);
    assert!(matches!(
        wrong_verify,
        Err(TokenCodecError::InvalidSignature)
    ));
});
