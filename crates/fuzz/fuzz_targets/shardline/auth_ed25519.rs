#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_auth::Ed25519AuthProvider;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);

    // Split the input into a 32-byte public key, a signature, and a payload.
    let pub_bytes = data.get(..32).unwrap_or(&[]);
    if pub_bytes.len() != 32 {
        // Without a full public key there is nothing to exercise.
        return;
    }
    let rest = data.get(32..).unwrap_or(&[]);
    let sig_bytes = rest.get(..64).unwrap_or(rest);
    let payload = rest.get(64..).unwrap_or(&[]);

    let provider = match Ed25519AuthProvider::with_public_key(pub_bytes) {
        Ok(provider) => provider,
        // Invalid/weak public key: a legitimate non-verification path, not a bug.
        Err(_) => return,
    };

    // Build a token from hex-encoded payload and signature bytes.
    let token = format!("{}.{}", hex::encode(payload), hex::encode(sig_bytes));

    // Verification must be deterministic and never panic. Both true (a forged
    // token can still verify if the derived signature matches) and false
    // outcomes are valid.
    let first = provider.verify_at(&token, 1_000_000);
    let second = provider.verify_at(&token, 1_000_000);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
});
