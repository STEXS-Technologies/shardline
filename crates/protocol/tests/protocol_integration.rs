//! Integration tests for `shardline-protocol`.
//!
//! These tests exercise the public API from an external consumer's perspective,
//! covering cross-module interactions and round-trips that in-module unit tests
//! do not necessarily exercise together.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

use shardline_protocol::{
    ByteRange, ChunkRange, HashParseError, HttpRangeParseError, RangeError, RepositoryProvider,
    RepositoryScope, SecretBytes, SecretString, ShardlineHash, TokenClaims, TokenClaimsError,
    TokenCodecError, TokenScope, TokenSigner, parse_bool, parse_http_byte_range,
    unix_now_seconds_lossy,
};

// ===========================================================================
// ShardlineHash
// ===========================================================================

#[test]
fn hash_from_bytes_hex_string_parse_hex_round_trip() {
    let cases: [[u8; 32]; 4] = [
        [0u8; 32],
        [0xffu8; 32],
        [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67,
            0x89, 0xab, 0xcd, 0xef,
        ],
        [42u8; 32],
    ];

    for raw in cases {
        let hash = ShardlineHash::from_bytes(raw);
        assert_eq!(
            hash.as_bytes(),
            &raw,
            "from_bytes should preserve raw bytes"
        );

        let hex = hash.hex_string();
        assert_eq!(hex.len(), 64, "hex_string should be 64 chars");
        assert!(
            hex.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        );

        let parsed = ShardlineHash::parse_hex(&hex);
        assert!(parsed.is_ok(), "parse_hex should succeed for valid hex");
        assert_eq!(
            parsed.unwrap(),
            hash,
            "round-trip should produce equal hash"
        );
    }
}

#[test]
fn hash_parse_hex_rejects_invalid_inputs() {
    // Empty string
    assert!(matches!(
        ShardlineHash::parse_hex(""),
        Err(HashParseError::InvalidLength)
    ));

    // Too short
    assert!(matches!(
        ShardlineHash::parse_hex("abc"),
        Err(HashParseError::InvalidLength)
    ));

    // 63 chars (correct length - 1)
    assert!(matches!(
        ShardlineHash::parse_hex(&"a".repeat(63)),
        Err(HashParseError::InvalidLength)
    ));

    // 65 chars (correct length + 1)
    assert!(matches!(
        ShardlineHash::parse_hex(&"a".repeat(65)),
        Err(HashParseError::InvalidLength)
    ));

    // Non-hex character at valid length
    assert!(matches!(
        ShardlineHash::parse_hex(&"g".repeat(64)),
        Err(HashParseError::InvalidCharacter(_))
    ));

    // Uppercase hex character
    assert!(matches!(
        ShardlineHash::parse_hex(&"A".repeat(64)),
        Err(HashParseError::InvalidCharacter(_))
    ));

    // Mixed case
    assert!(matches!(
        ShardlineHash::parse_hex(&format!("a{}", "A".repeat(63))),
        Err(HashParseError::InvalidCharacter(_))
    ));
}

#[test]
fn hash_hex_string_is_canonical() {
    let hash = ShardlineHash::from_bytes([0xab; 32]);
    let hex = hash.hex_string();
    // Every byte 0xab -> "ab" repeated 32 times
    assert_eq!(hex, "ab".repeat(32));
}

#[test]
fn hash_clone_copy_eq() {
    let a = ShardlineHash::from_bytes([7; 32]);
    let b = a; // Copy
    assert_eq!(a, b);

    let c = ShardlineHash::from_bytes([8; 32]);
    assert_ne!(a, c);
}

// ===========================================================================
// ByteRange
// ===========================================================================

#[test]
fn byte_range_new_valid() {
    let range = ByteRange::new(0, 0).unwrap();
    assert_eq!(range.start(), 0);
    assert_eq!(range.end_inclusive(), 0);
    assert_eq!(range.len(), Some(1));
    assert!(!range.is_empty());
}

#[test]
fn byte_range_rejects_inverted() {
    assert_eq!(ByteRange::new(100, 99), Err(RangeError::Inverted));
}

#[test]
fn byte_range_copy_semantics() {
    let range = ByteRange::new(5, 15).unwrap();
    let copied = range; // Copy (not move)
    assert_eq!(range, copied);
    assert_eq!(copied.start(), 5);
    assert_eq!(copied.end_inclusive(), 15);
}

#[test]
fn byte_range_length_accounting() {
    let range = ByteRange::new(10, 20).unwrap();
    assert_eq!(range.len(), Some(11)); // 20 - 10 + 1 = 11

    let single = ByteRange::new(7, 7).unwrap();
    assert_eq!(single.len(), Some(1));
}

#[test]
fn byte_range_u64_max_boundary() {
    let range = ByteRange::new(u64::MAX, u64::MAX).unwrap();
    assert_eq!(range.len(), Some(1));
}

// ===========================================================================
// ChunkRange
// ===========================================================================

#[test]
fn chunk_range_new_valid() {
    let range = ChunkRange::new(0, 1).unwrap();
    assert_eq!(range.start(), 0);
    assert_eq!(range.end_exclusive(), 1);
}

#[test]
fn chunk_range_rejects_empty() {
    assert_eq!(ChunkRange::new(5, 5), Err(RangeError::Empty));
}

#[test]
fn chunk_range_rejects_inverted() {
    assert_eq!(ChunkRange::new(10, 5), Err(RangeError::Inverted));
}

// ===========================================================================
// ByteRange overlap detection (manual, since there is no built-in overlap API)
// ===========================================================================

/// Returns `true` when two inclusive byte ranges share at least one byte.
const fn ranges_overlap(a: &ByteRange, b: &ByteRange) -> bool {
    a.start() <= b.end_inclusive() && b.start() <= a.end_inclusive()
}

#[test]
fn overlapping_ranges_detected() {
    let a = ByteRange::new(10, 20).unwrap();
    let b = ByteRange::new(15, 25).unwrap();
    assert!(
        ranges_overlap(&a, &b),
        "ranges [10,20] and [15,25] should overlap"
    );
    assert!(ranges_overlap(&b, &a), "overlap is symmetric");
}

#[test]
fn touching_ranges_overlap() {
    // [10, 20] and [20, 30] touch at byte 20.
    let a = ByteRange::new(10, 20).unwrap();
    let b = ByteRange::new(20, 30).unwrap();
    assert!(
        ranges_overlap(&a, &b),
        "touching ranges [10,20] and [20,30] overlap at byte 20"
    );
}

#[test]
fn disjoint_ranges_detected() {
    let a = ByteRange::new(10, 19).unwrap();
    let b = ByteRange::new(20, 30).unwrap();
    assert!(
        !ranges_overlap(&a, &b),
        "disjoint ranges [10,19] and [20,30] should not overlap"
    );
}

#[test]
fn nested_ranges_overlap() {
    let outer = ByteRange::new(0, 100).unwrap();
    let inner = ByteRange::new(25, 75).unwrap();
    assert!(
        ranges_overlap(&outer, &inner),
        "nested ranges should overlap"
    );
}

#[test]
fn single_byte_overlap_edges() {
    let a = ByteRange::new(5, 5).unwrap();
    let b = ByteRange::new(5, 5).unwrap();
    assert!(
        ranges_overlap(&a, &b),
        "identical single-byte ranges should overlap"
    );
}

#[test]
fn zero_length_not_possible() {
    // ByteRange::new(start, end) always has len >= 1 when end >= start.
    let range = ByteRange::new(42, 42).unwrap();
    assert_eq!(range.len(), Some(1));
    assert!(!range.is_empty());
}

// ===========================================================================
// parse_http_byte_range
// ===========================================================================

#[test]
fn parse_standard_byte_range() {
    let result = parse_http_byte_range("bytes=0-99", 100).unwrap();
    assert_eq!(result.start(), 0);
    assert_eq!(result.end_inclusive(), 99);
}

#[test]
fn parse_open_ended_byte_range() {
    // "bytes=N-" → clamped to resource_length - 1
    let result = parse_http_byte_range("bytes=50-", 100).unwrap();
    assert_eq!(result.start(), 50);
    assert_eq!(result.end_inclusive(), 99);
}

#[test]
fn parse_suffix_byte_range() {
    let result = parse_http_byte_range("bytes=-50", 200).unwrap();
    assert_eq!(result.start(), 150);
    assert_eq!(result.end_inclusive(), 199);
}

#[test]
fn parse_byte_range_rejects_unsatisfiable_start() {
    assert!(matches!(
        parse_http_byte_range("bytes=100-199", 100),
        Err(HttpRangeParseError::Unsatisfiable)
    ));
}

#[test]
fn parse_byte_range_rejects_missing_bytes_unit() {
    assert!(matches!(
        parse_http_byte_range("items=0-50", 100),
        Err(HttpRangeParseError::MissingBytesUnit)
    ));
}

#[test]
fn parse_byte_range_rejects_multi_range() {
    assert!(matches!(
        parse_http_byte_range("bytes=0-50,51-100", 200),
        Err(HttpRangeParseError::InvalidSyntax(_))
    ));
}

#[test]
fn parse_byte_range_clamps_to_resource_end() {
    let result = parse_http_byte_range("bytes=10-999", 50).unwrap();
    assert_eq!(result.start(), 10);
    assert_eq!(result.end_inclusive(), 49);
}

// ===========================================================================
// Token: RepositoryScope
// ===========================================================================

#[test]
fn repository_scope_accepts_all_providers() {
    for provider in [
        RepositoryProvider::GitHub,
        RepositoryProvider::Gitea,
        RepositoryProvider::GitLab,
        RepositoryProvider::Codeberg,
        RepositoryProvider::Generic,
    ] {
        let scope = RepositoryScope::new(provider, "owner", "name", Some("rev"));
        assert!(scope.is_ok(), "failed for provider {provider:?}");

        let scope = scope.unwrap();
        assert_eq!(scope.provider(), provider);
        assert_eq!(scope.owner(), "owner");
        assert_eq!(scope.name(), "name");
        assert_eq!(scope.revision(), Some("rev"));
    }
}

#[test]
fn repository_scope_accepts_missing_revision() {
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "name", None).unwrap();
    assert_eq!(scope.revision(), None);
}

#[test]
fn repository_scope_rejects_empty_fields() {
    assert_eq!(
        RepositoryScope::new(RepositoryProvider::GitHub, "", "name", None),
        Err(TokenClaimsError::EmptyRepositoryOwner)
    );
    assert_eq!(
        RepositoryScope::new(RepositoryProvider::GitHub, "owner", "", None),
        Err(TokenClaimsError::EmptyRepositoryName)
    );
    assert_eq!(
        RepositoryScope::new(RepositoryProvider::GitHub, "owner", "name", Some("")),
        Err(TokenClaimsError::EmptyRevision)
    );
}

#[test]
fn repository_provider_as_str_round_trip() {
    for provider in [
        RepositoryProvider::GitHub,
        RepositoryProvider::Gitea,
        RepositoryProvider::GitLab,
        RepositoryProvider::Codeberg,
        RepositoryProvider::Generic,
    ] {
        let text = provider.as_str();
        let parsed: RepositoryProvider = text.parse().unwrap();
        assert_eq!(parsed, provider);
    }
}

// ===========================================================================
// Token: TokenClaims + TokenSigner full cycle
// ===========================================================================

#[test]
fn token_sign_and_verify_full_round_trip() {
    let signer = TokenSigner::new(b"integration-test-key-32-bytes-long!!").unwrap();
    let repo = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "acme-corp",
        "widgets",
        Some("main"),
    )
    .unwrap();
    let claims = TokenClaims::new(
        "shardline-server",
        "alice",
        TokenScope::Write,
        repo,
        2_000_000_000,
    )
    .unwrap();

    // Sign
    let token = signer.sign(&claims).unwrap();
    assert!(!token.is_empty());
    assert!(token.contains('.'), "token should contain a dot separator");

    // Verify at a time before expiry
    let verified = signer.verify_at(&token, 1_700_000_000).unwrap();
    assert_eq!(verified, claims);
    assert_eq!(verified.issuer(), "shardline-server");
    assert_eq!(verified.subject(), "alice");
    assert_eq!(verified.scope(), TokenScope::Write);
    assert_eq!(verified.expires_at_unix_seconds(), 2_000_000_000);
    assert_eq!(verified.repository().owner(), "acme-corp");
    assert_eq!(verified.repository().name(), "widgets");
    assert_eq!(verified.repository().revision(), Some("main"));
}

#[test]
fn token_verify_rejects_tampered_signature() {
    let signer = TokenSigner::new(b"integration-test-key-32-bytes-long!!").unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", Some("main")).unwrap();
    let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 2_000_000_000).unwrap();
    let token = signer.sign(&claims).unwrap();

    // Corrupt the signature portion
    let dot_pos = token.find('.').unwrap();
    let payload_hex = &token[..dot_pos];
    let tampered = format!("{payload_hex}.{}", "00".repeat(32));

    let result = signer.verify_at(&tampered, 1_700_000_000);
    assert!(matches!(result, Err(TokenCodecError::InvalidSignature)));
}

#[test]
fn token_verify_rejects_expired_token() {
    let signer = TokenSigner::new(b"integration-test-key-32-bytes-long!!").unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", Some("main")).unwrap();
    let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 100).unwrap();
    let token = signer.sign(&claims).unwrap();

    // Verify at a time after expiry
    let result = signer.verify_at(&token, 101);
    assert!(matches!(result, Err(TokenCodecError::Expired)));
}

#[test]
fn token_verify_rejects_malformed_token() {
    let signer = TokenSigner::new(b"integration-test-key-32-bytes-long!!").unwrap();

    assert!(matches!(
        signer.verify_at("", 100),
        Err(TokenCodecError::InvalidFormat)
    ));
    assert!(matches!(
        signer.verify_at("no-dot-separator", 100),
        Err(TokenCodecError::InvalidFormat)
    ));
    assert!(matches!(
        signer.verify_at("..", 100),
        Err(TokenCodecError::InvalidFormat)
    ));
}

#[test]
fn token_signer_rejects_short_key() {
    let result = TokenSigner::new(b"short");
    assert!(matches!(
        result,
        Err(TokenCodecError::SigningKeyTooShort { actual_bytes: 5 })
    ));
}

#[test]
fn token_signer_rejects_empty_key() {
    let result = TokenSigner::new(&[]);
    assert!(matches!(result, Err(TokenCodecError::EmptySigningKey(_))));
}

#[test]
fn token_claims_rejects_empty_issuer_and_subject() {
    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();

    assert_eq!(
        TokenClaims::new("", "sub", TokenScope::Read, repo.clone(), 100),
        Err(TokenClaimsError::EmptyIssuer)
    );
    assert_eq!(
        TokenClaims::new("iss", "", TokenScope::Read, repo, 100),
        Err(TokenClaimsError::EmptySubject)
    );
}

#[test]
fn token_scope_permissions() {
    assert!(TokenScope::Read.allows_read());
    assert!(!TokenScope::Read.allows_write());
    assert!(TokenScope::Write.allows_read());
    assert!(TokenScope::Write.allows_write());
}

// ===========================================================================
// Token: Different signer keys produce different tokens
// ===========================================================================

#[test]
fn different_keys_produce_different_tokens() {
    let signer_a = TokenSigner::new(b"aaaaaaaa-key-aaaaaaaaaaaaaaaaa!!!!!").unwrap();
    let signer_b = TokenSigner::new(b"bbbbbbbb-key-bbbbbbbbbbbbbb!!!!!!!").unwrap();

    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", Some("main")).unwrap();
    let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 2_000_000_000).unwrap();

    let token_a = signer_a.sign(&claims).unwrap();
    let token_b = signer_b.sign(&claims).unwrap();

    assert_ne!(
        token_a, token_b,
        "different keys should produce different tokens"
    );

    // Token A should not verify with signer B
    assert!(matches!(
        signer_b.verify_at(&token_a, 1_700_000_000),
        Err(TokenCodecError::InvalidSignature)
    ));
}

// ===========================================================================
// unix_now_seconds_lossy
// ===========================================================================

#[test]
fn unix_now_seconds_is_reasonable() {
    let ts = unix_now_seconds_lossy();
    // As of July 2026, the Unix timestamp should be ~1,777,000,000.
    // We allow a generous lower bound to keep the test future-proof.
    assert!(
        ts >= 1_700_000_000,
        "timestamp {ts} seems too old (before 2023)"
    );
    assert!(
        ts <= 2_000_000_000,
        "timestamp {ts} seems too far in the future"
    );
}

// ===========================================================================
// parse_bool
// ===========================================================================

#[test]
fn parse_bool_true_variants() {
    assert_eq!(parse_bool("true"), Some(true));
    assert_eq!(parse_bool("1"), Some(true));
    assert_eq!(parse_bool("yes"), Some(true));
    assert_eq!(parse_bool("on"), Some(true));
}

#[test]
fn parse_bool_false_variants() {
    assert_eq!(parse_bool("false"), Some(false));
    assert_eq!(parse_bool("0"), Some(false));
    assert_eq!(parse_bool("no"), Some(false));
    assert_eq!(parse_bool("off"), Some(false));
}

#[test]
fn parse_bool_rejects_invalid_inputs() {
    assert_eq!(parse_bool(""), None);
    assert_eq!(parse_bool("True"), None);
    assert_eq!(parse_bool("FALSE"), None);
    assert_eq!(parse_bool("YES"), None);
    assert_eq!(parse_bool("maybe"), None);
    assert_eq!(parse_bool("  true"), None);
    assert_eq!(parse_bool("true "), None);
    assert_eq!(parse_bool("yes "), None);
}

// ===========================================================================
// SecretBytes
// ===========================================================================

#[test]
fn secret_bytes_construction_and_access() {
    let from_slice = SecretBytes::from_slice(b"my-secret");
    assert_eq!(from_slice.expose_secret(), b"my-secret");
    assert!(!from_slice.is_empty());
    assert_eq!(from_slice.len(), 9);

    let new_owned = SecretBytes::new(vec![1, 2, 3]);
    assert_eq!(new_owned.expose_secret(), &[1, 2, 3]);
}

#[test]
fn secret_bytes_constant_time_comparison() {
    let a = SecretBytes::from_slice(b"same-value");
    let b = SecretBytes::from_slice(b"same-value");
    let c = SecretBytes::from_slice(b"different");

    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn secret_bytes_debug_redacts_content() {
    let secret = SecretBytes::from_slice(b"sensitive-api-key");
    let debug = format!("{secret:?}");
    assert_eq!(debug, "***", "SecretBytes Debug should not leak content");
}

#[test]
fn secret_bytes_as_ref() {
    let secret = SecretBytes::from_slice(b"ref-test");
    let bytes: &[u8] = secret.as_ref();
    assert_eq!(bytes, b"ref-test");
}

#[test]
fn secret_bytes_empty() {
    let empty = SecretBytes::new(Vec::new());
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}

// ===========================================================================
// SecretString
// ===========================================================================

#[test]
fn secret_string_construction_and_access() {
    let from_secret = SecretString::from_secret("bootstrap-token");
    assert_eq!(from_secret.expose_secret(), "bootstrap-token");
    assert!(!from_secret.is_empty());

    let new_owned = SecretString::new("owned-secret".to_owned());
    assert_eq!(new_owned.expose_secret(), "owned-secret");
}

#[test]
fn secret_string_constant_time_comparison() {
    let a = SecretString::from_secret("match-value");
    let b = SecretString::from_secret("match-value");
    let c = SecretString::from_secret("other-value");

    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn secret_string_debug_redacts_content() {
    let secret = SecretString::from_secret("super-secret-token");
    let debug = format!("{secret:?}");
    assert_eq!(debug, "***", "SecretString Debug should not leak content");
}

#[test]
fn secret_string_as_ref() {
    let secret = SecretString::from_secret("ref-string");
    let s: &str = secret.as_ref();
    assert_eq!(s, "ref-string");
}

#[test]
fn secret_string_empty() {
    let empty = SecretString::new(String::new());
    assert!(empty.is_empty());
}

// ===========================================================================
// SecretBytes / SecretString clone
// ===========================================================================

#[test]
fn secret_bytes_clone_produces_equal_data() {
    let a = SecretBytes::from_slice(b"clone-me");
    let b = a.clone();
    assert_eq!(a.expose_secret(), b.expose_secret());
}

#[test]
fn secret_string_clone_produces_equal_data() {
    let a = SecretString::from_secret("clone-me-too");
    let b = a.clone();
    assert_eq!(a.expose_secret(), b.expose_secret());
}

// ===========================================================================
// RangeError Display
// ===========================================================================

#[test]
fn range_error_display_messages() {
    let inverted = RangeError::Inverted.to_string();
    assert!(inverted.contains("smaller"), "{inverted}");

    let empty = RangeError::Empty.to_string();
    assert!(empty.contains("at least one"), "{empty}");
}

// ===========================================================================
// HttpRangeParseError Display
// ===========================================================================

#[test]
fn http_range_parse_error_display_messages() {
    let missing = HttpRangeParseError::MissingBytesUnit.to_string();
    assert!(missing.contains("bytes="), "{missing}");

    let syntax = HttpRangeParseError::InvalidSyntax("bad".to_owned()).to_string();
    assert!(!syntax.is_empty());

    let num = HttpRangeParseError::InvalidNumber("NaN".to_owned()).to_string();
    assert!(num.contains("NaN"), "{num}");

    let unsat = HttpRangeParseError::Unsatisfiable.to_string();
    assert!(unsat.contains("satisfiable"), "{unsat}");
}
