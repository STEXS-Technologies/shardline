#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::dbg_macro,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::format_push_string
)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::AuthProvider;
use shardline_server_core::auth::LocalHmacProvider;

/// Signing key consistent with production-like configuration.
const SIGNING_KEY: &[u8] = b"benchmark-hmac-signing-key-32-bytes!";

/// Builds a token with realistic claims and returns both the token string
/// and the provider.
fn build_token(claims: &TokenClaims) -> (String, LocalHmacProvider) {
    let provider = LocalHmacProvider::new(SIGNING_KEY).expect("provider");
    let token = provider.mint_token(claims).expect("mint");
    (token, provider)
}

fn bench_verify_token(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_verify_token");

    // Token with max expiry (never expires) for consistent benchmark results.
    let repo = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "bench-owner",
        "bench-repo",
        Some("refs/heads/main"),
    )
    .expect("repo scope");
    let claims = TokenClaims::new(
        "shardline-bench",
        "bench-subject",
        TokenScope::Write,
        repo,
        u64::MAX,
    )
    .expect("claims");

    let (token, _provider) = build_token(&claims);
    let provider = LocalHmacProvider::new(SIGNING_KEY).expect("provider");

    group.bench_function("hmac_sha256_verify", |b| {
        b.iter(|| {
            let result = provider.verify_token(black_box(&token));
            black_box(result).expect("verify should succeed");
        });
    });

    group.finish();
}

fn bench_mint_and_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_mint_and_verify");

    let repo = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "bench-owner",
        "bench-repo",
        Some("refs/heads/main"),
    )
    .expect("repo scope");
    let claims = TokenClaims::new(
        "shardline-bench",
        "bench-subject",
        TokenScope::Write,
        repo,
        u64::MAX,
    )
    .expect("claims");

    group.bench_function("full_roundtrip", |b| {
        b.iter(|| {
            let provider = LocalHmacProvider::new(SIGNING_KEY).expect("provider");
            let token = provider.mint_token(black_box(&claims)).expect("mint");
            let verified = provider.verify_token(black_box(&token)).expect("verify");
            black_box((token, verified));
        });
    });

    group.finish();
}

fn bench_verify_varying_token_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth_verify_varying_size");

    for subject_len in [8, 32, 128, 512] {
        let subject = "x".repeat(subject_len);
        let repo =
            RepositoryScope::new(RepositoryProvider::Generic, "o", "r", None).expect("repo scope");
        let claims =
            TokenClaims::new("issuer", &subject, TokenScope::Read, repo, u64::MAX).expect("claims");
        let (token, _provider) = build_token(&claims);

        group.bench_with_input(
            BenchmarkId::from_parameter(subject_len),
            &token,
            |b, token| {
                let provider = LocalHmacProvider::new(SIGNING_KEY).expect("provider");
                b.iter(|| {
                    black_box(provider.verify_token(black_box(token))).expect("verify");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_verify_token,
    bench_mint_and_verify,
    bench_verify_varying_token_size
);
criterion_main!(benches);
