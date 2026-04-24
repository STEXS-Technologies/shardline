use std::{fmt::Display, hint::black_box, process::abort};

use criterion::{Criterion, criterion_group, criterion_main};
use shardline_protocol::{
    ByteRange, ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash, TokenClaims,
    TokenScope, TokenSigner,
};

const HASH_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

fn protocol_benchmarks(criterion: &mut Criterion) {
    let hash = must(
        ShardlineHash::parse_hex(HASH_HEX),
        "benchmark hash fixture should parse",
    );
    let repository = must(
        RepositoryScope::new(
            RepositoryProvider::GitHub,
            "team",
            "assets",
            Some("refs/heads/main"),
        ),
        "benchmark repository scope fixture should be valid",
    );
    let claims = must(
        TokenClaims::new(
            "bench-issuer",
            "bench-subject",
            TokenScope::Write,
            repository,
            u64::MAX,
        ),
        "benchmark token claims fixture should be valid",
    );
    let signer = must(
        TokenSigner::new(b"benchmark-signing-key"),
        "benchmark token signer fixture should be valid",
    );
    let token = must(
        signer.sign(&claims),
        "benchmark token signing fixture should be valid",
    );

    let mut group = criterion.benchmark_group("shardline_protocol");
    group.bench_function("hash_parse_hex", |bench| {
        bench.iter(|| {
            must(
                ShardlineHash::parse_hex(black_box(HASH_HEX)),
                "hash parse failed",
            )
        });
    });
    group.bench_function("hash_hex_string", |bench| {
        bench.iter(|| black_box(hash).hex_string());
    });
    group.bench_function("byte_range_new_and_len", |bench| {
        bench.iter(|| {
            let range = must(
                ByteRange::new(black_box(4096), black_box(8191)),
                "range failed",
            );
            black_box(range.len())
        });
    });
    group.bench_function("chunk_range_new", |bench| {
        bench.iter(|| {
            must(
                ChunkRange::new(black_box(32), black_box(96)),
                "chunk range failed",
            )
        });
    });
    group.bench_function("token_sign", |bench| {
        bench.iter(|| must(signer.sign(black_box(&claims)), "token sign failed"));
    });
    group.bench_function("token_verify_at", |bench| {
        bench.iter(|| {
            must(
                signer.verify_at(black_box(&token), black_box(1)),
                "token verify failed",
            )
        });
    });
    group.finish();
}

fn must<T, E: Display>(result: Result<T, E>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{context}: {error}");
            abort();
        }
    }
}

criterion_group!(benches, protocol_benchmarks);
criterion_main!(benches);
