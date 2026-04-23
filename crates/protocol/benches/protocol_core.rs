use std::{fmt::Display, hint::black_box, process::abort};

use criterion::{Criterion, criterion_group, criterion_main};
use shardline_protocol::{
    ByteRange, ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash, TokenClaims,
    TokenScope, TokenSigner,
};

const HASH_HEX: &str = "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918";

fn protocol_benchmarks(criterion: &mut Criterion) {
    let hash = must(
        ShardlineHash::parse_api_hex(HASH_HEX),
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
    group.bench_function("hash_parse_api_hex", |bench| {
        bench.iter(|| {
            must(
                ShardlineHash::parse_api_hex(black_box(HASH_HEX)),
                "hash parse failed",
            )
        });
    });
    group.bench_function("hash_api_hex_string", |bench| {
        bench.iter(|| black_box(hash).api_hex_string());
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
