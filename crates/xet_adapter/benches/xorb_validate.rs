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
use std::io::Cursor;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use shardline_protocol::ShardlineHash;
use shardline_xet_adapter::{decode_serialized_xorb_chunks, validate_serialized_xorb};
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

/// Builds a realistic serialized xorb with `num_chunks` fixed-size chunks.
fn build_xorb(num_chunks: usize, chunk_size: usize) -> (Vec<u8>, ShardlineHash) {
    let mut data = Vec::with_capacity(num_chunks * chunk_size);
    let mut hashes_and_boundaries = Vec::with_capacity(num_chunks);
    let mut chunk_hashes_and_sizes = Vec::with_capacity(num_chunks);

    for i in 0..num_chunks {
        // Use varying byte patterns so chunks are not all identical.
        let chunk: Vec<u8> = (0..chunk_size).map(|j| ((i + j) & 0xFF) as u8).collect();
        let chunk_hash = compute_data_hash(&chunk);
        data.extend_from_slice(&chunk);
        let boundary = u32::try_from((i + 1) * chunk_size).expect("boundary fits in u32");
        hashes_and_boundaries.push((chunk_hash, boundary));
        chunk_hashes_and_sizes.push((
            chunk_hash,
            u64::try_from(chunk_size).expect("size fits in u64"),
        ));
    }

    let xorb_hash = xorb_hash(&chunk_hashes_and_sizes);

    let SerializedXorbObject {
        serialized_data, ..
    } = serialized_xorb_object_from_components(
        &xorb_hash,
        data,
        hashes_and_boundaries,
        CompressionScheme::LZ4,
    )
    .expect("benchmark xorb fixture should build");

    // Convert merkle hash to ShardlineHash.
    let expected_hash =
        ShardlineHash::from_bytes(xorb_hash.as_bytes().try_into().expect("hash is 32 bytes"));

    (serialized_data, expected_hash)
}

fn bench_validate_serialized_xorb(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_serialized_xorb");

    for chunk_count in [1, 4, 16, 64] {
        let chunk_size = 4096; // 4 KiB chunks
        let (data, hash) = build_xorb(chunk_count, chunk_size);

        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_count),
            &(data, hash),
            |b, (data, hash)| {
                b.iter(|| {
                    let mut reader = Cursor::new(black_box(data));
                    let validated =
                        validate_serialized_xorb(&mut reader, *hash).expect("valid xorb");
                    black_box(validated);
                });
            },
        );
    }

    group.finish();
}

fn bench_decode_serialized_xorb_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_serialized_xorb_chunks");

    for chunk_count in [1, 4, 16, 64] {
        let chunk_size = 4096;
        let (data, hash) = build_xorb(chunk_count, chunk_size);

        // Pre-validate so decode benchmarks measure only the decode phase.
        let mut pre_reader = Cursor::new(data.as_slice());
        let validated = validate_serialized_xorb(&mut pre_reader, hash).expect("valid xorb");

        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_count),
            &(data, validated),
            |b, (data, validated)| {
                b.iter(|| {
                    let mut reader = Cursor::new(black_box(data.as_slice()));
                    let decoded =
                        decode_serialized_xorb_chunks(&mut reader, validated).expect("decode ok");
                    black_box(decoded);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_validate_serialized_xorb,
    bench_decode_serialized_xorb_chunks
);
criterion_main!(benches);
