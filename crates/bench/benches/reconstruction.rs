#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::float_cmp,
    clippy::panic,
    clippy::dbg_macro,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::format_push_string
)]

use criterion::{BenchmarkId, Criterion, black_box};
use shardline_index::{FileChunkRecord, FileRecord, StorageRepresentation};
use shardline_server_core::{chunk_hash, parse_stored_file_record_bytes, validate_content_hash};
use std::time::Instant;

fn make_record(chunk_count: usize) -> FileRecord {
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut offset = 0_u64;
    for i in 0..chunk_count {
        let length = 4096_u64;
        let hash = format!("{i:064x}");
        chunks.push(FileChunkRecord {
            hash,
            offset,
            length,
            range_start: i as u64,
            range_end: i as u64 + 1,
            packed_start: offset,
            packed_end: offset + length,
        });
        offset += length;
    }

    FileRecord {
        file_id: "bench.txt".to_owned(),
        content_hash: "b".repeat(64),
        total_bytes: offset,
        chunk_size: 4096,
        repository_scope: None,
        storage_repr: StorageRepresentation::FixedChunkV1,
        chunks,
    }
}

fn bench_reconstruction_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("reconstruction_plan");

    for chunk_count in [1, 10, 100, 1_000] {
        let record = make_record(chunk_count);
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_count),
            &record,
            |b, record| {
                b.iter(|| {
                    black_box(record.validate_reconstruction_plan()).expect("valid plan");
                });
            },
        );
    }

    group.finish();
}

fn bench_chunk_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_hash");

    for size in [1024, 4096, 65536, 1_048_576] {
        let data = vec![0xAB_u8; size];
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                black_box(chunk_hash(data));
            });
        });
    }

    group.finish();
}

fn bench_validate_content_hash(c: &mut Criterion) {
    let valid_hash = "a".repeat(64);
    c.bench_function("validate_content_hash", |b| {
        b.iter(|| {
            black_box(validate_content_hash(black_box(&valid_hash))).expect("valid hash");
        });
    });
}

fn bench_parse_stored_file_record_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_stored_file_record_bytes");

    let small_record = make_record(1);
    let small_json = serde_json::to_vec(&small_record).expect("serialize");
    group.bench_function("1_chunk", |b| {
        b.iter(|| {
            black_box(parse_stored_file_record_bytes(black_box(&small_json))).expect("parse");
        });
    });

    let large_record = make_record(100);
    let large_json = serde_json::to_vec(&large_record).expect("serialize");
    group.bench_function("100_chunks", |b| {
        b.iter(|| {
            black_box(parse_stored_file_record_bytes(black_box(&large_json))).expect("parse");
        });
    });

    group.finish();
}

/// Measured-loop throughput baseline for `validate_reconstruction_plan`.
///
/// Runs a fixed number of validations per chunk count (scaled so total work is
/// roughly constant) and prints records/s and MiB/s alongside the criterion
/// per-op timings. Pure in-memory; deterministic and bounded.
fn bench_reconstruction_throughput(c: &mut Criterion) {
    let group = c.benchmark_group("reconstruction_throughput");

    for chunk_count in [10, 100, 1_000, 10_000] {
        let record = make_record(chunk_count);
        let iterations = (200_000_usize / chunk_count).max(1);
        let total_bytes = record.total_bytes;

        let start = Instant::now();
        let mut validated = 0_u64;
        for _ in 0..iterations {
            if record.validate_reconstruction_plan().is_ok() {
                validated += 1;
            }
        }
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let records_per_sec = validated as f64 / secs;
        let mib_per_sec = (validated as f64 * total_bytes as f64 / (1024.0 * 1024.0)) / secs;
        println!(
            "reconstruction_throughput/{chunk_count:>6} chunks: \
             {records_per_sec:>10.1} records/s  {mib_per_sec:>10.2} MiB/s  \
             ({} iterations, {:?})",
            iterations, elapsed
        );
    }

    group.finish();
}

criterion::criterion_group!(
    benches,
    bench_reconstruction_plan,
    bench_chunk_hash,
    bench_validate_content_hash,
    bench_parse_stored_file_record_bytes,
    bench_reconstruction_throughput,
);
criterion::criterion_main!(benches);
