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
    clippy::format_push_string,
    clippy::option_if_let_else
)]

//! S3 I/O performance baseline against a local MinIO instance.
//!
//! Measures PUT/GET throughput and tail latency (p50/p95/p99) for:
//!   (a) small single-request objects (default 4 KiB), and
//!   (b) multipart uploads + GET of a larger object (default 16 MiB in 2 x 8 MiB parts;
//!       MinIO/S3 require every non-final part to be >= 5 MiB).
//!
//! Drives the real [`shardline_storage::s3::S3ObjectStore`] code path (the same
//! store the shardline server uses), configured to mirror the S3 raw config
//! (see `docker-compose.yml`). If the object store is unreachable the bench
//! prints a loud SKIP message and exits 0.
//!
//! Env config (defaults match docker-compose.yml):
//!   SHARDLINE_S3_ENDPOINT            (default http://127.0.0.1:19000)
//!   SHARDLINE_S3_BUCKET              (default shardline)
//!   SHARDLINE_S3_REGION              (default us-east-1)
//!   SHARDLINE_S3_ACCESS_KEY_ID       (default shardline)
//!   SHARDLINE_S3_SECRET_ACCESS_KEY   (default shardline-dev-password)
//!   SHARDLINE_S3_ALLOW_HTTP          (default "true")
//!   SHARDLINE_S3_KEY_PREFIX          (optional; default is a unique per-run prefix)
//!   SHARDLINE_BENCH_S3_ITERATIONS / BENCH_S3_ITERATIONS
//!                                    (small-object measured iterations, default 10)
//!   SHARDLINE_BENCH_S3_SMALL_SIZE     (bytes, default 4096)
//!   SHARDLINE_BENCH_S3_MULTIPART_SIZE (bytes, default 16 MiB)
//!   SHARDLINE_BENCH_S3_PART_SIZE      (bytes, default 8 MiB; must be >= 5 MiB)

use std::{
    env, process,
    time::{Duration, Instant},
};

use bytes::Bytes;
use shardline_protocol::{ByteRange, SecretString};
use shardline_server_core::chunk_hash;
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore as _, S3ObjectStore, S3ObjectStoreConfig,
};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:19000";
const DEFAULT_BUCKET: &str = "shardline";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_ACCESS_KEY: &str = "shardline";
const DEFAULT_SECRET_KEY: &str = "shardline-dev-password";

const DEFAULT_SMALL_SIZE: usize = 4096;
const DEFAULT_MULTIPART_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_MEASURED_ITERATIONS: usize = 10;
const SMALL_WARMUP_ITERATIONS: usize = 3;
const MULTIPART_WARMUP_ITERATIONS: usize = 1;

const MIB: f64 = 1024.0 * 1024.0;

fn env_u64(names: &[&str], default: u64) -> u64 {
    for name in names {
        if let Ok(raw) = env::var(name)
            && let Ok(parsed) = raw.trim().parse::<u64>()
        {
            return parsed;
        }
    }
    default
}

fn env_bool(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(raw) => raw.eq_ignore_ascii_case("true") || raw == "1",
        Err(_) => default,
    }
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// Returns the p-percentile (0..=100) of a sorted latency sample in milliseconds.
fn percentile_ms(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * percentile / 100.0).round() as usize;
    sorted[index]
}

fn print_row(label: &str, bytes_total: u64, ops: u64, elapsed: Duration, latencies_ms: &[f64]) {
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = ops as f64 / secs;
    let mib_per_sec = (bytes_total as f64 / MIB) / secs;
    let mut sorted = latencies_ms.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let p50 = percentile_ms(&sorted, 50.0);
    let p95 = percentile_ms(&sorted, 95.0);
    let p99 = percentile_ms(&sorted, 99.0);
    println!(
        "{label:<32} {ops_per_sec:>12.1} ops/s  {mib_per_sec:>12.2} MiB/s  p50 {p50:>9.2} ms  p95 {p95:>9.2} ms  p99 {p99:>9.2} ms"
    );
}

/// Formats an error plus its full `source()` chain for diagnostics.
fn format_error_chain(error: &dyn std::error::Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut source = error.source();
    while let Some(next) = source {
        parts.push(next.to_string());
        source = next.source();
    }
    parts.join(": ")
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        println!(
            "\n========== S3 BENCH SKIPPED ==========\n\
             Could not use S3 object store ({})\n\
             Start MinIO: docker compose up -d minio minio-init\n\
             =======================================",
            format_error_chain(&error)
        );
    }
    process::exit(0);
}

async fn run() -> Result<(), shardline_storage::S3ObjectStoreError> {
    let endpoint = env_string("SHARDLINE_S3_ENDPOINT", DEFAULT_ENDPOINT);
    let bucket = env_string("SHARDLINE_S3_BUCKET", DEFAULT_BUCKET);
    let region = env_string("SHARDLINE_S3_REGION", DEFAULT_REGION);
    let access_key = env_string("SHARDLINE_S3_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY);
    let secret_key = env_string("SHARDLINE_S3_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY);
    let allow_http = env_bool("SHARDLINE_S3_ALLOW_HTTP", true);
    let small_size = env_u64(
        &["SHARDLINE_BENCH_S3_SMALL_SIZE"],
        DEFAULT_SMALL_SIZE as u64,
    ) as usize;
    let multipart_size = env_u64(
        &["SHARDLINE_BENCH_S3_MULTIPART_SIZE"],
        DEFAULT_MULTIPART_SIZE as u64,
    ) as usize;
    let part_size = env_u64(&["SHARDLINE_BENCH_S3_PART_SIZE"], DEFAULT_PART_SIZE as u64) as usize;
    let measured_iterations = env_u64(
        &["SHARDLINE_BENCH_S3_ITERATIONS", "BENCH_S3_ITERATIONS"],
        DEFAULT_MEASURED_ITERATIONS as u64,
    ) as usize;

    let key_prefix = match env::var("SHARDLINE_S3_KEY_PREFIX") {
        Ok(raw) if !raw.trim().is_empty() => raw,
        _ => format!(
            "bench/{}/{}",
            process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0_u128, |duration| duration.as_nanos())
        ),
    };

    let config = S3ObjectStoreConfig::new(bucket.clone(), region)
        .with_endpoint(Some(endpoint.clone()))
        .with_allow_http(allow_http)
        .with_credentials(
            Some(SecretString::new(access_key)),
            Some(SecretString::new(secret_key)),
            None,
        )
        .with_key_prefix(Some(&key_prefix));
    let store = S3ObjectStore::new(config)?;

    // Preflight: one cheap round-trip; anything but a clean reply means skip.
    let probe_key = ObjectKey::parse("preflight-probe").expect("static key parses");
    store.contains(&probe_key)?;

    println!(
        "S3 I/O benchmark: endpoint={endpoint} bucket={bucket} prefix={key_prefix} \
         small={small_size}B multipart={multipart_size}B part={part_size}B measured_iters={measured_iterations}"
    );
    println!(
        "{:<32} {:>12} {:>12} {:>11} {:>11} {:>11}",
        "operation", "throughput", "throughput", "p50", "p95", "p99"
    );
    println!(
        "{:<32} {:>12} {:>12} {:>11} {:>11} {:>11}",
        "", "ops/s", "MiB/s", "ms", "ms", "ms"
    );

    let multipart_iterations = (measured_iterations / 3).max(1);

    bench_small_put(&store, small_size, measured_iterations).await?;
    bench_small_get(&store, small_size, measured_iterations).await?;
    bench_multipart(&store, multipart_size, part_size, multipart_iterations).await?;

    println!("\nDone. Re-run with SHARDLINE_BENCH_S3_ITERATIONS=30 for a more stable baseline.");
    Ok(())
}

async fn bench_small_put(
    store: &S3ObjectStore,
    size: usize,
    measured_iterations: usize,
) -> Result<(), shardline_storage::S3ObjectStoreError> {
    let key = ObjectKey::parse("small.bin").expect("static key parses");
    let body = vec![0xAB_u8; size];
    let integrity = ObjectIntegrity::new(chunk_hash(&body), size as u64);

    for _ in 0..SMALL_WARMUP_ITERATIONS {
        store.put_overwrite(&key, ObjectBody::from_slice(&body), &integrity)?;
    }

    let mut latencies_ms = Vec::with_capacity(measured_iterations);
    let mut total_bytes = 0_u64;
    let mut total_ops = 0_u64;
    let start = Instant::now();
    for _ in 0..measured_iterations {
        let op_start = Instant::now();
        store.put_overwrite(&key, ObjectBody::from_slice(&body), &integrity)?;
        latencies_ms.push(op_start.elapsed().as_secs_f64() * 1000.0);
        total_bytes += size as u64;
        total_ops += 1;
    }
    print_row(
        "small PUT",
        total_bytes,
        total_ops,
        start.elapsed(),
        &latencies_ms,
    );
    Ok(())
}

async fn bench_small_get(
    store: &S3ObjectStore,
    size: usize,
    measured_iterations: usize,
) -> Result<(), shardline_storage::S3ObjectStoreError> {
    let key = ObjectKey::parse("small.bin").expect("static key parses");
    // `ByteRange` end is inclusive, so the last byte index is size - 1.
    let range = ByteRange::new(0, size as u64 - 1).expect("valid range");

    for _ in 0..SMALL_WARMUP_ITERATIONS {
        store.read_range(&key, range)?;
    }

    let mut latencies_ms = Vec::with_capacity(measured_iterations);
    let mut total_bytes = 0_u64;
    let mut total_ops = 0_u64;
    let start = Instant::now();
    for _ in 0..measured_iterations {
        let op_start = Instant::now();
        let data = store.read_range(&key, range)?;
        if data.len() != size {
            return Err(shardline_storage::S3ObjectStoreError::IntegrityLengthMismatch);
        }
        latencies_ms.push(op_start.elapsed().as_secs_f64() * 1000.0);
        total_bytes += size as u64;
        total_ops += 1;
    }
    print_row(
        "small GET",
        total_bytes,
        total_ops,
        start.elapsed(),
        &latencies_ms,
    );
    Ok(())
}

async fn bench_multipart(
    store: &S3ObjectStore,
    total_size: usize,
    part_size: usize,
    measured_iterations: usize,
) -> Result<(), shardline_storage::S3ObjectStoreError> {
    let part_count = total_size.div_ceil(part_size);
    if part_count == 0 {
        return Ok(());
    }
    let last_part_size = total_size - (part_count - 1) * part_size;
    let parts: Vec<Bytes> = (0..part_count)
        .map(|part_index| {
            let part_len = if part_index == part_count - 1 {
                last_part_size
            } else {
                part_size
            };
            Bytes::from(vec![0xCD_u8; part_len])
        })
        .collect();

    for _ in 0..MULTIPART_WARMUP_ITERATIONS {
        let key = ObjectKey::parse("multipart-warmup.bin").expect("static key parses");
        upload_and_complete(store, &key, &parts).await?;
        store.read_range(
            &key,
            ByteRange::new(0, total_size as u64 - 1).expect("valid range"),
        )?;
    }

    // Multipart upload phase (create + all parts + complete) counts as one operation.
    let mut upload_latencies_ms = Vec::with_capacity(measured_iterations);
    let mut get_latencies_ms = Vec::with_capacity(measured_iterations);
    let mut upload_total_bytes = 0_u64;
    let mut upload_total_ops = 0_u64;
    let mut get_total_bytes = 0_u64;
    let mut get_total_ops = 0_u64;

    let upload_start = Instant::now();
    for iteration in 0..measured_iterations {
        let key = ObjectKey::parse(&format!("multipart-{iteration}.bin")).expect("key parses");
        let op_start = Instant::now();
        upload_and_complete(store, &key, &parts).await?;
        upload_latencies_ms.push(op_start.elapsed().as_secs_f64() * 1000.0);
        upload_total_bytes += total_size as u64;
        upload_total_ops += 1;
    }
    print_row(
        "multipart PUT (create+parts+complete)",
        upload_total_bytes,
        upload_total_ops,
        upload_start.elapsed(),
        &upload_latencies_ms,
    );

    let get_start = Instant::now();
    for iteration in 0..measured_iterations {
        let key = ObjectKey::parse(&format!("multipart-{iteration}.bin")).expect("key parses");
        let get_op_start = Instant::now();
        let data = store.read_range(
            &key,
            ByteRange::new(0, total_size as u64 - 1).expect("valid range"),
        )?;
        if data.len() != total_size {
            return Err(shardline_storage::S3ObjectStoreError::IntegrityLengthMismatch);
        }
        get_latencies_ms.push(get_op_start.elapsed().as_secs_f64() * 1000.0);
        get_total_bytes += total_size as u64;
        get_total_ops += 1;
    }
    print_row(
        "multipart GET",
        get_total_bytes,
        get_total_ops,
        get_start.elapsed(),
        &get_latencies_ms,
    );
    Ok(())
}

async fn upload_and_complete(
    store: &S3ObjectStore,
    key: &ObjectKey,
    parts: &[Bytes],
) -> Result<(), shardline_storage::S3ObjectStoreError> {
    let upload_id = store.create_resumable_upload(key).await?;
    let mut completed_parts = Vec::with_capacity(parts.len());
    for (part_index, part) in parts.iter().enumerate() {
        let etag = store
            .upload_resumable_part(key, &upload_id, part_index, part.clone())
            .await?;
        completed_parts.push((part_index, etag));
    }
    store
        .complete_resumable_upload(key, &upload_id, completed_parts)
        .await
}
