use std::{
    error::Error, fmt::Display, future::Future, hint::black_box, num::NonZeroUsize, process::abort,
};

use axum::body::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use shardline_server::{LocalBackend, ServerError};
use tempfile::TempDir;
use tokio::{
    runtime::{Handle, Runtime},
    task,
};

const CHUNK_SIZE_BYTES: usize = 65_536;
const BASE_BYTES: usize = 1_048_576;
const MUTATED_BYTES: usize = 4_096;

fn local_backend_benchmarks(criterion: &mut Criterion) {
    let runtime = must(Runtime::new(), "benchmark runtime should initialize");
    let chunk_size = must(
        NonZeroUsize::new(CHUNK_SIZE_BYTES).ok_or("chunk size must be non-zero"),
        "benchmark chunk size fixture should be valid",
    );
    let base = must(
        build_base_asset(BASE_BYTES),
        "benchmark base asset fixture should build",
    );
    let updated = must(
        build_sparse_update(&base, MUTATED_BYTES),
        "benchmark sparse update fixture should build",
    );
    let base_for_initial = base.as_slice();
    let base_for_sparse_setup = base.as_slice();
    let updated_for_sparse_upload = updated.as_slice();
    let updated_for_reconstruction = updated.as_slice();
    let updated_for_download = updated.as_slice();

    let mut group = criterion.benchmark_group("shardline_server_local_backend");
    group.bench_function("upload_initial", |bench| {
        bench.to_async(&runtime).iter_batched(
            || {
                must(
                    setup_backend(chunk_size),
                    "benchmark backend should initialize",
                )
            },
            |(storage, backend)| {
                let base_bytes = base_for_initial;
                async move {
                    let _storage = storage;
                    let response = backend
                        .upload_file(
                            "asset.bin",
                            Bytes::copy_from_slice(black_box(base_bytes)),
                            None,
                        )
                        .await;
                    black_box(must(response, "initial upload benchmark failed"));
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("upload_sparse_update", |bench| {
        bench.to_async(&runtime).iter_batched(
            || {
                must(
                    setup_backend_with_base(chunk_size, base_for_sparse_setup),
                    "benchmark backend should initialize",
                )
            },
            |(storage, backend)| {
                let updated_bytes = updated_for_sparse_upload;
                async move {
                    let _storage = storage;
                    let response = backend
                        .upload_file(
                            "asset.bin",
                            Bytes::copy_from_slice(black_box(updated_bytes)),
                            None,
                        )
                        .await;
                    black_box(must(response, "sparse update benchmark failed"));
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("reconstruction_latest", |bench| {
        bench.to_async(&runtime).iter_batched(
            || {
                must(
                    setup_backend_with_updated(chunk_size, updated_for_reconstruction),
                    "benchmark backend should initialize",
                )
            },
            |(storage, backend)| async move {
                let _storage = storage;
                let response = backend.reconstruction("asset.bin", None, None, None).await;
                black_box(must(response, "reconstruction benchmark failed"));
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("download_latest", |bench| {
        bench.to_async(&runtime).iter_batched(
            || {
                must(
                    setup_backend_with_updated(chunk_size, updated_for_download),
                    "benchmark backend should initialize",
                )
            },
            |(storage, backend)| async move {
                let _storage = storage;
                let response = backend.download_file("asset.bin", None, None).await;
                black_box(must(response, "download benchmark failed"));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn setup_backend(chunk_size: NonZeroUsize) -> Result<(TempDir, LocalBackend), Box<dyn Error>> {
    let storage = TempDir::new()?;
    let backend = run_async(LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    ))?;
    Ok((storage, backend))
}

fn setup_backend_with_base(
    chunk_size: NonZeroUsize,
    base: &[u8],
) -> Result<(TempDir, LocalBackend), Box<dyn Error>> {
    let (storage, backend) = setup_backend(chunk_size)?;
    let base_bytes = Bytes::copy_from_slice(base);
    let _response = run_async(backend.upload_file("asset.bin", base_bytes, None))?;
    Ok((storage, backend))
}

fn setup_backend_with_updated(
    chunk_size: NonZeroUsize,
    updated: &[u8],
) -> Result<(TempDir, LocalBackend), Box<dyn Error>> {
    let (storage, backend) = setup_backend(chunk_size)?;
    let updated_bytes = Bytes::copy_from_slice(updated);
    let _response = run_async(backend.upload_file("asset.bin", updated_bytes, None))?;
    Ok((storage, backend))
}

fn build_base_asset(length: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = Vec::with_capacity(length);
    for index in 0..length {
        let value = u8::try_from((index.saturating_mul(31).saturating_add(17)) % 251)?;
        bytes.push(value);
    }

    Ok(bytes)
}

fn build_sparse_update(base: &[u8], mutated_bytes: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut updated = base.to_vec();
    let remaining = base
        .len()
        .checked_sub(mutated_bytes)
        .ok_or("mutated bytes must not exceed base bytes")?;
    let start = remaining
        .checked_div(2)
        .ok_or("benchmark divisor was zero")?;
    let end = start
        .checked_add(mutated_bytes)
        .ok_or("mutation window overflowed")?;
    let window = updated
        .get_mut(start..end)
        .ok_or("mutation window was out of bounds")?;
    for (offset, byte) in window.iter_mut().enumerate() {
        let delta = u8::try_from((offset.saturating_mul(13).saturating_add(29)) % 251)?;
        *byte = byte.wrapping_add(delta).wrapping_add(1);
    }

    Ok(updated)
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

fn run_async<T>(future: impl Future<Output = Result<T, ServerError>>) -> Result<T, Box<dyn Error>> {
    match Handle::try_current() {
        Ok(handle) => Ok(task::block_in_place(|| handle.block_on(future))?),
        Err(_error) => Ok(Runtime::new()?.block_on(future)?),
    }
}

criterion_group!(benches, local_backend_benchmarks);
criterion_main!(benches);
