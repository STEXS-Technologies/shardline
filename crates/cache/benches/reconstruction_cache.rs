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
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use shardline_cache::{
    AsyncReconstructionCache, MemoryReconstructionCache, ReconstructionCacheError,
    ReconstructionCacheKey,
};
use tokio::runtime::Runtime;

/// Pre-populate a cache with `key -> payload` and return the prepared cache.
/// Must be called from within a tokio runtime context (it uses `Handle::current()`).
fn prepare_hit_cache(
    payload: &[u8],
) -> (MemoryReconstructionCache, ReconstructionCacheKey) {
    let cache = MemoryReconstructionCache::new(
        NonZeroU64::new(3600).unwrap(),
        NonZeroUsize::new(10_000).unwrap(),
    );
    let key = ReconstructionCacheKey::latest("hit-test.bin", None);
    tokio::task::block_in_place(|| {
        let cache_ref: &dyn AsyncReconstructionCache = &cache;
        tokio::runtime::Handle::current()
            .block_on(cache_ref.put(&key, payload))
            .expect("setup put");
    });
    (cache, key)
}

fn bench_get_or_load_cache_hit(c: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("reconstruction_cache_get_or_load_hit");

    for payload_size in [256, 4096, 65536] {
        let payload = vec![0xAB_u8; payload_size];

        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(payload_size),
            &payload,
            |b, payload| {
                b.to_async(&runtime).iter_batched(
                    || prepare_hit_cache(payload),
                    |(cache, key)| async move {
                        let result = cache
                            .get_or_load(&key, || {
                                panic!("loader should not be called on cache hit");
                                #[allow(unreachable_code)]
                                Box::pin(async { Ok::<_, ReconstructionCacheError>(vec![]) })
                            })
                            .await;
                        black_box(result.expect("get_or_load hit"));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_get_or_load_cache_miss(c: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("reconstruction_cache_get_or_load_miss");

    for payload_size in [256, 4096, 65536] {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(payload_size),
            &payload_size,
            |b, &payload_size| {
                b.to_async(&runtime).iter_batched(
                    || {
                        let cache = MemoryReconstructionCache::new(
                            NonZeroU64::new(3600).unwrap(),
                            NonZeroUsize::new(10_000).unwrap(),
                        );
                        let key = ReconstructionCacheKey::latest("miss-test.bin", None);
                        let payload = vec![0xCD_u8; payload_size];
                        (cache, key, payload)
                    },
                    |(cache, key, payload)| async move {
                        let result = cache
                            .get_or_load(&key, || {
                                Box::pin(async {
                                    Ok::<_, ReconstructionCacheError>(payload)
                                })
                            })
                            .await;
                        black_box(result.expect("get_or_load miss"));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_get_or_load_concurrent_dedup(c: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");
    let payload = vec![0xEF_u8; 4096];

    let mut group = c.benchmark_group("reconstruction_cache_concurrent_dedup");

    group.bench_function("10_concurrent_loaders", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let cache = Arc::new(MemoryReconstructionCache::new(
                    NonZeroU64::new(3600).unwrap(),
                    NonZeroUsize::new(10_000).unwrap(),
                ));
                let key = ReconstructionCacheKey::latest("concurrent-dedup.bin", None);
                let payload = payload.clone();
                (cache, key, payload)
            },
            |(cache, key, payload)| async move {
                let mut handles = Vec::with_capacity(10);
                for _ in 0..10 {
                    let c = Arc::clone(&cache);
                    let k = key.clone();
                    let p = payload.clone();
                    handles.push(tokio::spawn(async move {
                        let result = c
                            .get_or_load(&k, || {
                                let data = p.clone();
                                Box::pin(async { Ok::<_, ReconstructionCacheError>(data) })
                            })
                            .await;
                        black_box(result.expect("concurrent get_or_load"));
                    }));
                }
                for h in handles {
                    h.await.expect("task joined");
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_put_and_get(c: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("reconstruction_cache_put_and_get");

    group.bench_function("put_then_get", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let cache = MemoryReconstructionCache::new(
                    NonZeroU64::new(3600).unwrap(),
                    NonZeroUsize::new(10_000).unwrap(),
                );
                let key = ReconstructionCacheKey::latest("put-get.bin", None);
                let payload = vec![0x42_u8; 4096];
                (cache, key, payload)
            },
            |(cache, key, payload)| async move {
                let cache_ref: &dyn AsyncReconstructionCache = &cache;
                cache_ref.put(&key, &payload).await.expect("put");
                let result = cache_ref.get(&key).await.expect("get");
                black_box(result);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get_or_load_cache_hit,
    bench_get_or_load_cache_miss,
    bench_get_or_load_concurrent_dedup,
    bench_put_and_get,
);
criterion_main!(benches);
