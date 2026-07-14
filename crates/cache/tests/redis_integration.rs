#![allow(clippy::unwrap_used)]

use std::num::NonZeroU64;

use shardline_cache::{AsyncReconstructionCache, ReconstructionCacheKey, RedisReconstructionCache};
use shardline_test_support::DockerLocalStack;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_put_get_roundtrip() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("test-file", None);
    let payload = b"Hello, Redis!";

    // Initially empty
    assert!(cache.get(&key).await.unwrap().is_none());

    // Put
    cache.put(&key, payload).await.unwrap();

    // Get
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(payload.to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_delete_removes_entry() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("del-test", None);
    cache.put(&key, b"delete me").await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());

    let deleted = cache.delete(&key).await.unwrap();
    assert!(deleted);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_delete_missing_returns_false() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("missing", None);
    assert!(!cache.delete(&key).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_overwrite_updates_value() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("overwrite-test", None);
    cache.put(&key, b"first").await.unwrap();
    cache.put(&key, b"second").await.unwrap();

    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"second".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_works_with_repository_scope() {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"));
    let Ok(scope) = scope else {
        return;
    };
    let key = ReconstructionCacheKey::latest("scoped-file", Some(&scope));

    cache.put(&key, b"scoped data").await.unwrap();
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"scoped data".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_ready_returns_ok() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();
    cache.ready().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_multiple_keys_are_independent() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key_a = ReconstructionCacheKey::latest("file-a", None);
    let key_b = ReconstructionCacheKey::latest("file-b", None);

    cache.put(&key_a, b"aaa").await.unwrap();
    cache.put(&key_b, b"bbb").await.unwrap();

    assert_eq!(cache.get(&key_a).await.unwrap(), Some(b"aaa".to_vec()));
    assert_eq!(cache.get(&key_b).await.unwrap(), Some(b"bbb".to_vec()));

    cache.delete(&key_a).await.unwrap();
    assert!(cache.get(&key_a).await.unwrap().is_none());
    assert_eq!(cache.get(&key_b).await.unwrap(), Some(b"bbb".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_version_key_latest_not_found() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    // Version key uses a content_hash
    let key = ReconstructionCacheKey::version("file-v", "abcdef0123456789", None);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_get_returns_none_for_missing_key() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    let key = ReconstructionCacheKey::latest("nonexistent", None);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_roundtrip_binary_payload() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let cache =
        RedisReconstructionCache::new(&redis_url, NonZeroU64::new(3600).unwrap()).unwrap();

    // Binary data including null bytes
    let binary = vec![0u8, 1, 2, 255, 128, 64, 0, 255];
    let key = ReconstructionCacheKey::latest("binary-test", None);
    cache.put(&key, &binary).await.unwrap();
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(binary));
}
