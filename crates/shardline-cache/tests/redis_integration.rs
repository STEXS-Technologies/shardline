#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::{fs::read, num::NonZeroU64, time::Duration};

use redis::AsyncCommands;
use shardline_cache::{
    AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheKey,
    ReconstructionCacheLookup, RedisReconstructionCache, RedisTlsConfig,
};
use shardline_protocol::SecretBytes;
use shardline_test_support::DockerLocalStack;

fn redis_fixture(ttl_seconds: u64) -> Option<(DockerLocalStack, RedisReconstructionCache)> {
    DockerLocalStack::builder()
        .with_redis()
        .start()
        .unwrap()
        .map(|stack| {
            let cache = RedisReconstructionCache::new(
                &stack
                    .redis_url()
                    .expect("started Redis fixture must have a URL"),
                NonZeroU64::new(ttl_seconds).unwrap(),
            )
            .unwrap();
            (stack, cache)
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_roundtrips_over_tls_with_mutual_authentication() {
    let Some(stack) = DockerLocalStack::builder()
        .with_redis_tls()
        .start()
        .unwrap()
    else {
        eprintln!("skipping: docker not available");
        return;
    };
    let redis_url = stack.redis_tls_url().unwrap();
    let root_cert = read(stack.redis_tls_ca_cert_path().unwrap()).unwrap();
    let client_cert = read(stack.redis_tls_client_cert_path().unwrap()).unwrap();
    let client_key = read(stack.redis_tls_client_key_path().unwrap()).unwrap();
    let tls = RedisTlsConfig::new(Some(SecretBytes::new(root_cert)))
        .with_client_identity(SecretBytes::new(client_cert), SecretBytes::new(client_key));
    let cache =
        RedisReconstructionCache::new_with_tls(&redis_url, NonZeroU64::new(3600).unwrap(), tls)
            .unwrap();
    let key = ReconstructionCacheKey::latest("mutual-tls-file", None);

    cache.ready().await.unwrap();
    cache
        .put(&key, b"TLS-protected Redis payload")
        .await
        .unwrap();

    assert_eq!(
        cache.get(&key).await.unwrap(),
        Some(b"TLS-protected Redis payload".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_put_get_roundtrip() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

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
async fn redis_cache_serializes_cold_loads_across_replicas() {
    let Some(stack) = DockerLocalStack::builder().with_redis().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let ttl = NonZeroU64::new(3600).unwrap();
    let first_replica = RedisReconstructionCache::new(&redis_url, ttl).unwrap();
    let second_replica = RedisReconstructionCache::new(&redis_url, ttl).unwrap();
    let key = ReconstructionCacheKey::latest("distributed-cold-load", None);

    let first_reservation = match first_replica.get_or_reserve(&key).await.unwrap() {
        ReconstructionCacheLookup::Reserved(reservation) => reservation,
        ReconstructionCacheLookup::Hit(_) => panic!("test key must start cold"),
    };

    let second_key = key.clone();
    let second_load = tokio::spawn(async move { second_replica.get_or_reserve(&second_key).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !second_load.is_finished(),
        "the losing replica must wait for the reservation owner"
    );

    first_replica
        .put_reserved(&key, b"reconstructed-once", &first_reservation)
        .await
        .unwrap();
    assert!(matches!(
        second_load.await.unwrap().unwrap(),
        ReconstructionCacheLookup::Hit(payload) if payload == b"reconstructed-once"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_releases_cancelled_load_reservations() {
    let Some(stack) = DockerLocalStack::builder().with_redis().start().unwrap() else {
        eprintln!("skipping: docker not available");
        return;
    };
    let Some(redis_url) = stack.redis_url() else {
        return;
    };
    let ttl = NonZeroU64::new(3600).unwrap();
    let cancelled_replica = RedisReconstructionCache::new(&redis_url, ttl).unwrap();
    let replacement_replica = RedisReconstructionCache::new(&redis_url, ttl).unwrap();
    let key = ReconstructionCacheKey::latest("cancelled-distributed-load", None);

    let cancelled_reservation = match cancelled_replica.get_or_reserve(&key).await.unwrap() {
        ReconstructionCacheLookup::Reserved(reservation) => reservation,
        ReconstructionCacheLookup::Hit(_) => panic!("test key must start cold"),
    };
    cancelled_replica.release_reservation(&key, &cancelled_reservation);

    let replacement = tokio::time::timeout(
        Duration::from_millis(500),
        replacement_replica.get_or_reserve(&key),
    )
    .await
    .expect("replacement replica should promptly acquire the released reservation")
    .unwrap();
    let replacement_reservation = match replacement {
        ReconstructionCacheLookup::Reserved(reservation) => reservation,
        ReconstructionCacheLookup::Hit(_) => panic!("cancelled load must not publish a value"),
    };
    replacement_replica.release_reservation(&key, &replacement_reservation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_rejects_stale_reservation_publication() {
    let Some((stack, cache)) = redis_fixture(3600) else {
        return;
    };
    let redis_url = stack.redis_url().unwrap();
    let file_id = "stale-distributed-loader";
    let key = ReconstructionCacheKey::latest(file_id, None);
    let stale_reservation = match cache.get_or_reserve(&key).await.unwrap() {
        ReconstructionCacheLookup::Reserved(reservation) => reservation,
        ReconstructionCacheLookup::Hit(_) => panic!("test key must start cold"),
    };

    let client = redis::Client::open(redis_url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let redis_key = format!(
        "shardline:reconstruction:v1:global:latest:{}",
        hex::encode(file_id)
    );
    let loading_key = format!("{redis_key}:loading");
    let _: () = connection
        .set_ex(&loading_key, "replacement-owner", 30_u64)
        .await
        .unwrap();

    let stale_publish = cache
        .put_reserved(&key, b"must-not-publish", &stale_reservation)
        .await;
    assert!(matches!(
        stale_publish,
        Err(ReconstructionCacheError::LostLoadingReservation)
    ));
    let stored: Option<Vec<u8>> = connection.get(&redis_key).await.unwrap();
    assert!(stored.is_none(), "a fenced stale loader must not publish");
    let owner: Option<String> = connection.get(&loading_key).await.unwrap();
    assert_eq!(owner.as_deref(), Some("replacement-owner"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_delete_removes_entry() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("del-test", None);
    cache.put(&key, b"delete me").await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());

    let deleted = cache.delete(&key).await.unwrap();
    assert!(deleted);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_delete_missing_returns_false() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("missing", None);
    assert!(!cache.delete(&key).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_overwrite_updates_value() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("overwrite-test", None);
    cache.put(&key, b"first").await.unwrap();
    cache.put(&key, b"second").await.unwrap();

    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"second".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_works_with_repository_scope() {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

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
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };
    cache.ready().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_multiple_keys_are_independent() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

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
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    // Version key uses a content_hash
    let key = ReconstructionCacheKey::version("file-v", "abcdef0123456789", None);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_get_returns_none_for_missing_key() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("nonexistent", None);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_roundtrip_binary_payload() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    // Binary data including null bytes
    let binary = vec![0u8, 1, 2, 255, 128, 64, 0, 255];
    let key = ReconstructionCacheKey::latest("binary-test", None);
    cache.put(&key, &binary).await.unwrap();
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(binary));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_ttl_expires_entries() {
    let Some((_stack, cache)) = redis_fixture(1) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("ttl-test", None);
    cache.put(&key, b"short-lived").await.unwrap();
    assert_eq!(
        cache.get(&key).await.unwrap(),
        Some(b"short-lived".to_vec())
    );

    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_scoped_keys_are_distinct() {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let scope_a =
        RepositoryScope::new(RepositoryProvider::GitHub, "team-a", "repo-x", Some("main")).unwrap();
    let scope_b = RepositoryScope::new(
        RepositoryProvider::GitLab,
        "team-b",
        "repo-y",
        Some("develop"),
    )
    .unwrap();

    let key_a = ReconstructionCacheKey::latest("file", Some(&scope_a));
    let key_b = ReconstructionCacheKey::latest("file", Some(&scope_b));

    cache.put(&key_a, b"team-a-data").await.unwrap();
    cache.put(&key_b, b"team-b-data").await.unwrap();

    assert_eq!(
        cache.get(&key_a).await.unwrap(),
        Some(b"team-a-data".to_vec())
    );
    assert_eq!(
        cache.get(&key_b).await.unwrap(),
        Some(b"team-b-data".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_cache_large_payload_roundtrip() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };
    for (file_id, byte) in [("large-payload", 0xABu8), ("large-value-roundtrip", 0xCDu8)] {
        let large = vec![byte; 102400];
        let key = ReconstructionCacheKey::latest(file_id, None);
        cache.put(&key, &large).await.unwrap();
        assert_eq!(cache.get(&key).await.unwrap(), Some(large), "{file_id}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_insert_and_get() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    // Use a version key for variation
    let key = ReconstructionCacheKey::version("insert-get-test", "hash123", None);
    let payload = b"insert and get immediately";

    // Initially empty
    assert!(cache.get(&key).await.unwrap().is_none());

    // Insert
    cache.put(&key, payload).await.unwrap();

    // Retrieve
    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(payload.to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_get_missing_key_returns_none() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("definitely-not-cached", None);
    let result = cache.get(&key).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_insert_overwrite() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("overwrite-test-2", None);
    cache.put(&key, b"first write").await.unwrap();
    cache.put(&key, b"second write").await.unwrap();

    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"second write".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_delete_existing() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("delete-me-existing", None);
    cache.put(&key, b"to be deleted").await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());

    let deleted = cache.delete(&key).await.unwrap();
    assert!(deleted);
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_delete_non_existent() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("never-inserted-key", None);
    let deleted = cache.delete(&key).await.unwrap();
    assert!(!deleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_ttl_expiry() {
    let Some((_stack, cache)) = redis_fixture(2) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("ttl-expiry-test", None);
    cache.put(&key, b"short-lived-data").await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());

    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(cache.get(&key).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_insert_with_custom_ttl() {
    let Some((_stack, cache)) = redis_fixture(300) else {
        return;
    };

    let key = ReconstructionCacheKey::latest("custom-ttl-key", None);
    cache.put(&key, b"custom ttl data").await.unwrap();

    let result = cache.get(&key).await.unwrap();
    assert_eq!(result, Some(b"custom ttl data".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_bulk_insert_and_get_multiple_keys() {
    let Some((_stack, cache)) = redis_fixture(3600) else {
        return;
    };

    // Insert 100 distinct keys
    let mut expected = Vec::with_capacity(100);
    for i in 0..100 {
        let key = ReconstructionCacheKey::latest(&format!("bulk-key-{i:04}"), None);
        let value = format!("value-{i}");
        cache.put(&key, value.as_bytes()).await.unwrap();
        expected.push((key, value));
    }

    // Read all back
    for (key, expected_value) in &expected {
        let result = cache.get(key).await.unwrap();
        assert_eq!(result, Some(expected_value.as_bytes().to_vec()));
    }

    // Delete all
    for (key, _) in &expected {
        let deleted = cache.delete(key).await.unwrap();
        assert!(deleted);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_redis_cache_flush_all() {
    let Some((stack, cache)) = redis_fixture(3600) else {
        return;
    };
    let redis_url = stack.redis_url().unwrap();

    // Insert some data
    let key = ReconstructionCacheKey::latest("flush-test-key", None);
    cache.put(&key, b"data before flush").await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());

    // Flush Redis directly using the raw connection
    let client = redis::Client::open(redis_url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    // After flush, the key should be gone
    let result = cache.get(&key).await.unwrap();
    assert!(result.is_none(), "expected None after Redis flush");
}
