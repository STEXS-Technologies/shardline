#![no_main]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use bytes::Bytes;
use futures_util::TryStreamExt;
use libfuzzer_sys::fuzz_target;
use object_store::{
    CopyMode, CopyOptions, GetOptions, ObjectStore as _, ObjectStoreExt, PutMode,
    memory::InMemory, path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{
    LocalObjectStore, LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey,
    ObjectPrefix, ObjectStore, PutOutcome,
};

const MAX_BODY_BYTES: usize = 256;
const CONCURRENT_WRITERS: usize = 4;

fuzz_target!(|data: (u8, Vec<u8>)| {
    let (op, mut body) = data;
    body.truncate(MAX_BODY_BYTES);
    if body.is_empty() {
        body.push(0);
    }

    let hash = ShardlineHash::from_bytes(*blake3::hash(&body).as_bytes());
    let integrity = ObjectIntegrity::new(hash, body.len() as u64);

    match op % 12 {
        0 => local_concurrent_put_same_key_same_body(&body, &integrity),
        1 => local_concurrent_put_same_key_different_body(&body),
        2 => local_concurrent_put_and_read(&body, &integrity),
        3 => async_concurrent_put_same_key_same_body(&body, &integrity),
        4 => async_toctou_race_check_then_put(&body, &integrity),
        5 => async_concurrent_put_different_bytes(&body),
        6 => async_concurrent_delete_and_put(&body, &integrity),
        7 => async_list_consistency_during_writes(&body, &integrity),
        8 => local_concurrent_contains_and_put(&body, &integrity),
        9 => async_concurrent_copy_race(&body, &integrity),
        10 => local_concurrent_delete_and_put(&body, &integrity),
        11 => async_toctou_multipart_race(&body, &integrity),
        _ => {}
    }
});

// ---------------------------------------------------------------------------
// LocalObjectStore helpers
// ---------------------------------------------------------------------------

fn create_local_store() -> (tempfile::TempDir, LocalObjectStore) {
    let sandbox = tempfile::tempdir().expect("tempdir");
    let root = sandbox.path().join("objects");
    let store = LocalObjectStore::new(root).expect("LocalObjectStore::new");
    (sandbox, store)
}

fn create_key(seed: &[u8]) -> ObjectKey {
    let h = blake3::hash(seed);
    let hex = hex::encode(h.as_bytes());
    let key_str = format!("aa/{hex}.obj");
    ObjectKey::parse(&key_str).expect("ObjectKey::parse")
}

fn spawn_concurrent_writers<F>(count: usize, f: F)
where
    F: Fn(usize) + Send + Clone + 'static,
{
    let mut threads = Vec::with_capacity(count);
    for i in 0..count {
        let f = f.clone();
        threads.push(thread::spawn(move || f(i)));
    }
    for t in threads {
        t.join().expect("thread join");
    }
}

// ---------------------------------------------------------------------------
// Local concurrent operations
// ---------------------------------------------------------------------------

/// Operation 0: Concurrent put_if_absent for same key with identical body.
/// All writers must either Inserted (one) or AlreadyExists (rest).
fn local_concurrent_put_same_key_same_body(body: &[u8], integrity: &ObjectIntegrity) {
    let (_sandbox, store) = create_local_store();
    let store = Arc::new(store);
    let key = create_key(body);
    let body = body.to_vec();
    let integrity = *integrity;

    let outcomes: Arc<std::sync::Mutex<Vec<PutOutcome>>> = Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let key = key.clone();
        let body = body.clone();
        let outcomes = outcomes.clone();
        move |_i| {
            let result =
                store.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
            if let Ok(outcome) = result {
                outcomes.lock().unwrap().push(outcome);
            }
        }
    });

    let outcomes = outcomes.lock().unwrap();
    let inserted = outcomes.iter().filter(|o| **o == PutOutcome::Inserted).count();
    let exists = outcomes.iter().filter(|o| **o == PutOutcome::AlreadyExists).count();
    assert!(
        inserted <= 1,
        "concurrent put_if_absent for same key with same body: \
         expected at most 1 Inserted, got {inserted}"
    );
    assert_eq!(
        inserted + exists,
        outcomes.len(),
        "all outcomes must be Inserted or AlreadyExists"
    );
    let end = u64::try_from(body.len())
        .ok()
        .and_then(|len| len.checked_sub(1))
        .expect("non-empty body");
    let range = ByteRange::new(0, end).expect("valid range");
    let stored = store.read_range(&key, range);
    if let Ok(bytes) = stored {
        assert_eq!(bytes, body, "stored bytes must match");
    }
}

/// Operation 1: Concurrent put_if_absent for same key with different bodies.
/// At most one Inserted; all others must be ExistingObjectConflict or AlreadyExists.
fn local_concurrent_put_same_key_different_body(body: &[u8]) {
    let (_sandbox, store) = create_local_store();
    let store = Arc::new(store);
    let key = create_key(body);

    let mut bodies: Vec<Vec<u8>> = (0..CONCURRENT_WRITERS)
        .map(|i| {
            let mut b = body.to_vec();
            b.push(i as u8);
            b
        })
        .collect();
    bodies[0] = body.to_vec();

    let results: Arc<std::sync::Mutex<Vec<Result<PutOutcome, LocalObjectStoreError>>>> =
        Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let key = key.clone();
        let bodies = bodies.clone();
        let results = results.clone();
        move |i| {
            let h = ShardlineHash::from_bytes(*blake3::hash(&bodies[i]).as_bytes());
            let integ = ObjectIntegrity::new(h, bodies[i].len() as u64);
            let result = store.put_if_absent(&key, ObjectBody::from_slice(&bodies[i]), &integ);
            results.lock().unwrap().push(result);
        }
    });

    let results = results.lock().unwrap();
    let inserted = results.iter().filter(|r| matches!(r, Ok(PutOutcome::Inserted))).count();
    assert!(
        inserted <= 1,
        "concurrent put_if_absent for same key with different bodies: \
         expected at most 1 Inserted, got {inserted}"
    );
    if inserted == 0 {
        for r in results.iter() {
            assert!(
                r.is_err(),
                "conflicting concurrent writes: expected all errors, got Ok: {r:?}"
            );
        }
    }
}

/// Operation 2: Concurrent put_if_absent and read_range.
fn local_concurrent_put_and_read(body: &[u8], integrity: &ObjectIntegrity) {
    let (_sandbox, store) = create_local_store();
    let store = Arc::new(store);
    let key = create_key(body);
    let body = body.to_vec();
    let integrity = *integrity;

    let stop = Arc::new(AtomicBool::new(false));
    let reader_store = store.clone();
    let reader_key = key.clone();
    let reader_stop = stop.clone();
    let reader = thread::spawn(move || {
        let mut iterations = 0_u64;
        while !reader_stop.load(Ordering::SeqCst) && iterations < 100 {
            if let Ok(Some(meta)) = reader_store.metadata(&reader_key) {
                if meta.length() > 0 {
                    let end = meta.length().checked_sub(1).expect("non-zero length");
                    let range = ByteRange::new(0, end).expect("valid range");
                    if let Ok(bytes) = reader_store.read_range(&reader_key, range) {
                        assert_eq!(bytes.len() as u64, meta.length());
                    }
                }
            }
            iterations += 1;
        }
    });

    let _put = store.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    stop.store(true, Ordering::SeqCst);
    let _ = reader.join();
}

/// Operation 8: Concurrent contains + put_if_absent.
fn local_concurrent_contains_and_put(body: &[u8], integrity: &ObjectIntegrity) {
    let (_sandbox, store) = create_local_store();
    let store = Arc::new(store);
    let key = create_key(body);
    let body = body.to_vec();
    let integrity = *integrity;

    let stop = Arc::new(AtomicBool::new(false));
    let checker_store = store.clone();
    let checker_key = key.clone();
    let checker_stop = stop.clone();
    let checker = thread::spawn(move || {
        let mut iterations = 0_u64;
        while !checker_stop.load(Ordering::SeqCst) && iterations < 100 {
            let _before = checker_store.contains(&checker_key);
            iterations += 1;
        }
    });

    let outcome = store.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    stop.store(true, Ordering::SeqCst);
    let _ = checker.join();

    let present = store.contains(&key);
    assert!(present.is_ok(), "contains must succeed after put_if_absent");
    if outcome.is_ok() {
        assert!(present.unwrap(), "object must exist after successful put_if_absent");
    }
}

/// Operation 10: Concurrent delete and put for same key.
fn local_concurrent_delete_and_put(body: &[u8], integrity: &ObjectIntegrity) {
    let (_sandbox, store) = create_local_store();
    let store = Arc::new(store);
    let key = create_key(body);
    let body = body.to_vec();
    let integrity = *integrity;

    let _pre = store.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);

    let stop = Arc::new(AtomicBool::new(false));
    let deleter_store = store.clone();
    let deleter_key = key.clone();
    let deleter_stop = stop.clone();
    let deleter = thread::spawn(move || {
        let mut iterations = 0_u64;
        while !deleter_stop.load(Ordering::SeqCst) && iterations < 10 {
            let _ = deleter_store.delete_if_present(&deleter_key);
            iterations += 1;
        }
    });

    let _re = store.put_if_absent(&key, ObjectBody::from_slice(&body), &integrity);
    stop.store(true, Ordering::SeqCst);
    let _ = deleter.join();
    let _present = store.contains(&key);
}

// ---------------------------------------------------------------------------
// Async in-memory operations (simulate S3ObjectStore patterns)
// ---------------------------------------------------------------------------

fn create_async_store() -> InMemory {
    InMemory::new()
}

fn async_location(key: &ObjectKey) -> ObjectStorePath {
    ObjectStorePath::from(key.as_str())
}

/// Operation 3: Async concurrent put_if_absent for same key with identical body.
fn async_concurrent_put_same_key_same_body(body: &[u8], _integrity: &ObjectIntegrity) {
    let store = Arc::new(create_async_store());
    let key = create_key(body);
    let location = async_location(&key);
    let body = body.to_vec();
    let length = body.len() as u64;

    let results: Arc<std::sync::Mutex<Vec<bool>>> = Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let location = location.clone();
        let body = body.clone();
        let results = results.clone();
        move |_i| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            let inserted = rt.block_on(async {
                let exists = store
                    .get_opts(&location, GetOptions::new().with_range(Some(0..1)))
                    .await;
                if exists.is_ok() {
                    return false;
                }
                let result = store
                    .put_opts(&location, Bytes::from(body.clone()).into(), PutMode::Create.into())
                    .await;
                result.is_ok()
            });
            results.lock().unwrap().push(inserted);
        }
    });

    let results = results.lock().unwrap();
    let inserted = results.iter().filter(|r| **r).count();
    assert!(
        inserted <= 1,
        "async concurrent put same key same body: expected at most 1 Inserted, got {inserted}"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async {
        if let Ok(data) = store.get(&location).await {
            let meta = data.meta.clone();
            let stored = data.bytes().await.unwrap();
            assert_eq!(stored.as_ref(), body.as_slice());
            assert_eq!(meta.size, length);
        }
    });
}

/// Operation 4: Async TOCTOU race — explicit check-then-put pattern.
/// Exercises the same race window as S3ObjectStore::begin_content_addressed_upload.
fn async_toctou_race_check_then_put(body: &[u8], _integrity: &ObjectIntegrity) {
    let store = Arc::new(create_async_store());
    let key = create_key(body);
    let location = async_location(&key);
    let body = body.to_vec();

    let results: Arc<std::sync::Mutex<Vec<Result<(), String>>>> = Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let location = location.clone();
        let body = body.clone();
        let results = results.clone();
        move |_i| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            let result: Result<(), String> = rt.block_on(async {
                let head = store.head(&location).await;
                if head.is_ok() {
                    return Ok(());
                }
                let write = store
                    .put_opts(&location, Bytes::from(body.clone()).into(), PutMode::Create.into())
                    .await;
                write.map(|_| ()).map_err(|e| e.to_string())
            });
            results.lock().unwrap().push(result);
        }
    });

    let results = results.lock().unwrap();
    let ok_count = results.iter().filter(|r| r.is_ok()).count();
    assert!(ok_count >= 1, "TOCTOU race: expected at least 1 successful write");
    assert!(
        ok_count <= CONCURRENT_WRITERS,
        "TOCTOU race: at most {CONCURRENT_WRITERS} inserted"
    );
}

/// Operation 5: Async concurrent put for same key with different bodies.
fn async_concurrent_put_different_bytes(body: &[u8]) {
    let store = Arc::new(create_async_store());
    let key = create_key(body);
    let location = async_location(&key);

    let bodies: Vec<Vec<u8>> = (0..CONCURRENT_WRITERS)
        .map(|i| {
            let mut b = body.to_vec();
            b.push(i as u8);
            b
        })
        .collect();

    let first_body = bodies[0].clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async {
        let _ = store
            .put_opts(&location, Bytes::from(first_body).into(), PutMode::Create.into())
            .await;
    });

    let results: Arc<std::sync::Mutex<Vec<bool>>> = Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let location = location.clone();
        let bodies = bodies.clone();
        let results = results.clone();
        move |i| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            let inserted = rt.block_on(async {
                let result = store
                    .put_opts(&location, Bytes::from(bodies[i].clone()).into(), PutMode::Create.into())
                    .await;
                result.is_ok()
            });
            results.lock().unwrap().push(inserted);
        }
    });

    let results = results.lock().unwrap();
    let inserted = results.iter().filter(|r| **r).count();
    assert!(
        inserted <= 1,
        "async different bytes: expected at most 1 Inserted, got {inserted}"
    );

    rt.block_on(async {
        if let Ok(data) = store.get(&location).await {
            let stored = data.bytes().await.unwrap();
            let matches = bodies.iter().any(|b| stored.as_ref() == b.as_slice());
            assert!(matches, "stored bytes don't match any written body");
        }
    });
}

/// Operation 6: Concurrent delete and put on same key (async).
fn async_concurrent_delete_and_put(body: &[u8], _integrity: &ObjectIntegrity) {
    let store = Arc::new(create_async_store());
    let key = create_key(body);
    let location = async_location(&key);
    let body = body.to_vec();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async {
        let _ = store
            .put_opts(&location, Bytes::from(body.clone()).into(), PutMode::Create.into())
            .await;
    });

    let stop = Arc::new(AtomicBool::new(false));
    let deleter_store = store.clone();
    let deleter_location = location.clone();
    let deleter_stop = stop.clone();
    let deleter = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut iterations = 0_u64;
            while !deleter_stop.load(Ordering::SeqCst) && iterations < 10 {
                let _ = deleter_store.delete(&deleter_location).await;
                iterations += 1;
            }
        });
    });

    let writer_stop = stop.clone();
    let writer_store = store.clone();
    let writer_location = location.clone();
    let writer = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut iterations = 0_u64;
            while !writer_stop.load(Ordering::SeqCst) && iterations < 10 {
                let _ = writer_store
                    .put_opts(&writer_location, Bytes::from(body.clone()).into(), PutMode::Create.into())
                    .await;
                iterations += 1;
            }
        });
    });

    stop.store(true, Ordering::SeqCst);
    let _ = deleter.join();
    let _ = writer.join();
}

/// Operation 7: List consistency during concurrent writes (async).
fn async_list_consistency_during_writes(body: &[u8], _integrity: &ObjectIntegrity) {
    let store = Arc::new(create_async_store());
    let base_key = create_key(body);
    let base_location = async_location(&base_key);
    let prefix_str = base_key.as_str().split('/').next().unwrap_or("aa").to_owned();
    let _prefix_str_with_slash = format!("{prefix_str}/");
    let _prefix = ObjectPrefix::parse(&format!("{prefix_str}/")).expect("ObjectPrefix");
    let body = body.to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer_store = store.clone();
    let writer_location = base_location.clone();
    let writer = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut iterations = 0_u64;
            while !writer_stop.load(Ordering::SeqCst) && iterations < 20 {
                let _ = writer_store
                    .put_opts(&writer_location, Bytes::from(body.clone()).into(), PutMode::Overwrite.into())
                    .await;
                iterations += 1;
            }
        });
    });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async {
        for _ in 0..20 {
            let mut listed = store.list(Some(&ObjectStorePath::from(prefix_str.clone())));
            while let Some(entry) = listed.try_next().await.unwrap_or(None) {
                assert!(!entry.location.as_ref().is_empty(), "listed key must not be empty");
            }
        }
    });

    stop.store(true, Ordering::SeqCst);
    let _ = writer.join();
}

/// Operation 9: Async concurrent copy race — simulates copy_object_if_absent TOCTOU.
fn async_concurrent_copy_race(body: &[u8], _integrity: &ObjectIntegrity) {
    let store = Arc::new(create_async_store());
    let source_key = create_key(b"source-seed");
    let dest_key = create_key(body);
    let source_location = async_location(&source_key);
    let dest_location = async_location(&dest_key);
    let body = body.to_vec();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async {
        let _ = store
            .put_opts(&source_location, Bytes::from(body.clone()).into(), PutMode::Create.into())
            .await;
    });

    let results: Arc<std::sync::Mutex<Vec<bool>>> = Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let source_location = source_location.clone();
        let dest_location = dest_location.clone();
        let results = results.clone();
        move |_i| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            let ok = rt.block_on(async {
                let source_head = store.head(&source_location).await;
                if source_head.is_err() {
                    return false;
                }
                let copy = store
                    .copy_opts(&source_location, &dest_location, CopyOptions::new().with_mode(CopyMode::Create))
                    .await;
                copy.is_ok()
            });
            results.lock().unwrap().push(ok);
        }
    });

    let results = results.lock().unwrap();
    let ok_count = results.iter().filter(|r| **r).count();
    assert!(
        ok_count <= 1,
        "concurrent copy race: expected at most 1 successful copy, got {ok_count}"
    );
}

/// Operation 11: Async in-memory multipart-style TOCTOU race.
/// Simulates the exact begin_content_addressed_upload race pattern:
/// 1. Check existence (HEAD/metadata)
/// 2. If absent, start multipart upload (simulated with PutMode::Create)
fn async_toctou_multipart_race(body: &[u8], _integrity: &ObjectIntegrity) {
    let store = Arc::new(create_async_store());
    let key = create_key(body);
    let location = async_location(&key);

    let chunk_size = body.len().max(1);
    let chunks: Vec<Vec<u8>> = body
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let results: Arc<std::sync::Mutex<Vec<Result<(), String>>>> = Arc::default();
    spawn_concurrent_writers(CONCURRENT_WRITERS, {
        let store = store.clone();
        let location = location.clone();
        let chunks = chunks.clone();
        let results = results.clone();
        move |_i| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            let result: Result<(), String> = rt.block_on(async {
                let exists = store.head(&location).await;
                if exists.is_ok() {
                    return Ok(());
                }

                let tmp_seg = location.as_ref().replace('/', "_");
                let tmp_location = ObjectStorePath::from(format!("__tmp/{tmp_seg}"));

                for chunk in &chunks {
                    let write = store
                        .put_opts(&tmp_location, Bytes::from(chunk.clone()).into(), PutMode::Overwrite.into())
                        .await;
                    if write.is_err() {
                        let _ = store.delete(&tmp_location).await;
                        return Err("chunk write failed".to_owned());
                    }
                }

                let copy = store
                    .copy_opts(&tmp_location, &location, CopyOptions::new().with_mode(CopyMode::Create))
                    .await;

                let _ = store.delete(&tmp_location).await;

                match copy {
                    Ok(()) => Ok(()),
                    Err(e) => Err(e.to_string()),
                }
            });
            results.lock().unwrap().push(result);
        }
    });

    let results = results.lock().unwrap();
    let ok_count = results.iter().filter(|r| r.is_ok()).count();
    assert!(
        ok_count >= 1,
        "multipart TOCTOU race: expected at least 1 successful write, got {ok_count}"
    );
    assert!(
        ok_count <= CONCURRENT_WRITERS,
        "multipart TOCTOU race: at most {CONCURRENT_WRITERS} writes, got {ok_count}"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async {
        if let Ok(data) = store.get(&location).await {
            let stored = data.bytes().await.unwrap();
            assert_eq!(
                stored.as_ref(),
                body,
                "multipart TOCTOU race: stored bytes must match original body"
            );
        }
    });
}
