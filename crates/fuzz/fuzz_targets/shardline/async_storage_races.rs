#![no_main]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::redundant_clone,
    clippy::shadow_unrelated
)]

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
    CopyMode, CopyOptions, GetOptions, ObjectStore as _, ObjectStoreExt, PutMode, memory::InMemory,
    path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{
    LocalObjectStore, LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore,
    PutOutcome,
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

fn create_local_store() -> Option<(tempfile::TempDir, LocalObjectStore)> {
    let sandbox = tempfile::tempdir().ok()?;
    let root = sandbox.path().join("objects");
    let store = LocalObjectStore::new(root).ok()?;
    Some((sandbox, store))
}

fn create_key(seed: &[u8]) -> Option<ObjectKey> {
    let h = blake3::hash(seed);
    let hex = hex::encode(h.as_bytes());
    let key_str = format!("aa/{hex}.obj");
    ObjectKey::parse(&key_str).ok()
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
        let _j = t.join();
    }
}

fn range_up_to(length: u64) -> Option<ByteRange> {
    if length == 0 {
        return None;
    }
    let end = length.checked_sub(1)?;
    ByteRange::new(0, end).ok()
}

fn build_rt() -> Option<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()
}

fn local_concurrent_put_same_key_same_body(body: &[u8], integrity: &ObjectIntegrity) {
    let Some((_sandbox, store)) = create_local_store() else {
        return;
    };
    let store = Arc::new(store);
    let Some(key) = create_key(body) else { return };
    let body = body.to_vec();

    let outcomes: Arc<std::sync::Mutex<Vec<PutOutcome>>> = Arc::default();
    let store_writer = store.clone();
    let key_writer = key.clone();
    let body_writer = body.clone();
    let outcomes_writer = outcomes.clone();
    let integrity_writer = *integrity;
    spawn_concurrent_writers(CONCURRENT_WRITERS, move |_i| {
        if let Ok(result) = store_writer.put_if_absent(
            &key_writer,
            ObjectBody::from_slice(&body_writer),
            &integrity_writer,
        ) && let Ok(mut guard) = outcomes_writer.lock()
        {
            guard.push(result);
        }
    });

    let inserted = outcomes.lock().map_or(0, |guard| {
        guard.iter().filter(|o| **o == PutOutcome::Inserted).count()
    });
    let exists = outcomes.lock().map_or(0, |guard| {
        guard
            .iter()
            .filter(|o| **o == PutOutcome::AlreadyExists)
            .count()
    });
    let total = outcomes.lock().map_or(0, |guard| guard.len());
    assert!(
        inserted <= 1,
        "concurrent put_if_absent for same key with same body: \
         expected at most 1 Inserted, got {inserted}"
    );
    assert_eq!(
        inserted.wrapping_add(exists),
        total,
        "all outcomes must be Inserted or AlreadyExists"
    );
    if let Some(range) = range_up_to(body.len() as u64)
        && let Ok(bytes) = store.read_range(&key, range)
    {
        assert_eq!(bytes, body, "stored bytes must match");
    }
}

fn local_concurrent_put_same_key_different_body(body: &[u8]) {
    let Some((_sandbox, store)) = create_local_store() else {
        return;
    };
    let store = Arc::new(store);
    let Some(key) = create_key(body) else { return };

    let mut bodies: Vec<Vec<u8>> = (0..CONCURRENT_WRITERS)
        .map(|i| {
            let mut b = body.to_vec();
            b.push(i as u8);
            b
        })
        .collect();
    if let Some(first) = bodies.first_mut() {
        *first = body.to_vec();
    }

    let results: Arc<std::sync::Mutex<Vec<Result<PutOutcome, LocalObjectStoreError>>>> =
        Arc::default();
    let store_writer = store.clone();
    let key_writer = key.clone();
    let results_writer = results.clone();
    spawn_concurrent_writers(CONCURRENT_WRITERS, move |i| {
        if let Some(chosen) = bodies.get(i) {
            let h = ShardlineHash::from_bytes(*blake3::hash(chosen).as_bytes());
            let integ = ObjectIntegrity::new(h, chosen.len() as u64);
            if let Ok(result) =
                store_writer.put_if_absent(&key_writer, ObjectBody::from_slice(chosen), &integ)
                && let Ok(mut guard) = results_writer.lock()
            {
                guard.push(Ok(result));
            }
        }
    });

    let inserted = results.lock().map_or(0, |guard| {
        guard
            .iter()
            .filter(|r| matches!(r, Ok(PutOutcome::Inserted)))
            .count()
    });
    assert!(
        inserted <= 1,
        "concurrent put_if_absent for same key with different bodies: \
         expected at most 1 Inserted, got {inserted}"
    );
    if inserted == 0 {
        let all_err = results
            .lock()
            .map_or(true, |guard| guard.iter().all(|r| r.is_err()));
        assert!(
            all_err,
            "conflicting concurrent writes: expected all errors"
        );
    }
}

fn local_concurrent_put_and_read(body: &[u8], integrity: &ObjectIntegrity) {
    let Some((_sandbox, store)) = create_local_store() else {
        return;
    };
    let store = Arc::new(store);
    let Some(key) = create_key(body) else { return };
    let body = body.to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let reader_store = store.clone();
    let reader_key = key.clone();
    let reader_stop = stop.clone();
    let reader = thread::spawn(move || {
        let mut iterations = 0_u64;
        while !reader_stop.load(Ordering::SeqCst) && iterations < 100 {
            if let Ok(Some(meta)) = reader_store.metadata(&reader_key)
                && let Some(range) = range_up_to(meta.length())
                && let Ok(bytes) = reader_store.read_range(&reader_key, range)
            {
                assert_eq!(bytes.len() as u64, meta.length());
            }
            iterations = iterations.wrapping_add(1);
        }
    });

    store
        .put_if_absent(&key, ObjectBody::from_slice(&body), integrity)
        .ok();
    stop.store(true, Ordering::SeqCst);
    reader.join().ok();
}

fn local_concurrent_contains_and_put(body: &[u8], integrity: &ObjectIntegrity) {
    let Some((_sandbox, store)) = create_local_store() else {
        return;
    };
    let store = Arc::new(store);
    let Some(key) = create_key(body) else { return };
    let body = body.to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let checker_store = store.clone();
    let checker_key = key.clone();
    let checker_stop = stop.clone();
    let checker = thread::spawn(move || {
        let mut iterations = 0_u64;
        while !checker_stop.load(Ordering::SeqCst) && iterations < 100 {
            checker_store.contains(&checker_key).ok();
            iterations = iterations.wrapping_add(1);
        }
    });

    let outcome = store.put_if_absent(&key, ObjectBody::from_slice(&body), integrity);
    stop.store(true, Ordering::SeqCst);
    checker.join().ok();

    let present = store.contains(&key);
    assert!(present.is_ok(), "contains must succeed after put_if_absent");
    if outcome.is_ok() {
        assert_eq!(
            present.as_ref().ok(),
            Some(&true),
            "object must exist after successful put_if_absent"
        );
    }
}

fn local_concurrent_delete_and_put(body: &[u8], integrity: &ObjectIntegrity) {
    let Some((_sandbox, store)) = create_local_store() else {
        return;
    };
    let store = Arc::new(store);
    let Some(key) = create_key(body) else { return };
    let body = body.to_vec();

    store
        .put_if_absent(&key, ObjectBody::from_slice(&body), integrity)
        .ok();

    let stop = Arc::new(AtomicBool::new(false));
    let deleter_store = store.clone();
    let deleter_key = key.clone();
    let deleter_stop = stop.clone();
    let deleter = thread::spawn(move || {
        let mut iterations = 0_u64;
        while !deleter_stop.load(Ordering::SeqCst) && iterations < 10 {
            deleter_store.delete_if_present(&deleter_key).ok();
            iterations = iterations.wrapping_add(1);
        }
    });

    store
        .put_if_absent(&key, ObjectBody::from_slice(&body), integrity)
        .ok();
    stop.store(true, Ordering::SeqCst);
    deleter.join().ok();
    store.contains(&key).ok();
}

fn async_concurrent_put_same_key_same_body(body: &[u8], _integrity: &ObjectIntegrity) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(key) = create_key(body) else { return };
    let location = ObjectStorePath::from(key.as_str());
    let body = body.to_vec();
    let length = body.len() as u64;

    let results: Arc<std::sync::Mutex<Vec<bool>>> = Arc::default();
    {
        let store = store.clone();
        let location = location.clone();
        let body = body.clone();
        let results = results.clone();
        spawn_concurrent_writers(CONCURRENT_WRITERS, move |_i| {
            if let Some(rt) = build_rt() {
                let inserted = rt.block_on(async {
                    let exists = store
                        .get_opts(&location, GetOptions::new().with_range(Some(0..1)))
                        .await;
                    if exists.is_ok() {
                        return false;
                    }
                    store
                        .put_opts(
                            &location,
                            Bytes::from(body.clone()).into(),
                            PutMode::Create.into(),
                        )
                        .await
                        .is_ok()
                });
                if let Ok(mut guard) = results.lock() {
                    guard.push(inserted);
                }
            }
        });
    }

    let inserted_count = results
        .lock()
        .map_or(0, |guard| guard.iter().filter(|r| **r).count());
    assert!(
        inserted_count <= 1,
        "async concurrent put same key same body: expected at most 1 Inserted, got {inserted_count}"
    );

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            if let Ok(data) = store.get(&location).await {
                let meta = data.meta.clone();
                if let Ok(stored) = data.bytes().await {
                    assert_eq!(stored.as_ref(), body.as_slice());
                    assert_eq!(meta.size, length);
                }
            }
        });
    }
}

fn async_toctou_race_check_then_put(body: &[u8], _integrity: &ObjectIntegrity) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(key) = create_key(body) else { return };
    let location = ObjectStorePath::from(key.as_str());
    let body = body.to_vec();

    let results: Arc<std::sync::Mutex<Vec<Result<(), String>>>> = Arc::default();
    {
        let store = store.clone();
        let location = location.clone();
        let body = body.clone();
        let results = results.clone();
        spawn_concurrent_writers(CONCURRENT_WRITERS, move |_i| {
            if let Some(rt) = build_rt() {
                let result = rt.block_on(async {
                    let head = store.head(&location).await;
                    if head.is_ok() {
                        return Ok(());
                    }
                    store
                        .put_opts(
                            &location,
                            Bytes::from(body.clone()).into(),
                            PutMode::Create.into(),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                });
                if let Ok(mut guard) = results.lock() {
                    guard.push(result);
                }
            }
        });
    }

    let ok_count = results
        .lock()
        .map_or(0, |guard| guard.iter().filter(|r| r.is_ok()).count());
    assert!(
        ok_count >= 1,
        "TOCTOU race: expected at least 1 successful write"
    );
    assert!(
        ok_count <= CONCURRENT_WRITERS,
        "TOCTOU race: at most {CONCURRENT_WRITERS} inserted, got {ok_count}"
    );
}

fn async_concurrent_put_different_bytes(body: &[u8]) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(key) = create_key(body) else { return };
    let location = ObjectStorePath::from(key.as_str());
    let body_owned = body.to_vec();

    let mut bodies: Vec<Vec<u8>> = (0..CONCURRENT_WRITERS)
        .map(|i| {
            let mut b = body_owned.clone();
            b.push(i as u8);
            b
        })
        .collect();
    bodies[0] = body_owned.clone();

    if let Some(rt) = build_rt() {
        let first_body = bodies
            .first()
            .map_or_else(|| body_owned.clone(), Clone::clone);
        rt.block_on(async {
            let _discard = store
                .put_opts(
                    &location,
                    Bytes::from(first_body).into(),
                    PutMode::Create.into(),
                )
                .await;
        });
    }

    let results: Arc<std::sync::Mutex<Vec<bool>>> = Arc::default();
    let bodies_for_writers = bodies.clone();
    {
        let store = store.clone();
        let location = location.clone();
        let results = results.clone();
        spawn_concurrent_writers(CONCURRENT_WRITERS, move |i| {
            if let Some(rt) = build_rt() {
                let chosen = bodies_for_writers
                    .get(i)
                    .map_or_else(|| body_owned.clone(), Clone::clone);
                let inserted = rt.block_on(async {
                    store
                        .put_opts(
                            &location,
                            Bytes::from(chosen).into(),
                            PutMode::Create.into(),
                        )
                        .await
                        .is_ok()
                });
                if let Ok(mut guard) = results.lock() {
                    guard.push(inserted);
                }
            }
        });
    }

    let inserted_count = results
        .lock()
        .map_or(0, |guard| guard.iter().filter(|r| **r).count());
    assert!(
        inserted_count <= 1,
        "async different bytes: expected at most 1 Inserted, got {inserted_count}"
    );

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            if let Ok(data) = store.get(&location).await
                && let Ok(stored) = data.bytes().await
            {
                let matches = bodies.iter().any(|b| stored.as_ref() == b.as_slice());
                assert!(matches, "stored bytes don't match any written body");
            }
        });
    }

    let inserted_count = results
        .lock()
        .map_or(0, |guard| guard.iter().filter(|r| **r).count());
    assert!(
        inserted_count <= 1,
        "async different bytes: expected at most 1 Inserted, got {inserted_count}"
    );

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            if let Ok(data) = store.get(&location).await
                && let Ok(stored) = data.bytes().await
            {
                let matches = bodies.iter().any(|b| stored.as_ref() == b.as_slice());
                assert!(matches, "stored bytes don't match any written body");
            }
        });
    }
}

fn async_concurrent_delete_and_put(body: &[u8], _integrity: &ObjectIntegrity) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(key) = create_key(body) else { return };
    let location = ObjectStorePath::from(key.as_str());
    let body = body.to_vec();

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            let _discard = store
                .put_opts(
                    &location,
                    Bytes::from(body.clone()).into(),
                    PutMode::Create.into(),
                )
                .await;
        });
    }

    let stop = Arc::new(AtomicBool::new(false));
    let deleter_store = store.clone();
    let deleter_location = location.clone();
    let deleter_stop = stop.clone();
    let deleter = thread::spawn(move || {
        if let Some(rt) = build_rt() {
            rt.block_on(async {
                let mut iterations = 0_u64;
                while !deleter_stop.load(Ordering::SeqCst) && iterations < 10 {
                    let _discard = deleter_store.delete(&deleter_location).await;
                    iterations = iterations.wrapping_add(1);
                }
            });
        }
    });

    let writer_stop = stop.clone();
    let writer_store = store.clone();
    let writer_location = location.clone();
    let writer = thread::spawn(move || {
        if let Some(rt) = build_rt() {
            rt.block_on(async {
                let mut iterations = 0_u64;
                while !writer_stop.load(Ordering::SeqCst) && iterations < 10 {
                    let _discard = writer_store
                        .put_opts(
                            &writer_location,
                            Bytes::from(body.clone()).into(),
                            PutMode::Create.into(),
                        )
                        .await;
                    iterations = iterations.wrapping_add(1);
                }
            });
        }
    });

    stop.store(true, Ordering::SeqCst);
    let _discard = deleter.join();
    let _discard = writer.join();
}

fn async_list_consistency_during_writes(body: &[u8], _integrity: &ObjectIntegrity) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(base_key) = create_key(body) else {
        return;
    };
    let location = ObjectStorePath::from(base_key.as_str());
    let prefix_str = base_key
        .as_str()
        .split('/')
        .next()
        .map_or_else(|| "aa".to_owned(), str::to_owned);
    let body = body.to_vec();

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer_store = store.clone();
    let writer_location = location.clone();
    let writer = thread::spawn(move || {
        if let Some(rt) = build_rt() {
            rt.block_on(async {
                let mut iterations = 0_u64;
                while !writer_stop.load(Ordering::SeqCst) && iterations < 20 {
                    let _discard = writer_store
                        .put_opts(
                            &writer_location,
                            Bytes::from(body.clone()).into(),
                            PutMode::Overwrite.into(),
                        )
                        .await;
                    iterations = iterations.wrapping_add(1);
                }
            });
        }
    });

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            for _iteration in 0..20 {
                let mut listed = store.list(Some(&ObjectStorePath::from(prefix_str.clone())));
                while let Some(entry) = listed.try_next().await.unwrap_or(None) {
                    assert!(
                        !entry.location.as_ref().is_empty(),
                        "listed key must not be empty"
                    );
                }
            }
        });
    }

    stop.store(true, Ordering::SeqCst);
    let _discard = writer.join();
}

fn async_concurrent_copy_race(body: &[u8], _integrity: &ObjectIntegrity) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(source_key) = create_key(b"source-seed") else {
        return;
    };
    let Some(dest_key) = create_key(body) else {
        return;
    };
    let source_location = ObjectStorePath::from(source_key.as_str());
    let dest_location = ObjectStorePath::from(dest_key.as_str());
    let body = body.to_vec();

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            let _discard = store
                .put_opts(
                    &source_location,
                    Bytes::from(body.clone()).into(),
                    PutMode::Create.into(),
                )
                .await;
        });
    }

    let results: Arc<std::sync::Mutex<Vec<bool>>> = Arc::default();
    {
        let store = store.clone();
        let source_location = source_location.clone();
        let dest_location = dest_location.clone();
        let results = results.clone();
        spawn_concurrent_writers(CONCURRENT_WRITERS, move |_i| {
            if let Some(rt) = build_rt() {
                let ok = rt.block_on(async {
                    let source_head = store.head(&source_location).await;
                    if source_head.is_err() {
                        return false;
                    }
                    store
                        .copy_opts(
                            &source_location,
                            &dest_location,
                            CopyOptions::new().with_mode(CopyMode::Create),
                        )
                        .await
                        .is_ok()
                });
                if let Ok(mut guard) = results.lock() {
                    guard.push(ok);
                }
            }
        });
    }

    let ok_count = results
        .lock()
        .map_or(0, |guard| guard.iter().filter(|r| **r).count());
    assert!(
        ok_count <= 1,
        "concurrent copy race: expected at most 1 successful copy, got {ok_count}"
    );
}

fn async_toctou_multipart_race(body: &[u8], _integrity: &ObjectIntegrity) {
    let store: Arc<InMemory> = Arc::new(InMemory::new());
    let Some(key) = create_key(body) else { return };
    let location = ObjectStorePath::from(key.as_str());

    let chunk_size = if body.is_empty() { 1 } else { body.len() };
    let chunks: Vec<Vec<u8>> = body.chunks(chunk_size).map(|c| c.to_vec()).collect();

    let results: Arc<std::sync::Mutex<Vec<Result<(), String>>>> = Arc::default();
    {
        let store = store.clone();
        let location = location.clone();
        let chunks = chunks.clone();
        let results = results.clone();
        spawn_concurrent_writers(CONCURRENT_WRITERS, move |_i| {
            if let Some(rt) = build_rt() {
                let result = rt.block_on(async {
                    let exists = store.head(&location).await;
                    if exists.is_ok() {
                        return Ok(());
                    }

                    let tmp_seg = location.as_ref().replace('/', "_");
                    let tmp_location = ObjectStorePath::from(format!("__tmp/{tmp_seg}"));

                    for chunk in &chunks {
                        let write = store
                            .put_opts(
                                &tmp_location,
                                Bytes::from(chunk.clone()).into(),
                                PutMode::Overwrite.into(),
                            )
                            .await;
                        if write.is_err() {
                            let _discard = store.delete(&tmp_location).await;
                            return Err("chunk write failed".to_owned());
                        }
                    }

                    let copy = store
                        .copy_opts(
                            &tmp_location,
                            &location,
                            CopyOptions::new().with_mode(CopyMode::Create),
                        )
                        .await;

                    let _discard = store.delete(&tmp_location).await;

                    match copy {
                        Ok(()) => Ok(()),
                        Err(e) => Err(e.to_string()),
                    }
                });
                if let Ok(mut guard) = results.lock() {
                    guard.push(result);
                }
            }
        });
    }

    let ok_count = results
        .lock()
        .map_or(0, |guard| guard.iter().filter(|r| r.is_ok()).count());
    assert!(
        ok_count >= 1,
        "multipart TOCTOU race: expected at least 1 successful write, got {ok_count}"
    );
    assert!(
        ok_count <= CONCURRENT_WRITERS,
        "multipart TOCTOU race: at most {CONCURRENT_WRITERS} writes, got {ok_count}"
    );

    if let Some(rt) = build_rt() {
        rt.block_on(async {
            if let Ok(data) = store.get(&location).await
                && let Ok(stored) = data.bytes().await
            {
                assert_eq!(
                    stored.as_ref(),
                    body,
                    "multipart TOCTOU race: stored bytes must match original body"
                );
            }
        });
    }
}
