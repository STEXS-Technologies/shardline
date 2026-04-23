#![no_main]

use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use libfuzzer_sys::fuzz_target;
use shardline::write_output_bytes;
use shardline_protocol::ShardlineHash;
use shardline_storage::{LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

const MAX_BODY_BYTES: usize = 4096;
const MAX_MUTATIONS: usize = 16;

fuzz_target!(|data: (u8, Vec<u8>)| {
    let (operation, mut body) = data;
    body.truncate(MAX_BODY_BYTES);
    if body.is_empty() {
        body.push(0);
    }

    match operation % 2 {
        0 => exercise_output_write(body.as_slice()),
        _ => exercise_local_object_write(body.as_slice()),
    }
});

fn exercise_output_write(body: &[u8]) {
    let Ok(sandbox) = tempfile::tempdir() else {
        return;
    };
    let Ok(outside) = tempfile::tempdir() else {
        return;
    };

    let parent = sandbox.path().join("reports");
    let output = parent.join("result.json");
    assert!(fs::create_dir_all(&parent).is_ok());

    let stop = Arc::new(AtomicBool::new(false));
    let mutator = spawn_parent_swap_mutator(parent, outside.path().to_path_buf(), stop.clone());
    let write_result = write_output_bytes(&output, body, true);
    stop.store(true, Ordering::SeqCst);
    let _joined = mutator.join();

    assert_no_escape(outside.path(), "result.json");
    if write_result.is_ok() {
        assert_file_is_either_absent_or_expected(output.as_path(), body);
    }
}

fn exercise_local_object_write(body: &[u8]) {
    let Ok(sandbox) = tempfile::tempdir() else {
        return;
    };
    let Ok(outside) = tempfile::tempdir() else {
        return;
    };

    let root = sandbox.path().join("chunks");
    let store = LocalObjectStore::new(root.clone());
    assert!(store.is_ok());
    let Ok(store) = store else {
        return;
    };
    let hash = ShardlineHash::from_bytes(*blake3::hash(body).as_bytes());
    let key_text = format!("aa/{}", hash.api_hex_string());
    let key = ObjectKey::parse(&key_text);
    assert!(key.is_ok());
    let Ok(key) = key else {
        return;
    };
    let integrity = ObjectIntegrity::new(hash, u64::try_from(body.len()).unwrap_or(u64::MAX));

    let swapped_parent = root.join("aa");
    assert!(fs::create_dir_all(&swapped_parent).is_ok());
    let stop = Arc::new(AtomicBool::new(false));
    let mutator =
        spawn_parent_swap_mutator(swapped_parent, outside.path().to_path_buf(), stop.clone());
    let put_result = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    stop.store(true, Ordering::SeqCst);
    let _joined = mutator.join();

    assert_no_escape(outside.path(), hash.api_hex_string().as_str());
    if put_result.is_ok() {
        assert_file_is_either_absent_or_expected(store.path_for_key(&key).as_path(), body);
    }
}

fn spawn_parent_swap_mutator(
    parent: PathBuf,
    outside: PathBuf,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let detached = parent.with_extension("detached");
            for _attempt in 0..MAX_MUTATIONS {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let _removed_file = fs::remove_file(&parent);
                let _removed_detached = fs::remove_dir_all(&detached);
                if fs::rename(&parent, &detached).is_ok() {
                    let _linked = symlink(&outside, &parent);
                    let _removed_link = fs::remove_file(&parent);
                    let _restored = fs::rename(&detached, &parent);
                }
            }
        }

        #[cfg(not(unix))]
        {
            let _ = (parent, outside);
        }
    })
}

fn assert_no_escape(outside: &Path, file_name: &str) {
    assert!(
        !outside.join(file_name).exists(),
        "local filesystem write escaped through a swapped parent directory"
    );
}

fn assert_file_is_either_absent_or_expected(path: &Path, expected: &[u8]) {
    match fs::read(path) {
        Ok(actual) => assert_eq!(actual, expected),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => assert_eq!(error.kind(), ErrorKind::NotFound),
    }
}
