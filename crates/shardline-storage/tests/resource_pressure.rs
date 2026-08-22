#![cfg(unix)]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::shadow_unrelated,
    clippy::unwrap_used
)]

use std::{fs::File, io, mem::MaybeUninit, process::Command};

use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{
    LocalObjectStore, LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey,
    ObjectStore as _, PutOutcome,
};

const FD_EXHAUSTION_CHILD_ENV: &str = "SHARDLINE_FD_EXHAUSTION_CHILD";
const FD_EXHAUSTION_CHILD_TEST: &str = "local_store_fd_exhaustion_child";
const TEST_SOFT_FD_LIMIT: libc::rlim_t = 64;

fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

fn lower_soft_fd_limit() -> io::Result<()> {
    let mut current = MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: `current` points to writable storage for one `rlimit`, and the
    // operating system initializes it before a successful return.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, current.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful `getrlimit` call initialized `current`.
    let current = unsafe { current.assume_init() };
    let constrained = libc::rlimit {
        rlim_cur: current.rlim_cur.min(TEST_SOFT_FD_LIMIT),
        rlim_max: current.rlim_max,
    };
    // SAFETY: `constrained` is initialized, preserves the hard limit, and only
    // lowers the soft limit. This runs in a disposable child test process.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &constrained) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn exhaust_file_descriptors() -> io::Result<Vec<File>> {
    let mut held = Vec::new();
    loop {
        match File::open("/dev/null") {
            Ok(file) => held.push(file),
            Err(error) if error.raw_os_error() == Some(libc::EMFILE) => return Ok(held),
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn local_store_fd_exhaustion_is_atomic_and_recoverable() {
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(FD_EXHAUSTION_CHILD_TEST)
        .arg("--test-threads=1")
        .arg("--nocapture")
        .env(FD_EXHAUSTION_CHILD_ENV, "1")
        .output()
        .expect("run isolated fd-exhaustion child");

    assert!(
        output.status.success(),
        "fd-exhaustion child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn local_store_fd_exhaustion_child() {
    if std::env::var_os(FD_EXHAUSTION_CHILD_ENV).is_none() {
        return;
    }

    let root = tempfile::tempdir().expect("create root before applying fd pressure");
    let store = LocalObjectStore::new(root.path().join("objects")).expect("create local store");
    let key = ObjectKey::parse("xorbs/default/fd/exhaustion.xorb").expect("valid key");
    let body = b"fd exhaustion must not expose partial object bytes";
    let integrity = ObjectIntegrity::new(chunk_hash(body), body.len() as u64);

    lower_soft_fd_limit().expect("lower child soft fd limit");
    let held = exhaust_file_descriptors().expect("exhaust child file descriptors");

    let failed = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
    assert!(
        matches!(
            failed,
            Err(LocalObjectStoreError::Io(ref error))
                if error.raw_os_error() == Some(libc::EMFILE)
        ),
        "publication should surface EMFILE, got {failed:?}",
    );

    drop(held);

    assert!(
        !store.contains(&key).expect("check failed publication"),
        "fd exhaustion exposed a partial object",
    );
    assert_eq!(
        store
            .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
            .expect("retry after releasing descriptors"),
        PutOutcome::Inserted,
    );

    let range = ByteRange::new(0, body.len() as u64 - 1).expect("valid full range");
    assert_eq!(
        store
            .read_range(&key, range)
            .expect("read recovered object"),
        body,
    );
}
