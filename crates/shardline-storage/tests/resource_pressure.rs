#![cfg(unix)]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::shadow_unrelated,
    clippy::unwrap_used
)]

use std::{
    fs::File,
    io,
    mem::MaybeUninit,
    process::Command,
    sync::{Arc, Barrier, mpsc},
    time::Duration,
};

use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_storage::{
    LocalObjectStore, LocalObjectStoreError, ObjectBody, ObjectIntegrity, ObjectKey,
    ObjectStore as _, PutOutcome,
};

const FD_EXHAUSTION_CHILD_ENV: &str = "SHARDLINE_FD_EXHAUSTION_CHILD";
const FD_EXHAUSTION_CHILD_TEST: &str = "local_store_fd_exhaustion_child";
const TEST_SOFT_FD_LIMIT: libc::rlim_t = 64;
#[cfg(target_os = "linux")]
const MEMORY_PRESSURE_CHILD_ENV: &str = "SHARDLINE_MEMORY_PRESSURE_CHILD";
#[cfg(target_os = "linux")]
const MEMORY_PRESSURE_CHILD_TEST: &str = "local_store_memory_pressure_child";
#[cfg(target_os = "linux")]
const MEMORY_PRESSURE_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;
#[cfg(target_os = "linux")]
const MEMORY_PRESSURE_BLOCK_BYTES: usize = 1024 * 1024;

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

#[cfg(target_os = "linux")]
fn current_virtual_memory_bytes() -> io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let Some(vm_size_line) = status.lines().find(|line| line.starts_with("VmSize:")) else {
        return Err(io::Error::other("VmSize missing from /proc/self/status"));
    };
    let Some(kibibytes) = vm_size_line.split_ascii_whitespace().nth(1) else {
        return Err(io::Error::other("VmSize value missing"));
    };
    let kibibytes = kibibytes
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    kibibytes
        .checked_mul(1024)
        .ok_or_else(|| io::Error::other("VmSize overflow"))
}

#[cfg(target_os = "linux")]
fn constrain_address_space() -> io::Result<()> {
    let mut current = MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: `current` is valid writable storage and a successful call
    // initializes one complete `rlimit` value.
    if unsafe { libc::getrlimit(libc::RLIMIT_AS, current.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful `getrlimit` call initialized `current`.
    let current = unsafe { current.assume_init() };
    let desired = current_virtual_memory_bytes()?
        .checked_add(MEMORY_PRESSURE_HEADROOM_BYTES)
        .ok_or_else(|| io::Error::other("address-space limit overflow"))?;
    let constrained = libc::rlimit {
        rlim_cur: desired.min(current.rlim_max),
        rlim_max: current.rlim_max,
    };
    // SAFETY: `constrained` preserves the hard limit and only lowers the soft
    // limit in a disposable child test process.
    if unsafe { libc::setrlimit(libc::RLIMIT_AS, &constrained) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn exhaust_address_space(held: &mut Vec<Vec<u8>>) {
    loop {
        let mut block = Vec::new();
        if block
            .try_reserve_exact(MEMORY_PRESSURE_BLOCK_BYTES)
            .is_err()
        {
            return;
        }
        block.resize(MEMORY_PRESSURE_BLOCK_BYTES, 0xa5);
        held.push(block);
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

#[cfg(target_os = "linux")]
#[test]
fn local_store_memory_pressure_preserves_durable_state_and_recovers() {
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(MEMORY_PRESSURE_CHILD_TEST)
        .arg("--test-threads=1")
        .arg("--nocapture")
        .env(MEMORY_PRESSURE_CHILD_ENV, "1")
        .output()
        .expect("run isolated memory-pressure child");

    assert!(
        output.status.success(),
        "memory-pressure child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[cfg(target_os = "linux")]
#[test]
fn local_store_memory_pressure_child() {
    if std::env::var_os(MEMORY_PRESSURE_CHILD_ENV).is_none() {
        return;
    }

    let root = tempfile::tempdir().expect("create root before applying memory pressure");
    let store = LocalObjectStore::new(root.path().join("objects")).expect("create local store");
    let stable_key = ObjectKey::parse("xorbs/default/memory/stable.xorb").expect("valid key");
    let stable_body = b"durable bytes survive allocator pressure";
    let stable_integrity = ObjectIntegrity::new(
        chunk_hash(stable_body),
        u64::try_from(stable_body.len()).expect("body length fits u64"),
    );
    assert_eq!(
        store
            .put_if_absent(
                &stable_key,
                ObjectBody::from_slice(stable_body),
                &stable_integrity,
            )
            .expect("seed durable object"),
        PutOutcome::Inserted,
    );

    let mut held = Vec::with_capacity(128);
    constrain_address_space().expect("constrain child address space");
    exhaust_address_space(&mut held);
    assert!(
        !held.is_empty(),
        "memory pressure must reserve some address space"
    );
    let mut allocation_probe = Vec::<u8>::new();
    assert!(
        allocation_probe
            .try_reserve_exact(MEMORY_PRESSURE_BLOCK_BYTES)
            .is_err(),
        "address-space pressure must produce a deterministic allocation failure",
    );
    assert!(
        store.contains(&stable_key).expect("check durable object"),
        "memory pressure must not hide previously committed state",
    );

    drop(held);

    let full_range = ByteRange::new(
        0,
        u64::try_from(stable_body.len() - 1).expect("range end fits u64"),
    )
    .expect("valid full range");
    assert_eq!(
        store
            .read_range(&stable_key, full_range)
            .expect("read after pressure release"),
        stable_body,
    );

    let recovered_key = ObjectKey::parse("xorbs/default/memory/recovered.xorb").expect("valid key");
    let recovered_body = b"publication succeeds after memory pressure is released";
    let recovered_integrity = ObjectIntegrity::new(
        chunk_hash(recovered_body),
        u64::try_from(recovered_body.len()).expect("body length fits u64"),
    );
    assert_eq!(
        store
            .put_if_absent(
                &recovered_key,
                ObjectBody::from_slice(recovered_body),
                &recovered_integrity,
            )
            .expect("publish after pressure release"),
        PutOutcome::Inserted,
    );
}

#[test]
fn local_store_recovers_after_all_tokio_workers_are_starved() {
    let root = tempfile::tempdir().expect("create runtime-starvation root");
    let store = Arc::new(
        LocalObjectStore::new(root.path().join("objects")).expect("create local object store"),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build disposable Tokio runtime");
    let release_workers = Arc::new(Barrier::new(3));
    let (started_tx, started_rx) = mpsc::channel();

    for _worker in 0..2 {
        let release_workers = Arc::clone(&release_workers);
        let started_tx = started_tx.clone();
        runtime.spawn(async move {
            started_tx.send(()).expect("report starved worker");
            release_workers.wait();
        });
    }
    drop(started_tx);
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first runtime worker must enter starvation fault");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second runtime worker must enter starvation fault");

    let key = ObjectKey::parse("xorbs/default/runtime/recovered.xorb").expect("valid key");
    let body = b"queued storage work resumes after runtime workers recover";
    let integrity = ObjectIntegrity::new(
        chunk_hash(body),
        u64::try_from(body.len()).expect("body length fits u64"),
    );
    let task_store = Arc::clone(&store);
    let task_key = key.clone();
    let (result_tx, result_rx) = mpsc::channel();
    runtime.spawn(async move {
        let result = task_store.put_if_absent(&task_key, ObjectBody::from_slice(body), &integrity);
        result_tx.send(result).expect("report storage result");
    });

    assert!(
        matches!(
            result_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "storage work must remain queued while every runtime worker is starved",
    );
    assert!(
        !store.contains(&key).expect("check pre-recovery visibility"),
        "queued work must not become partially visible",
    );

    release_workers.wait();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("queued storage work must resume")
            .expect("publication after runtime recovery"),
        PutOutcome::Inserted,
    );
    drop(runtime);

    let full_range = ByteRange::new(
        0,
        u64::try_from(body.len() - 1).expect("range end fits u64"),
    )
    .expect("valid full range");
    assert_eq!(
        store
            .read_range(&key, full_range)
            .expect("read publication after runtime recovery"),
        body,
    );
}
