#![cfg(unix)]

use std::{fs, io, mem::MaybeUninit, process::Command};

use shardline_storage::{
    AsyncObjectStore, DeleteOutcome, LocalObjectStore, ObjectKey, ObjectStore,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const CHILD_ENV: &str = "SHARDLINE_DEEP_DELETE_CHILD";

struct ChildFdLimit(libc::rlimit);

impl ChildFdLimit {
    fn restore(&self) -> io::Result<()> {
        // SAFETY: the saved limit was obtained from this same disposable child.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for ChildFdLimit {
    fn drop(&mut self) {
        // Restore before TempDir cleanup even when a regression returns early.
        let _restore_result = self.restore();
    }
}

fn lower_child_fd_limit(limit: libc::rlim_t) -> io::Result<ChildFdLimit> {
    let mut current = MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: current points to writable storage for one rlimit value.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, current.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: getrlimit initialized current on success.
    let current = unsafe { current.assume_init() };
    let constrained = libc::rlimit {
        rlim_cur: current.rlim_cur.min(limit),
        rlim_max: current.rlim_max,
    };
    // SAFETY: this initialized limit preserves the hard limit and only lowers
    // the soft limit in a disposable child containing a single selected test.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &constrained) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ChildFdLimit(current))
}

#[test]
fn deep_deletion_uses_bounded_descriptors() -> TestResult {
    for limit in [32, 64] {
        let output = Command::new(std::env::current_exe()?)
            .args(["--exact", "deep_deletion_child", "--nocapture"])
            .env(CHILD_ENV, limit.to_string())
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "deep deletion failed at fd limit {limit}:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
            .into());
        }
    }
    Ok(())
}

#[test]
fn deep_deletion_child() -> TestResult {
    let Some(limit) = std::env::var_os(CHILD_ENV) else {
        return Ok(());
    };
    let limit: libc::rlim_t = limit.to_string_lossy().parse()?;
    let sandbox = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let fd_limit = lower_child_fd_limit(limit)?;
    for depth in [140, 300, 700] {
        let root = sandbox.path().join(depth.to_string());
        let name = format!("{}object", "d/".repeat(depth));
        let key = ObjectKey::parse(&name)?;
        let path = root.join(&name);
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("missing parent"))?;
        fs::create_dir_all(parent)?;
        fs::write(&path, b"payload")?;
        let store = LocalObjectStore::open(root.clone());
        let deleted = if depth == 700 {
            runtime.block_on(AsyncObjectStore::delete_if_present(&store, &key))?
        } else {
            ObjectStore::delete_if_present(&store, &key)?
        };
        if deleted != DeleteOutcome::Deleted
            || path.exists()
            || !root.is_dir()
            || root.join("d").exists()
        {
            return Err(io::Error::other(
                "deep deletion did not prune exactly the empty ancestors",
            )
            .into());
        }
        if ObjectStore::delete_if_present(&store, &key)? != DeleteOutcome::NotFound
            || root.join("d").exists()
        {
            return Err(io::Error::other("Missing retry created directories").into());
        }
        fs::create_dir_all(parent)?;
        let sibling = parent.join("sibling");
        fs::write(&sibling, b"untouched")?;
        if ObjectStore::delete_if_present(&store, &key)? != DeleteOutcome::NotFound
            || fs::read(&sibling)? != b"untouched"
        {
            return Err(io::Error::other("Missing existing-parent retry changed sibling").into());
        }
        fs::write(&path, b"payload")?;
        if ObjectStore::delete_if_present(&store, &key)? != DeleteOutcome::Deleted
            || fs::read(&sibling)? != b"untouched"
        {
            return Err(io::Error::other("deep deletion changed sibling").into());
        }
        let sibling_key = ObjectKey::parse(&format!("{}sibling", "d/".repeat(depth)))?;
        if ObjectStore::delete_if_present(&store, &sibling_key)? != DeleteOutcome::Deleted
            || root.join("d").exists()
        {
            return Err(io::Error::other("sibling cleanup did not prune empty ancestors").into());
        }
        fs::remove_dir(&root)?;
        println!(
            "depth={depth} key_bytes={} fd_limit={limit} deleted/pruned/retried/sibling_preserved",
            name.len()
        );
    }
    fd_limit.restore()?;
    sandbox.close()?;
    Ok(())
}
