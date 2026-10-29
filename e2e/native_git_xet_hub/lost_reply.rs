//! Observe committed Git refs before retrying a real client's lost status reply.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

pub(super) fn publish_without_reply(
    writer: &Path,
    home: &Path,
    bytes: &[u8],
    armed: &AtomicBool,
    dropped: &AtomicUsize,
) -> Result<(), TestError> {
    write_file(writer.join("asset.bin"), bytes)?;
    run_git_with_home(writer, home, ["add", ".gitattributes", "asset.bin"])?;
    run_git_with_home(writer, home, ["commit", "-m", "native Hub Xet upload"])?;
    run_git_with_home(writer, home, ["branch", "-M", "main"])?;
    armed.store(true, Ordering::SeqCst);
    let mut command = Command::new("git");
    with_test_home(&mut command, home)
        .current_dir(writer)
        .args(["push", "-u", "origin", "main"]);
    let output = run_command(&mut command)?;
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the real receive-pack handler must accept the ref before losing its reply"
    );
    assert!(
        !output.status.success(),
        "the client unexpectedly received its lost status"
    );
    assert_remote_head(writer, home)?;
    Ok(())
}

pub(super) fn retry_committed(
    writer: &Path,
    home: &Path,
    xorbs: &AtomicUsize,
    shards: &AtomicUsize,
) -> Result<(), TestError> {
    let before = (
        xorbs.load(Ordering::Relaxed),
        shards.load(Ordering::Relaxed),
    );
    run_git_with_home(writer, home, ["push", "-u", "origin", "main"])?;
    assert_remote_head(writer, home)?;
    assert_eq!(
        (
            xorbs.load(Ordering::Relaxed),
            shards.load(Ordering::Relaxed)
        ),
        before,
        "retrying an already committed Git ref must not reupload native Xet payloads"
    );
    Ok(())
}

fn assert_remote_head(writer: &Path, home: &Path) -> Result<(), TestError> {
    let head = git_stdout_with_home(writer, home, ["rev-parse", "HEAD"])?;
    let remote = git_stdout_with_home(writer, home, ["ls-remote", "origin", "refs/heads/main"])?;
    assert_eq!(remote, format!("{head}\trefs/heads/main"));
    Ok(())
}
