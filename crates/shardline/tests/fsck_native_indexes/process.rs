//! Kill a real repair process after its first uncommitted member writes.
use super::*;
use shardline_index::{PostgresRecordStore, XetXorbDescriptor};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Stdio};
use std::time::Duration;

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        drop(self.0.kill());
        drop(self.0.wait());
    }
}

fn spawn(input: &Path, pid: &Path) -> Result<Worker> {
    Ok(Worker(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "fsck_native_indexes::process::repair_worker",
                "--ignored",
            ])
            .env_clear()
            .env("SHARDLINE_REPAIR_INPUT", input)
            .env("SHARDLINE_REPAIR_PID", pid)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

#[tokio::test]
#[ignore = "subprocess fixture launched by the PostgreSQL/MinIO parent test"]
async fn repair_worker() -> Result<()> {
    let input = std::env::var_os("SHARDLINE_REPAIR_INPUT").ok_or("missing repair input")?;
    let pid = std::env::var_os("SHARDLINE_REPAIR_PID").ok_or("missing repair pid path")?;
    let (url, descriptors): (String, Vec<XetXorbDescriptor>) =
        serde_json::from_slice(&fs::read(input)?)?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    let pid_path = std::path::PathBuf::from(pid);
    let pending_pid = pid_path.with_extension("pending");
    fs::write(&pending_pid, backend.to_string())?;
    fs::rename(pending_pid, pid_path)?;
    PostgresRecordStore::new(pool.clone())
        .cache_xet_xorb_descriptors(&descriptors)
        .await?;
    pool.close().await;
    Ok(())
}

pub(super) async fn qualify(
    root: &Path,
    remote: &RemoteStorage,
    observer: &sqlx::PgPool,
    first: &XetXorbDescriptor,
    second: &XetXorbDescriptor,
) -> Result<()> {
    let (low, high) = if first.hash < second.hash {
        (first, second)
    } else {
        (second, first)
    };
    let hashes = vec![low.hash.clone(), high.hash.clone()];
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=ANY($1)")
        .bind(&hashes)
        .execute(observer)
        .await?;
    let mut blocker = observer.begin().await?;
    sqlx::query("SELECT xorb_hash FROM shardline_xet_xorbs WHERE xorb_hash=$1 FOR UPDATE")
        .bind(&high.hash)
        .fetch_one(&mut *blocker)
        .await?;
    let directory = tempfile::tempdir()?;
    let mut input = tempfile::NamedTempFile::new_in(directory.path())?;
    serde_json::to_writer(
        &mut input,
        &(&remote.database_url, [high.clone(), low.clone()]),
    )?;
    let pid_path = directory.path().join("backend.pid");
    let mut worker = spawn(input.path(), &pid_path)?;
    let startup_deadline = deadline()?;
    let pid = loop {
        if let Ok(value) = fs::read_to_string(&pid_path) {
            break value.parse::<i32>()?;
        }
        assert!(worker.0.try_wait()?.is_none(), "repair worker exited early");
        assert!(
            tokio::time::Instant::now() < startup_deadline,
            "worker startup timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",
        )
        .bind(pid)
        .fetch_one(observer)
        .await?;
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < startup_deadline,
            "repair did not block"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let wrote: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND relation='shardline_xet_chunks'::regclass AND mode='RowExclusiveLock' AND granted)")
        .bind(pid).fetch_one(observer).await?;
    assert!(wrote, "SIGKILL must follow actual member writes");
    expect_scan(root, remote, 1, "process repair remains uncommitted")?;
    worker.0.kill()?;
    assert_eq!(worker.0.wait()?.signal(), Some(9));
    blocker.rollback().await?;
    let cleanup_deadline = deadline()?;
    loop {
        let alive: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                .bind(pid)
                .fetch_one(observer)
                .await?;
        if !alive {
            break;
        }
        assert!(
            tokio::time::Instant::now() < cleanup_deadline,
            "killed session remains"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_xet_chunks WHERE xorb_hash=ANY($1)")
            .bind(&hashes)
            .fetch_one(observer)
            .await?;
    assert_eq!(count, 0, "SIGKILL must roll back the entire repair batch");
    expect_scan(root, remote, 1, "SIGKILL leaves missing index visible")?;
    let mut restarted = spawn(input.path(), &pid_path)?;
    let restart_deadline = deadline()?;
    loop {
        if let Some(status) = restarted.0.try_wait()? {
            assert!(status.success(), "fresh repair process failed");
            break;
        }
        assert!(
            tokio::time::Instant::now() < restart_deadline,
            "repair restart timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    expect_scan(root, remote, 0, "fresh process repairs after SIGKILL")?;
    Ok(())
}

fn deadline() -> Result<tokio::time::Instant> {
    tokio::time::Instant::now()
        .checked_add(Duration::from_secs(20))
        .ok_or_else(|| "process repair deadline overflow".into())
}
