//! Saved command outcomes must preserve FSCK's actual shell status.
use super::super::*;
use bytes::Bytes;
use shardline_server::{LocalBackend, ServerConfig, task_tracking};
use std::num::NonZeroUsize;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_history_preserves_scan_status_when_storage_becomes_unavailable() -> Result<()> {
    let sandbox = tempfile::tempdir()?;
    let root = sandbox.path().join("deployment");
    let backend = LocalBackend::new(
        root.clone(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(1024).ok_or("chunk size")?,
    )
    .await?;
    backend
        .upload_file("asset", Bytes::from_static(b"saved FSCK outcome"), None)
        .await?;
    let scan = || {
        shardline_test_support::native_client_output(
            Command::new(binary())
                .env_clear()
                .current_dir(&root)
                .args(["fsck", "--root"])
                .arg(&root),
            Duration::from_secs(60),
        )
    };
    assert_eq!(scan()?.status.code(), Some(0));

    let retained = sandbox.path().join("retained");
    fs::rename(root.join("chunks"), &retained)?;
    fs::create_dir(root.join("chunks"))?;
    let missing = scan()?;
    assert_eq!(missing.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("issue:"));

    // Tracking also opens the object store. If that admission fails, FSCK must
    // still execute once and preserve its operational failure without inventing
    // a completed history entry or a successful report.
    fs::remove_dir(root.join("chunks"))?;
    fs::write(root.join("chunks"), b"not an object directory")?;
    let failed = scan()?;
    assert_eq!(failed.status.code(), Some(2));
    assert!(!failed.stderr.is_empty());
    assert!(!String::from_utf8_lossy(&failed.stdout).contains("issue_count: 0"));

    let database = Connection::open_with_flags(
        root.join("metadata.sqlite3"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut statement = database.prepare(
        "SELECT payload FROM shardline_task_artifacts_v1
         WHERE artifact_key GLOB '*/artifacts/*'
           AND CAST(payload AS TEXT) LIKE 'maintenance.success:%'",
    )?;
    let reports = statement.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
    let mut observed = Vec::new();
    for report in reports {
        let report = report?;
        let newline = report
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or("outcome header")?;
        let body: serde_json::Value =
            serde_json::from_slice(report.get(newline + 1..).ok_or("outcome body")?)?;
        assert_eq!(
            body.get("schema").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        let status = body
            .get("exit_code")
            .and_then(serde_json::Value::as_u64)
            .ok_or("outcome exit code")?;
        assert_eq!(
            report.get(..newline).ok_or("outcome header")?,
            format!("maintenance.success: {}", status == 0).as_bytes()
        );
        observed.push(status);
    }
    observed.sort_unstable();
    assert_eq!(observed, [0, 1]);

    fs::remove_file(root.join("chunks"))?;
    fs::rename(retained, root.join("chunks"))?;

    let config = ServerConfig::new(
        "127.0.0.1:8080".parse()?,
        "http://127.0.0.1:8080".to_owned(),
        root.clone(),
        NonZeroUsize::new(1024).ok_or("chunk size")?,
    );
    let store = task_tracking::TaskEvidenceStore::from_config(&config)?;
    let summaries = store.summaries(task_tracking::deployment_fingerprint(&config)?, None, 10)?;
    assert_eq!(summaries.len(), 2);
    assert_eq!(
        summaries
            .iter()
            .filter(|run| run.state == task_tracking::TaskRunState::Completed)
            .count(),
        1
    );
    assert_eq!(
        summaries
            .iter()
            .filter(|run| run.state == task_tracking::TaskRunState::Failed)
            .count(),
        1
    );

    // Restore the original immutable bytes and require a fresh successful scan.
    assert_eq!(scan()?.status.code(), Some(0));
    Ok(())
}
