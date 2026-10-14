use super::super::*;
use shardline_server::task_tracking::TaskRunIdentity;

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned PostgreSQL process-cut verifier"]
async fn postgres_commands_preserve_actual_process_receipt_cuts() -> Result<()> {
    let url = std::env::var("SHARDLINE_OWNED_PG_FIXTURE_URL")?;
    let token = std::env::var("SHARDLINE_OWNED_PG_FIXTURE_TOKEN")?;
    let admin = sqlx::PgPool::connect(&url).await?;
    let saved: String = sqlx::query_scalar("SELECT token FROM shardline_validation_owner")
        .fetch_one(&admin)
        .await?;
    assert_eq!(saved, token, "test must run only in its owned fixture");
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).map_err(|error| error.to_string())?;
    let nonce = hex::encode(nonce);
    for (operation, (kind, arguments)) in [
        ("Fsck", vec!["fsck", "--root"]),
        ("IndexRebuild", vec!["index", "rebuild", "--root"]),
        ("GarbageCollection", vec!["gc", "--mark", "--root"]),
        ("LifecycleRepair", vec!["repair", "lifecycle", "--root"]),
        ("LifecycleRepair", vec!["repair", "--root"]),
    ]
    .into_iter()
    .enumerate()
    {
        for phase in 0..5_u32 {
            verify_cut(&admin, &url, &nonce, operation, kind, &arguments, phase).await?;
        }
    }
    admin.close().await;
    Ok(())
}

async fn verify_cut(
    admin: &sqlx::PgPool,
    url: &str,
    nonce: &str,
    operation: usize,
    kind: &str,
    arguments: &[&str],
    phase: u32,
) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let root = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    let payload = root.path().join("chunks/user-payload");
    fs::create_dir_all(root.path().join("chunks"))?;
    fs::write(&payload, b"preserve payload across receipt cuts")?;
    let name = format!("owned_cut_{nonce}_{operation}_{phase}");
    let condition = match phase {
        0..=2 => format!("NEW.artifact_key LIKE '%/records/{phase:010}'"),
        3 => "NEW.artifact_key LIKE '%/artifacts/%' AND convert_from(NEW.payload,'UTF8') LIKE 'maintenance.success:%'".to_owned(),
        4 => "NEW.artifact_key LIKE '%/records/0000000003'".to_owned(),
        _ => return Err("unsupported receipt phase".into()),
    };
    // Only generated hex, fixed literals and numeric counters enter this DDL.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF {condition} THEN
           PERFORM set_config('application_name','{name}',true);
           PERFORM pg_sleep(600);
         END IF; RETURN NEW; END $$;
         CREATE TRIGGER {name} BEFORE INSERT OR UPDATE ON shardline_task_artifacts_v1
           FOR EACH ROW EXECUTE FUNCTION {name}();"
    )))
    .execute(admin)
    .await?;
    let mut child = StopOnDrop(
        Command::new(binary())
            .env_clear()
            .env("SHARDLINE_INDEX_POSTGRES_URL", url)
            .env("TMPDIR", scratch.path())
            .current_dir(root.path())
            .args(arguments)
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("stdout"))?)
            .stderr(fs::File::create(scratch.path().join("stderr"))?)
            .spawn()?,
    );
    let backend_pid = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(pid) = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep'"
            ).bind(&name).fetch_optional(admin).await? { return Ok::<_, Box<dyn std::error::Error>>(pid); }
            if child.0.try_wait()?.is_some() { return Err("CLI exited before the receipt cut".into()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await??;
    let manifests: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/manifests/%'",
    )
    .fetch_all(admin)
    .await?;
    let identity = manifests
        .iter()
        .map(|bytes| serde_json::from_slice::<TaskRunIdentity>(bytes))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .find(|identity| identity.owner.pid == child.0.id())
        .ok_or("missing original CLI identity")?;
    assert_eq!(identity.operation, kind);
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(child.0.wait()?.signal(), Some(15));
    // The injected server-side sleep can outlive its client. Cancel only this
    // named owned backend so its uncommitted receipt transaction rolls back.
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(backend_pid)
        .fetch_one(admin)
        .await?;
    assert!(cancelled);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let paused: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND application_name=$2 AND wait_event='PgSleep')")
                .bind(backend_pid).bind(&name).fetch_one(admin).await?;
            if !paused { return Ok::<_, sqlx::Error>(()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await??;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER {name} ON shardline_task_artifacts_v1; DROP FUNCTION {name}();"
    )))
    .execute(admin)
    .await?;
    let committed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE $1",
    )
    .bind(format!("%/runs/{}/records/%", identity.id))
    .fetch_one(admin)
    .await?;
    assert_eq!(committed, i64::from(phase.min(3)));
    let workspace = scratch
        .path()
        .join(format!(
            "shardline-maintenance-{}-{}",
            rustix::process::geteuid().as_raw(),
            hex::encode(identity.deployment.0)
        ))
        .join(hex::encode(Digest::of(identity.id.as_bytes()).0));
    let native = Connection::open_with_flags(
        workspace.join("tasks.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let native_records = match phase {
        0..=2 => phase.saturating_add(1),
        3 | 4 => 4,
        _ => return Err("unsupported native phase".into()),
    };
    assert_eq!(
        native.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
            .get::<_, u32>(0))?,
        native_records
    );
    let native_before = fs::read(workspace.join("tasks.sqlite"))?;
    let native_wal_before = fs::read(workspace.join("tasks.sqlite-wal")).ok();
    let snapshot = || async {
        sqlx::query_as::<_, (String, Vec<u8>)>(
            "SELECT artifact_key,payload FROM shardline_task_artifacts_v1 ORDER BY artifact_key",
        )
        .fetch_all(admin)
        .await
    };
    let before = snapshot().await?;
    let inspection = Command::new(binary())
        .env_clear()
        .env("SHARDLINE_INDEX_POSTGRES_URL", url)
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(
        inspection.status.success(),
        "{}",
        String::from_utf8_lossy(&inspection.stderr)
    );
    let page: serde_json::Value = serde_json::from_slice(&inspection.stdout)?;
    let state = if phase < 2 {
        "pending"
    } else {
        "needs_reconciliation"
    };
    assert_eq!(
        page.pointer("/tasks/0/id")
            .and_then(serde_json::Value::as_str),
        Some(identity.id.as_str())
    );
    assert_eq!(
        page.pointer("/tasks/0/state")
            .and_then(serde_json::Value::as_str),
        Some(state)
    );
    assert_eq!(snapshot().await?, before);
    assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, native_before);
    assert_eq!(
        fs::read(workspace.join("tasks.sqlite-wal")).ok(),
        native_wal_before
    );
    assert_eq!(fs::read(&payload)?, b"preserve payload across receipt cuts");
    let fresh = Command::new(binary())
        .env_clear()
        .env("SHARDLINE_INDEX_POSTGRES_URL", url)
        .env("TMPDIR", scratch.path())
        .current_dir(root.path())
        .args(arguments)
        .arg(root.path())
        .output()?;
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    let refreshed = Command::new(binary())
        .env_clear()
        .env("SHARDLINE_INDEX_POSTGRES_URL", url)
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(refreshed.status.success());
    let refreshed_page: serde_json::Value = serde_json::from_slice(&refreshed.stdout)?;
    let tasks = refreshed_page
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .ok_or("missing shared tasks")?;
    assert_eq!(tasks.len(), 2);
    let interrupted = tasks
        .iter()
        .find(|task| {
            task.get("id").and_then(serde_json::Value::as_str) == Some(identity.id.as_str())
        })
        .ok_or("missing interrupted task")?;
    assert_eq!(
        interrupted.get("state").and_then(serde_json::Value::as_str),
        Some(match phase {
            0 => "pending",
            1 | 2 => "needs_reconciliation",
            3 | 4 => "completed",
            _ => return Err("unsupported refreshed phase".into()),
        })
    );
    assert_eq!(
        tasks
            .iter()
            .filter(
                |task| task.get("state").and_then(serde_json::Value::as_str) == Some("completed")
            )
            .count(),
        if phase >= 3 { 2 } else { 1 }
    );
    let after = snapshot().await?;
    for artifact in before {
        assert!(
            after.contains(&artifact),
            "fresh work changed earlier evidence"
        );
    }
    if phase < 3 {
        assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, native_before);
        assert_eq!(
            fs::read(workspace.join("tasks.sqlite-wal")).ok(),
            native_wal_before
        );
    } else {
        assert!(
            !workspace.exists(),
            "verified shared completion should retire private scratch"
        );
    }
    assert_eq!(fs::read(payload)?, b"preserve payload across receipt cuts");
    println!("PASS: PostgreSQL {operation} {kind} receipt phase {phase}");
    Ok(())
}
