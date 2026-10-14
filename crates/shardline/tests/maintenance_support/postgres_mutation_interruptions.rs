use super::super::*;
use super::postgres_fixture;
use shardline_index::{AsyncIndexStore, PostgresIndexStore, RetentionHold};
use shardline_server::task_tracking::TaskRunIdentity;
use shardline_storage::ObjectKey;

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned PostgreSQL process-cut verifier"]
async fn postgres_partial_repair_retains_ownership_until_database_session_ends() -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let (url, admin) = postgres_fixture::connect_owned().await?;
    let root = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("chunks"))?;
    let payload = root.path().join("chunks/user-payload");
    fs::write(&payload, b"protected PostgreSQL payload")?;
    let store = PostgresIndexStore::new(admin.clone());
    let permanent = RetentionHold::new(
        ObjectKey::parse("user-payload")?,
        "permanent protection".into(),
        0,
        None,
    )?;
    store.upsert_retention_hold(&permanent).await?;
    let seed = |ordinal: u32| -> Result<RetentionHold> {
        Ok(RetentionHold::new(
            ObjectKey::parse(&format!("repair-cut/{ordinal}"))?,
            "expired fixture".into(),
            0,
            Some(1),
        )?)
    };
    for ordinal in 0..=1 {
        store.upsert_retention_hold(&seed(ordinal)?).await?;
    }
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).map_err(|error| error.to_string())?;
    let name = format!("owned_mutation_{}", hex::encode(nonce));
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF OLD.object_key = 'repair-cut/1' THEN
           PERFORM set_config('application_name','{name}',true);
           PERFORM pg_sleep(600);
         END IF; RETURN OLD; END $$;
         CREATE TRIGGER {name} BEFORE DELETE ON shardline_retention_holds
           FOR EACH ROW EXECUTE FUNCTION {name}();"
    )))
    .execute(&admin)
    .await?;
    // Configure a long disconnect interval at connection startup so this
    // fixture exercises a retained backend owner even with slow debug startup.
    let mut work_url = url::Url::parse(&url)?;
    work_url
        .query_pairs_mut()
        .append_pair("options", "-c client_connection_check_interval=600000");
    let command = || {
        let mut cli = Command::new(binary());
        cli.env_clear()
            .env("SHARDLINE_INDEX_POSTGRES_URL", work_url.as_str())
            .env("TMPDIR", scratch.path())
            .current_dir(root.path());
        cli
    };
    let mut original = StopOnDrop(
        command()
            .args(["repair", "lifecycle", "--root"])
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("original.stdout"))?)
            .stderr(fs::File::create(scratch.path().join("original.stderr"))?)
            .spawn()?,
    );
    let backend_pid = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(pid) = sqlx::query_scalar::<_, i32>("SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep'")
                .bind(&name).fetch_optional(&admin).await? { return Ok::<_, Box<dyn std::error::Error>>(pid); }
            if original.0.try_wait()?.is_some() { return Err("repair exited before mutation pause".into()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await??;
    assert_eq!(store.list_retention_holds().await?.len(), 2);
    let manifests: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM shardline_task_artifacts_v1 WHERE artifact_key LIKE '%/manifests/%'",
    )
    .fetch_all(&admin)
    .await?;
    let identity = manifests
        .iter()
        .map(|bytes| serde_json::from_slice::<TaskRunIdentity>(bytes))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .find(|identity| identity.owner.pid == original.0.id())
        .ok_or("missing repair identity")?;
    assert_eq!(identity.operation, "LifecycleRepair");
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(original.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(original.0.wait()?.signal(), Some(15));
    // The server-side mutation is still paused after its client is gone. Its
    // shared maintenance owner must continue to exclude destructive/exclusive work.
    let owns: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted)",
    )
    .bind(backend_pid)
    .fetch_one(&admin)
    .await?;
    assert!(owns);
    let mut replacement = StopOnDrop(
        command()
            .args(["index", "rebuild", "--root"])
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("replacement.stdout"))?)
            .stderr(fs::File::create(scratch.path().join("replacement.stderr"))?)
            .spawn()?,
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>$1 AND wait_event='advisory' AND query LIKE '%pg_advisory_lock%')")
                .bind(backend_pid).fetch_one(&admin).await?;
            if waiting { return Ok::<_, Box<dyn std::error::Error>>(()); }
            if let Some(status) = replacement.0.try_wait()? {
                let owner: Option<serde_json::Value> = sqlx::query_scalar(
                    "SELECT jsonb_build_object('pid',pid,'state',state,'wait',wait_event,'query',query) FROM pg_stat_activity WHERE pid=$1",
                ).bind(backend_pid).fetch_optional(&admin).await?;
                return Err(format!(
                    "replacement exited before observing the original owner: {status}; owner: {owner:?}; stdout: {}; stderr: {}",
                    String::from_utf8_lossy(&fs::read(scratch.path().join("replacement.stdout"))?),
                    String::from_utf8_lossy(&fs::read(scratch.path().join("replacement.stderr"))?),
                ).into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await??;
    assert!(replacement.0.try_wait()?.is_none());
    assert_eq!(store.list_retention_holds().await?.len(), 2);
    assert_eq!(fs::read(&payload)?, b"protected PostgreSQL payload");
    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(backend_pid)
        .fetch_one(&admin)
        .await?;
    assert!(cancelled);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = replacement.0.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    assert!(
        replacement.0.wait()?.success(),
        "{}",
        String::from_utf8_lossy(&fs::read(scratch.path().join("replacement.stderr"))?)
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER {name} ON shardline_retention_holds; DROP FUNCTION {name}();"
    )))
    .execute(&admin)
    .await?;
    let surviving = store.list_retention_holds().await?;
    assert_eq!(surviving.len(), 2);
    assert!(surviving.contains(&seed(1)?));
    assert!(surviving.contains(&permanent));
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
    assert_eq!(
        native.query_row("SELECT count(*) FROM penelope_records_v1", [], |row| row
            .get::<_, u32>(0))?,
        3
    );
    let before = fs::read(workspace.join("tasks.sqlite"))?;
    let before_wal = fs::read(workspace.join("tasks.sqlite-wal")).ok();
    let inspection = command()
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(inspection.status.success());
    assert_eq!(store.list_retention_holds().await?, surviving);
    store.upsert_retention_hold(&seed(2)?).await?;
    let fresh = command()
        .args(["repair", "lifecycle", "--root"])
        .arg(root.path())
        .output()?;
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    assert_eq!(store.list_retention_holds().await?, vec![permanent]);
    assert_eq!(fs::read(&payload)?, b"protected PostgreSQL payload");
    assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, before);
    assert_eq!(
        fs::read(workspace.join("tasks.sqlite-wal")).ok(),
        before_wal
    );
    let refreshed = command()
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(refreshed.status.success());
    let page: serde_json::Value = serde_json::from_slice(&refreshed.stdout)?;
    let tasks = page
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .ok_or("missing shared tasks")?;
    assert_eq!(tasks.len(), 3);
    let interrupted = tasks
        .iter()
        .find(|task| {
            task.get("id").and_then(serde_json::Value::as_str) == Some(identity.id.as_str())
        })
        .ok_or("missing interrupted repair")?;
    assert_eq!(
        interrupted.get("state").and_then(serde_json::Value::as_str),
        Some("needs_reconciliation")
    );
    assert_eq!(
        tasks
            .iter()
            .filter(
                |task| task.get("state").and_then(serde_json::Value::as_str) == Some("completed")
            )
            .count(),
        2
    );
    admin.close().await;
    println!("PASS: PostgreSQL partial repair retains server-side ownership and fresh recovery");
    Ok(())
}
