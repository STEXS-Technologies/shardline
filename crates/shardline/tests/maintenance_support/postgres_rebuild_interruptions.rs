//! Actual PostgreSQL latest-pointer mutation followed by CLI termination.
use super::super::*;
use super::postgres_fixture;
use shardline_index::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, PostgresIndexStore,
    ReconstructionTerm, StoredObjectId,
};
use shardline_index::{FileRecord, PostgresRecordStore, RecordMutation, StorageRepresentation};
use shardline_protocol::{ChunkRange, ShardlineHash};
use shardline_server::task_tracking::TaskRunIdentity;
use shardline_storage::ObjectKey;

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Heads,
    ReconstructionPrune,
    DedupePrune,
    DedupeRestore,
}
impl Mutation {
    const fn table(self) -> &'static str {
        match self {
            Self::Heads => "shardline_file_records",
            Self::ReconstructionPrune => "shardline_file_reconstructions",
            Self::DedupePrune | Self::DedupeRestore => "shardline_dedupe_shards",
        }
    }
    const fn predicate(self) -> &'static str {
        if matches!(self, Self::Heads) {
            "record_kind='latest' AND file_id LIKE 'owned-rebuild-%'"
        } else {
            "true"
        }
    }
    const fn action(self) -> &'static str {
        if matches!(self, Self::Heads | Self::DedupeRestore) {
            "INSERT"
        } else {
            "DELETE"
        }
    }
    async fn seed(self, root: &Path, admin: &sqlx::PgPool, ordinal: u8) -> Result<()> {
        let hash = ShardlineHash::from_bytes([ordinal.saturating_add(17); 32]);
        let store = PostgresIndexStore::new(admin.clone());
        match self {
            Self::Heads => {
                PostgresRecordStore::new(admin.clone())
                    .write_version_record(&FileRecord {
                        file_id: format!("owned-rebuild-{ordinal}"),
                        content_hash: hex::encode(Digest::of(b"").0),
                        total_bytes: 0,
                        chunk_size: 128,
                        storage_repr: StorageRepresentation::FixedChunkV1,
                        repository_scope: None,
                        chunks: Vec::new(),
                    })
                    .await?;
            }
            Self::ReconstructionPrune => {
                store
                    .insert_reconstruction(
                        &FileId::new(hash),
                        &FileReconstruction::new(vec![ReconstructionTerm::new(
                            StoredObjectId::new(hash),
                            ChunkRange::new(0, 1)?,
                            1,
                        )]),
                    )
                    .await?
            }
            Self::DedupePrune => {
                store
                    .upsert_dedupe_shard_mapping(&DedupeShardMapping::new(
                        hash,
                        ObjectKey::parse(&format!("shards/not-retained-{ordinal}"))?,
                    ))
                    .await?
            }
            Self::DedupeRestore => {
                super::rebuild_derived_interruptions::seed_retained_shard(root, ordinal)?
            }
        }
        Ok(())
    }
    async fn count(self, admin: &sqlx::PgPool) -> Result<i64> {
        Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {} WHERE {}",
            self.table(),
            self.predicate()
        )))
        .fetch_one(admin)
        .await?)
    }
}

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned PostgreSQL process-cut verifier"]
async fn postgres_rebuild_preserves_partial_heads_and_fresh_discovery() -> Result<()> {
    verify_cut(Mutation::Heads, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned PostgreSQL process-cut verifier"]
async fn postgres_rebuild_preserves_partial_derived_indexes_and_fresh_discovery() -> Result<()> {
    for mutation in [
        Mutation::ReconstructionPrune,
        Mutation::DedupePrune,
        Mutation::DedupeRestore,
    ] {
        verify_cut(mutation, false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned PostgreSQL process-cut verifier"]
async fn postgres_rebuild_sigkill_preserves_heads_and_derived_indexes() -> Result<()> {
    for mutation in [
        Mutation::Heads,
        Mutation::ReconstructionPrune,
        Mutation::DedupePrune,
        Mutation::DedupeRestore,
    ] {
        verify_cut(mutation, true).await?;
    }
    Ok(())
}

async fn verify_cut(mutation: Mutation, hard_kill: bool) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let (url, admin) = postgres_fixture::connect_owned().await?;
    let root = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("chunks"))?;
    let payload = root.path().join("chunks/protected-rebuild-payload");
    fs::write(&payload, b"unchanged PostgreSQL rebuild payload")?;
    for ordinal in 0..=1 {
        mutation.seed(root.path(), &admin, ordinal).await?;
    }
    let retained_before = if matches!(mutation, Mutation::DedupeRestore) {
        (0..=1)
            .map(|ordinal| {
                let path = root
                    .path()
                    .join(format!("chunks/shards/retained-{ordinal}"));
                Ok((path.clone(), fs::read(path)?))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let versions_before: Vec<(String, String)> = sqlx::query_as(
        "SELECT record_key, record::text FROM shardline_file_records WHERE record_kind='version' ORDER BY record_key",
    ).fetch_all(&admin).await?;
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).map_err(|error| error.to_string())?;
    let name = format!("owned_rebuild_{}", hex::encode(nonce));
    let table = mutation.table();
    let predicate = mutation.predicate();
    let action = mutation.action();
    let trigger_row = if action == "DELETE" { "OLD" } else { "NEW" };
    let row_predicate = if matches!(mutation, Mutation::Heads) {
        "NEW.record_kind='latest' AND NEW.file_id LIKE 'owned-rebuild-%'"
    } else {
        "true"
    };
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF {row_predicate}
           AND (SELECT count(*) FROM {table} WHERE {predicate})=1 THEN
           PERFORM set_config('application_name','{name}',true);
           PERFORM pg_sleep(600);
         END IF; RETURN {trigger_row}; END $$;
         CREATE TRIGGER {name} BEFORE {action} ON {table}
           FOR EACH ROW EXECUTE FUNCTION {name}();"
    )))
    .execute(&admin)
    .await?;
    let command = || {
        let mut cli = Command::new(binary());
        cli.env_clear()
            .env("SHARDLINE_INDEX_POSTGRES_URL", &url)
            .env("TMPDIR", scratch.path())
            .current_dir(root.path());
        cli
    };
    let mut original = StopOnDrop(
        command()
            .args(["index", "rebuild", "--root"])
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("original.stdout"))?)
            .stderr(fs::File::create(scratch.path().join("original.stderr"))?)
            .spawn()?,
    );
    let backend_pid = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(pid) = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep'",
            ).bind(&name).fetch_optional(&admin).await? {
                return Ok::<_, Box<dyn std::error::Error>>(pid);
            }
            if original.0.try_wait()?.is_some() { return Err("rebuild exited before mutation pause".into()); }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await??;
    let heads = || mutation.count(&admin);
    assert_eq!(
        heads().await?,
        1,
        "first head committed before the paused second write"
    );
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
        .ok_or("missing rebuild identity")?;
    assert_eq!(identity.operation, "IndexRebuild");
    assert!(
        Command::new("kill")
            .arg(if hard_kill { "-KILL" } else { "-TERM" })
            .arg(original.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(
        original.0.wait()?.signal(),
        Some(if hard_kill { 9 } else { 15 })
    );
    let owns: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted)",
    )
    .bind(backend_pid)
    .fetch_one(&admin)
    .await?;
    assert!(
        owns,
        "paused server-side write retains its original maintenance owner"
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(backend_pid)
            .fetch_one(&admin)
            .await?
    );
    // Wait for the original backend to release the table before removing only
    // the owned trigger. No application retry or second mutation is dispatched.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let alive: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                    .bind(backend_pid)
                    .fetch_one(&admin)
                    .await?;
            if !alive {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER {name} ON {table}; DROP FUNCTION {name}();"
    )))
    .execute(&admin)
    .await?;
    assert_eq!(heads().await?, 1);
    let versions_after: Vec<(String, String)> = sqlx::query_as(
        "SELECT record_key, record::text FROM shardline_file_records WHERE record_kind='version' ORDER BY record_key",
    ).fetch_all(&admin).await?;
    assert_eq!(versions_after, versions_before);
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
    assert_eq!(
        heads().await?,
        1,
        "inspection does not resume the interrupted rebuild"
    );
    mutation.seed(root.path(), &admin, 2).await?;
    let fresh_child = command()
        .args(["index", "rebuild", "--root"])
        .arg(root.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let fresh_pid = fresh_child.id();
    let fresh = fresh_child.wait_with_output()?;
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    assert_eq!(
        heads().await?,
        if matches!(mutation, Mutation::Heads | Mutation::DedupeRestore) {
            3
        } else {
            0
        },
        "fresh rebuild also discovers the later version"
    );
    let retained_versions: Vec<(String, String)> = sqlx::query_as(
        "SELECT record_key, record::text FROM shardline_file_records WHERE record_kind='version' AND ($1 OR file_id <> 'owned-rebuild-2') ORDER BY record_key",
    ).bind(!matches!(mutation, Mutation::Heads)).fetch_all(&admin).await?;
    assert_eq!(
        retained_versions, versions_before,
        "fresh rebuilding preserves earlier immutable versions"
    );
    assert_eq!(fs::read(&payload)?, b"unchanged PostgreSQL rebuild payload");
    for (path, bytes) in retained_before {
        assert_eq!(fs::read(path)?, bytes);
    }
    if matches!(mutation, Mutation::DedupeRestore) {
        let store = PostgresIndexStore::new(admin.clone());
        for ordinal in 0..=2 {
            let hash = shardline_index::parse_xet_hash_hex(
                &shardline_xet_core::merklehash::compute_data_hash(
                    format!("retained shard chunk {ordinal}").as_bytes(),
                )
                .hex(),
            )?;
            let mapping = store
                .dedupe_shard_mapping(&hash)
                .await?
                .ok_or("missing restored mapping")?;
            assert_eq!(
                mapping.shard_object_key().as_str(),
                format!("shards/retained-{ordinal}")
            );
        }
    }
    assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, before);
    assert_eq!(
        fs::read(workspace.join("tasks.sqlite-wal")).ok(),
        before_wal
    );
    let history = command()
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(history.status.success());
    let page: serde_json::Value = serde_json::from_slice(&history.stdout)?;
    let tasks = page
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .ok_or("missing task history")?;
    assert!(
        tasks
            .iter()
            .any(|task| task["id"] == identity.id && task["state"] == "needs_reconciliation")
    );
    assert!(tasks.iter().any(|task| {
        task["id"] != identity.id
            && task["operation"] == "IndexRebuild"
            && task
                .get("owner")
                .and_then(|owner| owner.get("pid"))
                .and_then(serde_json::Value::as_u64)
                == Some(u64::from(fresh_pid))
            && task["state"] == "completed"
    }));
    admin.close().await;
    if matches!(mutation, Mutation::Heads) {
        println!(
            "PASS: PostgreSQL partial rebuild preserves heads, native evidence and fresh discovery"
        );
    } else {
        println!(
            "PASS: PostgreSQL {mutation:?} preserves partial mutation, native evidence and fresh discovery"
        );
    }
    Ok(())
}
