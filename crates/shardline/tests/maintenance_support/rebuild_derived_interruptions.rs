use super::super::*;
use shardline_index::{
    DedupeShardMapping, DedupeStore, FileId, FileReconstruction, LocalIndexStore,
    ReconstructionStore, ReconstructionTerm, StoredObjectId, parse_xet_hash_hex,
};
use shardline_protocol::{ChunkRange, ShardlineHash};
use shardline_server::LocalBackend;
use shardline_storage::ObjectKey;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    metadata_shard::{
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
};

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[derive(Clone, Copy)]
enum Mutation {
    ReconstructionPrune,
    DedupePrune,
    DedupeRestore,
}
impl Mutation {
    const fn table(self) -> &'static str {
        match self {
            Self::ReconstructionPrune => "shardline_file_reconstructions",
            Self::DedupePrune | Self::DedupeRestore => "shardline_dedupe_shards",
        }
    }
    const fn trigger_action(self) -> &'static str {
        match self {
            Self::DedupeRestore => "INSERT",
            Self::ReconstructionPrune | Self::DedupePrune => "DELETE",
        }
    }
    fn seed(self, root: &Path, store: &LocalIndexStore, ordinal: u8) -> Result<()> {
        let hash = ShardlineHash::from_bytes([ordinal.saturating_add(17); 32]);
        match self {
            Self::ReconstructionPrune => {
                store.insert_reconstruction(
                    &FileId::new(hash),
                    &FileReconstruction::new(vec![ReconstructionTerm::new(
                        StoredObjectId::new(hash),
                        ChunkRange::new(0, 1)?,
                        1,
                    )]),
                )?;
            }
            Self::DedupePrune => store.upsert_dedupe_shard_mapping(&DedupeShardMapping::new(
                hash,
                ObjectKey::parse(&format!("shards/not-retained-{ordinal}"))?,
            ))?,
            Self::DedupeRestore => {
                let body = format!("retained shard chunk {ordinal}");
                let chunk = compute_data_hash(body.as_bytes());
                let length = u64::try_from(body.len())?;
                let xorb = xorb_hash(&[(chunk, length)]);
                let mut shard = MDBInMemoryShard::default();
                shard.add_xorb_block(MDBXorbInfo {
                    metadata: XorbChunkSequenceHeader::new(xorb, 1, length),
                    chunks: vec![
                        XorbChunkSequenceEntry::new(chunk, length, 0).with_global_dedup_flag(true),
                    ],
                })?;
                fs::create_dir_all(root.join("chunks/shards"))?;
                fs::write(
                    root.join(format!("chunks/shards/retained-{ordinal}")),
                    shard.to_bytes()?,
                )?;
            }
        }
        Ok(())
    }
    fn verify_count(self, store: &LocalIndexStore) -> Result<usize> {
        match self {
            Self::ReconstructionPrune => {
                let files = store.list_reconstruction_file_ids()?;
                for file in &files {
                    assert!(store.reconstruction(file)?.is_some());
                }
                Ok(files.len())
            }
            Self::DedupePrune | Self::DedupeRestore => {
                Ok(store.list_dedupe_shard_mappings()?.len())
            }
        }
    }
}

#[test]
fn rebuild_sigterm_preserves_derived_index_mutations_and_fresh_recovery() -> Result<()> {
    for mutation in [
        Mutation::ReconstructionPrune,
        Mutation::DedupePrune,
        Mutation::DedupeRestore,
    ] {
        verify_cut(mutation)?;
    }
    Ok(())
}

fn verify_cut(mutation: Mutation) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let root = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let backend = runtime.block_on(LocalBackend::new(
        root.path().to_path_buf(),
        "http://localhost:8080".into(),
        std::num::NonZeroUsize::new(128).ok_or("chunk size")?,
    ))?;
    runtime.block_on(backend.upload_file(
        "protected-file",
        b"protected payload".to_vec().into(),
        None,
    ))?;
    let store = LocalIndexStore::open(root.path().to_path_buf());
    mutation.seed(root.path(), &store, 0)?;
    mutation.seed(root.path(), &store, 1)?;
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
    let metadata = Connection::open(root.path().join("metadata.sqlite3"))?;
    metadata.busy_timeout(Duration::ZERO)?;
    let predicate = if matches!(mutation, Mutation::DedupeRestore) {
        ">= 1"
    } else {
        "<= 1"
    };
    metadata.execute_batch(&format!(
        "CREATE TRIGGER hold_derived_rebuild BEFORE {} ON {} WHEN (SELECT count(*) FROM {}) {} BEGIN
         SELECT (WITH RECURSIVE pause(counter) AS
           (VALUES(0) UNION ALL SELECT counter+1 FROM pause WHERE counter<1000000000)
           SELECT sum(counter) FROM pause); END;", mutation.trigger_action(), mutation.table(), mutation.table(), predicate
    ))?;
    let count_sql = format!("SELECT count(*) FROM {}", mutation.table());
    let mut child = StopOnDrop(
        Command::new(binary())
            .env_clear()
            .current_dir(root.path())
            .args(["index", "rebuild", "--root"])
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("stdout"))?)
            .stderr(fs::File::create(scratch.path().join("stderr"))?)
            .spawn()?,
    );
    wait_for(|| {
        if metadata
            .query_row(&count_sql, [], |row| row.get::<_, u32>(0))
            .ok()
            == Some(1)
        {
            if metadata.execute_batch("BEGIN IMMEDIATE").is_err() {
                return true;
            }
            let _rollback = metadata.execute_batch("ROLLBACK");
        }
        child.0.try_wait().ok().flatten().is_some()
    })?;
    assert!(
        child.0.try_wait()?.is_none(),
        "{}",
        String::from_utf8_lossy(&fs::read(scratch.path().join("stderr"))?)
    );
    let identities = maintenance_identities(root.path())?;
    assert_eq!(identities.len(), 1);
    let identity = identities.first().ok_or("missing rebuild identity")?;
    assert_eq!(identity.owner.pid, child.0.id());
    assert_eq!(identity.operation, "IndexRebuild");
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(child.0.wait()?.signal(), Some(15));
    metadata.execute_batch("DROP TRIGGER hold_derived_rebuild")?;
    assert_eq!(mutation.verify_count(&store)?, 1);
    assert_eq!(
        runtime.block_on(backend.download_file("protected-file", None, None))?,
        b"protected payload"
    );
    let workspace = root
        .path()
        .join(".shardline-maintenance")
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
    let inspection = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(inspection.status.success());
    let page: serde_json::Value = serde_json::from_slice(&inspection.stdout)?;
    assert_eq!(
        page.pointer("/tasks/0/state")
            .and_then(serde_json::Value::as_str),
        Some("needs_reconciliation")
    );
    assert_eq!(mutation.verify_count(&store)?, 1);
    mutation.seed(root.path(), &store, 2)?;
    let fresh = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["index", "rebuild", "--root"])
        .arg(root.path())
        .output()?;
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    let expected = if matches!(mutation, Mutation::DedupeRestore) {
        3
    } else {
        0
    };
    assert_eq!(mutation.verify_count(&store)?, expected);
    for (path, bytes) in retained_before {
        assert_eq!(fs::read(path)?, bytes);
    }
    if matches!(mutation, Mutation::DedupeRestore) {
        for ordinal in 0..=2 {
            let hash = parse_xet_hash_hex(
                &compute_data_hash(format!("retained shard chunk {ordinal}").as_bytes()).hex(),
            )?;
            let mapping = store
                .dedupe_shard_mapping(&hash)?
                .ok_or("missing restored mapping")?;
            assert_eq!(
                mapping.shard_object_key().as_str(),
                format!("shards/retained-{ordinal}")
            );
        }
    }
    assert_eq!(
        runtime.block_on(backend.download_file("protected-file", None, None))?,
        b"protected payload"
    );
    assert_eq!(fs::read(workspace.join("tasks.sqlite"))?, before);
    assert_eq!(
        fs::read(workspace.join("tasks.sqlite-wal")).ok(),
        before_wal
    );
    let refreshed = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(refreshed.status.success());
    let refreshed_page: serde_json::Value = serde_json::from_slice(&refreshed.stdout)?;
    let tasks = refreshed_page
        .get("tasks")
        .and_then(serde_json::Value::as_array)
        .ok_or("missing rebuilt tasks")?;
    assert_eq!(tasks.len(), 2);
    let interrupted = tasks
        .iter()
        .find(|task| {
            task.get("id").and_then(serde_json::Value::as_str) == Some(identity.id.as_str())
        })
        .ok_or("missing interrupted rebuild")?;
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
        1
    );
    Ok(())
}
