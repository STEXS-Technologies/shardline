use super::super::*;
use shardline_index::{
    LifecycleStore, LocalIndexStore, QuarantineCandidate, RetentionHold, WebhookDelivery,
};
use shardline_protocol::RepositoryProvider;
use shardline_storage::ObjectKey;

struct StopOnDrop(std::process::Child);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[derive(Clone, Copy)]
enum Mutation {
    Hold,
    Quarantine,
    Webhook,
}
impl Mutation {
    const fn table(self) -> &'static str {
        match self {
            Self::Hold => "shardline_retention_holds",
            Self::Quarantine => "shardline_quarantine_candidates",
            Self::Webhook => "shardline_webhook_deliveries",
        }
    }
    const fn condition(self) -> &'static str {
        match self {
            Self::Hold | Self::Quarantine => "OLD.object_key = 'chunks/repair-cut/1'",
            Self::Webhook => "OLD.delivery_id = '1'",
        }
    }
    fn seed(self, store: &LocalIndexStore, ordinal: u32) -> Result<()> {
        let key = ObjectKey::parse(&format!("chunks/repair-cut/{ordinal}"))?;
        match self {
            Self::Hold => store.upsert_retention_hold(&RetentionHold::new(
                key,
                "expired fixture".into(),
                0,
                Some(1),
            )?)?,
            Self::Quarantine => {
                store.upsert_quarantine_candidate(&QuarantineCandidate::new(key, 1, 0, 1)?)?
            }
            Self::Webhook => {
                store.record_webhook_delivery(&WebhookDelivery::new(
                    RepositoryProvider::GitHub,
                    "owner".into(),
                    "repo".into(),
                    ordinal.to_string(),
                    1,
                )?)?;
            }
        }
        Ok(())
    }
}

#[test]
fn repair_sigterm_preserves_committed_mutations_and_fresh_recovery() -> Result<()> {
    for arguments in [
        vec!["repair", "lifecycle", "--root"],
        vec!["repair", "--root"],
    ] {
        for mutation in [Mutation::Hold, Mutation::Quarantine, Mutation::Webhook] {
            verify_mutation_cut(&arguments, mutation)?;
        }
    }
    Ok(())
}

fn verify_mutation_cut(arguments: &[&str], mutation: Mutation) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let root = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    let store = LocalIndexStore::new(root.path().to_path_buf())?;
    fs::create_dir_all(root.path().join("chunks/preserved"))?;
    let payload = root.path().join("chunks/preserved/sentinel");
    fs::write(&payload, b"preserve user bytes")?;
    let permanent = RetentionHold::new(
        ObjectKey::parse("chunks/preserved/sentinel")?,
        "permanent protection".into(),
        0,
        None,
    )?;
    store.upsert_retention_hold(&permanent)?;
    mutation.seed(&store, 0)?;
    mutation.seed(&store, 1)?;
    let metadata = Connection::open(root.path().join("metadata.sqlite3"))?;
    metadata.busy_timeout(Duration::ZERO)?;
    metadata.execute_batch(&format!(
        "CREATE TRIGGER hold_repair_mutation BEFORE DELETE ON {} WHEN {} BEGIN
         SELECT (WITH RECURSIVE pause(counter) AS
           (VALUES(0) UNION ALL SELECT counter+1 FROM pause WHERE counter<1000000000)
           SELECT sum(counter) FROM pause); END;",
        mutation.table(),
        mutation.condition()
    ))?;
    let mut child = StopOnDrop(
        Command::new(binary())
            .env_clear()
            .current_dir(root.path())
            .args(arguments)
            .arg(root.path())
            .stdout(fs::File::create(scratch.path().join("stdout"))?)
            .stderr(fs::File::create(scratch.path().join("stderr"))?)
            .spawn()?,
    );
    let count_sql = format!("SELECT count(*) FROM {}", mutation.table());
    let expected = if matches!(mutation, Mutation::Hold) {
        2
    } else {
        1
    };
    wait_for(|| {
        let observed = metadata
            .query_row(&count_sql, [], |row| row.get::<_, u32>(0))
            .ok()
            == Some(expected);
        if observed {
            if metadata.execute_batch("BEGIN IMMEDIATE").is_err() {
                return true;
            }
            let _rollback = metadata.execute_batch("ROLLBACK");
        }
        child.0.try_wait().ok().flatten().is_some()
    })?;
    assert!(child.0.try_wait()?.is_none());
    let identities = maintenance_identities(root.path())?;
    assert_eq!(identities.len(), 1);
    let identity = identities.first().ok_or("missing repair identity")?;
    assert_eq!(identity.owner.pid, child.0.id());
    assert_eq!(identity.operation, "LifecycleRepair");
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()?
            .success()
    );
    assert_eq!(child.0.wait()?.signal(), Some(15));
    metadata.execute_batch("DROP TRIGGER hold_repair_mutation")?;
    // The first deletion committed; the interrupted second transaction rolled back.
    assert_eq!(
        metadata.query_row(&count_sql, [], |row| row.get::<_, u32>(0))?,
        expected
    );
    // Public readers verify the surviving rows' reliability evidence as well
    // as their domain values after the interrupted transaction rolls back.
    let verified_remaining = match mutation {
        Mutation::Hold => store.list_retention_holds()?.len(),
        Mutation::Quarantine => store.list_quarantine_candidates()?.len(),
        Mutation::Webhook => store.list_webhook_deliveries()?.len(),
    };
    assert_eq!(verified_remaining, usize::try_from(expected)?);
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
    let inspected = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--root"])
        .arg(root.path())
        .output()?;
    assert!(inspected.status.success());
    let page: serde_json::Value = serde_json::from_slice(&inspected.stdout)?;
    assert_eq!(
        page.pointer("/tasks/0/state")
            .and_then(serde_json::Value::as_str),
        Some("needs_reconciliation")
    );
    assert_eq!(
        metadata.query_row(&count_sql, [], |row| row.get::<_, u32>(0))?,
        expected
    );
    mutation.seed(&store, 2)?;
    let fresh = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(arguments)
        .arg(root.path())
        .output()?;
    assert!(
        fresh.status.success(),
        "{}",
        String::from_utf8_lossy(&fresh.stderr)
    );
    let remaining = if matches!(mutation, Mutation::Hold) {
        1
    } else {
        0
    };
    assert_eq!(
        metadata.query_row(&count_sql, [], |row| row.get::<_, u32>(0))?,
        remaining
    );
    assert_eq!(store.list_retention_holds()?, vec![permanent]);
    assert_eq!(fs::read(&payload)?, b"preserve user bytes");
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
        .ok_or("missing repair tasks")?;
    assert_eq!(tasks.len(), 2);
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
        1
    );
    Ok(())
}
