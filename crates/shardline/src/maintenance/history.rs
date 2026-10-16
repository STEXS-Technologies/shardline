use super::{Result, artifact};
use ed25519_dalek::VerifyingKey;
use penelope::core::task::{
    TaskDefinition, TaskExecutionEvent, TaskScope, TaskSequence, runtime::TaskJournal,
};
use penelope::statechronicle::task::CheckpointStateVerifier;
use shardline_reliability::{
    CheckpointArtifactVerifier, MaintenanceArtifactKind, SqliteTaskJournal,
};
use std::path::Path;

pub(super) fn verify(
    path: &Path,
    directory: &Path,
    scope: TaskScope,
    definition: TaskDefinition,
    key: &VerifyingKey,
) -> Result<()> {
    let mut journal = SqliteTaskJournal::open_read_only(path, scope, definition)?;
    let key_id = artifact::key_id(key);
    let mut buffer = vec![0; 65536];
    for index in 0..definition.budget.max_history_records {
        let Some(record) = journal.read(scope, TaskSequence::new(index))? else {
            return Ok(());
        };
        if let TaskExecutionEvent::CheckpointCommitted { checkpoint }
        | TaskExecutionEvent::CheckpointMigrated { checkpoint, .. } = record.event
        {
            let count = journal.read_blob(scope, checkpoint, &mut buffer)?;
            let bytes = buffer.get(..count).ok_or("invalid checkpoint length")?;
            CheckpointArtifactVerifier {
                bytes,
                key,
                key_id: &key_id,
            }
            .verify(&record)?;
            artifact::verify(
                directory,
                bytes,
                scope,
                record.generation,
                MaintenanceArtifactKind::Checkpoint,
                key,
            )?;
        }
    }
    Ok(())
}
