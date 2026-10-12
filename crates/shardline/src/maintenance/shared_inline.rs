//! Observation without requiring writable private task storage.
//! Deployment-backed receipts are evidence; domain fences authorize effects.
use super::{MaintenanceOperation, Result};
use ed25519_dalek::SigningKey;
use penelope_core::task::{
    BlobRef, Digest, TaskDefinition, TaskExecutionEvent, TaskGeneration, TaskProjection, TaskScope,
};
use shardline_reliability::{
    BootTaskClock, MaintenanceArtifact, MaintenanceArtifactKind, sign_maintenance_artifact,
    verify_maintenance_artifact,
};
use shardline_server::task_tracking::{
    TaskEvidenceStore, TaskExecutionMode, TaskOwner, TaskRunIdentity,
};
use std::{ffi::OsString, process::ExitCode};

pub(super) fn run(
    operation: &MaintenanceOperation,
    arguments: Vec<OsString>,
    config: &shardline_server::ServerConfig,
) -> Result<super::CommandOutput> {
    let mut nonce = [0_u8; 16];
    getrandom::fill(&mut nonce).map_err(|error| error.to_string())?;
    let id = hex::encode(nonce);
    let command = arguments
        .iter()
        .map(|argument| argument.as_os_str().as_encoded_bytes().to_vec())
        .collect();
    let (input, definition, scope) = super::inline::prepare(config, &id, operation, command)?;
    let admission = if operation.read_only() {
        Ok(None)
    } else {
        Receipt::begin(operation, &id, config, &input, definition, scope).map(Some)
    };
    let mut receipt = match admission {
        Ok(receipt) => receipt,
        Err(error) => {
            // Preserve the existing administrative command's availability. No
            // completion is claimed when shared observation cannot initialize.
            tracing::warn!(run = %id, "maintenance tracking unavailable; running the existing command under its domain barriers; no verified task outcome will be available: {error}");
            None
        }
    };
    let observer = receipt.as_ref().map(|receipt| {
        (
            receipt.store.clone(),
            receipt.identity.clone(),
            receipt.key.clone(),
        )
    });
    let status = tokio::runtime::Handle::current().block_on(async move {
        let work = crate::entry::run_direct(arguments.into_iter());
        if let Some((store, identity, key)) = observer {
            store.observe_history_scopes(identity, key, work).await
        } else {
            work.await
        }
    });
    let exit_code = if status == ExitCode::SUCCESS {
        0
    } else if status == ExitCode::FAILURE {
        1
    } else {
        2
    };
    let evidence_published = receipt.as_mut().is_some_and(|receipt| {
        match receipt.finish(operation.kind(), exit_code) {
            Ok(()) => true,
            Err(error) => {
                // The original operation already ran exactly once here. Never
                // repeat it to recover a missing acknowledgement or receipt.
                tracing::warn!(run = %id, "maintenance completed with exit {exit_code}, but shared completion is unconfirmed; inspect effects before retrying: {error}");
                false
            }
        }
    });
    Ok(super::CommandOutput {
        text: String::new(),
        success: exit_code == 0,
        capture: None,
        evidence_published,
        exit_code: Some(exit_code),
    })
}

struct Receipt {
    store: TaskEvidenceStore,
    identity: TaskRunIdentity,
    id: String,
    scope: TaskScope,
    key: SigningKey,
    projection: TaskProjection,
}

impl Receipt {
    fn begin(
        operation: &MaintenanceOperation,
        id: &str,
        config: &shardline_server::ServerConfig,
        input: &[u8],
        definition: TaskDefinition,
        scope: TaskScope,
    ) -> Result<Self> {
        let export_config = super::publication_config(operation, config)?;
        let store = super::evidence::store(operation, &export_config)?;
        let deployment = shardline_server::task_tracking::deployment_fingerprint(&export_config)?;
        if let Err(error) =
            store.prune_completed(deployment, shardline_protocol::unix_now_seconds_lossy())
        {
            tracing::warn!(
                "maintenance evidence retention unavailable; existing command continues: {error}"
            );
        }
        let mut seed = zeroize::Zeroizing::new([0_u8; 32]);
        getrandom::fill(seed.as_mut()).map_err(|error| error.to_string())?;
        let key = SigningKey::from_bytes(&seed);
        let identity = TaskRunIdentity {
            schema: 1,
            id: id.to_owned(),
            operation: format!("{:?}", operation.kind()),
            created_at: shardline_protocol::unix_now_seconds_lossy(),
            scope,
            definition,
            public_key: *key.verifying_key().as_bytes(),
            deployment,
            execution: TaskExecutionMode::Inline,
            owner: TaskOwner::current(),
        };
        store.publish(&format!("manifests/{id}"), &serde_json::to_vec(&identity)?)?;
        let mut receipt = Self {
            store,
            identity,
            id: id.to_owned(),
            scope,
            key,
            projection: TaskProjection::new(scope, definition)?,
        };
        receipt.commit(
            TaskExecutionEvent::IntentCommitted {
                definition,
                input: reference(input)?,
            },
            TaskGeneration::ZERO,
            Some(input),
        )?;
        receipt.commit(TaskExecutionEvent::Started, TaskGeneration::new(1), None)?;
        let proof = receipt.artifact(
            MaintenanceArtifactKind::Checkpoint,
            1,
            b"ordinary CLI admitted; no application work completed",
        )?;
        receipt.commit(
            TaskExecutionEvent::CheckpointCommitted {
                checkpoint: reference(&proof)?,
            },
            TaskGeneration::new(1),
            Some(&proof),
        )?;
        Ok(receipt)
    }

    fn commit(
        &mut self,
        event: TaskExecutionEvent,
        generation: TaskGeneration,
        bytes: Option<&[u8]>,
    ) -> Result<()> {
        let record = self
            .projection
            .propose(event, generation, BootTaskClock::current())?;
        if let Some(bytes) = bytes {
            self.store.publish(
                &format!(
                    "runs/{}/blobs/{}",
                    self.id,
                    hex::encode(Digest::of(bytes).0)
                ),
                bytes,
            )?;
        }
        self.store.publish(
            &format!("runs/{}/records/{:010}", self.id, record.sequence.get()),
            &serde_json::to_vec(&record)?,
        )?;
        self.projection.apply(&record)?;
        Ok(())
    }

    fn artifact(&self, kind: MaintenanceArtifactKind, step: u32, body: &[u8]) -> Result<Vec<u8>> {
        let key_id = super::artifact::key_id(&self.key.verifying_key());
        let proof = sign_maintenance_artifact(
            MaintenanceArtifact {
                schema_version: 1,
                scope: self.scope,
                generation: TaskGeneration::new(1),
                step,
                kind,
                digest: Digest::of(body),
                bytes: u64::try_from(body.len())?,
            },
            chrono::Utc::now(),
            &self.key,
            key_id.clone(),
        )?;
        let verified = verify_maintenance_artifact(
            &proof,
            self.scope,
            TaskGeneration::new(1),
            &self.key.verifying_key(),
            &key_id,
        )?;
        verified.verify_materialized(body)?;
        self.store.publish(
            &format!(
                "runs/{}/artifacts/{}",
                self.id,
                hex::encode(verified.digest.0)
            ),
            body,
        )?;
        Ok(proof.to_json()?)
    }

    fn finish(&mut self, kind: MaintenanceArtifactKind, exit_code: u8) -> Result<()> {
        let report = format!(
            "maintenance.success: {}\n{}",
            exit_code == 0,
            serde_json::to_string(&serde_json::json!({"schema": 1, "exit_code": exit_code}))?,
        );
        let proof = self.artifact(kind, 2, report.as_bytes())?;
        self.commit(
            TaskExecutionEvent::Completed {
                output: reference(&proof)?,
            },
            TaskGeneration::new(1),
            Some(&proof),
        )
    }
}

fn reference(bytes: &[u8]) -> Result<BlobRef> {
    if bytes.len() > 65536 {
        return Err("task evidence exceeds 64 KiB".into());
    }
    Ok(BlobRef {
        digest: Digest::of(bytes),
        bytes: u32::try_from(bytes.len())?,
        schema: 1,
    })
}

#[cfg(test)]
#[path = "shared_inline_tests.rs"]
mod tests;
