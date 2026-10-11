use ed25519_dalek::VerifyingKey;
use penelope_core::task::{
    BlobRef, TaskGeneration, TaskScope,
    runtime::{RuntimeProgress, TaskObservation, TaskPortError, TaskRuntime, TaskStart},
};
use shardline_reliability::{MaintenanceArtifactKind, ProcessTaskRuntime};
use std::path::PathBuf;

pub(super) struct VerifiedRuntime {
    pub inner: ProcessTaskRuntime,
    pub directory: PathBuf,
    pub key: VerifyingKey,
    pub kind: MaintenanceArtifactKind,
    pub capture: bool,
}

impl TaskRuntime for VerifiedRuntime {
    fn start(&mut self, request: TaskStart<'_>) -> Result<(), TaskPortError> {
        if let Some(checkpoint) = request.checkpoint {
            let proof = shardline_reliability::MaintenanceArtifactProof::from_json(checkpoint)
                .map_err(|_error| TaskPortError::Corrupt)?;
            super::artifact::verify(
                &self.directory,
                checkpoint,
                request.scope,
                proof.artifact.generation,
                MaintenanceArtifactKind::Checkpoint,
                &self.key,
            )
            .map_err(|_error| TaskPortError::Corrupt)?;
            if proof.artifact.generation >= request.generation {
                return Err(TaskPortError::Corrupt);
            }
        }
        self.inner.start(request)
    }
    fn poll(
        &mut self,
        scope: TaskScope,
        generation: TaskGeneration,
        buffer: &mut [u8],
    ) -> Result<TaskObservation, TaskPortError> {
        let observation = self.inner.poll(scope, generation, buffer)?;
        let proposal = match observation.progress {
            RuntimeProgress::Checkpoint { bytes, schema } => {
                Some((bytes, schema, MaintenanceArtifactKind::Checkpoint))
            }
            RuntimeProgress::Complete { bytes, schema } => Some((bytes, schema, self.kind)),
            RuntimeProgress::Pending | RuntimeProgress::Fault(_) => None,
        };
        if let Some((bytes, schema, kind)) = proposal {
            let length = usize::try_from(bytes).map_err(|_error| TaskPortError::Corrupt)?;
            let valid = schema == 1
                && buffer.get(..length).is_some_and(|body| {
                    super::artifact::verify(
                        &self.directory,
                        body,
                        scope,
                        generation,
                        kind,
                        &self.key,
                    )
                    .is_ok_and(|proof| {
                        !self.capture
                            || kind == MaintenanceArtifactKind::Checkpoint
                            || super::capture::verify(
                                &self.directory,
                                proof.artifact.digest,
                                generation,
                            )
                            .is_ok()
                    })
                });
            if !valid {
                self.inner.fence(scope, generation)?;
                return Err(TaskPortError::Corrupt);
            }
        }
        Ok(observation)
    }
    fn acknowledge_checkpoint(
        &mut self,
        scope: TaskScope,
        generation: TaskGeneration,
        checkpoint: BlobRef,
    ) -> Result<(), TaskPortError> {
        self.inner
            .acknowledge_checkpoint(scope, generation, checkpoint)
    }
    fn fence(&mut self, scope: TaskScope, generation: TaskGeneration) -> Result<(), TaskPortError> {
        self.inner.fence(scope, generation)
    }
}
