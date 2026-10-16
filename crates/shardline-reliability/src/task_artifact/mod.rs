//! Native immutable task artifact proofs, independent of historical evidence formats.
mod checkpoint;
mod json;
mod profile;
mod proof;

pub use checkpoint::CheckpointArtifactVerifier;
pub use profile::{MaintenanceArtifact, MaintenanceArtifactKind};
pub use proof::{MaintenanceArtifactProof, sign_maintenance_artifact, verify_maintenance_artifact};
