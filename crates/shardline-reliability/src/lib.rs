#![deny(unsafe_code)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]

//! Compatibility-preserving reliability primitives for Shardline.
//!
//! The implementation is split by responsibility; this root only declares
//! modules and preserves the public compatibility surface through re-exports.

mod digest;
mod error;
mod event;
mod operation;
mod session_evidence;
mod states;

#[cfg(test)]
mod tests;

pub use error::ReliabilityError;
pub use event::{
    LifecycleEvent, StateTransitionEvent, baseline_resumable_session_events,
    baseline_upload_lifecycle_events, resumable_session_event, upload_lifecycle_event,
    verify_lifecycle_chain, verify_lifecycle_chain_ends_at, verify_state_transition_chain,
    verify_state_transition_chain_ends_at,
};
pub use operation::{OperationIdentity, OperationKind};
pub use session_evidence::SessionEvidenceLog;
pub use session_evidence::verify_resumable_session_events;
pub use states::{ResumableLifecycleState, UploadLifecycleState};
