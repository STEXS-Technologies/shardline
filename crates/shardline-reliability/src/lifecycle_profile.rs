//! Native deterministic semantics; existing evidence encodings remain independent.
use serde::{Deserialize, Serialize};
use statechronicle_domain::intent::Operation;
use statechronicle_profile::StateProfile;
use statechronicle_profiles::{
    custom::TypedProfile,
    registry::{ProfileRegistry, ProfileRules},
};
use std::sync::LazyLock;

use crate::{
    ReliabilityError, ResumableLifecycleState, UploadLifecycleState, states::EvidenceState,
};

/// A command binds its expected state as well as the requested transition.
/// Initialization cannot import an arbitrary terminal state into a fresh resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleCommand<S> {
    Initialize,
    Transition { expected: S, next: S },
}

/// StateChronicle profile for content-addressed upload intent transitions.
#[derive(Debug, Clone, Copy, Default)]
pub struct UploadLifecycleProfile;

/// StateChronicle profile for resumable session transitions, including explicit reuse.
#[derive(Debug, Clone, Copy, Default)]
pub struct ResumableLifecycleProfile;

static UPLOAD_OPERATIONS: LazyLock<[Operation; 2]> = LazyLock::new(|| {
    [
        Operation("shardline.upload.initialize".to_owned()),
        Operation("shardline.upload.transition".to_owned()),
    ]
});
static SESSION_OPERATIONS: LazyLock<[Operation; 2]> = LazyLock::new(|| {
    [
        Operation("shardline.resumable.initialize".to_owned()),
        Operation("shardline.resumable.transition".to_owned()),
    ]
});
static UPLOAD: LazyLock<TypedProfile<UploadLifecycleProfile>> =
    LazyLock::new(|| TypedProfile::new(UploadLifecycleProfile, &*UPLOAD_OPERATIONS));
static RESUMABLE: LazyLock<TypedProfile<ResumableLifecycleProfile>> =
    LazyLock::new(|| TypedProfile::new(ResumableLifecycleProfile, &*SESSION_OPERATIONS));
static PROFILES: LazyLock<[&'static dyn ProfileRules; 2]> =
    LazyLock::new(|| [&*UPLOAD, &*RESUMABLE]);

/// Native adapter for explicit versioned upload resources. Authority is required.
#[must_use]
pub fn native_upload_profile() -> &'static TypedProfile<UploadLifecycleProfile> {
    &UPLOAD
}

/// Native adapter for explicit versioned resumable resources. Authority is required.
#[must_use]
pub fn native_resumable_profile() -> &'static TypedProfile<ResumableLifecycleProfile> {
    &RESUMABLE
}

/// Registers both native domains with the standard StateChronicle pipeline.
/// This does not replace the historical Merkle profile or activate new writes.
///
/// # Errors
/// Returns registration failures if domain/schema/operation rules are invalid.
pub fn native_lifecycle_registry()
-> Result<ProfileRegistry, statechronicle_profiles::error::ProfileError> {
    ProfileRegistry::with_custom(&*PROFILES)
}

fn reduce<S: EvidenceState>(
    current: Option<&S>,
    command: &LifecycleCommand<S>,
    initial: S,
) -> Result<S, ReliabilityError> {
    match (current, command) {
        (None, LifecycleCommand::Initialize) => Ok(initial),
        (Some(current), LifecycleCommand::Transition { expected, next }) => {
            if current != expected {
                return Err(ReliabilityError::StateMismatch);
            }
            if !current.can_transition_to(*next) {
                return Err(ReliabilityError::InvalidTransition {
                    before: current.as_str(),
                    after: next.as_str(),
                });
            }
            Ok(*next)
        }
        _ => Err(ReliabilityError::StateMismatch),
    }
}

macro_rules! lifecycle_profile {
    ($profile:ty, $state:ty, $domain:literal, $initial:expr, $initialize:literal, $transition:literal) => {
        impl StateProfile for $profile {
            type State = $state;
            type Command = LifecycleCommand<$state>;
            type Error = ReliabilityError;
            const DOMAIN_ID: &'static str = $domain;
            const SCHEMA_VERSION: u32 = 1;
            // Only enum discriminants; complete multipart state stays in digest snapshots.
            const MAX_PAYLOAD_BYTES: usize = 32;

            fn operation(&self, command: &Self::Command) -> &'static str {
                match command {
                    LifecycleCommand::Initialize => $initialize,
                    LifecycleCommand::Transition { .. } => $transition,
                }
            }

            fn validate(
                &self,
                current: Option<&Self::State>,
                command: &Self::Command,
            ) -> Result<(), Self::Error> {
                reduce(current, command, $initial).map(|_| ())
            }

            fn apply(
                &self,
                current: Option<&Self::State>,
                command: &Self::Command,
            ) -> Result<Self::State, Self::Error> {
                reduce(current, command, $initial)
            }

            fn validate_state(&self, _state: &Self::State) -> Result<(), Self::Error> {
                // The closed enum's decoder admits only valid lifecycle states.
                Ok(())
            }
        }
    };
}

lifecycle_profile!(
    UploadLifecycleProfile,
    UploadLifecycleState,
    "shardline.upload-lifecycle",
    UploadLifecycleState::Created,
    "shardline.upload.initialize",
    "shardline.upload.transition"
);
lifecycle_profile!(
    ResumableLifecycleProfile,
    ResumableLifecycleState,
    "shardline.resumable-lifecycle",
    ResumableLifecycleState::Active,
    "shardline.resumable.initialize",
    "shardline.resumable.transition"
);
