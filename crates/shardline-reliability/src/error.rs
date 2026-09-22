use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReliabilityError {
    #[error("operation identity field is empty: {0}")]
    EmptyField(&'static str),
    #[error("content SHA-256 must be exactly 64 hexadecimal characters")]
    InvalidSha256,
    #[error("invalid lifecycle transition: {before} -> {after}")]
    InvalidTransition {
        before: &'static str,
        after: &'static str,
    },
    #[error("could not canonicalize reliability identity: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("state digest does not match the lifecycle boundary")]
    StateDigestMismatch,
    #[error("process digest does not match the lifecycle boundary")]
    ProcessDigestMismatch,
    #[error("lifecycle events belong to different operations")]
    OperationMismatch,
    #[error("lifecycle event sequence regressed")]
    SequenceRegression,
    #[error("lifecycle event chain is discontinuous")]
    ChainDiscontinuity,
}
