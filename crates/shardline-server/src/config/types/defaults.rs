use std::num::{NonZeroU64, NonZeroUsize};

pub(crate) const MIN_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize =
    match NonZeroUsize::new(64) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const MAX_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize =
    match NonZeroUsize::new(1024) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize = match NonZeroUsize::new(64)
{
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize =
    match NonZeroUsize::new(256) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const DEFAULT_MAX_REQUEST_BODY_BYTES: NonZeroUsize = match NonZeroUsize::new(67_108_864)
{
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_MAX_SHARD_FILES: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_MAX_SHARD_XORBS: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS: NonZeroUsize =
    match NonZeroUsize::new(65_536) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const DEFAULT_MAX_SHARD_XORB_CHUNKS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const MAX_TOKEN_SIGNING_KEY_BYTES: u64 = 1_048_576;
pub(crate) const MAX_ED25519_KEY_BYTES: u64 = 16_384;
/// AES-256 webhook secret encryption keys are exactly 32 bytes.
pub(crate) const HUB_WEBHOOK_SECRET_KEY_BYTES: u64 = 32;
/// AES-256 provider-config secret encryption keys are exactly 32 bytes.
pub(crate) const CONFIG_SECRET_KEY_BYTES: u64 = 32;
pub(crate) const MAX_PROVIDER_API_KEY_BYTES: u64 = 4096;
pub(crate) const MAX_METRICS_TOKEN_BYTES: u64 = 4096;
pub(crate) const MAX_REDIS_TLS_MATERIAL_BYTES: u64 = 1_048_576;
pub(crate) const MAX_S3_CREDENTIAL_BYTES: u64 = 4096;

pub(crate) const DEFAULT_PARALLELISM_FALLBACK: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_OCI_UPLOAD_SESSION_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(3_600) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

pub(crate) const DEFAULT_OCI_UPLOAD_MAX_ACTIVE_SESSIONS: NonZeroUsize =
    match NonZeroUsize::new(1_024) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const DEFAULT_OCI_REGISTRY_TOKEN_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(300) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

pub(crate) const DEFAULT_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS: NonZeroUsize =
    match NonZeroUsize::new(64) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

/// S3 multipart part sizes below this value are rejected (1 MiB).
pub(crate) const MIN_S3_MAX_PART_BYTES: u64 = 1_048_576;

/// Default maximum S3 multipart part size in bytes (1 GiB).
pub(crate) const DEFAULT_S3_MAX_PART_BYTES: NonZeroU64 = match NonZeroU64::new(1_073_741_824) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

/// Default S3 multipart upload session TTL in seconds (1 hour).
pub(crate) const DEFAULT_S3_UPLOAD_SESSION_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(3_600) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

/// Default maximum number of concurrently active S3 multipart upload sessions.
pub(crate) const DEFAULT_S3_UPLOAD_MAX_ACTIVE_SESSIONS: NonZeroUsize =
    match NonZeroUsize::new(1_024) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

/// S3's minimum multipart part size in bytes (5 MiB), enforced for every part
/// except the final one.
pub(crate) const DEFAULT_S3_MIN_PART_BYTES: NonZeroU64 = match NonZeroU64::new(5_242_880) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

/// Default per-session multipart byte quota (1 TiB).
pub(crate) const DEFAULT_S3_UPLOAD_SESSION_MAX_BYTES: NonZeroU64 =
    match NonZeroU64::new(1_099_511_627_776) {
        Some(value) => value,
        None => NonZeroU64::MIN,
    };

/// Default aggregate multipart byte quota across active sessions (4 TiB).
pub(crate) const DEFAULT_S3_UPLOAD_TOTAL_MAX_BYTES: NonZeroU64 =
    match NonZeroU64::new(4_398_046_511_104) {
        Some(value) => value,
        None => NonZeroU64::MIN,
    };
