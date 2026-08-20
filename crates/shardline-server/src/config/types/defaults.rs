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

/// Default per-repo revision-registry cap (10k rows ≈ 250KB in SQLite).
///
/// `create_revision` rejects new revision names once a repository has reached
/// this many registered revisions, bounding the unbounded-registry growth
/// surface (F-75) even in providerless/permissive deployments where every
/// revision insert is a distinct row.
pub(crate) const DEFAULT_MAX_REVISIONS_PER_REPO: NonZeroUsize = match NonZeroUsize::new(10_000) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default per-repo tree-entry cap (100k rows ≈ 4MB+ in SQLite).
///
/// `register_path` rejects new path mappings once a repository has reached
/// this many tree-entry rows, bounding the unbounded tree-entry growth
/// surface (F-103) even in providerless/permissive deployments where one
/// valid file_id can be registered under arbitrarily many distinct paths.
/// Unlike the F-89 revision cap (which exempts a refresh of an existing
/// revision), the tree-entry cap rejects at capacity regardless of whether
/// the path already exists, mirroring `create_revision`'s count-before-insert
/// gate (F-108).
pub(crate) const DEFAULT_MAX_TREE_ENTRIES_PER_REPO: NonZeroUsize = match NonZeroUsize::new(100_000)
{
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
/// except the last one at `CompleteMultipartUpload` (matching S3, which never
/// validates part sizes at `UploadPart`).
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

/// Default global cap on the number of part FILES stored across all active S3
/// multipart upload sessions (200_000).
///
/// UploadPart accepts any body size (matching S3, which enforces the 5 MiB
/// minimum only at CompleteMultipartUpload), so the byte quotas alone do not
/// bound the part-file COUNT: with the 1 TiB per-session quota and a 5 MiB
/// minimum part, one fully-packed session holds ~200k part files, and the
/// 10_000-part protocol ceiling times the 1024-session cap would otherwise let
/// an attacker materialize ~10M tiny files per TTL window. This cap bounds the
/// file count directly, at roughly one fully-packed session's worth of
/// minimum-size parts (1 TiB / 5 MiB ≈ 200k).
pub(crate) const DEFAULT_S3_UPLOAD_MAX_ACTIVE_PART_FILES: NonZeroUsize =
    match NonZeroUsize::new(200_000) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

/// Default LFS chunked-patch (PATCH) staging TTL in seconds (1 hour, mirroring
/// the S3 multipart upload TTL).
pub(crate) const DEFAULT_LFS_PATCH_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(3_600) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

/// Default maximum number of concurrently active LFS chunked-patch sessions
/// (mirroring the S3 multipart active-session cap).
pub(crate) const DEFAULT_LFS_PATCH_MAX_ACTIVE_SESSIONS: NonZeroUsize =
    match NonZeroUsize::new(1_024) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

/// Default aggregate byte cap across active LFS chunked-patch sessions
/// (4 TiB, mirroring the S3 multipart aggregate quota).
pub(crate) const DEFAULT_LFS_PATCH_TOTAL_MAX_BYTES: NonZeroU64 =
    match NonZeroU64::new(4_398_046_511_104) {
        Some(value) => value,
        None => NonZeroU64::MIN,
    };

/// Default maximum distance an LFS chunked-patch (PATCH) `Content-Range` may
/// start ahead of the session's current high-water mark (64 MiB, matching the
/// default maximum request-body size).
///
/// The chunked-upload path only accepts sequential growth (mirroring the S3
/// multipart path): a chunk may begin at most this far beyond the last
/// recorded end-of-range. Together with allocated-block staging accounting this
/// closes the sparse-file write amplification hole, where a handful of 1-byte
/// writes at multi-TiB offsets would otherwise create huge logical staging
/// files while consuming almost no disk (F-30).
pub(crate) const DEFAULT_LFS_PATCH_MAX_SEEK_AHEAD_BYTES: NonZeroU64 =
    match NonZeroU64::new(67_108_864) {
        Some(value) => value,
        None => NonZeroU64::MIN,
    };
