use std::{
    io::Error as IoError,
    net::{AddrParseError, SocketAddr},
    num::ParseIntError,
};

use thiserror::Error;

/// Server configuration loading failure.
#[derive(Debug, Error)]
pub enum ServerConfigError {
    /// The bind address could not be parsed.
    #[error("invalid bind address")]
    BindAddress(#[from] AddrParseError),
    /// The local deployment root contained an invalid filesystem component.
    #[error("invalid local deployment root")]
    RootDir(#[source] IoError),
    /// The server role token was invalid.
    #[error("invalid server role")]
    InvalidServerRole,
    /// The server frontend token was invalid.
    #[error("invalid server frontend")]
    InvalidServerFrontend,
    /// The configured server frontend set was empty.
    #[error("at least one server frontend must be enabled")]
    MissingServerFrontends,
    /// The object-storage adapter token was invalid.
    #[error("invalid object storage adapter")]
    InvalidObjectStorageAdapter,
    /// The auth provider token was invalid.
    #[error("invalid auth provider")]
    InvalidAuthProvider,
    /// S3 object storage was selected without a bucket.
    #[error("s3 object storage requires SHARDLINE_S3_BUCKET")]
    MissingS3Bucket,
    /// S3 object storage was selected with an invalid allow-http flag.
    #[error("invalid s3 allow-http flag")]
    InvalidS3AllowHttp,
    /// S3 object storage was selected with an invalid virtual-hosted-style flag.
    #[error("invalid s3 virtual-hosted-style request flag")]
    InvalidS3VirtualHostedStyleRequest,
    /// An S3 credential was provided through both direct env and file indirection.
    #[error("s3 credential source conflict: both {env} and {file_env} are set")]
    S3CredentialSourceConflict {
        /// Direct environment variable name.
        env: &'static str,
        /// File-indirection environment variable name.
        file_env: &'static str,
    },
    /// An S3 credential file could not be read.
    #[error("s3 credential file {name} could not be read")]
    S3CredentialFile {
        /// Credential file-indirection environment variable name.
        name: &'static str,
        /// Underlying filesystem failure.
        #[source]
        source: IoError,
    },
    /// An S3 credential file exceeded the bounded parser ceiling.
    #[error("s3 credential file {name} exceeded the bounded parser ceiling")]
    S3CredentialTooLarge {
        /// Credential file-indirection environment variable name.
        name: &'static str,
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// An S3 credential file changed after validation and was rejected.
    #[error("s3 credential file {name} changed during bounded read")]
    S3CredentialLengthMismatch {
        /// Credential file-indirection environment variable name.
        name: &'static str,
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// An S3 credential file was not valid UTF-8.
    #[error("s3 credential file {name} was not valid utf-8")]
    S3CredentialUtf8 {
        /// Credential file-indirection environment variable name.
        name: &'static str,
    },
    /// The chunk size could not be parsed.
    #[error("invalid chunk size")]
    ChunkSize(#[from] ParseIntError),
    /// The chunk size string could not be parsed.
    #[error("invalid chunk size: {0}")]
    ChunkSizeParse(String),
    /// The maximum request body size could not be parsed.
    #[error("invalid max request body size")]
    MaxRequestBodyBytes(ParseIntError),
    /// The maximum request body size was zero.
    #[error("max request body size must be greater than zero")]
    ZeroMaxRequestBodyBytes,
    /// The maximum shard file section count could not be parsed.
    #[error("invalid max shard file section count")]
    MaxShardFiles(ParseIntError),
    /// The maximum shard file section count was zero.
    #[error("max shard file section count must be greater than zero")]
    ZeroMaxShardFiles,
    /// The maximum shard xorb section count could not be parsed.
    #[error("invalid max shard xorb section count")]
    MaxShardXorbs(ParseIntError),
    /// The maximum shard xorb section count was zero.
    #[error("max shard xorb section count must be greater than zero")]
    ZeroMaxShardXorbs,
    /// The maximum shard reconstruction term count could not be parsed.
    #[error("invalid max shard reconstruction term count")]
    MaxShardReconstructionTerms(ParseIntError),
    /// The maximum shard reconstruction term count was zero.
    #[error("max shard reconstruction term count must be greater than zero")]
    ZeroMaxShardReconstructionTerms,
    /// The maximum shard xorb chunk record count could not be parsed.
    #[error("invalid max shard xorb chunk record count")]
    MaxShardXorbChunks(ParseIntError),
    /// The maximum shard xorb chunk record count was zero.
    #[error("max shard xorb chunk record count must be greater than zero")]
    ZeroMaxShardXorbChunks,
    /// The chunk size was zero.
    #[error("chunk size must be greater than zero")]
    ZeroChunkSize,
    /// The per-upload chunk processing window could not be parsed.
    #[error("invalid upload max in-flight chunks")]
    UploadMaxInFlightChunks(ParseIntError),
    /// The per-upload chunk processing window was zero.
    #[error("upload max in-flight chunks must be greater than zero")]
    ZeroUploadMaxInFlightChunks,
    /// The transfer concurrency budget could not be parsed.
    #[error("invalid transfer max in-flight chunks")]
    TransferMaxInFlightChunks(ParseIntError),
    /// The transfer concurrency budget was zero.
    #[error("transfer max in-flight chunks must be greater than zero")]
    ZeroTransferMaxInFlightChunks,
    /// The reconstruction-cache adapter token was invalid.
    #[error("invalid reconstruction cache adapter")]
    InvalidReconstructionCacheAdapter,
    /// The reconstruction-cache TTL could not be parsed.
    #[error("invalid reconstruction cache ttl")]
    ReconstructionCacheTtl(ParseIntError),
    /// The reconstruction-cache TTL was zero.
    #[error("reconstruction cache ttl must be greater than zero")]
    ZeroReconstructionCacheTtlSeconds,
    /// The in-memory reconstruction-cache capacity could not be parsed.
    #[error("invalid reconstruction cache memory max entries")]
    ReconstructionCacheMemoryMaxEntries(ParseIntError),
    /// The in-memory reconstruction-cache capacity was zero.
    #[error("reconstruction cache memory max entries must be greater than zero")]
    ZeroReconstructionCacheMemoryMaxEntries,
    /// The OCI upload-session TTL could not be parsed.
    #[error("invalid oci upload session ttl")]
    OciUploadSessionTtl(ParseIntError),
    /// The OCI upload-session TTL was zero.
    #[error("oci upload session ttl must be greater than zero")]
    ZeroOciUploadSessionTtlSeconds,
    /// The OCI upload live-session ceiling could not be parsed.
    #[error("invalid oci upload max active sessions")]
    OciUploadMaxActiveSessions(ParseIntError),
    /// The OCI upload live-session ceiling was zero.
    #[error("oci upload max active sessions must be greater than zero")]
    ZeroOciUploadMaxActiveSessions,
    /// The OCI registry token TTL could not be parsed.
    #[error("invalid oci registry token ttl")]
    OciRegistryTokenTtl(ParseIntError),
    /// The OCI registry token TTL was zero.
    #[error("oci registry token ttl must be greater than zero")]
    ZeroOciRegistryTokenTtlSeconds,
    /// The OCI registry token in-flight request ceiling could not be parsed.
    #[error("invalid oci registry token max in-flight requests")]
    OciRegistryTokenMaxInFlightRequests(ParseIntError),
    /// The OCI registry token in-flight request ceiling was zero.
    #[error("oci registry token max in-flight requests must be greater than zero")]
    ZeroOciRegistryTokenMaxInFlightRequests,
    /// The Redis reconstruction-cache URL was empty.
    #[error("reconstruction cache redis url must not be empty")]
    EmptyReconstructionCacheRedisUrl,
    /// Redis reconstruction-cache configuration was incomplete.
    #[error("redis reconstruction cache requires SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL")]
    MissingReconstructionCacheRedisUrl,
    /// A Redis TLS certificate, CA, or private key file could not be read.
    #[error("redis TLS material from {name} could not be read")]
    RedisTlsMaterial {
        /// Environment variable that supplied the file path.
        name: &'static str,
        /// Underlying secure file-read failure.
        #[source]
        source: IoError,
    },
    /// Redis TLS material exceeded the bounded parser ceiling.
    #[error("redis TLS material from {name} exceeded the bounded parser ceiling")]
    RedisTlsMaterialTooLarge {
        /// Environment variable that supplied the file path.
        name: &'static str,
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// Redis mTLS needs both halves of a client identity.
    #[error(
        "redis mTLS requires both SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_CERT_FILE and SHARDLINE_RECONSTRUCTION_CACHE_REDIS_TLS_CLIENT_KEY_FILE"
    )]
    IncompleteRedisTlsClientIdentity,
    /// The Postgres metadata URL was empty.
    #[error("postgres metadata url must not be empty")]
    EmptyIndexPostgresUrl,
    /// The token signing key file could not be read.
    #[error("token signing key could not be read")]
    TokenSigningKey(#[source] IoError),
    /// The token signing key exceeded the bounded parser ceiling.
    #[error("token signing key exceeded the bounded parser ceiling")]
    TokenSigningKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The token signing key changed after validation and was rejected.
    #[error("token signing key changed during bounded read")]
    TokenSigningKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The provider token TTL could not be parsed.
    #[error("invalid provider token ttl")]
    ProviderTokenTtl,
    /// The token signing key was empty.
    #[error("token signing key must not be empty")]
    EmptyTokenSigningKey,
    /// The Hub webhook secret key file could not be read.
    #[error("hub webhook secret key could not be read")]
    HubWebhookSecretKey(#[source] IoError),
    /// The Hub webhook secret key exceeded the bounded parser ceiling.
    #[error("hub webhook secret key exceeded the bounded parser ceiling")]
    HubWebhookSecretKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The Hub webhook secret key changed after validation and was rejected.
    #[error("hub webhook secret key changed during bounded read")]
    HubWebhookSecretKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The Hub webhook secret key was empty.
    #[error("hub webhook secret key must not be empty")]
    EmptyHubWebhookSecretKey,
    /// The Hub webhook secret key is not a valid AES-256 key length.
    #[error(
        "hub webhook secret key must be exactly {expected} bytes (a trailing newline is stripped automatically); got {observed}"
    )]
    HubWebhookSecretKeyLength {
        /// Required key length in bytes.
        expected: usize,
        /// Observed key length in bytes.
        observed: usize,
    },
    /// The provider-config secret key file could not be read.
    #[error("provider-config secret key could not be read")]
    ConfigSecretKey(#[source] IoError),
    /// The provider-config secret key exceeded the bounded parser ceiling.
    #[error("provider-config secret key exceeded the bounded parser ceiling")]
    ConfigSecretKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The provider-config secret key changed after validation and was rejected.
    #[error("provider-config secret key changed during bounded read")]
    ConfigSecretKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The provider-config secret key was empty.
    #[error("provider-config secret key must not be empty")]
    EmptyConfigSecretKey,
    /// The provider-config secret key is not a valid AES-256 key length.
    #[error(
        "provider-config secret key must be exactly {expected} bytes (a trailing newline is stripped automatically); got {observed}"
    )]
    ConfigSecretKeyLength {
        /// Required key length in bytes.
        expected: usize,
        /// Observed key length in bytes.
        observed: usize,
    },
    /// The metrics token file could not be read.
    #[error("metrics token could not be read")]
    MetricsToken(#[source] IoError),
    /// The metrics bearer token was empty.
    #[error("metrics token must not be empty")]
    EmptyMetricsToken,
    /// The metrics bearer token exceeded the bounded parser ceiling.
    #[error("metrics token exceeded the bounded parser ceiling")]
    MetricsTokenTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The metrics bearer token changed after validation and was rejected.
    #[error("metrics token changed during bounded read")]
    MetricsTokenLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The selected role uses the local HMAC provider and would expose CAS routes
    /// without bearer-token verification.
    #[error("served shardline routes require shardline token signing key configuration")]
    MissingTokenSigningKeyForServedRoutes,
    /// The provider bootstrap key was empty.
    #[error("provider bootstrap key must not be empty")]
    EmptyProviderApiKey,
    /// The provider bootstrap key file could not be read.
    #[error("provider bootstrap key could not be read")]
    ProviderApiKey(#[source] IoError),
    /// The provider bootstrap key exceeded the bounded parser ceiling.
    #[error("provider bootstrap key exceeded the bounded parser ceiling")]
    ProviderApiKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The provider bootstrap key changed after validation and was rejected.
    #[error("provider bootstrap key changed during bounded read")]
    ProviderApiKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The provider token issuer was empty.
    #[error("provider token issuer must not be empty")]
    EmptyProviderTokenIssuer,
    /// The provider token TTL was zero.
    #[error("provider token ttl must be greater than zero")]
    ZeroProviderTokenTtl,
    /// Provider token issuance was only partially configured.
    #[error("provider token issuance requires both provider config and provider api key files")]
    IncompleteProviderTokenConfig,
    /// Provider token issuance needs the CAS signing key.
    #[error("provider token issuance requires shardline token signing key configuration")]
    ProviderTokensRequireSigningKey,
    /// The chunk size exceeds the maximum allowed value.
    #[error("chunk size must not exceed 1 GB")]
    ChunkSizeTooLarge,
    /// The chunk size was not a power of two (the CDC chunker requires it).
    #[error("chunk size must be a power of two (e.g. 64KiB = 65536 bytes)")]
    ChunkSizeNotPowerOfTwo,
    /// The maximum S3 multipart part size could not be parsed.
    #[error("invalid s3 max part bytes")]
    S3MaxPartBytes(ParseIntError),
    /// The maximum S3 multipart part size was zero.
    #[error("s3 max part bytes must be greater than zero")]
    ZeroS3MaxPartBytes,
    /// The maximum S3 multipart part size was below the 1 MiB floor.
    #[error("s3 max part bytes must be at least {minimum_bytes} bytes")]
    S3MaxPartBytesTooSmall {
        /// The minimum accepted part size in bytes.
        minimum_bytes: u64,
    },
    /// The S3 multipart upload-session TTL could not be parsed.
    #[error("invalid s3 upload session ttl")]
    S3UploadSessionTtl(ParseIntError),
    /// The S3 multipart upload-session TTL was zero.
    #[error("s3 upload session ttl must be greater than zero")]
    ZeroS3UploadSessionTtlSeconds,
    /// The S3 multipart upload live-session ceiling could not be parsed.
    #[error("invalid s3 upload max active sessions")]
    S3UploadMaxActiveSessions(ParseIntError),
    /// The S3 multipart upload live-session ceiling was zero.
    #[error("s3 upload max active sessions must be greater than zero")]
    ZeroS3UploadMaxActiveSessions,
    /// The S3 multipart minimum part size could not be parsed.
    #[error("invalid s3 min part bytes")]
    S3MinPartBytes(ParseIntError),
    /// The S3 multipart minimum part size was zero.
    #[error("s3 min part bytes must be greater than zero")]
    ZeroS3MinPartBytes,
    /// The S3 multipart session byte quota could not be parsed.
    #[error("invalid s3 upload session max bytes")]
    S3UploadSessionMaxBytes(ParseIntError),
    /// The S3 multipart session byte quota was zero.
    #[error("s3 upload session max bytes must be greater than zero")]
    ZeroS3UploadSessionMaxBytes,
    /// The S3 multipart aggregate byte quota could not be parsed.
    #[error("invalid s3 upload total max bytes")]
    S3UploadTotalMaxBytes(ParseIntError),
    /// The S3 multipart aggregate byte quota was zero.
    #[error("s3 upload total max bytes must be greater than zero")]
    ZeroS3UploadTotalMaxBytes,
    /// The public base URL is not a valid URL.
    #[error("SHARDLINE_PUBLIC_BASE_URL is not a valid URL: {0}")]
    InvalidPublicBaseUrl(String),
    /// A configuration file could not be loaded.
    #[error("configuration file error: {0}")]
    ConfigFileError(String),
    /// Persistent secrets would be stored in plaintext in a production deployment mode.
    #[error(
        "production deployment mode requires at-rest secret encryption, but {surfaces}; \
         set SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION=true only if you accept \
         storing secrets unencrypted"
    )]
    PlaintextSecretsInProduction { surfaces: String },
    /// OIDC auth provider requires an issuer URL.
    #[error("oidc auth provider requires SHARDLINE_AUTH_OIDC_ISSUER")]
    MissingOidcIssuer,
    /// JWKS auth provider requires a JWKS URL.
    #[error("jwks auth provider requires SHARDLINE_AUTH_JWKS_URL")]
    MissingJwksUrl,
    /// Hub frontend requires an auth provider to be configured.
    #[error(
        "hub frontend requires auth configuration (SHARDLINE_AUTH_PROVIDER with token signing key or oidc/jwks)"
    )]
    HubRequiresAuth,
    /// Passthrough auth provider requires a loopback bind address.
    #[error("passthrough auth provider requires a loopback bind address, got {bind_addr}")]
    PassthroughProviderRequiresLoopbackBind {
        /// The rejected bind address.
        bind_addr: SocketAddr,
    },
    /// A secret was provided through both direct env and file indirection.
    #[error("secret source conflict: both {env} and {file_env} are set")]
    SecretSourceConflict {
        /// Direct environment variable name.
        env: &'static str,
        /// File-indirection environment variable name.
        file_env: &'static str,
    },
    /// Ed25519 auth provider requires a key.
    #[error("ed25519 auth provider requires exactly one private or public key")]
    MissingEd25519Key,
    /// Ed25519 signing and verification-only modes were configured together.
    #[error("ed25519 private and public keys must not both be configured")]
    ConflictingEd25519Keys,
    /// The Ed25519 private key file could not be read.
    #[error("ed25519 private key could not be read")]
    Ed25519PrivateKey(#[source] IoError),
    /// The Ed25519 public key file could not be read.
    #[error("ed25519 public key could not be read")]
    Ed25519PublicKey(#[source] IoError),
    /// The Ed25519 private key exceeded the bounded parser ceiling.
    #[error("ed25519 private key exceeded the bounded parser ceiling")]
    Ed25519PrivateKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The Ed25519 public key exceeded the bounded parser ceiling.
    #[error("ed25519 public key exceeded the bounded parser ceiling")]
    Ed25519PublicKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The Ed25519 private key changed after validation and was rejected.
    #[error("ed25519 private key changed during bounded read")]
    Ed25519PrivateKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The Ed25519 public key changed after validation and was rejected.
    #[error("ed25519 public key changed during bounded read")]
    Ed25519PublicKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The Ed25519 private key was empty.
    #[error("ed25519 private key must not be empty")]
    EmptyEd25519PrivateKey,
    /// The Ed25519 public key was empty.
    #[error("ed25519 public key must not be empty")]
    EmptyEd25519PublicKey,
}
