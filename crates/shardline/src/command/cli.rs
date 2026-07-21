use std::{fmt, path::PathBuf};

use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{
    DatabaseMigrationCommand, ObjectStorageAdapter, ServerFrontend, ServerRole,
};

use crate::bench::{BenchDeploymentTarget, BenchScenario};

use super::definition::{BenchMode, CompletionShell};

/// Wrapper that redacts sensitive database URLs in Debug output.
#[derive(Clone, PartialEq, Eq)]
pub struct RedactedDbUrl(pub(crate) String);

impl RedactedDbUrl {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RedactedDbUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

/// Supported Shardline CLI commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    /// Bootstrap a providerless local source-checkout deployment.
    ProviderlessSetup,
    /// Run the server.
    Serve {
        /// Optional role override for the current process.
        role: Option<ServerRole>,
        /// Optional protocol frontend override for the current process.
        frontends: Option<Vec<ServerFrontend>>,
    },
    /// Validate configuration.
    ConfigCheck,
    /// Manage the Postgres metadata schema.
    DbMigrate {
        /// Optional explicit Postgres metadata URL override.
        database_url: Option<RedactedDbUrl>,
        /// Requested migration action.
        command: DatabaseMigrationCommand,
    },
    /// Manage local administrative tokens.
    AdminToken {
        /// Issuer embedded in the signed token.
        issuer: String,
        /// Subject embedded in the signed token.
        subject: String,
        /// Granted CAS scope.
        scope: TokenScope,
        /// Repository hosting provider.
        provider: RepositoryProvider,
        /// Repository owner or namespace.
        owner: String,
        /// Repository name.
        repo: String,
        /// Scoped revision when one is required.
        revision: Option<String>,
        /// Token lifetime in seconds.
        ttl_seconds: u64,
        /// Local signing-key file path.
        key_file: Option<PathBuf>,
        /// Environment variable that stores the signing key.
        key_env: Option<String>,
    },
    /// Verify object and index integrity.
    Fsck {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
    },
    /// Rebuild latest-record state from immutable version records.
    IndexRebuild {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
    },
    /// Run garbage collection.
    Gc {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// Persist currently orphaned chunks into durable quarantine state.
        mark: bool,
        /// Delete orphan chunks after reporting them.
        sweep: bool,
        /// Retention window applied to newly quarantined chunks.
        retention_seconds: u64,
        /// Optional JSON file that receives active quarantine state after the run.
        retention_report: Option<PathBuf>,
        /// Optional JSON file that receives current orphan inventory after the run.
        orphan_inventory: Option<PathBuf>,
    },
    /// Install scheduled garbage-collection systemd units.
    GcScheduleInstall {
        /// Directory that receives the generated systemd units.
        output_dir: PathBuf,
        /// Unit basename without `.service` or `.timer`.
        unit_prefix: String,
        /// `systemd.timer` calendar expression.
        calendar: String,
        /// Retention window passed to the scheduled collector.
        retention_seconds: u64,
        /// Path to the `shardline` binary in the generated unit.
        binary_path: PathBuf,
        /// Environment file referenced by the generated unit.
        env_file: PathBuf,
        /// Working directory and writable state path.
        working_directory: PathBuf,
        /// Service user.
        user: String,
        /// Service group.
        group: String,
        /// Print the generated unit files to stdout instead of writing them.
        dry_run: bool,
    },
    /// Remove scheduled garbage-collection systemd units.
    GcScheduleUninstall {
        /// Directory that contains the generated systemd units.
        output_dir: PathBuf,
        /// Unit basename without `.service` or `.timer`.
        unit_prefix: String,
    },
    /// Repair stale lifecycle metadata.
    Repair {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// Retention applied to processed webhook delivery claims.
        webhook_retention_seconds: u64,
    },
    /// Repair stale lifecycle metadata.
    RepairLifecycle {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// Retention applied to processed webhook delivery claims.
        webhook_retention_seconds: u64,
    },
    /// Export an adapter-neutral backup manifest.
    BackupManifest {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// JSON manifest output path.
        output: PathBuf,
    },
    /// Copy immutable payload objects between object-storage adapters.
    StorageMigrate {
        /// Source object-storage adapter.
        from: ObjectStorageAdapter,
        /// Optional source local state root.
        from_root: Option<PathBuf>,
        /// Destination object-storage adapter.
        to: ObjectStorageAdapter,
        /// Optional destination local state root.
        to_root: Option<PathBuf>,
        /// Object-key prefix to migrate.
        prefix: String,
        /// Whether to inventory without writing destination objects.
        dry_run: bool,
    },
    /// Create or update a retention hold.
    HoldSet {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// Object-store key protected by the hold.
        object_key: String,
        /// Operator-supplied hold reason.
        reason: String,
        /// Optional time-to-live in seconds.
        ttl_seconds: Option<u64>,
    },
    /// List retention holds.
    HoldList {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// Whether to exclude expired holds.
        active_only: bool,
    },
    /// Release one retention hold.
    HoldRelease {
        /// Optional deployment-root override for the active Shardline config.
        root: Option<PathBuf>,
        /// Object-store key released from protection.
        object_key: String,
    },
    /// Run storage and protocol benchmarks.
    Bench {
        mode: BenchMode,
        /// End-to-end benchmark deployment target.
        deployment_target: BenchDeploymentTarget,
        /// Focused benchmark scenario.
        scenario: BenchScenario,
        /// Root directory used to create isolated benchmark iteration stores.
        storage_dir: Option<PathBuf>,
        /// Number of benchmark iterations to run.
        iterations: u32,
        /// Number of concurrent workers used for concurrent sub-scenarios.
        concurrency: u32,
        /// Maximum upload chunks processed in parallel per upload.
        upload_max_in_flight_chunks: usize,
        /// Chunk size in bytes used by the local benchmark backend.
        chunk_size_bytes: usize,
        /// Logical size of the benchmark asset in bytes.
        base_bytes: usize,
        /// Number of bytes changed in the sparse-update benchmark step.
        mutated_bytes: usize,
        /// Whether to emit the full report as JSON.
        json: bool,
    },
    /// Check server health.
    Health {
        /// Shardline server base URL.
        server_url: String,
    },
    /// Generate shell-completion scripts.
    Completion {
        /// Target shell.
        shell: CompletionShell,
        /// Optional output path. Defaults to stdout.
        output: Option<PathBuf>,
    },
    /// Generate one manpage for the CLI.
    Manpage {
        /// Optional output path. Defaults to stdout.
        output: Option<PathBuf>,
    },
}
