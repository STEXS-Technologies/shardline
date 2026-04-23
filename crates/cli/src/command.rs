use std::{ffi::OsString, fmt, num::NonZeroUsize, path::PathBuf};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
    DatabaseMigrationCommand, ObjectStorageAdapter, ServerFrontend, ServerRole,
};
use thiserror::Error;

use crate::bench::{BenchDeploymentTarget, BenchScenario};

const CLI_AFTER_LONG_HELP: &str = "\
Examples:
  shardline providerless setup
  shardline serve --role all
  shardline config check
  shardline gc --mark --sweep --retention-seconds 86400
  shardline gc schedule install --env-file /etc/shardline/shardline.env --user shardline --group shardline
  shardline storage migrate --from local --from-root /srv/assets/.shardline/data --to s3 --prefix xorbs/default/
  shardline bench --mode ingest --iterations 5 --concurrency 16
  shardline completion bash > /usr/share/bash-completion/completions/shardline
  shardline manpage --output ./shardline.1";

const GC_INSTALL_AFTER_LONG_HELP: &str = "\
Examples:
  shardline gc schedule install --output-dir ./systemd --env-file /etc/shardline/shardline.env --user shardline --group shardline
  shardline gc schedule install --calendar 'hourly' --retention-seconds 600 --binary-path /usr/local/bin/shardline";

const BENCH_AFTER_LONG_HELP: &str = "\
Examples:
  shardline bench --storage-dir /var/lib/shardline-bench
  shardline bench --storage-dir /var/lib/shardline-bench --deployment-target configured
  shardline bench --storage-dir /var/lib/shardline-bench --scenario cross-repository-upload --iterations 5
  shardline bench --mode ingest --iterations 10 --concurrency 32 --chunk-size-bytes 1048576";

const COMPLETION_AFTER_HELP: &str = "\
Examples:
  shardline completion bash
  shardline completion zsh --output ./_shardline
  shardline completion fish --output ~/.config/fish/completions/shardline.fish";

const MANPAGE_AFTER_HELP: &str = "\
Examples:
  shardline manpage
  shardline manpage --output ./shardline.1";

/// Supported Shardline CLI commands.
#[derive(Clone, PartialEq, Eq)]
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
        database_url: Option<String>,
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
        /// Benchmark mode.
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

impl fmt::Debug for CliCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProviderlessSetup => formatter.write_str("ProviderlessSetup"),
            Self::Serve { role, frontends } => formatter
                .debug_struct("Serve")
                .field("role", role)
                .field("frontends", frontends)
                .finish(),
            Self::ConfigCheck => formatter.write_str("ConfigCheck"),
            Self::DbMigrate {
                database_url,
                command,
            } => formatter
                .debug_struct("DbMigrate")
                .field("database_url", &database_url.as_ref().map(|_url| "***"))
                .field("command", command)
                .finish(),
            Self::AdminToken {
                issuer,
                subject,
                scope,
                provider,
                owner,
                repo,
                revision,
                ttl_seconds,
                key_file,
                key_env,
            } => formatter
                .debug_struct("AdminToken")
                .field("issuer", issuer)
                .field("subject", subject)
                .field("scope", scope)
                .field("provider", provider)
                .field("owner", owner)
                .field("repo", repo)
                .field("revision", revision)
                .field("ttl_seconds", ttl_seconds)
                .field("key_file", key_file)
                .field("key_env", key_env)
                .finish(),
            Self::Fsck { root } => formatter.debug_struct("Fsck").field("root", root).finish(),
            Self::IndexRebuild { root } => formatter
                .debug_struct("IndexRebuild")
                .field("root", root)
                .finish(),
            Self::Gc {
                root,
                mark,
                sweep,
                retention_seconds,
                retention_report,
                orphan_inventory,
            } => formatter
                .debug_struct("Gc")
                .field("root", root)
                .field("mark", mark)
                .field("sweep", sweep)
                .field("retention_seconds", retention_seconds)
                .field("retention_report", retention_report)
                .field("orphan_inventory", orphan_inventory)
                .finish(),
            Self::GcScheduleInstall {
                output_dir,
                unit_prefix,
                calendar,
                retention_seconds,
                binary_path,
                env_file,
                working_directory,
                user,
                group,
            } => formatter
                .debug_struct("GcScheduleInstall")
                .field("output_dir", output_dir)
                .field("unit_prefix", unit_prefix)
                .field("calendar", calendar)
                .field("retention_seconds", retention_seconds)
                .field("binary_path", binary_path)
                .field("env_file", env_file)
                .field("working_directory", working_directory)
                .field("user", user)
                .field("group", group)
                .finish(),
            Self::GcScheduleUninstall {
                output_dir,
                unit_prefix,
            } => formatter
                .debug_struct("GcScheduleUninstall")
                .field("output_dir", output_dir)
                .field("unit_prefix", unit_prefix)
                .finish(),
            Self::Repair {
                root,
                webhook_retention_seconds,
            } => formatter
                .debug_struct("Repair")
                .field("root", root)
                .field("webhook_retention_seconds", webhook_retention_seconds)
                .finish(),
            Self::RepairLifecycle {
                root,
                webhook_retention_seconds,
            } => formatter
                .debug_struct("RepairLifecycle")
                .field("root", root)
                .field("webhook_retention_seconds", webhook_retention_seconds)
                .finish(),
            Self::BackupManifest { root, output } => formatter
                .debug_struct("BackupManifest")
                .field("root", root)
                .field("output", output)
                .finish(),
            Self::StorageMigrate {
                from,
                from_root,
                to,
                to_root,
                prefix,
                dry_run,
            } => formatter
                .debug_struct("StorageMigrate")
                .field("from", from)
                .field("from_root", from_root)
                .field("to", to)
                .field("to_root", to_root)
                .field("prefix", prefix)
                .field("dry_run", dry_run)
                .finish(),
            Self::HoldSet {
                root,
                object_key,
                reason,
                ttl_seconds,
            } => formatter
                .debug_struct("HoldSet")
                .field("root", root)
                .field("object_key", object_key)
                .field("reason", reason)
                .field("ttl_seconds", ttl_seconds)
                .finish(),
            Self::HoldList { root, active_only } => formatter
                .debug_struct("HoldList")
                .field("root", root)
                .field("active_only", active_only)
                .finish(),
            Self::HoldRelease { root, object_key } => formatter
                .debug_struct("HoldRelease")
                .field("root", root)
                .field("object_key", object_key)
                .finish(),
            Self::Bench {
                mode,
                deployment_target,
                scenario,
                storage_dir,
                iterations,
                concurrency,
                upload_max_in_flight_chunks,
                chunk_size_bytes,
                base_bytes,
                mutated_bytes,
                json,
            } => formatter
                .debug_struct("Bench")
                .field("mode", mode)
                .field("deployment_target", deployment_target)
                .field("scenario", scenario)
                .field("storage_dir", storage_dir)
                .field("iterations", iterations)
                .field("concurrency", concurrency)
                .field("upload_max_in_flight_chunks", upload_max_in_flight_chunks)
                .field("chunk_size_bytes", chunk_size_bytes)
                .field("base_bytes", base_bytes)
                .field("mutated_bytes", mutated_bytes)
                .field("json", json)
                .finish(),
            Self::Health { server_url } => formatter
                .debug_struct("Health")
                .field("server_url", server_url)
                .finish(),
            Self::Completion { shell, output } => formatter
                .debug_struct("Completion")
                .field("shell", shell)
                .field("output", output)
                .finish(),
            Self::Manpage { output } => formatter
                .debug_struct("Manpage")
                .field("output", output)
                .finish(),
        }
    }
}

/// Supported benchmark modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BenchMode {
    /// Run the full end-to-end storage benchmark suite.
    #[value(name = "e2e")]
    EndToEnd,
    /// Run the zero-storage upload-ingest benchmark suite.
    #[value(name = "ingest")]
    Ingest,
}

#[derive(Debug, Parser)]
#[command(
    name = "shardline",
    about = "Native Xet-compatible asset coordinator and operations CLI.",
    long_about = "Shardline serves native Xet traffic, provider integration, storage maintenance, and operational workflows from one CLI.\n\nUse `shardline help <command>` to inspect a command in detail.",
    after_help = CLI_AFTER_LONG_HELP,
    arg_required_else_help = true,
    next_line_help = true
)]
struct CliDefinition {
    #[command(subcommand)]
    command: CliDefinitionCommand,
}

#[derive(Debug, Subcommand)]
enum CliDefinitionCommand {
    /// Bootstrap a local providerless source-checkout deployment.
    Providerless(ProviderlessCommandArgs),
    /// Run the Shardline server.
    Serve(ServeArgs),
    /// Validate the effective runtime configuration.
    Config(ConfigCommandArgs),
    /// Manage the Postgres metadata schema.
    Db(DbCommandArgs),
    /// Mint a local administrative token.
    Admin(AdminCommandArgs),
    /// Verify object-store and metadata integrity.
    Fsck(RootArgs),
    /// Rebuild mutable indexes from immutable version state.
    Index(IndexCommandArgs),
    /// Repair lifecycle metadata and webhook delivery state.
    Repair(RepairCommandArgs),
    /// Export recovery artifacts.
    Backup(BackupCommandArgs),
    /// Copy immutable objects between storage adapters.
    Storage(StorageCommandArgs),
    /// Run garbage collection or install a schedule.
    Gc(GcCommandArgs),
    /// Manage retention holds.
    Hold(HoldCommandArgs),
    /// Run performance benchmarks.
    Bench(BenchArgs),
    /// Probe server health.
    Health(HealthArgs),
    /// Generate shell-completion scripts for supported shells.
    Completion(CompletionArgs),
    /// Generate a manpage for packaged or self-hosted deployments.
    Manpage(ManpageArgs),
}

#[derive(Debug, Args)]
#[command(
    about = "Bootstrap a local providerless source-checkout deployment.",
    long_about = "Create the `.shardline` local state directory, generate a signing key, and write a providerless environment file for a source checkout.\n\nThis command only prepares local state. `shardline serve` and `shardline config check` also auto-bootstrap the same layout when they run from a fresh source checkout."
)]
struct ProviderlessCommandArgs {
    #[command(subcommand)]
    command: ProviderlessSubcommand,
}

#[derive(Debug, Subcommand)]
enum ProviderlessSubcommand {
    /// Create `.shardline/`, `.shardline/data`, the signing key, and `providerless.env`.
    Setup,
}

#[derive(Debug, Args)]
#[command(
    about = "Run the Shardline server.",
    long_about = "Start the Shardline process for single-node deployments or for one split-role process.\n\nThe active environment still supplies the adapter and provider wiring. `--role` only selects which server surface this process exposes. `--frontend` selects which protocol frontends this process serves."
)]
struct ServeArgs {
    /// Pin the process to `all`, `api`, or `transfer`.
    #[arg(long, value_enum)]
    role: Option<CliServerRole>,
    /// Enable one or more protocol frontends. Repeat the flag or pass a comma-separated list.
    #[arg(long = "frontend", value_enum, value_delimiter = ',', action = clap::ArgAction::Append, num_args = 1..)]
    frontends: Vec<CliServerFrontend>,
}

#[derive(Debug, Args)]
#[command(
    about = "Validate the effective runtime configuration.",
    long_about = "Load the active Shardline environment, resolve adapters, and report the effective runtime profile without starting the server."
)]
struct ConfigCommandArgs {
    #[command(subcommand)]
    command: ConfigSubcommand,
}

#[derive(Debug, Subcommand)]
enum ConfigSubcommand {
    /// Validate the active environment and adapter wiring.
    Check,
}

#[derive(Debug, Args)]
#[command(
    about = "Manage the Postgres metadata schema.",
    long_about = "Apply, revert, or inspect the Shardline metadata schema used by Postgres-backed index state."
)]
struct DbCommandArgs {
    #[command(subcommand)]
    command: DbSubcommand,
}

#[derive(Debug, Subcommand)]
enum DbSubcommand {
    /// Apply, revert, or inspect metadata migrations.
    Migrate(DbMigrateCommandArgs),
}

#[derive(Debug, Args)]
struct DbMigrateCommandArgs {
    #[command(subcommand)]
    command: DbMigrateSubcommand,
}

#[derive(Debug, Subcommand)]
enum DbMigrateSubcommand {
    /// Apply pending migrations.
    Up(DbMigrateUpArgs),
    /// Revert applied migrations.
    Down(DbMigrateDownArgs),
    /// Show applied and pending migrations.
    Status(DbMigrateStatusArgs),
}

#[derive(Debug, Args)]
struct DbMigrateUpArgs {
    /// Override the configured Postgres metadata URL.
    #[arg(long)]
    database_url: Option<String>,
    /// Limit the number of migrations to apply.
    #[arg(long, value_parser = parse_positive_usize)]
    steps: Option<NonZeroUsize>,
}

#[derive(Debug, Args)]
struct DbMigrateDownArgs {
    /// Override the configured Postgres metadata URL.
    #[arg(long)]
    database_url: Option<String>,
    /// Limit the number of migrations to revert.
    #[arg(long, value_parser = parse_positive_usize)]
    steps: Option<NonZeroUsize>,
}

#[derive(Debug, Args)]
struct DbMigrateStatusArgs {
    /// Override the configured Postgres metadata URL.
    #[arg(long)]
    database_url: Option<String>,
}

#[derive(Debug, Args)]
#[command(
    about = "Mint a local administrative token.",
    long_about = "Create a signed bearer token for self-hosted operations, provider mediation, or debugging workflows."
)]
struct AdminCommandArgs {
    #[command(subcommand)]
    command: AdminSubcommand,
}

#[derive(Debug, Subcommand)]
enum AdminSubcommand {
    /// Mint a local bearer token for self-hosted operation and testing.
    Token(AdminTokenArgs),
}

#[derive(Debug, Args)]
struct AdminTokenArgs {
    /// Token issuer identifier.
    #[arg(long)]
    issuer: String,
    /// Token subject identifier.
    #[arg(long)]
    subject: String,
    /// Granted repository scope.
    #[arg(long, value_enum)]
    scope: CliTokenScope,
    /// Repository hosting provider.
    #[arg(long, value_enum)]
    provider: CliRepositoryProvider,
    /// Repository owner or namespace.
    #[arg(long)]
    owner: String,
    /// Repository name.
    #[arg(long)]
    repo: String,
    /// Scoped revision when one is required.
    #[arg(long)]
    revision: Option<String>,
    /// Token lifetime in seconds.
    #[arg(long, default_value_t = 3_600_u64)]
    ttl_seconds: u64,
    /// Local signing-key file path.
    #[arg(long, conflicts_with = "key_env", required_unless_present = "key_env")]
    key_file: Option<PathBuf>,
    /// Environment variable that stores the signing key.
    #[arg(
        long,
        conflicts_with = "key_file",
        required_unless_present = "key_file"
    )]
    key_env: Option<String>,
}

#[derive(Debug, Args)]
struct IndexCommandArgs {
    #[command(subcommand)]
    command: IndexSubcommand,
}

#[derive(Debug, Subcommand)]
enum IndexSubcommand {
    /// Rebuild latest-file and dedupe indexes from immutable history.
    Rebuild(RootArgs),
}

#[derive(Debug, Args)]
#[command(
    about = "Repair lifecycle metadata and webhook delivery state.",
    long_about = "Run the repair orchestrator for local or Postgres-backed metadata.\n\n`repair` runs the full repair flow. `repair lifecycle` limits the run to lifecycle-specific reconciliation."
)]
struct RepairCommandArgs {
    #[command(subcommand)]
    command: Option<RepairSubcommand>,
    #[command(flatten)]
    options: RepairOptionsArgs,
}

#[derive(Debug, Subcommand)]
enum RepairSubcommand {
    /// Repair lifecycle state only.
    Lifecycle(RepairOptionsArgs),
}

#[derive(Debug, Clone, Args)]
struct RepairOptionsArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Retention window for processed webhook-delivery claims.
    #[arg(
        long,
        default_value_t = DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
    )]
    webhook_retention_seconds: u64,
}

#[derive(Debug, Args)]
#[command(
    about = "Export recovery artifacts.",
    long_about = "Generate recovery data that can be used to audit or rebuild Shardline metadata state."
)]
struct BackupCommandArgs {
    #[command(subcommand)]
    command: BackupSubcommand,
}

#[derive(Debug, Subcommand)]
enum BackupSubcommand {
    /// Export an adapter-neutral backup manifest.
    Manifest(BackupManifestArgs),
}

#[derive(Debug, Args)]
struct BackupManifestArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Manifest output path.
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Args)]
#[command(
    about = "Copy immutable objects between storage adapters.",
    long_about = "Inventory immutable payload objects under one object-storage adapter and copy them into another adapter.\n\nThis is intended for storage migrations, dry runs, and object namespace moves."
)]
struct StorageCommandArgs {
    #[command(subcommand)]
    command: StorageSubcommand,
}

#[derive(Debug, Subcommand)]
enum StorageSubcommand {
    /// Copy immutable payload objects between object-storage adapters.
    Migrate(StorageMigrateArgs),
}

#[derive(Debug, Args)]
struct StorageMigrateArgs {
    /// Source object-storage adapter.
    #[arg(long, value_enum)]
    from: CliObjectStorageAdapter,
    /// Local state root when the source adapter is `local`.
    #[arg(long)]
    from_root: Option<PathBuf>,
    /// Destination object-storage adapter.
    #[arg(long, value_enum)]
    to: CliObjectStorageAdapter,
    /// Local state root when the destination adapter is `local`.
    #[arg(long)]
    to_root: Option<PathBuf>,
    /// Object-key prefix to migrate.
    #[arg(long, default_value_t = String::new())]
    prefix: String,
    /// Inventory objects without writing destination payloads.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
#[command(
    about = "Run garbage collection or install a schedule.",
    long_about = "Run mark, sweep, or dry-run garbage collection against the active metadata and object adapters.\n\nUse `gc schedule` to generate validated systemd units for recurring collector runs."
)]
struct GcCommandArgs {
    #[command(subcommand)]
    command: Option<GcSubcommand>,
    #[command(flatten)]
    options: GcOptionsArgs,
}

#[derive(Debug, Subcommand)]
enum GcSubcommand {
    /// Install or remove a systemd timer for garbage collection.
    Schedule(GcScheduleCommandArgs),
}

#[derive(Debug, Args)]
#[command(
    about = "Install or remove a systemd timer for garbage collection.",
    long_about = "Generate or remove a systemd `.service` and `.timer` pair for scheduled garbage collection.\n\nThe install workflow validates the target binary, environment file, user, group, and referenced secret/config files before writing units."
)]
struct GcScheduleCommandArgs {
    #[command(subcommand)]
    command: GcScheduleSubcommand,
}

#[derive(Debug, Subcommand)]
enum GcScheduleSubcommand {
    /// Generate validated systemd units for scheduled garbage collection.
    Install(GcScheduleInstallArgs),
    /// Remove generated garbage-collection systemd units.
    Uninstall(GcScheduleUninstallArgs),
}

#[derive(Debug, Clone, Args)]
struct GcOptionsArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Persist currently orphaned chunks into quarantine.
    #[arg(long)]
    mark: bool,
    /// Delete eligible orphaned chunks after reporting them.
    #[arg(long)]
    sweep: bool,
    /// Retention window for newly quarantined chunks.
    #[arg(long, default_value_t = DEFAULT_LOCAL_GC_RETENTION_SECONDS)]
    retention_seconds: u64,
    /// Write active quarantine state to a JSON report.
    #[arg(long)]
    retention_report: Option<PathBuf>,
    /// Write the current orphan inventory to a JSON report.
    #[arg(long)]
    orphan_inventory: Option<PathBuf>,
}

#[derive(Debug, Args)]
#[command(
    about = "Generate validated systemd units for scheduled garbage collection.",
    long_about = "Write a `systemd` service and timer for `shardline gc`.\n\nThe installer resolves the active Shardline binary when the default path is left in place, requires the environment file to exist, honors `SHARDLINE_ROOT_DIR`, and validates referenced secret/config files plus the selected service user and group.",
    after_help = GC_INSTALL_AFTER_LONG_HELP
)]
struct GcScheduleInstallArgs {
    /// Directory that receives the generated systemd units.
    #[arg(long, default_value = "/etc/systemd/system")]
    output_dir: PathBuf,
    /// Unit basename without `.service` or `.timer`.
    #[arg(long, default_value = "shardline-gc")]
    unit_prefix: String,
    /// `systemd.timer` calendar expression.
    #[arg(long, default_value = "*-*-* 03:17:00")]
    calendar: String,
    /// Retention window passed to the scheduled collector.
    #[arg(long, default_value_t = 86_400_u64)]
    retention_seconds: u64,
    /// Path to the `shardline` binary embedded in the unit.
    #[arg(long, default_value = "/usr/local/bin/shardline")]
    binary_path: PathBuf,
    /// Environment file referenced by the generated service.
    #[arg(long, default_value = "/etc/shardline/shardline.env")]
    env_file: PathBuf,
    /// Working directory and writable state path.
    #[arg(long, default_value = "/var/lib/shardline")]
    working_directory: PathBuf,
    /// Service user.
    #[arg(long, default_value = "shardline")]
    user: String,
    /// Service group.
    #[arg(long, default_value = "shardline")]
    group: String,
}

#[derive(Debug, Args)]
struct GcScheduleUninstallArgs {
    /// Directory that contains the generated systemd units.
    #[arg(long, default_value = "/etc/systemd/system")]
    output_dir: PathBuf,
    /// Unit basename without `.service` or `.timer`.
    #[arg(long, default_value = "shardline-gc")]
    unit_prefix: String,
}

#[derive(Debug, Args)]
#[command(
    about = "Manage retention holds.",
    long_about = "Create, list, and release retention holds that protect object keys from garbage collection."
)]
struct HoldCommandArgs {
    #[command(subcommand)]
    command: HoldSubcommand,
}

#[derive(Debug, Subcommand)]
enum HoldSubcommand {
    /// Create or update a retention hold.
    Set(HoldSetArgs),
    /// List retention holds.
    List(HoldListArgs),
    /// Release one retention hold.
    Release(HoldReleaseArgs),
}

#[derive(Debug, Args)]
struct HoldSetArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Object-store key protected by the hold.
    #[arg(long)]
    object_key: String,
    /// Human-readable hold reason.
    #[arg(long)]
    reason: String,
    /// Optional hold time-to-live in seconds.
    #[arg(long)]
    ttl_seconds: Option<u64>,
}

#[derive(Debug, Args)]
struct HoldListArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Exclude expired holds.
    #[arg(long)]
    active_only: bool,
}

#[derive(Debug, Args)]
struct HoldReleaseArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Object-store key released from protection.
    #[arg(long)]
    object_key: String,
}

#[derive(Debug, Args)]
#[command(
    about = "Run performance benchmarks.",
    long_about = "Measure upload, download, reconstruction, and concurrency behavior.\n\n`e2e` mode can run either an isolated local SQLite plus filesystem deployment or the active configured runtime backend, and requires `--storage-dir`. `ingest` mode measures upload ingestion without storing payloads.",
    after_help = BENCH_AFTER_LONG_HELP
)]
struct BenchArgs {
    /// Benchmark mode.
    #[arg(long, value_enum, default_value_t = BenchMode::EndToEnd)]
    mode: BenchMode,
    /// End-to-end benchmark deployment target.
    #[arg(long, value_enum, default_value_t = BenchDeploymentTarget::IsolatedLocal)]
    deployment_target: BenchDeploymentTarget,
    /// Focus one benchmark scenario instead of running all steps.
    #[arg(long, value_enum, default_value_t = BenchScenario::Full)]
    scenario: BenchScenario,
    /// Root directory used to create isolated benchmark iteration stores.
    #[arg(long)]
    storage_dir: Option<PathBuf>,
    /// Number of benchmark iterations to run.
    #[arg(long, default_value_t = 1_u32)]
    iterations: u32,
    /// Number of concurrent workers used for concurrent sub-scenarios.
    #[arg(long, default_value_t = 4_u32)]
    concurrency: u32,
    /// Maximum upload chunks processed in parallel per upload.
    #[arg(long, default_value_t = 64_usize)]
    upload_max_in_flight_chunks: usize,
    /// Chunk size in bytes used by the local benchmark backend.
    #[arg(long, default_value_t = 65_536_usize)]
    chunk_size_bytes: usize,
    /// Logical size of the benchmark asset in bytes.
    #[arg(long, default_value_t = 1_048_576_usize)]
    base_bytes: usize,
    /// Number of bytes changed in the sparse-update benchmark step.
    #[arg(long, default_value_t = 4_096_usize)]
    mutated_bytes: usize,
    /// Emit the full report as JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
#[command(
    about = "Probe server health.",
    long_about = "Send a health probe to a running Shardline server and fail when the server does not answer successfully."
)]
struct HealthArgs {
    /// Base URL of the Shardline server to probe.
    #[arg(long = "server")]
    server_url: String,
}

#[derive(Debug, Args)]
#[command(
    about = "Generate shell-completion scripts for supported shells.",
    long_about = "Render a completion script from the live Shardline CLI definition.\n\nThis keeps shell completions aligned with the real command surface instead of shipping a handwritten static script.",
    after_help = COMPLETION_AFTER_HELP
)]
struct CompletionArgs {
    /// Target shell.
    #[arg(value_enum)]
    shell: CompletionShell,
    /// Write the generated script to one file instead of stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
#[command(
    about = "Generate a manpage for packaged or self-hosted deployments.",
    long_about = "Render a manpage from the live Shardline CLI definition.\n\nThis is intended for packaging, system installations, and offline operator documentation.",
    after_help = MANPAGE_AFTER_HELP
)]
struct ManpageArgs {
    /// Write the generated manpage to one file instead of stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Default, Args)]
struct RootArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliServerRole {
    /// Serve both control-plane and transfer routes from one process.
    All,
    /// Serve control-plane and metadata routes only.
    Api,
    /// Serve upload and download routes only.
    Transfer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliServerFrontend {
    /// Serve the Xet-compatible CAS frontend.
    Xet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliTokenScope {
    /// Allow read-only access.
    Read,
    /// Allow writes and uploads.
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliRepositoryProvider {
    /// GitHub repository hosting.
    #[value(name = "github")]
    GitHub,
    /// Gitea repository hosting.
    #[value(name = "gitea")]
    Gitea,
    /// GitLab repository hosting.
    #[value(name = "gitlab")]
    GitLab,
    /// Generic Git provider integration.
    #[value(name = "generic")]
    Generic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliObjectStorageAdapter {
    /// Local filesystem-backed object storage.
    Local,
    /// S3-compatible object storage.
    S3,
}

/// Supported completion targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompletionShell {
    /// Bash completion script.
    Bash,
    /// Elvish completion script.
    Elvish,
    /// Fish completion script.
    Fish,
    /// PowerShell completion script.
    #[value(name = "powershell")]
    PowerShell,
    /// Zsh completion script.
    Zsh,
}

impl CliCommand {
    /// Parses a command from process arguments.
    ///
    /// # Errors
    ///
    /// Returns [`CliParseError`] when the argument vector is invalid or help/version
    /// output was requested.
    pub fn parse<I, T>(args: I) -> Result<Self, CliParseError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let mut args = args.into_iter().map(Into::into).collect::<Vec<OsString>>();
        if args.is_empty() {
            args.push(OsString::from("shardline"));
        }

        let definition = CliDefinition::try_parse_from(args).map_err(CliParseError::from)?;
        Self::try_from(definition)
    }

    /// Returns top-level help text.
    #[must_use]
    pub fn help_text() -> String {
        cli_definition_command().render_long_help().to_string()
    }
}

/// CLI parse failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct CliParseError {
    kind: ErrorKind,
    message: String,
}

impl CliParseError {
    /// Returns the underlying clap error kind.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns whether this parse failure represents help or version output.
    #[must_use]
    pub const fn is_help(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::DisplayHelp
                | ErrorKind::DisplayVersion
                | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        )
    }

    fn validation(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl From<clap::Error> for CliParseError {
    fn from(error: clap::Error) -> Self {
        Self {
            kind: error.kind(),
            message: format!("{error}"),
        }
    }
}

impl TryFrom<CliDefinition> for CliCommand {
    type Error = CliParseError;

    fn try_from(value: CliDefinition) -> Result<Self, Self::Error> {
        match value.command {
            CliDefinitionCommand::Providerless(args) => match args.command {
                ProviderlessSubcommand::Setup => Ok(Self::ProviderlessSetup),
            },
            CliDefinitionCommand::Serve(args) => Ok(Self::Serve {
                role: args.role.map(Into::into),
                frontends: if args.frontends.is_empty() {
                    None
                } else {
                    Some(deduplicated_cli_frontends(
                        args.frontends.into_iter().map(Into::into),
                    ))
                },
            }),
            CliDefinitionCommand::Config(args) => match args.command {
                ConfigSubcommand::Check => Ok(Self::ConfigCheck),
            },
            CliDefinitionCommand::Db(db_args) => match db_args.command {
                DbSubcommand::Migrate(migrate) => match migrate.command {
                    DbMigrateSubcommand::Up(up_args) => Ok(Self::DbMigrate {
                        database_url: up_args.database_url,
                        command: DatabaseMigrationCommand::Up {
                            steps: up_args.steps.map(NonZeroUsize::get),
                        },
                    }),
                    DbMigrateSubcommand::Down(down_args) => Ok(Self::DbMigrate {
                        database_url: down_args.database_url,
                        command: DatabaseMigrationCommand::Down {
                            steps: down_args.steps.map_or(1, NonZeroUsize::get),
                        },
                    }),
                    DbMigrateSubcommand::Status(status_args) => Ok(Self::DbMigrate {
                        database_url: status_args.database_url,
                        command: DatabaseMigrationCommand::Status,
                    }),
                },
            },
            CliDefinitionCommand::Admin(args) => match args.command {
                AdminSubcommand::Token(args) => Ok(Self::AdminToken {
                    issuer: args.issuer,
                    subject: args.subject,
                    scope: args.scope.into(),
                    provider: args.provider.into(),
                    owner: args.owner,
                    repo: args.repo,
                    revision: args.revision,
                    ttl_seconds: args.ttl_seconds,
                    key_file: args.key_file,
                    key_env: args.key_env,
                }),
            },
            CliDefinitionCommand::Fsck(args) => Ok(Self::Fsck { root: args.root }),
            CliDefinitionCommand::Index(args) => match args.command {
                IndexSubcommand::Rebuild(args) => Ok(Self::IndexRebuild { root: args.root }),
            },
            CliDefinitionCommand::Repair(args) => match args.command {
                Some(RepairSubcommand::Lifecycle(options)) => Ok(Self::RepairLifecycle {
                    root: options.root,
                    webhook_retention_seconds: options.webhook_retention_seconds,
                }),
                None => Ok(Self::Repair {
                    root: args.options.root,
                    webhook_retention_seconds: args.options.webhook_retention_seconds,
                }),
            },
            CliDefinitionCommand::Backup(args) => match args.command {
                BackupSubcommand::Manifest(args) => Ok(Self::BackupManifest {
                    root: args.root,
                    output: args.output,
                }),
            },
            CliDefinitionCommand::Storage(args) => match args.command {
                StorageSubcommand::Migrate(args) => Ok(Self::StorageMigrate {
                    from: args.from.into(),
                    from_root: args.from_root,
                    to: args.to.into(),
                    to_root: args.to_root,
                    prefix: args.prefix,
                    dry_run: args.dry_run,
                }),
            },
            CliDefinitionCommand::Gc(gc_args) => match gc_args.command {
                Some(GcSubcommand::Schedule(schedule)) => match schedule.command {
                    GcScheduleSubcommand::Install(install_args) => Ok(Self::GcScheduleInstall {
                        output_dir: install_args.output_dir,
                        unit_prefix: install_args.unit_prefix,
                        calendar: install_args.calendar,
                        retention_seconds: install_args.retention_seconds,
                        binary_path: install_args.binary_path,
                        env_file: install_args.env_file,
                        working_directory: install_args.working_directory,
                        user: install_args.user,
                        group: install_args.group,
                    }),
                    GcScheduleSubcommand::Uninstall(uninstall_args) => {
                        Ok(Self::GcScheduleUninstall {
                            output_dir: uninstall_args.output_dir,
                            unit_prefix: uninstall_args.unit_prefix,
                        })
                    }
                },
                None => Ok(Self::Gc {
                    root: gc_args.options.root,
                    mark: gc_args.options.mark,
                    sweep: gc_args.options.sweep,
                    retention_seconds: gc_args.options.retention_seconds,
                    retention_report: gc_args.options.retention_report,
                    orphan_inventory: gc_args.options.orphan_inventory,
                }),
            },
            CliDefinitionCommand::Hold(args) => match args.command {
                HoldSubcommand::Set(args) => Ok(Self::HoldSet {
                    root: args.root,
                    object_key: args.object_key,
                    reason: args.reason,
                    ttl_seconds: args.ttl_seconds,
                }),
                HoldSubcommand::List(args) => Ok(Self::HoldList {
                    root: args.root,
                    active_only: args.active_only,
                }),
                HoldSubcommand::Release(args) => Ok(Self::HoldRelease {
                    root: args.root,
                    object_key: args.object_key,
                }),
            },
            CliDefinitionCommand::Bench(args) => {
                if args.mode == BenchMode::EndToEnd && args.storage_dir.is_none() {
                    return Err(CliParseError::validation(
                        ErrorKind::MissingRequiredArgument,
                        "end-to-end benchmark mode requires --storage-dir",
                    ));
                }

                Ok(Self::Bench {
                    mode: args.mode,
                    deployment_target: args.deployment_target,
                    scenario: args.scenario,
                    storage_dir: args.storage_dir,
                    iterations: args.iterations,
                    concurrency: args.concurrency,
                    upload_max_in_flight_chunks: args.upload_max_in_flight_chunks,
                    chunk_size_bytes: args.chunk_size_bytes,
                    base_bytes: args.base_bytes,
                    mutated_bytes: args.mutated_bytes,
                    json: args.json,
                })
            }
            CliDefinitionCommand::Health(args) => Ok(Self::Health {
                server_url: args.server_url,
            }),
            CliDefinitionCommand::Completion(args) => Ok(Self::Completion {
                shell: args.shell,
                output: args.output,
            }),
            CliDefinitionCommand::Manpage(args) => Ok(Self::Manpage {
                output: args.output,
            }),
        }
    }
}

pub(crate) fn cli_definition_command() -> clap::Command {
    CliDefinition::command()
}

impl From<CliServerRole> for ServerRole {
    fn from(value: CliServerRole) -> Self {
        match value {
            CliServerRole::All => Self::All,
            CliServerRole::Api => Self::Api,
            CliServerRole::Transfer => Self::Transfer,
        }
    }
}

impl From<CliServerFrontend> for ServerFrontend {
    fn from(value: CliServerFrontend) -> Self {
        match value {
            CliServerFrontend::Xet => Self::Xet,
        }
    }
}

impl From<CliTokenScope> for TokenScope {
    fn from(value: CliTokenScope) -> Self {
        match value {
            CliTokenScope::Read => Self::Read,
            CliTokenScope::Write => Self::Write,
        }
    }
}

impl From<CliRepositoryProvider> for RepositoryProvider {
    fn from(value: CliRepositoryProvider) -> Self {
        match value {
            CliRepositoryProvider::GitHub => Self::GitHub,
            CliRepositoryProvider::Gitea => Self::Gitea,
            CliRepositoryProvider::GitLab => Self::GitLab,
            CliRepositoryProvider::Generic => Self::Generic,
        }
    }
}

impl From<CliObjectStorageAdapter> for ObjectStorageAdapter {
    fn from(value: CliObjectStorageAdapter) -> Self {
        match value {
            CliObjectStorageAdapter::Local => Self::Local,
            CliObjectStorageAdapter::S3 => Self::S3,
        }
    }
}

fn parse_positive_usize(value: &str) -> Result<NonZeroUsize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_error| "value must be a positive integer".to_owned())?;
    NonZeroUsize::new(parsed).ok_or_else(|| "value must be a positive integer".to_owned())
}

fn deduplicated_cli_frontends(
    frontends: impl IntoIterator<Item = ServerFrontend>,
) -> Vec<ServerFrontend> {
    let mut deduplicated = Vec::new();
    for frontend in frontends {
        if !deduplicated.contains(&frontend) {
            deduplicated.push(frontend);
        }
    }
    deduplicated
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::error::ErrorKind;
    use shardline_protocol::{RepositoryProvider, TokenScope};
    use shardline_server::{
        DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
        DatabaseMigrationCommand, ObjectStorageAdapter, ServerFrontend, ServerRole,
    };

    use super::{BenchMode, CliCommand, CompletionShell};
    use crate::bench::{BenchDeploymentTarget, BenchScenario};

    #[test]
    fn parse_defaults_to_help() {
        let args = vec!["shardline".to_owned()];
        let parsed = CliCommand::parse(args);

        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert!(error.is_help());
        assert!(format!("{error}").contains("Usage: shardline"));
    }

    #[test]
    fn parse_help_aliases() {
        let long = vec!["shardline".to_owned(), "--help".to_owned()];
        let short = vec!["shardline".to_owned(), "-h".to_owned()];
        let command = vec!["shardline".to_owned(), "help".to_owned()];

        for args in [long, short, command] {
            let parsed = CliCommand::parse(args);
            assert!(parsed.is_err());
            let Err(error) = parsed else {
                return;
            };
            assert!(error.is_help());
            assert!(format!("{error}").contains("Usage: shardline"));
        }
    }

    #[test]
    fn help_text_is_generated_from_clap() {
        let help = CliCommand::help_text();

        assert!(help.contains("Usage: shardline"));
        assert!(help.contains("Examples:"));
        assert!(help.contains("gc schedule install"));
        assert!(help.contains("completion"));
        assert!(help.contains("manpage"));
        assert!(help.contains("db"));
        assert!(help.contains("gc"));
        assert!(help.contains("bench"));
        assert!(help.contains("providerless"));
    }

    #[test]
    fn nested_help_includes_examples_for_gc_schedule_install() {
        let args = vec![
            "shardline".to_owned(),
            "gc".to_owned(),
            "schedule".to_owned(),
            "install".to_owned(),
            "--help".to_owned(),
        ];
        let parsed = CliCommand::parse(args);

        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert!(error.is_help());
        assert!(format!("{error}").contains("Examples:"));
        assert!(
            error
                .to_string()
                .contains("--env-file /etc/shardline/shardline.env")
        );
    }

    #[test]
    fn parse_top_level_commands() {
        let providerless = vec![
            "shardline".to_owned(),
            "providerless".to_owned(),
            "setup".to_owned(),
        ];
        let serve = vec!["shardline".to_owned(), "serve".to_owned()];
        let bench = vec![
            "shardline".to_owned(),
            "bench".to_owned(),
            "--storage-dir".to_owned(),
            "/var/lib/shardline-bench".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(providerless),
            Ok(CliCommand::ProviderlessSetup)
        );
        assert_eq!(
            CliCommand::parse(serve),
            Ok(CliCommand::Serve {
                role: None,
                frontends: None,
            })
        );
        assert_eq!(
            CliCommand::parse(bench),
            Ok(CliCommand::Bench {
                mode: BenchMode::EndToEnd,
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
                iterations: 1,
                concurrency: 4,
                upload_max_in_flight_chunks: 64,
                chunk_size_bytes: 65_536,
                base_bytes: 1_048_576,
                mutated_bytes: 4_096,
                json: false,
            })
        );
    }

    #[test]
    fn parse_serve_with_role() {
        let args = vec![
            "shardline".to_owned(),
            "serve".to_owned(),
            "--role".to_owned(),
            "transfer".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Serve {
                role: Some(ServerRole::Transfer),
                frontends: None,
            })
        );
    }

    #[test]
    fn parse_serve_with_frontends() {
        let args = vec![
            "shardline".to_owned(),
            "serve".to_owned(),
            "--frontend".to_owned(),
            "xet,xet".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Serve {
                role: None,
                frontends: Some(vec![ServerFrontend::Xet]),
            })
        );
    }

    #[test]
    fn parse_fsck() {
        let args = vec![
            "shardline".to_owned(),
            "fsck".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Fsck {
                root: Some(PathBuf::from("/var/lib/shardline")),
            })
        );
    }

    #[test]
    fn parse_fsck_without_root_override() {
        let args = vec!["shardline".to_owned(), "fsck".to_owned()];

        assert_eq!(CliCommand::parse(args), Ok(CliCommand::Fsck { root: None }));
    }

    #[test]
    fn parse_index_rebuild() {
        let args = vec![
            "shardline".to_owned(),
            "index".to_owned(),
            "rebuild".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::IndexRebuild {
                root: Some(PathBuf::from("/var/lib/shardline")),
            })
        );
    }

    #[test]
    fn parse_repair_lifecycle() {
        let args = vec![
            "shardline".to_owned(),
            "repair".to_owned(),
            "lifecycle".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--webhook-retention-seconds".to_owned(),
            "3600".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::RepairLifecycle {
                root: Some(PathBuf::from("/var/lib/shardline")),
                webhook_retention_seconds: 3600,
            })
        );
    }

    #[test]
    fn parse_repair_orchestrator_with_defaults() {
        let args = vec!["shardline".to_owned(), "repair".to_owned()];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Repair {
                root: None,
                webhook_retention_seconds: DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
            })
        );
    }

    #[test]
    fn parse_repair_orchestrator_with_options() {
        let args = vec![
            "shardline".to_owned(),
            "repair".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--webhook-retention-seconds".to_owned(),
            "3600".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Repair {
                root: Some(PathBuf::from("/var/lib/shardline")),
                webhook_retention_seconds: 3600,
            })
        );
    }

    #[test]
    fn parse_gc() {
        let args = vec![
            "shardline".to_owned(),
            "gc".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--mark".to_owned(),
            "--sweep".to_owned(),
            "--retention-seconds".to_owned(),
            "600".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Gc {
                root: Some(PathBuf::from("/var/lib/shardline")),
                mark: true,
                sweep: true,
                retention_seconds: 600,
                retention_report: None,
                orphan_inventory: None,
            })
        );
    }

    #[test]
    fn parse_gc_schedule_install() {
        let args = vec![
            "shardline".to_owned(),
            "gc".to_owned(),
            "schedule".to_owned(),
            "install".to_owned(),
            "--output-dir".to_owned(),
            "/tmp/systemd".to_owned(),
            "--unit-prefix".to_owned(),
            "assets-gc".to_owned(),
            "--calendar".to_owned(),
            "hourly".to_owned(),
            "--retention-seconds".to_owned(),
            "600".to_owned(),
            "--binary-path".to_owned(),
            "/usr/bin/shardline".to_owned(),
            "--env-file".to_owned(),
            "/etc/shardline/env".to_owned(),
            "--working-directory".to_owned(),
            "/srv/assets".to_owned(),
            "--user".to_owned(),
            "svc".to_owned(),
            "--group".to_owned(),
            "svc".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::GcScheduleInstall {
                output_dir: PathBuf::from("/tmp/systemd"),
                unit_prefix: "assets-gc".to_owned(),
                calendar: "hourly".to_owned(),
                retention_seconds: 600,
                binary_path: PathBuf::from("/usr/bin/shardline"),
                env_file: PathBuf::from("/etc/shardline/env"),
                working_directory: PathBuf::from("/srv/assets"),
                user: "svc".to_owned(),
                group: "svc".to_owned(),
            })
        );
    }

    #[test]
    fn parse_gc_schedule_uninstall() {
        let args = vec![
            "shardline".to_owned(),
            "gc".to_owned(),
            "schedule".to_owned(),
            "uninstall".to_owned(),
            "--output-dir".to_owned(),
            "/tmp/systemd".to_owned(),
            "--unit-prefix".to_owned(),
            "assets-gc".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::GcScheduleUninstall {
                output_dir: PathBuf::from("/tmp/systemd"),
                unit_prefix: "assets-gc".to_owned(),
            })
        );
    }

    #[test]
    fn parse_backup_manifest() {
        let args = vec![
            "shardline".to_owned(),
            "backup".to_owned(),
            "manifest".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--output".to_owned(),
            "backup.json".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::BackupManifest {
                root: Some(PathBuf::from("/var/lib/shardline")),
                output: PathBuf::from("backup.json"),
            })
        );
    }

    #[test]
    fn parse_backup_manifest_requires_output() {
        let args = vec![
            "shardline".to_owned(),
            "backup".to_owned(),
            "manifest".to_owned(),
        ];

        let parsed = CliCommand::parse(args);
        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        assert!(format!("{error}").contains("--output"));
    }

    #[test]
    fn parse_db_migrate_up() {
        let args = vec![
            "shardline".to_owned(),
            "db".to_owned(),
            "migrate".to_owned(),
            "up".to_owned(),
            "--database-url".to_owned(),
            "postgres://user:password@localhost:5432/shardline".to_owned(),
            "--steps".to_owned(),
            "2".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::DbMigrate {
                database_url: Some("postgres://user:password@localhost:5432/shardline".to_owned()),
                command: DatabaseMigrationCommand::Up { steps: Some(2) },
            })
        );
    }

    #[test]
    fn cli_command_debug_redacts_database_url_credentials() {
        let parsed = CliCommand::parse(vec![
            "shardline".to_owned(),
            "db".to_owned(),
            "migrate".to_owned(),
            "up".to_owned(),
            "--database-url".to_owned(),
            "postgres://user:database-secret@localhost:5432/shardline".to_owned(),
        ]);
        assert!(parsed.is_ok());
        let Ok(parsed) = parsed else {
            return;
        };

        let rendered = format!("{parsed:?}");

        assert!(!rendered.contains("database-secret"));
        assert!(rendered.contains("***"));
    }

    #[test]
    fn parse_db_migrate_status() {
        let args = vec![
            "shardline".to_owned(),
            "db".to_owned(),
            "migrate".to_owned(),
            "status".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::DbMigrate {
                database_url: None,
                command: DatabaseMigrationCommand::Status,
            })
        );
    }

    #[test]
    fn parse_db_migrate_rejects_zero_steps() {
        let args = vec![
            "shardline".to_owned(),
            "db".to_owned(),
            "migrate".to_owned(),
            "down".to_owned(),
            "--steps".to_owned(),
            "0".to_owned(),
        ];

        let parsed = CliCommand::parse(args);
        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(
            error
                .to_string()
                .contains("value must be a positive integer")
        );
    }

    #[test]
    fn parse_storage_migrate() {
        let args = vec![
            "shardline".to_owned(),
            "storage".to_owned(),
            "migrate".to_owned(),
            "--from".to_owned(),
            "local".to_owned(),
            "--from-root".to_owned(),
            "/srv/assets/.shardline/data".to_owned(),
            "--to".to_owned(),
            "s3".to_owned(),
            "--prefix".to_owned(),
            "xorbs/default/".to_owned(),
            "--dry-run".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::StorageMigrate {
                from: ObjectStorageAdapter::Local,
                from_root: Some(PathBuf::from("/srv/assets/.shardline/data")),
                to: ObjectStorageAdapter::S3,
                to_root: None,
                prefix: "xorbs/default/".to_owned(),
                dry_run: true,
            })
        );
    }

    #[test]
    fn parse_gc_with_export_paths() {
        let args = vec![
            "shardline".to_owned(),
            "gc".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--retention-report".to_owned(),
            "retention.json".to_owned(),
            "--orphan-inventory".to_owned(),
            "orphans.json".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Gc {
                root: Some(PathBuf::from("/var/lib/shardline")),
                mark: false,
                sweep: false,
                retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
                retention_report: Some(PathBuf::from("retention.json")),
                orphan_inventory: Some(PathBuf::from("orphans.json")),
            })
        );
    }

    #[test]
    fn parse_hold_set() {
        let args = vec![
            "shardline".to_owned(),
            "hold".to_owned(),
            "set".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--object-key".to_owned(),
            format!("de/{}", "de".repeat(32)),
            "--reason".to_owned(),
            "provider deletion grace".to_owned(),
            "--ttl-seconds".to_owned(),
            "600".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::HoldSet {
                root: Some(PathBuf::from("/var/lib/shardline")),
                object_key: format!("de/{}", "de".repeat(32)),
                reason: "provider deletion grace".to_owned(),
                ttl_seconds: Some(600),
            })
        );
    }

    #[test]
    fn parse_hold_list() {
        let args = vec![
            "shardline".to_owned(),
            "hold".to_owned(),
            "list".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--active-only".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::HoldList {
                root: Some(PathBuf::from("/var/lib/shardline")),
                active_only: true,
            })
        );
    }

    #[test]
    fn parse_hold_release() {
        let args = vec![
            "shardline".to_owned(),
            "hold".to_owned(),
            "release".to_owned(),
            "--root".to_owned(),
            "/var/lib/shardline".to_owned(),
            "--object-key".to_owned(),
            format!("de/{}", "de".repeat(32)),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::HoldRelease {
                root: Some(PathBuf::from("/var/lib/shardline")),
                object_key: format!("de/{}", "de".repeat(32)),
            })
        );
    }

    #[test]
    fn parse_bench_with_explicit_options() {
        let args = vec![
            "shardline".to_owned(),
            "bench".to_owned(),
            "--storage-dir".to_owned(),
            "/var/lib/shardline-bench".to_owned(),
            "--iterations".to_owned(),
            "5".to_owned(),
            "--concurrency".to_owned(),
            "8".to_owned(),
            "--upload-max-in-flight-chunks".to_owned(),
            "32".to_owned(),
            "--chunk-size-bytes".to_owned(),
            "4096".to_owned(),
            "--base-bytes".to_owned(),
            "65536".to_owned(),
            "--mutated-bytes".to_owned(),
            "1024".to_owned(),
            "--json".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Bench {
                mode: BenchMode::EndToEnd,
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
                iterations: 5,
                concurrency: 8,
                upload_max_in_flight_chunks: 32,
                chunk_size_bytes: 4096,
                base_bytes: 65_536,
                mutated_bytes: 1024,
                json: true,
            })
        );
    }

    #[test]
    fn parse_bench_with_configured_deployment_target() {
        let args = vec![
            "shardline".to_owned(),
            "bench".to_owned(),
            "--storage-dir".to_owned(),
            "/var/lib/shardline-bench".to_owned(),
            "--deployment-target".to_owned(),
            "configured".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Bench {
                mode: BenchMode::EndToEnd,
                deployment_target: BenchDeploymentTarget::Configured,
                scenario: BenchScenario::Full,
                storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
                iterations: 1,
                concurrency: 4,
                upload_max_in_flight_chunks: 64,
                chunk_size_bytes: 65_536,
                base_bytes: 1_048_576,
                mutated_bytes: 4_096,
                json: false,
            })
        );
    }

    #[test]
    fn parse_ingest_bench_without_storage_dir() {
        let args = vec![
            "shardline".to_owned(),
            "bench".to_owned(),
            "--mode".to_owned(),
            "ingest".to_owned(),
            "--iterations".to_owned(),
            "3".to_owned(),
            "--concurrency".to_owned(),
            "16".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Bench {
                mode: BenchMode::Ingest,
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::Full,
                storage_dir: None,
                iterations: 3,
                concurrency: 16,
                upload_max_in_flight_chunks: 64,
                chunk_size_bytes: 65_536,
                base_bytes: 1_048_576,
                mutated_bytes: 4_096,
                json: false,
            })
        );
    }

    #[test]
    fn parse_bench_with_focused_scenario() {
        let args = vec![
            "shardline".to_owned(),
            "bench".to_owned(),
            "--storage-dir".to_owned(),
            "/var/lib/shardline-bench".to_owned(),
            "--scenario".to_owned(),
            "cross-repository-upload".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Bench {
                mode: BenchMode::EndToEnd,
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::CrossRepositoryUpload,
                storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
                iterations: 1,
                concurrency: 4,
                upload_max_in_flight_chunks: 64,
                chunk_size_bytes: 65_536,
                base_bytes: 1_048_576,
                mutated_bytes: 4_096,
                json: false,
            })
        );
    }

    #[test]
    fn parse_bench_with_cached_reconstruction_scenario() {
        let args = vec![
            "shardline".to_owned(),
            "bench".to_owned(),
            "--storage-dir".to_owned(),
            "/var/lib/shardline-bench".to_owned(),
            "--scenario".to_owned(),
            "cached-latest-reconstruction".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Bench {
                mode: BenchMode::EndToEnd,
                deployment_target: BenchDeploymentTarget::IsolatedLocal,
                scenario: BenchScenario::CachedLatestReconstruction,
                storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
                iterations: 1,
                concurrency: 4,
                upload_max_in_flight_chunks: 64,
                chunk_size_bytes: 65_536,
                base_bytes: 1_048_576,
                mutated_bytes: 4_096,
                json: false,
            })
        );
    }

    #[test]
    fn parse_bench_requires_storage_dir_for_e2e_mode() {
        let args = vec!["shardline".to_owned(), "bench".to_owned()];
        let parsed = CliCommand::parse(args);

        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        assert!(format!("{error}").contains("--storage-dir"));
    }

    #[test]
    fn parse_health() {
        let args = vec![
            "shardline".to_owned(),
            "health".to_owned(),
            "--server".to_owned(),
            "http://127.0.0.1:8080".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Health {
                server_url: "http://127.0.0.1:8080".to_owned()
            })
        );
    }

    #[test]
    fn parse_completion() {
        let args = vec![
            "shardline".to_owned(),
            "completion".to_owned(),
            "bash".to_owned(),
            "--output".to_owned(),
            "./shardline.bash".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Completion {
                shell: CompletionShell::Bash,
                output: Some(PathBuf::from("./shardline.bash")),
            })
        );
    }

    #[test]
    fn parse_manpage() {
        let args = vec![
            "shardline".to_owned(),
            "manpage".to_owned(),
            "--output".to_owned(),
            "./shardline.1".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::Manpage {
                output: Some(PathBuf::from("./shardline.1")),
            })
        );
    }

    #[test]
    fn parse_config_check() {
        let args = vec![
            "shardline".to_owned(),
            "config".to_owned(),
            "check".to_owned(),
        ];

        assert_eq!(CliCommand::parse(args), Ok(CliCommand::ConfigCheck));
    }

    #[test]
    fn parse_admin_token() {
        let args = vec![
            "shardline".to_owned(),
            "admin".to_owned(),
            "token".to_owned(),
            "--issuer".to_owned(),
            "local".to_owned(),
            "--subject".to_owned(),
            "operator-1".to_owned(),
            "--scope".to_owned(),
            "write".to_owned(),
            "--provider".to_owned(),
            "github".to_owned(),
            "--owner".to_owned(),
            "team".to_owned(),
            "--repo".to_owned(),
            "assets".to_owned(),
            "--revision".to_owned(),
            "main".to_owned(),
            "--key-file".to_owned(),
            "/tmp/shardline.key".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::AdminToken {
                issuer: "local".to_owned(),
                subject: "operator-1".to_owned(),
                scope: TokenScope::Write,
                provider: RepositoryProvider::GitHub,
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: Some("main".to_owned()),
                ttl_seconds: 3600,
                key_file: Some(PathBuf::from("/tmp/shardline.key")),
                key_env: None,
            })
        );
    }

    #[test]
    fn parse_admin_token_with_key_env() {
        let args = vec![
            "shardline".to_owned(),
            "admin".to_owned(),
            "token".to_owned(),
            "--issuer".to_owned(),
            "local".to_owned(),
            "--subject".to_owned(),
            "operator-1".to_owned(),
            "--scope".to_owned(),
            "write".to_owned(),
            "--provider".to_owned(),
            "github".to_owned(),
            "--owner".to_owned(),
            "team".to_owned(),
            "--repo".to_owned(),
            "assets".to_owned(),
            "--key-env".to_owned(),
            "SHARDLINE_TOKEN_SIGNING_KEY".to_owned(),
        ];

        assert_eq!(
            CliCommand::parse(args),
            Ok(CliCommand::AdminToken {
                issuer: "local".to_owned(),
                subject: "operator-1".to_owned(),
                scope: TokenScope::Write,
                provider: RepositoryProvider::GitHub,
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: None,
                ttl_seconds: 3600,
                key_file: None,
                key_env: Some("SHARDLINE_TOKEN_SIGNING_KEY".to_owned()),
            })
        );
    }

    #[test]
    fn parse_rejects_unknown_command() {
        let args = vec!["shardline".to_owned(), "unknown".to_owned()];
        let parsed = CliCommand::parse(args);

        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert_eq!(error.kind(), ErrorKind::InvalidSubcommand);
        assert!(format!("{error}").contains("unknown"));
    }

    #[test]
    fn parse_rejects_incomplete_nested_commands() {
        let config = vec!["shardline".to_owned(), "config".to_owned()];
        let admin = vec!["shardline".to_owned(), "admin".to_owned()];
        let providerless = vec!["shardline".to_owned(), "providerless".to_owned()];

        for args in [config, admin, providerless] {
            let parsed = CliCommand::parse(args);
            assert!(parsed.is_err());
            let Err(error) = parsed else {
                return;
            };
            assert_eq!(
                error.kind(),
                ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            );
        }
    }
}
