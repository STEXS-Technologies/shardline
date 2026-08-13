use std::{num::NonZeroUsize, path::PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};
use shardline_server::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
};

use crate::bench::{BenchDeploymentTarget, BenchScenario};

use super::help::{
    BENCH_AFTER_LONG_HELP, CLI_AFTER_LONG_HELP, COMPLETION_AFTER_HELP, GC_INSTALL_AFTER_LONG_HELP,
    MANPAGE_AFTER_HELP,
};

// ── Top-level CLI definition ────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name = "shardline",
    version,
    about = "Content-addressed storage server and operations CLI.",
    long_about = "Shardline serves CAS protocol frontends, provider integration, storage maintenance, and operational workflows from one CLI.\n\nThe current frontend set in this repository is Xet, Git LFS, Bazel HTTP remote cache, OCI Distribution, and S3.\n\nUse `shardline help <command>` to inspect a command in detail.",
    after_help = CLI_AFTER_LONG_HELP,
    arg_required_else_help = true,
    next_line_help = true
)]
pub(crate) struct CliDefinition {
    /// Path to a .env file to load before resolving configuration.
    /// Variables in this file are set as environment variables and are
    /// available to the server and all subcommands.
    #[arg(long = "env-file", value_name = "PATH", global = true)]
    pub(crate) env_file: Option<PathBuf>,
    /// Path to a shardline.toml configuration file.
    /// When omitted, shardline.toml is auto-detected from the current
    /// directory, ~/.config/shardline/, and /etc/shardline/.
    #[arg(short = 'c', long = "config", value_name = "FILE", global = true)]
    pub(crate) config: Option<PathBuf>,
    #[command(subcommand)]
    pub(crate) command: CliDefinitionCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CliDefinitionCommand {
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

// ── Providerless ────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Bootstrap a local providerless source-checkout deployment.",
    long_about = "Create the `.shardline` local state directory, generate a signing key, and write a providerless environment file for a source checkout.\n\nThis command only prepares local state. `shardline serve` and `shardline config check` also auto-bootstrap the same layout when they run from a fresh source checkout."
)]
pub(crate) struct ProviderlessCommandArgs {
    #[command(subcommand)]
    pub(crate) command: ProviderlessSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProviderlessSubcommand {
    /// Create `.shardline/`, `.shardline/data`, the signing key, and `providerless.env`.
    Setup,
}

// ── Serve ───────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Run the Shardline server.",
    long_about = "Start the Shardline process for single-node deployments or for one split-role process.\n\nThe active environment still supplies the adapter and provider wiring. `--role` only selects which server surface this process exposes. `--frontend` selects which protocol frontends this process serves."
)]
pub(crate) struct ServeArgs {
    /// Pin the process to `all`, `api`, or `transfer`.
    #[arg(long, value_enum)]
    pub(crate) role: Option<CliServerRole>,
    /// Enable one or more protocol frontends. Repeat the flag or pass a comma-separated list.
    #[arg(long = "frontend", value_enum, value_delimiter = ',', action = clap::ArgAction::Append, num_args = 1..)]
    pub(crate) frontends: Vec<CliServerFrontend>,
}

// ── Config ──────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Validate the effective runtime configuration.",
    long_about = "Load the active Shardline environment, resolve adapters, and report the effective runtime profile without starting the server."
)]
pub(crate) struct ConfigCommandArgs {
    #[command(subcommand)]
    pub(crate) command: ConfigSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigSubcommand {
    /// Validate the active environment and adapter wiring.
    Check,
}

// ── Db (migration) ──────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Manage the Postgres metadata schema.",
    long_about = "Apply, revert, or inspect the Shardline metadata schema used by Postgres-backed index state."
)]
pub(crate) struct DbCommandArgs {
    #[command(subcommand)]
    pub(crate) command: DbSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum DbSubcommand {
    /// Apply, revert, or inspect metadata migrations.
    Migrate(DbMigrateCommandArgs),
}

#[derive(Debug, Args)]
pub(crate) struct DbMigrateCommandArgs {
    #[command(subcommand)]
    pub(crate) command: DbMigrateSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum DbMigrateSubcommand {
    /// Apply pending migrations.
    Up(DbMigrateUpArgs),
    /// Revert applied migrations.
    Down(DbMigrateDownArgs),
    /// Show applied and pending migrations.
    Status(DbMigrateStatusArgs),
}

#[derive(Debug, Args)]
pub(crate) struct DbMigrateUpArgs {
    /// Override the configured Postgres metadata URL.
    #[arg(long)]
    pub(crate) database_url: Option<String>,
    /// Limit the number of migrations to apply.
    #[arg(long, value_parser = parse_positive_usize)]
    pub(crate) steps: Option<NonZeroUsize>,
}

#[derive(Debug, Args)]
pub(crate) struct DbMigrateDownArgs {
    /// Override the configured Postgres metadata URL.
    #[arg(long)]
    pub(crate) database_url: Option<String>,
    /// Limit the number of migrations to revert.
    #[arg(long, value_parser = parse_positive_usize)]
    pub(crate) steps: Option<NonZeroUsize>,
}

#[derive(Debug, Args)]
pub(crate) struct DbMigrateStatusArgs {
    /// Override the configured Postgres metadata URL.
    #[arg(long)]
    pub(crate) database_url: Option<String>,
}

// ── Admin ───────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Mint a local administrative token.",
    long_about = "Create a signed bearer token for self-hosted operations, provider mediation, or debugging workflows."
)]
pub(crate) struct AdminCommandArgs {
    #[command(subcommand)]
    pub(crate) command: AdminSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminSubcommand {
    /// Mint a local bearer token for self-hosted operation and testing.
    Token(AdminTokenArgs),
}

#[derive(Debug, Args)]
pub(crate) struct AdminTokenArgs {
    /// Token issuer identifier.
    #[arg(long)]
    pub(crate) issuer: String,
    /// Token subject identifier.
    #[arg(long)]
    pub(crate) subject: String,
    /// Granted repository scope.
    #[arg(long, value_enum)]
    pub(crate) scope: CliTokenScope,
    /// Repository hosting provider.
    #[arg(long, value_enum)]
    pub(crate) provider: CliRepositoryProvider,
    /// Repository owner or namespace.
    #[arg(long)]
    pub(crate) owner: String,
    /// Repository name.
    #[arg(long)]
    pub(crate) repo: String,
    /// Scoped revision when one is required.
    #[arg(long)]
    pub(crate) revision: Option<String>,
    /// Token lifetime in seconds.
    #[arg(long, default_value_t = 3_600_u64)]
    pub(crate) ttl_seconds: u64,
    /// Local signing-key file path.
    #[arg(long, conflicts_with = "key_env", required_unless_present = "key_env")]
    pub(crate) key_file: Option<PathBuf>,
    /// Environment variable that stores the signing key.
    #[arg(
        long,
        conflicts_with = "key_file",
        required_unless_present = "key_file"
    )]
    pub(crate) key_env: Option<String>,
}

// ── Index ───────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub(crate) struct IndexCommandArgs {
    #[command(subcommand)]
    pub(crate) command: IndexSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum IndexSubcommand {
    /// Rebuild latest-file and dedupe indexes from immutable history.
    Rebuild(RootArgs),
}

// ── Repair ──────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Repair lifecycle metadata and webhook delivery state.",
    long_about = "Run the repair orchestrator for local or Postgres-backed metadata.\n\n`repair` runs the full repair flow. `repair lifecycle` limits the run to lifecycle-specific reconciliation."
)]
pub(crate) struct RepairCommandArgs {
    #[command(subcommand)]
    pub(crate) command: Option<RepairSubcommand>,
    #[command(flatten)]
    pub(crate) options: RepairOptionsArgs,
}

#[derive(Debug, Subcommand)]
pub(crate) enum RepairSubcommand {
    /// Repair lifecycle state only.
    Lifecycle(RepairOptionsArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct RepairOptionsArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
    /// Retention window for processed webhook-delivery claims.
    #[arg(
        long,
        default_value_t = DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
    )]
    pub(crate) webhook_retention_seconds: u64,
}

// ── Backup ──────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Export recovery artifacts.",
    long_about = "Generate recovery data that can be used to audit or rebuild Shardline metadata state."
)]
pub(crate) struct BackupCommandArgs {
    #[command(subcommand)]
    pub(crate) command: BackupSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum BackupSubcommand {
    /// Export an adapter-neutral backup manifest.
    Manifest(BackupManifestArgs),
}

#[derive(Debug, Args)]
pub(crate) struct BackupManifestArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
    /// Manifest output path.
    #[arg(long)]
    pub(crate) output: PathBuf,
}

// ── Storage migrate ─────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Copy immutable objects between storage adapters.",
    long_about = "Inventory immutable payload objects under one object-storage adapter and copy them into another adapter.\n\nThis is intended for storage migrations, dry runs, and object namespace moves."
)]
pub(crate) struct StorageCommandArgs {
    #[command(subcommand)]
    pub(crate) command: StorageSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum StorageSubcommand {
    /// Copy immutable payload objects between object-storage adapters.
    Migrate(StorageMigrateArgs),
}

#[derive(Debug, Args)]
pub(crate) struct StorageMigrateArgs {
    /// Source object-storage adapter.
    #[arg(long, value_enum)]
    pub(crate) from: CliObjectStorageAdapter,
    /// Local state root when the source adapter is `local`.
    #[arg(long)]
    pub(crate) from_root: Option<PathBuf>,
    /// Destination object-storage adapter.
    #[arg(long, value_enum)]
    pub(crate) to: CliObjectStorageAdapter,
    /// Local state root when the destination adapter is `local`.
    #[arg(long)]
    pub(crate) to_root: Option<PathBuf>,
    /// Object-key prefix to migrate.
    #[arg(long, default_value_t = String::new())]
    pub(crate) prefix: String,
    /// Inventory objects without writing destination payloads.
    #[arg(long)]
    pub(crate) dry_run: bool,
}

// ── Gc ──────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Run garbage collection or install a schedule.",
    long_about = "Run mark, sweep, or dry-run garbage collection against the active metadata and object adapters.\n\nUse `gc schedule` to generate validated systemd units for recurring collector runs."
)]
pub(crate) struct GcCommandArgs {
    #[command(subcommand)]
    pub(crate) command: Option<GcSubcommand>,
    #[command(flatten)]
    pub(crate) options: GcOptionsArgs,
}

#[derive(Debug, Subcommand)]
pub(crate) enum GcSubcommand {
    /// Install or remove a systemd timer for garbage collection.
    Schedule(GcScheduleCommandArgs),
}

#[derive(Debug, Args)]
#[command(
    about = "Install or remove a systemd timer for garbage collection.",
    long_about = "Generate or remove a systemd `.service` and `.timer` pair for scheduled garbage collection.\n\nThe install workflow validates the target binary, environment file, user, group, and referenced secret/config files before writing units."
)]
pub(crate) struct GcScheduleCommandArgs {
    #[command(subcommand)]
    pub(crate) command: GcScheduleSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum GcScheduleSubcommand {
    /// Generate validated systemd units for scheduled garbage collection.
    Install(GcScheduleInstallArgs),
    /// Remove generated garbage-collection systemd units.
    Uninstall(GcScheduleUninstallArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct GcOptionsArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
    /// Persist currently orphaned chunks into quarantine.
    #[arg(long)]
    pub(crate) mark: bool,
    /// Delete eligible orphaned chunks after reporting them.
    #[arg(long)]
    pub(crate) sweep: bool,
    /// Retention window for newly quarantined chunks.
    #[arg(long, default_value_t = DEFAULT_LOCAL_GC_RETENTION_SECONDS)]
    pub(crate) retention_seconds: u64,
    /// Write active quarantine state to a JSON report.
    #[arg(long)]
    pub(crate) retention_report: Option<PathBuf>,
    /// Write the current orphan inventory to a JSON report.
    #[arg(long)]
    pub(crate) orphan_inventory: Option<PathBuf>,
}

#[derive(Debug, Args)]
#[command(
    about = "Generate validated systemd units for scheduled garbage collection.",
    long_about = "Write a `systemd` service and timer for `shardline gc`.\n\nThe installer resolves the active Shardline binary when the default path is left in place, requires the environment file to exist, honors `SHARDLINE_ROOT_DIR`, and validates referenced secret/config files plus the selected service user and group.",
    after_help = GC_INSTALL_AFTER_LONG_HELP
)]
pub(crate) struct GcScheduleInstallArgs {
    /// Directory that receives the generated systemd units.
    #[arg(long, default_value = "/etc/systemd/system")]
    pub(crate) output_dir: PathBuf,
    /// Unit basename without `.service` or `.timer`.
    #[arg(long, default_value = "shardline-gc")]
    pub(crate) unit_prefix: String,
    /// `systemd.timer` calendar expression.
    #[arg(long, default_value = "*-*-* 03:17:00")]
    pub(crate) calendar: String,
    /// Retention window passed to the scheduled collector.
    #[arg(long, default_value_t = 86_400_u64)]
    pub(crate) retention_seconds: u64,
    /// Path to the `shardline` binary embedded in the unit.
    #[arg(long, default_value = "/usr/local/bin/shardline")]
    pub(crate) binary_path: PathBuf,
    /// Environment file referenced by the generated service.
    #[arg(long, default_value = "/etc/shardline/shardline.env")]
    pub(crate) env_file: PathBuf,
    /// Working directory and writable state path.
    #[arg(long, default_value = "/var/lib/shardline")]
    pub(crate) working_directory: PathBuf,
    /// Service user.
    #[arg(long, default_value = "shardline")]
    pub(crate) user: String,
    /// Service group.
    #[arg(long, default_value = "shardline")]
    pub(crate) group: String,
    /// Print the generated unit files to stdout instead of writing them.
    #[arg(long)]
    pub(crate) dry_run: bool,
}

#[derive(Debug, Args)]
pub(crate) struct GcScheduleUninstallArgs {
    /// Directory that contains the generated systemd units.
    #[arg(long, default_value = "/etc/systemd/system")]
    pub(crate) output_dir: PathBuf,
    /// Unit basename without `.service` or `.timer`.
    #[arg(long, default_value = "shardline-gc")]
    pub(crate) unit_prefix: String,
}

// ── Hold ────────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Manage retention holds.",
    long_about = "Create, list, and release retention holds that protect object keys from garbage collection."
)]
pub(crate) struct HoldCommandArgs {
    #[command(subcommand)]
    pub(crate) command: HoldSubcommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum HoldSubcommand {
    /// Create or update a retention hold.
    Set(HoldSetArgs),
    /// List retention holds.
    List(HoldListArgs),
    /// Release one retention hold.
    Release(HoldReleaseArgs),
}

#[derive(Debug, Args)]
pub(crate) struct HoldSetArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
    /// Object-store key protected by the hold.
    #[arg(long)]
    pub(crate) object_key: String,
    /// Human-readable hold reason.
    #[arg(long)]
    pub(crate) reason: String,
    /// Optional hold time-to-live in seconds.
    #[arg(long)]
    pub(crate) ttl_seconds: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct HoldListArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
    /// Exclude expired holds.
    #[arg(long)]
    pub(crate) active_only: bool,
}

#[derive(Debug, Args)]
pub(crate) struct HoldReleaseArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
    /// Object-store key released from protection.
    #[arg(long)]
    pub(crate) object_key: String,
}

// ── Bench ───────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Run performance benchmarks.",
    long_about = "Measure upload, download, reconstruction, and concurrency behavior.\n\n`e2e` mode can run either an isolated local SQLite plus filesystem deployment or the active configured runtime backend, and requires `--storage-dir`. `ingest` mode measures upload ingestion without storing payloads.",
    after_help = BENCH_AFTER_LONG_HELP
)]
pub(crate) struct BenchArgs {
    /// Benchmark mode.
    #[arg(long, value_enum, default_value_t = BenchMode::EndToEnd)]
    pub(crate) mode: BenchMode,
    /// End-to-end benchmark deployment target.
    #[arg(long, value_enum, default_value_t = BenchDeploymentTarget::IsolatedLocal)]
    pub(crate) deployment_target: BenchDeploymentTarget,
    /// Focus one benchmark scenario instead of running all steps.
    #[arg(long, value_enum, default_value_t = BenchScenario::Full)]
    pub(crate) scenario: BenchScenario,
    /// Root directory used to create isolated benchmark iteration stores.
    #[arg(long)]
    pub(crate) storage_dir: Option<PathBuf>,
    /// Number of benchmark iterations to run.
    #[arg(long, default_value_t = 1_u32)]
    pub(crate) iterations: u32,
    /// Number of concurrent workers used for concurrent sub-scenarios.
    #[arg(long, default_value_t = 4_u32)]
    pub(crate) concurrency: u32,
    /// Maximum upload chunks processed in parallel per upload.
    #[arg(long, default_value_t = 64_usize)]
    pub(crate) upload_max_in_flight_chunks: usize,
    /// Chunk size in bytes used by the local benchmark backend.
    #[arg(long, default_value_t = 65_536_usize)]
    pub(crate) chunk_size_bytes: usize,
    /// Logical size of the benchmark asset in bytes.
    #[arg(long, default_value_t = 1_048_576_usize)]
    pub(crate) base_bytes: usize,
    /// Number of bytes changed in the sparse-update benchmark step.
    #[arg(long, default_value_t = 4_096_usize)]
    pub(crate) mutated_bytes: usize,
    /// Emit the full report as JSON.
    #[arg(long)]
    pub(crate) json: bool,
}

// ── Health ──────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Probe server health.",
    long_about = "Send a health probe to a running Shardline server and fail when the server does not answer successfully."
)]
pub(crate) struct HealthArgs {
    /// Base URL of the Shardline server to probe.
    #[arg(long = "server")]
    pub(crate) server_url: String,
}

// ── Completion ──────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Generate shell-completion scripts for supported shells.",
    long_about = "Render a completion script from the live Shardline CLI definition.\n\nThis keeps shell completions aligned with the real command surface instead of shipping a handwritten static script.",
    after_help = COMPLETION_AFTER_HELP
)]
pub(crate) struct CompletionArgs {
    /// Target shell.
    #[arg(value_enum)]
    pub(crate) shell: CompletionShell,
    /// Write the generated script to one file instead of stdout.
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

// ── Manpage ─────────────────────────────────────────────────────────────

#[derive(Debug, Args)]
#[command(
    about = "Generate a manpage for packaged or self-hosted deployments.",
    long_about = "Render a manpage from the live Shardline CLI definition.\n\nThis is intended for packaging, system installations, and offline operator documentation.",
    after_help = MANPAGE_AFTER_HELP
)]
pub(crate) struct ManpageArgs {
    /// Write the generated manpage to one file instead of stdout.
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

// ── Shared arg structs ──────────────────────────────────────────────────

#[derive(Debug, Default, Args)]
pub(crate) struct RootArgs {
    /// Override the deployment root for local metadata state.
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,
}

// ── CLI value enums ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CliServerRole {
    /// Serve both control-plane and transfer routes from one process.
    All,
    /// Serve control-plane and metadata routes only.
    Api,
    /// Serve upload and download routes only.
    Transfer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CliServerFrontend {
    /// Serve the validated Xet-compatible CAS frontend.
    Xet,
    /// Serve the Git LFS batch and object-transfer frontend.
    Lfs,
    /// Serve the Bazel-compatible HTTP remote-cache frontend.
    #[value(name = "bazel-http")]
    BazelHttp,
    /// Serve the OCI Distribution frontend.
    Oci,
    /// Serve the HuggingFace Hub API compatibility frontend.
    Hub,
    /// Serve the S3-compatible object-storage frontend.
    S3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CliTokenScope {
    /// Allow read-only access.
    Read,
    /// Allow writes and uploads.
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CliRepositoryProvider {
    /// GitHub repository hosting.
    #[value(name = "github")]
    GitHub,
    /// Gitea repository hosting.
    #[value(name = "gitea")]
    Gitea,
    /// GitLab repository hosting.
    #[value(name = "gitlab")]
    GitLab,
    /// Codeberg (Gitea-based) repository hosting.
    #[value(name = "codeberg")]
    Codeberg,
    /// Generic Git provider integration.
    #[value(name = "generic")]
    Generic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CliObjectStorageAdapter {
    /// Local filesystem-backed object storage.
    Local,
    /// S3-compatible object storage.
    S3,
}

// ── Public value enums (used in CliCommand fields) ──────────────────────

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

// ── Helpers used in value_parser attributes ─────────────────────────────

pub(crate) fn parse_positive_usize(value: &str) -> Result<NonZeroUsize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|e| format!("value must be a positive integer: {e}"))?;
    NonZeroUsize::new(parsed).ok_or_else(|| "value must be a positive integer".to_owned())
}
