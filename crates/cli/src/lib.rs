#![deny(unsafe_code)]

//! Library surface behind the `shardline` command-line entry point.
//!
//! The binary target is intentionally thin: it delegates parsing, artifact
//! generation, configuration loading, and operator workflows to this crate. That
//! keeps command behavior testable and lets integration tests exercise the same
//! code paths as the installed `shardline` executable.
//!
//! The most commonly embedded pieces are [`CliCommand`] for parsing, [`render_manpage`]
//! and [`render_completion`] for packaging artifacts, and [`run_providerless_setup`]
//! for bootstrapping a local host deployment.
//!
//! # Example
//!
//! ```
//! use shardline::{CliCommand, CompletionShell, render_completion};
//!
//! let command = CliCommand::parse(["shardline", "config", "check"])?;
//! assert!(matches!(command, CliCommand::ConfigCheck));
//!
//! let completion = render_completion(CompletionShell::Bash)?;
//! assert!(completion.contains("shardline"));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod adapter;
mod admin;
mod artifact;
mod backup;
mod bench;
mod command;
mod config;
mod db;
mod fsck;
mod gc;
mod gc_schedule;
mod hold;
mod local_output;
mod local_path;
mod providerless;
mod rebuild;
mod repair;
mod storage_migration;

pub use adapter::{CliRuntimeError, run_health_check};
pub use admin::{AdminTokenError, mint_admin_token, mint_admin_token_from_sources};
pub use artifact::{CliArtifactError, render_completion, render_manpage};
pub use backup::{BackupRuntimeError, run_backup_manifest};
pub use bench::{
    BenchConfig, BenchDeploymentTarget, BenchInventoryScope, BenchReport, BenchRuntimeError,
    BenchScenario, IngestBenchReport, run_bench, run_ingest_bench,
};
pub use command::{BenchMode, CliCommand, CliParseError, CompletionShell};
pub use config::{
    ConfigRuntimeError, effective_root, load_server_config, run_config_check_from_env,
};
pub use db::{DbRuntimeError, run_db_migration};
pub use fsck::{FsckRuntimeError, run_fsck};
pub use gc::{GcRuntimeError, run_gc, run_gc_diagnostics};
pub use gc_schedule::{
    GcScheduleError, GcScheduleInstallOptions, GcScheduleInstallReport, GcScheduleUninstallReport,
    install_gc_schedule, uninstall_gc_schedule,
};
pub use hold::{HoldRuntimeError, run_hold_list, run_hold_release, run_hold_set};
pub use local_output::write_output_bytes;
pub use providerless::{
    ProviderlessRuntimeError, ProviderlessSetupReport, load_runtime_server_config,
    run_providerless_setup,
};
pub use rebuild::{RebuildRuntimeError, run_index_rebuild};
pub use repair::{RepairReport, RepairRuntimeError, run_lifecycle_repair, run_repair};
pub use storage_migration::{StorageMigrationRuntimeError, run_storage_migration};
