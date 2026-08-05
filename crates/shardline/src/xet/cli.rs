//! Clap definition for the `sdx` file-management command tree.
//!
//! This is the CLI surface the `sdx` symlink (or the `xet` escape-hatch
//! subcommand on the operator binary) routes to. It lives alongside the
//! operator commands but is a distinct, self-contained clap tree so the
//! operator surface stays unchanged.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

/// The `sdx` top-level command definition.
#[derive(Debug, Parser)]
#[command(
    name = "sdx",
    version,
    about = "Xet-native file management CLI.",
    long_about = "Transfer files to and from a Xet CAS server over the Xet protocol.\n\nUse `sdx cp`, `sdx sync`, `sdx ls`, `sdx rm`, `sdx cat`, `sdx info`, and `sdx branch` to manage content directly, without a Git workflow.",
    arg_required_else_help = true,
    next_line_help = true
)]
pub(crate) struct XetCli {
    /// Global flags shared by every subcommand.
    #[command(flatten)]
    pub(crate) global: GlobalArgs,
    #[command(subcommand)]
    pub(crate) command: XetCommand,
}

/// Flags available on every `sdx` subcommand.
#[derive(Debug, Args)]
pub(crate) struct GlobalArgs {
    /// Path to a shardline.toml configuration file.
    #[arg(short = 'c', long = "config", value_name = "FILE", global = true)]
    pub(crate) config: Option<PathBuf>,
    /// Opaque bearer token for the server endpoint.
    #[arg(long, global = true)]
    pub(crate) token: Option<String>,
    /// Provider bootstrap API key.
    #[arg(long = "api-key", global = true)]
    pub(crate) api_key: Option<String>,
    /// Path to a file containing a bearer token.
    #[arg(long = "token-file", global = true)]
    pub(crate) token_file: Option<PathBuf>,
    /// Subject identifier sent on token issuance (required by the server).
    #[arg(long, global = true)]
    pub(crate) subject: Option<String>,
}

/// The `sdx` subcommand tree.
#[derive(Debug, Subcommand)]
pub(crate) enum XetCommand {
    /// Copy files between local paths and a remote repository.
    Cp(CpArgs),
    /// Push-only directory synchronization (upload changed files).
    Sync(SyncArgs),
    /// List a remote directory or repository revisions.
    Ls(LsArgs),
    /// Remove a remote file or directory.
    Rm(RmArgs),
    /// Stream a remote file to standard output.
    Cat(CatArgs),
    /// Show metadata about a remote path or revision.
    Info(InfoArgs),
    /// List, create, or delete revisions (branches).
    Branch(BranchArgs),
}

/// Transfer options shared by `cp` and `sync`.
#[derive(Debug, Args)]
pub(crate) struct TransferFlags {
    /// CDC chunk size in bytes (power of two, greater than 64).
    #[arg(long = "chunk-size", value_name = "BYTES", value_parser = parse_chunk_size)]
    pub(crate) chunk_size: Option<usize>,
    /// Chunk/xorb compression mode.
    #[arg(long, value_enum, default_value_t = CompressionMode::Lz4)]
    pub(crate) compression: CompressionMode,
    /// Register file metadata with the server after upload (default).
    #[arg(long, conflicts_with = "no_register")]
    pub(crate) register: bool,
    /// Skip file-metadata registration after upload (data-only).
    #[arg(long, conflicts_with = "register")]
    pub(crate) no_register: bool,
    /// Recurse into directories.
    #[arg(long)]
    pub(crate) recursive: bool,
}

/// `sdx cp` arguments.
#[derive(Debug, Args)]
pub(crate) struct CpArgs {
    #[command(flatten)]
    pub(crate) transfer: TransferFlags,
    /// Source path or remote URL.
    pub(crate) src: String,
    /// Destination path or remote URL.
    pub(crate) dst: String,
}

/// `sdx sync` arguments.
#[derive(Debug, Args)]
pub(crate) struct SyncArgs {
    #[command(flatten)]
    pub(crate) transfer: TransferFlags,
    /// Source directory path or remote URL.
    pub(crate) src: String,
    /// Destination directory path or remote URL.
    pub(crate) dst: String,
}

/// `sdx ls` arguments.
#[derive(Debug, Args)]
pub(crate) struct LsArgs {
    /// Remote URL to list.
    pub(crate) url: String,
    /// Include size, mtime, and content hash.
    #[arg(long)]
    pub(crate) long: bool,
    /// List available revisions instead of directory contents.
    #[arg(long)]
    pub(crate) branches: bool,
}

/// `sdx rm` arguments.
#[derive(Debug, Args)]
pub(crate) struct RmArgs {
    /// Remote URL to remove.
    pub(crate) url: String,
    /// Remove a directory subtree.
    #[arg(long)]
    pub(crate) recursive: bool,
}

/// `sdx cat` arguments.
#[derive(Debug, Args)]
pub(crate) struct CatArgs {
    /// Remote file URL to stream.
    pub(crate) url: String,
}

/// `sdx info` arguments.
#[derive(Debug, Args)]
pub(crate) struct InfoArgs {
    /// Remote URL to inspect.
    pub(crate) url: String,
}

/// `sdx branch` arguments.
#[derive(Debug, Args)]
pub(crate) struct BranchArgs {
    /// Repository URL (identity, no path).
    pub(crate) url: String,
    /// Create a revision with this name.
    #[arg(long = "create", value_name = "REVISION", conflicts_with = "delete")]
    pub(crate) create: Option<String>,
    /// Delete a revision with this name.
    #[arg(long = "delete", value_name = "REVISION", conflicts_with = "create")]
    pub(crate) delete: Option<String>,
}

/// Chunk compression mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CompressionMode {
    /// Raw bytes, no compression.
    None,
    /// LZ4 block compression.
    Lz4,
    /// bg4lz4 compression.
    Bg4lz4,
}

/// Parses a `--chunk-size` value: a power of two greater than 64.
fn parse_chunk_size(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid chunk size {value:?}: {error}"))?;
    if parsed <= 64 || !parsed.is_power_of_two() {
        return Err("chunk size must be a power of two greater than 64".to_owned());
    }
    Ok(parsed)
}
