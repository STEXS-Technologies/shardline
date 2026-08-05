//! The `sdx` file-management CLI lane.
//!
//! This module implements the command tree that the `sdx` symlink (and the
//! `xet` escape-hatch subcommand on the operator binary) routes to. It is a
//! thin clap wrapper over the `sdx` client library, providing
//! `cp`/`sync`/`ls`/`rm`/`cat`/`info`/`branch` against any Xet CAS server.

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{CommandFactory, Parser};

use crate::xet::cli::{XetCli, XetCommand};
use crate::xet::error::XetError;
use crate::xet::resolve::load_config;

mod cli;
mod commands;
mod error;
mod resolve;

/// Returns the clap [`Command`] for the `sdx` tree (used for manpage and
/// shell-completion rendering).
pub(crate) fn xet_cli_command() -> clap::Command {
    XetCli::command()
}

/// Runs the `sdx` command lane to completion, returning a process exit code.
///
/// `args` includes `argv[0]` (which is `sdx` when invoked via the symlink, or
/// `shardline xet` when used as the escape hatch).
pub(crate) async fn run_xet(args: Vec<OsString>) -> ExitCode {
    match XetCli::try_parse_from(args) {
        Ok(cli) => {
            let config = match load_config(cli.global.config.as_deref()) {
                Ok(config) => config,
                Err(error) => {
                    eprintln!("sdx: {error}");
                    return ExitCode::from(2);
                }
            };
            match run_command(&cli, config.as_ref()).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("sdx: {error}");
                    ExitCode::from(2)
                }
            }
        }
        Err(error) => {
            use clap::error::ErrorKind;
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) {
                print!("{error}");
                ExitCode::SUCCESS
            } else {
                eprint!("{error}");
                ExitCode::from(2)
            }
        }
    }
}

/// Dispatches a parsed `sdx` invocation to its command handler.
async fn run_command(cli: &XetCli, config: Option<&sdx::SdxConfig>) -> Result<(), XetError> {
    match &cli.command {
        XetCommand::Cp(args) => commands::cp(&cli.global, args, config).await,
        XetCommand::Sync(args) => commands::sync(&cli.global, args, config).await,
        XetCommand::Ls(args) => commands::ls(&cli.global, args, config).await,
        XetCommand::Rm(args) => commands::rm(&cli.global, args, config).await,
        XetCommand::Cat(args) => commands::cat(&cli.global, args, config).await,
        XetCommand::Info(args) => commands::info(&cli.global, args, config).await,
        XetCommand::Branch(args) => commands::branch(&cli.global, args, config).await,
    }
}
