//! The `sdx` file-management CLI lane.
//!
//! This module implements the command tree that the `sdx` symlink (and the
//! `xet` escape-hatch subcommand on the operator binary) routes to. It is a
//! thin clap wrapper over the `sdx` client library, providing
//! `cp`/`sync`/`ls`/`rm`/`cat`/`info`/`branch` against any Xet CAS server.

mod cli;
mod command;
mod commands;
mod error;
mod resolve;

pub(crate) use command::{run_xet, xet_cli_command};
