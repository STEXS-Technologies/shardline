use std::ffi::OsString;
use std::io::Write;
use std::process::ExitCode;

use clap::{CommandFactory, Parser};

use super::cli::{XetCli, XetCommand};
use super::commands;
use super::error::XetError;
use super::resolve::load_config;

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
                    diagnose(format_args!("sdx: {error}"));
                    return ExitCode::from(2);
                }
            };
            match run_command(&cli, config.as_ref()).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    diagnose(format_args!("sdx: {error}"));
                    ExitCode::from(2)
                }
            }
        }
        Err(error) => parse_failure(
            &error,
            &mut std::io::stdout().lock(),
            &mut std::io::stderr().lock(),
        ),
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

fn write_message(writer: &mut impl Write, message: std::fmt::Arguments<'_>) -> std::io::Result<()> {
    write!(writer, "{message}")?;
    writer.flush()
}

fn diagnose(message: std::fmt::Arguments<'_>) {
    let _output_result = write_message(&mut std::io::stderr().lock(), format_args!("{message}\n"));
}

fn parse_failure(
    error: &clap::Error,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> ExitCode {
    use clap::error::ErrorKind;
    if matches!(
        error.kind(),
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
    ) {
        match write_message(stdout, format_args!("{error}")) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                let _output_result = write_message(stderr, format_args!("sdx: {error}\n"));
                ExitCode::from(2)
            }
        }
    } else {
        let _output_result = write_message(stderr, format_args!("{error}"));
        ExitCode::from(2)
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;

    struct FailingWriter {
        flush_only: bool,
    }

    impl Write for FailingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.flush_only {
                Ok(bytes.len())
            } else {
                Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
    }

    #[test]
    fn help_and_version_preserve_text_and_exit_on_write_or_flush_failure() {
        for option in ["--help", "--version"] {
            let error = XetCli::try_parse_from(["sdx", option]).unwrap_err();
            let mut healthy = Vec::new();
            assert_eq!(
                parse_failure(&error, &mut healthy, &mut Vec::new()),
                ExitCode::SUCCESS
            );
            assert_eq!(healthy, error.to_string().into_bytes());
            for flush_only in [false, true] {
                assert_eq!(
                    parse_failure(
                        &error,
                        &mut FailingWriter { flush_only },
                        &mut FailingWriter { flush_only: false }
                    ),
                    ExitCode::from(2)
                );
            }
        }
    }

    #[test]
    fn invalid_option_diagnostic_failure_keeps_usage_exit() {
        let error = XetCli::try_parse_from(["sdx", "--invalid-option"]).unwrap_err();
        for flush_only in [false, true] {
            assert_eq!(
                parse_failure(&error, &mut Vec::new(), &mut FailingWriter { flush_only }),
                ExitCode::from(2)
            );
        }
    }
}
