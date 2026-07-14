use std::{
    io::{Error as IoError, Write},
    string::FromUtf8Error,
};

use clap_mangen::Man;
use thiserror::Error;

use crate::command::{CompletionShell, cli_definition_command};

/// CLI artifact generation failure.
#[derive(Debug, Error)]
pub enum CliArtifactError {
    /// Artifact rendering failed while writing to the provided output.
    #[error(transparent)]
    Io(#[from] IoError),
    /// Generated output was not valid UTF-8.
    #[error(transparent)]
    Utf8(#[from] FromUtf8Error),
}

/// Render a shell-completion script into one UTF-8 string.
///
/// # Errors
///
/// Returns [`CliArtifactError`] when the generated output cannot be rendered or encoded.
pub fn render_completion(shell: CompletionShell) -> Result<String, CliArtifactError> {
    let mut output = Vec::new();
    write_completion(&mut output, shell);
    Ok(String::from_utf8(output)?)
}

/// Render the Shardline manpage into one UTF-8 string.
///
/// # Errors
///
/// Returns [`CliArtifactError`] when the generated output cannot be rendered or encoded.
pub fn render_manpage() -> Result<String, CliArtifactError> {
    let mut output = Vec::new();
    write_manpage(&mut output)?;
    Ok(String::from_utf8(output)?)
}

/// Write a shell-completion script to one output writer.
///
pub fn write_completion<W>(writer: &mut W, shell: CompletionShell)
where
    W: Write,
{
    let mut command = cli_definition_command();
    let command_name = command.get_name().to_owned();
    match shell {
        CompletionShell::Bash => {
            clap_complete::generate(
                clap_complete::Shell::Bash,
                &mut command,
                command_name,
                writer,
            );
        }
        CompletionShell::Elvish => {
            clap_complete::generate(
                clap_complete::Shell::Elvish,
                &mut command,
                command_name,
                writer,
            );
        }
        CompletionShell::Fish => {
            clap_complete::generate(
                clap_complete::Shell::Fish,
                &mut command,
                command_name,
                writer,
            );
        }
        CompletionShell::PowerShell => {
            clap_complete::generate(
                clap_complete::Shell::PowerShell,
                &mut command,
                command_name,
                writer,
            );
        }
        CompletionShell::Zsh => {
            clap_complete::generate(
                clap_complete::Shell::Zsh,
                &mut command,
                command_name,
                writer,
            );
        }
    }
}

/// Write the generated manpage to one output writer.
///
/// # Errors
///
/// Returns [`CliArtifactError`] when writing the generated manpage fails.
pub fn write_manpage<W>(writer: &mut W) -> Result<(), CliArtifactError>
where
    W: Write,
{
    let command = cli_definition_command();
    Man::new(command).render(writer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{render_completion, render_manpage, write_completion, CliArtifactError};
    use crate::command::CompletionShell;

    #[test]
    fn bash_completion_mentions_shardline() {
        let rendered = render_completion(CompletionShell::Bash).unwrap();
        assert!(rendered.contains("shardline"));
        assert!(rendered.contains("complete"));
    }

    #[test]
    fn manpage_mentions_core_commands() {
        let rendered = render_manpage().unwrap();
        assert!(rendered.contains(".TH shardline"));
        assert!(rendered.contains("gc"));
        assert!(rendered.contains("bench"));
    }

    // ── All completion shells via write_completion ─────────────────────────

    fn write_completion_to_string(shell: CompletionShell) -> String {
        let mut buf = Vec::new();
        write_completion(&mut buf, shell);
        String::from_utf8(buf).expect("completion output must be valid UTF-8")
    }

    #[test]
    fn elvish_completion_contains_shardline() {
        let output = write_completion_to_string(CompletionShell::Elvish);
        assert!(output.contains("shardline"), "Elvish completion should mention shardline");
        assert!(output.contains("edit:"), "Elvish completion should contain edit: namespace");
    }

    #[test]
    fn fish_completion_contains_shardline() {
        let output = write_completion_to_string(CompletionShell::Fish);
        assert!(output.contains("shardline"), "Fish completion should mention shardline");
        assert!(output.contains("complete"), "Fish completion should contain complete statement");
    }

    #[test]
    fn powershell_completion_contains_shardline() {
        let output = write_completion_to_string(CompletionShell::PowerShell);
        assert!(output.contains("shardline"), "PowerShell completion should mention shardline");
        assert!(output.contains("Register-ArgumentCompleter"), "PowerShell should register an argument completer");
    }

    #[test]
    fn zsh_completion_contains_shardline() {
        let output = write_completion_to_string(CompletionShell::Zsh);
        assert!(output.contains("shardline"), "Zsh completion should mention shardline");
        assert!(output.contains("#compdef"), "Zsh completion should have compdef directive");
    }

    // ── CliArtifactError Display / Debug ──────────────────────────────────

    #[test]
    fn cli_artifact_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "write denied");
        let err = CliArtifactError::Io(io_err);
        let msg = err.to_string();
        assert!(msg.contains("write denied"));
    }

    #[test]
    fn cli_artifact_error_utf8_display() {
        let invalid = vec![0xff, 0xfe];
        let utf8_err = String::from_utf8(invalid).unwrap_err();
        let err = CliArtifactError::Utf8(utf8_err);
        let msg = err.to_string();
        assert!(msg.contains("invalid utf-8"));
    }

    #[test]
    fn cli_artifact_error_debug() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err = CliArtifactError::Io(io_err);
        let debug = format!("{err:?}");
        assert!(debug.contains("Io("));
    }
}
