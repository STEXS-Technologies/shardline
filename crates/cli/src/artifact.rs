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
    use super::{render_completion, render_manpage};
    use crate::command::CompletionShell;

    #[test]
    fn bash_completion_mentions_shardline() {
        let rendered = render_completion(CompletionShell::Bash);
        assert!(rendered.is_ok());
        let Ok(rendered) = rendered else {
            return;
        };
        assert!(rendered.contains("shardline"));
        assert!(rendered.contains("complete"));
    }

    #[test]
    fn manpage_mentions_core_commands() {
        let rendered = render_manpage();
        assert!(rendered.is_ok());
        let Ok(rendered) = rendered else {
            return;
        };
        assert!(rendered.contains(".TH shardline"));
        assert!(rendered.contains("gc"));
        assert!(rendered.contains("bench"));
    }
}
