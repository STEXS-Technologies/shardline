mod cli;
mod definition;
mod error;
mod funcs;
mod help;

pub use cli::{CliCommand, RedactedDbUrl};
pub use definition::{BenchMode, CompletionShell};
pub use error::CliParseError;

pub(crate) use funcs::cli_definition_command;

#[cfg(test)]
pub(crate) use definition::{
    CliDefinition, CliObjectStorageAdapter, CliRepositoryProvider, CliServerFrontend,
    CliServerRole, CliTokenScope, parse_positive_usize,
};
#[cfg(test)]
pub(crate) use funcs::deduplicated_cli_frontends;

#[cfg(test)]
mod tests;
