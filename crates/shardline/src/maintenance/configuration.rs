//! One resolved invocation, shared by evidence production and original handlers.
//! This scope lives on the existing maintenance blocking thread, never globally.
use crate::CliCommand;
use shardline_server::{ServerConfig, StorageMigrationEndpoint};
use std::{cell::RefCell, path::PathBuf};

#[derive(Clone)]
pub(crate) struct Configuration {
    pub config: ServerConfig,
    pub command: CliCommand,
    pub config_path: Option<PathBuf>,
    pub migration: Option<(StorageMigrationEndpoint, StorageMigrationEndpoint)>,
}

thread_local! {
    static CURRENT: RefCell<Option<Configuration>> = const { RefCell::new(None) };
}

struct Restore(Option<Configuration>);

impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.0.take());
        });
    }
}

pub(crate) fn scope<T>(configuration: Configuration, run: impl FnOnce() -> T) -> T {
    let _restore = Restore(CURRENT.with(|current| current.replace(Some(configuration))));
    run()
}

pub(crate) fn config() -> Option<ServerConfig> {
    CURRENT.with(|current| current.borrow().as_ref().map(|value| value.config.clone()))
}

pub(crate) fn command() -> Option<CliCommand> {
    CURRENT.with(|current| current.borrow().as_ref().map(|value| value.command.clone()))
}

pub(crate) fn config_path() -> Option<Option<PathBuf>> {
    CURRENT.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|value| value.config_path.clone())
    })
}

pub(crate) fn endpoint(side: &str) -> Option<StorageMigrationEndpoint> {
    CURRENT.with(|current| {
        let current = current.borrow();
        let (source, destination) = current.as_ref()?.migration.as_ref()?;
        match side {
            "source" => Some(source.clone()),
            "destination" => Some(destination.clone()),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests;
