use std::{ffi::OsString, num::NonZeroUsize};

use clap::{CommandFactory, Parser, error::ErrorKind};
use dotenvy::{from_filename, from_read_override};
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{
    DatabaseMigrationCommand, ObjectStorageAdapter, ServerFrontend, ServerRole,
};

use super::cli::{CliCommand, RedactedDbUrl};
use super::definition::{
    AdminSubcommand, BackupSubcommand, BenchMode, CliAdminTokenAuthProvider, CliDefinition,
    CliDefinitionCommand, CliObjectStorageAdapter, CliRepositoryProvider, CliServerFrontend,
    CliServerRole, CliTokenScope, ConfigSubcommand, DbMigrateSubcommand, DbSubcommand,
    GcScheduleSubcommand, GcSubcommand, HoldSubcommand, IndexSubcommand, ProviderlessSubcommand,
    RepairSubcommand, StorageSubcommand,
};
use super::error::CliParseError;

impl CliCommand {
    /// Parses a command from process arguments.
    ///
    /// # Errors
    ///
    /// Returns [`CliParseError`] when the argument vector is invalid or help/version
    /// output was requested.
    pub fn parse<I, T>(args: I) -> Result<Self, CliParseError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let mut args = args.into_iter().map(Into::into).collect::<Vec<OsString>>();
        if args.is_empty() {
            args.push(OsString::from("shardline"));
        }

        let definition = CliDefinition::try_parse_from(args).map_err(CliParseError::from)?;

        // Load the --env-file into the process environment before any
        // configuration resolution, so env vars referenced in config files
        // or by the server are available.
        // `gc schedule install --env-file` names the EnvironmentFile to emit
        // into the unit; it is not an input dotenv file for this invocation.
        let gc_schedule_install = matches!(
            &definition.command,
            CliDefinitionCommand::Gc(gc_args)
                if matches!(gc_args.command, Some(GcSubcommand::Schedule(_)))
        );
        if let Some(env_path) = &definition.env_file
            && !gc_schedule_install
        {
            from_filename(env_path).map_err(|error| {
                CliParseError::validation(
                    ErrorKind::InvalidValue,
                    format!("failed to load env file {}: {error}", env_path.display()),
                )
            })?;
        }

        // Preserve the explicit path for every configuration-consuming command.
        // `load_server_config` consumes this internal marker before auto-detection.
        // Clear a marker left by an earlier in-process parse before applying
        // this invocation's optional override.
        from_read_override(std::io::Cursor::new("SHARDLINE_CLI_CONFIG_FILE=\n")).map_err(
            |error| {
                CliParseError::validation(
                    ErrorKind::InvalidValue,
                    format!("failed to clear selected config file: {error}"),
                )
            },
        )?;
        if let Some(config_path) = &definition.config {
            let encoded = format!("SHARDLINE_CLI_CONFIG_FILE={:?}\n", config_path);
            from_read_override(std::io::Cursor::new(encoded)).map_err(|error| {
                CliParseError::validation(
                    ErrorKind::InvalidValue,
                    format!(
                        "failed to select config file {}: {error}",
                        config_path.display()
                    ),
                )
            })?;
        }

        // Load shardline.toml (--config or auto-detected) for direct
        // struct deserialization. The TOML values are applied via
        // load_server_config_from_env_with_toml later during config resolution.
        Self::try_from(definition)
    }

    /// Returns top-level help text.
    #[must_use]
    pub fn help_text() -> String {
        cli_definition_command().render_long_help().to_string()
    }
}

pub(crate) fn cli_definition_command() -> clap::Command {
    CliDefinition::command()
}

impl TryFrom<CliDefinition> for CliCommand {
    type Error = CliParseError;

    fn try_from(value: CliDefinition) -> Result<Self, Self::Error> {
        match value.command {
            CliDefinitionCommand::Providerless(args) => match args.command {
                ProviderlessSubcommand::Setup => Ok(Self::ProviderlessSetup),
            },
            CliDefinitionCommand::Serve(args) => Ok(Self::Serve {
                role: args.role.map(Into::into),
                frontends: if args.frontends.is_empty() {
                    None
                } else {
                    Some(deduplicated_cli_frontends(
                        args.frontends.into_iter().map(Into::into),
                    ))
                },
            }),
            CliDefinitionCommand::Config(args) => match args.command {
                ConfigSubcommand::Check => Ok(Self::ConfigCheck),
            },
            CliDefinitionCommand::Db(db_args) => match db_args.command {
                DbSubcommand::Migrate(migrate) => match migrate.command {
                    DbMigrateSubcommand::Up(up_args) => Ok(Self::DbMigrate {
                        database_url: up_args.database_url.map(RedactedDbUrl),
                        command: DatabaseMigrationCommand::Up {
                            steps: up_args.steps.map(NonZeroUsize::get),
                        },
                    }),
                    DbMigrateSubcommand::Down(down_args) => Ok(Self::DbMigrate {
                        database_url: down_args.database_url.map(RedactedDbUrl),
                        command: DatabaseMigrationCommand::Down {
                            steps: down_args.steps.map_or(1, NonZeroUsize::get),
                        },
                    }),
                    DbMigrateSubcommand::Status(status_args) => Ok(Self::DbMigrate {
                        database_url: status_args.database_url.map(RedactedDbUrl),
                        command: DatabaseMigrationCommand::Status,
                    }),
                },
            },
            CliDefinitionCommand::Admin(args) => match args.command {
                AdminSubcommand::Token(args) => Ok(Self::AdminToken {
                    auth_provider: args.auth_provider.into(),
                    issuer: args.issuer,
                    subject: args.subject,
                    scope: args.scope.into(),
                    provider: args.provider.into(),
                    owner: args.owner,
                    repo: args.repo,
                    revision: args.revision,
                    ttl_seconds: args.ttl_seconds,
                    key_file: args.key_file,
                    key_env: args.key_env,
                }),
            },
            CliDefinitionCommand::Fsck(args) => Ok(Self::Fsck { root: args.root }),
            CliDefinitionCommand::Index(args) => match args.command {
                IndexSubcommand::Rebuild(args) => Ok(Self::IndexRebuild { root: args.root }),
            },
            CliDefinitionCommand::Repair(args) => match args.command {
                Some(RepairSubcommand::Lifecycle(options)) => Ok(Self::RepairLifecycle {
                    root: options.root,
                    webhook_retention_seconds: options.webhook_retention_seconds,
                }),
                None => Ok(Self::Repair {
                    root: args.options.root,
                    webhook_retention_seconds: args.options.webhook_retention_seconds,
                }),
            },
            CliDefinitionCommand::Backup(args) => match args.command {
                BackupSubcommand::Manifest(args) => Ok(Self::BackupManifest {
                    root: args.root,
                    output: args.output,
                }),
            },
            CliDefinitionCommand::Storage(args) => match args.command {
                StorageSubcommand::Migrate(args) => Ok(Self::StorageMigrate {
                    from: args.from.into(),
                    from_root: args.from_root,
                    to: args.to.into(),
                    to_root: args.to_root,
                    prefix: args.prefix,
                    dry_run: args.dry_run,
                }),
            },
            CliDefinitionCommand::Gc(gc_args) => match gc_args.command {
                Some(GcSubcommand::Schedule(schedule)) => match schedule.command {
                    GcScheduleSubcommand::Install(install_args) => Ok(Self::GcScheduleInstall {
                        output_dir: install_args.output_dir,
                        unit_prefix: install_args.unit_prefix,
                        calendar: install_args.calendar,
                        retention_seconds: install_args.retention_seconds,
                        binary_path: install_args.binary_path,
                        env_file: install_args.env_file,
                        working_directory: install_args.working_directory,
                        user: install_args.user,
                        group: install_args.group,
                        dry_run: install_args.dry_run,
                    }),
                    GcScheduleSubcommand::Uninstall(uninstall_args) => {
                        Ok(Self::GcScheduleUninstall {
                            output_dir: uninstall_args.output_dir,
                            unit_prefix: uninstall_args.unit_prefix,
                        })
                    }
                },
                None => Ok(Self::Gc {
                    root: gc_args.options.root,
                    mark: gc_args.options.mark,
                    sweep: gc_args.options.sweep,
                    retention_seconds: gc_args.options.retention_seconds,
                    retention_report: gc_args.options.retention_report,
                    orphan_inventory: gc_args.options.orphan_inventory,
                }),
            },
            CliDefinitionCommand::Hold(args) => match args.command {
                HoldSubcommand::Set(args) => Ok(Self::HoldSet {
                    root: args.root,
                    object_key: args.object_key,
                    reason: args.reason,
                    ttl_seconds: args.ttl_seconds,
                }),
                HoldSubcommand::List(args) => Ok(Self::HoldList {
                    root: args.root,
                    active_only: args.active_only,
                }),
                HoldSubcommand::Release(args) => Ok(Self::HoldRelease {
                    root: args.root,
                    object_key: args.object_key,
                }),
            },
            CliDefinitionCommand::Bench(args) => {
                if args.mode == BenchMode::EndToEnd && args.storage_dir.is_none() {
                    return Err(CliParseError::validation(
                        ErrorKind::MissingRequiredArgument,
                        "end-to-end benchmark mode requires --storage-dir",
                    ));
                }

                Ok(Self::Bench {
                    mode: args.mode,
                    deployment_target: args.deployment_target,
                    scenario: args.scenario,
                    storage_dir: args.storage_dir,
                    iterations: args.iterations,
                    concurrency: args.concurrency,
                    upload_max_in_flight_chunks: args.upload_max_in_flight_chunks,
                    chunk_size_bytes: args.chunk_size_bytes,
                    base_bytes: args.base_bytes,
                    mutated_bytes: args.mutated_bytes,
                    json: args.json,
                })
            }
            CliDefinitionCommand::Health(args) => Ok(Self::Health {
                server_url: args.server_url,
            }),
            CliDefinitionCommand::Completion(args) => Ok(Self::Completion {
                shell: args.shell,
                output: args.output,
            }),
            CliDefinitionCommand::Manpage(args) => Ok(Self::Manpage {
                output: args.output,
            }),
        }
    }
}

impl From<CliServerRole> for ServerRole {
    fn from(value: CliServerRole) -> Self {
        match value {
            CliServerRole::All => Self::All,
            CliServerRole::Api => Self::Api,
            CliServerRole::Transfer => Self::Transfer,
        }
    }
}

impl From<CliServerFrontend> for ServerFrontend {
    fn from(value: CliServerFrontend) -> Self {
        match value {
            CliServerFrontend::Xet => Self::Xet,
            CliServerFrontend::Lfs => Self::Lfs,
            CliServerFrontend::BazelHttp => Self::BazelHttp,
            CliServerFrontend::Oci => Self::Oci,
            CliServerFrontend::Hub => Self::Hub,
            CliServerFrontend::S3 => Self::S3,
        }
    }
}

impl From<CliTokenScope> for TokenScope {
    fn from(value: CliTokenScope) -> Self {
        match value {
            CliTokenScope::Read => Self::Read,
            CliTokenScope::Write => Self::Write,
        }
    }
}

impl From<CliAdminTokenAuthProvider> for crate::AdminTokenAuthProvider {
    fn from(value: CliAdminTokenAuthProvider) -> Self {
        match value {
            CliAdminTokenAuthProvider::Local => Self::Local,
            CliAdminTokenAuthProvider::Ed25519 => Self::Ed25519,
        }
    }
}

impl From<CliRepositoryProvider> for RepositoryProvider {
    fn from(value: CliRepositoryProvider) -> Self {
        match value {
            CliRepositoryProvider::GitHub => Self::GitHub,
            CliRepositoryProvider::Gitea => Self::Gitea,
            CliRepositoryProvider::GitLab => Self::GitLab,
            CliRepositoryProvider::Codeberg => Self::Codeberg,
            CliRepositoryProvider::Generic => Self::Generic,
        }
    }
}

impl From<CliObjectStorageAdapter> for ObjectStorageAdapter {
    fn from(value: CliObjectStorageAdapter) -> Self {
        match value {
            CliObjectStorageAdapter::Local => Self::Local,
            CliObjectStorageAdapter::S3 => Self::S3,
        }
    }
}

pub(crate) fn deduplicated_cli_frontends(
    frontends: impl IntoIterator<Item = ServerFrontend>,
) -> Vec<ServerFrontend> {
    let mut deduplicated = Vec::new();
    for frontend in frontends {
        if !deduplicated.contains(&frontend) {
            deduplicated.push(frontend);
        }
    }
    deduplicated
}
