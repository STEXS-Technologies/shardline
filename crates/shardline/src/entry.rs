#![deny(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::ffi::OsString;
use std::io::{self, Write};
use std::process::ExitCode;

use serde_json::to_string_pretty;
use shardline_protocol::RepositoryScope;
use shardline_server::serve;

use crate::{
    BenchConfig, BenchMode, CliCommand, GcScheduleInstallOptions, MINIMUM_GC_RETENTION_SECONDS,
    load_runtime_server_config, local_output::write_error_chain, local_path::resolve_root,
    mint_admin_token_for_provider_from_sources, render_completion, render_manpage, report_output,
    run_backup_manifest, run_bench, run_config_check_from_env, run_db_migration, run_fsck, run_gc,
    run_health_check, run_hold_list, run_hold_release, run_hold_set, run_index_rebuild,
    run_ingest_bench, run_lfs_evidence_repair, run_lifecycle_repair, run_local_db_migration,
    run_providerless_setup, run_repair, run_storage_migration, uninstall_gc_schedule,
    write_output_bytes,
};

pub async fn run(args: impl Iterator<Item = OsString>) -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args: Vec<OsString> = args.collect();
    if let Some(xet_args) = xet_args(&args) {
        return crate::xet::run_xet(xet_args).await;
    }

    match CliCommand::parse(args) {
        Ok(CliCommand::ProviderlessSetup) => match run_providerless_setup(None) {
            Ok(report) => output_status(
                report.write_summary(&mut io::stdout().lock()),
                ExitCode::SUCCESS,
            ),
            Err(error) => error_chain_status(&error, ExitCode::from(2)),
        },
        Ok(CliCommand::Serve { role, frontends }) => match load_runtime_server_config(None) {
            Ok(config) => {
                let mut config = if let Some(role) = role {
                    config.with_server_role(role)
                } else {
                    config
                };
                if let Some(frontends) = frontends {
                    match config.with_server_frontends(frontends) {
                        Ok(updated) => {
                            config = updated;
                        }
                        Err(error) => {
                            return error_chain_status(&error, ExitCode::from(2));
                        }
                    }
                }
                match serve(config).await {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(error) => error_chain_status(&error, ExitCode::FAILURE),
                }
            }
            Err(error) => error_chain_status(&error, ExitCode::from(2)),
        },
        Ok(CliCommand::ConfigCheck) => match run_config_check_from_env().await {
            Ok(report) => output_status(
                report_output::write_config_check_summary(&mut io::stdout().lock(), &report),
                ExitCode::SUCCESS,
            ),
            Err(error) => error_chain_status(&error, ExitCode::from(2)),
        },
        Ok(CliCommand::DbMigrate {
            database_url,
            command,
        }) => match run_db_migration(database_url.as_ref().map(|r| r.as_str()), command).await {
            Ok(report) => output_status(
                report_output::write_database_migration_summary(&mut io::stdout().lock(), &report),
                ExitCode::SUCCESS,
            ),
            Err(error) => error_chain_status(&error, ExitCode::from(2)),
        },
        Ok(CliCommand::DbMigrateLocalUp { root }) => match run_local_db_migration(&root) {
            Ok(()) => stdout_text(format_args!(
                "local SQLite migrations applied: {}\n",
                root.display()
            )),
            Err(error) => error_chain_status(&error, ExitCode::from(2)),
        },
        Ok(CliCommand::AdminToken {
            auth_provider,
            issuer,
            subject,
            scope,
            provider,
            owner,
            repo,
            revision,
            ttl_seconds,
            key_file,
            key_env,
        }) => match RepositoryScope::new(provider, &owner, &repo, revision.as_deref()) {
            Ok(repository) => {
                match mint_admin_token_for_provider_from_sources(
                    auth_provider,
                    key_file.as_deref(),
                    key_env.as_deref(),
                    &issuer,
                    &subject,
                    scope,
                    repository,
                    ttl_seconds,
                ) {
                    Ok(token) => stdout_text(format_args!("{token}\n")),
                    Err(error) => report_output_error(&error),
                }
            }
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::RepairHubTree { root, state_file }) => {
            match crate::run_hub_tree_repair(root.as_deref(), &state_file).await {
                Ok(revision) => stdout_text(format_args!("recovered Hub revision: {revision}\n")),
                Err(error) => error_chain_status(&error, ExitCode::from(2)),
            }
        }
        Ok(CliCommand::Fsck { root }) => match run_fsck(root.as_deref()).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        return report_output_error(&error);
                    }
                };
                if let Err(error) =
                    report_output::write_fsck_cli_summary(&mut io::stdout().lock(), &report, &root)
                {
                    return report_output_error(&error);
                }
                if report.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    if let Err(error) =
                        report_output::write_fsck_issues(&mut io::stderr().lock(), &report)
                    {
                        return report_output_error(&error);
                    }
                    ExitCode::FAILURE
                }
            }
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::IndexRebuild { root }) => match run_index_rebuild(root.as_deref()).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        return report_output_error(&error);
                    }
                };
                if let Err(error) = report_output::write_index_rebuild_cli_summary(
                    &mut io::stdout().lock(),
                    &report,
                    &root,
                ) {
                    return report_output_error(&error);
                }
                if report.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    if let Err(error) =
                        report_output::write_index_rebuild_issues(&mut io::stderr().lock(), &report)
                    {
                        return report_output_error(&error);
                    }
                    ExitCode::FAILURE
                }
            }
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::Repair {
            root,
            webhook_retention_seconds,
        }) => match run_repair(root.as_deref(), webhook_retention_seconds).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        return report_output_error(&error);
                    }
                };
                if let Err(error) = report.write_cli_summary(
                    &mut io::stdout().lock(),
                    &root,
                    webhook_retention_seconds,
                ) {
                    return report_output_error(&error);
                }
                if report.index_rebuild.is_clean() && report.fsck.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    if let Err(error) = report.write_issues(&mut io::stderr().lock()) {
                        return report_output_error(&error);
                    }
                    ExitCode::FAILURE
                }
            }
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::RepairLifecycle {
            root,
            webhook_retention_seconds,
        }) => match run_lifecycle_repair(root.as_deref(), webhook_retention_seconds).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        return report_output_error(&error);
                    }
                };
                output_status(
                    report_output::write_lifecycle_repair_cli_summary(
                        &mut io::stdout().lock(),
                        &report,
                        &root,
                        webhook_retention_seconds,
                    ),
                    ExitCode::SUCCESS,
                )
            }
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::RepairLfsEvidence { root, state_file }) => {
            match run_lfs_evidence_repair(root.as_deref(), &state_file) {
                Ok(()) => stdout_text(format_args!(
                    "lfs evidence repair completed: {}\n",
                    state_file.display()
                )),
                Err(error) => error_chain_status(&error, ExitCode::from(2)),
            }
        }
        Ok(CliCommand::BackupManifest { root, output }) => {
            match run_backup_manifest(root.as_deref(), &output).await {
                Ok(report) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            return report_output_error(&error);
                        }
                    };
                    if let Err(error) = report_output::write_backup_manifest_cli_summary(
                        &mut io::stdout().lock(),
                        &report,
                        &root,
                        &output,
                    ) {
                        return report_output_error(&error);
                    }
                    ExitCode::SUCCESS
                }
                Err(error) => report_output_error(&error),
            }
        }
        Ok(CliCommand::StorageMigrate {
            from,
            from_root,
            to,
            to_root,
            prefix,
            dry_run,
        }) => match run_storage_migration(
            from,
            from_root.as_deref(),
            to,
            to_root.as_deref(),
            prefix,
            dry_run,
        ) {
            Ok(report) => output_status(
                report_output::write_storage_migration_summary(&mut io::stdout().lock(), &report),
                ExitCode::SUCCESS,
            ),
            Err(error) => error_chain_status(&error, ExitCode::from(2)),
        },
        Ok(CliCommand::Gc {
            root,
            mark,
            sweep,
            retention_seconds,
            retention_report,
            orphan_inventory,
        }) => {
            let retention_seconds = retention_seconds.max(MINIMUM_GC_RETENTION_SECONDS);
            match run_gc(
                root.as_deref(),
                mark,
                sweep,
                retention_seconds,
                retention_report.as_deref(),
                orphan_inventory.as_deref(),
            )
            .await
            {
                Ok(report) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            return report_output_error(&error);
                        }
                    };
                    output_status(
                        report_output::write_local_gc_cli_summary(
                            &mut io::stdout().lock(),
                            &report,
                            gc_mode_name(mark, sweep),
                            &root,
                            retention_seconds,
                            mark,
                            retention_report.as_deref(),
                            orphan_inventory.as_deref(),
                        ),
                        ExitCode::SUCCESS,
                    )
                }
                Err(error) => report_output_error(&error),
            }
        }
        Ok(CliCommand::GcScheduleInstall {
            output_dir,
            unit_prefix,
            calendar,
            retention_seconds,
            binary_path,
            env_file,
            working_directory,
            user,
            group,
            dry_run,
        }) => match crate::gc_schedule::install_gc_schedule_with_writer(
            &mut io::stdout().lock(),
            &GcScheduleInstallOptions {
                output_dir,
                unit_prefix,
                calendar,
                retention_seconds,
                binary_path,
                env_file,
                working_directory,
                user,
                group,
                dry_run,
            },
        ) {
            Ok(report) => output_status(
                report.write_summary(&mut io::stdout().lock()),
                ExitCode::SUCCESS,
            ),
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::GcScheduleUninstall {
            output_dir,
            unit_prefix,
        }) => match uninstall_gc_schedule(&output_dir, &unit_prefix) {
            Ok(report) => output_status(
                report.write_summary(&mut io::stdout().lock()),
                ExitCode::SUCCESS,
            ),
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::HoldSet {
            root,
            object_key,
            reason,
            ttl_seconds,
        }) => match run_hold_set(root.as_deref(), &object_key, &reason, ttl_seconds).await {
            Ok(hold) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        return report_output_error(&error);
                    }
                };
                let mut stdout = io::stdout().lock();
                output_status(
                    writeln!(stdout, "root: {}", root.display())
                        .and_then(|()| crate::hold::write_hold_summary(&mut stdout, &hold)),
                    ExitCode::SUCCESS,
                )
            }
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::HoldList { root, active_only }) => {
            match run_hold_list(root.as_deref(), active_only).await {
                Ok(holds) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            return report_output_error(&error);
                        }
                    };
                    output_status(
                        crate::hold::write_hold_list_summary(
                            &mut io::stdout().lock(),
                            &root,
                            active_only,
                            &holds,
                        ),
                        ExitCode::SUCCESS,
                    )
                }
                Err(error) => report_output_error(&error),
            }
        }
        Ok(CliCommand::HoldRelease { root, object_key }) => {
            match run_hold_release(root.as_deref(), &object_key).await {
                Ok(released) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            return report_output_error(&error);
                        }
                    };
                    stdout_text(format_args!(
                        "root: {}\nobject_key: {object_key}\nreleased: {released}\n",
                        root.display()
                    ))
                }
                Err(error) => report_output_error(&error),
            }
        }
        Ok(CliCommand::Bench {
            mode,
            deployment_target,
            scenario,
            storage_dir,
            iterations,
            concurrency,
            upload_max_in_flight_chunks,
            chunk_size_bytes,
            base_bytes,
            mutated_bytes,
            json,
        }) => match mode {
            BenchMode::EndToEnd => {
                let Some(storage_dir) = storage_dir.as_deref() else {
                    return report_output_error(&"missing argument: --storage-dir");
                };
                let config = BenchConfig {
                    deployment_target,
                    scenario,
                    iterations,
                    concurrency,
                    upload_max_in_flight_chunks,
                    chunk_size_bytes,
                    base_bytes,
                    mutated_bytes,
                };
                match run_bench(storage_dir, config).await {
                    Ok(report) => {
                        if json {
                            match to_string_pretty(&report) {
                                Ok(value) => stdout_text(format_args!("{value}\n")),
                                Err(error) => report_output_error(&error),
                            }
                        } else {
                            output_status(
                                report.write_summary(&mut io::stdout().lock()),
                                ExitCode::SUCCESS,
                            )
                        }
                    }
                    Err(error) => report_output_error(&error),
                }
            }
            BenchMode::Ingest => {
                let config = BenchConfig {
                    deployment_target,
                    scenario,
                    iterations,
                    concurrency,
                    upload_max_in_flight_chunks,
                    chunk_size_bytes,
                    base_bytes,
                    mutated_bytes,
                };
                match run_ingest_bench(config).await {
                    Ok(report) => {
                        if json {
                            match to_string_pretty(&report) {
                                Ok(value) => stdout_text(format_args!("{value}\n")),
                                Err(error) => report_output_error(&error),
                            }
                        } else {
                            output_status(
                                report.write_summary(&mut io::stdout().lock()),
                                ExitCode::SUCCESS,
                            )
                        }
                    }
                    Err(error) => report_output_error(&error),
                }
            }
        },
        Ok(CliCommand::Health { server_url }) => match run_health_check(&server_url).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::Completion { shell, output }) => match render_completion(shell) {
            Ok(rendered) => output.map_or_else(
                || stdout_text(format_args!("{rendered}")),
                |path| match write_output_bytes(&path, rendered.as_bytes(), false) {
                    Ok(()) => stdout_text(format_args!("output: {}\n", path.display())),
                    Err(error) => report_output_error(&error),
                },
            ),
            Err(error) => report_output_error(&error),
        },
        Ok(CliCommand::Manpage { output }) => match render_manpage() {
            Ok(rendered) => output.map_or_else(
                || stdout_text(format_args!("{rendered}")),
                |path| match write_output_bytes(&path, rendered.as_bytes(), false) {
                    Ok(()) => stdout_text(format_args!("output: {}\n", path.display())),
                    Err(error) => report_output_error(&error),
                },
            ),
            Err(error) => report_output_error(&error),
        },
        Err(error) if error.is_help() => stdout_text(format_args!("{error}")),
        Err(error) => diagnostic_text(format_args!("{error}"), ExitCode::from(2)),
    }
}

fn stdout_text(arguments: std::fmt::Arguments<'_>) -> ExitCode {
    let mut stdout = io::stdout().lock();
    output_status(write_text(&mut stdout, arguments), ExitCode::SUCCESS)
}

fn write_text(
    writer: &mut (impl Write + ?Sized),
    arguments: std::fmt::Arguments<'_>,
) -> io::Result<()> {
    writer.write_fmt(arguments)?;
    writer.flush()
}

fn output_status(result: io::Result<()>, success: ExitCode) -> ExitCode {
    match result {
        Ok(()) => success,
        Err(error) => report_output_error(&error),
    }
}

fn diagnostic_text(arguments: std::fmt::Arguments<'_>, status: ExitCode) -> ExitCode {
    let mut stderr = io::stderr().lock();
    diagnostic_status(write_text(&mut stderr, arguments), status)
}

fn diagnostic_status(result: io::Result<()>, status: ExitCode) -> ExitCode {
    match result {
        Ok(()) => status,
        Err(_write_error) => ExitCode::from(2),
    }
}

fn error_chain_status(error: &dyn std::error::Error, status: ExitCode) -> ExitCode {
    diagnostic_status(write_error_chain(&mut io::stderr().lock(), error), status)
}

fn report_output_error(error: &impl std::fmt::Display) -> ExitCode {
    diagnostic_text(format_args!("{error}\n"), ExitCode::from(2))
}

const fn gc_mode_name(mark: bool, sweep: bool) -> &'static str {
    match (mark, sweep) {
        (false, false) => "dry-run",
        (true, false) => "mark",
        (false, true) => "sweep",
        (true, true) => "mark-and-sweep",
    }
}

/// Returns the argument vector to pass to the `sdx` file-management lane, or
/// `None` when the invocation should go to the operator CLI.
///
/// Two routes dispatch here:
/// * the `sdx` symlink (`argv[0]` basename is `sdx`) — pass args unchanged;
/// * the `xet` escape-hatch subcommand on the operator binary — strip the
///   leading `xet` token so the xet parser sees the subcommand directly.
fn xet_args(args: &[OsString]) -> Option<Vec<OsString>> {
    if args
        .first()
        .is_some_and(|program| program_basename(program) == "sdx")
    {
        return Some(args.to_vec());
    }
    // Scan for the bare `xet` escape-hatch token, skipping global flag values.
    let mut index = 1;
    while index < args.len() {
        let Some(token) = args.get(index) else {
            break;
        };
        let value = token.to_string_lossy();
        match value.as_ref() {
            "--env-file" | "-c" | "--config" => index = index.saturating_add(2),
            _ if value.starts_with("--env-file=") || value.starts_with("--config=") => {
                index = index.saturating_add(1);
            }
            "xet" => {
                let mut stripped = args.to_vec();
                stripped.remove(index);
                return Some(stripped);
            }
            _ if value.starts_with('-') => index = index.saturating_add(1),
            _ => break,
        }
    }
    None
}

/// Returns the basename of an executable path string.
fn program_basename(program: &OsString) -> String {
    let value = program.to_string_lossy();
    value.rsplit('/').next().unwrap_or_default().to_owned()
}

#[cfg(test)]
mod tests {
    use super::gc_mode_name;

    #[test]
    fn gc_mode_name_dry_run() {
        assert_eq!(gc_mode_name(false, false), "dry-run");
    }

    #[test]
    fn gc_mode_name_mark() {
        assert_eq!(gc_mode_name(true, false), "mark");
    }

    #[test]
    fn gc_mode_name_sweep() {
        assert_eq!(gc_mode_name(false, true), "sweep");
    }

    #[test]
    fn gc_mode_name_mark_and_sweep() {
        assert_eq!(gc_mode_name(true, true), "mark-and-sweep");
    }
    struct FailingTextWriter {
        fail_write: bool,
        error_kind: std::io::ErrorKind,
        bytes: Vec<u8>,
    }

    impl std::io::Write for FailingTextWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.fail_write {
                return Err(std::io::Error::from(self.error_kind));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::from(self.error_kind))
        }
    }

    #[test]
    fn text_output_propagates_write_and_delayed_flush_errors_including_broken_pipe() {
        for error_kind in [
            std::io::ErrorKind::StorageFull,
            std::io::ErrorKind::BrokenPipe,
        ] {
            for fail_write in [true, false] {
                let mut writer = FailingTextWriter {
                    fail_write,
                    error_kind,
                    bytes: Vec::new(),
                };
                let error =
                    super::write_text(&mut writer, format_args!("help text\n")).unwrap_err();
                assert_eq!(error.kind(), error_kind);
                assert_eq!(writer.bytes.is_empty(), fail_write);
                assert_eq!(
                    super::diagnostic_status(Err(error), std::process::ExitCode::FAILURE),
                    std::process::ExitCode::from(2)
                );
            }
        }
    }

    #[test]
    fn diagnostic_status_preserves_semantic_exit_codes_when_output_succeeds() {
        for status in [
            std::process::ExitCode::SUCCESS,
            std::process::ExitCode::FAILURE,
            std::process::ExitCode::from(2),
        ] {
            assert_eq!(super::diagnostic_status(Ok(()), status), status);
        }
        let mut output = Vec::new();
        super::write_text(&mut output, format_args!("help text without extra newline")).unwrap();
        assert_eq!(output, b"help text without extra newline");
    }
}
