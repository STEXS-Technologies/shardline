#![deny(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use std::ffi::OsString;
use std::process::ExitCode;

use serde_json::to_string_pretty;
use shardline_protocol::RepositoryScope;
use shardline_server::serve;

use crate::local_output::print_error_chain;
use crate::local_path::resolve_root;
use crate::report_output;
use crate::{
    BenchConfig, BenchMode, CliCommand, GcScheduleInstallOptions, MINIMUM_GC_RETENTION_SECONDS,
    install_gc_schedule, load_runtime_server_config, mint_admin_token_from_sources,
    print_hold_list_summary, print_hold_summary, render_completion, render_manpage,
    run_backup_manifest, run_bench, run_config_check_from_env, run_db_migration, run_fsck, run_gc,
    run_health_check, run_hold_list, run_hold_release, run_hold_set, run_index_rebuild,
    run_ingest_bench, run_lifecycle_repair, run_providerless_setup, run_repair,
    run_storage_migration, uninstall_gc_schedule, write_output_bytes,
};

pub async fn run(args: impl Iterator<Item = OsString>) -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match CliCommand::parse(args) {
        Ok(CliCommand::ProviderlessSetup) => match run_providerless_setup(None) {
            Ok(report) => {
                report.print_summary();
                ExitCode::SUCCESS
            }
            Err(error) => {
                print_error_chain(&error);
                ExitCode::from(2)
            }
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
                            print_error_chain(&error);
                            return ExitCode::from(2);
                        }
                    }
                }
                match serve(config).await {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(error) => {
                        print_error_chain(&error);
                        ExitCode::FAILURE
                    }
                }
            }
            Err(error) => {
                print_error_chain(&error);
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::ConfigCheck) => match run_config_check_from_env().await {
            Ok(report) => {
                report_output::print_config_check_summary(&report);
                ExitCode::SUCCESS
            }
            Err(error) => {
                print_error_chain(&error);
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::DbMigrate {
            database_url,
            command,
        }) => match run_db_migration(database_url.as_ref().map(|r| r.as_str()), command).await {
            Ok(report) => {
                report_output::print_database_migration_summary(&report);
                ExitCode::SUCCESS
            }
            Err(error) => {
                print_error_chain(&error);
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::AdminToken {
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
                match mint_admin_token_from_sources(
                    key_file.as_deref(),
                    key_env.as_deref(),
                    &issuer,
                    &subject,
                    scope,
                    repository,
                    ttl_seconds,
                ) {
                    Ok(token) => {
                        println!("{token}");
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("{error}");
                        ExitCode::from(2)
                    }
                }
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::Fsck { root }) => match run_fsck(root.as_deref()).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        eprintln!("{error}");
                        return ExitCode::from(2);
                    }
                };
                report_output::print_fsck_cli_summary(&report, &root);
                if report.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    report_output::print_fsck_issues(&report);
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::IndexRebuild { root }) => match run_index_rebuild(root.as_deref()).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        eprintln!("{error}");
                        return ExitCode::from(2);
                    }
                };
                report_output::print_index_rebuild_cli_summary(&report, &root);
                if report.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    report_output::print_index_rebuild_issues(&report);
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::Repair {
            root,
            webhook_retention_seconds,
        }) => match run_repair(root.as_deref(), webhook_retention_seconds).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        eprintln!("{error}");
                        return ExitCode::from(2);
                    }
                };
                report.print_cli_summary(&root, webhook_retention_seconds);
                if report.index_rebuild.is_clean() && report.fsck.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    report.print_issues();
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::RepairLifecycle {
            root,
            webhook_retention_seconds,
        }) => match run_lifecycle_repair(root.as_deref(), webhook_retention_seconds).await {
            Ok(report) => {
                let root = match resolve_root(root.as_deref()) {
                    Ok(path) => path,
                    Err(error) => {
                        eprintln!("{error}");
                        return ExitCode::from(2);
                    }
                };
                report_output::print_lifecycle_repair_cli_summary(
                    &report,
                    &root,
                    webhook_retention_seconds,
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::BackupManifest { root, output }) => {
            match run_backup_manifest(root.as_deref(), &output).await {
                Ok(report) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            eprintln!("{error}");
                            return ExitCode::from(2);
                        }
                    };
                    report_output::print_backup_manifest_cli_summary(&report, &root, &output);
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::from(2)
                }
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
            Ok(report) => {
                report_output::print_storage_migration_summary(&report);
                ExitCode::SUCCESS
            }
            Err(error) => {
                print_error_chain(&error);
                ExitCode::from(2)
            }
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
                            eprintln!("{error}");
                            return ExitCode::from(2);
                        }
                    };
                    report_output::print_local_gc_cli_summary(
                        &report,
                        gc_mode_name(mark, sweep),
                        &root,
                        retention_seconds,
                        mark,
                        retention_report.as_deref(),
                        orphan_inventory.as_deref(),
                    );
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::from(2)
                }
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
        }) => match install_gc_schedule(&GcScheduleInstallOptions {
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
        }) {
            Ok(report) => {
                report.print_summary();
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::GcScheduleUninstall {
            output_dir,
            unit_prefix,
        }) => match uninstall_gc_schedule(&output_dir, &unit_prefix) {
            Ok(report) => {
                report.print_summary();
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
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
                        eprintln!("{error}");
                        return ExitCode::from(2);
                    }
                };
                println!("root: {}", root.display());
                print_hold_summary(&hold);
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::HoldList { root, active_only }) => {
            match run_hold_list(root.as_deref(), active_only).await {
                Ok(holds) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            eprintln!("{error}");
                            return ExitCode::from(2);
                        }
                    };
                    print_hold_list_summary(&root, active_only, &holds);
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::from(2)
                }
            }
        }
        Ok(CliCommand::HoldRelease { root, object_key }) => {
            match run_hold_release(root.as_deref(), &object_key).await {
                Ok(released) => {
                    let root = match resolve_root(root.as_deref()) {
                        Ok(path) => path,
                        Err(error) => {
                            eprintln!("{error}");
                            return ExitCode::from(2);
                        }
                    };
                    println!("root: {}", root.display());
                    println!("object_key: {object_key}");
                    println!("released: {released}");
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("{error}");
                    ExitCode::from(2)
                }
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
                    eprintln!("missing argument: --storage-dir");
                    return ExitCode::from(2);
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
                                Ok(value) => {
                                    println!("{value}");
                                    ExitCode::SUCCESS
                                }
                                Err(error) => {
                                    eprintln!("{error}");
                                    ExitCode::from(2)
                                }
                            }
                        } else {
                            report.print_summary();
                            ExitCode::SUCCESS
                        }
                    }
                    Err(error) => {
                        eprintln!("{error}");
                        ExitCode::from(2)
                    }
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
                                Ok(value) => {
                                    println!("{value}");
                                    ExitCode::SUCCESS
                                }
                                Err(error) => {
                                    eprintln!("{error}");
                                    ExitCode::from(2)
                                }
                            }
                        } else {
                            report.print_summary();
                            ExitCode::SUCCESS
                        }
                    }
                    Err(error) => {
                        eprintln!("{error}");
                        ExitCode::from(2)
                    }
                }
            }
        },
        Ok(CliCommand::Health { server_url }) => match run_health_check(&server_url).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::Completion { shell, output }) => match render_completion(shell) {
            Ok(rendered) => output.map_or_else(
                || {
                    print!("{rendered}");
                    ExitCode::SUCCESS
                },
                |path| match write_output_bytes(&path, rendered.as_bytes(), false) {
                    Ok(()) => {
                        println!("output: {}", path.display());
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("{error}");
                        ExitCode::from(2)
                    }
                },
            ),
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Ok(CliCommand::Manpage { output }) => match render_manpage() {
            Ok(rendered) => output.map_or_else(
                || {
                    print!("{rendered}");
                    ExitCode::SUCCESS
                },
                |path| match write_output_bytes(&path, rendered.as_bytes(), false) {
                    Ok(()) => {
                        println!("output: {}", path.display());
                        ExitCode::SUCCESS
                    }
                    Err(error) => {
                        eprintln!("{error}");
                        ExitCode::from(2)
                    }
                },
            ),
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
        Err(error) if error.is_help() => {
            print!("{error}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprint!("{error}");
            ExitCode::from(2)
        }
    }
}

const fn gc_mode_name(mark: bool, sweep: bool) -> &'static str {
    match (mark, sweep) {
        (false, false) => "dry-run",
        (true, false) => "mark",
        (false, true) => "sweep",
        (true, true) => "mark-and-sweep",
    }
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
}
