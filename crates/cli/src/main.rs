#![deny(unsafe_code)]

use std::{
    env::args_os,
    error::Error,
    path::{Path, PathBuf},
    process::ExitCode,
};

use serde_json::to_string_pretty;
use shardline::{
    BenchConfig, BenchMode, BenchScenario, CliCommand, GcScheduleInstallOptions, effective_root,
    install_gc_schedule, load_runtime_server_config, mint_admin_token_from_sources,
    render_completion, render_manpage, run_backup_manifest, run_bench, run_config_check_from_env,
    run_db_migration, run_fsck, run_gc, run_health_check, run_hold_list, run_hold_release,
    run_hold_set, run_index_rebuild, run_ingest_bench, run_lifecycle_repair,
    run_providerless_setup, run_repair, run_storage_migration, uninstall_gc_schedule,
    write_output_bytes,
};
use shardline_protocol::RepositoryScope;
use shardline_server::{ServerConfigError, serve};

#[tokio::main]
async fn main() -> ExitCode {
    match CliCommand::parse(args_os()) {
        Ok(CliCommand::ProviderlessSetup) => match run_providerless_setup(None) {
            Ok(report) => {
                println!("state_dir: {}", report.state_dir.display());
                println!("root_dir: {}", report.data_dir.display());
                println!("token_signing_key_file: {}", report.key_file.display());
                println!("env_file: {}", report.env_file.display());
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
                println!("status: {}", report.status);
                println!("server_role: {}", report.server_role);
                println!("server_frontends: {}", report.server_frontends.join(","));
                println!("metadata_backend: {}", report.metadata_backend);
                println!("object_backend: {}", report.object_backend);
                println!("cache_backend: {}", report.cache_backend);
                println!("auth_enabled: {}", report.auth_enabled);
                println!(
                    "provider_tokens_enabled: {}",
                    report.provider_tokens_enabled
                );
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
        }) => match run_db_migration(database_url.as_deref(), command).await {
            Ok(report) => {
                println!("backend: {}", report.backend);
                println!("applied_count: {}", report.applied_count);
                println!("reverted_count: {}", report.reverted_count);
                println!("applied_total_count: {}", report.applied_total_count);
                println!("pending_count: {}", report.pending_count);
                for migration in report.migrations {
                    println!(
                        "migration: version={} name={} applied={} applied_at_utc={}",
                        migration.version,
                        migration.name,
                        migration.applied,
                        migration.applied_at_utc.unwrap_or_else(|| "-".to_owned())
                    );
                }
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
                println!("root: {}", root.display());
                println!("latest_records: {}", report.latest_records);
                println!("version_records: {}", report.version_records);
                println!(
                    "inspected_chunk_references: {}",
                    report.inspected_chunk_references
                );
                println!(
                    "inspected_dedupe_shard_mappings: {}",
                    report.inspected_dedupe_shard_mappings
                );
                println!(
                    "inspected_reconstructions: {}",
                    report.inspected_reconstructions
                );
                println!(
                    "inspected_webhook_deliveries: {}",
                    report.inspected_webhook_deliveries
                );
                println!(
                    "inspected_provider_repository_states: {}",
                    report.inspected_provider_repository_states
                );
                println!("issue_count: {}", report.issue_count());
                if report.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    for issue in report.issues {
                        eprintln!(
                            "issue: {} location={} detail={}",
                            issue.kind.as_str(),
                            issue.location,
                            issue.detail
                        );
                    }
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
                println!("root: {}", root.display());
                println!(
                    "scanned_version_records: {}",
                    report.scanned_version_records
                );
                println!(
                    "scanned_retained_shards: {}",
                    report.scanned_retained_shards
                );
                println!("rebuilt_latest_records: {}", report.rebuilt_latest_records);
                println!(
                    "unchanged_latest_records: {}",
                    report.unchanged_latest_records
                );
                println!(
                    "removed_stale_latest_records: {}",
                    report.removed_stale_latest_records
                );
                println!(
                    "scanned_reconstructions: {}",
                    report.scanned_reconstructions
                );
                println!(
                    "unchanged_reconstructions: {}",
                    report.unchanged_reconstructions
                );
                println!(
                    "removed_stale_reconstructions: {}",
                    report.removed_stale_reconstructions
                );
                println!(
                    "rebuilt_dedupe_shard_mappings: {}",
                    report.rebuilt_dedupe_shard_mappings
                );
                println!(
                    "unchanged_dedupe_shard_mappings: {}",
                    report.unchanged_dedupe_shard_mappings
                );
                println!(
                    "removed_stale_dedupe_shard_mappings: {}",
                    report.removed_stale_dedupe_shard_mappings
                );
                println!("issue_count: {}", report.issue_count());
                if report.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    for issue in report.issues {
                        eprintln!(
                            "issue: {} location={} detail={}",
                            issue.kind.as_str(),
                            issue.location,
                            issue.detail
                        );
                    }
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
                println!("root: {}", root.display());
                println!("webhook_retention_seconds: {webhook_retention_seconds}");
                println!(
                    "index_rebuild.scanned_version_records: {}",
                    report.index_rebuild.scanned_version_records
                );
                println!(
                    "index_rebuild.scanned_retained_shards: {}",
                    report.index_rebuild.scanned_retained_shards
                );
                println!(
                    "index_rebuild.rebuilt_latest_records: {}",
                    report.index_rebuild.rebuilt_latest_records
                );
                println!(
                    "index_rebuild.unchanged_latest_records: {}",
                    report.index_rebuild.unchanged_latest_records
                );
                println!(
                    "index_rebuild.removed_stale_latest_records: {}",
                    report.index_rebuild.removed_stale_latest_records
                );
                println!(
                    "index_rebuild.scanned_reconstructions: {}",
                    report.index_rebuild.scanned_reconstructions
                );
                println!(
                    "index_rebuild.unchanged_reconstructions: {}",
                    report.index_rebuild.unchanged_reconstructions
                );
                println!(
                    "index_rebuild.removed_stale_reconstructions: {}",
                    report.index_rebuild.removed_stale_reconstructions
                );
                println!(
                    "index_rebuild.rebuilt_dedupe_shard_mappings: {}",
                    report.index_rebuild.rebuilt_dedupe_shard_mappings
                );
                println!(
                    "index_rebuild.unchanged_dedupe_shard_mappings: {}",
                    report.index_rebuild.unchanged_dedupe_shard_mappings
                );
                println!(
                    "index_rebuild.removed_stale_dedupe_shard_mappings: {}",
                    report.index_rebuild.removed_stale_dedupe_shard_mappings
                );
                println!(
                    "index_rebuild.issue_count: {}",
                    report.index_rebuild.issue_count()
                );
                println!(
                    "lifecycle_repair.scanned_records: {}",
                    report.lifecycle_repair.scanned_records
                );
                println!(
                    "lifecycle_repair.referenced_objects: {}",
                    report.lifecycle_repair.referenced_objects
                );
                println!(
                    "lifecycle_repair.scanned_quarantine_candidates: {}",
                    report.lifecycle_repair.scanned_quarantine_candidates
                );
                println!(
                    "lifecycle_repair.removed_missing_quarantine_candidates: {}",
                    report
                        .lifecycle_repair
                        .removed_missing_quarantine_candidates
                );
                println!(
                    "lifecycle_repair.removed_reachable_quarantine_candidates: {}",
                    report
                        .lifecycle_repair
                        .removed_reachable_quarantine_candidates
                );
                println!(
                    "lifecycle_repair.removed_held_quarantine_candidates: {}",
                    report.lifecycle_repair.removed_held_quarantine_candidates
                );
                println!(
                    "lifecycle_repair.scanned_retention_holds: {}",
                    report.lifecycle_repair.scanned_retention_holds
                );
                println!(
                    "lifecycle_repair.removed_expired_retention_holds: {}",
                    report.lifecycle_repair.removed_expired_retention_holds
                );
                println!(
                    "lifecycle_repair.removed_missing_retention_holds: {}",
                    report.lifecycle_repair.removed_missing_retention_holds
                );
                println!(
                    "lifecycle_repair.scanned_webhook_deliveries: {}",
                    report.lifecycle_repair.scanned_webhook_deliveries
                );
                println!(
                    "lifecycle_repair.removed_stale_webhook_deliveries: {}",
                    report.lifecycle_repair.removed_stale_webhook_deliveries
                );
                println!(
                    "lifecycle_repair.removed_future_webhook_deliveries: {}",
                    report.lifecycle_repair.removed_future_webhook_deliveries
                );
                println!("fsck.latest_records: {}", report.fsck.latest_records);
                println!("fsck.version_records: {}", report.fsck.version_records);
                println!(
                    "fsck.inspected_chunk_references: {}",
                    report.fsck.inspected_chunk_references
                );
                println!(
                    "fsck.inspected_dedupe_shard_mappings: {}",
                    report.fsck.inspected_dedupe_shard_mappings
                );
                println!(
                    "fsck.inspected_reconstructions: {}",
                    report.fsck.inspected_reconstructions
                );
                println!(
                    "fsck.inspected_webhook_deliveries: {}",
                    report.fsck.inspected_webhook_deliveries
                );
                println!(
                    "fsck.inspected_provider_repository_states: {}",
                    report.fsck.inspected_provider_repository_states
                );
                println!("fsck.issue_count: {}", report.fsck.issue_count());
                if report.index_rebuild.is_clean() && report.fsck.is_clean() {
                    ExitCode::SUCCESS
                } else {
                    for issue in report.index_rebuild.issues {
                        eprintln!(
                            "index_rebuild.issue: {} location={} detail={}",
                            issue.kind.as_str(),
                            issue.location,
                            issue.detail
                        );
                    }
                    for issue in report.fsck.issues {
                        eprintln!(
                            "fsck.issue: {} location={} detail={}",
                            issue.kind.as_str(),
                            issue.location,
                            issue.detail
                        );
                    }
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
                println!("root: {}", root.display());
                println!("webhook_retention_seconds: {webhook_retention_seconds}");
                println!("scanned_records: {}", report.scanned_records);
                println!("referenced_objects: {}", report.referenced_objects);
                println!(
                    "scanned_quarantine_candidates: {}",
                    report.scanned_quarantine_candidates
                );
                println!(
                    "removed_missing_quarantine_candidates: {}",
                    report.removed_missing_quarantine_candidates
                );
                println!(
                    "removed_reachable_quarantine_candidates: {}",
                    report.removed_reachable_quarantine_candidates
                );
                println!(
                    "removed_held_quarantine_candidates: {}",
                    report.removed_held_quarantine_candidates
                );
                println!(
                    "scanned_retention_holds: {}",
                    report.scanned_retention_holds
                );
                println!(
                    "removed_expired_retention_holds: {}",
                    report.removed_expired_retention_holds
                );
                println!(
                    "removed_missing_retention_holds: {}",
                    report.removed_missing_retention_holds
                );
                println!(
                    "scanned_webhook_deliveries: {}",
                    report.scanned_webhook_deliveries
                );
                println!(
                    "removed_stale_webhook_deliveries: {}",
                    report.removed_stale_webhook_deliveries
                );
                println!(
                    "removed_future_webhook_deliveries: {}",
                    report.removed_future_webhook_deliveries
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
                    println!("root: {}", root.display());
                    println!("output: {}", output.display());
                    println!("manifest_version: {}", report.manifest_version);
                    println!("metadata_backend: {}", report.metadata_backend);
                    println!("object_backend: {}", report.object_backend);
                    println!("object_count: {}", report.object_count);
                    println!("object_bytes: {}", report.object_bytes);
                    println!("latest_records: {}", report.latest_records);
                    println!("version_records: {}", report.version_records);
                    println!("reconstruction_rows: {}", report.reconstruction_rows);
                    println!("dedupe_shard_mappings: {}", report.dedupe_shard_mappings);
                    println!("quarantine_candidates: {}", report.quarantine_candidates);
                    println!("retention_holds: {}", report.retention_holds);
                    println!("webhook_deliveries: {}", report.webhook_deliveries);
                    println!(
                        "provider_repository_states: {}",
                        report.provider_repository_states
                    );
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
                println!("source_backend: {}", report.source_backend);
                println!("destination_backend: {}", report.destination_backend);
                println!("prefix: {}", report.prefix);
                println!("dry_run: {}", report.dry_run);
                println!("scanned_objects: {}", report.scanned_objects);
                println!("scanned_bytes: {}", report.scanned_bytes);
                println!("inserted_objects: {}", report.inserted_objects);
                println!(
                    "already_present_objects: {}",
                    report.already_present_objects
                );
                println!("copied_bytes: {}", report.copied_bytes);
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
        }) => match run_gc(
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
                println!("mode: {}", gc_mode_name(mark, sweep));
                println!("root: {}", root.display());
                if mark {
                    println!("retention_seconds: {retention_seconds}");
                }
                if let Some(path) = retention_report {
                    println!("retention_report: {}", path.display());
                }
                if let Some(path) = orphan_inventory {
                    println!("orphan_inventory: {}", path.display());
                }
                println!("scanned_records: {}", report.scanned_records);
                println!("referenced_chunks: {}", report.referenced_chunks);
                println!("orphan_chunks: {}", report.orphan_chunks);
                println!("orphan_chunk_bytes: {}", report.orphan_chunk_bytes);
                println!(
                    "active_quarantine_candidates: {}",
                    report.active_quarantine_candidates
                );
                println!(
                    "new_quarantine_candidates: {}",
                    report.new_quarantine_candidates
                );
                println!(
                    "retained_quarantine_candidates: {}",
                    report.retained_quarantine_candidates
                );
                println!(
                    "released_quarantine_candidates: {}",
                    report.released_quarantine_candidates
                );
                println!("deleted_chunks: {}", report.deleted_chunks);
                println!("deleted_bytes: {}", report.deleted_bytes);
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(2)
            }
        },
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
        }) {
            Ok(report) => {
                println!("service_path: {}", report.service_path.display());
                println!("timer_path: {}", report.timer_path.display());
                println!("binary_path: {}", report.binary_path.display());
                println!("env_file: {}", report.env_file.display());
                println!("working_directory: {}", report.working_directory.display());
                println!("calendar: {}", report.calendar);
                println!("retention_seconds: {}", report.retention_seconds);
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
                println!("service_path: {}", report.service_path.display());
                println!("timer_path: {}", report.timer_path.display());
                println!("removed_service: {}", report.removed_service);
                println!("removed_timer: {}", report.removed_timer);
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
                println!("object_key: {}", hold.object_key().as_str());
                println!("reason: {}", hold.reason());
                println!("held_at_unix_seconds: {}", hold.held_at_unix_seconds());
                match hold.release_after_unix_seconds() {
                    Some(value) => println!("release_after_unix_seconds: {value}"),
                    None => println!("release_after_unix_seconds: none"),
                }
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
                    println!("root: {}", root.display());
                    println!("active_only: {active_only}");
                    println!("hold_count: {}", holds.len());
                    for (index, hold) in holds.iter().enumerate() {
                        println!("hold[{index}].object_key: {}", hold.object_key().as_str());
                        println!("hold[{index}].reason: {}", hold.reason());
                        println!(
                            "hold[{index}].held_at_unix_seconds: {}",
                            hold.held_at_unix_seconds()
                        );
                        match hold.release_after_unix_seconds() {
                            Some(value) => {
                                println!("hold[{index}].release_after_unix_seconds: {value}");
                            }
                            None => {
                                println!("hold[{index}].release_after_unix_seconds: none");
                            }
                        }
                    }
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
                            println!("mode: e2e");
                            println!("deployment_target: {}", report.deployment_target.as_str());
                            println!("metadata_backend: {}", report.metadata_backend);
                            println!("object_backend: {}", report.object_backend);
                            println!("inventory_scope: {}", report.inventory_scope.as_str());
                            println!("scenario: {}", report.scenario.as_str());
                            if report.scenario == BenchScenario::Full {
                                println!("scenario: sparse-update");
                                println!("scenario: concurrent-latest-download");
                                println!("scenario: concurrent-upload");
                                println!("scenario: cross-repository-upload");
                                println!("scenario: cached-latest-reconstruction");
                            }
                            println!("storage_dir: {}", report.storage_dir.display());
                            println!("iterations: {}", report.iterations);
                            println!("concurrency: {}", report.concurrency);
                            println!(
                                "upload_max_in_flight_chunks: {}",
                                report.upload_max_in_flight_chunks
                            );
                            println!("chunk_size_bytes: {}", report.chunk_size_bytes);
                            println!("base_bytes: {}", report.base_bytes);
                            println!("mutated_bytes: {}", report.mutated_bytes);
                            println!("available_parallelism: {}", report.available_parallelism);
                            println!(
                                "average_initial_upload_micros: {}",
                                report.average_initial_upload_micros
                            );
                            println!(
                                "average_sparse_update_upload_micros: {}",
                                report.average_sparse_update_upload_micros
                            );
                            println!(
                                "average_latest_download_micros: {}",
                                report.average_latest_download_micros
                            );
                            println!(
                                "average_previous_download_micros: {}",
                                report.average_previous_download_micros
                            );
                            println!(
                                "average_ranged_reconstruction_micros: {}",
                                report.average_ranged_reconstruction_micros
                            );
                            println!(
                                "average_concurrent_latest_download_micros: {}",
                                report.average_concurrent_latest_download_micros
                            );
                            println!(
                                "average_concurrent_upload_micros: {}",
                                report.average_concurrent_upload_micros
                            );
                            println!(
                                "average_cross_repository_upload_micros: {}",
                                report.average_cross_repository_upload_micros
                            );
                            println!(
                                "average_cached_latest_reconstruction_cold_micros: {}",
                                report.average_cached_latest_reconstruction_cold_micros
                            );
                            println!(
                                "average_cached_latest_reconstruction_hot_micros: {}",
                                report.average_cached_latest_reconstruction_hot_micros
                            );
                            println!(
                                "average_process_cpu_micros: {}",
                                report.average_process_cpu_micros
                            );
                            println!(
                                "average_process_cpu_cores_per_mille: {}",
                                report.average_process_cpu_cores_per_mille
                            );
                            println!(
                                "average_process_host_utilization_per_mille: {}",
                                report.average_process_host_utilization_per_mille
                            );
                            println!(
                                "average_initial_upload_bytes_per_second: {}",
                                report.average_initial_upload_bytes_per_second
                            );
                            println!(
                                "average_sparse_update_upload_bytes_per_second: {}",
                                report.average_sparse_update_upload_bytes_per_second
                            );
                            println!(
                                "average_latest_download_bytes_per_second: {}",
                                report.average_latest_download_bytes_per_second
                            );
                            println!(
                                "average_previous_download_bytes_per_second: {}",
                                report.average_previous_download_bytes_per_second
                            );
                            println!(
                                "average_concurrent_latest_download_bytes_per_second: {}",
                                report.average_concurrent_latest_download_bytes_per_second
                            );
                            println!(
                                "average_concurrent_upload_bytes_per_second: {}",
                                report.average_concurrent_upload_bytes_per_second
                            );
                            println!(
                                "average_cross_repository_upload_bytes_per_second: {}",
                                report.average_cross_repository_upload_bytes_per_second
                            );
                            println!(
                                "average_cached_latest_reconstruction_hit_bytes_per_second: {}",
                                report.average_cached_latest_reconstruction_hit_bytes_per_second
                            );
                            println!(
                                "concurrent_latest_download_scaling_per_mille: {}",
                                report.concurrent_latest_download_scaling_per_mille
                            );
                            println!(
                                "concurrent_upload_scaling_per_mille: {}",
                                report.concurrent_upload_scaling_per_mille
                            );
                            println!("total_uploaded_bytes: {}", report.total_uploaded_bytes);
                            println!("total_downloaded_bytes: {}", report.total_downloaded_bytes);
                            println!(
                                "total_cached_reconstruction_response_bytes: {}",
                                report.total_cached_reconstruction_response_bytes
                            );
                            println!("cache_hit_iterations: {}", report.cache_hit_iterations);
                            println!(
                                "total_concurrent_downloaded_bytes: {}",
                                report.total_concurrent_downloaded_bytes
                            );
                            println!(
                                "total_concurrent_uploaded_bytes: {}",
                                report.total_concurrent_uploaded_bytes
                            );
                            println!(
                                "total_newly_stored_bytes: {}",
                                report.total_newly_stored_bytes
                            );
                            println!(
                                "total_concurrent_newly_stored_bytes: {}",
                                report.total_concurrent_newly_stored_bytes
                            );
                            println!(
                                "total_cross_repository_newly_stored_bytes: {}",
                                report.total_cross_repository_newly_stored_bytes
                            );
                            println!(
                                "total_initial_inserted_chunks: {}",
                                report.total_initial_inserted_chunks
                            );
                            println!(
                                "total_sparse_update_inserted_chunks: {}",
                                report.total_sparse_update_inserted_chunks
                            );
                            println!(
                                "total_sparse_update_reused_chunks: {}",
                                report.total_sparse_update_reused_chunks
                            );
                            println!(
                                "total_concurrent_upload_inserted_chunks: {}",
                                report.total_concurrent_upload_inserted_chunks
                            );
                            println!(
                                "total_concurrent_upload_reused_chunks: {}",
                                report.total_concurrent_upload_reused_chunks
                            );
                            println!(
                                "total_cross_repository_inserted_chunks: {}",
                                report.total_cross_repository_inserted_chunks
                            );
                            println!(
                                "total_cross_repository_reused_chunks: {}",
                                report.total_cross_repository_reused_chunks
                            );
                            if let Some(last) = report.iterations_detail.last() {
                                println!("last_iteration_chunk_objects: {}", last.chunk_objects);
                                println!("last_iteration_chunk_bytes: {}", last.chunk_bytes);
                                println!("last_iteration_visible_files: {}", last.visible_files);
                            }
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
                            println!("mode: ingest");
                            println!("scenario: {}", report.scenario.as_str());
                            if report.scenario == BenchScenario::Full {
                                println!("scenario: sparse-update");
                                println!("scenario: concurrent-upload");
                            }
                            println!("iterations: {}", report.iterations);
                            println!("concurrency: {}", report.concurrency);
                            println!(
                                "upload_max_in_flight_chunks: {}",
                                report.upload_max_in_flight_chunks
                            );
                            println!("chunk_size_bytes: {}", report.chunk_size_bytes);
                            println!("base_bytes: {}", report.base_bytes);
                            println!("mutated_bytes: {}", report.mutated_bytes);
                            println!("available_parallelism: {}", report.available_parallelism);
                            println!(
                                "average_initial_upload_micros: {}",
                                report.average_initial_upload_micros
                            );
                            println!(
                                "average_sparse_update_upload_micros: {}",
                                report.average_sparse_update_upload_micros
                            );
                            println!(
                                "average_concurrent_upload_micros: {}",
                                report.average_concurrent_upload_micros
                            );
                            println!(
                                "average_initial_upload_bytes_per_second: {}",
                                report.average_initial_upload_bytes_per_second
                            );
                            println!(
                                "average_sparse_update_upload_bytes_per_second: {}",
                                report.average_sparse_update_upload_bytes_per_second
                            );
                            println!(
                                "average_concurrent_upload_bytes_per_second: {}",
                                report.average_concurrent_upload_bytes_per_second
                            );
                            println!(
                                "average_concurrent_upload_process_cpu_micros: {}",
                                report.average_concurrent_upload_process_cpu_micros
                            );
                            println!(
                                "average_concurrent_upload_process_cpu_cores_per_mille: {}",
                                report.average_concurrent_upload_process_cpu_cores_per_mille
                            );
                            println!(
                                "average_concurrent_upload_process_host_utilization_per_mille: {}",
                                report.average_concurrent_upload_process_host_utilization_per_mille
                            );
                            println!(
                                "average_process_cpu_micros: {}",
                                report.average_process_cpu_micros
                            );
                            println!(
                                "average_process_cpu_cores_per_mille: {}",
                                report.average_process_cpu_cores_per_mille
                            );
                            println!(
                                "average_process_host_utilization_per_mille: {}",
                                report.average_process_host_utilization_per_mille
                            );
                            println!(
                                "concurrent_upload_scaling_per_mille: {}",
                                report.concurrent_upload_scaling_per_mille
                            );
                            println!("total_uploaded_bytes: {}", report.total_uploaded_bytes);
                            println!(
                                "total_concurrent_uploaded_bytes: {}",
                                report.total_concurrent_uploaded_bytes
                            );
                            println!(
                                "total_initial_inserted_chunks: {}",
                                report.total_initial_inserted_chunks
                            );
                            println!(
                                "total_sparse_update_inserted_chunks: {}",
                                report.total_sparse_update_inserted_chunks
                            );
                            println!(
                                "total_concurrent_upload_inserted_chunks: {}",
                                report.total_concurrent_upload_inserted_chunks
                            );
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

fn resolve_root(root_override: Option<&Path>) -> Result<PathBuf, ServerConfigError> {
    effective_root(root_override)
}

fn print_error_chain(error: &dyn Error) {
    eprintln!("{error}");
    let mut source = error.source();
    while let Some(next) = source {
        eprintln!("caused by: {next}");
        source = next.source();
    }
}
