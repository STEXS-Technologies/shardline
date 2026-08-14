use std::path::PathBuf;

use clap::error::ErrorKind;
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
    DatabaseMigrationCommand, ObjectStorageAdapter, ServerFrontend, ServerRole,
};

use super::{BenchMode, CliCommand, CompletionShell, RedactedDbUrl};
use crate::bench::{BenchDeploymentTarget, BenchScenario};

#[test]
fn parse_defaults_to_help() {
    let args = vec!["shardline".to_owned()];
    let parsed = CliCommand::parse(args);

    assert!(parsed.is_err());
    let Err(error) = parsed else {
        return;
    };
    assert!(error.is_help());
    assert!(format!("{error}").contains("Usage: shardline"));
}

#[test]
fn parse_help_aliases() {
    let long = vec!["shardline".to_owned(), "--help".to_owned()];
    let short = vec!["shardline".to_owned(), "-h".to_owned()];
    let command = vec!["shardline".to_owned(), "help".to_owned()];

    for args in [long, short, command] {
        let parsed = CliCommand::parse(args);
        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert!(error.is_help());
        assert!(format!("{error}").contains("Usage: shardline"));
    }
}

#[test]
fn help_text_is_generated_from_clap() {
    let help = CliCommand::help_text();

    assert!(help.contains("Usage: shardline"));
    assert!(help.contains("Examples:"));
    assert!(help.contains("gc schedule install"));
    assert!(help.contains("completion"));
    assert!(help.contains("manpage"));
    assert!(help.contains("db"));
    assert!(help.contains("gc"));
    assert!(help.contains("bench"));
    assert!(help.contains("providerless"));
}

#[test]
fn nested_help_includes_examples_for_gc_schedule_install() {
    let args = vec![
        "shardline".to_owned(),
        "gc".to_owned(),
        "schedule".to_owned(),
        "install".to_owned(),
        "--help".to_owned(),
    ];
    let parsed = CliCommand::parse(args);

    assert!(parsed.is_err());
    let Err(error) = parsed else {
        return;
    };
    assert!(error.is_help());
    assert!(format!("{error}").contains("Examples:"));
    assert!(
        error
            .to_string()
            .contains("--env-file /etc/shardline/shardline.env")
    );
}

#[test]
fn parse_top_level_commands() {
    let providerless = vec![
        "shardline".to_owned(),
        "providerless".to_owned(),
        "setup".to_owned(),
    ];
    let serve = vec!["shardline".to_owned(), "serve".to_owned()];
    let bench = vec![
        "shardline".to_owned(),
        "bench".to_owned(),
        "--storage-dir".to_owned(),
        "/var/lib/shardline-bench".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(providerless),
        Ok(CliCommand::ProviderlessSetup)
    );
    assert_eq!(
        CliCommand::parse(serve),
        Ok(CliCommand::Serve {
            role: None,
            frontends: None,
        })
    );
    assert_eq!(
        CliCommand::parse(bench),
        Ok(CliCommand::Bench {
            mode: BenchMode::EndToEnd,
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::Full,
            storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
            iterations: 1,
            concurrency: 4,
            upload_max_in_flight_chunks: 64,
            chunk_size_bytes: 65_536,
            base_bytes: 1_048_576,
            mutated_bytes: 4_096,
            json: false,
        })
    );
}

#[test]
fn parse_serve_with_role() {
    let args = vec![
        "shardline".to_owned(),
        "serve".to_owned(),
        "--role".to_owned(),
        "transfer".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Serve {
            role: Some(ServerRole::Transfer),
            frontends: None,
        })
    );
}

#[test]
fn parse_serve_with_frontends() {
    let args = vec![
        "shardline".to_owned(),
        "serve".to_owned(),
        "--frontend".to_owned(),
        "xet,xet".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Serve {
            role: None,
            frontends: Some(vec![ServerFrontend::Xet]),
        })
    );
}

#[test]
fn parse_serve_with_multiple_frontends() {
    let args = vec![
        "shardline".to_owned(),
        "serve".to_owned(),
        "--frontend".to_owned(),
        "lfs,bazel-http,oci".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Serve {
            role: None,
            frontends: Some(vec![
                ServerFrontend::Lfs,
                ServerFrontend::BazelHttp,
                ServerFrontend::Oci,
            ]),
        })
    );
}

#[test]
fn parse_serve_with_s3_frontend() {
    let args = vec![
        "shardline".to_owned(),
        "serve".to_owned(),
        "--frontend".to_owned(),
        "s3".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Serve {
            role: None,
            frontends: Some(vec![ServerFrontend::S3]),
        })
    );
}

#[test]
fn parse_fsck() {
    let args = vec![
        "shardline".to_owned(),
        "fsck".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Fsck {
            root: Some(PathBuf::from("/var/lib/shardline")),
        })
    );
}

#[test]
fn parse_fsck_without_root_override() {
    let args = vec!["shardline".to_owned(), "fsck".to_owned()];

    assert_eq!(CliCommand::parse(args), Ok(CliCommand::Fsck { root: None }));
}

#[test]
fn parse_index_rebuild() {
    let args = vec![
        "shardline".to_owned(),
        "index".to_owned(),
        "rebuild".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::IndexRebuild {
            root: Some(PathBuf::from("/var/lib/shardline")),
        })
    );
}

#[test]
fn parse_repair_lifecycle() {
    let args = vec![
        "shardline".to_owned(),
        "repair".to_owned(),
        "lifecycle".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--webhook-retention-seconds".to_owned(),
        "3600".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::RepairLifecycle {
            root: Some(PathBuf::from("/var/lib/shardline")),
            webhook_retention_seconds: 3600,
        })
    );
}

#[test]
fn parse_repair_orchestrator_with_defaults() {
    let args = vec!["shardline".to_owned(), "repair".to_owned()];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Repair {
            root: None,
            webhook_retention_seconds: DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
        })
    );
}

#[test]
fn parse_repair_orchestrator_with_options() {
    let args = vec![
        "shardline".to_owned(),
        "repair".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--webhook-retention-seconds".to_owned(),
        "3600".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Repair {
            root: Some(PathBuf::from("/var/lib/shardline")),
            webhook_retention_seconds: 3600,
        })
    );
}

#[test]
fn parse_gc() {
    let args = vec![
        "shardline".to_owned(),
        "gc".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--mark".to_owned(),
        "--sweep".to_owned(),
        "--retention-seconds".to_owned(),
        "600".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Gc {
            root: Some(PathBuf::from("/var/lib/shardline")),
            mark: true,
            sweep: true,
            retention_seconds: 600,
            retention_report: None,
            orphan_inventory: None,
        })
    );
}

#[test]
fn parse_gc_schedule_install() {
    let args = vec![
        "shardline".to_owned(),
        "gc".to_owned(),
        "schedule".to_owned(),
        "install".to_owned(),
        "--output-dir".to_owned(),
        "/tmp/systemd".to_owned(),
        "--unit-prefix".to_owned(),
        "assets-gc".to_owned(),
        "--calendar".to_owned(),
        "hourly".to_owned(),
        "--retention-seconds".to_owned(),
        "600".to_owned(),
        "--binary-path".to_owned(),
        "/usr/bin/shardline".to_owned(),
        "--env-file".to_owned(),
        "/etc/shardline/env".to_owned(),
        "--working-directory".to_owned(),
        "/srv/assets".to_owned(),
        "--user".to_owned(),
        "svc".to_owned(),
        "--group".to_owned(),
        "svc".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::GcScheduleInstall {
            output_dir: PathBuf::from("/tmp/systemd"),
            unit_prefix: "assets-gc".to_owned(),
            calendar: "hourly".to_owned(),
            retention_seconds: 600,
            binary_path: PathBuf::from("/usr/bin/shardline"),
            env_file: PathBuf::from("/etc/shardline/env"),
            working_directory: PathBuf::from("/srv/assets"),
            user: "svc".to_owned(),
            group: "svc".to_owned(),
            dry_run: false,
        })
    );
}

#[test]
fn parse_gc_schedule_uninstall() {
    let args = vec![
        "shardline".to_owned(),
        "gc".to_owned(),
        "schedule".to_owned(),
        "uninstall".to_owned(),
        "--output-dir".to_owned(),
        "/tmp/systemd".to_owned(),
        "--unit-prefix".to_owned(),
        "assets-gc".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::GcScheduleUninstall {
            output_dir: PathBuf::from("/tmp/systemd"),
            unit_prefix: "assets-gc".to_owned(),
        })
    );
}

#[test]
fn parse_backup_manifest() {
    let args = vec![
        "shardline".to_owned(),
        "backup".to_owned(),
        "manifest".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--output".to_owned(),
        "backup.json".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::BackupManifest {
            root: Some(PathBuf::from("/var/lib/shardline")),
            output: PathBuf::from("backup.json"),
        })
    );
}

#[test]
fn parse_backup_manifest_requires_output() {
    let args = vec![
        "shardline".to_owned(),
        "backup".to_owned(),
        "manifest".to_owned(),
    ];

    let parsed = CliCommand::parse(args);
    assert!(parsed.is_err());
    let Err(error) = parsed else {
        return;
    };
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    assert!(format!("{error}").contains("--output"));
}

#[test]
fn parse_db_migrate_up() {
    let args = vec![
        "shardline".to_owned(),
        "db".to_owned(),
        "migrate".to_owned(),
        "up".to_owned(),
        "--database-url".to_owned(),
        "postgres://user:password@localhost:5432/shardline".to_owned(),
        "--steps".to_owned(),
        "2".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::DbMigrate {
            database_url: Some(RedactedDbUrl(
                "postgres://user:password@localhost:5432/shardline".to_owned()
            )),
            command: DatabaseMigrationCommand::Up { steps: Some(2) },
        })
    );
}

#[test]
fn cli_command_debug_redacts_database_url_credentials() {
    let parsed = CliCommand::parse(vec![
        "shardline".to_owned(),
        "db".to_owned(),
        "migrate".to_owned(),
        "up".to_owned(),
        "--database-url".to_owned(),
        "postgres://user:database-secret@localhost:5432/shardline".to_owned(),
    ]);
    assert!(parsed.is_ok());
    let Ok(parsed) = parsed else {
        return;
    };

    let rendered = format!("{parsed:?}");

    assert!(!rendered.contains("database-secret"));
    assert!(rendered.contains("***"));
}

#[test]
fn parse_db_migrate_status() {
    let args = vec![
        "shardline".to_owned(),
        "db".to_owned(),
        "migrate".to_owned(),
        "status".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::DbMigrate {
            database_url: None,
            command: DatabaseMigrationCommand::Status,
        })
    );
}

#[test]
fn parse_db_migrate_rejects_zero_steps() {
    let args = vec![
        "shardline".to_owned(),
        "db".to_owned(),
        "migrate".to_owned(),
        "down".to_owned(),
        "--steps".to_owned(),
        "0".to_owned(),
    ];

    let parsed = CliCommand::parse(args);
    assert!(parsed.is_err());
    let Err(error) = parsed else {
        return;
    };
    assert_eq!(error.kind(), ErrorKind::ValueValidation);
    assert!(
        error
            .to_string()
            .contains("value must be a positive integer")
    );
}

#[test]
fn parse_storage_migrate() {
    let args = vec![
        "shardline".to_owned(),
        "storage".to_owned(),
        "migrate".to_owned(),
        "--from".to_owned(),
        "local".to_owned(),
        "--from-root".to_owned(),
        "/srv/assets/.shardline/data".to_owned(),
        "--to".to_owned(),
        "s3".to_owned(),
        "--prefix".to_owned(),
        "xorbs/default/".to_owned(),
        "--dry-run".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::StorageMigrate {
            from: ObjectStorageAdapter::Local,
            from_root: Some(PathBuf::from("/srv/assets/.shardline/data")),
            to: ObjectStorageAdapter::S3,
            to_root: None,
            prefix: "xorbs/default/".to_owned(),
            dry_run: true,
        })
    );
}

#[test]
fn parse_gc_with_export_paths() {
    let args = vec![
        "shardline".to_owned(),
        "gc".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--retention-report".to_owned(),
        "retention.json".to_owned(),
        "--orphan-inventory".to_owned(),
        "orphans.json".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Gc {
            root: Some(PathBuf::from("/var/lib/shardline")),
            mark: false,
            sweep: false,
            retention_seconds: DEFAULT_LOCAL_GC_RETENTION_SECONDS,
            retention_report: Some(PathBuf::from("retention.json")),
            orphan_inventory: Some(PathBuf::from("orphans.json")),
        })
    );
}

#[test]
fn parse_hold_set() {
    let args = vec![
        "shardline".to_owned(),
        "hold".to_owned(),
        "set".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--object-key".to_owned(),
        format!("de/{}", "de".repeat(32)),
        "--reason".to_owned(),
        "provider deletion grace".to_owned(),
        "--ttl-seconds".to_owned(),
        "600".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::HoldSet {
            root: Some(PathBuf::from("/var/lib/shardline")),
            object_key: format!("de/{}", "de".repeat(32)),
            reason: "provider deletion grace".to_owned(),
            ttl_seconds: Some(600),
        })
    );
}

#[test]
fn parse_hold_list() {
    let args = vec![
        "shardline".to_owned(),
        "hold".to_owned(),
        "list".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--active-only".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::HoldList {
            root: Some(PathBuf::from("/var/lib/shardline")),
            active_only: true,
        })
    );
}

#[test]
fn parse_hold_release() {
    let args = vec![
        "shardline".to_owned(),
        "hold".to_owned(),
        "release".to_owned(),
        "--root".to_owned(),
        "/var/lib/shardline".to_owned(),
        "--object-key".to_owned(),
        format!("de/{}", "de".repeat(32)),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::HoldRelease {
            root: Some(PathBuf::from("/var/lib/shardline")),
            object_key: format!("de/{}", "de".repeat(32)),
        })
    );
}

#[test]
fn parse_bench_with_explicit_options() {
    let args = vec![
        "shardline".to_owned(),
        "bench".to_owned(),
        "--storage-dir".to_owned(),
        "/var/lib/shardline-bench".to_owned(),
        "--iterations".to_owned(),
        "5".to_owned(),
        "--concurrency".to_owned(),
        "8".to_owned(),
        "--upload-max-in-flight-chunks".to_owned(),
        "32".to_owned(),
        "--chunk-size-bytes".to_owned(),
        "4096".to_owned(),
        "--base-bytes".to_owned(),
        "65536".to_owned(),
        "--mutated-bytes".to_owned(),
        "1024".to_owned(),
        "--json".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Bench {
            mode: BenchMode::EndToEnd,
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::Full,
            storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
            iterations: 5,
            concurrency: 8,
            upload_max_in_flight_chunks: 32,
            chunk_size_bytes: 4096,
            base_bytes: 65_536,
            mutated_bytes: 1024,
            json: true,
        })
    );
}

#[test]
fn parse_bench_with_configured_deployment_target() {
    let args = vec![
        "shardline".to_owned(),
        "bench".to_owned(),
        "--storage-dir".to_owned(),
        "/var/lib/shardline-bench".to_owned(),
        "--deployment-target".to_owned(),
        "configured".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Bench {
            mode: BenchMode::EndToEnd,
            deployment_target: BenchDeploymentTarget::Configured,
            scenario: BenchScenario::Full,
            storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
            iterations: 1,
            concurrency: 4,
            upload_max_in_flight_chunks: 64,
            chunk_size_bytes: 65_536,
            base_bytes: 1_048_576,
            mutated_bytes: 4_096,
            json: false,
        })
    );
}

#[test]
fn parse_ingest_bench_without_storage_dir() {
    let args = vec![
        "shardline".to_owned(),
        "bench".to_owned(),
        "--mode".to_owned(),
        "ingest".to_owned(),
        "--iterations".to_owned(),
        "3".to_owned(),
        "--concurrency".to_owned(),
        "16".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Bench {
            mode: BenchMode::Ingest,
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::Full,
            storage_dir: None,
            iterations: 3,
            concurrency: 16,
            upload_max_in_flight_chunks: 64,
            chunk_size_bytes: 65_536,
            base_bytes: 1_048_576,
            mutated_bytes: 4_096,
            json: false,
        })
    );
}

#[test]
fn parse_bench_with_focused_scenario() {
    let args = vec![
        "shardline".to_owned(),
        "bench".to_owned(),
        "--storage-dir".to_owned(),
        "/var/lib/shardline-bench".to_owned(),
        "--scenario".to_owned(),
        "cross-repository-upload".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Bench {
            mode: BenchMode::EndToEnd,
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::CrossRepositoryUpload,
            storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
            iterations: 1,
            concurrency: 4,
            upload_max_in_flight_chunks: 64,
            chunk_size_bytes: 65_536,
            base_bytes: 1_048_576,
            mutated_bytes: 4_096,
            json: false,
        })
    );
}

#[test]
fn parse_bench_with_cached_reconstruction_scenario() {
    let args = vec![
        "shardline".to_owned(),
        "bench".to_owned(),
        "--storage-dir".to_owned(),
        "/var/lib/shardline-bench".to_owned(),
        "--scenario".to_owned(),
        "cached-latest-reconstruction".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Bench {
            mode: BenchMode::EndToEnd,
            deployment_target: BenchDeploymentTarget::IsolatedLocal,
            scenario: BenchScenario::CachedLatestReconstruction,
            storage_dir: Some(PathBuf::from("/var/lib/shardline-bench")),
            iterations: 1,
            concurrency: 4,
            upload_max_in_flight_chunks: 64,
            chunk_size_bytes: 65_536,
            base_bytes: 1_048_576,
            mutated_bytes: 4_096,
            json: false,
        })
    );
}

#[test]
fn parse_bench_requires_storage_dir_for_e2e_mode() {
    let args = vec!["shardline".to_owned(), "bench".to_owned()];
    let parsed = CliCommand::parse(args);

    assert!(parsed.is_err());
    let Err(error) = parsed else {
        return;
    };
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    assert!(format!("{error}").contains("--storage-dir"));
}

#[test]
fn parse_health() {
    let args = vec![
        "shardline".to_owned(),
        "health".to_owned(),
        "--server".to_owned(),
        "http://127.0.0.1:8080".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Health {
            server_url: "http://127.0.0.1:8080".to_owned()
        })
    );
}

#[test]
fn parse_completion() {
    let args = vec![
        "shardline".to_owned(),
        "completion".to_owned(),
        "bash".to_owned(),
        "--output".to_owned(),
        "./shardline.bash".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Completion {
            shell: CompletionShell::Bash,
            output: Some(PathBuf::from("./shardline.bash")),
        })
    );
}

#[test]
fn parse_manpage() {
    let args = vec![
        "shardline".to_owned(),
        "manpage".to_owned(),
        "--output".to_owned(),
        "./shardline.1".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::Manpage {
            output: Some(PathBuf::from("./shardline.1")),
        })
    );
}

#[test]
fn parse_config_check() {
    let args = vec![
        "shardline".to_owned(),
        "config".to_owned(),
        "check".to_owned(),
    ];

    assert_eq!(CliCommand::parse(args), Ok(CliCommand::ConfigCheck));
}

#[test]
fn parse_admin_token() {
    let args = vec![
        "shardline".to_owned(),
        "admin".to_owned(),
        "token".to_owned(),
        "--issuer".to_owned(),
        "local".to_owned(),
        "--subject".to_owned(),
        "operator-1".to_owned(),
        "--scope".to_owned(),
        "write".to_owned(),
        "--provider".to_owned(),
        "github".to_owned(),
        "--owner".to_owned(),
        "team".to_owned(),
        "--repo".to_owned(),
        "assets".to_owned(),
        "--revision".to_owned(),
        "main".to_owned(),
        "--key-file".to_owned(),
        "/tmp/shardline.key".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::AdminToken {
            issuer: "local".to_owned(),
            subject: "operator-1".to_owned(),
            scope: TokenScope::Write,
            provider: RepositoryProvider::GitHub,
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: Some("main".to_owned()),
            ttl_seconds: 3600,
            key_file: Some(PathBuf::from("/tmp/shardline.key")),
            key_env: None,
        })
    );
}

#[test]
fn parse_admin_token_with_key_env() {
    let args = vec![
        "shardline".to_owned(),
        "admin".to_owned(),
        "token".to_owned(),
        "--issuer".to_owned(),
        "local".to_owned(),
        "--subject".to_owned(),
        "operator-1".to_owned(),
        "--scope".to_owned(),
        "write".to_owned(),
        "--provider".to_owned(),
        "github".to_owned(),
        "--owner".to_owned(),
        "team".to_owned(),
        "--repo".to_owned(),
        "assets".to_owned(),
        "--key-env".to_owned(),
        "SHARDLINE_TOKEN_SIGNING_KEY".to_owned(),
    ];

    assert_eq!(
        CliCommand::parse(args),
        Ok(CliCommand::AdminToken {
            issuer: "local".to_owned(),
            subject: "operator-1".to_owned(),
            scope: TokenScope::Write,
            provider: RepositoryProvider::GitHub,
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: None,
            ttl_seconds: 3600,
            key_file: None,
            key_env: Some("SHARDLINE_TOKEN_SIGNING_KEY".to_owned()),
        })
    );
}

#[test]
fn parse_rejects_unknown_command() {
    let args = vec!["shardline".to_owned(), "unknown".to_owned()];
    let parsed = CliCommand::parse(args);

    assert!(parsed.is_err());
    let Err(error) = parsed else {
        return;
    };
    assert_eq!(error.kind(), ErrorKind::InvalidSubcommand);
    assert!(format!("{error}").contains("unknown"));
}

#[test]
fn parse_rejects_incomplete_nested_commands() {
    let config = vec!["shardline".to_owned(), "config".to_owned()];
    let admin = vec!["shardline".to_owned(), "admin".to_owned()];
    let providerless = vec!["shardline".to_owned(), "providerless".to_owned()];

    for args in [config, admin, providerless] {
        let parsed = CliCommand::parse(args);
        assert!(parsed.is_err());
        let Err(error) = parsed else {
            return;
        };
        assert_eq!(
            error.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }
}

// ── parse_positive_usize ───────────────────────────────────────────────

#[test]
fn parse_positive_usize_accepts_positive_number() {
    let result = super::parse_positive_usize("42");
    assert_eq!(result, Ok(std::num::NonZeroUsize::new(42).unwrap()));
}

#[test]
fn parse_positive_usize_rejects_zero() {
    let result = super::parse_positive_usize("0");
    assert!(result.is_err());
}

#[test]
fn parse_positive_usize_rejects_non_numeric() {
    let result = super::parse_positive_usize("abc");
    assert!(result.is_err());
}

#[test]
fn parse_positive_usize_rejects_negative_number() {
    let result = super::parse_positive_usize("-5");
    assert!(result.is_err());
}

#[test]
fn parse_positive_usize_rejects_overflow() {
    // A value larger than usize::MAX should fail to parse.
    let result = super::parse_positive_usize("99999999999999999999999999999999999999");
    assert!(result.is_err());
}

#[test]
fn parse_positive_usize_accepts_leading_zeros() {
    let result = super::parse_positive_usize("001");
    assert_eq!(result, Ok(std::num::NonZeroUsize::new(1).unwrap()));
}

#[test]
fn parse_positive_usize_rejects_empty_string() {
    let result = super::parse_positive_usize("");
    assert!(result.is_err());
}

#[test]
fn parse_positive_usize_accepts_plus_prefix() {
    // Rust's usize::from_str_radix accepts an optional '+' sign.
    let result = super::parse_positive_usize("+42");
    assert_eq!(result, Ok(std::num::NonZeroUsize::new(42).unwrap()));
}

// ── deduplicated_cli_frontends ──────────────────────────────────────────

#[test]
fn deduplicated_cli_frontends_removes_duplicates() {
    use shardline_server::ServerFrontend;
    let frontends = vec![
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::Xet,
        ServerFrontend::Oci,
        ServerFrontend::Lfs,
    ];
    let deduped = super::deduplicated_cli_frontends(frontends);
    assert_eq!(
        deduped,
        vec![
            ServerFrontend::Xet,
            ServerFrontend::Lfs,
            ServerFrontend::Oci
        ]
    );
}

#[test]
fn deduplicated_cli_frontends_preserves_order_of_first_seen() {
    use shardline_server::ServerFrontend;
    let frontends = vec![
        ServerFrontend::Oci,
        ServerFrontend::Xet,
        ServerFrontend::Oci,
    ];
    let deduped = super::deduplicated_cli_frontends(frontends);
    assert_eq!(deduped, vec![ServerFrontend::Oci, ServerFrontend::Xet]);
}

#[test]
fn deduplicated_cli_frontends_handles_empty_input() {
    let deduped = super::deduplicated_cli_frontends(vec![]);
    assert!(deduped.is_empty());
}

// ── RedactedDbUrl ──────────────────────────────────────────────────────

#[test]
fn redacted_db_url_debug_hides_url() {
    let url = RedactedDbUrl("postgres://user:pass@localhost/db".to_owned());
    let debug = format!("{url:?}");
    assert_eq!(debug, "***");
    assert!(!debug.contains("postgres"));
}

#[test]
fn redacted_db_url_as_str_returns_original() {
    let url = RedactedDbUrl("postgres://localhost/db".to_owned());
    assert_eq!(url.as_str(), "postgres://localhost/db");
}

#[allow(clippy::redundant_clone)]
#[test]
fn redacted_db_url_clone() {
    let url = RedactedDbUrl("url".to_owned());
    let cloned = url.clone();
    assert_eq!(cloned.as_str(), "url");
}

#[test]
fn redacted_db_url_partial_eq() {
    let a = RedactedDbUrl("same".to_owned());
    let b = RedactedDbUrl("same".to_owned());
    assert_eq!(a, b);
}

// ── CliParseError ──────────────────────────────────────────────────────

#[test]
fn cli_parse_error_display_contains_message() {
    let error = super::CliParseError::validation(
        clap::error::ErrorKind::InvalidSubcommand,
        "unknown command",
    );
    let msg = format!("{error}");
    assert!(msg.contains("unknown command"));
}

#[test]
fn cli_parse_error_kind() {
    let error = super::CliParseError::validation(clap::error::ErrorKind::InvalidSubcommand, "bad");
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
}

#[test]
fn cli_parse_error_is_help_true_for_display_help() {
    let error = super::CliParseError::validation(clap::error::ErrorKind::DisplayHelp, "help");
    assert!(error.is_help());
}

#[test]
fn cli_parse_error_is_help_true_for_display_version() {
    let error = super::CliParseError::validation(clap::error::ErrorKind::DisplayVersion, "version");
    assert!(error.is_help());
}

#[test]
fn cli_parse_error_is_help_false_for_other_kinds() {
    let error = super::CliParseError::validation(clap::error::ErrorKind::InvalidSubcommand, "bad");
    assert!(!error.is_help());
}

#[test]
fn cli_parse_error_from_clap_error() {
    use clap::CommandFactory;
    let clap_err = super::CliDefinition::command().error(
        clap::error::ErrorKind::MissingRequiredArgument,
        "missing --flag",
    );
    let error = super::CliParseError::from(clap_err);
    assert!(format!("{error}").contains("missing --flag"));
}

// ── From impls for CLI enums ──────────────────────────────────────────

#[test]
fn cli_server_role_to_server_role() {
    use shardline_server::ServerRole;
    assert_eq!(ServerRole::from(super::CliServerRole::All), ServerRole::All);
    assert_eq!(ServerRole::from(super::CliServerRole::Api), ServerRole::Api);
    assert_eq!(
        ServerRole::from(super::CliServerRole::Transfer),
        ServerRole::Transfer
    );
}

#[test]
fn cli_server_frontend_to_server_frontend() {
    use shardline_server::ServerFrontend;
    assert_eq!(
        ServerFrontend::from(super::CliServerFrontend::Xet),
        ServerFrontend::Xet
    );
    assert_eq!(
        ServerFrontend::from(super::CliServerFrontend::Lfs),
        ServerFrontend::Lfs
    );
    assert_eq!(
        ServerFrontend::from(super::CliServerFrontend::BazelHttp),
        ServerFrontend::BazelHttp
    );
    assert_eq!(
        ServerFrontend::from(super::CliServerFrontend::Oci),
        ServerFrontend::Oci
    );
    assert_eq!(
        ServerFrontend::from(super::CliServerFrontend::Hub),
        ServerFrontend::Hub
    );
    assert_eq!(
        ServerFrontend::from(super::CliServerFrontend::S3),
        ServerFrontend::S3
    );
}

#[test]
fn cli_token_scope_to_token_scope() {
    use shardline_protocol::TokenScope;
    assert_eq!(
        TokenScope::from(super::CliTokenScope::Read),
        TokenScope::Read
    );
    assert_eq!(
        TokenScope::from(super::CliTokenScope::Write),
        TokenScope::Write
    );
}

#[test]
fn cli_repository_provider_to_repository_provider() {
    use shardline_protocol::RepositoryProvider;
    assert_eq!(
        RepositoryProvider::from(super::CliRepositoryProvider::GitHub),
        RepositoryProvider::GitHub
    );
    assert_eq!(
        RepositoryProvider::from(super::CliRepositoryProvider::Gitea),
        RepositoryProvider::Gitea
    );
    assert_eq!(
        RepositoryProvider::from(super::CliRepositoryProvider::GitLab),
        RepositoryProvider::GitLab
    );
    assert_eq!(
        RepositoryProvider::from(super::CliRepositoryProvider::Codeberg),
        RepositoryProvider::Codeberg
    );
    assert_eq!(
        RepositoryProvider::from(super::CliRepositoryProvider::Generic),
        RepositoryProvider::Generic
    );
}

#[test]
fn cli_object_storage_adapter_to_object_storage_adapter() {
    use shardline_server::ObjectStorageAdapter;
    assert_eq!(
        ObjectStorageAdapter::from(super::CliObjectStorageAdapter::Local),
        ObjectStorageAdapter::Local
    );
    assert_eq!(
        ObjectStorageAdapter::from(super::CliObjectStorageAdapter::S3),
        ObjectStorageAdapter::S3
    );
}

// ── CompletionShell ─────────────────────────────────────────────────────

#[test]
fn completion_shell_value_enum_variants() {
    assert_eq!(super::CompletionShell::Bash as u8, 0);
    assert_eq!(super::CompletionShell::Elvish as u8, 1);
    assert_eq!(super::CompletionShell::Fish as u8, 2);
    assert_eq!(super::CompletionShell::PowerShell as u8, 3);
    assert_eq!(super::CompletionShell::Zsh as u8, 4);
}
