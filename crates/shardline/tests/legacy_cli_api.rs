#![allow(clippy::panic_in_result_fn)]
use shardline::CliCommand;

// Compile as a downstream crate: adding a variant to the released exhaustive
// enum would break this match even when existing CLI invocations still work.
const fn legacy_command(command: &CliCommand) -> bool {
    match command {
        CliCommand::ProviderlessSetup
        | CliCommand::Serve { .. }
        | CliCommand::ConfigCheck
        | CliCommand::DbMigrate { .. }
        | CliCommand::DbMigrateLocalUp { .. }
        | CliCommand::AdminToken { .. }
        | CliCommand::Fsck { .. }
        | CliCommand::IndexRebuild { .. }
        | CliCommand::Gc { .. }
        | CliCommand::GcScheduleInstall { .. }
        | CliCommand::GcScheduleUninstall { .. }
        | CliCommand::RepairHubTree { .. }
        | CliCommand::Repair { .. }
        | CliCommand::RepairLifecycle { .. }
        | CliCommand::RepairLfsEvidence { .. }
        | CliCommand::BackupManifest { .. }
        | CliCommand::StorageMigrate { .. }
        | CliCommand::HoldSet { .. }
        | CliCommand::HoldList { .. }
        | CliCommand::HoldRelease { .. }
        | CliCommand::Bench { .. }
        | CliCommand::Health { .. }
        | CliCommand::Completion { .. }
        | CliCommand::Manpage { .. } => true,
    }
}

#[test]
fn released_exhaustive_match_still_compiles_and_parses() -> Result<(), Box<dyn std::error::Error>> {
    let command = CliCommand::parse(["shardline", "config", "check"])?;
    assert!(legacy_command(&command));
    assert_eq!(command, CliCommand::ConfigCheck);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn managed_commands_are_rejected_before_loading_legacy_callers_env_file()
-> Result<(), Box<dyn std::error::Error>> {
    let error = CliCommand::parse([
        "shardline",
        "--env-file",
        "/nonexistent/shardline-legacy-api.env",
        "maintenance-worker",
        "--journal",
        "/nonexistent/journal",
        "--scope",
        "test",
        "--generation",
        "1",
    ])
    .err()
    .ok_or("legacy parsing unexpectedly admitted a managed command")?;
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    assert!(error.to_string().contains("shardline::run"));
    Ok(())
}
#[test]
fn history_is_rejected_before_loading_legacy_callers_env_file()
-> Result<(), Box<dyn std::error::Error>> {
    let error = shardline::CliCommand::parse([
        "shardline",
        "--env-file",
        "/nonexistent/shardline-history.env",
        "history",
    ])
    .err()
    .ok_or("legacy parsing unexpectedly admitted history dispatch")?;
    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    assert!(error.to_string().contains("shardline::run"));
    Ok(())
}
