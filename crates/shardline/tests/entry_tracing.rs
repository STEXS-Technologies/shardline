#![allow(clippy::expect_used)]

use std::{
    env,
    ffi::OsString,
    process::{Command, ExitCode},
};

const CHILD_MODE: &str = "SHARDLINE_TEST_ENTRY_TRACING_CASE";

#[test]
fn entry_run_can_be_called_repeatedly() {
    run_isolated_case("entry_run_can_be_called_repeatedly", "repeat");
}

#[test]
fn entry_run_preserves_the_callers_subscriber() {
    run_isolated_case("entry_run_preserves_the_callers_subscriber", "preinstalled");
}

fn run_isolated_case(test_name: &str, mode: &str) {
    if let Ok(child_mode) = env::var(CHILD_MODE) {
        assert_eq!(child_mode, mode);
        exercise_entry(mode);
        return;
    }

    // Each child owns its tracing globals. Neither test installs a subscriber
    // in the parent process, so parallel tests and nextest remain independent.
    let sandbox = tempfile::tempdir().expect("owned working directory");
    let output = Command::new(env::current_exe().expect("test executable"))
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_MODE, mode)
        .env("RUST_LOG", "off")
        .current_dir(sandbox.path())
        .output()
        .expect("isolated entry subprocess");
    assert!(
        output.status.success(),
        "entry subprocess failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("shardline"));
}

fn exercise_entry(mode: &str) {
    if mode == "preinstalled" {
        tracing::subscriber::set_global_default(tracing::subscriber::NoSubscriber::default())
            .expect("fresh process subscriber");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("entry runtime");

    for option in ["--version", "--help"] {
        let result = runtime.block_on(shardline::entry::run(
            [OsString::from("shardline"), OsString::from(option)].into_iter(),
        ));
        assert_eq!(result, ExitCode::SUCCESS);
        if mode == "preinstalled" {
            tracing::dispatcher::get_default(|dispatcher| {
                assert!(dispatcher.is::<tracing::subscriber::NoSubscriber>());
            });
        }
    }
}
