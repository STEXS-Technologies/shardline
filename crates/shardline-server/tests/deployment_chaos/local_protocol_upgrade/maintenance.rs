//! Real candidate maintenance against metadata shared with released processes.
use super::*;

pub(super) async fn verify_hub_lifecycle(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
) {
    let output = run(
        binary,
        root,
        postgres,
        storage_env,
        "hub",
        &["repair", "lifecycle"],
    )
    .await;
    let summary = String::from_utf8_lossy(&output.stdout);
    assert!(
        summary.lines().any(|line| {
            line.strip_prefix("scanned_records: ")
                .and_then(|count| count.parse::<u64>().ok())
                .is_some_and(|count| count > 0)
        }),
        "lifecycle repair must inspect published records"
    );
}

pub(super) async fn gc_before_rollback(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
    frontends: &str,
) {
    run(
        binary,
        root,
        postgres,
        storage_env,
        frontends,
        &["gc", "--mark", "--sweep", "--retention-seconds", "0"],
    )
    .await;
}

pub(super) async fn verify_and_rebuild(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
) {
    for (args, counter) in [
        (&["fsck"][..], "version_records"),
        (&["index", "rebuild"][..], "scanned_version_records"),
        (&["fsck"][..], "version_records"),
    ] {
        let output = run(binary, root, postgres, storage_env, "hub", args).await;
        let summary = String::from_utf8_lossy(&output.stdout);
        let count = summary.lines().find_map(|line| {
            let (name, value) = line.split_once(": ")?;
            (name == counter)
                .then(|| value.parse::<usize>().ok())
                .flatten()
        });
        assert!(
            count.is_some_and(|count| count >= 1),
            "maintenance {args:?} must inspect native storage records: {counter}={count:?}"
        );
        assert!(
            summary.lines().any(|line| line == "issue_count: 0"),
            "maintenance must report zero integrity issues"
        );
    }
}

async fn run(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
    frontends: &str,
    args: &[&str],
) -> std::process::Output {
    run_with_status(binary, root, postgres, storage_env, frontends, args, 0).await
}

pub(super) async fn recover_missing_native_index(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
) {
    let output = run_with_status(binary, root, postgres, storage_env, "xet", &["fsck"], 1).await;
    let summary = String::from_utf8_lossy(&output.stdout);
    let count = summary
        .lines()
        .find_map(|line| line.strip_prefix("issue_count: ")?.parse::<usize>().ok())
        .unwrap();
    assert!(count > 0, "missing native discovery must remain observable");
    let details = String::from_utf8_lossy(&output.stderr);
    let issues: Vec<_> = details
        .lines()
        .filter_map(|line| line.strip_prefix("issue: "))
        .collect();
    assert_eq!(
        issues.len(),
        count,
        "every finding must have a classified diagnostic"
    );
    assert!(
        issues.iter().all(|line| line.starts_with("missing_chunk ")),
        "index loss must not hide other integrity failures"
    );
    for args in [&["index", "rebuild"][..], &["fsck"][..]] {
        let output = run(binary, root, postgres, storage_env, "xet", args).await;
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == "issue_count: 0"),
            "recovered native index must pass maintenance checks"
        );
    }
}

async fn run_with_status(
    binary: &Path,
    root: &Path,
    postgres: Option<&str>,
    storage_env: &[(&str, String)],
    frontends: &str,
    args: &[&str],
    expected_status: i32,
) -> std::process::Output {
    let config = NamedTempFile::new().unwrap();
    let mut command = Command::new(binary);
    command
        .env_clear()
        .arg("--config")
        .arg(config.path())
        .args(args)
        .env("SHARDLINE_ROOT_DIR", root)
        .env(
            "SHARDLINE_TOKEN_SIGNING_KEY",
            std::str::from_utf8(TEST_SIGNING_KEY).unwrap(),
        )
        .env("TMPDIR", std::env::temp_dir())
        .env("SHARDLINE_OBJECT_STORAGE_ADAPTER", "local")
        .env("SHARDLINE_SERVER_FRONTENDS", frontends);
    if let Some(url) = postgres {
        command.env("SHARDLINE_INDEX_POSTGRES_URL", url);
    }
    for (name, value) in storage_env {
        command.env(name, value);
    }
    let output = tokio::task::spawn_blocking(move || {
        shardline_test_support::native_client_output(&mut command, Duration::from_secs(90))
    })
    .await
    .expect("candidate maintenance worker")
    .expect("candidate maintenance process");
    // Classify failures without printing URLs, tokens or raw process output.
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    let diagnostic: Vec<_> = [
        "content hash",
        "configuration",
        "frontend",
        "encryption",
        "token",
        "metadata",
        "migration",
    ]
    .into_iter()
    .filter(|category| stderr.contains(category))
    .collect();
    assert!(
        output.status.code() == Some(expected_status),
        "candidate maintenance {args:?} expected exit {expected_status}: {} {diagnostic:?}",
        output.status,
    );
    output
}
