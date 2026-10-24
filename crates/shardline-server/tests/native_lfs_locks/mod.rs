//! Stock Git LFS locking across independently started replicas and restart.
use super::*;
use std::process::{Command, Output};

async fn command(
    root: &Path,
    program: &str,
    arguments: &[&str],
    endpoint: &str,
    credential: &str,
) -> Output {
    let root = root.to_owned();
    let program = program.to_owned();
    let arguments: Vec<_> = arguments.iter().map(|value| (*value).to_owned()).collect();
    let endpoint = endpoint.to_owned();
    let credential = credential.to_owned();
    tokio::task::spawn_blocking(move || {
        shardline_test_support::native_client_output(
            Command::new(program)
                .current_dir(root)
                .args(arguments)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GIT_CONFIG_COUNT", "2")
                .env("GIT_CONFIG_KEY_0", "lfs.url")
                .env("GIT_CONFIG_VALUE_0", endpoint)
                .env("GIT_CONFIG_KEY_1", "http.extraHeader")
                .env(
                    "GIT_CONFIG_VALUE_1",
                    format!("Authorization: Bearer {credential}"),
                ),
            std::time::Duration::from_secs(600),
        )
        .expect("stock Git/LFS client failed to execute or exceeded its deadline")
    })
    .await
    .unwrap()
}

async fn lfs(root: &Path, arguments: &[&str], endpoint: &str, credential: &str) -> Value {
    let output = command(root, "git-lfs", arguments, endpoint, credential).await;
    assert!(output.status.success(), "stock Git LFS command failed");
    serde_json::from_slice(&output.stdout).expect("stock Git LFS JSON response")
}

async fn checkout(root: &Path) {
    for arguments in [
        vec!["init", "--initial-branch=main"],
        vec!["config", "user.name", "LFS Qualification"],
        vec!["config", "user.email", "lfs@example.invalid"],
        vec!["remote", "add", "origin", "http://localhost/unused.git"],
    ] {
        assert!(
            command(root, "git", &arguments, "", "")
                .await
                .status
                .success()
        );
    }
    std::fs::write(root.join("asset.bin"), b"locked asset").unwrap();
    for arguments in [
        vec!["add", "asset.bin"],
        vec!["-c", "commit.gpgsign=false", "commit", "-m", "asset"],
    ] {
        assert!(
            command(root, "git", &arguments, "", "")
                .await
                .status
                .success()
        );
    }
}

async fn exercise(database: Option<&str>) {
    for frontend in [ServerFrontend::Lfs, ServerFrontend::Hub] {
        let first_root = tempfile::tempdir().unwrap();
        let second_root = tempfile::tempdir().unwrap();
        let first = start(first_root.path(), frontend, true, database).await;
        let second = start(
            if database.is_some() {
                second_root.path()
            } else {
                first_root.path()
            },
            frontend,
            true,
            database,
        )
        .await;
        let repository = format!(
            "native-lfs-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        let endpoint = |base: &str| {
            lock_url(base, &repository, frontend)
                .trim_end_matches("/locks")
                .to_owned()
        };
        let alice = token("alice", &repository, TokenScope::Write);
        let bob = token("bob", &repository, TokenScope::Write);
        let work = tempfile::tempdir().unwrap();
        let other_work = tempfile::tempdir().unwrap();
        checkout(work.path()).await;
        checkout(other_work.path()).await;
        let created = lfs(
            work.path(),
            &["lock", "--json", "asset.bin"],
            &endpoint(&first.base),
            &alice,
        )
        .await;
        assert_eq!(created.as_array().unwrap().len(), 1);
        let id = created[0]["id"].as_str().expect("created lock id");
        assert_eq!(created[0]["owner"]["name"], "alice");
        let listed = lfs(
            other_work.path(),
            &["locks", "--json"],
            &endpoint(&second.base),
            &bob,
        )
        .await;
        assert_eq!(listed[0]["id"], id);
        let verified = lfs(
            other_work.path(),
            &["locks", "--verify", "--json"],
            &endpoint(&second.base),
            &bob,
        )
        .await;
        assert!(verified["ours"].as_array().unwrap().is_empty());
        assert_eq!(verified["theirs"][0]["id"], id);
        let denied = command(
            other_work.path(),
            "git-lfs",
            &["unlock", "--json", "--id", id],
            &endpoint(&second.base),
            &bob,
        )
        .await;
        assert!(
            !denied.status.success(),
            "another owner must not unlock without force"
        );
        let conflict = command(
            other_work.path(),
            "git-lfs",
            &["lock", "--json", "asset.bin"],
            &endpoint(&second.base),
            &bob,
        )
        .await;
        assert!(
            !conflict.status.success(),
            "an existing lock must remain exclusive"
        );
        first.stop().await;
        let restarted = start(first_root.path(), frontend, true, database).await;
        let listed = lfs(
            work.path(),
            &["locks", "--json"],
            &endpoint(&restarted.base),
            &alice,
        )
        .await;
        assert_eq!(listed[0]["id"], id);
        lfs(
            work.path(),
            &["unlock", "--json", "--id", id],
            &endpoint(&restarted.base),
            &alice,
        )
        .await;
        let listed = lfs(
            work.path(),
            &["locks", "--json"],
            &endpoint(&second.base),
            &bob,
        )
        .await;
        assert!(listed.as_array().unwrap().is_empty());
        let first_endpoint = endpoint(&restarted.base);
        let second_endpoint = endpoint(&second.base);
        let (alice_attempt, bob_attempt) = tokio::join!(
            command(
                work.path(),
                "git-lfs",
                &["lock", "--json", "asset.bin"],
                &first_endpoint,
                &alice
            ),
            command(
                other_work.path(),
                "git-lfs",
                &["lock", "--json", "asset.bin"],
                &second_endpoint,
                &bob
            ),
        );
        assert_ne!(
            alice_attempt.status.success(),
            bob_attempt.status.success(),
            "exactly one competing client must acquire the lock"
        );
        let (winner, other_owner) = if alice_attempt.status.success() {
            (&alice_attempt, &bob)
        } else {
            (&bob_attempt, &alice)
        };
        let winner: Value = serde_json::from_slice(&winner.stdout).unwrap();
        let winner_id = winner[0]["id"].as_str().unwrap();
        let listed = lfs(
            other_work.path(),
            &["locks", "--json"],
            &second_endpoint,
            &bob,
        )
        .await;
        assert_eq!(listed.as_array().unwrap().len(), 1);
        assert_eq!(listed[0]["id"], winner_id);
        lfs(
            other_work.path(),
            &["unlock", "--json", "--force", "--id", winner_id],
            &second_endpoint,
            other_owner,
        )
        .await;
        let listed = lfs(work.path(), &["locks", "--json"], &first_endpoint, &alice).await;
        assert!(listed.as_array().unwrap().is_empty());
        second.stop().await;
        restarted.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stock_git_lfs_locks_across_sqlite_replicas_and_restart() {
    let available = shardline_test_support::native_client_output(
        Command::new("git-lfs").arg("version"),
        std::time::Duration::from_secs(10),
    )
    .is_ok_and(|out| out.status.success());
    if !shardline_test_support::require_native_client("git-lfs", available) {
        return;
    }
    exercise(None).await;
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and stock git-lfs"]
async fn stock_git_lfs_locks_across_postgres_replicas_and_restart() {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .start()
            .unwrap()
            .expect("Docker PostgreSQL is required")
    })
    .await
    .unwrap();
    let database = stack.postgres_url().unwrap();
    exercise(Some(&database)).await;
}
