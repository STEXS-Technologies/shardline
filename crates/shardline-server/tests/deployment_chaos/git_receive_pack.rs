//! Kill the deployed server inside an atomic receive-pack ref transaction.
use super::*;
use std::process::Output;

async fn git(directory: &Path, token: &str, arguments: Vec<String>) -> Output {
    let directory = directory.to_owned();
    let token = token.to_owned();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("git")
            .arg("-C")
            .arg(directory)
            .arg("-c")
            .arg(format!("http.extraHeader=Authorization: Bearer {token}"))
            .args(arguments)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_HTTP_LOW_SPEED_LIMIT", "1")
            .env("GIT_HTTP_LOW_SPEED_TIME", "10")
            .output()
            .expect("native Git must be installed")
    })
    .await
    .expect("Git process task did not panic")
}

fn assert_git_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_deploy_git_receive_pack_process_kill_rolls_back_atomic_refs() {
    let drill = "drill_deploy_git_receive_pack_process_kill_rolls_back_atomic_refs";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    migrate_chaos_postgres(&stack.pg_url).await;

    let nonce = lfs_run_nonce();
    let repository = format!("process-kill/git-{nonce}");
    let token = mint_token("process-kill", &format!("git-{nonce}"), TokenScope::Write);
    let server_root = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "hub"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
            (
                "SHARDLINE_HUB_WEBHOOK_SECRET_KEY",
                "0123456789abcdef0123456789abcdef",
            ),
        ],
        server_root.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;
    let base = server.base_url();

    let created = reqwest::Client::new()
        .post(format!("{base}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"type": "model", "name": repository}))
        .send()
        .await
        .expect("create repository");
    assert_eq!(created.status().as_u16(), 201, "repository is created");

    let work = TempDir::new().unwrap();
    let remote = format!("{base}/models/{repository}");
    let checkout = work.path().join("checkout");
    assert_git_success(
        &git(
            work.path(),
            &token,
            vec![
                "clone".into(),
                remote.clone(),
                checkout.display().to_string(),
            ],
        )
        .await,
        "clone empty Hub repository",
    );
    assert_git_success(
        &git(
            &checkout,
            &token,
            vec!["config".into(), "user.name".into(), "Crash Drill".into()],
        )
        .await,
        "configure Git author name",
    );
    assert_git_success(
        &git(
            &checkout,
            &token,
            vec![
                "config".into(),
                "user.email".into(),
                "crash-drill@example.invalid".into(),
            ],
        )
        .await,
        "configure Git author email",
    );
    std::fs::write(checkout.join("payload.txt"), b"before process kill\n").unwrap();
    for arguments in [
        vec!["add".into(), "payload.txt".into()],
        vec!["commit".into(), "-m".into(), "Initial".into()],
    ] {
        assert_git_success(
            &git(&checkout, &token, arguments).await,
            "create initial commit",
        );
    }
    assert_git_success(
        &git(
            &checkout,
            &token,
            vec![
                "push".into(),
                "--atomic".into(),
                "origin".into(),
                "HEAD:main".into(),
                "HEAD:peer".into(),
            ],
        )
        .await,
        "publish initial refs",
    );
    let previous_head = String::from_utf8(
        git(&checkout, &token, vec!["rev-parse".into(), "HEAD".into()])
            .await
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();

    std::fs::write(checkout.join("payload.txt"), b"after process kill\n").unwrap();
    for arguments in [
        vec!["add".into(), "payload.txt".into()],
        vec!["commit".into(), "-m".into(), "Second".into()],
    ] {
        assert_git_success(
            &git(&checkout, &token, arguments).await,
            "create next commit",
        );
    }
    let next_head = String::from_utf8(
        git(&checkout, &token, vec!["rev-parse".into(), "HEAD".into()])
            .await
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .connect(&stack.pg_url)
        .await
        .expect("connect to disposable PostgreSQL");
    let function = format!("git_process_kill_{}_{}", std::process::id(), nonce);
    let marker = i32::try_from(nonce % (i64::from(i32::MAX) as u64)).unwrap();
    assert!(
        repository
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"/-".contains(&byte)),
        "fixture repository name contains only safe SQL predicate bytes"
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION public.{function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
         PERFORM pg_advisory_xact_lock(174893, {marker}); PERFORM pg_sleep(8); RETURN NEW; END $$;
         CREATE TRIGGER {function} BEFORE INSERT ON public.shardline_hub_refs
         FOR EACH ROW WHEN (NEW.repo_id = '{repository}' AND NEW.ref_name = 'peer')
         EXECUTE FUNCTION public.{function}();"
    )))
    .execute(&pool)
    .await
    .expect("install deterministic mid-publication pause");

    let push_directory = checkout.clone();
    let push_token = token.clone();
    let push = tokio::spawn(async move {
        git(
            &push_directory,
            &push_token,
            vec![
                "push".into(),
                "--atomic".into(),
                "origin".into(),
                "HEAD:main".into(),
                "HEAD:peer".into(),
            ],
        )
        .await
    });

    let backend_pid = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let pid = sqlx::query_scalar::<_, i32>(
                "SELECT activity.pid FROM pg_locks locks
                 JOIN pg_stat_activity activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory' AND locks.classid = 174893
                   AND locks.objid::bigint = $1 AND locks.objsubid = 2 AND locks.granted
                   AND activity.wait_event = 'PgSleep'",
            )
            .bind(i64::from(marker))
            .fetch_optional(&pool)
            .await
            .expect("inspect active publication transaction");
            if let Some(pid) = pid {
                break pid;
            }
            assert!(
                !push.is_finished(),
                "native Git push ended before the second-ref transaction boundary"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("receive-pack reached the paused second-ref write");

    server.kill_hard();
    assert!(!server.alive(), "the Shardline process was killed");
    let push_result = tokio::time::timeout(Duration::from_secs(15), push)
        .await
        .expect("Git client disconnects after server process death")
        .expect("Git process task did not panic");
    assert!(
        !push_result.status.success(),
        "the client must observe an interrupted push"
    );

    // PostgreSQL may finish `pg_sleep` before it notices the killed client's
    // closed connection; wait for that bounded trigger body and transaction
    // rollback rather than force-terminating the database backend.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let active: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
                    .bind(backend_pid)
                    .fetch_one(&pool)
                    .await
                    .expect("check terminated publication session");
            if !active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("killed process releases its PostgreSQL transaction");

    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER {function} ON public.shardline_hub_refs;
         DROP FUNCTION public.{function}();"
    )))
    .execute(&pool)
    .await
    .expect("remove deterministic publication pause");

    server.restart();
    server.wait_ready(Duration::from_secs(15)).await;
    let refs = git(
        &checkout,
        &token,
        vec![
            "ls-remote".into(),
            "origin".into(),
            "refs/heads/main".into(),
            "refs/heads/peer".into(),
        ],
    )
    .await;
    assert_git_success(&refs, "read refs after process restart");
    let refs = String::from_utf8(refs.stdout).unwrap();
    assert_eq!(
        refs,
        format!("{previous_head}\trefs/heads/main\n{previous_head}\trefs/heads/peer\n"),
        "the interrupted atomic push must leave every ref at the old commit"
    );

    assert_git_success(
        &git(
            &checkout,
            &token,
            vec![
                "push".into(),
                "--atomic".into(),
                "origin".into(),
                "HEAD:main".into(),
                "HEAD:peer".into(),
            ],
        )
        .await,
        "retry atomic push after process restart",
    );
    let refs = git(
        &checkout,
        &token,
        vec![
            "ls-remote".into(),
            "origin".into(),
            "refs/heads/main".into(),
            "refs/heads/peer".into(),
        ],
    )
    .await;
    assert_git_success(&refs, "verify refs after retry");
    assert_eq!(
        String::from_utf8(refs.stdout).unwrap(),
        format!("{next_head}\trefs/heads/main\n{next_head}\trefs/heads/peer\n")
    );

    let clone_dir = work.path().join("after-process-kill");
    assert_git_success(
        &git(
            work.path(),
            &token,
            vec![
                "clone".into(),
                remote.clone(),
                clone_dir.display().to_string(),
            ],
        )
        .await,
        "clone after successful retry",
    );
    assert_eq!(
        std::fs::read(clone_dir.join("payload.txt")).unwrap(),
        b"after process kill\n"
    );
    assert_git_success(
        &git(&clone_dir, &token, vec!["fsck".into(), "--strict".into()]).await,
        "verify repository after process restart",
    );

    pool.close().await;
    server.kill_hard();
    server_root.close().unwrap();
    eprintln!(
        "chaos({drill}): PASS - killed inside atomic receive-pack; refs rolled back together and retry survived restart"
    );
}
