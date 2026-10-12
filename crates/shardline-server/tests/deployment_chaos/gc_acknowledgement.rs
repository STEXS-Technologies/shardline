//! A real S3 mutation succeeds while its ordinary CLI acknowledgement is lost.
use super::*;
use axum::{Router, body::Body, extract::State, http::Request, response::IntoResponse};
use shardline_index::{AsyncIndexStore, PostgresIndexStore, QuarantineCandidate};
use shardline_server::{
    ObjectStorageAdapter, ServerConfig,
    task_tracking::{TaskEvidenceStore, TaskExecutionMode, TaskRunState, deployment_fingerprint},
};
use shardline_storage::{ObjectBody, ObjectIntegrity};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::Semaphore;

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _removed = Command::new("docker")
            .args(["rm", "--force", &self.0])
            .output();
    }
}

#[derive(Clone)]
struct FaultProxy {
    backend: String,
    delete_path: String,
    delete_key: String,
    applied: Arc<AtomicUsize>,
    arrived: Arc<AtomicUsize>,
    before_apply: bool,
    deny_saved_replies: bool,
    denied_replies: Arc<AtomicUsize>,
    acknowledge: Arc<Semaphore>,
    client: reqwest::Client,
}

async fn forward(
    State(proxy): State<FaultProxy>,
    request: Request<Body>,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
    if proxy.deny_saved_replies
        && parts.method == axum::http::Method::PUT
        && parts
            .uri
            .path()
            .contains("/_shardline_tasks/v1/delete-replies/")
    {
        proxy.denied_replies.fetch_add(1, Ordering::Release);
        return (
            axum::http::StatusCode::FORBIDDEN,
            "saved reply writes denied by fixture policy",
        )
            .into_response();
    }
    // object_store 0.14 routes single-object deletion through S3's bulk API.
    // Match the actual object in either protocol, not every bucket mutation.
    let is_target_delete = (parts.method == axum::http::Method::DELETE
        && parts.uri.path() == proxy.delete_path)
        || (parts.method == axum::http::Method::POST
            && parts.uri.path() == format!("/{OBJECT_BUCKET}")
            && parts.uri.query().is_some_and(|query| {
                query
                    .split('&')
                    .any(|part| part.split('=').next() == Some("delete"))
            })
            && String::from_utf8_lossy(&bytes)
                .contains(&format!("<Key>{}</Key>", proxy.delete_key)));
    if is_target_delete {
        eprintln!(
            "fault proxy saw delete mutation {} (target {})",
            parts.uri.path(),
            proxy.delete_path
        );
        if proxy.before_apply {
            proxy.arrived.fetch_add(1, Ordering::Release);
            let permit = proxy.acknowledge.acquire().await.unwrap();
            permit.forget();
        }
    }
    // Preserve the signed path and Host header while forwarding to real MinIO.
    let response = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.backend, parts.uri))
        .headers(parts.headers)
        .body(bytes)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.unwrap();
    if is_target_delete && status.is_success() {
        proxy.applied.fetch_add(1, Ordering::Release);
        // MinIO has already applied DELETE. Only its response is withheld.
        if !proxy.before_apply {
            let permit = proxy.acknowledge.acquire().await.unwrap();
            permit.forget();
        }
    }
    (status, headers, body).into_response()
}

struct ProxyTask(JoinHandle<()>);
impl Drop for ProxyTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_applied_remote_delete_with_lost_acknowledgement_is_not_replayed() {
    run_remote_gc_fault(false, false, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_delayed_delete_rejects_hold_after_takeover() {
    run_remote_gc_fault(true, false, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_acknowledged_remote_delete_settles_its_marker() {
    run_remote_gc_fault(false, true, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_existing_role_needs_no_new_fence_grant() {
    let Some(released) =
        resolve_n_minus_one_binary("drill_gc_existing_role_needs_no_new_fence_grant")
    else {
        return;
    };
    run_remote_gc_fault(false, true, Some(&released)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_saved_reply_recovers_after_database_outage() {
    run_remote_gc_fault_with_recovery(true, false, None, SavedReplyScenario::DatabaseOutage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_sweep_recovers_saved_reply_after_database_outage() {
    run_remote_gc_fault_with_recovery(true, false, None, SavedReplyScenario::DatabaseOutageSweep)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_gc_optional_receipt_write_denial_preserves_existing_completion() {
    run_remote_gc_fault_with_recovery(false, true, None, SavedReplyScenario::WriteDenied).await;
}

enum SavedReplyScenario {
    Normal,
    DatabaseOutage,
    DatabaseOutageSweep,
    WriteDenied,
}

async fn run_remote_gc_fault(before_apply: bool, complete: bool, released: Option<&Path>) {
    run_remote_gc_fault_with_recovery(before_apply, complete, released, SavedReplyScenario::Normal)
        .await;
}

async fn run_remote_gc_fault_with_recovery(
    before_apply: bool,
    complete: bool,
    released: Option<&Path>,
    saved_reply_scenario: SavedReplyScenario,
) {
    let recover_after_outage = matches!(
        saved_reply_scenario,
        SavedReplyScenario::DatabaseOutage | SavedReplyScenario::DatabaseOutageSweep
    );
    let recover_by_sweep = matches!(
        saved_reply_scenario,
        SavedReplyScenario::DatabaseOutageSweep
    );
    let deny_saved_replies = matches!(saved_reply_scenario, SavedReplyScenario::WriteDenied);
    let drill = if recover_by_sweep {
        "drill_gc_sweep_recovers_saved_reply_after_database_outage"
    } else if recover_after_outage {
        "drill_gc_saved_reply_recovers_after_database_outage"
    } else if deny_saved_replies {
        "drill_gc_optional_receipt_write_denial_preserves_existing_completion"
    } else if released.is_some() {
        "drill_gc_existing_role_needs_no_new_fence_grant"
    } else if complete {
        "drill_gc_acknowledged_remote_delete_settles_its_marker"
    } else if before_apply {
        "drill_gc_delayed_delete_rejects_hold_after_takeover"
    } else {
        "drill_gc_applied_remote_delete_with_lost_acknowledgement_is_not_replayed"
    };
    let Some(mut stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — current executable unavailable");
        return;
    };
    migrate_chaos_postgres_with_binary(&binary, &stack.pg_url).await;
    let bytes = b"remote GC acknowledgement boundary";
    let hash = shardline_server_core::chunk_hash(bytes);
    let key = shardline_server_core::chunk_object_key(&shardline_index::xet_hash_hex_string(hash))
        .unwrap();
    let proxy = FaultProxy {
        backend: stack.s3_endpoint.clone(),
        delete_path: format!("/{OBJECT_BUCKET}/{}", key.as_str()),
        delete_key: key.as_str().to_owned(),
        applied: Arc::new(AtomicUsize::new(0)),
        arrived: Arc::new(AtomicUsize::new(0)),
        before_apply,
        deny_saved_replies,
        denied_replies: Arc::new(AtomicUsize::new(0)),
        acknowledge: Arc::new(Semaphore::new(0)),
        client: reqwest::Client::new(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    stack.s3_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(forward).with_state(proxy.clone());
    let _proxy_task = ProxyTask(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    let s3 = S3ObjectStoreConfig::new(OBJECT_BUCKET.to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(stack.s3_endpoint.clone()))
        .with_credentials(
            Some(SecretString::from_secret("shardline")),
            Some(SecretString::from_secret("shardline-dev-password")),
            None,
        )
        .with_allow_http(true);
    let objects = ServerObjectStore::s3(s3.clone()).unwrap();
    objects
        .put_if_absent(
            &key,
            ObjectBody::Borrowed(bytes),
            &ObjectIntegrity::new(hash, bytes.len() as u64),
        )
        .unwrap();
    let mut config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/var/lib/shardline"),
        NonZeroUsize::new(CHUNK_SIZE).unwrap(),
    )
    .with_index_postgres_url(stack.pg_url.clone())
    .unwrap()
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3));
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let pool = sqlx::PgPool::connect(&stack.pg_url).await.unwrap();
    let index = PostgresIndexStore::new(pool.clone());
    if let Some(released) = released {
        sqlx::raw_sql(
            "CREATE ROLE legacy_gc_writer LOGIN PASSWORD 'owned_gc_fixture_password';
             GRANT USAGE ON SCHEMA public TO legacy_gc_writer;
             DO $$ DECLARE relation_name text; BEGIN
             FOR relation_name IN SELECT tablename FROM pg_tables
               WHERE schemaname='public' AND tablename LIKE 'shardline_%'
               AND tablename <> 'shardline_resource_fences'
             LOOP EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.%I TO legacy_gc_writer',relation_name);
             END LOOP; END $$;",
        ).execute(&pool).await.unwrap();
        let mut writer_url = url::Url::parse(&stack.pg_url).unwrap();
        writer_url.set_username("legacy_gc_writer").unwrap();
        writer_url
            .set_password(Some("owned_gc_fixture_password"))
            .unwrap();
        stack.pg_url = writer_url.to_string();
        let baseline = docker_run_owned(
            super::readonly_maintenance_deployment::readonly_cli_arguments(
                released,
                &stack,
                &["gc", "--mark", "--retention-seconds", "0"],
                false,
            ),
        )
        .await;
        assert!(
            baseline.status.success(),
            "released GC role must work: {}",
            String::from_utf8_lossy(&baseline.stderr)
        );
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT has_table_privilege('legacy_gc_writer', 'shardline_resource_fences', 'SELECT')",
        ).fetch_one(&pool).await.unwrap());
        let baseline_now = sqlx::query_scalar::<_, i64>(
            "SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::bigint",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let baseline_expired = QuarantineCandidate::new(
            key.clone(),
            bytes.len() as u64,
            u64::try_from(baseline_now - 7200).unwrap(),
            u64::try_from(baseline_now - 3600).unwrap(),
        )
        .unwrap();
        index
            .upsert_quarantine_candidate(&baseline_expired)
            .await
            .unwrap();
        proxy.acknowledge.add_permits(1);
        let baseline_sweep = docker_run_owned(
            super::readonly_maintenance_deployment::readonly_cli_arguments(
                released,
                &stack,
                &["gc", "--sweep", "--retention-seconds", "0"],
                false,
            ),
        )
        .await;
        assert!(
            baseline_sweep.status.success(),
            "released sweep role must work: {}",
            String::from_utf8_lossy(&baseline_sweep.stderr)
        );
        assert!(!objects.contains(&key).unwrap());
        assert!(index.quarantine_candidate(&key).await.unwrap().is_none());
        objects
            .put_if_absent(
                &key,
                ObjectBody::Borrowed(bytes),
                &ObjectIntegrity::new(hash, bytes.len() as u64),
            )
            .unwrap();
        proxy.applied.store(0, Ordering::Release);
        config = config
            .with_index_postgres_url(stack.pg_url.clone())
            .unwrap();
    }
    let deployment = deployment_fingerprint(&config).unwrap();
    let mark = docker_run_owned(
        super::readonly_maintenance_deployment::readonly_cli_arguments(
            &binary,
            &stack,
            &["gc", "--mark", "--retention-seconds", "0"],
            false,
        ),
    )
    .await;
    assert!(
        mark.status.success(),
        "mark: {}",
        String::from_utf8_lossy(&mark.stderr)
    );
    assert!(index.quarantine_candidate(&key).await.unwrap().is_some());
    assert!(objects.contains(&key).unwrap());
    // GC deliberately enforces a minimum retention period even for a zero CLI
    // argument. Seed an already expired, canonically recorded fixture rather
    // than weakening that production guard to reach the remote boundary.
    let now =
        sqlx::query_scalar::<_, i64>("SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::bigint")
            .fetch_one(&pool)
            .await
            .unwrap();
    let expired = QuarantineCandidate::new(
        key.clone(),
        bytes.len() as u64,
        u64::try_from(now - 7200).unwrap(),
        u64::try_from(now - 3600).unwrap(),
    )
    .unwrap();
    index.upsert_quarantine_candidate(&expired).await.unwrap();
    let container = Container(format!("{}-sweep", stack.project));
    let mut arguments = super::readonly_maintenance_deployment::readonly_cli_arguments(
        &binary,
        &stack,
        &["gc", "--sweep", "--retention-seconds", "0"],
        false,
    );
    arguments.splice(
        1..2,
        [
            "--detach".to_owned(),
            "--name".to_owned(),
            container.0.clone(),
        ],
    );
    let started = docker_run_owned(arguments).await;
    assert!(
        started.status.success(),
        "start: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    let deletion = tokio::time::timeout(Duration::from_secs(45), async {
        while if before_apply {
            proxy.arrived.load(Ordering::Acquire) == 0
        } else {
            proxy.applied.load(Ordering::Acquire) == 0
        } {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if deletion.is_err() {
        let logs = docker_run_owned(vec!["logs".to_owned(), container.0.clone()]).await;
        panic!(
            "target DELETE was not applied; source stdout={} stderr={}",
            String::from_utf8_lossy(&logs.stdout),
            String::from_utf8_lossy(&logs.stderr)
        );
    }
    assert!(
        objects.contains(&key).unwrap() == before_apply,
        "the object must match the injected pre/post-mutation boundary"
    );
    assert!(index.quarantine_candidate(&key).await.unwrap().is_none());
    let pending_epoch = sqlx::query_scalar::<_, i64>(
        "SELECT epoch FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource=$1",
    )
    .bind(key.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        pending_epoch > 0,
        "the remote request must have durable admission"
    );
    let tasks = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(tasks.len(), 2);
    let interrupted = tasks
        .iter()
        .find(|task| task.state == TaskRunState::RunningOrInterrupted)
        .unwrap();
    assert_eq!(interrupted.execution, TaskExecutionMode::Inline);
    assert_eq!(
        interrupted.owner.pid, 1,
        "ordinary CLI is the container's original process"
    );
    let live = docker_run_owned(vec![
        "inspect".to_owned(),
        "--format".to_owned(),
        "{{.State.Running}}".to_owned(),
        container.0.clone(),
    ])
    .await;
    assert!(live.status.success());
    assert_eq!(
        String::from_utf8_lossy(&live.stdout).trim(),
        "true",
        "fault is injected while the original CLI still waits for its acknowledgement"
    );
    if complete {
        proxy.acknowledge.add_permits(1);
        let exited = tokio::time::timeout(
            Duration::from_secs(25),
            docker_run_owned(vec!["wait".to_owned(), container.0.clone()]),
        )
        .await
        .unwrap();
        assert!(exited.status.success());
        assert_eq!(String::from_utf8_lossy(&exited.stdout).trim(), "0");
        assert_eq!(proxy.applied.load(Ordering::Acquire), 1);
        assert!(!objects.contains(&key).unwrap());
        if deny_saved_replies {
            assert!(
                proxy.denied_replies.load(Ordering::Acquire) > 0,
                "fixture must actually deny receipt writes before proving existing GC still completes"
            );
        }
        assert!(sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource=$1",
        ).bind(key.as_str()).fetch_optional(&pool).await.unwrap().is_none());
        let after = store.summaries(deployment, None, 10).unwrap();
        assert_eq!(
            after
                .iter()
                .find(|task| task.id == interrupted.id)
                .unwrap()
                .state,
            TaskRunState::Completed
        );
        let hold = shardline_index::RetentionHold::new(
            key.clone(),
            "settled delete permits later protection".to_owned(),
            u64::try_from(now).unwrap(),
            None,
        )
        .unwrap();
        shardline_server::set_retention_hold(&config, &hold)
            .await
            .unwrap();
        assert_eq!(index.retention_hold(&key).await.unwrap(), Some(hold));
        return;
    }
    if before_apply {
        use shardline_index::RetentionHold;
        let coordinator = sqlx::query_scalar::<_, i32>(
            "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1
             AND classid::bigint=(($1::bigint >> 32) & 4294967295)
             AND objid::bigint=($1::bigint & 4294967295)",
        )
        .bind(0x5348_4152_4447_4301_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(coordinator)
                .fetch_one(&pool)
                .await
                .unwrap()
        );
        let now = u64::try_from(
            sqlx::query_scalar::<_, i64>(
                "SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::bigint",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
        )
        .unwrap();
        let hold = RetentionHold::new(
            key.clone(),
            "takeover protection regression".to_owned(),
            now,
            None,
        )
        .unwrap();
        let hold_result = tokio::time::timeout(
            Duration::from_secs(15),
            shardline_server::set_retention_hold(&config, &hold),
        )
        .await
        .unwrap();
        let acknowledged = hold_result.is_ok();
        let rejected_as_pending = hold_result
            .as_ref()
            .is_err_and(|error| error.is_remote_delete_pending());
        let expected_hold = acknowledged.then_some(hold.clone());
        assert_eq!(index.retention_hold(&key).await.unwrap(), expected_hold);
        assert!(
            objects.contains(&key).unwrap(),
            "original bytes must exist when the later hold request returns"
        );
        eprintln!(
            "takeover hold acknowledged={acknowledged} while the former GC owner's remote request is paused"
        );
        // Give a replacement ordinary GC a genuinely expired candidate while
        // the original remote attempt is still paused. It must not replay that
        // unknown delete, even though it can now acquire database ownership.
        index.upsert_quarantine_candidate(&expired).await.unwrap();
        let replacement = tokio::time::timeout(
            Duration::from_secs(25),
            docker_run_owned(
                super::readonly_maintenance_deployment::readonly_cli_arguments(
                    &binary,
                    &stack,
                    &["gc", "--sweep", "--retention-seconds", "0"],
                    false,
                ),
            ),
        )
        .await
        .unwrap();
        assert!(!replacement.status.success());
        assert!(
            String::from_utf8_lossy(&replacement.stderr)
                .contains("unresolved remote delete; retry after reconciliation")
        );
        assert_eq!(proxy.arrived.load(Ordering::Acquire), 1);
        assert_eq!(proxy.applied.load(Ordering::Acquire), 0);
        assert!(objects.contains(&key).unwrap());
        if recover_by_sweep {
            assert!(
                index.quarantine_candidate(&key).await.unwrap().is_none(),
                "the fresh sweep must have no candidate requiring a second user-object DELETE"
            );
        }
        let reply_namespace = index
            .remote_delete_receipt_namespace()
            .await
            .unwrap()
            .unwrap();
        if recover_after_outage {
            let stopped =
                docker_run_owned(vec!["stop".to_owned(), CONTAINER_POSTGRES.to_owned()]).await;
            assert!(stopped.status.success());
        }
        proxy.acknowledge.add_permits(1);
        let exited = tokio::time::timeout(
            Duration::from_secs(25),
            docker_run_owned(vec!["wait".to_owned(), container.0.clone()]),
        )
        .await
        .unwrap();
        assert!(exited.status.success());
        assert_ne!(String::from_utf8_lossy(&exited.stdout).trim(), "0");
        assert_eq!(proxy.applied.load(Ordering::Acquire), 1);
        if recover_after_outage {
            let prefix = shardline_storage::ObjectPrefix::parse(&format!(
                "_shardline_tasks/v1/delete-replies/{reply_namespace}/replies/",
            ))
            .unwrap();
            assert_eq!(
                objects.list_prefix(&prefix).unwrap().len(),
                1,
                "the observed reply must survive failed database settlement"
            );
            let restarted =
                docker_run_owned(vec!["start".to_owned(), CONTAINER_POSTGRES.to_owned()]).await;
            assert!(restarted.status.success());
            assert!(ready_within(chaos_postgres_tcp_ready, Duration::from_secs(30)).await);
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if let Ok(ready_pool) = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(Duration::from_secs(2))
                        .connect(&stack.pg_url)
                        .await
                    {
                        let ready = sqlx::query_scalar::<_, i32>("SELECT 1")
                            .fetch_one(&ready_pool)
                            .await
                            .is_ok();
                        ready_pool.close().await;
                        if ready {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("restarted PostgreSQL must accept metadata queries");
            assert!(
                shardline_server::set_retention_hold(&config, &hold)
                    .await
                    .unwrap_err()
                    .is_remote_delete_pending()
            );
            let recovery_command: &[&str] = if recover_by_sweep {
                &["gc", "--sweep", "--retention-seconds", "0"]
            } else {
                &["repair"]
            };
            let repaired = docker_run_owned(
                super::readonly_maintenance_deployment::readonly_cli_arguments(
                    &binary,
                    &stack,
                    recovery_command,
                    false,
                ),
            )
            .await;
            assert!(
                repaired.status.success(),
                "repair: {}",
                String::from_utf8_lossy(&repaired.stderr)
            );
            assert_eq!(sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource=$1",
            ).bind(key.as_str()).fetch_one(&pool).await.unwrap(), 0);
            assert!(objects.list_prefix(&prefix).unwrap().is_empty());
            shardline_server::set_retention_hold(&config, &hold)
                .await
                .unwrap();
            assert_eq!(index.retention_hold(&key).await.unwrap(), Some(hold));
            assert!(!objects.contains(&key).unwrap());
            assert_eq!(
                proxy.arrived.load(Ordering::Acquire),
                1,
                "repair must not issue a user-object DELETE"
            );
            assert_eq!(proxy.applied.load(Ordering::Acquire), 1);
            assert_eq!(
                store
                    .summaries(deployment, None, 10)
                    .unwrap()
                    .iter()
                    .find(|task| task.id == interrupted.id)
                    .unwrap()
                    .state,
                TaskRunState::Failed,
                "settling a saved reply cannot turn failed GC into successful history"
            );
            return;
        }
        assert_eq!(index.retention_hold(&key).await.unwrap(), expected_hold);
        let after = store.summaries(deployment, None, 10).unwrap();
        assert_eq!(
            after
                .iter()
                .find(|task| task.id == interrupted.id)
                .unwrap()
                .state,
            TaskRunState::Failed
        );
        assert!(
            !acknowledged,
            "a hold racing an unresolved remote delete must return a retryable error, not acknowledge protection"
        );
        assert!(
            rejected_as_pending,
            "the hold must receive the typed retryable error"
        );
        assert_eq!(
            proxy.arrived.load(Ordering::Acquire),
            1,
            "GC must not retry its delete"
        );
        assert_eq!(sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource=$1",
        ).bind(key.as_str()).fetch_one(&pool).await.unwrap(), 0,
            "the observed successful reply settles the exact attempt without restoring GC ownership");
        shardline_server::set_retention_hold(&config, &hold)
            .await
            .unwrap();
        assert_eq!(index.retention_hold(&key).await.unwrap(), Some(hold));
        assert!(!objects.contains(&key).unwrap());
        assert_eq!(proxy.arrived.load(Ordering::Acquire), 1);
        return;
    }
    let before = [0_u32, 1, 2].map(|sequence| {
        store
            .read(
                &format!("runs/{}/records/{sequence:010}", interrupted.id),
                16384,
            )
            .unwrap()
            .unwrap()
    });
    let killed = docker_run_owned(vec![
        "kill".to_owned(),
        "--signal".to_owned(),
        "KILL".to_owned(),
        container.0.clone(),
    ])
    .await;
    assert!(killed.status.success());
    let exited = docker_run_owned(vec!["wait".to_owned(), container.0.clone()]).await;
    assert!(exited.status.success());
    assert_eq!(String::from_utf8_lossy(&exited.stdout).trim(), "137");
    proxy.acknowledge.add_permits(1);
    let diagnostic = docker_run_owned(
        super::readonly_maintenance_deployment::readonly_cli_arguments(
            &binary,
            &stack,
            &["fsck"],
            false,
        ),
    )
    .await;
    assert!(
        diagnostic.status.success(),
        "fsck: {}",
        String::from_utf8_lossy(&diagnostic.stderr)
    );
    let tasks = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.id == interrupted.id)
            .unwrap()
            .state,
        TaskRunState::RunningOrInterrupted
    );
    assert_eq!(
        tasks
            .iter()
            .filter(|task| task.state == TaskRunState::Completed)
            .count(),
        2
    );
    for (sequence, bytes) in before.iter().enumerate() {
        assert_eq!(
            store
                .read(
                    &format!("runs/{}/records/{sequence:010}", interrupted.id),
                    16384
                )
                .unwrap()
                .as_ref(),
            Some(bytes)
        );
    }
    assert!(
        store
            .read(
                &format!("runs/{}/records/0000000003", interrupted.id),
                16384
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        proxy.applied.load(Ordering::Acquire),
        1,
        "diagnostics must not replay DELETE"
    );
    assert!(!objects.contains(&key).unwrap());
    assert!(index.quarantine_candidate(&key).await.unwrap().is_none());
    assert_eq!(sqlx::query_scalar::<_, i64>(
        "SELECT epoch FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource=$1",
    ).bind(key.as_str()).fetch_one(&pool).await.unwrap(), pending_epoch,
        "diagnostics must preserve the unresolved remote attempt");
    eprintln!(
        "PASS: real MinIO delete applied; lost acknowledgement remains unresolved; fresh fsck does not replay it"
    );
}
