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
    acknowledge: Arc<Semaphore>,
    client: reqwest::Client,
}

async fn forward(
    State(proxy): State<FaultProxy>,
    request: Request<Body>,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
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
    run_remote_gc_fault(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "merge-blocking: delayed S3 deletion is not yet fenced across database takeover"]
async fn drill_gc_delayed_delete_rejects_hold_after_takeover() {
    run_remote_gc_fault(true).await;
}

async fn run_remote_gc_fault(before_apply: bool) {
    let drill = if before_apply {
        "drill_gc_delayed_delete_preserves_hold_acknowledged_after_takeover"
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
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        PathBuf::from("/var/lib/shardline"),
        NonZeroUsize::new(CHUNK_SIZE).unwrap(),
    )
    .with_index_postgres_url(stack.pg_url.clone())
    .unwrap()
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3));
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let pool = sqlx::PgPool::connect(&stack.pg_url).await.unwrap();
    let index = PostgresIndexStore::new(pool.clone());
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
        let expected_hold = acknowledged.then_some(hold);
        assert_eq!(index.retention_hold(&key).await.unwrap(), expected_hold);
        assert!(
            objects.contains(&key).unwrap(),
            "original bytes must exist when the later hold request returns"
        );
        eprintln!(
            "takeover hold acknowledged={acknowledged} while the former GC owner's remote request is paused"
        );
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
    eprintln!(
        "PASS: real MinIO delete applied; lost acknowledgement remains unresolved; fresh fsck does not replay it"
    );
}
