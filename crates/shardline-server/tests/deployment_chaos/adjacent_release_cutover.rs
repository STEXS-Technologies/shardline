//! Coordinated Postgres cutover contract for the immediately preceding release.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_deploy_f_adjacent_release_cutover_requires_backup_restore() {
    let drill = "drill_deploy_f_adjacent_release_cutover_requires_backup_restore";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(current_binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — current shardline binary not found");
        return;
    };
    let Some(previous_binary) = resolve_n_minus_one_binary(drill) else {
        return;
    };
    log_deployment_binary_identity("candidate", &current_binary);
    log_deployment_binary_identity("predecessor", &previous_binary);
    assert_eq!(
        chaos_pg_url(),
        stack.pg_url,
        "server processes and migration commands must target the same isolated database"
    );
    let environment = [
        ("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str()),
        ("SHARDLINE_SERVER_FRONTENDS", "s3,lfs"),
        ("SHARDLINE_ADMIN_READ_TOKEN", ADMIN_READ_TOKEN),
    ];
    let token = mint_token("drill", "drill", TokenScope::Write);
    let before = deterministic_bytes(98_323, 611);
    let after = deterministic_bytes(114_711, 612);
    let rollback = deterministic_bytes(74_011, 613);

    eprintln!("chaos({drill}): phase=predecessor-migration");
    migrate_chaos_postgres_with_binary(&previous_binary, &stack.pg_url).await;

    let seed_root = TempDir::new().unwrap();
    let mut predecessor = DeploymentServer::spawn_at(
        &previous_binary,
        &chaos_bind_addr(),
        &environment,
        seed_root.path(),
    );
    eprintln!("chaos({drill}): phase=predecessor-seed-start");
    predecessor.wait_ready(Duration::from_secs(20)).await;
    let response = s3_put(
        &predecessor.base_url(),
        &token,
        "f-adjacent-before",
        before.clone(),
    )
    .await;
    assert_eq!(
        response.status().as_u16(),
        200,
        "predecessor seed: {}",
        response.text().await.unwrap()
    );
    drop(predecessor);

    // The stopped predecessor's database and unchanged MinIO payloads form the
    // rollback point. No server or GC writer is active between this snapshot
    // and restored reads. The per-drill ChaosStack removes the cloned database
    // and payload volume during cleanup, including test unwinding.
    // The identifier is a fixed prefix plus the numeric nextest process ID.
    let backup_database = format!("shardline_adjacent_backup_{}", std::process::id());
    let mut backup_url = reqwest::Url::parse(&stack.pg_url).unwrap();
    backup_url.set_path(&backup_database);
    let backup_url = backup_url.to_string();
    clone_database(&stack.pg_url, &backup_database).await;

    eprintln!("chaos({drill}): phase=candidate-migration");
    migrate_chaos_postgres_with_binary(&current_binary, &stack.pg_url).await;
    let pool = sqlx::PgPool::connect(&stack.pg_url).await.unwrap();
    shardline_server::check_database_schema_compatibility(&pool)
        .await
        .unwrap();
    pool.close().await;

    let upgraded_root = TempDir::new().unwrap();
    let mut upgraded = DeploymentServer::spawn_at(
        &current_binary,
        &chaos_bind_addr(),
        &environment,
        upgraded_root.path(),
    );
    eprintln!("chaos({drill}): phase=candidate-start-after-migration");
    upgraded.wait_ready(Duration::from_secs(20)).await;
    assert_s3_bytes(
        &upgraded.base_url(),
        &token,
        "f-adjacent-before",
        &before,
        "candidate reads predecessor bytes",
    )
    .await;
    drop(upgraded);

    // A direct old-binary restart on N's schema must fail closed. Rollback
    // starts from the pre-upgrade database clone while MinIO still holds its
    // matching payload bytes.
    let rejected_root = TempDir::new().unwrap();
    let mut rejected = DeploymentServer::spawn_at(
        &previous_binary,
        &chaos_bind_addr(),
        &environment,
        rejected_root.path(),
    );
    eprintln!("chaos({drill}): phase=predecessor-rejects-upgraded-database");
    rejected
        .wait_startup_rejected("frontend schema is incompatible", Duration::from_secs(20))
        .await;
    drop(rejected);

    let backup_environment = [
        ("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str()),
        ("SHARDLINE_SERVER_FRONTENDS", "s3,lfs"),
        ("SHARDLINE_ADMIN_READ_TOKEN", ADMIN_READ_TOKEN),
        ("SHARDLINE_INDEX_POSTGRES_URL", backup_url.as_str()),
    ];
    let rollback_root = TempDir::new().unwrap();
    let mut restored = DeploymentServer::spawn_at(
        &previous_binary,
        &chaos_bind_addr(),
        &backup_environment,
        rollback_root.path(),
    );
    eprintln!("chaos({drill}): phase=predecessor-restored-backup");
    restored.wait_ready(Duration::from_secs(20)).await;
    assert_s3_bytes(
        &restored.base_url(),
        &token,
        "f-adjacent-before",
        &before,
        "restored predecessor reader",
    )
    .await;
    let response = s3_put(
        &restored.base_url(),
        &token,
        "f-adjacent-rollback",
        rollback.clone(),
    )
    .await;
    assert_eq!(
        response.status().as_u16(),
        200,
        "restored predecessor writer: {}",
        response.text().await.unwrap()
    );
    assert_s3_bytes(
        &restored.base_url(),
        &token,
        "f-adjacent-rollback",
        &rollback,
        "restored predecessor writer readback",
    )
    .await;
    drop(restored);

    // The pre-upgrade restore boundary has now been verified. Current-only
    // writes begin afterward; later rollback requires forward recovery.
    let mut upgraded = DeploymentServer::spawn_at(
        &current_binary,
        &chaos_bind_addr(),
        &environment,
        upgraded_root.path(),
    );
    eprintln!("chaos({drill}): phase=current-only-writes");
    upgraded.wait_ready(Duration::from_secs(20)).await;
    let response = s3_put(
        &upgraded.base_url(),
        &token,
        "f-adjacent-after",
        after.clone(),
    )
    .await;
    assert_eq!(
        response.status().as_u16(),
        200,
        "candidate write after cutover: {}",
        response.text().await.unwrap()
    );
    drop(upgraded);

    let mut upgraded = DeploymentServer::spawn_at(
        &current_binary,
        &chaos_bind_addr(),
        &environment,
        upgraded_root.path(),
    );
    upgraded.wait_ready(Duration::from_secs(20)).await;
    assert_s3_bytes(
        &upgraded.base_url(),
        &token,
        "f-adjacent-before",
        &before,
        "candidate restart reads predecessor bytes",
    )
    .await;
    assert_s3_bytes(
        &upgraded.base_url(),
        &token,
        "f-adjacent-after",
        &after,
        "candidate restart reads current bytes",
    )
    .await;
    eprintln!(
        "chaos({drill}): PASS — candidate read predecessor bytes; predecessor rejected upgraded schema and round-tripped on matching backup before current-only writes"
    );
}

async fn clone_database(source_url: &str, backup_database: &str) {
    let mut admin_url = reqwest::Url::parse(source_url).unwrap();
    admin_url.set_path("postgres");
    let pool = sqlx::PgPool::connect(admin_url.as_str()).await.unwrap();
    assert!(
        backup_database
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "generated backup database identifier is restricted"
    );
    let statement = format!("CREATE DATABASE \"{backup_database}\" TEMPLATE shardline");
    sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
