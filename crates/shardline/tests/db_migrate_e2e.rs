mod support;

use std::{
    env::var,
    error::Error,
    path::PathBuf,
    process::{Command, id as process_id},
};

use sqlx::{PgPool, postgres::PgPoolOptions, query};
use support::CliE2eInvariantError;
use url::Url;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn db_migrate_applies_reports_and_reverts_live_postgres_schema() {
    let result = exercise_db_migrate_applies_reports_and_reverts_live_postgres_schema().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "db migrate e2e failed: {error:?}");
}

async fn exercise_db_migrate_applies_reports_and_reverts_live_postgres_schema()
-> Result<(), Box<dyn Error>> {
    let Some(base_url) = var("DATABASE_URL").ok() else {
        return Ok(());
    };

    let database_name = format!("shardline_db_migrate_{}", process_id());
    let admin_url = database_url_for(&base_url, "postgres")?;
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await?;
    recreate_database(&admin_pool, &database_name).await?;

    let database_url = database_url_for(&base_url, &database_name)?;
    let shardline_bin = shardline_binary()?;

    let up = Command::new(&shardline_bin)
        .args([
            "db",
            "migrate",
            "up",
            "--database-url",
            &database_url,
            "--steps",
            "2",
        ])
        .output()?;
    if !up.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "db migrate up failed: {}",
            String::from_utf8_lossy(&up.stderr)
        ))
        .into());
    }
    let up_stdout = String::from_utf8(up.stdout)?;
    if !up_stdout.contains("applied_count: 2") {
        return Err(
            CliE2eInvariantError::new("db migrate up did not report applied migrations").into(),
        );
    }

    let status = Command::new(&shardline_bin)
        .args(["db", "migrate", "status", "--database-url", &database_url])
        .output()?;
    if !status.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "db migrate status failed: {}",
            String::from_utf8_lossy(&status.stderr)
        ))
        .into());
    }
    let status_stdout = String::from_utf8(status.stdout)?;
    if !status_stdout.contains("migration: version=20260417000000 name=metadata_store applied=true")
    {
        return Err(CliE2eInvariantError::new("db migrate status missed first migration").into());
    }
    if !status_stdout
        .contains("migration: version=20260418020000 name=provider_repository_states applied=false")
    {
        return Err(CliE2eInvariantError::new("db migrate status missed pending migration").into());
    }

    let down = Command::new(shardline_bin)
        .args([
            "db",
            "migrate",
            "down",
            "--database-url",
            &database_url,
            "--steps",
            "1",
        ])
        .output()?;
    if !down.status.success() {
        return Err(CliE2eInvariantError::new(format!(
            "db migrate down failed: {}",
            String::from_utf8_lossy(&down.stderr)
        ))
        .into());
    }
    let down_stdout = String::from_utf8(down.stdout)?;
    if !down_stdout.contains("reverted_count: 1") {
        return Err(
            CliE2eInvariantError::new("db migrate down did not report reverted migration").into(),
        );
    }

    Ok(())
}

async fn recreate_database(pool: &PgPool, database_name: &str) -> Result<(), Box<dyn Error>> {
    query(&format!("DROP DATABASE IF EXISTS {database_name}"))
        .execute(pool)
        .await?;
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(pool)
        .await?;
    Ok(())
}

fn database_url_for(base_url: &str, database_name: &str) -> Result<String, Box<dyn Error>> {
    let mut url = Url::parse(base_url)?;
    url.set_path(database_name);
    Ok(url.to_string())
}

fn shardline_binary() -> Result<PathBuf, Box<dyn Error>> {
    if let Ok(path) = var("CARGO_BIN_EXE_shardline") {
        return Ok(PathBuf::from(path));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace_root) = manifest_dir.ancestors().nth(2) else {
        return Err(CliE2eInvariantError::new("workspace root could not be resolved").into());
    };
    let binary = workspace_root
        .join("target")
        .join("debug")
        .join("shardline");
    if !binary.is_file() {
        return Err(CliE2eInvariantError::new(format!(
            "shardline binary was not built at {}",
            binary.display()
        ))
        .into());
    }

    Ok(binary)
}
