//! Real scanner behavior with damaged, rebuildable PostgreSQL member metadata.
use super::*;

mod cancellation;
mod concurrency;

pub(super) async fn qualify(
    root: &Path,
    remote: &RemoteStorage,
    source: &str,
    other_source: &str,
    member: &str,
    content: &[u8],
) -> Result<()> {
    let pool = sqlx::PgPool::connect(&remote.database_url).await?;
    let records = shardline_index::PostgresRecordStore::new(pool.clone());
    // The fixture published this descriptor through real Xorb/shard ingestion,
    // and the preceding complete CLI scan verified its immutable source bytes.
    let descriptor = records
        .xet_indexed_xorb_descriptor(source)
        .await?
        .ok_or("published native descriptor is missing")?;
    let objects = shardline_server_core::ServerObjectStore::s3(remote.config.clone())?;
    let key = shardline_server_core::chunk_object_key(member)?;
    shardline_storage::ObjectStore::delete_if_present(&objects, &key)?;
    expect_scan(root, remote, 0, "missing standalone copy with intact index")?;

    let deleted = sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=$1")
        .bind(source)
        .execute(&pool)
        .await?;
    assert_eq!(deleted.rows_affected(), descriptor.chunks.len() as u64);
    let missing = scan_configured(root, Some(remote))?;
    assert_eq!(missing.status.code(), Some(1), "missing member rows");
    assert!(String::from_utf8_lossy(&missing.stderr).contains("missing_chunk"));
    records
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await?;
    expect_scan(root, remote, 0, "repaired member rows")?;

    sqlx::query("DELETE FROM shardline_xet_xorbs WHERE xorb_hash=$1")
        .bind(source)
        .execute(&pool)
        .await?;
    expect_scan(root, remote, 1, "missing descriptor")?;
    records
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await?;
    expect_scan(root, remote, 0, "repaired descriptor")?;

    // A plausible row count is insufficient: incorrect member identity must
    // leave the missing-copy finding visible until verified repair.
    sqlx::query("UPDATE shardline_xet_chunks SET chunk_hash=$2 WHERE xorb_hash=$1")
        .bind(source)
        .bind("0".repeat(64))
        .execute(&pool)
        .await?;
    expect_scan(root, remote, 1, "wrong member identity")?;
    records
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await?;
    expect_scan(root, remote, 0, "repaired member identity")?;

    let mut forged = descriptor.clone();
    let mut extra = forged
        .chunks
        .last()
        .cloned()
        .ok_or("fixture member missing")?;
    extra.hash = "0".repeat(64);
    extra.packed_start = extra.packed_end;
    extra.packed_end = extra
        .packed_end
        .checked_add(1)
        .ok_or("packed offset overflow")?;
    extra.unpacked_start = extra.unpacked_end;
    extra.unpacked_end = extra
        .unpacked_end
        .checked_add(1)
        .ok_or("decoded offset overflow")?;
    forged.chunks.push(extra.clone());
    assert!(forged.has_valid_structure());
    sqlx::query("UPDATE shardline_xet_xorbs SET descriptor=$2 WHERE xorb_hash=$1")
        .bind(source)
        .bind(serde_json::to_value(&forged)?)
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES($1,$2,$3)",
    )
    .bind(source)
    .bind(i64::try_from(descriptor.chunks.len())?)
    .bind(&extra.hash)
    .execute(&pool)
    .await?;
    expect_scan(root, remote, 1, "forged descriptor root")?;

    // Malformed derived metadata is an operational scan failure, never a clean
    // report or evidence that the acknowledged payload itself was corrupted.
    sqlx::query("UPDATE shardline_xet_xorbs SET descriptor='{}'::jsonb WHERE xorb_hash=$1")
        .bind(source)
        .execute(&pool)
        .await?;
    expect_scan(root, remote, 2, "malformed descriptor")?;
    sqlx::query("UPDATE shardline_xet_xorbs SET descriptor=$2 WHERE xorb_hash=$1")
        .bind(source)
        .bind(serde_json::to_value(&descriptor)?)
        .execute(&pool)
        .await?;
    records
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await?;
    expect_scan(root, remote, 0, "restored verified descriptor")?;
    let other = records
        .xet_indexed_xorb_descriptor(other_source)
        .await?
        .ok_or("second verified descriptor missing")?;
    concurrency::qualify(root, remote, &pool, &descriptor, &other).await?;
    cancellation::qualify(root, remote, &pool, &descriptor, &other).await?;
    overwrite_remote_member(remote, member, content)?;
    expect_scan(root, remote, 0, "restored standalone copy")?;
    pool.close().await;
    Ok(())
}

fn expect_scan(root: &Path, remote: &RemoteStorage, expected: i32, phase: &str) -> Result<()> {
    let output = scan_configured(root, Some(remote))?;
    assert_eq!(
        output.status.code(),
        Some(expected),
        "native index phase: {phase}"
    );
    Ok(())
}
