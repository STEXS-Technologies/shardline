//! Native CAS encoding reused by HTTP S3 publication and the actual predecessor.
use std::borrow::Cow;

use shardline_index::FileRecord;
use shardline_xet_core::xorb_object::{CompressionScheme, RawXorbData, SerializedXorbObject};

use super::*;

fn storage_environment(
    services: &shardline_test_support::DockerLocalStack,
    prefix: &str,
) -> Vec<(&'static str, String)> {
    let raw = services.s3_raw_config(Some(prefix)).unwrap();
    vec![
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", raw.bucket),
        ("SHARDLINE_S3_REGION", raw.region),
        ("SHARDLINE_S3_ENDPOINT", raw.endpoint.unwrap()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", raw.access_key.unwrap()),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", raw.secret_key.unwrap()),
        ("SHARDLINE_S3_KEY_PREFIX", prefix.to_owned()),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ]
}

async fn only_record(database: &str) -> FileRecord {
    let pool = sqlx::PgPool::connect(database).await.unwrap();
    let records: Vec<String> = sqlx::query_scalar(
        "SELECT record::text FROM shardline_file_records WHERE record_kind = 'latest'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    pool.close().await;
    assert_eq!(records.len(), 1);
    serde_json::from_str(&records[0]).unwrap()
}

#[tokio::test]
async fn native_encoding_s3_publication_survives_gc_and_released_rollback() {
    let old = resolve_n_minus_one_binary("native encoding rollback")
        .expect("released predecessor required");
    let current = resolve_shardline_binary().expect("candidate binary required");
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .unwrap()
        .expect("Docker required");
    let database = services.postgres_url().unwrap();
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    sqlx::query("CREATE DATABASE encoding_probe")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let mut probe_url = reqwest::Url::parse(&database).unwrap();
    probe_url.set_path("/encoding_probe");
    let probe_database = probe_url.as_str();
    let probe_root = TempDir::new().unwrap();
    migrate_metadata(&current, probe_root.path(), Some(probe_database)).await;
    let probe_env = storage_environment(&services, "encoding-probe");
    let token = mint_token("drill", "drill", TokenScope::Write);
    let mut random = 948_317_u64;
    let bytes: Vec<u8> = (0..512 * 1024)
        .map(|index| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            if index % 4 == 0 { random as u8 } else { 0 }
        })
        .collect();
    let mut probe = LocalServer::spawn_with_storage(
        &current,
        probe_root.path(),
        Some(probe_database),
        "s3",
        &probe_env,
    );
    probe.ready().await;
    assert!(
        s3_put(&probe.base, &token, "probe", bytes.clone())
            .await
            .status()
            .is_success()
    );
    let expected = only_record(probe_database).await;
    drop(probe);
    assert!(expected.chunks.len() > 1);
    let hash = &expected.chunks[0].hash;
    assert!(expected.chunks.iter().all(|chunk| &chunk.hash == hash));
    let chunks: Vec<_> = expected
        .chunks
        .iter()
        .map(|chunk| {
            let start = usize::try_from(chunk.offset).unwrap();
            let length = usize::try_from(chunk.length).unwrap();
            Cow::Owned(bytes[start..start + length].to_vec())
        })
        .collect();
    let raw = RawXorbData::from_owned_data(chunks, Vec::new());
    let plain =
        SerializedXorbObject::from_xorb_ref_with_compression(&raw, CompressionScheme::None, true)
            .unwrap();
    assert_eq!(&plain.hash.hex(), hash);
    let root = TempDir::new().unwrap();
    let storage = storage_environment(&services, "encoding-target");
    migrate_metadata(&current, root.path(), Some(&database)).await;
    let mut server =
        LocalServer::spawn_with_storage(&current, root.path(), Some(&database), "s3,xet", &storage);
    server.ready().await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/xorbs/default/{hash}", server.base))
        .bearer_auth(&token)
        .body(plain.serialized_data)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "native encoding upload failed"
    );
    assert!(
        s3_put(&server.base, &token, "reused", bytes.clone())
            .await
            .status()
            .is_success()
    );
    let actual = only_record(&database).await;
    assert_eq!(actual.chunks.len(), expected.chunks.len());
    assert!(actual.chunks.iter().all(|chunk| &chunk.hash == hash));
    assert!(
        actual
            .chunks
            .iter()
            .zip(&expected.chunks)
            .skip(1)
            .any(|(new, probe)| new.packed_start != probe.packed_start)
    );
    let boundaries: Vec<_> = actual
        .chunks
        .iter()
        .skip(1)
        .map(|chunk| usize::try_from(chunk.offset).unwrap())
        .collect();
    verify(&server, &token, "reused", &bytes, &boundaries).await;
    maintenance::gc_before_rollback(&current, root.path(), Some(&database), &storage, "s3,xet")
        .await;
    verify(&server, &token, "reused", &bytes, &boundaries).await;
    drop(server);
    for binary in [&old, &current] {
        let mut reader = LocalServer::spawn_with_storage(
            binary,
            root.path(),
            Some(&database),
            "s3,xet",
            &storage,
        );
        reader.ready().await;
        verify(&reader, &token, "reused", &bytes, &boundaries).await;
    }
}
