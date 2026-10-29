//! Actual predecessor native-CAS publication, upgrade and rollback reads.

mod mixed_writers;
mod rebuild;
mod repeated_members;

use super::*;
use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash, file_hash},
    metadata_shard::{
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, MDBFileInfo,
        },
        shard_in_memory::MDBInMemoryShard,
    },
};

struct NativeFile {
    file_id: String,
    members: Vec<(String, Vec<u8>)>,
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap()
}

async fn upload(base: &str, token: &str, seed: u8) -> NativeFile {
    upload_parts(
        base,
        token,
        vec![
            vec![seed; 131_079],
            vec![seed.checked_add(1).unwrap(); 131_079],
        ],
        false,
    )
    .await
}

async fn upload_parts(base: &str, token: &str, parts: Vec<Vec<u8>>, identity: bool) -> NativeFile {
    let client = http_client();
    let mut members = Vec::new();
    for data in parts {
        let (encoded, hash) = shardline_server::test_fixtures::single_chunk_xorb(&data);
        let response = client
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .bearer_auth(token)
            .body(encoded)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let diagnostic = response.text().await.unwrap();
        assert!(
            status.is_success(),
            "native Xorb upload: {status}: {diagnostic}"
        );
        members.push((hash, data));
    }
    let leaves: Vec<_> = members
        .iter()
        .map(|(_, data)| (compute_data_hash(data), data.len() as u64))
        .collect();
    let file_hash = file_hash(&leaves);
    let file_id = file_hash.hex();
    let digest = Sha256::digest(
        members
            .iter()
            .flat_map(|(_, data)| data.iter())
            .copied()
            .collect::<Vec<_>>(),
    );
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, members.len() as u64, false, identity),
            segments: members
                .iter()
                .map(|(hash, data)| {
                    FileDataSequenceEntry::new(
                        MerkleHash::from_hex(hash).unwrap(),
                        data.len() as u64,
                        0,
                        1,
                    )
                })
                .collect(),
            verification: Vec::new(),
            metadata_ext: identity.then(|| {
                FileMetadataExt::new(MerkleHash::from_hex(&format!("{digest:x}")).unwrap())
            }),
        })
        .unwrap();
    let response = client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(token)
        .body(shard.to_bytes().unwrap())
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "native shard upload: {}",
        response.status()
    );
    NativeFile { file_id, members }
}

async fn verify_identities(
    root: &Path,
    postgres: Option<&str>,
    canonical: &NativeFile,
    historical: &NativeFile,
) {
    let scope =
        RepositoryScope::new(RepositoryProvider::Generic, "drill", "drill", Some("main")).unwrap();
    let bytes: Vec<_> = canonical
        .members
        .iter()
        .flat_map(|(_, data)| data.iter())
        .copied()
        .collect();
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let hidden =
        RepositoryScope::new(RepositoryProvider::Generic, "other", "drill", Some("main")).unwrap();
    let files = [canonical, historical];
    let (identity, aliases, hidden_record) = if let Some(url) = postgres {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        let store = shardline_index::PostgresRecordStore::new(pool.clone());
        let identity = store
            .xet_file_identity(&scope, &sha256)
            .await
            .unwrap()
            .unwrap();
        let mut aliases = Vec::new();
        for file in files {
            aliases.push(
                store
                    .repository_xet_file(&scope, &file.file_id)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let hidden_record = store
            .repository_xet_file(&hidden, &historical.file_id)
            .await
            .unwrap();
        pool.close().await;
        (identity, aliases, hidden_record)
    } else {
        let store = LocalRecordStore::new(root.to_path_buf()).unwrap();
        let identity = store.xet_file_identity(&scope, &sha256).unwrap().unwrap();
        let aliases = files
            .iter()
            .map(|file| {
                store
                    .repository_xet_file(&scope, &file.file_id)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        let hidden_record = store
            .repository_xet_file(&hidden, &historical.file_id)
            .unwrap();
        (identity, aliases, hidden_record)
    };
    assert_eq!(identity.file_id, canonical.file_id);
    assert_eq!(identity.total_bytes, bytes.len() as u64);
    for (file, (stored_sha, record)) in files.into_iter().zip(aliases) {
        assert_eq!(stored_sha, sha256);
        assert_eq!(record.file_id, file.file_id);
        assert_eq!(record.total_bytes, bytes.len() as u64);
    }
    assert!(hidden_record.is_none());
}

async fn verify_identity_download(base: &str, token: &str, file: &NativeFile) {
    let expected: Vec<_> = file
        .members
        .iter()
        .flat_map(|(_, data)| data.iter())
        .copied()
        .collect();
    let sha256 = format!("{:x}", Sha256::digest(&expected));
    let url = format!("{base}/v1/hub/xet-files/{sha256}");
    let client = http_client();
    let actual = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(actual.as_ref(), expected.as_slice());
    let hidden = client
        .get(&url)
        .bearer_auth(mint_token("other", "drill", TokenScope::Write))
        .send()
        .await
        .unwrap();
    assert_eq!(hidden.status(), reqwest::StatusCode::NOT_FOUND);
}

async fn verify(base: &str, token: &str, file: &NativeFile) {
    let client = http_client();
    let response = client
        .get(format!("{base}/v1/reconstructions/{}", file.file_id))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let reconstruction: serde_json::Value = response.json().await.unwrap();
    let terms = reconstruction["terms"].as_array().unwrap();
    assert_eq!(terms.len(), file.members.len());
    let mut actual_file = Vec::new();
    let mut expected_file = Vec::new();
    for (term, (hash, expected)) in terms.iter().zip(&file.members) {
        assert_eq!(term["hash"].as_str(), Some(hash.as_str()));
        assert_eq!(
            term["unpacked_length"].as_u64(),
            Some(expected.len() as u64)
        );
        let encoded = client
            .get(format!("{base}/transfer/xorb/default/{hash}"))
            .bearer_auth(token)
            .header(reqwest::header::RANGE, "bytes=0-")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let (_, decoded) = shardline_xet_adapter::validate_serialized_xorb_with_data(
            &mut std::io::Cursor::new(encoded),
            shardline_index::parse_xet_hash_hex(hash).unwrap(),
            expected.len(),
            Vec::new(),
        )
        .unwrap();
        let decoded = decoded.unwrap();
        assert_eq!(&decoded, expected);
        actual_file.extend_from_slice(&decoded);
        expected_file.extend_from_slice(expected);
    }
    assert_eq!(Sha256::digest(&actual_file), Sha256::digest(&expected_file));
}

#[tokio::test]
async fn released_native_xet_upgrade_and_rollback_preserve_old_and_new_content() {
    upgrade(None, &[]).await;
}

#[cfg(feature = "docker")]
#[tokio::test]
async fn released_remote_native_xet_upgrade_and_rollback_preserve_identities() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio_tmpfs()
        .start()
        .expect("remote native migration services")
        .expect("remote native migration requires Docker");
    let postgres = services.postgres_url().expect("PostgreSQL fixture");
    let prefix = services.unique_s3_key_prefix("native-predecessor");
    let s3 = services
        .s3_raw_config(Some(&prefix))
        .expect("MinIO fixture");
    let storage_env = vec![
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", s3.bucket),
        ("SHARDLINE_S3_REGION", s3.region),
        ("SHARDLINE_S3_ENDPOINT", s3.endpoint.unwrap()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", s3.access_key.unwrap()),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", s3.secret_key.unwrap()),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    upgrade(Some(&postgres), &storage_env).await;
}

async fn upgrade(postgres: Option<&str>, storage_env: &[(&str, String)]) {
    let Some(old) = resolve_n_minus_one_binary("native Xet upgrade and rollback") else {
        return;
    };
    let current = resolve_shardline_binary().expect("current binary required");
    let root = TempDir::new().unwrap();
    let token = mint_token("drill", "drill", TokenScope::Write);
    migrate_metadata(&old, root.path(), postgres).await;
    let mut predecessor =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "xet", storage_env);
    predecessor.ready().await;
    let original = upload(&predecessor.base, &token, 17).await;
    verify(&predecessor.base, &token, &original).await;
    drop(predecessor);
    migrate_metadata(&current, root.path(), postgres).await;
    let mut candidate =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "xet", storage_env);
    candidate.ready().await;
    verify(&candidate.base, &token, &original).await;
    let mixed = mixed_writers::publish(
        &old,
        root.path(),
        postgres,
        storage_env,
        &candidate.base,
        &token,
        &original,
    )
    .await;
    let added = upload(&candidate.base, &token, 29).await;
    verify(&candidate.base, &token, &added).await;
    let repeated = repeated_members::publish(&candidate.base, &token).await;
    verify(&candidate.base, &token, &repeated).await;
    // The predecessor's original identity did not include an extended SHA-256.
    // Publish that binding, then the same bytes with different chunk boundaries.
    let original_bytes: Vec<_> = original
        .members
        .iter()
        .map(|(_, data)| data.clone())
        .collect();
    let bound = upload_parts(&candidate.base, &token, original_bytes.clone(), true).await;
    assert_eq!(bound.file_id, original.file_id);
    let repacked = upload_parts(&candidate.base, &token, vec![original_bytes.concat()], true).await;
    assert_ne!(repacked.file_id, original.file_id);
    verify(&candidate.base, &token, &repacked).await;
    verify_identities(root.path(), postgres, &repacked, &original).await;
    verify_identity_download(&candidate.base, &token, &repacked).await;
    drop(candidate);
    maintenance::gc_before_rollback(&current, root.path(), postgres, storage_env, "xet").await;
    let mut rollback =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "xet", storage_env);
    rollback.ready().await;
    verify(&rollback.base, &token, &original).await;
    verify(&rollback.base, &token, &added).await;
    verify(&rollback.base, &token, &repeated).await;
    verify(&rollback.base, &token, &repacked).await;
    for file in [&mixed.0, &mixed.1] {
        verify(&rollback.base, &token, file).await;
    }
    // Old writers do not populate the new SHA-256/alias indexes, even when the
    // shard carries extended SHA-256 metadata. They must preserve existing
    // bindings while publishing additional readable native file identities.
    let replayed = upload(&rollback.base, &token, 17).await;
    assert_eq!(replayed.file_id, original.file_id);
    // Use a third, previously unseen native identity so this is a real old
    // metadata write, rather than an idempotent replay of a completed intent.
    let bytes = original_bytes.concat();
    let rollback_parts: Vec<_> = bytes.chunks(87_386).map(<[u8]>::to_vec).collect();
    let replaced = upload_parts(&rollback.base, &token, rollback_parts.clone(), true).await;
    assert_ne!(replaced.file_id, original.file_id);
    assert_ne!(replaced.file_id, repacked.file_id);
    verify(&rollback.base, &token, &replaced).await;
    verify(&rollback.base, &token, &repacked).await;
    verify_identities(root.path(), postgres, &repacked, &original).await;
    drop(rollback);
    let mut restart =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "xet", storage_env);
    restart.ready().await;
    verify(&restart.base, &token, &original).await;
    verify(&restart.base, &token, &added).await;
    verify(&restart.base, &token, &repeated).await;
    verify(&restart.base, &token, &repacked).await;
    verify(&restart.base, &token, &replaced).await;
    verify_identities(root.path(), postgres, &repacked, &original).await;
    // An identical retry through the new binary must verify and backfill the
    // legacy publication instead of bypassing work for its visible old intent.
    let rebound = upload_parts(&restart.base, &token, rollback_parts, true).await;
    assert_eq!(rebound.file_id, replaced.file_id);
    verify_identities(root.path(), postgres, &rebound, &original).await;
    verify_identities(root.path(), postgres, &rebound, &repacked).await;
    verify_identity_download(&restart.base, &token, &rebound).await;
    verify_identity_download(&restart.base, &token, &repacked).await;
    drop(restart);
    let published_descriptors = rebuild::descriptors(root.path(), postgres, false).await;
    assert!(published_descriptors > 0);
    assert_eq!(rebuild::descriptors(root.path(), postgres, true).await, 0);
    maintenance::recover_missing_native_index(&current, root.path(), postgres, storage_env).await;
    assert!(rebuild::descriptors(root.path(), postgres, false).await >= published_descriptors);
    let mut recovered =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "xet", storage_env);
    recovered.ready().await;
    for file in [
        &original, &added, &repacked, &replaced, &mixed.0, &mixed.1, &repeated,
    ] {
        verify(&recovered.base, &token, file).await;
    }
    verify_identities(root.path(), postgres, &rebound, &original).await;
    verify_identities(root.path(), postgres, &rebound, &repacked).await;
    for historical in [&mixed.0, &mixed.1] {
        verify_identities(root.path(), postgres, &rebound, historical).await;
    }
    verify_identity_download(&recovered.base, &token, &rebound).await;
    drop(recovered);
    // Repair and the following sweep must preserve historical aliases and the
    // Xorbs consumed by the unchanged predecessor's reconstruction path.
    maintenance::gc_before_rollback(&current, root.path(), postgres, storage_env, "xet").await;
    let mut rollback =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "xet", storage_env);
    rollback.ready().await;
    for file in [
        &original, &added, &repacked, &replaced, &mixed.0, &mixed.1, &repeated,
    ] {
        verify(&rollback.base, &token, file).await;
    }
    drop(rollback);
    let mut retained =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "xet", storage_env);
    retained.ready().await;
    verify_identities(root.path(), postgres, &rebound, &original).await;
    verify_identities(root.path(), postgres, &rebound, &repacked).await;
    for historical in [&mixed.0, &mixed.1] {
        verify_identities(root.path(), postgres, &rebound, historical).await;
    }
    verify_identity_download(&retained.base, &token, &rebound).await;
}
