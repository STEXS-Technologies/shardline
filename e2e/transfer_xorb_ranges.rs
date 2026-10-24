//! Real HTTP publication and transfer of a container shared by two repositories.

use super::*;
use shardline_protocol::RepositoryScope;
use shardline_server::test_fixtures::xet_hash_hex;
use shardline_xet_core::{
    merklehash::{compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_xorb_transfer_does_not_expose_another_repositorys_terms()
-> Result<(), Box<dyn std::error::Error>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let base = format!("http://{address}");
    let config = ServerConfig::new(
        address,
        base.clone(),
        storage.path().to_owned(),
        NonZeroUsize::new(128).ok_or("invalid fixture chunk size")?,
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
    .with_admin_read_token(TEST_ADMIN_TOKEN.as_bytes().to_vec())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    if let Err(error) = wait_for_health(&base).await {
        if server.is_finished() {
            return Err(format!("server startup failed: {:?}", server.await).into());
        }
        server.abort();
        return Err(error);
    }
    let left_write = bearer_token(
        "user",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "left",
        "repo",
        Some("main"),
    )?;
    let left_read = bearer_token(
        "user",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "left",
        "repo",
        Some("main"),
    )?;
    let right_write = bearer_token(
        "user",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "right",
        "repo",
        Some("main"),
    )?;
    let right_read = bearer_token(
        "user",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "right",
        "repo",
        Some("main"),
    )?;
    let chunks = [
        (compute_data_hash(b"aaaa"), 4),
        (compute_data_hash(b"bbbb"), 4),
    ];
    let identity = xorb_hash(&chunks);
    let hash = xet_hash_hex(&identity);
    let serialized = serialized_xorb_object_from_components(
        &identity,
        b"aaaabbbb".to_vec(),
        vec![(chunks[0].0, 4), (chunks[1].0, 8)],
        CompressionScheme::None,
    )?;
    let bytes = serialized.serialized_data;
    // Model restart recovery classifying a pre-persistence attempt as failed.
    // Use the public store API so the failed attempt has valid lifecycle evidence.
    use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
    let intents = shardline_index::LocalIndexStore::new(storage.path().to_owned())?;
    let failed_id = format!("xorb-v2-{hash}-{}", bytes.len());
    let failed = UploadIntent::new(
        failed_id.clone(),
        format!("xorbs/default/{}/{hash}.xorb", &hash[..2]),
        hash.clone(),
        u64::try_from(bytes.len())?,
    );
    intents.create_intent(&failed).await?;
    assert!(
        intents
            .transition_intent(&failed_id, UploadIntentState::Failed)
            .await?
    );
    let client = Client::new();
    let upload = client
        .post(format!("{base}/v1/xorbs/default/{hash}"))
        .bearer_auth(&left_write)
        .body(bytes.clone())
        .send()
        .await?;
    assert!(
        upload.status().is_success(),
        "Xorb upload: {} {}",
        upload.status(),
        upload.text().await?
    );
    assert_eq!(
        intents
            .intent_by_id(&failed_id)
            .await?
            .ok_or("missing failed intent")?
            .state(),
        UploadIntentState::Failed
    );
    assert_eq!(
        intents
            .intent_by_id(&format!("{failed_id}-retry-1"))
            .await?
            .ok_or("missing retry intent")?
            .state(),
        UploadIntentState::Visible
    );
    let mut publications = Vec::new();
    for index in 0..2_usize {
        let mut shard = MDBInMemoryShard::default();
        shard.add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(identity, 2, 8),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunks[0].0, 4, 0),
                XorbChunkSequenceEntry::new(chunks[1].0, 4, 4),
            ],
        })?;
        let file = file_hash(&[chunks[index]]);
        shard.add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file, 1, false, false),
            segments: vec![FileDataSequenceEntry::new(
                identity,
                4,
                u64::try_from(index)?,
                u64::try_from(index)?
                    .checked_add(1)
                    .ok_or("index overflow")?,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })?;
        let mut body = Vec::new();
        MDBShardInfo::serialize_from(&mut body, &shard, None)?;
        publications.push((body, xet_hash_hex(&file)));
    }
    let mut ranges = Vec::new();
    for ((body, file), (write, read)) in publications
        .iter()
        .zip([(&left_write, &left_read), (&right_write, &right_read)])
    {
        let owner = if write == &left_write {
            "left"
        } else {
            "right"
        };
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, owner, "repo", Some("main"))?;
        let repository = format!("scope-json-v1:{}", serde_json::to_string(&scope)?);
        let body_hash = blake3::hash(body).to_hex().to_string();
        let failed_shard_id = format!(
            "shard-v2-{body_hash}-{}",
            blake3::hash(repository.as_bytes()).to_hex()
        );
        let failed_shard = UploadIntent::new(
            failed_shard_id.clone(),
            format!("shards/{}/{body_hash}.shard", &body_hash[..2]),
            body_hash,
            u64::try_from(body.len())?,
        );
        intents
            .create_intent_scoped(&failed_shard, "shardline", &repository)
            .await?;
        assert!(
            intents
                .transition_intent(&failed_shard_id, UploadIntentState::Failed)
                .await?
        );
        let version = if owner == "left" { "v1" } else { "v2" };
        let response = client
            .post(format!("{base}/{version}/shards"))
            .bearer_auth(write)
            .body(body.clone())
            .send()
            .await?;
        assert!(response.status().is_success());
        if version == "v2" {
            assert_eq!(response.headers()["content-type"], "application/x-ndjson");
            assert_eq!(response.headers()["cache-control"], "no-cache");
            let text = response.text().await?;
            let events = text
                .lines()
                .map(serde_json::from_str::<serde_json::Value>)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(events.first().unwrap()["type"], "validating");
            assert_eq!(events.last().unwrap()["type"], "result");
            assert!(events.iter().all(|event| event["type"] != "error"));
            let invalid = client
                .post(format!("{base}/v2/shards"))
                .bearer_auth(write)
                .body("invalid shard")
                .send()
                .await?;
            assert_eq!(invalid.status(), StatusCode::OK);
            let invalid = invalid.text().await?;
            let terminal: serde_json::Value =
                serde_json::from_str(invalid.lines().last().unwrap())?;
            assert_eq!(terminal["type"], "error");
            assert!(terminal.get("result").is_none());
            assert_eq!(
                client
                    .post(format!("{base}/v2/shards"))
                    .body(body.clone())
                    .send()
                    .await?
                    .status(),
                StatusCode::UNAUTHORIZED
            );
            assert_eq!(
                client
                    .post(format!("{base}/v2/shards"))
                    .bearer_auth(read)
                    .body(body.clone())
                    .send()
                    .await?
                    .status(),
                StatusCode::FORBIDDEN
            );
        }
        assert_eq!(
            intents
                .intent_by_id(&failed_shard_id)
                .await?
                .ok_or("missing failed shard")?
                .state(),
            UploadIntentState::Failed
        );
        assert_eq!(
            intents
                .intent_by_id(&format!("{failed_shard_id}-retry-1"))
                .await?
                .ok_or("missing shard retry")?
                .state(),
            UploadIntentState::Visible
        );
        let plan = client
            .get(format!("{base}/v1/reconstructions/{file}"))
            .bearer_auth(read)
            .send()
            .await?
            .error_for_status()?
            .json::<FileReconstructionResponse>()
            .await?;
        let fetch = plan
            .fetch_info
            .get(&hash)
            .and_then(|entries| entries.first())
            .ok_or("missing transfer range")?;
        let range = format!("bytes={}-{}", fetch.url_range.start, fetch.url_range.end);
        if read == &left_read {
            verify_expired_url_refresh(&base, read, file, &hash, &plan).await?;
        }
        let response = client.get(&fetch.url).header(RANGE, &range).send().await?;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let start = usize::try_from(fetch.url_range.start)?;
        let end = usize::try_from(fetch.url_range.end)?
            .checked_add(1)
            .ok_or("range overflow")?;
        assert_eq!(
            response.bytes().await?.as_ref(),
            bytes.get(start..end).ok_or("bad fixture range")?
        );
        ranges.push(range);
    }
    let index = shardline_index::LocalRecordStore::new(storage.path().to_owned())?;
    let descriptor = index
        .xet_xorb_descriptor(&hash)
        .await?
        .ok_or("verified descriptor was not committed")?;
    assert_eq!(descriptor.total_bytes, u64::try_from(bytes.len())?);
    assert_eq!(descriptor.chunks.len(), 2);
    for (member, expected) in descriptor.chunks.iter().zip(chunks) {
        assert_eq!(member.hash, xet_hash_hex(&expected.0));
    }
    // Use the upstream client parser against actual HTTP replies. A repository
    // owning only one term must neither match nor discover the other's chunk,
    // while original container indices remain usable in uploaded file terms.
    for namespace in ["default", "default-merkledb"] {
        for (position, read) in [(0_usize, &left_read), (1, &right_read)] {
            let response = client
                .get(format!(
                    "{base}/v1/chunks/{namespace}/{}",
                    xet_hash_hex(&chunks[position].0)
                ))
                .bearer_auth(read)
                .send()
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get("cache-control")
                    .and_then(|value| value.to_str().ok()),
                Some("private, no-store")
            );
            assert_scoped_dedup_reply(
                &response.bytes().await?,
                chunks[position].0,
                chunks[1 - position].0,
                identity,
                position,
            )?;
            let denied = client
                .get(format!(
                    "{base}/v1/chunks/{namespace}/{}",
                    xet_hash_hex(&chunks[1 - position].0)
                ))
                .bearer_auth(read)
                .send()
                .await?;
            assert_eq!(denied.status(), StatusCode::NOT_FOUND);
        }
    }
    // Simulate an old writer/database: retained shard mappings and file records
    // exist, but the newly introduced acceleration metadata has not been built.
    let connection = rusqlite::Connection::open(storage.path().join("metadata.sqlite3"))?;
    connection.execute("DELETE FROM shardline_xet_chunks", [])?;
    connection.execute("DELETE FROM shardline_xet_xorbs", [])?;
    let recovered = client
        .get(format!(
            "{base}/v1/chunks/default/{}",
            xet_hash_hex(&chunks[0].0)
        ))
        .bearer_auth(&left_read)
        .send()
        .await?;
    assert_eq!(recovered.status(), StatusCode::OK);
    assert_scoped_dedup_reply(
        &recovered.bytes().await?,
        chunks[0].0,
        chunks[1].0,
        identity,
        0,
    )?;
    assert!(
        index.xet_xorb_descriptor(&hash).await?.is_some(),
        "legacy fallback must persist only a verified rebuildable descriptor"
    );
    // More than one page of older candidates must not hide a live authorized
    // source. Half have no source object; half authorize only the other member.
    let left_scope = shardline_protocol::RepositoryScope::new(
        RepositoryProvider::GitHub,
        "left",
        "repo",
        Some("main"),
    )?;
    let mut added = 0_usize;
    for nonce in 0_u64.. {
        let other = compute_data_hash(&nonce.to_le_bytes());
        let candidate_hash = xet_hash_hex(&xorb_hash(&[chunks[0], (other, 4)]));
        if candidate_hash >= hash {
            continue;
        }
        let mut candidate = descriptor.clone();
        candidate.hash = candidate_hash.clone();
        candidate.chunks[1].hash = xet_hash_hex(&other);
        let position = u64::try_from(added % 2)?;
        let record = shardline_index::FileRecord {
            file_id: format!("stale-dedup-candidate-{nonce}"),
            content_hash: xet_hash_hex(&file_hash(&[if position == 0 {
                chunks[0]
            } else {
                (other, 4)
            }])),
            total_bytes: 4,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: Some(left_scope.clone()),
            chunks: vec![shardline_index::FileChunkRecord {
                hash: candidate_hash,
                offset: 0,
                length: 4,
                range_start: position,
                range_end: position + 1,
                packed_start: candidate.chunks[usize::try_from(position)?].packed_start,
                packed_end: candidate.chunks[usize::try_from(position)?].packed_end,
            }],
        };
        index
            .commit_native_shard_metadata_with_descriptors(&[record], &[], &[], &[candidate])
            .await?;
        added += 1;
        if added == 40 {
            break;
        }
    }
    let later_page = client
        .get(format!(
            "{base}/v1/chunks/default/{}",
            xet_hash_hex(&chunks[0].0)
        ))
        .bearer_auth(&left_read)
        .send()
        .await?;
    assert_eq!(later_page.status(), StatusCode::OK);
    assert_scoped_dedup_reply(
        &later_page.bytes().await?,
        chunks[0].0,
        chunks[1].0,
        identity,
        0,
    )?;
    // A guessed member range must not be publishable under a forged file
    // identity to manufacture read authority for somebody else's data.
    let mut forged = MDBInMemoryShard::default();
    forged.add_file_reconstruction_info(MDBFileInfo {
        metadata: FileDataSequenceHeader::new(file_hash(&[chunks[0]]), 1, false, false),
        segments: vec![FileDataSequenceEntry::new(identity, 4, 1, 2)],
        verification: Vec::new(),
        metadata_ext: None,
    })?;
    let mut forged_body = Vec::new();
    MDBShardInfo::serialize_from(&mut forged_body, &forged, None)?;
    let denied = client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(&left_write)
        .body(forged_body)
        .send()
        .await?;
    assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
    let url = format!("{base}/transfer/xorb/default/{hash}");
    for (read, own, other) in [
        (&left_read, &ranges[0], &ranges[1]),
        (&right_read, &ranges[1], &ranges[0]),
    ] {
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(read)
                .header(RANGE, own)
                .send()
                .await?
                .status(),
            StatusCode::PARTIAL_CONTENT
        );
        for denied in [
            other.clone(),
            "bytes=0-".to_owned(),
            format!("bytes=24-{}", bytes.len().saturating_sub(1)),
        ] {
            let response = client
                .get(&url)
                .bearer_auth(read)
                .header(RANGE, denied)
                .send()
                .await?;
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "partial owners must not get other payload or shared footer"
            );
        }
    }
    // A repository owning every term retains full-container and footer reads.
    assert!(
        client
            .post(format!("{base}/v1/shards"))
            .bearer_auth(&left_write)
            .body(publications[1].0.clone())
            .send()
            .await?
            .status()
            .is_success()
    );
    let full = client
        .get(&url)
        .bearer_auth(&left_read)
        .header(RANGE, "bytes=0-")
        .send()
        .await?;
    assert_eq!(full.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(full.bytes().await?.as_ref(), bytes.as_slice());
    // Sharing a container does not authorize every member. Corruption of a
    // hidden member's source must not change the lookup response for a caller
    // authorized only for the other term.
    let source_path = storage
        .path()
        .join("chunks/xorbs/default")
        .join(hash.get(..2).ok_or("invalid fixture Xorb hash")?)
        .join(format!("{hash}.xorb"));
    std::fs::write(&source_path, b"damaged shared Xorb")?;
    for namespace in ["default", "default-merkledb"] {
        let hidden = client
            .get(format!(
                "{base}/v1/chunks/{namespace}/{}",
                xet_hash_hex(&chunks[0].0)
            ))
            .bearer_auth(&right_read)
            .send()
            .await?;
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
    }
    // A retained shard/index is a lookup hint, not evidence that its source
    // still exists. Both official lookup aliases must report a dedup miss
    // after the source disappears, allowing clients to upload their bytes.
    std::fs::remove_file(source_path)?;
    for namespace in ["default", "default-merkledb"] {
        let missing = client
            .get(format!(
                "{base}/v1/chunks/{namespace}/{}",
                xet_hash_hex(&chunks[0].0)
            ))
            .bearer_auth(&left_read)
            .send()
            .await?;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
    server.abort();
    Ok(())
}

/// Uses the upstream CAS client's actual forbidden-response retry loop. Only
/// the URL provider is test-owned; its refresh fetches a new real reconstruction.
async fn verify_expired_url_refresh(
    base: &str,
    read: &str,
    file: &str,
    hash: &str,
    plan: &FileReconstructionResponse,
) -> Result<(), Box<dyn std::error::Error>> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use shardline_protocol::{TokenClaims, TokenSigner};
    use xet_client::cas_client::Client as _;

    let fetch = plan
        .fetch_info
        .get(hash)
        .and_then(|entries| entries.first())
        .ok_or("missing transfer range")?;

    let mut derivation = Hmac::<Sha256>::new_from_slice(b"test-signing-key-32-bytes-long!!")?;
    derivation.update(b"shardline:xorb-range-read-capability:v1");
    let signer = TokenSigner::new(&derivation.finalize().into_bytes())?;
    let mut url = reqwest::Url::parse(&fetch.url)?;
    let token = url
        .query_pairs()
        .find(|(name, _)| name == "xorb_read_capability")
        .ok_or("missing capability")?
        .1
        .into_owned();
    let claims = signer.verify_now(&token)?;
    let expired = TokenClaims::new(
        claims.issuer(),
        claims.subject(),
        claims.scope(),
        claims.repository().clone(),
        1,
    )?;
    let expired = signer.sign(&expired)?;
    url.set_query(None);
    url.query_pairs_mut()
        .append_pair("xorb_read_capability", &expired);
    let range = xet_client::cas_types::HttpRange::new(fetch.url_range.start, fetch.url_range.end);
    let expired_url = url.to_string();
    let response = reqwest::Client::new()
        .get(&expired_url)
        .header(RANGE, format!("bytes={}-{}", range.start, range.end))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = reqwest::Client::new()
        .get(&expired_url)
        .bearer_auth(read)
        .header(RANGE, format!("bytes={}-{}", range.start, range.end))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::PARTIAL_CONTENT,
        "fresh bearer fallback remains supported"
    );
    let refreshed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let provider = RefreshingRange {
        current: tokio::sync::Mutex::new(expired_url),
        range,
        reconstruction: format!("{base}/v1/reconstructions/{file}"),
        bearer: read.to_owned(),
        hash: hash.to_owned(),
        refreshed: refreshed.clone(),
    };
    let remote =
        xet_client::cas_client::RemoteClient::new(base, &None, "expired-range-e2e", false, None);
    let permit = remote.acquire_download_permit().await?;
    let (data, _) = remote
        .get_file_term_data(Box::new(provider), permit, None, Some(4))
        .await?;
    assert_eq!(data.as_ref(), b"aaaa");
    assert_eq!(refreshed.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}

struct RefreshingRange {
    current: tokio::sync::Mutex<String>,
    range: xet_client::cas_types::HttpRange,
    reconstruction: String,
    bearer: String,
    hash: String,
    refreshed: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl xet_client::cas_client::URLProvider for RefreshingRange {
    async fn retrieve_url(
        &self,
    ) -> xet_client::error::Result<(String, Vec<xet_client::cas_types::HttpRange>)> {
        Ok((self.current.lock().await.clone(), vec![self.range]))
    }

    async fn refresh_url(&self) -> xet_client::error::Result<()> {
        let plan = reqwest::Client::new()
            .get(&self.reconstruction)
            .bearer_auth(&self.bearer)
            .send()
            .await
            .map_err(|_error| {
                xet_client::error::ClientError::Other(
                    "reconstruction refresh request failed".to_owned(),
                )
            })?
            .error_for_status()
            .map_err(|_error| {
                xet_client::error::ClientError::Other(
                    "reconstruction refresh was rejected".to_owned(),
                )
            })?
            .json::<FileReconstructionResponse>()
            .await
            .map_err(|_error| {
                xet_client::error::ClientError::Other(
                    "reconstruction refresh returned invalid metadata".to_owned(),
                )
            })?;
        let fetch = plan
            .fetch_info
            .get(&self.hash)
            .and_then(|entries| entries.first())
            .ok_or(xet_client::error::ClientError::InvalidRange)?;
        if fetch.url_range.start != self.range.start || fetch.url_range.end != self.range.end {
            return Err(xet_client::error::ClientError::InvalidRange);
        }
        *self.current.lock().await = fetch.url.clone();
        self.refreshed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

pub(super) fn assert_scoped_dedup_reply(
    bytes: &[u8],
    allowed: shardline_xet_core::MerkleHash,
    hidden: shardline_xet_core::MerkleHash,
    xorb: shardline_xet_core::MerkleHash,
    position: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use xet_core_structures::{
        merklehash::MerkleHash as WireHash, metadata_shard::shard_format::MDBShardInfo as WireShard,
    };
    let wire = |hash| WireHash::from(<[u8; 32]>::from(hash));
    let mut reader = std::io::Cursor::new(bytes);
    let shard = WireShard::load_from_reader(&mut reader)?;
    assert_eq!(shard.num_file_entries(), 0);
    assert!(shard.chunk_hmac_key().is_some());
    assert!(
        shard
            .chunk_hash_dedup_query(&mut reader, &[wire(hidden)])?
            .is_none()
    );
    let (count, term) = shard
        .chunk_hash_dedup_query(&mut reader, &[wire(allowed)])?
        .ok_or("authorized chunk not discoverable by upstream client")?;
    assert_eq!(count, 1);
    assert_eq!(term.xorb_hash, wire(xorb));
    assert_eq!(term.chunk_index_start, u32::try_from(position)?);
    assert_eq!(
        term.chunk_index_end,
        u32::try_from(position.checked_add(1).ok_or("index overflow")?)?
    );
    Ok(())
}
