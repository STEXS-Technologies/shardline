mod support;

use std::{
    collections::HashSet,
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use reqwest::{Client, StatusCode};
use shardline_protocol::ShardlineHash;
use shardline_server::{ServerConfig, serve_with_listener};
use shardline_vcs::{
    AuthorizationRequest, BuiltInProviderCatalog, GitHubAdapter, GrantedRepositoryAccess,
    ProviderIssuedToken, ProviderKind, ProviderRepositoryPolicy, ProviderSubject,
    ProviderTokenIssuer, RepositoryAccess, RepositoryRef, RepositoryVisibility, RevisionRef,
    configured_metadata,
};
use support::ServerE2eInvariantError;
use tokio::{net::TcpListener, spawn, time::sleep};
use xet_core_structures::{
    merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash},
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
async fn provider_authorization_mints_repository_scoped_tokens_for_server_access() {
    let result = exercise_provider_token_flow().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(result.is_ok(), "provider token flow failed: {error:?}");
}

async fn exercise_provider_token_flow() -> Result<(), Box<dyn Error>> {
    let signing_key = b"provider-signing-key";
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        non_zero_chunk_size()?,
    )
    .with_token_signing_key(signing_key.to_vec())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });

    wait_for_health(&base_url).await?;

    let adapter = github_adapter()?;
    let issuer = ProviderTokenIssuer::new("github-app", signing_key, non_zero_ttl()?)?;

    let write_token_a = issue_token(
        &adapter,
        &issuer,
        "github-user-1",
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets-a")?,
        RepositoryAccess::Write,
    )?;
    let write_token_b = issue_token(
        &adapter,
        &issuer,
        "github-user-1",
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets-b")?,
        RepositoryAccess::Write,
    )?;
    let denied = GrantedRepositoryAccess::authorize(
        &adapter,
        &AuthorizationRequest::new(
            ProviderSubject::new("github-user-2")?,
            RepositoryRef::new(ProviderKind::GitHub, "team", "assets-a")?,
            RevisionRef::new("refs/heads/main")?,
            RepositoryAccess::Write,
        ),
    )?;

    assert_eq!(write_token_a.claims().repository().owner(), "team");
    assert_eq!(write_token_a.claims().repository().name(), "assets-a");
    assert_eq!(
        write_token_a.claims().repository().revision(),
        Some("refs/heads/main")
    );
    assert!(denied.is_none());

    let client = Client::new();
    let (first_xorb, first_xorb_hash) = single_chunk_xorb(b"repo-a");
    let (second_xorb, second_xorb_hash) = single_chunk_xorb(b"repo-b");
    let (first_shard, first_file_hash) = single_file_shard(&[(b"repo-a", &first_xorb_hash)]);
    let (second_shard, second_file_hash) = single_file_shard(&[(b"repo-b", &second_xorb_hash)]);

    let first_xorb_upload = client
        .post(format!("{base_url}/v1/xorbs/default/{first_xorb_hash}"))
        .bearer_auth(write_token_a.token())
        .body(first_xorb)
        .send()
        .await?;
    let second_xorb_upload = client
        .post(format!("{base_url}/v1/xorbs/default/{second_xorb_hash}"))
        .bearer_auth(write_token_b.token())
        .body(second_xorb)
        .send()
        .await?;
    assert_eq!(first_xorb_upload.status(), StatusCode::OK);
    assert_eq!(second_xorb_upload.status(), StatusCode::OK);

    let first_shard_upload = client
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(write_token_a.token())
        .body(first_shard)
        .send()
        .await?;
    let second_shard_upload = client
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(write_token_b.token())
        .body(second_shard)
        .send()
        .await?;
    assert_eq!(first_shard_upload.status(), StatusCode::OK);
    assert_eq!(second_shard_upload.status(), StatusCode::OK);

    let first_reconstruction = client
        .get(format!("{base_url}/v1/reconstructions/{first_file_hash}"))
        .bearer_auth(write_token_a.token())
        .send()
        .await?;
    let second_reconstruction = client
        .get(format!("{base_url}/v1/reconstructions/{second_file_hash}"))
        .bearer_auth(write_token_b.token())
        .send()
        .await?;

    assert_eq!(first_reconstruction.status(), StatusCode::OK);
    assert_eq!(second_reconstruction.status(), StatusCode::OK);

    server.abort();
    Ok(())
}

fn xet_hash_hex(hash: &MerkleHash) -> String {
    let bytes: [u8; 32] = hash.as_bytes().try_into().unwrap_or([0; 32]);
    ShardlineHash::from_bytes(bytes).api_hex_string()
}

fn single_chunk_xorb(bytes: &[u8]) -> (Vec<u8>, String) {
    let chunk_hash = compute_data_hash(bytes);
    let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(bytes.len()).unwrap_or(0))]);
    let serialized = serialized_xorb_object_from_components(
        &xorb_hash,
        bytes.to_vec(),
        vec![(chunk_hash, u32::try_from(bytes.len()).unwrap_or(0))],
        CompressionScheme::None,
    )
    .ok();
    assert!(serialized.is_some());
    let Some(serialized) = serialized else {
        return (Vec::new(), String::new());
    };
    (serialized.serialized_data, xet_hash_hex(&xorb_hash))
}

fn single_file_shard(parts: &[(&[u8], &str)]) -> (Vec<u8>, String) {
    let mut shard = MDBInMemoryShard::default();
    let mut file_segments = Vec::with_capacity(parts.len());
    let mut file_chunks = Vec::with_capacity(parts.len());

    for (bytes, xorb_hash_hex) in parts {
        let xorb_hash = MerkleHash::from_hex(xorb_hash_hex).ok();
        assert!(xorb_hash.is_some());
        let Some(xorb_hash) = xorb_hash else {
            return (Vec::new(), String::new());
        };
        let chunk_hash = compute_data_hash(bytes);
        let add_xorb = shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(
                    xorb_hash,
                    1_u32,
                    u32::try_from(bytes.len()).unwrap_or(0),
                ),
                chunks: vec![XorbChunkSequenceEntry::new(
                    chunk_hash,
                    u32::try_from(bytes.len()).unwrap_or(0),
                    0_u32,
                )],
            })
            .ok();
        assert!(add_xorb.is_some());
        if add_xorb.is_none() {
            return (Vec::new(), String::new());
        }
        file_segments.push(FileDataSequenceEntry::new(
            xorb_hash,
            u32::try_from(bytes.len()).unwrap_or(0),
            0_u32,
            1_u32,
        ));
        file_chunks.push((chunk_hash, u64::try_from(bytes.len()).unwrap_or(0)));
    }

    let file_hash = file_hash(&file_chunks);
    let add_file = shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, file_segments.len(), false, false),
            segments: file_segments,
            verification: Vec::new(),
            metadata_ext: None,
        })
        .ok();
    assert!(add_file.is_some());
    if add_file.is_none() {
        return (Vec::new(), String::new());
    }

    let mut serialized = Vec::new();
    let serialized_result = MDBShardInfo::serialize_from(&mut serialized, &shard, None).ok();
    assert!(serialized_result.is_some());
    if serialized_result.is_none() {
        return (Vec::new(), String::new());
    }
    (serialized, xet_hash_hex(&file_hash))
}

fn github_adapter() -> Result<GitHubAdapter, Box<dyn Error>> {
    let mut catalog = BuiltInProviderCatalog::new("github-app")?;
    let write_subject = ProviderSubject::new("github-user-1")?;
    for repository in [
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets-a")?,
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets-b")?,
    ] {
        let metadata = configured_metadata(
            repository,
            RepositoryVisibility::Private,
            "main",
            "https://github.example/team/assets.git",
        )?;
        catalog.register(ProviderRepositoryPolicy::new(
            metadata,
            HashSet::from([write_subject.clone()]),
            HashSet::from([write_subject.clone()]),
        ))?;
    }

    Ok(GitHubAdapter::new(catalog, None))
}

fn issue_token(
    adapter: &GitHubAdapter,
    issuer: &ProviderTokenIssuer,
    subject: &str,
    repository: RepositoryRef,
    access: RepositoryAccess,
) -> Result<ProviderIssuedToken, Box<dyn Error>> {
    let request = AuthorizationRequest::new(
        ProviderSubject::new(subject)?,
        repository,
        RevisionRef::new("refs/heads/main")?,
        access,
    );
    let grant = GrantedRepositoryAccess::authorize(adapter, &request)?;
    let Some(grant) = grant else {
        return Err(ServerE2eInvariantError::new("provider denied token issuance").into());
    };

    Ok(issuer.issue(&grant)?)
}

fn non_zero_chunk_size() -> Result<NonZeroUsize, ServerE2eInvariantError> {
    NonZeroUsize::new(4).ok_or_else(|| ServerE2eInvariantError::new("chunk size must be non-zero"))
}

fn non_zero_ttl() -> Result<NonZeroU64, ServerE2eInvariantError> {
    NonZeroU64::new(300).ok_or_else(|| ServerE2eInvariantError::new("ttl must be non-zero"))
}

async fn wait_for_health(base_url: &str) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    for _attempt in 0..100 {
        if let Ok(response) = client.get(format!("{base_url}/healthz")).send().await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }

    Err(ServerE2eInvariantError::new("server did not become healthy").into())
}
