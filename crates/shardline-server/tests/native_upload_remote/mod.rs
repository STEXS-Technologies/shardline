//! Shared durable backend fixture and exact remote upload crash boundaries.

use super::*;
use shardline_index::{PostgresIndexStore, UploadIntentState, UploadIntentStore};
use shardline_protocol::SecretString;
use shardline_server_core::ServerObjectStore;
use shardline_storage::ObjectStore;

mod encoding;

pub(super) struct RemoteFixture {
    _services: shardline_test_support::DockerLocalStack,
    pub(super) pool: sqlx::PgPool,
    pub(super) environment: Vec<(&'static str, String)>,
    objects: ServerObjectStore,
}

impl RemoteFixture {
    pub(super) async fn start() -> Result<Self> {
        let services = shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()?
            .ok_or("Docker is required")?;
        let url = services
            .postgres_url()
            .ok_or("PostgreSQL fixture missing")?;
        let pool = sqlx::PgPool::connect(&url).await?;
        shardline_server::apply_database_migrations(&pool).await?;
        let prefix = services.unique_s3_key_prefix("native-process");
        let raw = services
            .s3_raw_config(Some(&prefix))
            .ok_or("MinIO fixture missing")?;
        let endpoint = raw.endpoint.ok_or("missing endpoint")?;
        let access = raw.access_key.ok_or("missing access key")?;
        let secret = raw.secret_key.ok_or("missing secret key")?;
        let config =
            shardline_storage::S3ObjectStoreConfig::new(raw.bucket.clone(), raw.region.clone())
                .with_endpoint(Some(endpoint.clone()))
                .with_key_prefix(Some(&prefix))
                .with_credentials(
                    Some(SecretString::new(access.clone())),
                    Some(SecretString::new(secret.clone())),
                    None,
                )
                .with_allow_http(true);
        let objects = ServerObjectStore::s3(config)?;
        // A healthy HTTP endpoint can still reject writes (for example when the
        // disposable object store's disk is full). Surface that backend error
        // before interpreting an upload timeout as a recovery failure.
        let probe = b"native upload fixture write readiness";
        let probe_key = shardline_storage::ObjectKey::parse("fixture/write-readiness")?;
        objects
            .put_if_absent(
                &probe_key,
                shardline_storage::ObjectBody::from_slice(probe),
                &shardline_storage::ObjectIntegrity::new(
                    shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(probe).as_bytes()),
                    u64::try_from(probe.len())?,
                ),
            )
            .map_err(|error| format!("MinIO fixture is not writable: {error:?}"))?;
        Ok(Self {
            _services: services,
            pool,
            objects,
            environment: vec![
                ("NATIVE_PROCESS_POSTGRES_URL", url),
                ("NATIVE_PROCESS_S3_BUCKET", raw.bucket),
                ("NATIVE_PROCESS_S3_REGION", raw.region),
                ("NATIVE_PROCESS_S3_ENDPOINT", endpoint),
                ("NATIVE_PROCESS_S3_PREFIX", prefix),
                ("NATIVE_PROCESS_S3_ACCESS", access),
                ("NATIVE_PROCESS_S3_SECRET", secret),
            ],
        })
    }
}

fn container(byte: u8) -> Result<(String, Vec<u8>)> {
    container_with_compression(byte, CompressionScheme::None)
}

fn container_with_compression(
    byte: u8,
    compression: CompressionScheme,
) -> Result<(String, Vec<u8>)> {
    let member_length = 128 * 1024_u64;
    let data = vec![byte; 32 * 1024 * 1024];
    let member = compute_data_hash(
        data.get(..usize::try_from(member_length)?)
            .ok_or("member")?,
    );
    let members = vec![(member, member_length); 256];
    let identity = xorb_hash(&members);
    let offsets = (1_u64..=256)
        .map(|index| {
            Ok((
                member,
                index
                    .checked_mul(member_length)
                    .ok_or("member offset overflow")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes = serialized_xorb_object_from_components(&identity, data, offsets, compression)?
        .serialized_data;
    Ok((
        shardline_server::test_fixtures::xet_hash_hex(&identity),
        bytes,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "exact remote process crash boundaries require disposable PostgreSQL and MinIO"]
async fn native_xorb_remote_sigkill_at_every_durable_boundary() -> Result {
    remote_boundaries(RetryEncoding::Identical).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "mixed-encoding process crash retries require disposable PostgreSQL and MinIO"]
async fn native_xorb_remote_mixed_encoding_sigkill_at_every_durable_boundary() -> Result {
    remote_boundaries(RetryEncoding::Alternate).await
}

#[derive(Clone, Copy)]
enum RetryEncoding {
    Identical,
    Alternate,
}

async fn remote_boundaries(encoding: RetryEncoding) -> Result {
    use std::{io::Read, os::unix::process::ExitStatusExt};
    let remote = RemoteFixture::start().await?;
    let index = PostgresIndexStore::new(remote.pool.clone());
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "remote", "boundaries", None)?;
    let token = LocalHmacProvider::new(KEY)?.mint_token(&TokenClaims::new(
        "shardline",
        "remote-process",
        TokenScope::Write,
        scope,
        u64::MAX,
    )?)?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    for (cut, byte) in [
        ("created", 1),
        ("storing", 2),
        ("object", 3),
        ("stored", 4),
        ("committed", 5),
        ("visible", 6),
    ] {
        let (hash, mut bytes) = container(byte)?;
        let intent_id = format!("xorb-v2-{hash}-{}", bytes.len());
        let key = shardline_xet_adapter::xorb_object_key(&hash)?;
        let root = tempfile::tempdir()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let base = format!("http://{address}");
        let mut owner = start_with_environment(
            root.path(),
            address,
            Some((cut, &intent_id)),
            &remote.environment,
        )?;
        ready(&http, &base, &mut owner).await?;
        let mut damaged_footer = bytes.clone();
        *damaged_footer.last_mut().ok_or("missing footer")? ^= 0xff;
        let damaged_response = http
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(damaged_footer)
            .send()
            .await?;
        assert_eq!(damaged_response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert!(index.intent_by_id(&intent_id).await?.is_none());
        assert!(remote.objects.metadata(&key)?.is_none());
        let request = http
            .post(format!("{base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(bytes.clone());
        let upload = tokio::spawn(async move { request.send().await });
        tokio::time::timeout(Duration::from_secs(60), async {
            while !root.path().join("reached").exists() {
                assert!(owner.0.try_wait()?.is_none(), "owner exited before {cut}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|error| format!("owner did not reach {cut} for {hash}: {error}"))??;
        let paused = index
            .intent_by_id(&intent_id)
            .await?
            .ok_or("missing paused intent")?;
        let expected = match cut {
            "created" => UploadIntentState::Created,
            "storing" | "object" => UploadIntentState::Storing,
            "stored" => UploadIntentState::Stored,
            "committed" => UploadIntentState::MetadataCommitted,
            "visible" => UploadIntentState::Visible,
            _ => return Err("unknown boundary".into()),
        };
        assert_eq!(paused.state(), expected, "durable state at {cut}");
        assert_eq!(
            remote.objects.metadata(&key)?.is_some(),
            !matches!(cut, "created" | "storing"),
            "object persistence at {cut}"
        );
        owner.0.kill()?;
        assert_eq!(owner.0.wait()?.signal(), Some(9));
        upload.abort();
        let _upload = upload.await;
        drop(owner);

        let takeover_root = tempfile::tempdir()?;
        let takeover_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let takeover_address = takeover_listener.local_addr()?;
        drop(takeover_listener);
        let takeover_base = format!("http://{takeover_address}");
        let mut takeover = start_with_environment(
            takeover_root.path(),
            takeover_address,
            None,
            &remote.environment,
        )?;
        ready(&http, &takeover_base, &mut takeover).await?;
        // Independent replacement replicas must agree on the same durable
        // content identity. Neither may depend on the killed owner's files.
        let peer_root = tempfile::tempdir()?;
        let peer_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let peer_address = peer_listener.local_addr()?;
        drop(peer_listener);
        let peer_base = format!("http://{peer_address}");
        let mut peer =
            start_with_environment(peer_root.path(), peer_address, None, &remote.environment)?;
        ready(&http, &peer_base, &mut peer).await?;
        let (alternate_hash, alternate) = container_with_compression(byte, CompressionScheme::LZ4)?;
        assert_eq!(alternate_hash, hash);
        assert!(alternate.len() < bytes.len());
        let peer_bytes = match encoding {
            RetryEncoding::Identical => &bytes,
            RetryEncoding::Alternate => &alternate,
        };
        let first = http
            .post(format!("{takeover_base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(bytes.clone())
            .send();
        let second = http
            .post(format!("{peer_base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(peer_bytes.clone())
            .send();
        let (first, second) = tokio::join!(first, second);
        let first = first?;
        let second = second?;
        assert!(
            first.status().is_success() || second.status().is_success(),
            "neither replacement made progress at {cut}: {} / {}",
            first.status(),
            second.status()
        );
        for (retry_base, response) in [(&takeover_base, first), (&peer_base, second)] {
            let status = response.status();
            if status.is_success() {
                continue;
            }
            assert_eq!(
                status,
                reqwest::StatusCode::CONFLICT,
                "unexpected competing retry failure at {cut}"
            );
            // An active owner may produce a retryable conflict, but after its
            // acknowledged completion the other replica must also succeed.
            let retried = http
                .post(format!("{retry_base}/v1/xorbs/default/{hash}"))
                .bearer_auth(&token)
                .body(bytes.clone())
                .send()
                .await?;
            assert!(
                retried.status().is_success(),
                "competing retry remained stuck at {cut}: {}",
                retried.status()
            );
        }
        let (mut object, length) = remote
            .objects
            .materialize_bounded_object_if_present(&key, 0)?
            .ok_or("missing durable Xorb")?;
        let mut actual = Vec::new();
        object.read_to_end(&mut actual)?;
        assert_eq!(length, u64::try_from(actual.len())?);
        assert!(
            actual == bytes || actual == *peer_bytes,
            "neither verified encoding survived {cut}"
        );
        assert!(
            index
                .intents_by_state(UploadIntentState::Visible)
                .await?
                .iter()
                .any(|intent| intent.object_key() == key.as_str()),
            "no visible intent after {cut}"
        );
        let original = index
            .intent_by_id(&intent_id)
            .await?
            .ok_or("lost intent history")?;
        assert!(matches!(
            original.state(),
            UploadIntentState::Failed | UploadIntentState::Visible
        ));
        let equivalent = http
            .post(format!("{peer_base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(alternate)
            .send()
            .await?;
        assert!(
            equivalent.status().is_success(),
            "equivalent compressed retry at {cut}: {}",
            equivalent.status()
        );
        let (mut retained, _) = remote
            .objects
            .materialize_bounded_object_if_present(&key, 0)?
            .ok_or("equivalent retry removed the durable Xorb")?;
        let mut after_equivalent = Vec::new();
        retained.read_to_end(&mut after_equivalent)?;
        assert_eq!(
            after_equivalent, actual,
            "equivalent retry replaced the immutable representation at {cut}"
        );
        let first_byte = bytes.get_mut(8).ok_or("missing payload")?;
        *first_byte ^= 1;
        let rejected = http
            .post(format!("{takeover_base}/v1/xorbs/default/{hash}"))
            .bearer_auth(&token)
            .body(bytes)
            .send()
            .await?;
        assert_eq!(
            rejected.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "mismatching retry at {cut}"
        );
        let (mut unchanged, _) = remote
            .objects
            .materialize_bounded_object_if_present(&key, 0)?
            .ok_or("mismatching retry removed the durable Xorb")?;
        let mut after_rejection = Vec::new();
        unchanged.read_to_end(&mut after_rejection)?;
        assert_eq!(
            after_rejection, actual,
            "mismatching retry changed bytes at {cut}"
        );
        drop(takeover);
        drop(peer);
    }
    remote.pool.close().await;
    Ok(())
}
