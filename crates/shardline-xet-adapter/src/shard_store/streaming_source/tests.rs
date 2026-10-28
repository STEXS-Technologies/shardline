//! Run the success case with an unavailable TMPDIR to prove no scratch is used.
use super::*;
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, SecretString};
use shardline_storage::S3ObjectStoreConfig;
use shardline_xet_core::{
    merklehash::{file_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};
use std::sync::{Arc, Mutex};

async fn exercise(change_second_read: bool, ignore_range: bool) -> Result<(), XetAdapterError> {
    let members: Vec<Vec<u8>> = (1..=4).map(|index| vec![index; 64 * 1024]).collect();
    let leaves: Vec<_> = members
        .iter()
        .map(|data| (compute_data_hash(data), data.len() as u64))
        .collect();
    let hash = xorb_hash(&leaves);
    let mut end = 0_u64;
    let boundaries = leaves
        .iter()
        .map(|(hash, length)| {
            end = end.checked_add(*length).unwrap();
            (*hash, end)
        })
        .collect();
    let body = axum::body::Bytes::from(
        serialized_xorb_object_from_components(
            &hash,
            members.concat(),
            boundaries,
            CompressionScheme::None,
        )
        .unwrap()
        .serialized_data,
    );
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
        let body = body.clone();
        let observed = observed.clone();
        async move {
            let range = headers
                .get("range")
                .map(|value| value.to_str().unwrap().to_owned());
            let ordinal = {
                let mut requests = observed.lock().unwrap();
                requests.push(range.clone());
                requests.len()
            };
            if let Some(range) = range {
                if ignore_range {
                    return axum::http::Response::new(axum::body::Body::from(body));
                }
                let (start, end) = range
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let start: usize = start.parse().unwrap();
                let end: usize = end.parse().unwrap();
                let mut selected = body[start..=end].to_vec();
                if change_second_read && ordinal == 2 {
                    selected[12] ^= 0x40;
                }
                axum::http::Response::builder()
                    .status(206)
                    .header(
                        "content-range",
                        format!("bytes {start}-{end}/{}", body.len()),
                    )
                    .body(axum::body::Body::from(selected))
                    .unwrap()
            } else {
                axum::http::Response::new(axum::body::Body::from(body))
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let objects = ServerObjectStore::s3(
        S3ObjectStoreConfig::new("fixture".into(), "us-east-1".into())
            .with_endpoint(Some(endpoint))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::new("fixture".into())),
                Some(SecretString::new("fixture-secret".into())),
                None,
            ),
    )
    .unwrap();
    let result = tokio::task::spawn_blocking(move || {
        let selected = [leaves[0], leaves[2], leaves[3], leaves[0], leaves[1]];
        let content = [
            &members[0][..],
            &members[2],
            &members[3],
            &members[0],
            &members[1],
        ]
        .concat();
        let expected = format!("{:x}", Sha256::digest(&content));
        let file = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash(&selected), 3, false, true),
            segments: vec![
                FileDataSequenceEntry::new(hash, 65536, 0, 1),
                FileDataSequenceEntry::new(hash, 131072, 2, 4),
                FileDataSequenceEntry::new(hash, 131072, 0, 2),
            ],
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&expected).unwrap(),
            )),
        };
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "streaming", None).unwrap();
        let mut verifier = file_digests::FileDigestVerifier::default();
        let (records, identities) = build_file_records_with_cache(
            vec![file],
            &objects,
            Some(&scope),
            None,
            &mut HashMap::new(),
            &mut verifier,
            &|| Ok(()),
        )?;
        assert_eq!(identities[0].sha256, expected);
        assert_eq!(records[0].total_bytes, content.len() as u64);
        assert_eq!(verifier.source_reads, 2);
        Ok(())
    })
    .await
    .unwrap();
    server.abort();
    if !change_second_read && !ignore_range {
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            2,
            "one full GET plus one exact term GET, no HEAD or member-by-member GETs"
        );
        assert!(requests[0].is_none());
        assert!(requests[1].is_some());
    }
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_source_and_repeated_terms_use_only_streams() {
    exercise(false, false).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_changes_before_revisited_terms_are_rejected() {
    assert!(exercise(true, false).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_backend_range_cannot_feed_the_file_digest() {
    assert!(exercise(false, true).await.is_err());
}
