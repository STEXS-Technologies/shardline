#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use super::*;
use axum::http::HeaderValue;

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";

// Independent AWS sigv4-streaming-trailers example. The published page's
// Authorization summary differs from its canonical request; use the latter's
// signed header list with the published seed and chunk/trailer signatures.
fn official_trailer_stream() -> (VerifiedSigV4, Vec<u8>) {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("host", "s3.amazonaws.com"),
        ("content-encoding", "aws-chunked"),
        (
            "x-amz-content-sha256",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        ),
        ("x-amz-date", "20130524T000000Z"),
        ("x-amz-decoded-content-length", "66560"),
        ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ("x-amz-trailer", "x-amz-checksum-crc32c"),
        (
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer,Signature=106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e",
        ),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    let verified = verify_sigv4(
        &Method::PUT,
        &"/examplebucket/chunkObject.txt".parse().unwrap(),
        &headers,
        &credential(),
        "us-east-1",
        1369353600,
    )
    .unwrap();
    let mut wire = Vec::new();
    for (size, signature) in [
        (
            65536usize,
            "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2",
        ),
        (
            1024,
            "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7",
        ),
        (
            0,
            "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992",
        ),
    ] {
        wire.extend_from_slice(format!("{size:x};chunk-signature={signature}\r\n").as_bytes());
        wire.extend(std::iter::repeat_n(b'a', size));
        if size != 0 {
            wire.extend_from_slice(b"\r\n");
        }
    }
    wire.extend_from_slice(b"x-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n\r\n");
    (verified, wire)
}

#[test]
fn official_signed_trailer_chain_and_checksum_accept_fragmentation() {
    let (verified, wire) = official_trailer_stream();
    for frame in [1, 7, 8192, wire.len()] {
        let mut verifier = verified.streaming_verifier().unwrap();
        for chunk in wire.chunks(frame) {
            verifier.update(chunk).unwrap();
        }
        verifier.finish().unwrap();
    }
}

#[test]
fn unsigned_trailer_framing_requires_complete_matching_checksum() {
    let (mut verified, _) = official_trailer_stream();
    verified.streaming.as_mut().unwrap().unsigned_chunks = true;
    let mut wire = b"10400\r\n".to_vec();
    wire.extend(std::iter::repeat_n(b'a', 66560));
    wire.extend_from_slice(b"\r\n0\r\nx-amz-checksum-crc32c:sOO8/Q==\r\n\r\n");
    for frame in [1, 7, 8192] {
        let mut verifier = verified.streaming_verifier().unwrap();
        for bytes in wire.chunks(frame) {
            verifier.update(bytes).unwrap();
        }
        verifier.finish().unwrap();
    }
    let mut corrupt = wire.clone();
    corrupt[100] ^= 1;
    let mut extra = wire.clone();
    extra.push(b'x');
    for invalid in [corrupt, extra, wire[..wire.len() - 2].to_vec()] {
        let mut verifier = verified.streaming_verifier().unwrap();
        assert!(verifier.update(&invalid).is_err() || verifier.finish().is_err());
    }
}

#[test]
fn signed_trailer_rejects_bad_checksum_signature_truncation_and_extra_fields() {
    let (verified, wire) = official_trailer_stream();
    let start = wire
        .windows(22)
        .position(|bytes| bytes == b"x-amz-checksum-crc32c:")
        .unwrap();
    for suffix in [
        b"x-amz-checksum-crc32c:AAAAAA==\r\n\r\n".as_slice(),
        b"x-amz-checksum-crc32c:sOO8/Q==\r\n\r\n",
        b"x-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-checksum-crc32c:sOO8/Q==\r\n\r\n",
        b"x-amz-checksum-sha256:AAAA\r\n\r\n",
        b"x-amz-trailer-signature:0000000000000000000000000000000000000000000000000000000000000000\r\n\r\n",
    ] {
        let mut invalid = wire[..start].to_vec(); invalid.extend_from_slice(suffix);
        let mut verifier = verified.streaming_verifier().unwrap();
        assert!(verifier.update(&invalid).is_err() || verifier.finish().is_err());
    }
    for end in [start, wire.len() - 1, wire.len() - 2] {
        let mut verifier = verified.streaming_verifier().unwrap();
        assert!(verifier.update(&wire[..end]).is_err() || verifier.finish().is_err());
    }
    let mut extra = wire;
    extra.extend_from_slice(b"x");
    assert!(
        verified
            .streaming_verifier()
            .unwrap()
            .update(&extra)
            .is_err()
    );
}

// Independent constants from AWS's published sigv4-streaming.html example.
fn official_stream() -> (VerifiedSigV4, Vec<u8>) {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("host", "s3.amazonaws.com"),
        ("content-encoding", "aws-chunked"),
        ("content-length", "66824"),
        ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        ("x-amz-date", "20130524T000000Z"),
        ("x-amz-decoded-content-length", "66560"),
        ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        (
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
        ),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    let verified = verify_sigv4(
        &Method::PUT,
        &"/examplebucket/chunkObject.txt".parse().unwrap(),
        &headers,
        &credential(),
        "us-east-1",
        1369353600,
    )
    .unwrap();
    let mut wire = Vec::new();
    for (size, signature) in [
        (
            65536usize,
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
        ),
        (
            1024,
            "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
        ),
        (
            0,
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
        ),
    ] {
        wire.extend_from_slice(format!("{size:x};chunk-signature={signature}\r\n").as_bytes());
        wire.extend(std::iter::repeat_n(b'a', size));
        wire.extend_from_slice(b"\r\n");
    }
    (verified, wire)
}

#[test]
fn official_streaming_chain_accepts_arbitrary_fragmentation() {
    let (verified, wire) = official_stream();
    assert!(!verified.matches_payload(&Sha256::digest(b"").into()));
    for frame in [1, 7, 8192, wire.len()] {
        let mut verifier = verified.streaming_verifier().unwrap();
        for bytes in wire.chunks(frame) {
            verifier.update(bytes).unwrap();
        }
        verifier.finish().unwrap();
    }
}

#[test]
fn streaming_chain_rejects_corruption_truncation_and_extra_bytes() {
    let (verified, wire) = official_stream();
    let mut corrupt_data = wire.clone();
    corrupt_data[100] ^= 1;
    let mut corrupt_signature = wire.clone();
    corrupt_signature[24] = b'0';
    let mut extra = wire.clone();
    extra.push(b'x');
    for invalid in [corrupt_data, corrupt_signature, extra, vec![b'x'; 1025]] {
        let mut verifier = verified.streaming_verifier().unwrap();
        assert!(verifier.update(&invalid).is_err());
        assert!(verifier.finish().is_err());
        assert!(verifier.update(&wire).is_err());
    }
    for length in [0, 1, 100, wire.len() - 2, wire.len() - 86] {
        let mut verifier = verified.streaming_verifier().unwrap();
        let result = verifier.update(&wire[..length]);
        assert!(result.is_err() || verifier.finish().is_err());
    }
}

#[test]
fn streaming_chain_enforces_length_order_and_framing() {
    let (verified, wire) = official_stream();
    for length in [0, 66559, 66561, u64::MAX] {
        let mut changed = verified.clone();
        changed.streaming.as_mut().unwrap().length = length;
        let mut verifier = changed.streaming_verifier().unwrap();
        assert!(verifier.update(&wire).is_err() || verifier.finish().is_err());
    }
    let first_end = wire
        .windows(4)
        .position(|bytes| bytes == b"\r\n40")
        .unwrap()
        + 2;
    let mut reordered = wire[first_end..].to_vec();
    reordered.extend_from_slice(&wire[..first_end]);
    let mut bad_separator = wire.clone();
    bad_separator[first_end - 1] = b'x';
    let mut bad_size = wire.clone();
    bad_size[0] = b'-';
    for invalid in [reordered, bad_separator, bad_size] {
        let mut verifier = verified.streaming_verifier().unwrap();
        assert!(verifier.update(&invalid).is_err());
        assert!(verifier.finish().is_err());
    }
}

fn credential() -> SigV4Credential<'static> {
    SigV4Credential {
        access_key: ACCESS,
        secret_key: SECRET,
        session_token: None,
    }
}

fn signed_post_policy(policy: &str) -> String {
    let prefixed_secret = format!("AWS4{SECRET}");
    let date_key = mac(prefixed_secret.as_bytes(), b"20130524").unwrap();
    let region_key = mac(&date_key, b"us-east-1").unwrap();
    let service_key = mac(&region_key, b"s3").unwrap();
    let signing_key = mac(&service_key, b"aws4_request").unwrap();
    let mut signer = Hmac::<Sha256>::new_from_slice(&signing_key).unwrap();
    signer.update(policy.as_bytes());
    hex::encode(signer.finalize().into_bytes())
}

#[test]
fn sigv4_post_policy_checks_signature_conditions_and_file_size() {
    let policy_json = serde_json::json!({
        "expiration": "2013-05-24T00:15:00.000Z",
        "conditions": [
            { "bucket": "team.models" },
            ["starts-with", "$key", "uploads/"],
            { "x-amz-algorithm": "AWS4-HMAC-SHA256" },
            { "x-amz-credential": "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request" },
            { "x-amz-date": "20130524T000000Z" },
            ["content-length-range", 1, 1024]
        ]
    });
    let policy = STANDARD.encode(serde_json::to_vec(&policy_json).unwrap());
    let signature = signed_post_policy(&policy);
    let verified = verify_sigv4_post_policy(
        &policy,
        &SigV4PostPolicyAuth {
            credential_scope: "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
            date: "20130524T000000Z",
            security_token: None,
            signature: &signature,
        },
        &credential(),
        "us-east-1",
        1369353600,
    )
    .unwrap();
    let fields = vec![
        ("key".to_owned(), "uploads/model.bin".to_owned()),
        ("x-amz-algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
        (
            "x-amz-credential".to_owned(),
            "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_owned(),
        ),
        ("x-amz-date".to_owned(), "20130524T000000Z".to_owned()),
        ("policy".to_owned(), policy.clone()),
        ("x-amz-signature".to_owned(), signature),
    ];
    verified
        .validate_form_fields(&fields, "team.models")
        .unwrap();
    assert_eq!(verified.maximum_file_size().unwrap(), Some(1024));
    verified.validate_form(&fields, "team.models", 512).unwrap();
    assert!(
        verified
            .validate_form(&fields, "other.models", 512)
            .is_err()
    );
    assert!(
        verified
            .validate_form(&fields, "team.models", 1025)
            .is_err()
    );

    let mut unconditioned = fields.clone();
    unconditioned.push(("acl".to_owned(), "public-read".to_owned()));
    assert!(
        verified
            .validate_form(&unconditioned, "team.models", 512)
            .is_err()
    );
    let mut duplicate = fields;
    duplicate.push(("key".to_owned(), "uploads/other.bin".to_owned()));
    assert!(
        verified
            .validate_form(&duplicate, "team.models", 512)
            .is_err()
    );
    assert!(
        verify_sigv4_post_policy(
            &policy,
            &SigV4PostPolicyAuth {
                credential_scope: "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
                date: "20130524T000000Z",
                security_token: None,
                signature: &"00".repeat(32),
            },
            &credential(),
            "us-east-1",
            1369353600,
        )
        .is_err()
    );
}

fn official_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "host",
        HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
    );
    headers.insert("range", HeaderValue::from_static("bytes=0-9"));
    headers.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_static(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
    );
    headers.insert("authorization", HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"));
    headers
}

// AWS's published GET Object signing example, not a signature generated by
// the implementation under test:
// https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sig-v4-header-based-auth.html
#[test]
fn official_header_vector_and_body_commitment() {
    let verified = verify_sigv4(
        &Method::GET,
        &"/test.txt".parse().unwrap(),
        &official_headers(),
        &credential(),
        "us-east-1",
        1369353600,
    )
    .unwrap();
    assert!(verified.matches_payload(&Sha256::digest(b"").into()));
    assert!(!verified.matches_payload(&Sha256::digest(b"changed").into()));
}

#[test]
fn rejects_tampered_method_path_header_and_region() {
    for (method, path, region) in [
        (Method::PUT, "/test.txt", "us-east-1"),
        (Method::GET, "/other.txt", "us-east-1"),
        (Method::GET, "/test.txt", "eu-west-1"),
    ] {
        assert!(
            verify_sigv4(
                &method,
                &path.parse().unwrap(),
                &official_headers(),
                &credential(),
                region,
                1369353600
            )
            .is_err()
        );
    }
    let mut headers = official_headers();
    headers.insert("range", HeaderValue::from_static("bytes=1-9"));
    assert!(
        verify_sigv4(
            &Method::GET,
            &"/test.txt".parse().unwrap(),
            &headers,
            &credential(),
            "us-east-1",
            1369353600
        )
        .is_err()
    );
}

#[test]
fn rejects_expired_future_and_ambiguous_authentication() {
    for now in [1369352699, 1369354501] {
        assert!(
            verify_sigv4(
                &Method::GET,
                &"/test.txt".parse().unwrap(),
                &official_headers(),
                &credential(),
                "us-east-1",
                now
            )
            .is_err()
        );
    }
    let mut headers = official_headers();
    headers.append("authorization", headers["authorization"].clone());
    assert!(
        verify_sigv4(
            &Method::GET,
            &"/test.txt".parse().unwrap(),
            &headers,
            &credential(),
            "us-east-1",
            1369353600
        )
        .is_err()
    );
    assert!(
        verify_sigv4(
            &Method::GET,
            &"/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256"
                .parse()
                .unwrap(),
            &official_headers(),
            &credential(),
            "us-east-1",
            1369353600
        )
        .is_err()
    );
}

#[test]
fn rejects_unknown_missing_and_duplicate_authorization_fields() {
    let original = official_headers()["authorization"]
        .to_str()
        .unwrap()
        .to_owned();
    for changed in [
        original.replace(",Signature=", ",Unknown=ignored,Signature="),
        original.replace("Credential=", "Unknown=ignored,Credential="),
        original.replace(",SignedHeaders=", ",Credential=duplicate,SignedHeaders="),
        original.replace(",SignedHeaders=", ",Signature=duplicate,SignedHeaders="),
        original.replace(",Signature=", ""),
        original.replace(",Signature=", ",Signature=duplicate,Signature="),
    ] {
        let mut headers = official_headers();
        headers.insert("authorization", HeaderValue::from_str(&changed).unwrap());
        assert!(
            verify_sigv4(
                &Method::GET,
                &"/test.txt".parse().unwrap(),
                &headers,
                &credential(),
                "us-east-1",
                1369353600,
            )
            .is_err(),
            "accepted Authorization fields: {changed}"
        );
    }
}

#[test]
fn rejects_unsigned_amz_headers_and_unbound_session_credentials() {
    let uri = "/test.txt".parse().unwrap();
    let mut headers = official_headers();
    headers.insert("x-amz-meta-owner", HeaderValue::from_static("changed"));
    assert!(
        verify_sigv4(
            &Method::GET,
            &uri,
            &headers,
            &credential(),
            "us-east-1",
            1369353600
        )
        .is_err()
    );
    let session = SigV4Credential {
        session_token: Some("session-secret"),
        ..credential()
    };
    assert!(
        verify_sigv4(
            &Method::GET,
            &uri,
            &official_headers(),
            &session,
            "us-east-1",
            1369353600
        )
        .is_err()
    );
    let mut session_headers = official_headers();
    session_headers.insert(
        "x-amz-security-token",
        HeaderValue::from_static("session-secret"),
    );
    assert!(
        verify_sigv4(
            &Method::GET,
            &uri,
            &session_headers,
            &session,
            "us-east-1",
            1369353600
        )
        .is_err()
    );
}

#[test]
fn query_encoding_preserves_literal_plus_and_repeated_values() {
    let pairs = query_pairs(&"/a?key=a+b&key=a%20b&empty&encoded=%2F".parse().unwrap()).unwrap();
    assert_eq!(pairs[0].1, "a+b");
    assert_eq!(encode(&pairs[0].1), "a%2Bb");
    assert_eq!(encode(&pairs[1].1), "a%20b");
    assert_eq!(pairs[2].1, "");
    assert!(optional_unique(&pairs, "key").is_err());
}

// Independent AWS presigned-URL example from sigv4-query-string-auth.html.
#[test]
fn official_presigned_vector_expiry_and_query_tampering() {
    let query = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";
    let mut headers = HeaderMap::new();
    headers.insert(
        "host",
        HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
    );
    let verified = verify_sigv4(
        &Method::GET,
        &query.parse().unwrap(),
        &headers,
        &credential(),
        "us-east-1",
        1369353600,
    )
    .unwrap();
    assert_eq!(verified.payload_sha256, None);
    assert!(
        verify_sigv4(
            &Method::GET,
            &query.parse().unwrap(),
            &headers,
            &credential(),
            "us-east-1",
            1369440001
        )
        .is_err()
    );
    for changed in [
        query.replace("86400", "604801"),
        format!("{query}&X-Amz-Date=20130524T000000Z"),
        format!("{query}&prefix=changed"),
        query.replace("test.txt", "test2.txt"),
    ] {
        assert!(
            verify_sigv4(
                &Method::GET,
                &changed.parse().unwrap(),
                &headers,
                &credential(),
                "us-east-1",
                1369353600
            )
            .is_err()
        );
    }
}
