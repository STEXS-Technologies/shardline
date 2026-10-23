use super::*;

#[tokio::test]
async fn committed_attributes_survive_overwrite_restart_and_reject_invalid_requests() {
    exercise(None).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn postgres_committed_attributes_survive_overwrite_restart() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise(Some(&database)).await;
}

async fn exercise(database: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let owner = format!(
        "attributes-{}",
        root.path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .trim_start_matches('.')
    )
    .to_ascii_lowercase();
    let credential = token("writer", &owner, TokenScope::Write);
    let read = token("reader", &owner, TokenScope::Read);
    let client = Client::new();
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let url = format!("{}/{}.locks/object", server.base, owner);
    for body in ["original", "replacement is a different size"] {
        let put = client
            .put(&url)
            .bearer_auth(&credential)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
        let etag = put.headers()["etag"]
            .to_str()
            .unwrap()
            .trim_matches('"')
            .to_owned();
        let response = client
            .get(format!("{url}?attributes"))
            .bearer_auth(&read)
            .header(
                "x-amz-object-attributes",
                "ETag,ObjectSize,StorageClass,Checksum",
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("last-modified"));
        let xml = response.text().await.unwrap();
        assert!(xml.contains(&format!("<ETag>{etag}</ETag>")), "{xml}");
        assert!(
            xml.contains(&format!("<ObjectSize>{}</ObjectSize>", body.len())),
            "{xml}"
        );
        // S3 omits StorageClass for STANDARD objects.
        assert!(!xml.contains("<StorageClass>"));
        assert!(!xml.contains("<Checksum>") && !xml.contains("<ObjectParts>"));
    }
    read_conditions(&client, &url, &read).await;
    for attributes in [
        None,
        Some(""),
        Some("ObjectSize,Unknown"),
        Some("ETag,ETag"),
    ] {
        let mut request = client.get(format!("{url}?attributes")).bearer_auth(&read);
        if let Some(attributes) = attributes {
            request = request.header("x-amz-object-attributes", attributes);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
    let response = client
        .get(format!("{url}?attributes"))
        .bearer_auth(&read)
        .header("x-amz-object-attributes", "ObjectSize")
        .header("x-amz-object-attributes", "ETag")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = client
        .get(format!("{url}?attributes"))
        .bearer_auth(&read)
        .header("x-amz-object-attributes", "ObjectParts")
        .send()
        .await
        .unwrap();
    // A current-version single-PUT record has a known zero multipart-part
    // count, represented by omitting ObjectParts as S3 does for single-part
    // objects.
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.text().await.unwrap().contains("<ObjectParts>"));

    // A newly completed multipart upload persists its part count alongside
    // (but separately from) the authoritative object record.
    let created = client
        .post(format!("{url}?uploads"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let created_xml = created.text().await.unwrap();
    let upload_id = xml_tag(&created_xml, "UploadId");
    let part_body = b"multipart payload";
    let uploaded = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .bearer_auth(&credential)
        .body(part_body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);
    let part_etag = uploaded.headers()["etag"].to_str().unwrap().to_owned();
    let completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let completed = client
        .post(format!("{url}?uploadId={upload_id}"))
        .bearer_auth(&credential)
        .body(completion)
        .send()
        .await
        .unwrap();
    assert_eq!(completed.status(), StatusCode::OK);
    let response = client
        .get(format!("{url}?attributes"))
        .bearer_auth(&read)
        .header("x-amz-object-attributes", "ObjectParts")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<PartsCount>1</PartsCount>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
    assert!(xml.contains("<MaxParts>1000</MaxParts>"), "{xml}");
    assert!(
        xml.contains("<PartNumberMarker>0</PartNumberMarker>"),
        "{xml}"
    );

    let created = client
        .post(format!("{url}?uploads"))
        .bearer_auth(&credential)
        .header("x-amz-checksum-algorithm", "CRC32C")
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let created_xml = created.text().await.unwrap();
    assert!(created_xml.contains("<ChecksumAlgorithm>CRC32C</ChecksumAlgorithm>"));
    let upload_id = xml_tag(&created_xml, "UploadId");
    let crc32c_part = b"CRC32C multipart payload";
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    digest.update(crc32c_part);
    let [_, _, _, _, p0, p1, p2, p3] = digest.finalize().to_be_bytes();
    let part_checksum =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [p0, p1, p2, p3]);
    let wrong_part_checksum =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0_u8, 0, 0, 0]);
    let rejected_part = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .bearer_auth(&credential)
        .header("x-amz-checksum-crc32c", &wrong_part_checksum)
        .body(crc32c_part.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(rejected_part.status(), StatusCode::BAD_REQUEST);
    assert!(
        rejected_part
            .text()
            .await
            .unwrap()
            .contains("<Code>BadDigest</Code>")
    );
    let uploaded = client
        .put(format!("{url}?partNumber=1&uploadId={upload_id}"))
        .bearer_auth(&credential)
        .header("x-amz-checksum-crc32c", &part_checksum)
        .body(crc32c_part.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);
    let part_etag = uploaded.headers()["etag"].to_str().unwrap().to_owned();
    let rejected = client
        .post(format!("{url}?uploadId={upload_id}"))
        .bearer_auth(&credential)
        .body(format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag><ChecksumCRC32C>{wrong_part_checksum}</ChecksumCRC32C></Part></CompleteMultipartUpload>"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert!(
        rejected
            .text()
            .await
            .unwrap()
            .contains("<Code>BadDigest</Code>")
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&read)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"multipart payload"
    );
    let completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag><ChecksumCRC32C>{part_checksum}</ChecksumCRC32C></Part></CompleteMultipartUpload>"
    );
    let completed = client
        .post(format!("{url}?uploadId={upload_id}"))
        .bearer_auth(&credential)
        .body(completion)
        .send()
        .await
        .unwrap();
    assert_eq!(completed.status(), StatusCode::OK);
    let completion_xml = completed.text().await.unwrap();
    assert!(
        completion_xml.contains("<ChecksumCRC32C>"),
        "{completion_xml}"
    );
    assert!(completion_xml.contains("<ChecksumType>COMPOSITE</ChecksumType>"));
    let response = client
        .get(format!("{url}?attributes"))
        .bearer_auth(&read)
        .header("x-amz-object-attributes", "Checksum,ObjectParts")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<ChecksumCRC32C>"), "{xml}");
    assert!(xml.contains("<Part><PartNumber>1</PartNumber>"), "{xml}");
    assert!(
        xml.contains(&format!("<ChecksumCRC32C>{part_checksum}</ChecksumCRC32C>")),
        "{xml}"
    );

    let response = client
        .get(format!("{url}?attributes&versionId=unavailable"))
        .bearer_auth(&read)
        .header("x-amz-object-attributes", "ObjectSize")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let other = token("other", "different-repository", TokenScope::Read);
    assert_eq!(
        client
            .get(format!("{url}?attributes"))
            .bearer_auth(&other)
            .header("x-amz-object-attributes", "ObjectSize")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    server.stop().await;
    let server = start(root.path(), ServerFrontend::S3, true, database).await;
    let url = format!("{}/{}.locks/object", server.base, owner);
    let response = client
        .get(format!("{url}?attributes"))
        .bearer_auth(&read)
        .header("x-amz-object-attributes", "ObjectSize,Checksum,ObjectParts")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let xml = response.text().await.unwrap();
    assert!(!xml.contains("<ETag>") && !xml.contains("<StorageClass>"));
    assert!(
        xml.contains(&format!("<ObjectSize>{}</ObjectSize>", crc32c_part.len())),
        "{xml}"
    );
    assert!(
        xml.contains(&format!("<ChecksumCRC32C>{part_checksum}</ChecksumCRC32C>")),
        "{xml}"
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&read)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        crc32c_part
    );
    assert_eq!(
        client
            .delete(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        client
            .get(format!("{url}?attributes"))
            .bearer_auth(&read)
            .header("x-amz-object-attributes", "ObjectSize")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    server.stop().await;
}

async fn read_conditions(client: &Client, url: &str, credential: &str) {
    let head = client
        .head(url)
        .bearer_auth(credential)
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    let modified = head.headers()["last-modified"].to_str().unwrap();
    let etag = head.headers()["etag"].to_str().unwrap();
    let past = "Thu, 01 Jan 1970 00:00:00 GMT";
    let future = "Fri, 01 Jan 2100 00:00:00 GMT";
    let cases: Vec<(Vec<(&str, &str)>, StatusCode)> = vec![
        (
            vec![("if-modified-since", modified)],
            StatusCode::NOT_MODIFIED,
        ),
        (
            vec![("if-modified-since", future)],
            StatusCode::NOT_MODIFIED,
        ),
        (vec![("if-modified-since", past)], StatusCode::OK),
        (
            vec![("if-modified-since", "Fri, 01 Jan 1960 00:00:00 GMT")],
            StatusCode::OK,
        ),
        (
            vec![("if-unmodified-since", "Fri, 01 Jan 1960 00:00:00 GMT")],
            StatusCode::PRECONDITION_FAILED,
        ),
        (vec![("if-unmodified-since", modified)], StatusCode::OK),
        (vec![("if-unmodified-since", future)], StatusCode::OK),
        (
            vec![("if-unmodified-since", past)],
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            vec![("if-match", etag), ("if-unmodified-since", past)],
            StatusCode::OK,
        ),
        (
            vec![("if-match", "\"different\""), ("if-modified-since", future)],
            StatusCode::PRECONDITION_FAILED,
        ),
        (
            vec![("if-none-match", etag), ("if-modified-since", past)],
            StatusCode::NOT_MODIFIED,
        ),
        (
            vec![
                ("if-none-match", "\"different\""),
                ("if-modified-since", future),
            ],
            StatusCode::OK,
        ),
        (
            vec![("if-none-match", etag), ("if-unmodified-since", past)],
            StatusCode::PRECONDITION_FAILED,
        ),
        (vec![("if-modified-since", "invalid-date")], StatusCode::OK),
        (
            vec![("if-unmodified-since", "invalid-date")],
            StatusCode::OK,
        ),
        (
            vec![("if-modified-since", past), ("if-modified-since", future)],
            StatusCode::OK,
        ),
    ];
    for method in [reqwest::Method::GET, reqwest::Method::HEAD] {
        for attributes in [false, true] {
            if attributes && method == reqwest::Method::HEAD {
                continue;
            }
            let target = if attributes {
                format!("{url}?attributes")
            } else {
                url.to_owned()
            };
            for (headers, expected) in &cases {
                let mut request = client
                    .request(method.clone(), &target)
                    .bearer_auth(credential);
                if attributes {
                    request = request.header("x-amz-object-attributes", "ETag,ObjectSize");
                }
                for (name, value) in headers {
                    request = request.header(*name, *value);
                }
                let response = request.send().await.unwrap();
                assert_eq!(
                    response.status(),
                    *expected,
                    "{method} {target} {headers:?}"
                );
                if *expected == StatusCode::NOT_MODIFIED {
                    assert_eq!(response.headers()["etag"], etag);
                    assert!(response.bytes().await.unwrap().is_empty());
                } else if *expected == StatusCode::OK
                    && method == reqwest::Method::GET
                    && !attributes
                {
                    assert_eq!(
                        response.bytes().await.unwrap().as_ref(),
                        b"replacement is a different size"
                    );
                }
            }
        }
    }
    assert_eq!(
        client
            .get(url)
            .bearer_auth(credential)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"replacement is a different size"
    );
}

fn xml_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open).unwrap() + open.len();
    let end = xml.find(&close).unwrap();
    xml[start..end].to_owned()
}
