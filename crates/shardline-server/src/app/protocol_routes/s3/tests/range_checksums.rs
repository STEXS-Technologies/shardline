//! Object-level checksums must never be presented as partial-payload checksums.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranged_get_omits_object_checksum_but_full_get_and_head_preserve_it() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let content = b"0123456789abcdef";
    let checksum = STANDARD.encode(sha2::Sha256::digest(content));
    let mut request = put_request(format!("/{BUCKET}/{KEY}"), content.to_vec());
    request
        .headers_mut()
        .insert("x-amz-checksum-sha256", checksum.parse().unwrap());
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::OK
    );
    let auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));
    for method in ["HEAD", "GET"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(format!("/{BUCKET}/{KEY}"))
                    .header(header::AUTHORIZATION, &auth)
                    .header("x-amz-checksum-mode", "ENABLED")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["x-amz-checksum-sha256"],
            checksum.as_str()
        );
        assert_eq!(response.headers()["x-amz-checksum-type"], "FULL_OBJECT");
        if method == "GET" {
            assert_eq!(body_bytes(response).await, content);
        }
    }
    for (range, expected) in [
        ("bytes=0-0", &content[..1]),
        ("bytes=2-5", &content[2..6]),
        ("bytes=0-15", content.as_slice()),
        ("bytes=-2", &content[14..]),
        ("bytes=2-", &content[2..]),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/{BUCKET}/{KEY}"))
                    .header(header::AUTHORIZATION, &auth)
                    .header(header::RANGE, range)
                    .header("x-amz-checksum-mode", "ENABLED")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(!response.headers().contains_key("x-amz-checksum-sha256"));
        assert!(!response.headers().contains_key("x-amz-checksum-type"));
        assert_eq!(body_bytes(response).await, expected);
    }
}
