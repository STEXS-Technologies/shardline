//! Percent escapes must be decoded exactly once by the HTTP extractors.
use super::*;

#[tokio::test]
async fn metadata_percent_literal_delete_does_not_delete_nested_victim() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(71);
    write_record(tmp.path(), &id, 10, None).await;
    let victim = "safe/a/b.txt";
    let registered = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url(victim))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"fileId": id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(registered.status(), StatusCode::OK);
    // One wire decode yields safe/a%2Fb.txt, a different filename.
    let deleted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(path_url("safe/a%252Fb.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::OK);
    let deleted = get_body(deleted).await;
    assert_eq!(
        deleted["deleted"], 0,
        "literal percent path must not delete nested victim"
    );
    let victim_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(tree_url("?path=safe%2Fa%2Fb.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(victim_response.status(), StatusCode::OK);
    assert_eq!(get_body(victim_response).await["fileId"], id);
}

#[tokio::test]
async fn metadata_percent_literals_roundtrip_register_resolve_list_delete() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(72);
    write_record(tmp.path(), &id, 20, None).await;
    for (wire, canonical, prefix) in [
        (
            "literal%252Fdir/a%252Fb.txt",
            "literal%2Fdir/a%2Fb.txt",
            "literal%252Fdir%2F",
        ),
        (
            "%252E%252E/a%2520b%2525.txt",
            "%2E%2E/a%20b%25.txt",
            "%252E%252E%2F",
        ),
    ] {
        let registered = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(path_url(wire))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"fileId": id}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(registered.status(), StatusCode::OK);
        assert_eq!(get_body(registered).await["path"], canonical);
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("path", canonical)
            .finish();
        let resolved = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(tree_url(&format!("?{query}")))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resolved.status(), StatusCode::OK);
        let resolved = get_body(resolved).await;
        assert_eq!(resolved["path"], canonical);
        assert_eq!(resolved["fileId"], id);
        let listed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(tree_url(&format!("?prefix={prefix}")))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed = get_body(listed).await;
        assert_eq!(listed["entries"].as_array().unwrap().len(), 1);
        assert_eq!(listed["entries"][0]["path"], canonical);
        let deleted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(path_url(wire))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::OK);
        let deleted = get_body(deleted).await;
        assert_eq!(deleted["path"], canonical);
        assert_eq!(deleted["deleted"], 1);
        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(tree_url(&format!("?{query}")))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn metadata_wire_decoded_unsafe_paths_remain_rejected() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(73);
    write_record(tmp.path(), &id, 30, None).await;
    for wire in ["%2E%2E/a", "a%00b", "a%5Cb", "%2Fabsolute"] {
        let registered = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(path_url(wire))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"fileId": id}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(registered.status(), StatusCode::BAD_REQUEST, "{wire}");
        let resolved = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(tree_url(&format!("?path={wire}")))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resolved.status(), StatusCode::BAD_REQUEST, "{wire}");
    }
}
