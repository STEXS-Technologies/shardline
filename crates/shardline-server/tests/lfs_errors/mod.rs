//! Malformed lock requests keep the LFS error envelope and cannot mutate locks.
use super::*;

#[tokio::test]
async fn malformed_lock_requests_preserve_lfs_errors_and_existing_locks() {
    exercise_malformed_locks(None).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn malformed_lock_requests_preserve_lfs_errors_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    exercise_malformed_locks(Some(&database)).await;
}

async fn exercise_malformed_locks(database: Option<&str>) {
    for frontend in [ServerFrontend::Lfs, ServerFrontend::Hub] {
        let root = tempfile::tempdir().unwrap();
        let server = start(root.path(), frontend, true, database).await;
        let repository = format!(
            "lock-errors-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        );
        let credential = token("writer", &repository, TokenScope::Write);
        let client = Client::new();
        let url = lock_url(&server.base, &repository, frontend);
        let created = client
            .post(&url)
            .bearer_auth(&credential)
            .json(&json!({"path":"keep.bin"}))
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: Value = created.json().await.unwrap();
        let id = created["lock"]["id"].as_str().unwrap();
        for query in [
            "limit=0",
            "limit=1001",
            "limit=abc",
            "limit=",
            "limit=1&limit=2",
            "%6cimit=1&limit=2",
            "cursor=a&cursor=b",
            "path=a&path=b",
            "id=a&id=b",
            "refspec=main&refspec=other",
        ] {
            let rejected = client
                .get(format!("{url}?{query}"))
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap();
            assert_eq!(
                rejected.status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{query}"
            );
            assert_eq!(
                rejected.headers()["content-type"],
                "application/vnd.git-lfs+json"
            );
            let error: Value = rejected.json().await.unwrap();
            assert!(error["message"].is_string());
        }
        for endpoint in [
            url.clone(),
            format!("{url}/verify"),
            format!("{url}/{id}/unlock"),
        ] {
            for (content_type, body, status) in [
                ("application/json", "{", StatusCode::BAD_REQUEST),
                ("text/plain", "{}", StatusCode::UNSUPPORTED_MEDIA_TYPE),
                ("application/json", "[]", StatusCode::UNPROCESSABLE_ENTITY),
            ] {
                let rejected = client
                    .post(&endpoint)
                    .bearer_auth(&credential)
                    .header("content-type", content_type)
                    .body(body)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(rejected.status(), status, "{endpoint}");
                assert_eq!(
                    rejected.headers()["content-type"],
                    "application/vnd.git-lfs+json"
                );
                let error: Value = rejected.json().await.unwrap();
                assert!(error["message"].is_string());
            }
        }
        let listed: Value = client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(listed["locks"].as_array().unwrap().len(), 1);
        assert_eq!(listed["locks"][0]["id"], id);
        server.stop().await;
    }
}
