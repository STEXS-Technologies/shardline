//! Forged v2 requests must obey snapshot authorization despite legacy discovery.
use super::*;

pub(super) async fn deny_hidden_objects(app: &Router, head: &str, parent: &str, blob: &str) {
    for revision in ["refs/tags/release", head] {
        for forbidden in [parent, blob] {
            for filtered in [false, true] {
                let want = format!("want {forbidden}\n");
                let filter = if filtered {
                    let line = "filter blob:none\n";
                    format!("{:04x}{line}", line.len().checked_add(4).unwrap())
                } else {
                    String::new()
                };
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/models/alice/assets/git-upload-pack")
                            .header("Git-Protocol", "version=2")
                            .header("Content-Type", "application/x-git-upload-pack-request")
                            .header(
                                "Authorization",
                                format!("Bearer {}", token("assets", Some(revision))),
                            )
                            .body(Body::from(format!(
                                "0012command=fetch\n0001{:04x}{want}{filter}0009done\n0000",
                                want.len().checked_add(4).unwrap()
                            )))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::FORBIDDEN);
                let body = axum::body::to_bytes(response.into_body(), 65536)
                    .await
                    .unwrap();
                assert!(!String::from_utf8_lossy(&body).contains("private parent payload"));
            }
        }
    }
}
