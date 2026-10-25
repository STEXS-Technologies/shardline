//! Signed downloads must use the Shardline transfer route without proxy credentials.
use super::*;

pub(super) async fn verify(
    client: &Client,
    public_base: &str,
    token: &str,
    file_id: &str,
) -> Result<(), Box<dyn Error>> {
    let reconstruction = client
        .get(format!("{public_base}/v1/reconstructions/{file_id}"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)?
        .json::<shardline_server::FileReconstructionResponse>()
        .await
        .map_err(reqwest::Error::without_url)?;
    // Checking the advertised origin alone is insufficient: following a
    // redirect could still bypass Shardline and expose a backend capability.
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?;
    let other_repository = bearer_token(
        "other-reader",
        TokenScope::Read,
        RepositoryProvider::Generic,
        "team",
        "other",
        Some("main"),
    )?;
    assert_eq!(
        client
            .get(format!("{public_base}/v1/reconstructions/{file_id}"))
            .bearer_auth(other_repository)
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .status(),
        StatusCode::NOT_FOUND,
        "another repository must not obtain this file's download grants"
    );
    let public = reqwest::Url::parse(public_base)?;
    let mut count = 0;
    for entries in reconstruction.fetch_info.values() {
        for entry in entries {
            let url = reqwest::Url::parse(&entry.url)?;
            // Do not print signed URLs even when this assertion fails.
            assert!(
                url.origin() == public.origin(),
                "download bypassed Shardline origin"
            );
            assert!(url.path().starts_with("/transfer/xorb/"));
            assert!(
                url.query_pairs()
                    .any(|(name, _)| name == "xorb_read_capability")
            );
            let range = format!("bytes={}-{}", entry.url_range.start, entry.url_range.end);
            let response = client
                .get(url.clone())
                .header("Range", &range)
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert!(!response.headers().contains_key("Location"));
            assert!(
                !response
                    .bytes()
                    .await
                    .map_err(reqwest::Error::without_url)?
                    .is_empty()
            );
            let mut damaged = url.clone();
            let mut parameters: Vec<_> = damaged.query_pairs().into_owned().collect();
            for (name, value) in &mut parameters {
                if name == "xorb_read_capability" {
                    value.push('!');
                }
            }
            damaged.query_pairs_mut().clear().extend_pairs(parameters);
            assert_eq!(
                client
                    .get(damaged)
                    .header("Range", &range)
                    .send()
                    .await
                    .map_err(reqwest::Error::without_url)?
                    .status(),
                StatusCode::UNAUTHORIZED,
                "a damaged capability must not authorize any download"
            );
            assert_eq!(
                client
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(reqwest::Error::without_url)?
                    .status(),
                StatusCode::BAD_REQUEST,
                "a signed range grant must not become a whole-object request"
            );
            let widened = entry.url_range.end.checked_add(1).ok_or("range overflow")?;
            let shifted = entry
                .url_range
                .start
                .checked_add(1)
                .ok_or("range overflow")?;
            assert!(
                shifted <= entry.url_range.end,
                "fixture needs a multi-byte grant"
            );
            for changed in [
                format!("bytes={}-{widened}", entry.url_range.start),
                format!("bytes={shifted}-{}", entry.url_range.end),
                "bytes=-1".to_owned(),
            ] {
                assert_eq!(
                    client
                        .get(url.clone())
                        .header("Range", changed)
                        .send()
                        .await
                        .map_err(reqwest::Error::without_url)?
                        .status(),
                    StatusCode::NOT_FOUND,
                    "a modified range must not disclose shared container bytes"
                );
            }
            let mut other_object = url.clone();
            other_object.set_path(&format!("/transfer/xorb/default/{}", "b".repeat(64)));
            assert_eq!(
                client
                    .get(other_object)
                    .header("Range", &range)
                    .send()
                    .await
                    .map_err(reqwest::Error::without_url)?
                    .status(),
                StatusCode::NOT_FOUND,
                "a grant must not authorize another object"
            );
            let mut unsigned = url;
            unsigned.set_query(None);
            let rejected = client
                .get(unsigned)
                .header("Range", range)
                .send()
                .await
                .map_err(reqwest::Error::without_url)?;
            assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
            count += 1;
        }
    }
    assert!(count > 0, "fixture must exercise signed transfers");
    Ok(())
}
