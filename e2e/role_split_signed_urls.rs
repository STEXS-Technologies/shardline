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
        .await?
        .error_for_status()?
        .json::<shardline_server::FileReconstructionResponse>()
        .await?;
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
                .await?;
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert!(!response.bytes().await?.is_empty());
            let mut unsigned = url;
            unsigned.set_query(None);
            let rejected = client.get(unsigned).header("Range", range).send().await?;
            assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
            count += 1;
        }
    }
    assert!(count > 0, "fixture must exercise signed transfers");
    Ok(())
}
