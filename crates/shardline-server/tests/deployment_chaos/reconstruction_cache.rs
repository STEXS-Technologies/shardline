//! Compare stable reconstruction plans while verifying their expiring grants.

use reqwest::{Client, StatusCode, Url};
use shardline_server::FileReconstructionResponse;

pub(super) async fn verified_plan(
    client: &Client,
    body: &[u8],
    base: &str,
    source: &[u8],
) -> FileReconstructionResponse {
    let mut plan: FileReconstructionResponse = serde_json::from_slice(body).unwrap();
    let origin = Url::parse(base).unwrap().origin();
    assert!(
        !plan.fetch_info.is_empty(),
        "fixture must advertise source reads"
    );
    for entries in plan.fetch_info.values_mut() {
        for fetch in entries {
            let mut url = Url::parse(&fetch.url).unwrap();
            assert_eq!(url.origin(), origin, "transfer stays on the fixture server");
            assert!(
                url.query_pairs()
                    .any(|(name, _)| name == "xorb_read_capability")
            );
            // Exercise the advertised grant without forwarding a bearer token.
            let response = client
                .get(url.clone())
                .header(
                    "Range",
                    format!("bytes={}-{}", fetch.url_range.start, fetch.url_range.end),
                )
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            let start = usize::try_from(fetch.url_range.start).unwrap();
            let end = usize::try_from(fetch.url_range.end.checked_add(1).unwrap()).unwrap();
            let expected = source
                .get(start..end)
                .expect("fixture range stays inside source");
            assert_eq!(
                response.bytes().await.unwrap().as_ref(),
                expected,
                "advertised source bytes remain exact"
            );

            // Only the per-response credential is transient. Keep the origin,
            // path, other query fields, term order, lengths and ranges intact.
            let stable: Vec<(String, String)> = url
                .query_pairs()
                .filter(|(name, _)| name != "xorb_read_capability")
                .map(|(name, value)| (name.into_owned(), value.into_owned()))
                .collect();
            url.set_query(None);
            if !stable.is_empty() {
                url.query_pairs_mut().extend_pairs(stable);
            }
            fetch.url = url.into();
        }
    }
    plan
}
