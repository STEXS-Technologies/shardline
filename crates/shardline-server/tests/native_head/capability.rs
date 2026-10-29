//! Replay the same headerless capability across authoritative revocation.
use super::*;

pub(super) struct Read {
    url: reqwest::Url,
    range: String,
    expected: Vec<u8>,
}

impl Read {
    pub(super) async fn issue(
        client: &Client,
        source: &str,
        credential: &str,
        file_id: &str,
        bytes: &[u8],
    ) -> Self {
        let origin = reqwest::Url::parse(source).unwrap().origin();
        let response = client
            .get(format!(
                "{}/v1/reconstructions/{file_id}",
                origin.ascii_serialization()
            ))
            .bearer_auth(credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let fetches = body.get("fetch_info").unwrap().as_object().unwrap();
        assert_eq!(fetches.len(), 1);
        let entry = fetches
            .values()
            .next()
            .unwrap()
            .as_array()
            .unwrap()
            .first()
            .unwrap();
        let url = reqwest::Url::parse(entry.get("url").unwrap().as_str().unwrap()).unwrap();
        assert_eq!(url.origin(), origin);
        let range = entry.get("url_range").unwrap();
        let start = range.get("start").unwrap().as_u64().unwrap();
        let end = range.get("end").unwrap().as_u64().unwrap();
        Self {
            url,
            range: format!("bytes={start}-{end}"),
            expected: bytes
                .get(usize::try_from(start).unwrap()..=usize::try_from(end).unwrap())
                .unwrap()
                .to_vec(),
        }
    }

    async fn response(&self, client: &Client) -> reqwest::Response {
        client
            .get(self.url.clone())
            .header(reqwest::header::RANGE, &self.range)
            .send()
            .await
            .map_err(|_error| "capability transport failed; URL omitted")
            .unwrap()
    }

    pub(super) async fn allowed(&self, client: &Client) {
        let response = self.response(client).await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let bytes = response
            .bytes()
            .await
            .map_err(|_error| "capability body failed; URL omitted")
            .unwrap();
        assert_eq!(bytes.as_ref(), self.expected);
    }

    pub(super) async fn denied(&self, client: &Client, proxy: &S3FaultProxy) {
        let before = (proxy.get_requests(), proxy.head_requests());
        let response = self.response(client).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(!response.headers().contains_key(reqwest::header::LOCATION));
        assert_eq!(
            (proxy.get_requests(), proxy.head_requests()),
            before,
            "revoked capability must not inspect provider objects"
        );
    }
}
