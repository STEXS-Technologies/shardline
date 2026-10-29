//! Transport for the administrator-configured internal native file reader.
//!
//! Webhook destinations are user supplied and must resolve only to public
//! addresses. The configured Shardline endpoint can instead be a private
//! service hostname. Keep their connection pools and DNS policies separate.
use std::{sync::OnceLock, time::Duration};

use crate::error::HubApiError;

pub(super) fn client() -> Result<&'static reqwest::Client, HubApiError> {
    static CLIENT: OnceLock<Result<reqwest::Client, reqwest::Error>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                // This endpoint comes from server configuration, never Host or
                // forwarding headers. Do not send its scoped token via an
                // environment proxy or forward it to a redirect destination.
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(300))
                .build()
        })
        .as_ref()
        .map_err(|_error| {
            HubApiError::CasError("internal Xet file reader is unavailable".to_owned())
        })
}
