use std::io::Error as IoError;

use reqwest::{Client, Error as ReqwestError, Response};
use serde::de::DeserializeOwned;
use serde_json::Error as JsonError;
use shardline_server::HealthResponse;
use thiserror::Error;

/// CLI runtime failure.
#[derive(Debug, Error)]
pub enum CliRuntimeError {
    /// Standard input or output failed.
    #[error("standard io failed")]
    Io(#[from] IoError),
    /// HTTP request failed.
    #[error("http request failed")]
    Http(#[from] ReqwestError),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] JsonError),
    /// The server returned a non-success status.
    #[error("server returned http status {status}: {body}")]
    ServerStatus {
        /// HTTP status code.
        status: u16,
        /// Response body.
        body: String,
    },
    /// The server did not return an ok health status.
    #[error("server health status was {status}")]
    Unhealthy {
        /// Reported health status.
        status: String,
    },
}

/// Runs the HTTP health check command.
///
/// # Errors
///
/// Returns [`CliRuntimeError`] when the server cannot be reached or returns a
/// non-ok health response.
pub async fn run_health_check(server_url: &str) -> Result<(), CliRuntimeError> {
    let client = Client::new();
    let response = client.get(endpoint(server_url, "healthz")).send().await?;
    let health = read_json_response::<HealthResponse>(response).await?;
    if health.status != "ok" {
        return Err(CliRuntimeError::Unhealthy {
            status: health.status,
        });
    }

    Ok(())
}

fn endpoint(server_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        server_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

async fn read_json_response<T>(response: Response) -> Result<T, CliRuntimeError>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await?;
        return Err(CliRuntimeError::ServerStatus {
            status: status.as_u16(),
            body,
        });
    }

    Ok(response.json::<T>().await?)
}

#[cfg(test)]
mod tests {
    use super::endpoint;

    #[test]
    fn endpoint_joins_base_url_and_path() {
        assert_eq!(
            endpoint("http://127.0.0.1:8080/", "/v1/stats"),
            "http://127.0.0.1:8080/v1/stats"
        );
    }
}
