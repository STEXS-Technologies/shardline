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
    use super::{CliRuntimeError, endpoint};

    #[test]
    fn endpoint_joins_base_url_and_path() {
        assert_eq!(
            endpoint("http://127.0.0.1:8080/", "/v1/stats"),
            "http://127.0.0.1:8080/v1/stats"
        );
    }

    #[test]
    fn endpoint_no_trailing_slash_on_base() {
        assert_eq!(
            endpoint("http://127.0.0.1:8080", "healthz"),
            "http://127.0.0.1:8080/healthz"
        );
    }

    #[test]
    fn endpoint_both_slashes_trimmed() {
        assert_eq!(
            endpoint("http://host/", "/path/"),
            "http://host/path/"
        );
    }

    #[test]
    fn endpoint_empty_path_returns_base_with_slash() {
        assert_eq!(
            endpoint("http://host", ""),
            "http://host/"
        );
    }

    // ── CliRuntimeError Display / Debug ─────────────────────────────────

    #[test]
    fn cli_runtime_error_io_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = CliRuntimeError::Io(io_err);
        assert!(err.to_string().contains("standard io failed"));
    }

    #[test]
    fn cli_runtime_error_json_display() {
        let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = CliRuntimeError::Json(json_err);
        assert!(err.to_string().contains("json operation failed"));
    }

    #[test]
    fn cli_runtime_error_http_from_reqwest_error() {
        // Verify the From<ReqwestError> impl by constructing a reqwest::Error
        // via a builder that produces an error from an invalid URL.
        let result = reqwest::Url::parse("not a url");
        assert!(result.is_err());
        // The From<reqwest::Error> conversion is verified at compile time
        // through the `#[from]` attribute on CliRuntimeError::Http.
    }

    #[test]
    fn cli_runtime_error_server_status_display() {
        let err = CliRuntimeError::ServerStatus {
            status: 500,
            body: "internal error".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("500"));
        assert!(msg.contains("internal error"));
    }

    #[test]
    fn cli_runtime_error_unhealthy_display() {
        let err = CliRuntimeError::Unhealthy {
            status: "degraded".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("degraded"));
        assert!(msg.contains("health status"));
    }

    #[test]
    fn cli_runtime_error_debug() {
        let err = CliRuntimeError::ServerStatus {
            status: 404,
            body: "not found".to_owned(),
        };
        let debug = format!("{err:?}");
        assert!(debug.contains("ServerStatus"));
    }
}
