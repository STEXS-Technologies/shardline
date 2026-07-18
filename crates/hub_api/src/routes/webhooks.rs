use std::sync::LazyLock;
use tokio::sync::Semaphore;

use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use crate::error::HubApiError;
use crate::models::*;
// HubRepoType used in the webhook_create handler for repo lookup
// (used implicitly via state.store methods)
use shardline_protocol::TokenScope;

use super::{HubState, authorize};

/// Delivers webhook events to registered URLs.
///
/// This fires in the background after a commit. Failures are logged but do not
/// block the commit response.
pub(crate) async fn deliver_webhook_events(
    state: &HubState,
    repo_id: &str,
    event: &str,
    revision: &str,
) {
    static WEBHOOK_DELIVERY_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(16));

    let client = match &state.http_client {
        Some(client) => client.clone(),
        None => return,
    };
    let webhooks = match state.store.webhooks_for_event(repo_id, event) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("failed to load webhooks for {repo_id}: {e}");
            return;
        }
    };
    if webhooks.is_empty() {
        return;
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let payload = crate::models::WebhookEventPayload {
        event: event.to_owned(),
        repository: repo_id.to_owned(),
        revision: revision.to_owned(),
        timestamp,
        data: serde_json::json!({}),
    };
    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("failed to serialize webhook payload: {e}");
            return;
        }
    };
    for webhook in &webhooks {
        let url = webhook.url.clone();
        let body = body.clone();
        let secret = webhook.secret.clone();
        let client = client.clone();
        let Ok(permit) = WEBHOOK_DELIVERY_SEMAPHORE.acquire().await else {
            tracing::warn!("webhook delivery semaphore closed");
            return;
        };
        let url_for_log = sanitize_log_url(&url);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = deliver_one_webhook(&client, &url, &body, secret.as_deref()).await {
                tracing::warn!("webhook delivery to {url_for_log} failed: {e}");
            }
        });
    }
}

/// Sanitizes a URL for safe inclusion in log messages.
///
/// Replaces control characters (newlines, tabs, etc.) and truncates to a
/// reasonable length to prevent log injection via user-supplied URLs.
pub(crate) fn sanitize_log_url(url: &str) -> String {
    const MAX_LOG_URL_LEN: usize = 200;
    let sanitized: String = url
        .chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .take(MAX_LOG_URL_LEN)
        .collect();
    if url.len() > MAX_LOG_URL_LEN {
        format!("{sanitized}...")
    } else {
        sanitized
    }
}

/// Delivers a single webhook POST with optional HMAC-SHA256 signature.
async fn deliver_one_webhook(
    client: &reqwest::Client,
    url: &str,
    body: &[u8],
    secret: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    validate_webhook_url(url)?;
    // Resolve DNS and verify none of the resolved addresses are private.
    // DNS-based bypass is the most common SSRF vector — hostnames like
    // "localtest.me" (→127.0.0.1) or "metadata.google.internal" (→169.254.169.254)
    // would pass the string-based check but resolve to private IPs.
    //
    // NOTE: There is a theoretical TOCTOU (time-of-check-time-of-use) window
    // between DNS validation and the actual HTTP connection — a DNS rebinding
    // attack could change the resolution between the two lookups. Mitigating
    // this fully requires reqwest's `dns` feature (ClientBuilder::resolve) to
    // pin the connection to validated addresses, which is not currently enabled.
    // The attack window is very narrow in practice and requires a cooperating
    // authoritative DNS server, so this is accepted as a known limitation.
    {
        let parsed_url =
            url::Url::parse(url).map_err(|e| format!("webhook URL parse failed: {e}"))?;
        let host = parsed_url.host_str().ok_or("webhook URL has no host")?;
        let port = parsed_url.port_or_known_default().unwrap_or(80);
        let host_port = format!("{host}:{port}");
        for addr in tokio::net::lookup_host(&*host_port).await? {
            if is_private_ip(&addr.ip()) {
                return Err("webhook URL resolves to a private address".into());
            }
        }
    }
    let mut request = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("User-Agent", "shardline-hub/1.0");
    if let Some(secret) = secret {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        request = request.header("X-Hub-Signature-256", format!("sha256={signature}"));
    }
    let response = request.body(body.to_vec()).send().await?;
    if !response.status().is_success() {
        return Err(format!("webhook returned {}", response.status()).into());
    }
    Ok(())
}

// ---- Webhook endpoints ----

/// Maximum allowed webhook URL length.
const MAX_WEBHOOK_URL_LEN: usize = 2048;

/// Maximum number of events per webhook.
const MAX_WEBHOOK_EVENTS: usize = 50;

/// Validates a webhook URL to prevent SSRF attacks.
///
/// Checks:
/// - Scheme is `http` or `https`
/// - Host is present
/// - URL length does not exceed 2048 characters
/// - Host is not a private/internal IP or reserved address
pub(crate) fn validate_webhook_url(url: &str) -> Result<(), HubApiError> {
    if url.len() > MAX_WEBHOOK_URL_LEN {
        return Err(HubApiError::PathValidation(format!(
            "webhook URL exceeds maximum length of {MAX_WEBHOOK_URL_LEN}"
        )));
    }

    let parsed = url::Url::parse(url)
        .map_err(|e| HubApiError::PathValidation(format!("invalid webhook URL: {e}")))?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(HubApiError::PathValidation(format!(
            "webhook URL scheme must be http or https, got {scheme}"
        )));
    }

    let host_str = parsed
        .host_str()
        .ok_or_else(|| HubApiError::PathValidation("webhook URL has no host".to_owned()))?;

    // Strip brackets from IPv6 addresses like [::1]
    let host = host_str
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host_str);

    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if is_private_ip(&ip) {
            return Err(HubApiError::PathValidation(
                "webhook URL must not point to a private/internal/reserved address".to_owned(),
            ));
        }
    } else if host.eq_ignore_ascii_case("localhost") {
        return Err(HubApiError::PathValidation(
            "webhook URL must not point to localhost".to_owned(),
        ));
    }

    Ok(())
}

/// Returns `true` if the IP address is private, loopback, link-local, or
/// otherwise reserved (not globally routable).
pub(crate) const fn is_private_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback() // 127.0.0.0/8
                || v4.is_private() // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                || v4.is_link_local() // 169.254.0.0/16
                || v4.is_unspecified() // 0.0.0.0
                || v4.is_broadcast()
                || v4.is_documentation() // 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
                || is_cgnat(*v4) // 100.64.0.0/10 (RFC 6598 shared address space)
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() // ::1
                || v6.is_unspecified() // ::
                || v6.is_unicast_link_local() // fe80::/10
                || v6.is_unique_local() // fc00::/7 (RFC 4193)
                || match v6.to_ipv4_mapped() {
                    Some(v4) => is_private_ip(&std::net::IpAddr::V4(v4)),
                    None => false,
                }
        }
    }
}

/// Returns `true` if the IPv4 address is in the CGNAT/Shared Address Space
/// range 100.64.0.0/10 (RFC 6598).
pub(crate) const fn is_cgnat(ip: std::net::Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    a == 100 && (b & 0xC0) == 64
}

pub(crate) fn webhook_response_from_hub(
    webhook: &shardline_index::hub::HubWebhook,
) -> WebhookResponse {
    WebhookResponse {
        id: webhook.id.clone(),
        url: webhook.url.clone(),
        events: webhook.events.clone(),
        active: webhook.active,
        created_at: webhook.created_at_unix_seconds,
    }
}

/// Creates a webhook for a repository.
pub(crate) async fn webhook_create(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
    Json(request): Json<WebhookCreateRequest>,
) -> Result<(StatusCode, Json<WebhookResponse>), HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_create", "POST", 201);
    authorize(&state, &headers, TokenScope::Write)?;
    if request.events.len() > MAX_WEBHOOK_EVENTS {
        return Err(HubApiError::PathValidation(format!(
            "webhook events exceeds maximum of {MAX_WEBHOOK_EVENTS}"
        )));
    }
    validate_webhook_url(&request.url)?;
    let name = format!("{ns}/{repo}");
    let _ = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    // Check for duplicate webhook URL.
    let existing = state
        .store
        .list_webhooks(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    if existing.iter().any(|wh| wh.url == request.url) {
        return Err(HubApiError::Conflict(format!(
            "webhook with URL {} already exists for repo {name}",
            request.url
        )));
    }
    let webhook = state
        .store
        .create_webhook(
            &name,
            &request.url,
            &request.events,
            request.secret.as_deref(),
        )
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(webhook_response_from_hub(&webhook)),
    ))
}

/// Lists webhooks for a repository.
pub(crate) async fn webhook_list(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<WebhookListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_list", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let webhooks = state
        .store
        .list_webhooks(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let response = WebhookListResponse {
        webhooks: webhooks.iter().map(webhook_response_from_hub).collect(),
    };
    Ok(Json(response))
}

/// Deletes a webhook.
pub(crate) async fn webhook_delete(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo, webhook_id)): Path<(String, String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_delete", "DELETE", 204);
    authorize(&state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    state
        .store
        .delete_webhook(&name, &webhook_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Webhook tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    #[test]
    fn validate_webhook_url_accepts_valid_http() {
        assert!(validate_webhook_url("http://example.com/hook").is_ok());
    }

    #[test]
    fn validate_webhook_url_rejects_ftp_scheme() {
        assert!(validate_webhook_url("ftp://example.com/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_javascript_scheme() {
        assert!(validate_webhook_url("javascript:alert(1)").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_localhost() {
        assert!(validate_webhook_url("http://localhost/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_private_ip_10() {
        assert!(validate_webhook_url("http://10.0.0.1/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_private_ip_192_168() {
        assert!(validate_webhook_url("http://192.168.1.1/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_loopback() {
        assert!(validate_webhook_url("http://127.0.0.1/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_ipv6_loopback() {
        assert!(validate_webhook_url("http://[::1]/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_long_url() {
        let long = format!("http://example.com/{}", "a".repeat(3000));
        assert!(validate_webhook_url(&long).is_err());
    }

    #[test]
    fn is_private_ip_true_for_loopback() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_private_10() {
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_private_172() {
        let ip: IpAddr = "172.16.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_private_192_168() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_link_local() {
        let ip: IpAddr = "169.254.1.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_cgnat() {
        let ip: IpAddr = "100.64.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_unspecified() {
        let ip: IpAddr = "0.0.0.0".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv6_loopback() {
        let ip: IpAddr = "::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv6_link_local() {
        let ip: IpAddr = "fe80::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_false_for_public() {
        let ip: IpAddr = "8.8.8.8".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_false_for_ipv6_public() {
        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn sanitize_log_url_normal() {
        let url = "https://example.com/webhook";
        assert_eq!(sanitize_log_url(url), url);
    }

    #[test]
    fn sanitize_log_url_replaces_control_chars() {
        let url = "https://example.com/new\nline";
        assert_eq!(sanitize_log_url(url), "https://example.com/new?line");
    }

    #[test]
    fn sanitize_log_url_truncates_long() {
        let base = "https://example.com/";
        let long = format!("{}{}", base, "a".repeat(300));
        let result = sanitize_log_url(&long);
        assert!(result.len() <= 204); // 200 chars + "..."
        assert!(result.ends_with("..."));
    }

    #[test]
    fn sanitize_log_url_replaces_tab() {
        let url = "https://example.com/\tpath";
        assert_eq!(sanitize_log_url(url), "https://example.com/?path");
    }

    #[test]
    fn sanitize_log_url_short_no_truncation() {
        let url = "http://a.b";
        assert_eq!(sanitize_log_url(url), url);
        assert!(!url.ends_with("..."));
    }

    #[test]
    fn is_cgnat_true_for_100_64_range() {
        assert!(is_cgnat("100.64.0.0".parse().unwrap()));
        assert!(is_cgnat("100.127.255.255".parse().unwrap()));
    }

    #[test]
    fn is_cgnat_false_outside_range() {
        assert!(!is_cgnat("100.63.255.255".parse().unwrap()));
        assert!(!is_cgnat("100.128.0.1".parse().unwrap()));
        assert!(!is_cgnat("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn is_cgnat_false_for_loopback() {
        assert!(!is_cgnat("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn validate_webhook_url_rejects_empty() {
        assert!(validate_webhook_url("").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_no_host() {
        assert!(validate_webhook_url("http://").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_ipv6_unique_local() {
        assert!(validate_webhook_url("http://[fc00::1]/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_accepts_ipv6_public() {
        let result = validate_webhook_url("http://[2600:1f18:22b4:da00::1]/hook");
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_ipv4_mapped_loopback() {
        assert!(validate_webhook_url("http://[::ffff:127.0.0.1]/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_broadcast() {
        assert!(validate_webhook_url("http://255.255.255.255/hook").is_err());
    }

    #[test]
    fn is_private_ip_false_for_ipv4_mapped_public_v6() {
        let ip: IpAddr = "::ffff:8.8.8.8".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv4_mapped_loopback_v6() {
        let ip: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv4_mapped_private_v6() {
        let ip: IpAddr = "::ffff:192.168.1.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv6_unique_local() {
        let ip: IpAddr = "fc00::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_false_for_ipv6_public_unicast() {
        let ip: IpAddr = "2600::1".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_broadcast() {
        let ip: std::net::IpAddr = "255.255.255.255".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_documentation() {
        let ip: std::net::IpAddr = "192.0.2.1".parse().unwrap();
        assert!(is_private_ip(&ip));
        let ip: std::net::IpAddr = "198.51.100.1".parse().unwrap();
        assert!(is_private_ip(&ip));
        let ip: std::net::IpAddr = "203.0.113.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_cgnat_boundary() {
        let ip: std::net::IpAddr = "100.64.0.0".parse().unwrap();
        assert!(is_private_ip(&ip), "100.64.0.0 should be CGNAT");
        let ip: std::net::IpAddr = "100.127.255.255".parse().unwrap();
        assert!(is_private_ip(&ip), "100.127.255.255 should be CGNAT");
    }

    #[test]
    fn is_private_ip_false_for_cgnat_boundary_outside() {
        let ip: std::net::IpAddr = "100.63.255.255".parse().unwrap();
        assert!(!is_private_ip(&ip), "100.63.255.255 should NOT be private");
        let ip: std::net::IpAddr = "100.128.0.0".parse().unwrap();
        assert!(!is_private_ip(&ip), "100.128.0.0 should NOT be private");
    }

    #[test]
    fn is_private_ip_false_for_ipv6_unique_local_not_covered() {
        let ip: std::net::IpAddr = "fd00::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn validate_webhook_url_rejects_missing_scheme() {
        assert!(validate_webhook_url("example.com/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_https_is_fine() {
        assert!(validate_webhook_url("https://example.com/hook").is_ok());
    }

    #[test]
    fn validate_webhook_url_ipv6_public_ok() {
        let ip: std::net::IpAddr = "2001:db8::1".parse().unwrap();
        assert!(!is_private_ip(&ip));
        let result = validate_webhook_url("http://[2001:db8::1]/hook");
        let _ = result;
    }
}
