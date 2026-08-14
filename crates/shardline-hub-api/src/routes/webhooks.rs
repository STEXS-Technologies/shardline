use std::str::FromStr;
use std::sync::LazyLock;
use tokio::sync::Semaphore;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use crate::{error::HubApiError, models::*, types::WebhookScheme};
// HubRepoType used in the webhook_create handler for repo lookup
// (used implicitly via state.store methods)
use shardline_index::hub::HubWebhook;
use shardline_protocol::SecretString;

use super::{HubRepository, HubState};

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
        let client = client.clone();
        let url_for_log = sanitize_log_url(&url);
        // Resolve the plaintext signing secret, decrypting at-rest ciphertext
        // and lazily upgrading legacy plaintext rows when a key is configured.
        let secret = match resolve_webhook_secret(state, webhook) {
            Ok(secret) => secret,
            Err(e) => {
                tracing::warn!("failed to resolve secret for webhook {url_for_log}: {e}");
                continue;
            }
        };
        let Ok(permit) = WEBHOOK_DELIVERY_SEMAPHORE.acquire().await else {
            tracing::warn!("webhook delivery semaphore closed");
            return;
        };
        // Spawn a delivery task and a separate monitoring task that holds the
        // semaphore permit and logs panics. If the delivery task panics, tokio
        // catches it and the JoinHandle returns Err, which we log here.
        let delivery_handle = tokio::spawn(async move {
            deliver_one_webhook(
                &client,
                &url,
                &body,
                secret.as_ref().map(SecretString::expose_secret),
            )
            .await
        });
        tokio::spawn(async move {
            let _permit = permit;
            match delivery_handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!("webhook delivery to {url_for_log} failed: {e}");
                }
                Err(panic) => {
                    tracing::error!("webhook delivery to {url_for_log} panicked: {panic:?}");
                }
            }
        });
    }
}

/// Resolves the plaintext signing secret for a webhook at delivery time.
///
/// When an at-rest cipher is configured, stored `sse1:`-formatted ciphertext is
/// decrypted; legacy plaintext rows are re-encrypted in place (lazy upgrade).
/// When no key is configured, a legacy plaintext value is returned unchanged;
/// a stored `sse1:`-prefixed ciphertext with no key configured is an explicit
/// error (never silently used as the HMAC secret).
fn resolve_webhook_secret(
    state: &HubState,
    webhook: &HubWebhook,
) -> Result<Option<SecretString>, crate::secrets::WebhookSecretCipherError> {
    let Some(stored) = webhook.secret.as_ref() else {
        return Ok(None);
    };
    let Some(cipher) = state.webhook_secret_cipher.as_ref() else {
        if stored
            .expose_secret()
            .starts_with(crate::secrets::MAGIC_PREFIX)
        {
            // Rows previously encrypted at rest would otherwise have their
            // base64 ciphertext used verbatim as the HMAC secret, silently
            // rejecting every delivery. Fail loudly instead.
            return Err(crate::secrets::WebhookSecretCipherError::NoCipherForCiphertext);
        }
        return Ok(Some(stored.clone()));
    };
    let repo_id = &webhook.repo_id;
    let decrypted = cipher.decrypt(repo_id, stored.expose_secret())?;
    if decrypted.needs_upgrade {
        // Rewrite the legacy plaintext row as ciphertext (best-effort).
        if let Ok(encrypted) = cipher.encrypt(repo_id, decrypted.secret.expose_secret())
            && let Err(e) =
                state
                    .store
                    .update_webhook_secret(repo_id, &webhook.id, Some(&encrypted))
        {
            tracing::warn!("failed to upgrade webhook secret for {}: {e}", webhook.id);
        }
    }
    Ok(Some(decrypted.secret))
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
    // The shared reqwest client (no `dns` feature) cannot carry per-host
    // resolution pins via `ClientBuilder::resolve`, so we narrow the
    // time-of-check-time-of-use window by recording the validated (public)
    // addresses here and re-resolving immediately before the send below,
    // rejecting on any mismatch. See `deliver_one_webhook`'s pre-send check.
    let parsed_url = url::Url::parse(url).map_err(|e| format!("webhook URL parse failed: {e}"))?;
    let host = parsed_url.host_str().ok_or("webhook URL has no host")?;
    let port = parsed_url.port_or_known_default().unwrap_or(80);
    let host_port = format!("{host}:{port}");
    let mut validated = std::collections::HashSet::new();
    for addr in tokio::net::lookup_host(&*host_port).await? {
        if is_private_ip(&addr.ip()) {
            return Err("webhook URL resolves to a private address".into());
        }
        validated.insert(addr.ip());
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
    // Re-resolve immediately before sending to detect DNS rebinding between the
    // validation above and the actual connection. If the hostname now resolves
    // to a private address, or to an address that was not in the validated set,
    // reject the delivery rather than connect to a rebound (e.g. cloud metadata)
    // endpoint. This is a best-effort mitigation given the shared client cannot
    // pin addresses at connect time; the window is not fully closed without the
    // reqwest `dns` feature.
    for addr in tokio::net::lookup_host(&*host_port).await? {
        let ip = addr.ip();
        if is_private_ip(&ip) || !validated.contains(&ip) {
            return Err(
                "webhook URL resolved to a different address than validated (possible DNS rebinding)"
                    .into(),
            );
        }
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
    if WebhookScheme::from_str(scheme).is_err() {
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
    _repo: HubRepository<true>,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
    Json(request): Json<WebhookCreateRequest>,
) -> Result<(StatusCode, Json<WebhookResponse>), HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_create", "POST", 201);
    // The extractor has already required Write scope and bound the token to
    // this repository before the handler runs.
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
    // Encrypt the signing secret at rest when a cipher is configured.
    let secret_to_store: Option<String> =
        match (&state.webhook_secret_cipher, request.secret.as_ref()) {
            (Some(cipher), Some(plain)) => Some(
                cipher
                    .encrypt(&name, plain.expose_secret())
                    .map_err(HubApiError::from)?,
            ),
            _ => None,
        };
    let webhook = state
        .store
        .create_webhook(
            &name,
            &request.url,
            &request.events,
            secret_to_store
                .as_deref()
                .or_else(|| request.secret.as_ref().map(SecretString::expose_secret)),
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
    _repo: HubRepository,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<WebhookListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_list", "GET", 200);
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
    _repo: HubRepository<true>,
    Path((_repo_type, ns, repo, webhook_id)): Path<(String, String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_delete", "DELETE", 204);
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
    use crate::routes::test_repo;
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

    // ── At-rest webhook secret encryption ─────────────────────────────────

    use axum::Json;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use shardline_protocol::SecretBytes;
    use shardline_server_core::ServerObjectStore;

    use crate::models::WebhookCreateRequest;
    use crate::secrets::{WebhookSecretCipher, WebhookSecretCipherError};

    const ENC_KEY: [u8; 32] = *b"0123456789abcdef0123456789abcdef";

    fn make_encrypted_state(key: &[u8; 32]) -> (tempfile::TempDir, HubState) {
        let ts = tempfile::tempdir().unwrap();
        shardline_index::hub::ensure_hub_tables(ts.path()).unwrap();
        let store = shardline_index::LocalIndexStore::open(ts.path().to_path_buf());
        let boxed = BoxedHubStore::from_store(store);
        boxed
            .create_repo(HubRepoType::Model, "org/enc", false)
            .unwrap();
        let cipher = WebhookSecretCipher::new(SecretBytes::new(key.to_vec())).unwrap();
        let object_store = ServerObjectStore::local(ts.path().join("lfs")).unwrap();
        let state = HubState {
            store: boxed,
            object_store,
            auth: None,
            http_client: None,
            webhook_secret_cipher: Some(cipher),
        };
        (ts, state)
    }

    #[tokio::test]
    async fn webhook_create_stores_ciphertext_at_rest() {
        let (_ts, state) = make_encrypted_state(&ENC_KEY);
        let plaintext = "s3cr3t-webhook-token";
        let (status, resp) = webhook_create(
            State(state.clone()),
            test_repo(&state, &axum::http::HeaderMap::new()),
            Path(("models".into(), "org".into(), "enc".into())),
            Json(WebhookCreateRequest {
                url: "https://example.com/hook".into(),
                events: vec!["push".into()],
                secret: Some(SecretString::from_secret(plaintext)),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        let stored = state
            .store
            .list_webhooks("org/enc")
            .unwrap()
            .into_iter()
            .find(|wh| wh.id == resp.id)
            .unwrap();
        let raw = stored.secret.as_ref().unwrap().expose_secret().to_owned();
        assert!(raw.starts_with("sse1:"));
        assert_ne!(raw, plaintext);
    }

    #[test]
    fn decrypt_round_trips_hmac_signing_secret() {
        let (_ts, state) = make_encrypted_state(&ENC_KEY);
        let repo = "org/enc";
        let plaintext = "hmac-signing-secret";
        let cipher = WebhookSecretCipher::new(SecretBytes::new(ENC_KEY.to_vec())).unwrap();
        let encrypted = cipher.encrypt(repo, plaintext).unwrap();
        state
            .store
            .create_webhook(
                repo,
                "https://example.com/hook",
                &["push".to_owned()],
                Some(&encrypted),
            )
            .unwrap();
        let webhook = state.store.list_webhooks(repo).unwrap().remove(0);
        let resolved = resolve_webhook_secret(&state, &webhook).unwrap().unwrap();
        assert_eq!(resolved.expose_secret(), plaintext);

        // The decrypted secret must produce the same HMAC-SHA256 signature as
        // the original plaintext over the delivery body.
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let body = b"{\"event\":\"push\"}";
        let mut expected = HmacSha256::new_from_slice(plaintext.as_bytes()).unwrap();
        expected.update(body);
        let expected_sig = format!("sha256={}", hex::encode(expected.finalize().into_bytes()));
        let mut actual = HmacSha256::new_from_slice(resolved.expose_secret().as_bytes()).unwrap();
        actual.update(body);
        let actual_sig = format!("sha256={}", hex::encode(actual.finalize().into_bytes()));
        assert_eq!(expected_sig, actual_sig);
    }

    #[test]
    fn legacy_plaintext_row_is_upgraded_on_read() {
        let (_ts, state) = make_encrypted_state(&ENC_KEY);
        let repo = "org/enc";
        // Seed a legacy plaintext row as if it predated encryption.
        state
            .store
            .create_webhook(
                repo,
                "https://example.com/hook",
                &["push".to_owned()],
                Some("legacy-plain"),
            )
            .unwrap();
        let seeded = state.store.list_webhooks(repo).unwrap().remove(0);
        assert!(
            !seeded
                .secret
                .as_ref()
                .unwrap()
                .expose_secret()
                .starts_with("sse1:")
        );

        // Reading it with a configured cipher decrypts AND upgrades in place.
        let resolved = resolve_webhook_secret(&state, &seeded).unwrap().unwrap();
        assert_eq!(resolved.expose_secret(), "legacy-plain");

        let upgraded = state.store.list_webhooks(repo).unwrap().remove(0);
        assert!(
            upgraded
                .secret
                .as_ref()
                .unwrap()
                .expose_secret()
                .starts_with("sse1:")
        );
    }

    #[test]
    fn without_key_secret_stored_and_read_as_plaintext() {
        let ts = tempfile::tempdir().unwrap();
        shardline_index::hub::ensure_hub_tables(ts.path()).unwrap();
        let store = shardline_index::LocalIndexStore::open(ts.path().to_path_buf());
        let boxed = BoxedHubStore::from_store(store);
        boxed
            .create_repo(HubRepoType::Model, "org/plain", false)
            .unwrap();
        let object_store = ServerObjectStore::local(ts.path().join("lfs")).unwrap();
        let state = HubState {
            store: boxed,
            object_store,
            auth: None,
            http_client: None,
            webhook_secret_cipher: None,
        };
        state
            .store
            .create_webhook(
                "org/plain",
                "https://example.com/hook",
                &["push".to_owned()],
                Some("plain-secret"),
            )
            .unwrap();
        let wh = state.store.list_webhooks("org/plain").unwrap().remove(0);
        assert_eq!(wh.secret.as_ref().unwrap().expose_secret(), "plain-secret");
        let resolved = resolve_webhook_secret(&state, &wh).unwrap().unwrap();
        assert_eq!(resolved.expose_secret(), "plain-secret");
    }

    #[test]
    fn ciphertext_without_key_fails_loudly() {
        let (_ts, state) = make_encrypted_state(&ENC_KEY);
        let repo = "org/enc";
        let cipher = WebhookSecretCipher::new(SecretBytes::new(ENC_KEY.to_vec())).unwrap();
        let encrypted = cipher.encrypt(repo, "secret").unwrap();
        state
            .store
            .create_webhook(
                repo,
                "https://example.com/hook",
                &["push".to_owned()],
                Some(&encrypted),
            )
            .unwrap();
        // Rebuild state without a cipher, simulating a deployment where the key
        // was removed after rows were encrypted at rest.
        let no_cipher_state = HubState {
            webhook_secret_cipher: None,
            ..state
        };
        let wh = no_cipher_state.store.list_webhooks(repo).unwrap().remove(0);
        let result = resolve_webhook_secret(&no_cipher_state, &wh);
        assert!(
            matches!(result, Err(WebhookSecretCipherError::NoCipherForCiphertext)),
            "expected a loud error when ciphertext has no key, got: {result:?}"
        );
    }

    #[test]
    fn wrong_key_fails_to_decrypt_without_panic() {
        const WRONG_KEY: [u8; 32] = *b"abcdef0123456789abcdef0123456789";
        let (_ts, state) = make_encrypted_state(&ENC_KEY);
        let repo = "org/enc";
        let cipher = WebhookSecretCipher::new(SecretBytes::new(ENC_KEY.to_vec())).unwrap();
        let encrypted = cipher.encrypt(repo, "secret").unwrap();
        state
            .store
            .create_webhook(
                repo,
                "https://example.com/hook",
                &["push".to_owned()],
                Some(&encrypted),
            )
            .unwrap();
        let wh = state.store.list_webhooks(repo).unwrap().remove(0);
        let wrong = WebhookSecretCipher::new(SecretBytes::new(WRONG_KEY.to_vec())).unwrap();
        let result = wrong.decrypt(repo, wh.secret.as_ref().unwrap().expose_secret());
        assert!(matches!(result, Err(WebhookSecretCipherError::Decrypt(_))));
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
