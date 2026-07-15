use prometheus::{IntCounter, IntGauge, Registry};

use crate::{must_counter, must_gauge};

pub struct ProtocolMetrics {
    pub lfs_uploads: IntCounter,
    pub lfs_downloads: IntCounter,
    pub oci_uploads: IntCounter,
    pub oci_downloads: IntCounter,
    pub hub_api_requests: IntCounter,
    pub hub_api_commits: IntCounter,
    pub hub_api_file_uploads: IntCounter,
    pub hub_api_file_downloads: IntCounter,
    pub oci_registry_token_requests: IntCounter,
    pub oci_registry_token_rate_limited: IntCounter,
    pub oci_registry_token_active: IntGauge,
}

impl ProtocolMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let lfs_uploads = must_counter(
            "shardline_lfs_upload_requests_total",
            "Git LFS upload requests",
        );
        let lfs_downloads = must_counter(
            "shardline_lfs_download_requests_total",
            "Git LFS download requests",
        );
        let oci_uploads =
            must_counter("shardline_oci_upload_requests_total", "OCI upload requests");
        let oci_downloads = must_counter(
            "shardline_oci_download_requests_total",
            "OCI download requests",
        );
        let hub_api_requests =
            must_counter("shardline_hub_api_requests_total", "Hub API requests");
        let hub_api_commits = must_counter(
            "shardline_hub_api_commit_operations_total",
            "Hub API commit operations",
        );
        let hub_api_file_uploads = must_counter(
            "shardline_hub_api_file_uploads_total",
            "Hub API file uploads",
        );
        let hub_api_file_downloads = must_counter(
            "shardline_hub_api_file_downloads_total",
            "Hub API file downloads",
        );
        let oci_registry_token_requests = must_counter(
            "shardline_oci_registry_token_requests_total",
            "OCI registry token requests",
        );
        let oci_registry_token_rate_limited = must_counter(
            "shardline_oci_registry_token_rate_limited_total",
            "OCI registry token requests rate limited",
        );
        let oci_registry_token_active = must_gauge(
            "shardline_oci_registry_token_active_requests",
            "Active OCI registry token requests",
        );

        registry.register(Box::new(lfs_uploads.clone())).ok();
        registry.register(Box::new(lfs_downloads.clone())).ok();
        registry.register(Box::new(oci_uploads.clone())).ok();
        registry.register(Box::new(oci_downloads.clone())).ok();
        registry.register(Box::new(hub_api_requests.clone())).ok();
        registry.register(Box::new(hub_api_commits.clone())).ok();
        registry
            .register(Box::new(hub_api_file_uploads.clone()))
            .ok();
        registry
            .register(Box::new(hub_api_file_downloads.clone()))
            .ok();
        registry
            .register(Box::new(oci_registry_token_requests.clone()))
            .ok();
        registry
            .register(Box::new(oci_registry_token_rate_limited.clone()))
            .ok();
        registry
            .register(Box::new(oci_registry_token_active.clone()))
            .ok();

        Self {
            lfs_uploads,
            lfs_downloads,
            oci_uploads,
            oci_downloads,
            hub_api_requests,
            hub_api_commits,
            hub_api_file_uploads,
            hub_api_file_downloads,
            oci_registry_token_requests,
            oci_registry_token_rate_limited,
            oci_registry_token_active,
        }
    }

    pub fn record_lfs_upload(&self) {
        self.lfs_uploads.inc();
    }
    pub fn record_lfs_download(&self) {
        self.lfs_downloads.inc();
    }
    pub fn record_oci_upload(&self) {
        self.oci_uploads.inc();
    }
    pub fn record_oci_download(&self) {
        self.oci_downloads.inc();
    }

    pub fn record_hub_api_request(&self, _endpoint: &str, _method: &str, _status: u16) {
        self.hub_api_requests.inc();
    }

    pub fn record_hub_api_commit(&self, _operation: &str) {
        self.hub_api_commits.inc();
    }

    pub fn record_hub_api_file_upload(&self) {
        self.hub_api_file_uploads.inc();
    }

    pub fn record_hub_api_file_download(&self) {
        self.hub_api_file_downloads.inc();
    }

    pub fn record_oci_registry_token_request(&self) {
        self.oci_registry_token_requests.inc();
    }

    pub fn record_oci_registry_token_rate_limited(&self) {
        self.oci_registry_token_rate_limited.inc();
    }

    pub fn begin_oci_registry_token_request(&self) {
        self.oci_registry_token_active.inc();
    }

    pub fn end_oci_registry_token_request(&self) {
        self.oci_registry_token_active.dec();
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::ProtocolMetrics;

    fn new_metrics() -> ProtocolMetrics {
        ProtocolMetrics::new(&Registry::new())
    }

    #[test]
    fn record_lfs_upload_increments_counter() {
        let m = new_metrics();
        m.record_lfs_upload();
        assert_eq!(m.lfs_uploads.get(), 1);
    }

    #[test]
    fn record_lfs_download_increments_counter() {
        let m = new_metrics();
        m.record_lfs_download();
        assert_eq!(m.lfs_downloads.get(), 1);
    }

    #[test]
    fn record_oci_upload_increments_counter() {
        let m = new_metrics();
        m.record_oci_upload();
        assert_eq!(m.oci_uploads.get(), 1);
    }

    #[test]
    fn record_oci_download_increments_counter() {
        let m = new_metrics();
        m.record_oci_download();
        assert_eq!(m.oci_downloads.get(), 1);
    }

    #[test]
    fn record_hub_api_request_increments_counter() {
        let m = new_metrics();
        m.record_hub_api_request("/test", "GET", 200);
        assert_eq!(m.hub_api_requests.get(), 1);
    }

    #[test]
    fn record_hub_api_commit_increments_counter() {
        let m = new_metrics();
        m.record_hub_api_commit("create");
        assert_eq!(m.hub_api_commits.get(), 1);
    }

    #[test]
    fn record_hub_api_file_upload_increments_counter() {
        let m = new_metrics();
        m.record_hub_api_file_upload();
        assert_eq!(m.hub_api_file_uploads.get(), 1);
    }

    #[test]
    fn record_hub_api_file_download_increments_counter() {
        let m = new_metrics();
        m.record_hub_api_file_download();
        assert_eq!(m.hub_api_file_downloads.get(), 1);
    }

    #[test]
    fn oci_registry_token_begin_end_balance() {
        let m = new_metrics();
        m.begin_oci_registry_token_request();
        m.begin_oci_registry_token_request();
        m.end_oci_registry_token_request();
        // Two begins, one end => gauge = 1
        assert_eq!(m.oci_registry_token_active.get(), 1);
    }

    #[test]
    fn oci_registry_token_request_records() {
        let m = new_metrics();
        m.record_oci_registry_token_request();
        m.record_oci_registry_token_rate_limited();
        assert_eq!(m.oci_registry_token_requests.get(), 1);
        assert_eq!(m.oci_registry_token_rate_limited.get(), 1);
    }

    #[test]
    fn all_counters_start_at_zero() {
        let m = new_metrics();
        assert_eq!(m.lfs_uploads.get(), 0);
        assert_eq!(m.lfs_downloads.get(), 0);
        assert_eq!(m.oci_uploads.get(), 0);
        assert_eq!(m.oci_downloads.get(), 0);
        assert_eq!(m.hub_api_requests.get(), 0);
        assert_eq!(m.hub_api_commits.get(), 0);
        assert_eq!(m.hub_api_file_uploads.get(), 0);
        assert_eq!(m.hub_api_file_downloads.get(), 0);
        assert_eq!(m.oci_registry_token_requests.get(), 0);
        assert_eq!(m.oci_registry_token_rate_limited.get(), 0);
        assert_eq!(m.oci_registry_token_active.get(), 0);
    }
}
