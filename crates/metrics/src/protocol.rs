use prometheus::{IntCounter, IntGauge, Registry};

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
    /// # Panics
    ///
    /// Panics if prometheus metric registration fails (should not happen with static names).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(registry: &Registry) -> Self {
        let lfs_uploads = IntCounter::new(
            "shardline_lfs_upload_requests_total",
            "Git LFS upload requests",
        )
        .expect("prometheus metric names are static constants");
        let lfs_downloads = IntCounter::new(
            "shardline_lfs_download_requests_total",
            "Git LFS download requests",
        )
        .expect("prometheus metric names are static constants");
        let oci_uploads =
            IntCounter::new("shardline_oci_upload_requests_total", "OCI upload requests")
                .expect("prometheus metric names are static constants");
        let oci_downloads = IntCounter::new(
            "shardline_oci_download_requests_total",
            "OCI download requests",
        )
        .expect("prometheus metric names are static constants");
        let hub_api_requests =
            IntCounter::new("shardline_hub_api_requests_total", "Hub API requests")
                .expect("prometheus metric names are static constants");
        let hub_api_commits = IntCounter::new(
            "shardline_hub_api_commit_operations_total",
            "Hub API commit operations",
        )
        .expect("prometheus metric names are static constants");
        let hub_api_file_uploads = IntCounter::new(
            "shardline_hub_api_file_uploads_total",
            "Hub API file uploads",
        )
        .expect("prometheus metric names are static constants");
        let hub_api_file_downloads = IntCounter::new(
            "shardline_hub_api_file_downloads_total",
            "Hub API file downloads",
        )
        .expect("prometheus metric names are static constants");
        let oci_registry_token_requests = IntCounter::new(
            "shardline_oci_registry_token_requests_total",
            "OCI registry token requests",
        )
        .expect("prometheus metric names are static constants");
        let oci_registry_token_rate_limited = IntCounter::new(
            "shardline_oci_registry_token_rate_limited_total",
            "OCI registry token requests rate limited",
        )
        .expect("prometheus metric names are static constants");
        let oci_registry_token_active = IntGauge::new(
            "shardline_oci_registry_token_active_requests",
            "Active OCI registry token requests",
        )
        .expect("prometheus metric names are static constants");

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
