use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

pub struct XetMetrics {
    pub shard_uploads: IntCounter,
    pub shard_upload_bytes: IntCounter,
    pub xorb_uploads: IntCounter,
    pub xorb_upload_bytes: IntCounter,
    pub xorb_downloads: IntCounter,
    pub reconstruction_requests: IntCounter,
    pub reconstruction_duration: Histogram,
    pub reconstruction_chunks: Histogram,
    pub dedupe_shard_queries: IntCounter,
    pub dedupe_shard_hits: IntCounter,
}

impl XetMetrics {
    /// # Panics
    ///
    /// Panics if prometheus metric registration fails (should not happen with static names).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(registry: &Registry) -> Self {
        let shard_uploads = IntCounter::new(
            "shardline_xet_shard_uploads_total",
            "Xet shard metadata uploads",
        )
        .expect("prometheus metric names are static constants");
        let shard_upload_bytes = IntCounter::new(
            "shardline_xet_shard_upload_bytes_total",
            "Xet shard metadata bytes uploaded",
        )
        .expect("prometheus metric names are static constants");
        let xorb_uploads = IntCounter::new("shardline_xet_xorb_uploads_total", "Xet xorb uploads")
            .expect("prometheus metric names are static constants");
        let xorb_upload_bytes = IntCounter::new(
            "shardline_xet_xorb_upload_bytes_total",
            "Xet xorb bytes uploaded",
        )
        .expect("prometheus metric names are static constants");
        let xorb_downloads =
            IntCounter::new("shardline_xet_xorb_downloads_total", "Xet xorb downloads")
                .expect("prometheus metric names are static constants");
        let reconstruction_requests = IntCounter::new(
            "shardline_xet_reconstruction_requests_total",
            "Xet file reconstruction requests",
        )
        .expect("prometheus metric names are static constants");
        let reconstruction_duration = Histogram::with_opts(
            HistogramOpts::new(
                "shardline_xet_reconstruction_duration_seconds",
                "Xet reconstruction latency",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
        )
        .expect("prometheus metric names are static constants");
        let reconstruction_chunks = Histogram::with_opts(
            HistogramOpts::new(
                "shardline_xet_reconstruction_chunks_total",
                "Chunks per Xet reconstruction",
            )
            .buckets(vec![
                1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
            ]),
        )
        .expect("prometheus metric names are static constants");
        let dedupe_shard_queries = IntCounter::new(
            "shardline_xet_dedupe_shard_queries_total",
            "Xet dedupe shard lookups",
        )
        .expect("prometheus metric names are static constants");
        let dedupe_shard_hits = IntCounter::new(
            "shardline_xet_dedupe_shard_hits_total",
            "Xet dedupe shard cache hits",
        )
        .expect("prometheus metric names are static constants");

        registry.register(Box::new(shard_uploads.clone())).ok();
        registry.register(Box::new(shard_upload_bytes.clone())).ok();
        registry.register(Box::new(xorb_uploads.clone())).ok();
        registry.register(Box::new(xorb_upload_bytes.clone())).ok();
        registry.register(Box::new(xorb_downloads.clone())).ok();
        registry
            .register(Box::new(reconstruction_requests.clone()))
            .ok();
        registry
            .register(Box::new(reconstruction_duration.clone()))
            .ok();
        registry
            .register(Box::new(reconstruction_chunks.clone()))
            .ok();
        registry
            .register(Box::new(dedupe_shard_queries.clone()))
            .ok();
        registry.register(Box::new(dedupe_shard_hits.clone())).ok();

        Self {
            shard_uploads,
            shard_upload_bytes,
            xorb_uploads,
            xorb_upload_bytes,
            xorb_downloads,
            reconstruction_requests,
            reconstruction_duration,
            reconstruction_chunks,
            dedupe_shard_queries,
            dedupe_shard_hits,
        }
    }

    pub fn record_shard_upload(&self, bytes: u64) {
        self.shard_uploads.inc();
        self.shard_upload_bytes.inc_by(bytes);
    }

    pub fn record_xorb_upload(&self, bytes: u64) {
        self.xorb_uploads.inc();
        self.xorb_upload_bytes.inc_by(bytes);
    }

    pub fn record_xorb_download(&self, bytes: u64) {
        self.xorb_downloads.inc_by(bytes);
    }

    pub fn record_reconstruction(&self, ok: bool, duration: std::time::Duration, chunks: u64) {
        self.reconstruction_requests.inc();
        self.reconstruction_duration.observe(duration.as_secs_f64());
        self.reconstruction_chunks.observe(chunks as f64);
        if !ok {
            tracing::warn!(chunks, "xet reconstruction failed");
        }
    }

    pub fn record_dedupe_shard_query(&self, hit: bool) {
        self.dedupe_shard_queries.inc();
        if hit {
            self.dedupe_shard_hits.inc();
        }
    }
}
