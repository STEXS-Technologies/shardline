use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

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
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let shard_uploads = must_counter(
            "shardline_xet_shard_uploads_total",
            "Xet shard metadata uploads",
        );
        let shard_upload_bytes = must_counter(
            "shardline_xet_shard_upload_bytes_total",
            "Xet shard metadata bytes uploaded",
        );
        let xorb_uploads = must_counter("shardline_xet_xorb_uploads_total", "Xet xorb uploads");
        let xorb_upload_bytes = must_counter(
            "shardline_xet_xorb_upload_bytes_total",
            "Xet xorb bytes uploaded",
        );
        let xorb_downloads =
            must_counter("shardline_xet_xorb_downloads_total", "Xet xorb downloads");
        let reconstruction_requests = must_counter(
            "shardline_xet_reconstruction_requests_total",
            "Xet file reconstruction requests",
        );
        let reconstruction_duration = must_histogram(
            HistogramOpts::new(
                "shardline_xet_reconstruction_duration_seconds",
                "Xet reconstruction latency",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
        );
        let reconstruction_chunks = must_histogram(
            HistogramOpts::new(
                "shardline_xet_reconstruction_chunks_total",
                "Chunks per Xet reconstruction",
            )
            .buckets(vec![
                1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
            ]),
        );
        let dedupe_shard_queries = must_counter(
            "shardline_xet_dedupe_shard_queries_total",
            "Xet dedupe shard lookups",
        );
        let dedupe_shard_hits = must_counter(
            "shardline_xet_dedupe_shard_hits_total",
            "Xet dedupe shard cache hits",
        );

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

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::XetMetrics;

    fn new_metrics() -> XetMetrics {
        XetMetrics::new(&Registry::new())
    }

    #[test]
    fn record_shard_upload_increments_both_counters() {
        let m = new_metrics();
        m.record_shard_upload(42);
        assert_eq!(m.shard_uploads.get(), 1);
        assert_eq!(m.shard_upload_bytes.get(), 42);
    }

    #[test]
    fn record_shard_upload_zero_bytes() {
        let m = new_metrics();
        m.record_shard_upload(0);
        assert_eq!(m.shard_uploads.get(), 1);
        assert_eq!(m.shard_upload_bytes.get(), 0);
    }

    #[test]
    fn record_xorb_upload_increments_both_counters() {
        let m = new_metrics();
        m.record_xorb_upload(100);
        assert_eq!(m.xorb_uploads.get(), 1);
        assert_eq!(m.xorb_upload_bytes.get(), 100);
    }

    #[test]
    fn record_xorb_upload_zero_bytes() {
        let m = new_metrics();
        m.record_xorb_upload(0);
        assert_eq!(m.xorb_uploads.get(), 1);
        assert_eq!(m.xorb_upload_bytes.get(), 0);
    }

    #[test]
    fn record_xorb_download_increments_bytes() {
        let m = new_metrics();
        m.record_xorb_download(256);
        assert_eq!(m.xorb_downloads.get(), 256);
    }

    #[test]
    fn record_xorb_download_zero() {
        let m = new_metrics();
        m.record_xorb_download(0);
        assert_eq!(m.xorb_downloads.get(), 0);
    }

    #[test]
    fn record_reconstruction_ok_increments_request_and_observes_histograms() {
        let m = new_metrics();
        m.record_reconstruction(true, std::time::Duration::from_millis(50), 5);
        assert_eq!(m.reconstruction_requests.get(), 1);
    }

    #[test]
    fn record_reconstruction_failure_increments_request() {
        let m = new_metrics();
        m.record_reconstruction(false, std::time::Duration::from_secs(1), 10);
        assert_eq!(m.reconstruction_requests.get(), 1);
    }

    #[test]
    fn record_reconstruction_multiple_calls_accumulate() {
        let m = new_metrics();
        m.record_reconstruction(true, std::time::Duration::from_millis(10), 2);
        m.record_reconstruction(true, std::time::Duration::from_millis(20), 3);
        assert_eq!(m.reconstruction_requests.get(), 2);
    }

    #[test]
    fn record_dedupe_shard_query_hit_increments_both() {
        let m = new_metrics();
        m.record_dedupe_shard_query(true);
        assert_eq!(m.dedupe_shard_queries.get(), 1);
        assert_eq!(m.dedupe_shard_hits.get(), 1);
    }

    #[test]
    fn record_dedupe_shard_query_miss_only_queries() {
        let m = new_metrics();
        m.record_dedupe_shard_query(false);
        assert_eq!(m.dedupe_shard_queries.get(), 1);
        assert_eq!(m.dedupe_shard_hits.get(), 0);
    }

    #[test]
    fn all_counters_start_at_zero() {
        let m = new_metrics();
        assert_eq!(m.shard_uploads.get(), 0);
        assert_eq!(m.shard_upload_bytes.get(), 0);
        assert_eq!(m.xorb_uploads.get(), 0);
        assert_eq!(m.xorb_upload_bytes.get(), 0);
        assert_eq!(m.xorb_downloads.get(), 0);
        assert_eq!(m.reconstruction_requests.get(), 0);
        assert_eq!(m.dedupe_shard_queries.get(), 0);
        assert_eq!(m.dedupe_shard_hits.get(), 0);
    }
}
