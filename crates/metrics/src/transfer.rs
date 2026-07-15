use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

pub struct TransferMetrics {
    pub upload_requests: IntCounter,
    pub upload_bytes: IntCounter,
    pub upload_duration: Histogram,
    pub download_requests: IntCounter,
    pub download_bytes: IntCounter,
    pub download_duration: Histogram,
    pub range_requests: IntCounter,
}

impl TransferMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let upload_requests =
            must_counter("shardline_upload_requests_total", "Total upload requests");
        let upload_bytes = must_counter("shardline_upload_bytes_total", "Total bytes uploaded");
        let upload_duration = must_histogram(
            HistogramOpts::new(
                "shardline_upload_duration_seconds",
                "Upload request duration",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        );
        let download_requests = must_counter(
            "shardline_download_requests_total",
            "Total download requests",
        );
        let download_bytes =
            must_counter("shardline_download_bytes_total", "Total bytes downloaded");
        let download_duration = must_histogram(
            HistogramOpts::new(
                "shardline_download_duration_seconds",
                "Download request duration",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        );
        let range_requests =
            must_counter("shardline_range_requests_total", "Total range download requests");

        registry.register(Box::new(upload_requests.clone())).ok();
        registry.register(Box::new(upload_bytes.clone())).ok();
        registry.register(Box::new(upload_duration.clone())).ok();
        registry.register(Box::new(download_requests.clone())).ok();
        registry.register(Box::new(download_bytes.clone())).ok();
        registry.register(Box::new(download_duration.clone())).ok();
        registry.register(Box::new(range_requests.clone())).ok();

        Self {
            upload_requests,
            upload_bytes,
            upload_duration,
            download_requests,
            download_bytes,
            download_duration,
            range_requests,
        }
    }

    pub fn record_upload(&self, _protocol: &str, bytes: u64) {
        self.upload_requests.inc();
        self.upload_bytes.inc_by(bytes);
    }

    pub fn record_download(&self, _protocol: &str, bytes: u64) {
        self.download_requests.inc();
        self.download_bytes.inc_by(bytes);
    }

    pub fn record_upload_duration(&self, seconds: f64) {
        self.upload_duration.observe(seconds);
    }

    pub fn record_download_duration(&self, seconds: f64) {
        self.download_duration.observe(seconds);
    }

    pub fn record_range_request(&self) {
        self.range_requests.inc();
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    #[test]
    fn transfer_metrics_record_upload() {
        let registry = Registry::new();
        let metrics = TransferMetrics::new(&registry);

        assert_eq!(metrics.upload_requests.get(), 0);
        assert_eq!(metrics.upload_bytes.get(), 0);

        metrics.record_upload("https", 512);
        assert_eq!(metrics.upload_requests.get(), 1);
        assert_eq!(metrics.upload_bytes.get(), 512);

        metrics.record_upload("grpc", 256);
        assert_eq!(metrics.upload_requests.get(), 2);
        assert_eq!(metrics.upload_bytes.get(), 768);
    }

    #[test]
    fn transfer_metrics_record_download() {
        let registry = Registry::new();
        let metrics = TransferMetrics::new(&registry);

        assert_eq!(metrics.download_requests.get(), 0);
        assert_eq!(metrics.download_bytes.get(), 0);

        metrics.record_download("https", 1024);
        assert_eq!(metrics.download_requests.get(), 1);
        assert_eq!(metrics.download_bytes.get(), 1024);
    }

    #[test]
    fn transfer_metrics_record_range_request() {
        let registry = Registry::new();
        let metrics = TransferMetrics::new(&registry);

        assert_eq!(metrics.range_requests.get(), 0);

        metrics.record_range_request();
        assert_eq!(metrics.range_requests.get(), 1);

        metrics.record_range_request();
        assert_eq!(metrics.range_requests.get(), 2);
    }
}
