use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

pub struct StorageBackendMetrics {
    pub s3_requests: IntCounter,
    pub s3_duration: Histogram,
    pub s3_errors: IntCounter,
    pub local_io_operations: IntCounter,
    pub local_io_duration: Histogram,
}

impl StorageBackendMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let s3_requests = must_counter("shardline_s3_requests_total", "S3 API requests");
        let s3_duration = must_histogram(
            HistogramOpts::new(
                "shardline_s3_request_duration_seconds",
                "S3 request latency",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
        );
        let s3_errors = must_counter("shardline_s3_errors_total", "S3 API errors");
        let local_io_operations = must_counter(
            "shardline_local_io_operations_total",
            "Local filesystem IO operations",
        );
        let local_io_duration = must_histogram(
            HistogramOpts::new(
                "shardline_local_io_duration_seconds",
                "Local filesystem IO latency",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
            ]),
        );

        registry.register(Box::new(s3_requests.clone())).ok();
        registry.register(Box::new(s3_duration.clone())).ok();
        registry.register(Box::new(s3_errors.clone())).ok();
        registry
            .register(Box::new(local_io_operations.clone()))
            .ok();
        registry.register(Box::new(local_io_duration.clone())).ok();

        Self {
            s3_requests,
            s3_duration,
            s3_errors,
            local_io_operations,
            local_io_duration,
        }
    }

    pub fn record_s3_request(&self, dur: std::time::Duration) {
        self.s3_requests.inc();
        self.s3_duration.observe(dur.as_secs_f64());
    }

    pub fn record_s3_error(&self) {
        self.s3_errors.inc();
    }

    pub fn record_local_io(&self, dur: std::time::Duration) {
        self.local_io_operations.inc();
        self.local_io_duration.observe(dur.as_secs_f64());
    }
}
