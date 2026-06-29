use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

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
    pub fn new(registry: &Registry) -> Self {
        let upload_requests = IntCounter::new("shardline_upload_requests_total", "Total upload requests").unwrap();
        let upload_bytes = IntCounter::new("shardline_upload_bytes_total", "Total bytes uploaded").unwrap();
        let upload_duration = Histogram::with_opts(HistogramOpts::new("shardline_upload_duration_seconds", "Upload request duration").buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])).unwrap();
        let download_requests = IntCounter::new("shardline_download_requests_total", "Total download requests").unwrap();
        let download_bytes = IntCounter::new("shardline_download_bytes_total", "Total bytes downloaded").unwrap();
        let download_duration = Histogram::with_opts(HistogramOpts::new("shardline_download_duration_seconds", "Download request duration").buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])).unwrap();
        let range_requests = IntCounter::new("shardline_range_requests_total", "Total range download requests").unwrap();

        registry.register(Box::new(upload_requests.clone())).ok();
        registry.register(Box::new(upload_bytes.clone())).ok();
        registry.register(Box::new(upload_duration.clone())).ok();
        registry.register(Box::new(download_requests.clone())).ok();
        registry.register(Box::new(download_bytes.clone())).ok();
        registry.register(Box::new(download_duration.clone())).ok();
        registry.register(Box::new(range_requests.clone())).ok();

        Self { upload_requests, upload_bytes, upload_duration, download_requests, download_bytes, download_duration, range_requests }
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
