use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

pub struct ReconstructionMetrics {
    pub requests: IntCounter,
    pub duration: Histogram,
    pub chunks_fetched: IntCounter,
    pub cache_hits: IntCounter,
    pub cache_misses: IntCounter,
}

impl ReconstructionMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let requests = must_counter(
            "shardline_reconstruction_requests_total",
            "Total reconstruction requests",
        );
        let duration = must_histogram(
            HistogramOpts::new(
                "shardline_reconstruction_duration_seconds",
                "Reconstruction latency",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        );
        let chunks_fetched = must_counter(
            "shardline_reconstruction_chunks_fetched_total",
            "Total chunks fetched for reconstructions",
        );
        let cache_hits = must_counter(
            "shardline_reconstruction_cache_hits_total",
            "Reconstruction cache hits",
        );
        let cache_misses = must_counter(
            "shardline_reconstruction_cache_misses_total",
            "Reconstruction cache misses",
        );

        registry.register(Box::new(requests.clone())).ok();
        registry.register(Box::new(duration.clone())).ok();
        registry.register(Box::new(chunks_fetched.clone())).ok();
        registry.register(Box::new(cache_hits.clone())).ok();
        registry.register(Box::new(cache_misses.clone())).ok();

        Self {
            requests,
            duration,
            chunks_fetched,
            cache_hits,
            cache_misses,
        }
    }

    pub fn record(&self, _ok: bool, dur: std::time::Duration, chunks: u64) {
        self.requests.inc();
        self.duration.observe(dur.as_secs_f64());
        self.chunks_fetched.inc_by(chunks);
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.inc();
    }
    pub fn record_cache_miss(&self) {
        self.cache_misses.inc();
    }
}
