use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

pub struct GcMetrics {
    pub runs: IntCounter,
    pub duration: Histogram,
    pub objects_collected: IntCounter,
    pub bytes_collected: IntCounter,
}

impl GcMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let runs = must_counter("shardline_gc_runs_total", "GC runs");
        let duration = must_histogram(
            HistogramOpts::new("shardline_gc_duration_seconds", "GC duration")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]),
        );
        let objects_collected = must_counter(
            "shardline_gc_objects_collected_total",
            "Objects collected by GC",
        );
        let bytes_collected = must_counter(
            "shardline_gc_bytes_collected_total",
            "Bytes collected by GC",
        );

        registry.register(Box::new(runs.clone())).ok();
        registry.register(Box::new(duration.clone())).ok();
        registry.register(Box::new(objects_collected.clone())).ok();
        registry.register(Box::new(bytes_collected.clone())).ok();

        Self {
            runs,
            duration,
            objects_collected,
            bytes_collected,
        }
    }

    pub fn record_run(&self, dur: std::time::Duration, objects: u64, bytes: u64) {
        self.runs.inc();
        self.duration.observe(dur.as_secs_f64());
        self.objects_collected.inc_by(objects);
        self.bytes_collected.inc_by(bytes);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    #[test]
    fn gc_metrics_record_run_increments_counters() {
        let registry = Registry::new();
        let metrics = GcMetrics::new(&registry);

        assert_eq!(metrics.runs.get(), 0);
        assert_eq!(metrics.objects_collected.get(), 0);
        assert_eq!(metrics.bytes_collected.get(), 0);

        metrics.record_run(std::time::Duration::from_secs(1), 5, 100);
        assert_eq!(metrics.runs.get(), 1);
        assert_eq!(metrics.objects_collected.get(), 5);
        assert_eq!(metrics.bytes_collected.get(), 100);

        metrics.record_run(std::time::Duration::from_secs(2), 3, 50);
        assert_eq!(metrics.runs.get(), 2);
        assert_eq!(metrics.objects_collected.get(), 8);
        assert_eq!(metrics.bytes_collected.get(), 150);
    }
}
