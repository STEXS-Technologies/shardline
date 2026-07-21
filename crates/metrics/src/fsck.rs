use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

pub struct FsckMetrics {
    pub runs: IntCounter,
    pub duration: Histogram,
    pub errors_found: IntCounter,
}

impl FsckMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let runs = must_counter("shardline_fsck_runs_total", "Fsck runs");
        let duration = must_histogram(
            HistogramOpts::new("shardline_fsck_duration_seconds", "Fsck duration")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
        );
        let errors_found = must_counter("shardline_fsck_errors_found_total", "Fsck errors found");

        registry.register(Box::new(runs.clone())).ok();
        registry.register(Box::new(duration.clone())).ok();
        registry.register(Box::new(errors_found.clone())).ok();

        Self {
            runs,
            duration,
            errors_found,
        }
    }

    pub fn record_run(&self, dur: std::time::Duration, errors: u64) {
        self.runs.inc();
        self.duration.observe(dur.as_secs_f64());
        self.errors_found.inc_by(errors);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    #[test]
    fn fsck_metrics_record_run_increments_counters() {
        let registry = Registry::new();
        let metrics = FsckMetrics::new(&registry);

        assert_eq!(metrics.runs.get(), 0);
        assert_eq!(metrics.errors_found.get(), 0);

        metrics.record_run(std::time::Duration::from_secs(5), 3);
        assert_eq!(metrics.runs.get(), 1);
        assert_eq!(metrics.errors_found.get(), 3);

        metrics.record_run(std::time::Duration::from_secs(2), 0);
        assert_eq!(metrics.runs.get(), 2);
        assert_eq!(metrics.errors_found.get(), 3);
    }
}
