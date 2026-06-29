use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

pub struct FsckMetrics {
    pub runs: IntCounter,
    pub duration: Histogram,
    pub errors_found: IntCounter,
}

impl FsckMetrics {
    pub fn new(registry: &Registry) -> Self {
        let runs = IntCounter::new("shardline_fsck_runs_total", "Fsck runs").unwrap();
        let duration = Histogram::with_opts(HistogramOpts::new("shardline_fsck_duration_seconds", "Fsck duration").buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0])).unwrap();
        let errors_found = IntCounter::new("shardline_fsck_errors_found_total", "Fsck errors found").unwrap();

        registry.register(Box::new(runs.clone())).ok();
        registry.register(Box::new(duration.clone())).ok();
        registry.register(Box::new(errors_found.clone())).ok();

        Self { runs, duration, errors_found }
    }

    pub fn record_run(&self, dur: std::time::Duration, errors: u64) {
        self.runs.inc();
        self.duration.observe(dur.as_secs_f64());
        self.errors_found.inc_by(errors);
    }
}
