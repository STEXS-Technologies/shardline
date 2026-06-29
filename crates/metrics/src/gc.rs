use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

pub struct GcMetrics {
    pub runs: IntCounter,
    pub duration: Histogram,
    pub objects_collected: IntCounter,
    pub bytes_collected: IntCounter,
}

impl GcMetrics {
    pub fn new(registry: &Registry) -> Self {
        let runs = IntCounter::new("shardline_gc_runs_total", "GC runs").unwrap();
        let duration = Histogram::with_opts(HistogramOpts::new("shardline_gc_duration_seconds", "GC duration").buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0])).unwrap();
        let objects_collected = IntCounter::new("shardline_gc_objects_collected_total", "Objects collected by GC").unwrap();
        let bytes_collected = IntCounter::new("shardline_gc_bytes_collected_total", "Bytes collected by GC").unwrap();

        registry.register(Box::new(runs.clone())).ok();
        registry.register(Box::new(duration.clone())).ok();
        registry.register(Box::new(objects_collected.clone())).ok();
        registry.register(Box::new(bytes_collected.clone())).ok();

        Self { runs, duration, objects_collected, bytes_collected }
    }

    pub fn record_run(&self, dur: std::time::Duration, objects: u64, bytes: u64) {
        self.runs.inc();
        self.duration.observe(dur.as_secs_f64());
        self.objects_collected.inc_by(objects);
        self.bytes_collected.inc_by(bytes);
    }
}
