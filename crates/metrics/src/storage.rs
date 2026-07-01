use prometheus::{IntCounter, IntGauge, Registry};

pub struct StorageMetrics {
    pub objects_total: IntGauge,
    pub objects_bytes_total: IntCounter,
    pub chunks_total: IntGauge,
    pub chunks_bytes_total: IntCounter,
    pub xorbs_total: IntGauge,
    pub xorbs_bytes_total: IntCounter,
    pub shards_total: IntGauge,
    pub dedup_saves_bytes_total: IntCounter,
}

impl StorageMetrics {
    /// # Panics
    ///
    /// Panics if prometheus metric registration fails (should not happen with static names).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(registry: &Registry) -> Self {
        let objects_total = IntGauge::new("shardline_objects_total", "Total objects stored")
            .expect("prometheus metric names are static constants");
        let objects_bytes_total = IntCounter::new(
            "shardline_objects_bytes_total",
            "Total bytes stored across all objects",
        )
        .expect("prometheus metric names are static constants");
        let chunks_total = IntGauge::new("shardline_chunks_total", "Total chunks stored")
            .expect("prometheus metric names are static constants");
        let chunks_bytes_total =
            IntCounter::new("shardline_chunks_bytes_total", "Total chunk bytes stored")
                .expect("prometheus metric names are static constants");
        let xorbs_total = IntGauge::new("shardline_xorbs_total", "Total xorbs stored")
            .expect("prometheus metric names are static constants");
        let xorbs_bytes_total =
            IntCounter::new("shardline_xorbs_bytes_total", "Total xorb bytes stored")
                .expect("prometheus metric names are static constants");
        let shards_total = IntGauge::new("shardline_shards_total", "Total shards stored")
            .expect("prometheus metric names are static constants");
        let dedup_saves_bytes_total = IntCounter::new(
            "shardline_dedup_saves_bytes_total",
            "Bytes saved by deduplication",
        )
        .expect("prometheus metric names are static constants");

        registry.register(Box::new(objects_total.clone())).ok();
        registry
            .register(Box::new(objects_bytes_total.clone()))
            .ok();
        registry.register(Box::new(chunks_total.clone())).ok();
        registry.register(Box::new(chunks_bytes_total.clone())).ok();
        registry.register(Box::new(xorbs_total.clone())).ok();
        registry.register(Box::new(xorbs_bytes_total.clone())).ok();
        registry.register(Box::new(shards_total.clone())).ok();
        registry
            .register(Box::new(dedup_saves_bytes_total.clone()))
            .ok();

        Self {
            objects_total,
            objects_bytes_total,
            chunks_total,
            chunks_bytes_total,
            xorbs_total,
            xorbs_bytes_total,
            shards_total,
            dedup_saves_bytes_total,
        }
    }

    pub fn record_object_stored(&self, bytes: u64) {
        self.objects_total.inc();
        self.objects_bytes_total.inc_by(bytes);
    }

    pub fn record_chunk_stored(&self, bytes: u64) {
        self.chunks_total.inc();
        self.chunks_bytes_total.inc_by(bytes);
    }

    pub fn record_xorb_stored(&self, bytes: u64) {
        self.xorbs_total.inc();
        self.xorbs_bytes_total.inc_by(bytes);
    }

    pub fn record_shard_stored(&self) {
        self.shards_total.inc();
    }

    pub fn record_dedup_saves(&self, bytes: u64) {
        self.dedup_saves_bytes_total.inc_by(bytes);
    }
}
