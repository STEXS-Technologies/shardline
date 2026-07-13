use prometheus::{IntCounter, IntGauge, Registry};

use crate::{must_counter, must_gauge};

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
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let objects_total = must_gauge("shardline_objects_total", "Total objects stored");
        let objects_bytes_total = must_counter(
            "shardline_objects_bytes_total",
            "Total bytes stored across all objects",
        );
        let chunks_total = must_gauge("shardline_chunks_total", "Total chunks stored");
        let chunks_bytes_total =
            must_counter("shardline_chunks_bytes_total", "Total chunk bytes stored");
        let xorbs_total = must_gauge("shardline_xorbs_total", "Total xorbs stored");
        let xorbs_bytes_total =
            must_counter("shardline_xorbs_bytes_total", "Total xorb bytes stored");
        let shards_total = must_gauge("shardline_shards_total", "Total shards stored");
        let dedup_saves_bytes_total = must_counter(
            "shardline_dedup_saves_bytes_total",
            "Bytes saved by deduplication",
        );

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
