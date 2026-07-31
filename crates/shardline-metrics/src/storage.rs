use prometheus::{IntCounter, IntCounterVec, IntGauge, Registry};

use crate::{must_counter, must_counter_vec, must_gauge};

pub struct StorageMetrics {
    pub objects_total: IntGauge,
    pub objects_bytes_total: IntCounter,
    pub chunks_total: IntGauge,
    pub chunks_bytes_total: IntCounter,
    pub xorbs_total: IntGauge,
    pub xorbs_bytes_total: IntCounter,
    pub shards_total: IntGauge,
    pub dedup_saves_bytes_total: IntCounter,
    pub compression_saved_bytes_total: IntCounter,
    pub objects_by_repr: IntCounterVec,
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
        let compression_saved_bytes_total = must_counter(
            "shardline_compression_saved_bytes_total",
            "Bytes saved by LZ4 compression",
        );
        let objects_by_repr = must_counter_vec(
            prometheus::opts!(
                "shardline_objects_by_repr_total",
                "Objects stored by representation"
            ),
            &["representation"],
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
        registry
            .register(Box::new(compression_saved_bytes_total.clone()))
            .ok();
        registry
            .register(Box::new(objects_by_repr.clone()))
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
            compression_saved_bytes_total,
            objects_by_repr,
        }
    }

    pub fn record_object_stored_by_repr(&self, representation: &str, bytes: u64) {
        self.objects_total.inc();
        self.objects_bytes_total.inc_by(bytes);
        self.objects_by_repr
            .with_label_values(&[representation])
            .inc();
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

    pub fn record_compression_saved(&self, bytes: u64) {
        self.compression_saved_bytes_total.inc_by(bytes);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    #[test]
    fn storage_metrics_record_object_stored() {
        let registry = Registry::new();
        let metrics = StorageMetrics::new(&registry);

        assert_eq!(metrics.objects_total.get(), 0);
        assert_eq!(metrics.objects_bytes_total.get(), 0);

        metrics.record_object_stored(100);
        assert_eq!(metrics.objects_total.get(), 1);
        assert_eq!(metrics.objects_bytes_total.get(), 100);

        metrics.record_object_stored(200);
        assert_eq!(metrics.objects_total.get(), 2);
        assert_eq!(metrics.objects_bytes_total.get(), 300);
    }

    #[test]
    fn storage_metrics_record_chunk_stored() {
        let registry = Registry::new();
        let metrics = StorageMetrics::new(&registry);

        assert_eq!(metrics.chunks_total.get(), 0);
        assert_eq!(metrics.chunks_bytes_total.get(), 0);

        metrics.record_chunk_stored(1024);
        assert_eq!(metrics.chunks_total.get(), 1);
        assert_eq!(metrics.chunks_bytes_total.get(), 1024);
    }

    #[test]
    fn storage_metrics_record_xorb_stored() {
        let registry = Registry::new();
        let metrics = StorageMetrics::new(&registry);

        assert_eq!(metrics.xorbs_total.get(), 0);
        assert_eq!(metrics.xorbs_bytes_total.get(), 0);

        metrics.record_xorb_stored(4096);
        assert_eq!(metrics.xorbs_total.get(), 1);
        assert_eq!(metrics.xorbs_bytes_total.get(), 4096);
    }

    #[test]
    fn storage_metrics_record_shard_stored() {
        let registry = Registry::new();
        let metrics = StorageMetrics::new(&registry);

        assert_eq!(metrics.shards_total.get(), 0);

        metrics.record_shard_stored();
        assert_eq!(metrics.shards_total.get(), 1);

        metrics.record_shard_stored();
        assert_eq!(metrics.shards_total.get(), 2);
    }

    #[test]
    fn storage_metrics_record_dedup_saves() {
        let registry = Registry::new();
        let metrics = StorageMetrics::new(&registry);

        assert_eq!(metrics.dedup_saves_bytes_total.get(), 0);

        metrics.record_dedup_saves(500);
        assert_eq!(metrics.dedup_saves_bytes_total.get(), 500);

        metrics.record_dedup_saves(300);
        assert_eq!(metrics.dedup_saves_bytes_total.get(), 800);
    }

    #[test]
    fn storage_metrics_record_compression_saved() {
        let registry = Registry::new();
        let metrics = StorageMetrics::new(&registry);

        assert_eq!(metrics.compression_saved_bytes_total.get(), 0);

        metrics.record_compression_saved(1000);
        assert_eq!(metrics.compression_saved_bytes_total.get(), 1000);

        metrics.record_compression_saved(500);
        assert_eq!(metrics.compression_saved_bytes_total.get(), 1500);
    }
}
