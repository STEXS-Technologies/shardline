use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

use prometheus::{
    IntCounter, IntGauge, Registry,
    core::{Collector, Desc},
    proto::MetricFamily,
};

use crate::must_counter;
use crate::must_gauge;

pub struct SystemMetrics {
    pub active_connections: IntGauge,
    pub server_uptime: IntGauge,
    uptime_started: Arc<OnceLock<Instant>>,
    pub admitted_total: IntCounter,
    pub queued_total: IntCounter,
    pub rejected_total: IntCounter,
}

struct UptimeCollector {
    gauge: IntGauge,
    started: Arc<OnceLock<Instant>>,
}

fn elapsed_uptime(started: &OnceLock<Instant>, gauge: &IntGauge) -> i64 {
    started.get().map_or_else(
        || gauge.get(),
        |start| i64::try_from(start.elapsed().as_secs()).unwrap_or(i64::MAX),
    )
}

impl Collector for UptimeCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.gauge.set(elapsed_uptime(&self.started, &self.gauge));
        self.gauge.collect()
    }
}

impl SystemMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let active_connections = must_gauge(
            "shardline_active_connections",
            "Current active HTTP connections",
        );
        let server_uptime = must_gauge(
            "shardline_server_uptime_seconds",
            "Server uptime in seconds",
        );
        let admitted_total = must_counter(
            "shardline_admitted_total",
            "Total number of admitted requests (permit granted)",
        );
        let queued_total = must_counter(
            "shardline_queued_total",
            "Total number of queued requests (waited for permit)",
        );
        let rejected_total = must_counter(
            "shardline_rejected_total",
            "Total number of rejected requests (permit denied)",
        );

        registry.register(Box::new(active_connections.clone())).ok();
        let uptime_started = Arc::new(OnceLock::new());
        registry
            .register(Box::new(UptimeCollector {
                gauge: server_uptime.clone(),
                started: uptime_started.clone(),
            }))
            .ok();
        registry.register(Box::new(admitted_total.clone())).ok();
        registry.register(Box::new(queued_total.clone())).ok();
        registry.register(Box::new(rejected_total.clone())).ok();

        Self {
            active_connections,
            server_uptime,
            uptime_started,
            admitted_total,
            queued_total,
            rejected_total,
        }
    }

    pub fn connection_opened(&self) {
        self.active_connections.inc();
    }
    pub fn connection_closed(&self) {
        self.active_connections.dec();
    }
    /// Starts the monotonic server clock once for the lifetime of this metrics instance.
    pub fn start_uptime(&self) {
        self.uptime_started.get_or_init(Instant::now);
    }

    /// Returns fresh monotonic elapsed seconds, independently of Prometheus scrapes.
    /// Before the server clock starts, returns the manually configured gauge value.
    #[must_use]
    pub fn uptime_seconds(&self) -> i64 {
        elapsed_uptime(&self.uptime_started, &self.server_uptime)
    }

    pub fn set_uptime(&self, seconds: i64) {
        self.server_uptime.set(seconds);
    }

    /// Records an admitted request (permit granted).
    pub fn record_admitted(&self) {
        self.admitted_total.inc();
    }

    /// Records a queued request (waited for permit).
    pub fn record_queued(&self) {
        self.queued_total.inc();
    }

    /// Records a rejected request (permit denied).
    pub fn record_rejected(&self) {
        self.rejected_total.inc();
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::SystemMetrics;

    fn new_metrics() -> SystemMetrics {
        SystemMetrics::new(&Registry::new())
    }

    #[test]
    fn connection_opened_increments_gauge() {
        let m = new_metrics();
        m.connection_opened();
        assert_eq!(m.active_connections.get(), 1);
    }

    #[test]
    fn connection_closed_decrements_gauge() {
        let m = new_metrics();
        m.connection_opened();
        m.connection_opened();
        m.connection_closed();
        assert_eq!(m.active_connections.get(), 1);
    }

    #[test]
    fn connection_gauge_can_go_negative() {
        let m = new_metrics();
        m.connection_closed();
        assert_eq!(m.active_connections.get(), -1);
    }

    #[test]
    fn set_uptime_stores_value() {
        let m = new_metrics();
        m.set_uptime(12345);
        assert_eq!(m.server_uptime.get(), 12345);
    }

    #[test]
    fn set_uptime_zero() {
        let m = new_metrics();
        m.set_uptime(0);
        assert_eq!(m.server_uptime.get(), 0);
    }

    #[test]
    fn set_uptime_negative() {
        let m = new_metrics();
        m.set_uptime(-1);
        assert_eq!(m.server_uptime.get(), -1);
    }

    #[test]
    fn set_uptime_overwrites_previous_value() {
        let m = new_metrics();
        m.set_uptime(100);
        m.set_uptime(200);
        assert_eq!(m.server_uptime.get(), 200);
    }

    #[test]
    fn all_gauges_start_at_zero() {
        let m = new_metrics();
        assert_eq!(m.active_connections.get(), 0);
        assert_eq!(m.server_uptime.get(), 0);
    }

    #[test]
    fn record_admitted_increments_counter() {
        let m = new_metrics();
        m.record_admitted();
        assert_eq!(m.admitted_total.get(), 1);
    }

    #[test]
    fn record_queued_increments_counter() {
        let m = new_metrics();
        m.record_queued();
        assert_eq!(m.queued_total.get(), 1);
    }

    #[test]
    fn record_rejected_increments_counter() {
        let m = new_metrics();
        m.record_rejected();
        assert_eq!(m.rejected_total.get(), 1);
    }

    #[test]
    fn all_counters_start_at_zero() {
        let m = new_metrics();
        assert_eq!(m.admitted_total.get(), 0);
        assert_eq!(m.queued_total.get(), 0);
        assert_eq!(m.rejected_total.get(), 0);
    }
}

#[cfg(test)]
mod uptime_tests {
    use super::*;

    #[test]
    fn accessor_and_collector_share_monotonic_clock() {
        let registry = Registry::new();
        let metrics = SystemMetrics::new(&registry);
        metrics
            .uptime_started
            .set(Instant::now() - std::time::Duration::from_secs(5))
            .unwrap();
        // No scrape has updated the public gauge yet, but the accessor is fresh.
        assert_eq!(metrics.server_uptime.get(), 0);
        assert!(metrics.uptime_seconds() >= 5);
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "shardline_server_uptime_seconds")
            .unwrap();
        assert!(family.get_metric().first().unwrap().get_gauge().get_value() >= 5.0);
        assert!(metrics.server_uptime.get() >= 5);
    }

    #[test]
    fn repeated_start_does_not_reset_clock_and_manual_gauge_is_preserved_before_start() {
        let metrics = SystemMetrics::new(&Registry::new());
        metrics.set_uptime(42);
        assert_eq!(metrics.uptime_seconds(), 42);
        let start = Instant::now() - std::time::Duration::from_secs(5);
        metrics.uptime_started.set(start).unwrap();
        metrics.start_uptime();
        assert_eq!(metrics.uptime_started.get(), Some(&start));
        assert!(metrics.uptime_seconds() >= 5);
    }
}
