use prometheus::{IntGauge, Registry};

use crate::must_gauge;

pub struct SystemMetrics {
    pub active_connections: IntGauge,
    pub server_uptime: IntGauge,
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

        registry.register(Box::new(active_connections.clone())).ok();
        registry.register(Box::new(server_uptime.clone())).ok();

        Self {
            active_connections,
            server_uptime,
        }
    }

    pub fn connection_opened(&self) {
        self.active_connections.inc();
    }
    pub fn connection_closed(&self) {
        self.active_connections.dec();
    }
    pub fn set_uptime(&self, seconds: i64) {
        self.server_uptime.set(seconds);
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
}
