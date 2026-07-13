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
