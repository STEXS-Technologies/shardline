use prometheus::{IntGauge, Registry};

pub struct SystemMetrics {
    pub active_connections: IntGauge,
    pub server_uptime: IntGauge,
}

impl SystemMetrics {
    /// # Panics
    ///
    /// Panics if prometheus metric registration fails (should not happen with static names).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(registry: &Registry) -> Self {
        let active_connections = IntGauge::new("shardline_active_connections", "Current active HTTP connections").expect("prometheus metric names are static constants");
        let server_uptime = IntGauge::new("shardline_server_uptime_seconds", "Server uptime in seconds").expect("prometheus metric names are static constants");

        registry.register(Box::new(active_connections.clone())).ok();
        registry.register(Box::new(server_uptime.clone())).ok();

        Self { active_connections, server_uptime }
    }

    pub fn connection_opened(&self) { self.active_connections.inc(); }
    pub fn connection_closed(&self) { self.active_connections.dec(); }
    pub fn set_uptime(&self, seconds: i64) { self.server_uptime.set(seconds); }
}
