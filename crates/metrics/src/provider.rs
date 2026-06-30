use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

pub struct ProviderMetrics {
    pub webhook_events: IntCounter,
    pub webhook_duration: Histogram,
    pub token_exchanges: IntCounter,
}

impl ProviderMetrics {
    /// # Panics
    ///
    /// Panics if prometheus metric registration fails (should not happen with static names).
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(registry: &Registry) -> Self {
        let webhook_events = IntCounter::new("shardline_provider_webhook_events_total", "Provider webhook events received").expect("prometheus metric names are static constants");
        let webhook_duration = Histogram::with_opts(HistogramOpts::new("shardline_provider_webhook_processing_duration_seconds", "Webhook processing latency").buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0])).expect("prometheus metric names are static constants");
        let token_exchanges = IntCounter::new("shardline_provider_token_exchange_total", "Provider token exchanges").expect("prometheus metric names are static constants");

        registry.register(Box::new(webhook_events.clone())).ok();
        registry.register(Box::new(webhook_duration.clone())).ok();
        registry.register(Box::new(token_exchanges.clone())).ok();

        Self { webhook_events, webhook_duration, token_exchanges }
    }

    pub fn record_webhook(&self, _provider: &str, _event_type: &str) {
        self.webhook_events.inc();
    }

    pub fn record_webhook_duration(&self, dur: std::time::Duration) {
        self.webhook_duration.observe(dur.as_secs_f64());
    }

    pub fn record_token_exchange(&self) {
        self.token_exchanges.inc();
    }
}
