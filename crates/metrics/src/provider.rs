use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

pub struct ProviderMetrics {
    pub webhook_events: IntCounter,
    pub webhook_duration: Histogram,
    pub token_exchanges: IntCounter,
}

impl ProviderMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let webhook_events = must_counter(
            "shardline_provider_webhook_events_total",
            "Provider webhook events received",
        );
        let webhook_duration = must_histogram(
            HistogramOpts::new(
                "shardline_provider_webhook_processing_duration_seconds",
                "Webhook processing latency",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        );
        let token_exchanges = must_counter(
            "shardline_provider_token_exchange_total",
            "Provider token exchanges",
        );

        registry.register(Box::new(webhook_events.clone())).ok();
        registry.register(Box::new(webhook_duration.clone())).ok();
        registry.register(Box::new(token_exchanges.clone())).ok();

        Self {
            webhook_events,
            webhook_duration,
            token_exchanges,
        }
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

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;

    #[test]
    fn provider_metrics_record_webhook() {
        let registry = Registry::new();
        let metrics = ProviderMetrics::new(&registry);

        assert_eq!(metrics.webhook_events.get(), 0);

        metrics.record_webhook("github", "push");
        assert_eq!(metrics.webhook_events.get(), 1);

        metrics.record_webhook("gitlab", "merge_request");
        assert_eq!(metrics.webhook_events.get(), 2);
    }

    #[test]
    fn provider_metrics_record_webhook_duration() {
        let registry = Registry::new();
        let metrics = ProviderMetrics::new(&registry);

        // No getter for histogram, just verify no panic
        metrics.record_webhook_duration(std::time::Duration::from_millis(150));
    }

    #[test]
    fn provider_metrics_record_token_exchange() {
        let registry = Registry::new();
        let metrics = ProviderMetrics::new(&registry);

        assert_eq!(metrics.token_exchanges.get(), 0);

        metrics.record_token_exchange();
        assert_eq!(metrics.token_exchanges.get(), 1);

        metrics.record_token_exchange();
        assert_eq!(metrics.token_exchanges.get(), 2);
    }
}
