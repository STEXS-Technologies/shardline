use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry};

use crate::{must_counter, must_histogram};

/// Metrics for bounded dataset query execution. No tenant, path, SQL, or row
/// values are included in these metrics.
pub struct QueryMetrics {
    pub requests: IntCounter,
    pub admissions_rejected: IntCounter,
    pub scan_limit_rejected: IntCounter,
    pub result_limit_rejected: IntCounter,
    pub cancellations: IntCounter,
    pub cancellation_reasons: IntCounterVec,
    pub returned_rows: IntCounter,
    pub returned_bytes: IntCounter,
    pub scanned_bytes: IntCounter,
    pub range_requests: IntCounter,
    pub failures: IntCounter,
    pub failure_classes: IntCounterVec,
    pub queue_seconds: Histogram,
    pub execution_seconds: Histogram,
}

impl QueryMetrics {
    #[must_use]
    pub fn new(registry: &Registry) -> Self {
        let requests = must_counter("shardline_query_requests_total", "Bounded dataset queries");
        let admissions_rejected = must_counter(
            "shardline_query_admission_rejected_total",
            "Queries rejected by concurrency admission",
        );
        let scan_limit_rejected = must_counter(
            "shardline_query_scan_limit_rejected_total",
            "Queries rejected by scanned-byte limits",
        );
        let result_limit_rejected = must_counter(
            "shardline_query_result_limit_rejected_total",
            "Queries rejected by result limits",
        );
        let cancellations = must_counter(
            "shardline_query_cancellations_total",
            "Queries cancelled by deadline or client failure",
        );
        let cancellation_reasons = IntCounterVec::new(
            Opts::new(
                "shardline_query_cancellations_by_reason_total",
                "Bounded query cancellations by stable reason",
            ),
            &["reason"],
        )
        .unwrap_or_else(|_| std::process::abort());
        let returned_rows = must_counter(
            "shardline_query_returned_rows_total",
            "Rows returned by bounded queries",
        );
        let returned_bytes = must_counter(
            "shardline_query_returned_bytes_total",
            "Result bytes returned by bounded queries",
        );
        let scanned_bytes = must_counter(
            "shardline_query_scanned_bytes_total",
            "Bytes fetched by bounded queries",
        );
        let range_requests = must_counter(
            "shardline_query_range_requests_total",
            "Object-store range requests issued by bounded queries",
        );
        let failures = must_counter(
            "shardline_query_failures_total",
            "Bounded queries that failed after admission",
        );
        let failure_classes = IntCounterVec::new(
            Opts::new(
                "shardline_query_failures_by_class_total",
                "Bounded query failures by stable non-sensitive class",
            ),
            &["class"],
        )
        .unwrap_or_else(|_| std::process::abort());
        let queue_seconds = must_histogram(HistogramOpts::new(
            "shardline_query_queue_seconds",
            "Time spent waiting for bounded query admission",
        ));
        let execution_seconds = must_histogram(HistogramOpts::new(
            "shardline_query_execution_seconds",
            "Bounded query execution time",
        ));
        for metric in [
            &requests,
            &admissions_rejected,
            &scan_limit_rejected,
            &result_limit_rejected,
            &cancellations,
            &returned_rows,
            &returned_bytes,
            &scanned_bytes,
            &range_requests,
            &failures,
        ] {
            registry.register(Box::new((*metric).clone())).ok();
        }
        registry.register(Box::new(execution_seconds.clone())).ok();
        registry.register(Box::new(queue_seconds.clone())).ok();
        registry.register(Box::new(failure_classes.clone())).ok();
        registry
            .register(Box::new(cancellation_reasons.clone()))
            .ok();
        Self {
            requests,
            admissions_rejected,
            scan_limit_rejected,
            result_limit_rejected,
            cancellations,
            cancellation_reasons,
            returned_rows,
            returned_bytes,
            scanned_bytes,
            range_requests,
            failures,
            failure_classes,
            queue_seconds,
            execution_seconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryMetrics;
    use prometheus::Registry;

    #[test]
    fn query_metrics_record_limits_and_results() {
        let metrics = QueryMetrics::new(&Registry::new());
        metrics.requests.inc();
        metrics.scan_limit_rejected.inc();
        metrics.returned_rows.inc_by(3);
        assert_eq!(metrics.requests.get(), 1);
        assert_eq!(metrics.scan_limit_rejected.get(), 1);
        assert_eq!(metrics.returned_rows.get(), 3);
    }
}
