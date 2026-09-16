use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};

use crate::{must_counter, must_histogram};

/// Metrics for bounded dataset query execution. No tenant, path, SQL, or row
/// values are included in these metrics.
pub struct QueryMetrics {
    pub requests: IntCounter,
    pub admissions_rejected: IntCounter,
    pub scan_limit_rejected: IntCounter,
    pub result_limit_rejected: IntCounter,
    pub cancellations: IntCounter,
    pub returned_rows: IntCounter,
    pub returned_bytes: IntCounter,
    pub scanned_bytes: IntCounter,
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
        ] {
            registry.register(Box::new((*metric).clone())).ok();
        }
        registry.register(Box::new(execution_seconds.clone())).ok();
        Self {
            requests,
            admissions_rejected,
            scan_limit_rejected,
            result_limit_rejected,
            cancellations,
            returned_rows,
            returned_bytes,
            scanned_bytes,
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
