use prometheus::proto::MetricFamily;

pub struct StorageMetricsManager {}

impl StorageMetricsManager {
    /// For every component who reports stats to Prometheus should maintain its own registry
    /// and register itself to ``MetricsManager``. ``MetricsManager`` will gather the stats through
    /// this method
    pub fn hummock_metrics_stats() -> Option<Vec<MetricFamily>> {
        todo!()
    }
}
