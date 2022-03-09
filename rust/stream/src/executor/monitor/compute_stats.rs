use std::sync::Arc;

use opentelemetry_prometheus::{self, PrometheusExporter};
pub struct ComputeStats {
    pub prometheus_exporter: PrometheusExporter,
}

lazy_static::lazy_static! {
    pub static ref
        DEFAULT_COMPUTE_STATS: Arc<ComputeStats> = Arc::new(ComputeStats::new());
}

impl Default for ComputeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ComputeStats {
    pub fn new() -> Self {
        let prometheus_exporter = opentelemetry_prometheus::exporter()
            .with_registry(prometheus::default_registry().clone())
            .init();
        Self {
            prometheus_exporter,
        }
    }
}
