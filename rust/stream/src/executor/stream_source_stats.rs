use std::sync::Arc;

use prometheus::{register_int_counter_with_registry, IntCounter, Registry};

pub struct StreamSourceStats {
    pub output_rows_total: IntCounter,
}

lazy_static::lazy_static! {
  pub static ref
  DEFAULT_STREAM_SOURCE_STATS: Arc<StreamSourceStats> = Arc::new(StreamSourceStats::new(prometheus::default_registry()));
}

impl StreamSourceStats {
    fn new(registry: &Registry) -> Self {
        let output_rows_total = register_int_counter_with_registry!(
            "stream_source_output_rows_total",
            "Total number of output rows from all source executors",
            registry
        )
        .unwrap();

        Self { output_rows_total }
    }
}
