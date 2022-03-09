use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{register_int_counter_vec_with_registry, Registry};

pub struct StreamingMetrics {
    pub registry: Registry,
    pub actor_row_count: GenericCounterVec<AtomicU64>,

    pub source_output_row_count: GenericCounterVec<AtomicU64>,
}

impl StreamingMetrics {
    pub fn new(registry: Registry) -> Self {
        let actor_row_count = register_int_counter_vec_with_registry!(
            "stream_actor_row_count",
            "Total number of rows that have been ouput from each actor",
            &["actor_id"],
            registry
        )
        .unwrap();

        let source_output_row_count = register_int_counter_vec_with_registry!(
            "stream_source_output_rows_counts",
            "xxx",
            &["source_id"],
            registry
        )
        .unwrap();

        Self {
            registry,
            actor_row_count,
            source_output_row_count,
        }
    }
}
