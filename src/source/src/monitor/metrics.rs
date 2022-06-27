use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{register_int_counter_vec_with_registry, Registry};

pub struct SourceMetrics {
    pub registry: Registry,
    pub partition_input_count: GenericCounterVec<AtomicU64>,
}

impl SourceMetrics {
    pub fn new(registry: Registry) -> Self {
        let partition_input_count = register_int_counter_vec_with_registry!(
            "partition_input_count",
            "Total number of rows that have been input from specific parition",
            &["actor_id", "source_id", "partition"],
            registry
        )
        .unwrap();
        SourceMetrics {
            registry,
            partition_input_count,
        }
    }

    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
