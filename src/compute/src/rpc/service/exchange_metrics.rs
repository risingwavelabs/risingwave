use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{register_int_counter_vec_with_registry, Registry};

pub struct ExchangeServiceMetrics {
    pub registry: Registry,
    pub stream_exchange_bytes: GenericCounterVec<AtomicU64>,
}

impl ExchangeServiceMetrics {
    pub fn new(registry: Registry) -> Self {
        let stream_exchange_bytes = register_int_counter_vec_with_registry!(
            "stream_exchange_send_size",
            "Total size of messages that have been send to downstream Actor",
            &["up_actor_id", "down_actor_id"],
            registry
        )
        .unwrap();

        Self {
            registry,
            stream_exchange_bytes,
        }
    }

    /// Create a new `ExchangeServiceMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
