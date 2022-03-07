use std::net::SocketAddr;
use std::sync::Arc;

// use hyper::{Body, Request, Response};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    histogram_opts, register_histogram_with_registry,register_int_counter_vec_with_registry,
    register_int_counter_with_registry, Encoder, Histogram, HistogramVec, IntGaugeVec, Registry,
    TextEncoder, DEFAULT_BUCKETS,
};
// use tower::make::Shared;
// use tower::ServiceBuilder;
// use tower_http::add_extension::AddExtensionLayer;


pub struct StreamingMetrics {
    registry: Registry,

    /// gRPC latency of meta services
    pub actor_row_count: GenericCounterVec<AtomicU64>,
    /// latency of each barrier
    pub source_output_row_count: GenericCounterVec<AtomicU64>,

}

impl StreamingMetrics {
    pub fn new() -> Self {
        let registry = prometheus::Registry::new();
        let actor_row_count = register_int_counter_vec_with_registry!(
            "stream_actor_row_count2333",
            "Total number of rows that have been ouput from each actor",
            &["actor_id"],
            registry
        )
        .unwrap();

        let source_output_row_count = register_int_counter_vec_with_registry!(
            "stream_source_output_rows_counts2333",
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
impl Default for StreamingMetrics {
    fn default() -> Self {
        Self::new()
    }
}
