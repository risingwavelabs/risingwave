use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    histogram_opts, register_histogram_with_registry,register_int_counter_vec_with_registry,
    register_int_counter_with_registry, Encoder, Histogram, HistogramVec, IntGaugeVec, Registry,
    TextEncoder, DEFAULT_BUCKETS,
};
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;


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
    pub fn boot_metrics_service(self: &Arc<Self>, listen_addr: SocketAddr) {
        let meta_metrics = self.clone();
        tokio::spawn(async move {
            tracing::info!(
                "Prometheus listener for Prometheus is set up on http://{}",
                listen_addr
            );

            let service = ServiceBuilder::new()
                .layer(AddExtensionLayer::new(meta_metrics))
                .service_fn(Self::metrics_service);

            let serve_future = hyper::Server::bind(&listen_addr).serve(Shared::new(service));

            if let Err(err) = serve_future.await {
                eprintln!("server error: {}", err);
            }
        });
    }

    async fn metrics_service(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let meta_metrics = req.extensions().get::<Arc<StreamingMetrics>>().unwrap();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = meta_metrics.registry.gather();
        encoder.encode(&mf, &mut buffer).unwrap();
        let response = Response::builder()
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
    
}
impl Default for StreamingMetrics {
    fn default() -> Self {
        Self::new()
    }
}
