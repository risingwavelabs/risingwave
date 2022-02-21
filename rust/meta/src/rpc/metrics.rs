use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response};
use itertools::Itertools;
use prometheus::{
    histogram_opts, register_histogram_vec_with_registry, Encoder, HistogramVec, Registry,
    TextEncoder, DEFAULT_BUCKETS,
};
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

pub struct MetaMetrics {
    registry: Registry,

    /// gRPC latency of meta services
    pub grpc_latency: HistogramVec,
}

impl MetaMetrics {
    pub fn new() -> Self {
        let registry = prometheus::Registry::new();
        let buckets = DEFAULT_BUCKETS;
        let opts = histogram_opts!(
            "meta_grpc_duration_seconds",
            "gRPC latency of meta services",
            buckets.iter().map(|x| *x * 0.1).collect_vec()
        );
        let grpc_latency =
            register_histogram_vec_with_registry!(opts, &["path"], registry).unwrap();
        Self {
            registry,
            grpc_latency,
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
        let meta_metrics = req.extensions().get::<Arc<MetaMetrics>>().unwrap();
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
