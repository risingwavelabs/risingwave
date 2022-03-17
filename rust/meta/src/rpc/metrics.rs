use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response};
use itertools::Itertools;
use prometheus::{
    histogram_opts, register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, CounterVec, Encoder,
    Histogram, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder,
    DEFAULT_BUCKETS,
};
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

pub const BARRIER_BUCKETS: &[f64; 36] = &[
    0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
    0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6,
    2.7, 2.8, 2.9, 3.0, 3.5, 4.0, 5.0,
];
pub struct MetaMetrics {
    registry: Registry,

    /// gRPC latency of meta services
    pub grpc_latency: HistogramVec,
    /// latency of each barrier
    pub barrier_latency: Histogram,
    /// max committed epoch
    pub max_committed_epoch: IntGauge,
    /// num of uncommitted SSTs,
    pub uncommitted_sst_num: IntGauge,
    /// num of SSTs in each level
    pub level_sst_num: IntGaugeVec,
    /// num of SSTs to be merged to next level in each level
    pub level_compact_cnt: IntGaugeVec,
    /// GBs read from current level during history compactions to next level
    pub level_compact_read_curr: CounterVec,
    /// GBs read from next level during history compactions to next level
    pub level_compact_read_next: CounterVec,
    /// GBs written into next level during history compactions to next level
    pub level_compact_write: CounterVec,
    /// num of SSTs read from current level during history compactions to next level
    pub level_compact_read_sstn_curr: IntCounterVec,
    /// num of SSTs read from next level during history compactions to next level
    pub level_compact_read_sstn_next: IntCounterVec,
    /// num of SSTs written into next level during history compactions to next level
    pub level_compact_write_sstn: IntCounterVec,
    /// num of compactions from each level to next level
    pub level_compact_frequence: IntCounterVec,
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

        let buckets = BARRIER_BUCKETS;
        let opts = histogram_opts!(
            "meta_barrier_duration_seconds",
            "barrier latency ",
            buckets.to_vec()
        );
        let barrier_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let max_committed_epoch = register_int_gauge_with_registry!(
            "storage_max_committed_epoch",
            "max committed epoch",
            registry
        )
        .unwrap();

        let uncommitted_sst_num = register_int_gauge_with_registry!(
            "storage_uncommitted_sst_num",
            "num of uncommitted SSTs",
            registry
        )
        .unwrap();

        let level_sst_num = register_int_gauge_vec_with_registry!(
            "storage_level_sst_num",
            "num of SSTs in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_cnt = register_int_gauge_vec_with_registry!(
            "storage_level_compact_cnt",
            "num of SSTs to be merged to next level in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_read_curr = register_counter_vec_with_registry!(
            "storage_level_compact_read_curr",
            "GBs read from current level during history compactions to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_read_next = register_counter_vec_with_registry!(
            "storage_level_compact_read_next",
            "GBs read from next level during history compactions to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_write = register_counter_vec_with_registry!(
            "storage_level_compact_write",
            "GBs written into next level during history compactions to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_read_sstn_curr = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_sstn_curr",
            "num of SSTs read from current level during history compactions to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_read_sstn_next = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_sstn_next",
            "num of SSTs read from next level during history compactions to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_write_sstn = register_int_counter_vec_with_registry!(
            "storage_level_compact_write_sstn",
            "num of SSTs written into next level during history compactions to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_frequence = register_int_counter_vec_with_registry!(
            "storage_level_compact_frequence",
            "num of compactions from each level to next level",
            &["level_index"],
            registry
        )
        .unwrap();

        Self {
            registry,
            grpc_latency,
            barrier_latency,
            max_committed_epoch,
            uncommitted_sst_num,
            level_sst_num,
            level_compact_cnt,
            level_compact_read_curr,
            level_compact_read_next,
            level_compact_write,
            level_compact_read_sstn_curr,
            level_compact_read_sstn_next,
            level_compact_write_sstn,
            level_compact_frequence,
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
impl Default for MetaMetrics {
    fn default() -> Self {
        Self::new()
    }
}
