use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, TextEncoder,Registry};
use risingwave_batch::rpc::service::task_service::BatchServiceImpl;
use risingwave_batch::task::{BatchEnvironment, BatchManager};
use risingwave_common::config::ComputeNodeConfig;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_rpc_client::MetaClient;
use risingwave_source::MemSourceManager;
use risingwave_storage::hummock::compactor::Compactor;
use risingwave_storage::monitor::DEFAULT_STATE_STORE_STATS;
use risingwave_storage::StateStoreImpl;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::{StreamEnvironment, StreamManager};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

use crate::rpc::service::exchange_service::ExchangeServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::ComputeNodeOpts;

fn load_config(opts: &ComputeNodeOpts) -> ComputeNodeConfig {
    if opts.config_path.is_empty() {
        return ComputeNodeConfig::default();
    }

    let config_path = PathBuf::from(opts.config_path.to_owned());
    ComputeNodeConfig::init(config_path).unwrap()
}

/// Bootstraps the compute-node.
pub async fn compute_node_serve(
    addr: SocketAddr,
    opts: ComputeNodeOpts,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    // Load the configuration.
    let config = load_config(&opts);
    let mut meta_client = MetaClient::new(&opts.meta_address).await.unwrap();

    // Register to the cluster. We're not ready to serve until activate is called.
    let worker_id = meta_client
        .register(addr, WorkerType::ComputeNode)
        .await
        .unwrap();

    // Initialize state store.
    let stats = DEFAULT_STATE_STORE_STATS.clone();
    let storage_config = Arc::new(config.storage.clone());
    let state_store = StateStoreImpl::new(
        &opts.state_store,
        storage_config,
        meta_client.clone(),
        stats.clone(),
    )
    .await
    .unwrap();

    // A hummock compactor is deployed along with compute node for now.
    let mut compactor_handle = None;
    if let StateStoreImpl::HummockStateStore(hummock) = state_store.clone() {
        let (compactor_join_handle, compactor_shutdown_sender) = Compactor::start_compactor(
            hummock.inner().storage.options().clone(),
            hummock.inner().storage.local_version_manager().clone(),
            hummock.inner().storage.hummock_meta_client().clone(),
            hummock.inner().storage.sstable_store(),
            stats,
        );
        compactor_handle = Some((compactor_join_handle, compactor_shutdown_sender));
    }

    let registry = prometheus::Registry::new();
    // Initialize the managers.
    let batch_mgr = Arc::new(BatchManager::new());
    let stream_mgr = Arc::new(StreamManager::new(addr, state_store.clone(), registry.clone()));
    let source_mgr = Arc::new(MemSourceManager::new());

    // Initialize batch environment.
    let batch_config = Arc::new(config.batch.clone());
    let batch_env = BatchEnvironment::new(
        source_mgr.clone(),
        batch_mgr.clone(),
        addr,
        batch_config,
        worker_id,
        state_store.clone(),
    );

    // Initialize the streaming environment.
    let stream_config = Arc::new(config.streaming.clone());
    let stream_env =
        StreamEnvironment::new(source_mgr, addr, stream_config, worker_id, state_store);

    // Boot the runtime gRPC services.
    let batch_srv = BatchServiceImpl::new(batch_mgr.clone(), batch_env);
    let exchange_srv = ExchangeServiceImpl::new(batch_mgr, stream_mgr.clone());
    let stream_srv = StreamServiceImpl::new(stream_mgr, stream_env.clone());

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TaskServiceServer::new(batch_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .serve_with_shutdown(addr, async move {
                tokio::select! {
                  _ = tokio::signal::ctrl_c() => {},
                  _ = shutdown_recv.recv() => {
                        // Gracefully shutdown compactor
                        if let Some((compactor_join_handle, compactor_shutdown_sender)) = compactor_handle {
                            if compactor_shutdown_sender.send(()).is_ok() {
                                compactor_join_handle.await.unwrap();
                            }
                        }
                    },
                }
            })
            .await
            .unwrap();
    });

    // Boot metrics service.
    if opts.metrics_level > 0 {
        MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone(), registry);
    }

    // All set, let the meta service know we're ready.
    meta_client.activate(addr).await.unwrap();

    (join_handle, shutdown_send)
}

pub struct MetricsManager {}

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String, registry: Registry) {
        tokio::spawn(async move {
            info!(
                "Prometheus listener for Prometheus is set up on http://{}",
                listen_addr
            );
            let listen_socket_addr: SocketAddr = listen_addr.parse().unwrap();
            let streaming_metrics = Arc::new(StreamingMetrics::new(registry.clone()));
            let service = ServiceBuilder::new()
            .layer(AddExtensionLayer::new(streaming_metrics))
            .service_fn(Self::metrics_service);
            let serve_future = hyper::Server::bind(&listen_socket_addr).serve(Shared::new(service));
            // let serve_future =
            //     Server::bind(&listen_socket_addr).serve(make_service_fn(|_| async {
            //         Ok::<_, hyper::Error>(service_fn(MetricsManager::metrics_service))
            //     }));

            if let Err(err) = serve_future.await {
                eprintln!("server error: {}", err);
            }
        });
    }

    async fn metrics_service(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let streaming_metrics = req.extensions().get::<Arc<StreamingMetrics>>().unwrap();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = streaming_metrics.registry.gather();
        encoder.encode(&mf, &mut buffer).unwrap();
        let response = Response::builder()
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
}
