// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Body, Request, Response};
use prometheus::{Encoder, Registry, TextEncoder};
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
use risingwave_storage::monitor::{HummockMetrics, StateStoreMetrics};
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
    info!("Starting compute node with config {:?}", config);
    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();

    let mut meta_client = MetaClient::new(&opts.meta_address).await.unwrap();

    // Register to the cluster. We're not ready to serve until activate is called.
    let worker_id = meta_client
        .register(addr, WorkerType::ComputeNode)
        .await
        .unwrap();
    info!("Assigned worker node id {}", worker_id);

    let mut sub_tasks: Vec<(JoinHandle<()>, UnboundedSender<()>)> =
        vec![MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval as u64),
        )];

    let registry = prometheus::Registry::new();
    // Initialize state store.
    let state_store_metrics = Arc::new(StateStoreMetrics::new(registry.clone()));
    let hummock_metrics = Arc::new(HummockMetrics::new(registry.clone()));
    let storage_config = Arc::new(config.storage.clone());
    let state_store = StateStoreImpl::new(
        &opts.state_store,
        storage_config,
        meta_client.clone(),
        state_store_metrics.clone(),
        hummock_metrics.clone(),
    )
    .await
    .unwrap();

    // A hummock compactor is deployed along with compute node for now.
    if let StateStoreImpl::HummockStateStore(hummock) = state_store.clone() {
        sub_tasks.push(Compactor::start_compactor(
            hummock.inner().storage.options().clone(),
            hummock.inner().storage.local_version_manager().clone(),
            hummock.inner().storage.hummock_meta_client().clone(),
            hummock.inner().storage.sstable_store(),
            state_store_metrics,
        ));
    }

    let streaming_metrics = Arc::new(StreamingMetrics::new(registry.clone()));
    // Initialize the managers.
    let batch_mgr = Arc::new(BatchManager::new());
    let stream_mgr = Arc::new(StreamManager::new(
        addr,
        state_store.clone(),
        streaming_metrics.clone(),
    ));
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
    let stream_env = StreamEnvironment::new(
        source_mgr,
        addr,
        stream_config,
        worker_id,
        state_store,
        shutdown_send.clone(),
    );

    // Boot the runtime gRPC services.
    let batch_srv = BatchServiceImpl::new(batch_mgr.clone(), batch_env);
    let exchange_srv = ExchangeServiceImpl::new(batch_mgr, stream_mgr.clone());
    let stream_srv = StreamServiceImpl::new(stream_mgr, stream_env.clone());

    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TaskServiceServer::new(batch_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .serve_with_shutdown(addr, async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = shutdown_recv.recv() => {
                        for (join_handle, shutdown_sender) in sub_tasks {
                            if shutdown_sender.send(()).is_ok() {
                                if let Err(err) = join_handle.await {
                                    tracing::warn!("shutdown err: {}", err);
                                }
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
        MetricsManager::boot_metrics_service(
            opts.prometheus_listener_addr.clone(),
            Arc::new(registry.clone()),
        );
    }

    // All set, let the meta service know we're ready.
    meta_client.activate(addr).await.unwrap();

    (join_handle, shutdown_send)
}

pub struct MetricsManager {}

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String, registry: Arc<Registry>) {
        tokio::spawn(async move {
            info!(
                "Prometheus listener for Prometheus is set up on http://{}",
                listen_addr
            );
            let listen_socket_addr: SocketAddr = listen_addr.parse().unwrap();
            let service = ServiceBuilder::new()
                .layer(AddExtensionLayer::new(registry))
                .service_fn(Self::metrics_service);
            let serve_future = hyper::Server::bind(&listen_socket_addr).serve(Shared::new(service));
            if let Err(err) = serve_future.await {
                eprintln!("server error: {}", err);
            }
        });
    }

    async fn metrics_service(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let registry = req.extensions().get::<Arc<Registry>>().unwrap();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = registry.gather();
        encoder.encode(&mf, &mut buffer).unwrap();
        let response = Response::builder()
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
}
