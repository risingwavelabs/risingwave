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
use std::sync::Arc;
use std::time::Duration;

use risingwave_batch::executor::monitor::BatchMetrics;
use risingwave_batch::rpc::service::task_service::BatchServiceImpl;
use risingwave_batch::task::{BatchEnvironment, BatchManager};
use risingwave_common::config::ComputeNodeConfig;
use risingwave_common::monitor::StreamingMetrics;
use risingwave_common::service::MetricsManager;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_rpc_client::MetaClient;
use risingwave_source::MemSourceManager;
use risingwave_storage::hummock::compaction_executor::CompactionExecutor;
use risingwave_storage::hummock::compactor::Compactor;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::monitor::{
    monitor_cache, HummockMetrics, ObjectStoreMetrics, StateStoreMetrics,
};
use risingwave_storage::StateStoreImpl;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::rpc::service::exchange_metrics::ExchangeServiceMetrics;
use crate::rpc::service::exchange_service::ExchangeServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::ComputeNodeOpts;

fn load_config(opts: &ComputeNodeOpts) -> ComputeNodeConfig {
    risingwave_common::config::load_config(&opts.config_path)
}

fn get_compile_mode() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Bootstraps the compute-node.
pub async fn compute_node_serve(
    listen_addr: SocketAddr,
    client_addr: HostAddr,
    opts: ComputeNodeOpts,
) -> (JoinHandle<()>, Sender<()>) {
    // Load the configuration.
    let config = load_config(&opts);
    info!(
        "Starting compute node with config {:?} in {} mode",
        config,
        get_compile_mode()
    );

    let mut meta_client = MetaClient::new(&opts.meta_address).await.unwrap();

    // Register to the cluster. We're not ready to serve until activate is called.
    let worker_id = meta_client
        .register(&client_addr, WorkerType::ComputeNode)
        .await
        .unwrap();
    info!("Assigned worker node id {}", worker_id);

    let mut sub_tasks: Vec<(JoinHandle<()>, Sender<()>)> = vec![MetaClient::start_heartbeat_loop(
        meta_client.clone(),
        Duration::from_millis(config.server.heartbeat_interval_ms as u64),
    )];
    // Initialize the metrics subsystem.
    let registry = prometheus::Registry::new();
    let hummock_metrics = Arc::new(HummockMetrics::new(registry.clone()));
    let streaming_metrics = Arc::new(StreamingMetrics::new(registry.clone()));
    let batch_metrics = Arc::new(BatchMetrics::new(registry.clone()));
    let exchange_srv_metrics = Arc::new(ExchangeServiceMetrics::new(registry.clone()));

    // Initialize state store.
    let storage_config = Arc::new(config.storage.clone());
    let state_store_metrics = Arc::new(StateStoreMetrics::new(registry.clone()));
    let object_store_metrics = Arc::new(ObjectStoreMetrics::new(registry.clone()));
    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));
    let state_store = StateStoreImpl::new(
        &opts.state_store,
        storage_config.clone(),
        hummock_meta_client.clone(),
        state_store_metrics.clone(),
        object_store_metrics,
    )
    .await
    .unwrap();
    if let StateStoreImpl::HummockStateStore(storage) = &state_store {
        if opts.state_store.starts_with("hummock+memory")
            || opts.state_store.starts_with("hummock+disk")
            || storage_config.disable_remote_compactor
        {
            tracing::info!("start embedded compactor");
            // todo: set shutdown_sender in HummockStorage.
            let (handle, shutdown_sender) = Compactor::start_compactor(
                storage_config,
                hummock_meta_client,
                storage.inner().sstable_store(),
                state_store_metrics.clone(),
                Some(Arc::new(CompactionExecutor::new(Some(1)))),
            );
            sub_tasks.push((handle, shutdown_sender));
        }
        monitor_cache(storage.inner().sstable_store(), &registry).unwrap();
    }

    // Initialize the managers.
    let batch_mgr = Arc::new(BatchManager::new());
    let stream_mgr = Arc::new(LocalStreamManager::new(
        client_addr.clone(),
        state_store.clone(),
        streaming_metrics.clone(),
        config.streaming.clone(),
    ));
    let source_mgr = Arc::new(MemSourceManager::new(worker_id));

    // Initialize batch environment.
    let batch_config = Arc::new(config.batch.clone());
    let batch_env = BatchEnvironment::new(
        source_mgr.clone(),
        batch_mgr.clone(),
        client_addr.clone(),
        batch_config,
        worker_id,
        state_store.clone(),
        batch_metrics.clone(),
    );

    // Initialize the streaming environment.
    let stream_config = Arc::new(config.streaming.clone());
    let stream_env = StreamEnvironment::new(
        source_mgr,
        client_addr.clone(),
        stream_config,
        worker_id,
        state_store,
    );

    // Boot the runtime gRPC services.
    let batch_srv = BatchServiceImpl::new(batch_mgr.clone(), batch_env);
    let exchange_srv =
        ExchangeServiceImpl::new(batch_mgr, stream_mgr.clone(), exchange_srv_metrics);
    let stream_srv = StreamServiceImpl::new(stream_mgr, stream_env.clone());

    let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel::<()>();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TaskServiceServer::new(batch_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .serve_with_shutdown(listen_addr, async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = &mut shutdown_recv => {
                        for (join_handle, shutdown_sender) in sub_tasks {
                            if let Err(err) = shutdown_sender.send(()) {
                                tracing::warn!("Failed to send shutdown: {:?}", err);
                                continue;
                            }
                            if let Err(err) = join_handle.await {
                                tracing::warn!("Failed to join shutdown: {:?}", err);
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
    meta_client.activate(&client_addr).await.unwrap();

    (join_handle, shutdown_send)
}
