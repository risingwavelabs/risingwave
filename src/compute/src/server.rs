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
use risingwave_batch::executor::BatchTaskMetricsManager;
use risingwave_batch::rpc::service::task_service::BatchServiceImpl;
use risingwave_batch::task::{BatchEnvironment, BatchManager};
use risingwave_common::config::{load_config, MAX_CONNECTION_WINDOW_SIZE};
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::monitor_service_server::MonitorServiceServer;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_rpc_client::{ComputeClientPool, ExtraInfoSourceRef, MetaClient};
use risingwave_source::monitor::SourceMetrics;
use risingwave_source::MemSourceManager;
use risingwave_storage::hummock::compactor::{
    CompactionExecutor, Compactor, CompactorContext, Context,
};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{
    CompactorSstableStore, HummockMemoryCollector, MemoryLimiter, TieredCacheMetricsBuilder,
};
use risingwave_storage::monitor::{
    monitor_cache, HummockMetrics, ObjectStoreMetrics, StateStoreMetrics,
};
use risingwave_storage::StateStoreImpl;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::rpc::service::exchange_metrics::ExchangeServiceMetrics;
use crate::rpc::service::exchange_service::ExchangeServiceImpl;
use crate::rpc::service::monitor_service::{
    GrpcStackTraceManagerRef, MonitorServiceImpl, StackTraceMiddlewareLayer,
};
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::{ComputeNodeConfig, ComputeNodeOpts};

/// Bootstraps the compute-node.
pub async fn compute_node_serve(
    listen_addr: SocketAddr,
    client_addr: HostAddr,
    opts: ComputeNodeOpts,
) -> (Vec<JoinHandle<()>>, Sender<()>) {
    // Load the configuration.
    let config: ComputeNodeConfig = load_config(&opts.config_path).unwrap();
    info!(
        "Starting compute node with config {:?} with debug assertions {}",
        config,
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    // Initialize all the configs
    let storage_config = Arc::new(config.storage.clone());
    let stream_config = Arc::new(config.streaming.clone());
    let batch_config = Arc::new(config.batch.clone());

    // Register to the cluster. We're not ready to serve until activate is called.
    let meta_client = MetaClient::register_new(
        &opts.meta_address,
        WorkerType::ComputeNode,
        &client_addr,
        config.streaming.worker_node_parallelism,
    )
    .await
    .unwrap();

    let worker_id = meta_client.worker_id();
    info!("Assigned worker node id {}", worker_id);

    let mut sub_tasks: Vec<(JoinHandle<()>, Sender<()>)> = vec![];
    // Initialize the metrics subsystem.
    let registry = prometheus::Registry::new();
    monitor_process(&registry).unwrap();
    let source_metrics = Arc::new(SourceMetrics::new(registry.clone()));
    let hummock_metrics = Arc::new(HummockMetrics::new(registry.clone()));
    let streaming_metrics = Arc::new(StreamingMetrics::new(registry.clone()));
    let batch_metrics = Arc::new(BatchMetrics::new());
    let batch_task_metrics_mgr = Arc::new(BatchTaskMetricsManager::new(registry.clone()));
    let exchange_srv_metrics = Arc::new(ExchangeServiceMetrics::new(registry.clone()));

    // Initialize state store.
    let state_store_metrics = Arc::new(StateStoreMetrics::new(registry.clone()));
    let object_store_metrics = Arc::new(ObjectStoreMetrics::new(registry.clone()));
    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));

    let mut join_handle_vec = vec![];

    let state_store = StateStoreImpl::new(
        &opts.state_store,
        &opts.file_cache_dir,
        storage_config.clone(),
        hummock_meta_client.clone(),
        state_store_metrics.clone(),
        object_store_metrics,
        TieredCacheMetricsBuilder::new(registry.clone()),
    )
    .await
    .unwrap();

    let mut extra_info_sources: Vec<ExtraInfoSourceRef> = vec![];
    if let StateStoreImpl::HummockStateStore(storage) = &state_store {
        extra_info_sources.push(storage.sstable_id_manager());
        // Note: we treat `hummock+memory-shared` as a shared storage, so we won't start the
        // compactor along with compute node.
        if opts.state_store == "hummock+memory"
            || opts.state_store.starts_with("hummock+disk")
            || storage_config.disable_remote_compactor
        {
            tracing::info!("start embedded compactor");
            let read_memory_limiter = Arc::new(MemoryLimiter::new(
                storage_config.compactor_memory_limit_mb as u64 * 1024 * 1024 / 2,
            ));
            // todo: set shutdown_sender in HummockStorage.
            let write_memory_limit =
                storage_config.compactor_memory_limit_mb as u64 * 1024 * 1024 / 2;
            let context = Arc::new(Context {
                options: storage_config,
                hummock_meta_client: hummock_meta_client.clone(),
                sstable_store: storage.sstable_store(),
                stats: state_store_metrics.clone(),
                is_share_buffer_compact: false,
                compaction_executor: Arc::new(CompactionExecutor::new(Some(1))),
                filter_key_extractor_manager: storage
                    .inner()
                    .filter_key_extractor_manager()
                    .clone(),
                read_memory_limiter,
                sstable_id_manager: storage.sstable_id_manager(),
                task_progress_manager: Default::default(),
            });
            // TODO: use normal sstable store for single-process mode.
            let compactor_sstable_store = CompactorSstableStore::new(
                storage.sstable_store(),
                Arc::new(MemoryLimiter::new(write_memory_limit)),
            );
            let compactor_context = Arc::new(CompactorContext {
                context,
                sstable_store: Arc::new(compactor_sstable_store),
            });

            let (handle, shutdown_sender) =
                Compactor::start_compactor(compactor_context, hummock_meta_client, 1);
            sub_tasks.push((handle, shutdown_sender));
        }
        let local_version_manager = storage.local_version_manager();
        let memory_limiter = local_version_manager
            .get_buffer_tracker()
            .get_memory_limiter();
        let memory_collector = Arc::new(HummockMemoryCollector::new(
            storage.sstable_store(),
            memory_limiter.clone(),
        ));
        monitor_cache(memory_collector, &registry).unwrap();
    }

    sub_tasks.push(MetaClient::start_heartbeat_loop(
        meta_client.clone(),
        Duration::from_millis(config.server.heartbeat_interval_ms as u64),
        extra_info_sources,
    ));

    // Initialize the managers.
    let batch_mgr = Arc::new(BatchManager::new(config.batch.worker_threads_num));
    let stream_mgr = Arc::new(LocalStreamManager::new(
        client_addr.clone(),
        state_store.clone(),
        streaming_metrics.clone(),
        config.streaming.clone(),
        opts.enable_async_stack_trace,
        opts.enable_managed_cache,
    ));
    let source_mgr = Arc::new(MemSourceManager::new(
        source_metrics,
        stream_config.developer.stream_connector_message_buffer_size,
    ));
    let grpc_stack_trace_mgr = GrpcStackTraceManagerRef::default();

    // Initialize batch environment.
    let client_pool = Arc::new(ComputeClientPool::new(config.server.connection_pool_size));
    let batch_env = BatchEnvironment::new(
        source_mgr.clone(),
        batch_mgr.clone(),
        client_addr.clone(),
        batch_config,
        worker_id,
        state_store.clone(),
        batch_task_metrics_mgr.clone(),
        batch_metrics.clone(),
        client_pool,
    );

    // Initialize the streaming environment.
    let stream_env = StreamEnvironment::new(
        source_mgr,
        client_addr.clone(),
        stream_config,
        worker_id,
        state_store,
    );

    // Generally, one may use `risedev ctl trace` to manually get the trace reports. However, if
    // this is not the case, we can use the following command to get it printed into the logs
    // periodically.
    //
    // Comment out the following line to enable.
    // TODO: may optionally enable based on the features
    #[cfg(any())]
    stream_mgr.clone().spawn_print_trace();

    // Boot the runtime gRPC services.
    let batch_srv = BatchServiceImpl::new(batch_mgr.clone(), batch_env);
    let exchange_srv =
        ExchangeServiceImpl::new(batch_mgr, stream_mgr.clone(), exchange_srv_metrics);
    let stream_srv = StreamServiceImpl::new(stream_mgr.clone(), stream_env.clone());
    let monitor_srv = MonitorServiceImpl::new(stream_mgr, grpc_stack_trace_mgr.clone());

    let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel::<()>();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .tcp_nodelay(true)
            .layer(StackTraceMiddlewareLayer::new_optional(
                opts.enable_async_stack_trace
                    .then_some(grpc_stack_trace_mgr),
            ))
            .add_service(TaskServiceServer::new(batch_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .add_service(MonitorServiceServer::new(monitor_srv))
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
    join_handle_vec.push(join_handle);

    // Boot metrics service.
    if opts.metrics_level > 0 {
        MetricsManager::boot_metrics_service(
            opts.prometheus_listener_addr.clone(),
            Arc::new(registry.clone()),
        );
    }

    // All set, let the meta service know we're ready.
    meta_client.activate(&client_addr).await.unwrap();

    (join_handle_vec, shutdown_send)
}
