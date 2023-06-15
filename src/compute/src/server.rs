// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use risingwave_batch::monitor::{BatchExecutorMetrics, BatchManagerMetrics, BatchTaskMetrics};
use risingwave_batch::rpc::service::task_service::BatchServiceImpl;
use risingwave_batch::task::{BatchEnvironment, BatchManager};
use risingwave_common::config::{
    load_config, AsyncStackTraceOption, StorageMemoryConfig, MAX_CONNECTION_WINDOW_SIZE,
    STREAM_WINDOW_SIZE,
};
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::pretty_bytes::convert;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_pb::common::WorkerType;
use risingwave_pb::compute::config_service_server::ConfigServiceServer;
use risingwave_pb::connector_service::SinkPayloadFormat;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::meta::add_worker_node_request::Property;
use risingwave_pb::monitor_service::monitor_service_server::MonitorServiceServer;
use risingwave_pb::stream_service::stream_service_server::StreamServiceServer;
use risingwave_pb::task_service::exchange_service_server::ExchangeServiceServer;
use risingwave_pb::task_service::task_service_server::TaskServiceServer;
use risingwave_rpc_client::{ComputeClientPool, ExtraInfoSourceRef, MetaClient};
use risingwave_source::dml_manager::DmlManager;
use risingwave_storage::hummock::compactor::{CompactionExecutor, Compactor, CompactorContext};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{
    HummockMemoryCollector, MemoryLimiter, TieredCacheMetricsBuilder,
};
use risingwave_storage::monitor::{
    monitor_cache, CompactorMetrics, HummockMetrics, HummockStateStoreMetrics,
    MonitoredStorageMetrics, ObjectStoreMetrics,
};
use risingwave_storage::opts::StorageOpts;
use risingwave_storage::StateStoreImpl;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::memory_management::memory_manager::GlobalMemoryManager;
use crate::memory_management::{
    build_memory_control_policy, reserve_memory_bytes, storage_memory_config, MIN_COMPUTE_MEMORY_MB,
};
use crate::observer::observer_manager::ComputeObserverNode;
use crate::rpc::service::config_service::ConfigServiceImpl;
use crate::rpc::service::exchange_metrics::ExchangeServiceMetrics;
use crate::rpc::service::exchange_service::ExchangeServiceImpl;
use crate::rpc::service::health_service::HealthServiceImpl;
use crate::rpc::service::monitor_service::{
    AwaitTreeMiddlewareLayer, AwaitTreeRegistryRef, MonitorServiceImpl,
};
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::telemetry::ComputeTelemetryCreator;
use crate::ComputeNodeOpts;

/// Bootstraps the compute-node.
pub async fn compute_node_serve(
    listen_addr: SocketAddr,
    advertise_addr: HostAddr,
    opts: ComputeNodeOpts,
    registry: prometheus::Registry,
) -> (Vec<JoinHandle<()>>, Sender<()>) {
    // Load the configuration.
    let config = load_config(&opts.config_path, Some(opts.override_config.clone()));

    info!("Starting compute node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    // Initialize all the configs
    let stream_config = Arc::new(config.streaming.clone());
    let batch_config = Arc::new(config.batch.clone());

    // Register to the cluster. We're not ready to serve until activate is called.
    let (meta_client, system_params) = MetaClient::register_new(
        &opts.meta_address,
        WorkerType::ComputeNode,
        &advertise_addr,
        Property {
            worker_node_parallelism: opts.parallelism as u64,
            is_streaming: opts.role.for_streaming(),
            is_serving: opts.role.for_serving(),
            is_schedulable: true,
            // TODO: Discussion. Do we want to support starting cordoned CNs?
        },
        &config.meta,
    )
    .await
    .unwrap();

    let state_store_url = system_params.state_store();

    let embedded_compactor_enabled =
        embedded_compactor_enabled(state_store_url, config.storage.disable_remote_compactor);

    let (reserved_memory_bytes, non_reserved_memory_bytes) =
        reserve_memory_bytes(opts.total_memory_bytes);
    let storage_memory_config = storage_memory_config(
        non_reserved_memory_bytes,
        embedded_compactor_enabled,
        &config.storage,
    );

    let storage_memory_bytes = total_storage_memory_limit_bytes(&storage_memory_config);
    let compute_memory_bytes = validate_compute_node_memory_config(
        opts.total_memory_bytes,
        reserved_memory_bytes,
        storage_memory_bytes,
    );
    print_memory_config(
        opts.total_memory_bytes,
        compute_memory_bytes,
        storage_memory_bytes,
        &storage_memory_config,
        embedded_compactor_enabled,
        reserved_memory_bytes,
    );

    // NOTE: Due to some limits, we use `compute_memory_bytes + storage_memory_bytes` as
    // `total_compute_memory_bytes` for memory control. This is just a workaround for some
    // memory control issues and should be modified as soon as we figure out a better solution.
    //
    // Related issues:
    // - https://github.com/risingwavelabs/risingwave/issues/8696
    // - https://github.com/risingwavelabs/risingwave/issues/8822
    let total_memory_bytes = compute_memory_bytes + storage_memory_bytes;
    let memory_control_policy = build_memory_control_policy(total_memory_bytes).unwrap();

    let storage_opts = Arc::new(StorageOpts::from((
        &config,
        &system_params,
        &storage_memory_config,
    )));

    let worker_id = meta_client.worker_id();
    info!("Assigned worker node id {}", worker_id);

    let mut sub_tasks: Vec<(JoinHandle<()>, Sender<()>)> = vec![];
    // Initialize the metrics subsystem.

    monitor_process(&registry).unwrap();
    let source_metrics = Arc::new(SourceMetrics::new(registry.clone()));
    let hummock_metrics = Arc::new(HummockMetrics::new(registry.clone()));
    let streaming_metrics = Arc::new(StreamingMetrics::new(registry.clone()));
    let batch_task_metrics = Arc::new(BatchTaskMetrics::new(registry.clone()));
    let batch_executor_metrics = Arc::new(BatchExecutorMetrics::new(registry.clone()));
    let batch_manager_metrics = BatchManagerMetrics::new(registry.clone());
    let exchange_srv_metrics = Arc::new(ExchangeServiceMetrics::new(registry.clone()));

    // Initialize state store.
    let state_store_metrics = Arc::new(HummockStateStoreMetrics::new(registry.clone()));
    let object_store_metrics = Arc::new(ObjectStoreMetrics::new(registry.clone()));
    let storage_metrics = Arc::new(MonitoredStorageMetrics::new(registry.clone()));
    let compactor_metrics = Arc::new(CompactorMetrics::new(registry.clone()));

    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));

    let mut join_handle_vec = vec![];

    let state_store = StateStoreImpl::new(
        state_store_url,
        storage_opts.clone(),
        hummock_meta_client.clone(),
        state_store_metrics.clone(),
        object_store_metrics,
        TieredCacheMetricsBuilder::new(registry.clone()),
        storage_metrics.clone(),
        compactor_metrics.clone(),
    )
    .await
    .unwrap();

    // Initialize observer manager.
    let system_params_manager = Arc::new(LocalSystemParamsManager::new(system_params.clone()));
    let compute_observer_node = ComputeObserverNode::new(system_params_manager.clone());
    let observer_manager =
        ObserverManager::new_with_meta_client(meta_client.clone(), compute_observer_node).await;
    observer_manager.start().await;

    let mut extra_info_sources: Vec<ExtraInfoSourceRef> = vec![];
    if let Some(storage) = state_store.as_hummock_trait() {
        extra_info_sources.push(storage.sstable_object_id_manager().clone());
        if embedded_compactor_enabled {
            tracing::info!("start embedded compactor");
            let output_memory_limiter = Arc::new(MemoryLimiter::new(
                storage_opts.compactor_memory_limit_mb as u64 * 1024 * 1024 / 2,
            ));
            let compactor_context = Arc::new(CompactorContext {
                storage_opts,
                hummock_meta_client: hummock_meta_client.clone(),
                sstable_store: storage.sstable_store(),
                compactor_metrics: compactor_metrics.clone(),
                is_share_buffer_compact: false,
                compaction_executor: Arc::new(CompactionExecutor::new(Some(1))),
                filter_key_extractor_manager: storage.filter_key_extractor_manager().clone(),
                output_memory_limiter,
                sstable_object_id_manager: storage.sstable_object_id_manager().clone(),
                task_progress_manager: Default::default(),
            });

            let (handle, shutdown_sender) =
                Compactor::start_compactor(compactor_context, hummock_meta_client);
            sub_tasks.push((handle, shutdown_sender));
        }
        let memory_limiter = storage.get_memory_limiter();
        let memory_collector = Arc::new(HummockMemoryCollector::new(
            storage.sstable_store(),
            memory_limiter,
            storage_memory_config,
        ));
        monitor_cache(memory_collector, &registry).unwrap();
        let backup_reader = storage.backup_reader();
        let system_params_mgr = system_params_manager.clone();
        tokio::spawn(async move {
            backup_reader
                .watch_config_change(system_params_mgr.watch_params())
                .await;
        });
    }

    sub_tasks.push(MetaClient::start_heartbeat_loop(
        meta_client.clone(),
        Duration::from_millis(config.server.heartbeat_interval_ms as u64),
        extra_info_sources,
    ));

    let await_tree_config = match &config.streaming.async_stack_trace {
        AsyncStackTraceOption::Off => None,
        c => await_tree::ConfigBuilder::default()
            .verbose(c.is_verbose().unwrap())
            .build()
            .ok(),
    };

    // Initialize the managers.
    let batch_mgr = Arc::new(BatchManager::new(
        config.batch.clone(),
        batch_manager_metrics,
    ));
    let stream_mgr = Arc::new(LocalStreamManager::new(
        advertise_addr.clone(),
        state_store.clone(),
        streaming_metrics.clone(),
        config.streaming.clone(),
        await_tree_config.clone(),
    ));

    // Spawn LRU Manager that have access to collect memory from batch mgr and stream mgr.
    let batch_mgr_clone = batch_mgr.clone();
    let stream_mgr_clone = stream_mgr.clone();

    let memory_mgr = GlobalMemoryManager::new(
        system_params.barrier_interval_ms(),
        streaming_metrics.clone(),
        memory_control_policy,
    );
    // Run a background memory monitor
    tokio::spawn(memory_mgr.clone().run(batch_mgr_clone, stream_mgr_clone));

    let watermark_epoch = memory_mgr.get_watermark_epoch();
    // Set back watermark epoch to stream mgr. Executor will read epoch from stream manager instead
    // of lru manager.
    stream_mgr.set_watermark_epoch(watermark_epoch).await;

    let telemetry_enabled = system_params.telemetry_enabled();

    let grpc_await_tree_reg = await_tree_config
        .map(|config| AwaitTreeRegistryRef::new(await_tree::Registry::new(config).into()));
    let dml_mgr = Arc::new(DmlManager::default());

    // Initialize batch environment.
    let client_pool = Arc::new(ComputeClientPool::new(config.server.connection_pool_size));
    let batch_env = BatchEnvironment::new(
        batch_mgr.clone(),
        advertise_addr.clone(),
        batch_config,
        worker_id,
        state_store.clone(),
        batch_task_metrics.clone(),
        batch_executor_metrics.clone(),
        client_pool,
        dml_mgr.clone(),
        source_metrics.clone(),
    );

    let connector_params = risingwave_connector::ConnectorParams {
        connector_rpc_endpoint: opts.connector_rpc_endpoint,
        sink_payload_format: match opts.connector_rpc_sink_payload_format.as_deref() {
            None | Some("json") => SinkPayloadFormat::Json,
            Some("stream_chunk") => SinkPayloadFormat::StreamChunk,
            _ => {
                unreachable!(
                    "invalid sink payload format: {:?}. Should be either json or stream_chunk",
                    opts.connector_rpc_sink_payload_format
                )
            }
        },
    };

    info!("connector param: {:?}", connector_params);

    // Initialize the streaming environment.
    let stream_env = StreamEnvironment::new(
        advertise_addr.clone(),
        connector_params,
        stream_config,
        worker_id,
        state_store,
        dml_mgr,
        system_params_manager.clone(),
        source_metrics,
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
        ExchangeServiceImpl::new(batch_mgr.clone(), stream_mgr.clone(), exchange_srv_metrics);
    let stream_srv = StreamServiceImpl::new(stream_mgr.clone(), stream_env.clone());
    let monitor_srv = MonitorServiceImpl::new(stream_mgr.clone(), grpc_await_tree_reg.clone());
    let config_srv = ConfigServiceImpl::new(batch_mgr, stream_mgr);
    let health_srv = HealthServiceImpl::new();

    let telemetry_manager = TelemetryManager::new(
        system_params_manager.watch_params(),
        Arc::new(meta_client.clone()),
        Arc::new(ComputeTelemetryCreator::new()),
    );

    // if the toml config file or env variable disables telemetry, do not watch system params change
    // because if any of configs disable telemetry, we should never start it
    if config.server.telemetry_enabled && telemetry_env_enabled() {
        // if all configs are true, start reporting
        if telemetry_enabled {
            telemetry_manager.start_telemetry_reporting().await;
        }
        // if config and env are true, starting watching
        sub_tasks.push(telemetry_manager.watch_params_change());
    } else {
        tracing::info!("Telemetry didn't start due to config");
    }

    let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel::<()>();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .initial_stream_window_size(STREAM_WINDOW_SIZE)
            .tcp_nodelay(true)
            .layer(AwaitTreeMiddlewareLayer::new_optional(grpc_await_tree_reg))
            .add_service(TaskServiceServer::new(batch_srv))
            .add_service(ExchangeServiceServer::new(exchange_srv))
            .add_service(StreamServiceServer::new(stream_srv))
            .add_service(MonitorServiceServer::new(monitor_srv))
            .add_service(ConfigServiceServer::new(config_srv))
            .add_service(HealthServer::new(health_srv))
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
    if config.server.metrics_level > 0 {
        MetricsManager::boot_metrics_service(
            opts.prometheus_listener_addr.clone(),
            registry.clone(),
        );
    }

    // All set, let the meta service know we're ready.
    meta_client.activate(&advertise_addr).await.unwrap();

    (join_handle_vec, shutdown_send)
}

/// Check whether the compute node has enough memory to perform computing tasks. Apart from storage,
/// it is recommended to reserve at least `MIN_COMPUTE_MEMORY_MB` for computing and
/// `SYSTEM_RESERVED_MEMORY_PROPORTION` of total memory for other system usage. If the requirement
/// is not met, we will print out a warning and enforce the memory used for computing tasks as
/// `MIN_COMPUTE_MEMORY_MB`.
fn validate_compute_node_memory_config(
    cn_total_memory_bytes: usize,
    reserved_memory_bytes: usize,
    storage_memory_bytes: usize,
) -> usize {
    if storage_memory_bytes > cn_total_memory_bytes {
        tracing::warn!(
            "The storage memory exceeds the total compute node memory:\nTotal compute node memory: {}\nStorage memory: {}\nWe recommend that at least 4 GiB memory should be reserved for RisingWave. Please increase the total compute node memory or decrease the storage memory in configurations.",
            convert(cn_total_memory_bytes as _),
            convert(storage_memory_bytes as _)
        );
        MIN_COMPUTE_MEMORY_MB << 20
    } else if storage_memory_bytes + (MIN_COMPUTE_MEMORY_MB << 20) + reserved_memory_bytes
        >= cn_total_memory_bytes
    {
        tracing::warn!(
            "No enough memory for computing and other system usage:\nTotal compute node memory: {}\nStorage memory: {}\nWe recommend that at least 4 GiB memory should be reserved for RisingWave. Please increase the total compute node memory or decrease the storage memory in configurations.",
            convert(cn_total_memory_bytes as _),
            convert(storage_memory_bytes as _)
        );
        MIN_COMPUTE_MEMORY_MB << 20
    } else {
        cn_total_memory_bytes - storage_memory_bytes - reserved_memory_bytes
    }
}

/// The maximal memory that storage components may use based on the configurations in bytes. Note
/// that this is the total storage memory for one compute node instead of the whole cluster.
fn total_storage_memory_limit_bytes(storage_memory_config: &StorageMemoryConfig) -> usize {
    let total_storage_memory_mb = storage_memory_config.block_cache_capacity_mb
        + storage_memory_config.meta_cache_capacity_mb
        + storage_memory_config.shared_buffer_capacity_mb
        + storage_memory_config.file_cache_total_buffer_capacity_mb
        + storage_memory_config.compactor_memory_limit_mb;
    total_storage_memory_mb << 20
}

/// Checks whether an embedded compactor starts with a compute node.
fn embedded_compactor_enabled(state_store_url: &str, disable_remote_compactor: bool) -> bool {
    // We treat `hummock+memory-shared` as a shared storage, so we won't start the compactor
    // along with the compute node.
    state_store_url == "hummock+memory"
        || state_store_url.starts_with("hummock+disk")
        || disable_remote_compactor
}

// Print out the memory outline of the compute node.
fn print_memory_config(
    cn_total_memory_bytes: usize,
    compute_memory_bytes: usize,
    storage_memory_bytes: usize,
    storage_memory_config: &StorageMemoryConfig,
    embedded_compactor_enabled: bool,
    reserved_memory_bytes: usize,
) {
    info!("Memory outline: ");
    info!("> total_memory: {}", convert(cn_total_memory_bytes as _));
    info!(
        ">     storage_memory: {}",
        convert(storage_memory_bytes as _)
    );
    info!(
        ">         block_cache_capacity: {}",
        convert((storage_memory_config.block_cache_capacity_mb << 20) as _)
    );
    info!(
        ">         meta_cache_capacity: {}",
        convert((storage_memory_config.meta_cache_capacity_mb << 20) as _)
    );
    info!(
        ">         shared_buffer_capacity: {}",
        convert((storage_memory_config.shared_buffer_capacity_mb << 20) as _)
    );
    info!(
        ">         file_cache_total_buffer_capacity: {}",
        convert((storage_memory_config.file_cache_total_buffer_capacity_mb << 20) as _)
    );
    if embedded_compactor_enabled {
        info!(
            ">         compactor_memory_limit: {}",
            convert((storage_memory_config.compactor_memory_limit_mb << 20) as _)
        );
    }
    info!(
        ">     compute_memory: {}",
        convert(compute_memory_bytes as _)
    );
    info!(
        ">     reserved_memory: {}",
        convert(reserved_memory_bytes as _)
    );
}
