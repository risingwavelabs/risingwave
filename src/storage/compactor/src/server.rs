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

use risingwave_common::config::{extract_storage_memory_config, load_config};
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::common::WorkerType;
use risingwave_pb::compactor::compactor_service_server::CompactorServiceServer;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::compactor::{CompactionExecutor, CompactorContext};
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::{
    CompactorMemoryCollector, MemoryLimiter, SstableObjectIdManager, SstableStore,
};
use risingwave_storage::monitor::{
    monitor_cache, CompactorMetrics, HummockMetrics, ObjectStoreMetrics,
};
use risingwave_storage::opts::StorageOpts;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tracing::info;

use super::compactor_observer::observer_manager::CompactorObserverNode;
use crate::rpc::CompactorServiceImpl;
use crate::telemetry::CompactorTelemetryCreator;
use crate::CompactorOpts;

/// Fetches and runs compaction tasks.
pub async fn compactor_serve(
    listen_addr: SocketAddr,
    advertise_addr: HostAddr,
    opts: CompactorOpts,
) -> (JoinHandle<()>, JoinHandle<()>, Sender<()>) {
    let config = load_config(&opts.config_path, Some(opts.override_config));
    info!("Starting compactor node",);
    info!("> config: {:?}", config);
    info!(
        "> debug assertions: {}",
        if cfg!(debug_assertions) { "on" } else { "off" }
    );
    info!("> version: {} ({})", RW_VERSION, GIT_SHA);

    // Register to the cluster.
    let (meta_client, system_params_reader) = MetaClient::register_new(
        &opts.meta_address,
        WorkerType::Compactor,
        &advertise_addr,
        0,
    )
    .await
    .unwrap();
    info!("Assigned compactor id {}", meta_client.worker_id());
    meta_client.activate(&advertise_addr).await.unwrap();

    // Boot compactor
    let registry = prometheus::Registry::new();
    monitor_process(&registry).unwrap();
    let hummock_metrics = Arc::new(HummockMetrics::new(registry.clone()));
    let object_metrics = Arc::new(ObjectStoreMetrics::new(registry.clone()));
    let compactor_metrics = Arc::new(CompactorMetrics::new(registry.clone()));

    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));

    let state_store_url = system_params_reader.state_store();

    let storage_memory_config = extract_storage_memory_config(&config);
    let storage_opts = Arc::new(StorageOpts::from((
        &config,
        &system_params_reader,
        &storage_memory_config,
    )));
    let object_store = Arc::new(
        parse_remote_object_store(
            state_store_url
                .strip_prefix("hummock+")
                .expect("object store must be hummock for compactor server"),
            object_metrics,
            "Hummock",
        )
        .await,
    );
    let sstable_store = Arc::new(SstableStore::for_compactor(
        object_store,
        storage_opts.data_directory.to_string(),
        1 << 20, // set 1MB memory to avoid panic.
        storage_opts.meta_cache_capacity_mb * (1 << 20),
    ));

    let telemetry_enabled = system_params_reader.telemetry_enabled();

    let filter_key_extractor_manager = Arc::new(FilterKeyExtractorManager::default());
    let system_params_manager = Arc::new(LocalSystemParamsManager::new(system_params_reader));
    let compactor_observer_node = CompactorObserverNode::new(
        filter_key_extractor_manager.clone(),
        system_params_manager.clone(),
    );
    let observer_manager =
        ObserverManager::new_with_meta_client(meta_client.clone(), compactor_observer_node).await;

    // use half of limit because any memory which would hold in meta-cache will be allocate by
    // limited at first.
    let observer_join_handle = observer_manager.start().await;
    let output_limit_mb = storage_opts.compactor_memory_limit_mb as u64 / 2;
    let memory_limiter = Arc::new(MemoryLimiter::new(output_limit_mb << 20));
    let input_limit_mb = storage_opts.compactor_memory_limit_mb as u64 / 2;
    let max_concurrent_task_number = storage_opts.max_concurrent_compaction_task_number;
    let memory_collector = Arc::new(CompactorMemoryCollector::new(
        memory_limiter.clone(),
        sstable_store.clone(),
        Arc::new(MemoryLimiter::new(input_limit_mb << 20)),
    ));
    monitor_cache(memory_collector, &registry).unwrap();
    let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
        hummock_meta_client.clone(),
        storage_opts.sstable_id_remote_fetch_number,
    ));
    let compactor_context = Arc::new(CompactorContext {
        storage_opts,
        hummock_meta_client: hummock_meta_client.clone(),
        sstable_store: sstable_store.clone(),
        compactor_metrics,
        is_share_buffer_compact: false,
        compaction_executor: Arc::new(CompactionExecutor::new(
            opts.compaction_worker_threads_number,
        )),
        filter_key_extractor_manager: filter_key_extractor_manager.clone(),
        read_memory_limiter: memory_limiter,
        sstable_object_id_manager: sstable_object_id_manager.clone(),
        task_progress_manager: Default::default(),
        compactor_runtime_config: Arc::new(tokio::sync::Mutex::new(CompactorRuntimeConfig {
            max_concurrent_task_number,
        })),
    });
    let mut sub_tasks = vec![
        MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
            Duration::from_secs(config.server.max_heartbeat_interval_secs as u64),
            vec![sstable_object_id_manager],
        ),
        risingwave_storage::hummock::compactor::Compactor::start_compactor(
            compactor_context.clone(),
            hummock_meta_client,
        ),
    ];

    let telemetry_manager = TelemetryManager::new(
        system_params_manager.watch_params(),
        Arc::new(meta_client.clone()),
        Arc::new(CompactorTelemetryCreator::new()),
    );
    // if the toml config file or env variable disables telemetry, do not watch system params change
    // because if any of configs disable telemetry, we should never start it
    if config.server.telemetry_enabled && telemetry_env_enabled() {
        if telemetry_enabled {
            telemetry_manager.start_telemetry_reporting();
        }
        sub_tasks.push(telemetry_manager.watch_params_change());
    } else {
        tracing::info!("Telemetry didn't start due to config");
    }

    let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CompactorServiceServer::new(CompactorServiceImpl::new(
                compactor_context,
                meta_client.clone(),
            )))
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
    if config.server.metrics_level > 0 {
        MetricsManager::boot_metrics_service(
            opts.prometheus_listener_addr.clone(),
            registry.clone(),
        );
    }

    (join_handle, observer_join_handle, shutdown_send)
}
