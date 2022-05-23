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

use risingwave_common::service::MetricsManager;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::compactor_service_server::CompactorServiceServer;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use risingwave_storage::hummock::SstableStore;
use risingwave_storage::monitor::{HummockMetrics, ObjectStoreMetrics, StateStoreMetrics};
use risingwave_storage::object::{parse_object_store, ObjectStoreImpl};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::rpc::CompactorServiceImpl;
use crate::{CompactorConfig, CompactorOpts};

/// Fetches and runs compaction tasks.
pub async fn compactor_serve(
    listen_addr: SocketAddr,
    client_addr: HostAddr,
    opts: CompactorOpts,
) -> (JoinHandle<()>, UnboundedSender<()>) {
    let config = {
        if opts.config_path.is_empty() {
            CompactorConfig::default()
        } else {
            let config_path = PathBuf::from(opts.config_path.to_owned());
            CompactorConfig::init(config_path).unwrap()
        }
    };
    tracing::info!("Starting compactor with config {:?}", config);

    // Register to the cluster.
    let mut meta_client = MetaClient::new(&opts.meta_address).await.unwrap();
    let worker_id = meta_client
        .register(&client_addr, WorkerType::Compactor)
        .await
        .unwrap();
    tracing::info!("Assigned compactor id {}", worker_id);
    meta_client.activate(&client_addr).await.unwrap();

    // Boot compactor
    let registry = prometheus::Registry::new();
    let hummock_metrics = Arc::new(HummockMetrics::new(registry.clone()));
    let object_metrics = Arc::new(ObjectStoreMetrics::new(registry.clone()));
    let hummock_meta_client = Arc::new(MonitoredHummockMetaClient::new(
        meta_client.clone(),
        hummock_metrics.clone(),
    ));
    let storage_config = Arc::new(config.storage);
    let state_store_stats = Arc::new(StateStoreMetrics::new(registry.clone()));
    let object_store = Arc::new(ObjectStoreImpl::new(
        parse_object_store(
            opts.state_store
                .strip_prefix("hummock+")
                .expect("object store must be hummock for compactor server"),
            false,
        )
        .await,
        object_metrics,
    ));
    let sstable_store = Arc::new(SstableStore::new(
        object_store,
        storage_config.data_directory.to_string(),
        storage_config.block_cache_capacity_mb * (1 << 20),
        storage_config.meta_cache_capacity_mb * (1 << 20),
    ));

    let sub_tasks = vec![
        MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
        ),
        risingwave_storage::hummock::compactor::Compactor::start_compactor(
            storage_config,
            hummock_meta_client,
            sstable_store,
            state_store_stats,
        ),
    ];

    let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(CompactorServiceServer::new(CompactorServiceImpl {}))
            .serve_with_shutdown(listen_addr, async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = shutdown_recv.recv() => {
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

    (join_handle, shutdown_send)
}
