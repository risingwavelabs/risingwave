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

use std::sync::Arc;
use std::time::Duration;

use risingwave_backup::storage::ObjectStoreMetaSnapshotStorage;
use risingwave_common::monitor::process_prometheus::monitor_process;
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::backup_service::backup_service_server::BackupServiceServer;
use risingwave_pb::ddl_service::ddl_service_server::DdlServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::leader::leader_service_server::LeaderServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::scale_service_server::ScaleServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use risingwave_pb::meta::MetaLeaderInfo;
use risingwave_pb::user::user_service_server::UserServiceServer;
use tokio::sync::watch::Receiver as WatchReceiver;

use super::intercept::MetricsMiddlewareLayer;
use super::service::health_service::HealthServiceImpl;
use super::service::notification_service::NotificationServiceImpl;
use super::service::scale_service::ScaleServiceImpl;
use super::DdlServiceImpl;
use crate::backup_restore::BackupManager;
use crate::barrier::{BarrierScheduler, GlobalBarrierManager};
use crate::hummock::{CompactionScheduler, HummockManager};
use crate::manager::{
    CatalogManager, ClusterManager, FragmentManager, IdleManager, MetaOpts, MetaSrvEnv,
};
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::server::{AddressInfo, ElectionClientRef};
use crate::rpc::service::backup_service::BackupServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::leader_service::LeaderServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::rpc::service::user_service::UserServiceImpl;
use crate::storage::MetaStore;
use crate::stream::{GlobalStreamManager, SourceManager};
use crate::{hummock, MetaResult};

/// Starts all services needed for the meta leader node
/// Only call this function once, since initializing the services multiple times will result in an
/// inconsistent state
///
/// ## Returns
/// Returns an error if the service initialization failed
pub async fn start_leader_srv<S: MetaStore>(
    meta_store: Arc<S>,
    address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    opts: MetaOpts,
    current_leader: MetaLeaderInfo,
    election_client: Option<ElectionClientRef>,
    mut svc_shutdown_rx: WatchReceiver<()>,
) -> MetaResult<()> {
    tracing::info!("Defining leader services");
    let prometheus_endpoint = opts.prometheus_endpoint.clone();
    let env = MetaSrvEnv::<S>::new(opts, meta_store.clone(), current_leader.clone()).await;
    let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await.unwrap());
    let meta_metrics = Arc::new(MetaMetrics::new());
    let registry = meta_metrics.registry();
    monitor_process(registry).unwrap();

    let cluster_manager = Arc::new(
        ClusterManager::new(env.clone(), max_heartbeat_interval)
            .await
            .unwrap(),
    );
    let heartbeat_srv = HeartbeatServiceImpl::new(cluster_manager.clone());

    let compactor_manager = Arc::new(
        hummock::CompactorManager::with_meta(env.clone(), max_heartbeat_interval.as_secs())
            .await
            .unwrap(),
    );

    let hummock_manager = hummock::HummockManager::new(
        env.clone(),
        cluster_manager.clone(),
        meta_metrics.clone(),
        compactor_manager.clone(),
    )
    .await
    .unwrap();

    #[cfg(not(madsim))]
    if let Some(ref dashboard_addr) = address_info.dashboard_addr {
        let dashboard_service = crate::dashboard::DashboardService {
            dashboard_addr: *dashboard_addr,
            cluster_manager: cluster_manager.clone(),
            fragment_manager: fragment_manager.clone(),
            meta_store: env.meta_store_ref(),
            prometheus_endpoint: prometheus_endpoint.clone(),
            prometheus_client: prometheus_endpoint.as_ref().map(|x| {
                use std::str::FromStr;
                prometheus_http_query::Client::from_str(x).unwrap()
            }),
        };
        // TODO: join dashboard service back to local thread.
        tokio::spawn(dashboard_service.serve(address_info.ui_path));
    }

    let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await.unwrap());

    let (barrier_scheduler, scheduled_barriers) =
        BarrierScheduler::new_pair(hummock_manager.clone(), env.opts.checkpoint_frequency);

    let source_manager = Arc::new(
        SourceManager::new(
            env.opts.connector_rpc_endpoint.clone(),
            barrier_scheduler.clone(),
            catalog_manager.clone(),
            fragment_manager.clone(),
        )
        .await
        .unwrap(),
    );

    let barrier_manager = Arc::new(GlobalBarrierManager::new(
        scheduled_barriers,
        env.clone(),
        cluster_manager.clone(),
        catalog_manager.clone(),
        fragment_manager.clone(),
        hummock_manager.clone(),
        source_manager.clone(),
        meta_metrics.clone(),
    ));

    {
        let source_manager = source_manager.clone();
        tokio::spawn(async move {
            source_manager.run().await.unwrap();
        });
    }

    let stream_manager = Arc::new(
        GlobalStreamManager::new(
            env.clone(),
            fragment_manager.clone(),
            barrier_scheduler.clone(),
            cluster_manager.clone(),
            source_manager.clone(),
            hummock_manager.clone(),
        )
        .unwrap(),
    );

    hummock_manager
        .purge_stale(
            &fragment_manager
                .list_table_fragments()
                .await
                .expect("list_table_fragments"),
        )
        .await
        .unwrap();

    // Initialize services.
    let backup_object_store = Arc::new(
        parse_remote_object_store(
            &env.opts.backup_storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            true,
            "Meta Backup",
        )
        .await,
    );
    let backup_storage = Arc::new(
        ObjectStoreMetaSnapshotStorage::new(
            &env.opts.backup_storage_directory,
            backup_object_store,
        )
        .await?,
    );
    let backup_manager = Arc::new(BackupManager::new(
        env.clone(),
        hummock_manager.clone(),
        backup_storage,
        meta_metrics.registry().clone(),
    ));
    let vacuum_manager = Arc::new(hummock::VacuumManager::new(
        env.clone(),
        hummock_manager.clone(),
        backup_manager.clone(),
        compactor_manager.clone(),
    ));

    let ddl_srv = DdlServiceImpl::<S>::new(
        env.clone(),
        catalog_manager.clone(),
        stream_manager.clone(),
        source_manager.clone(),
        cluster_manager.clone(),
        fragment_manager.clone(),
        barrier_manager.clone(),
    );

    let user_srv = UserServiceImpl::<S>::new(env.clone(), catalog_manager.clone());

    let scale_srv = ScaleServiceImpl::<S>::new(
        barrier_scheduler.clone(),
        fragment_manager.clone(),
        cluster_manager.clone(),
        source_manager,
        catalog_manager.clone(),
        stream_manager.clone(),
    );

    let cluster_srv = ClusterServiceImpl::<S>::new(cluster_manager.clone());
    let stream_srv = StreamServiceImpl::<S>::new(
        env.clone(),
        barrier_scheduler.clone(),
        fragment_manager.clone(),
    );
    let hummock_srv = HummockServiceImpl::new(
        hummock_manager.clone(),
        compactor_manager.clone(),
        vacuum_manager.clone(),
        fragment_manager.clone(),
    );
    let notification_srv = NotificationServiceImpl::new(
        env.clone(),
        catalog_manager,
        cluster_manager.clone(),
        hummock_manager.clone(),
        fragment_manager.clone(),
        backup_manager.clone(),
    );
    let health_srv = HealthServiceImpl::new();
    let backup_srv = BackupServiceImpl::new(backup_manager);

    if let Some(prometheus_addr) = address_info.prometheus_addr {
        MetricsManager::boot_metrics_service(
            prometheus_addr.to_string(),
            meta_metrics.registry().clone(),
        )
    }

    let compaction_scheduler = Arc::new(CompactionScheduler::new(
        env.clone(),
        hummock_manager.clone(),
        compactor_manager.clone(),
    ));

    // sub_tasks executed concurrently. Can be shutdown via shutdown_all
    let mut sub_tasks =
        hummock::start_hummock_workers(vacuum_manager, compaction_scheduler, &env.opts);
    sub_tasks.push(
        ClusterManager::start_worker_num_monitor(
            cluster_manager.clone(),
            Duration::from_secs(env.opts.node_num_monitor_interval_sec),
            meta_metrics.clone(),
        )
        .await,
    );
    sub_tasks.push(HummockManager::start_compaction_heartbeat(hummock_manager).await);

    if cfg!(not(test)) {
        sub_tasks.push(
            ClusterManager::start_heartbeat_checker(cluster_manager, Duration::from_secs(1)).await,
        );
        sub_tasks.push(GlobalBarrierManager::start(barrier_manager).await);
    }
    let (idle_send, idle_recv) = tokio::sync::oneshot::channel();
    sub_tasks.push(
        IdleManager::start_idle_checker(env.idle_manager_ref(), Duration::from_secs(30), idle_send)
            .await,
    );

    let shutdown_all = async move {
        for (join_handle, shutdown_sender) in sub_tasks {
            if let Err(_err) = shutdown_sender.send(()) {
                // Maybe it is already shut down
                continue;
            }
            if let Err(err) = join_handle.await {
                tracing::warn!("Failed to join shutdown: {:?}", err);
            }
        }
    };

    let leader_srv = LeaderServiceImpl::new(election_client, current_leader);

    tonic::transport::Server::builder()
        .layer(MetricsMiddlewareLayer::new(meta_metrics))
        .add_service(HeartbeatServiceServer::new(heartbeat_srv))
        .add_service(ClusterServiceServer::new(cluster_srv))
        .add_service(StreamManagerServiceServer::new(stream_srv))
        .add_service(HummockManagerServiceServer::new(hummock_srv))
        .add_service(NotificationServiceServer::new(notification_srv))
        .add_service(LeaderServiceServer::new(leader_srv))
        .add_service(DdlServiceServer::new(ddl_srv))
        .add_service(UserServiceServer::new(user_srv))
        .add_service(ScaleServiceServer::new(scale_srv))
        .add_service(HealthServer::new(health_srv))
        .add_service(BackupServiceServer::new(backup_srv))
        .serve_with_shutdown(address_info.listen_addr, async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                res = svc_shutdown_rx.changed() => {
                    match res {
                        Ok(_) => tracing::info!("Shutting down services"),
                        Err(_) => tracing::error!("Service shutdown receiver dropped")
                    }
                    shutdown_all.await;
                },
                _ = idle_recv => {
                    shutdown_all.await;
                },
            }
        })
        .await
        .unwrap();
    Ok(())
}
