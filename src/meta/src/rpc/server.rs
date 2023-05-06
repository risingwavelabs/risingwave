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

use either::Either;
use etcd_client::ConnectOptions;
use futures::future::join_all;
use itertools::Itertools;
use risingwave_common::config::MetaBackend;
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::telemetry::manager::TelemetryManager;
use risingwave_common::telemetry::telemetry_env_enabled;
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_pb::backup_service::backup_service_server::BackupServiceServer;
use risingwave_pb::ddl_service::ddl_service_server::DdlServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::meta_member_service_server::MetaMemberServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::scale_service_server::ScaleServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use risingwave_pb::meta::system_params_service_server::SystemParamsServiceServer;
use risingwave_pb::meta::telemetry_info_service_server::TelemetryInfoServiceServer;
use risingwave_pb::meta::SystemParams;
use risingwave_pb::user::user_service_server::UserServiceServer;
use risingwave_rpc_client::ComputeClientPool;
use tokio::sync::oneshot::{channel as OneChannel, Receiver as OneReceiver};
use tokio::sync::watch;
use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};
use tokio::task::JoinHandle;

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
    SystemParamsManager,
};
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::rpc::election_client::{ElectionClient, EtcdElectionClient};
use crate::rpc::metrics::{start_fragment_info_monitor, start_worker_info_monitor, MetaMetrics};
use crate::rpc::service::backup_service::BackupServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::meta_member_service::MetaMemberServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::rpc::service::system_params_service::SystemParamsServiceImpl;
use crate::rpc::service::telemetry_service::TelemetryInfoServiceImpl;
use crate::rpc::service::user_service::UserServiceImpl;
use crate::storage::{EtcdMetaStore, MemStore, MetaStore, WrappedEtcdClient as EtcdClient};
use crate::stream::{GlobalStreamManager, SourceManager};
use crate::telemetry::{MetaReportCreator, MetaTelemetryInfoFetcher};
use crate::{hummock, MetaResult};

#[derive(Debug)]
pub enum MetaStoreBackend {
    Etcd {
        endpoints: Vec<String>,
        credentials: Option<(String, String)>,
    },
    Mem,
}

#[derive(Clone)]
pub struct AddressInfo {
    pub advertise_addr: String,
    pub listen_addr: SocketAddr,
    pub prometheus_addr: Option<SocketAddr>,
    pub dashboard_addr: Option<SocketAddr>,
    pub ui_path: Option<String>,
}

impl Default for AddressInfo {
    fn default() -> Self {
        Self {
            advertise_addr: "".to_string(),
            listen_addr: SocketAddr::V4("127.0.0.1:0000".parse().unwrap()),
            prometheus_addr: None,
            dashboard_addr: None,
            ui_path: None,
        }
    }
}

pub type ElectionClientRef = Arc<dyn ElectionClient>;

pub async fn rpc_serve(
    address_info: AddressInfo,
    meta_store_backend: MetaStoreBackend,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
    init_system_params: SystemParams,
) -> MetaResult<(JoinHandle<()>, Option<JoinHandle<()>>, WatchSender<()>)> {
    match meta_store_backend {
        MetaStoreBackend::Etcd {
            endpoints,
            credentials,
        } => {
            let mut options = ConnectOptions::default()
                .with_keep_alive(Duration::from_secs(3), Duration::from_secs(5));
            if let Some((username, password)) = &credentials {
                options = options.with_user(username, password)
            }
            let auth_enabled = credentials.is_some();
            let client =
                EtcdClient::connect(endpoints.clone(), Some(options.clone()), auth_enabled)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to connect etcd {}", e))?;
            let meta_store = Arc::new(EtcdMetaStore::new(client));

            // `with_keep_alive` option will break the long connection in election client.
            let mut election_options = ConnectOptions::default();
            if let Some((username, password)) = &credentials {
                election_options = election_options.with_user(username, password)
            }

            let election_client = Arc::new(
                EtcdElectionClient::new(
                    endpoints,
                    Some(election_options),
                    auth_enabled,
                    address_info.advertise_addr.clone(),
                )
                .await?,
            );

            rpc_serve_with_store(
                meta_store,
                Some(election_client),
                address_info,
                max_heartbeat_interval,
                lease_interval_secs,
                opts,
                init_system_params,
            )
            .await
        }
        MetaStoreBackend::Mem => {
            let meta_store = Arc::new(MemStore::new());
            rpc_serve_with_store(
                meta_store,
                None,
                address_info,
                max_heartbeat_interval,
                lease_interval_secs,
                opts,
                init_system_params,
            )
            .await
        }
    }
}

pub async fn rpc_serve_with_store<S: MetaStore>(
    meta_store: Arc<S>,
    election_client: Option<ElectionClientRef>,
    address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
    init_system_params: SystemParams,
) -> MetaResult<(JoinHandle<()>, Option<JoinHandle<()>>, WatchSender<()>)> {
    let (svc_shutdown_tx, svc_shutdown_rx) = watch::channel(());

    let leader_lost_handle = if let Some(election_client) = election_client.clone() {
        let stop_rx = svc_shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            while let Err(e) = election_client
                .run_once(lease_interval_secs as i64, stop_rx.clone())
                .await
            {
                tracing::error!("election error happened, {}", e.to_string());
            }
        });

        Some(handle)
    } else {
        None
    };

    let join_handle = tokio::spawn(async move {
        if let Some(election_client) = election_client.clone() {
            let mut is_leader_watcher = election_client.subscribe();
            let mut svc_shutdown_rx_clone = svc_shutdown_rx.clone();
            let (follower_shutdown_tx, follower_shutdown_rx) = OneChannel::<()>();

            tokio::select! {
                _ = svc_shutdown_rx_clone.changed() => return,
                res = is_leader_watcher.changed() => {
                    if let Err(err) = res {
                        tracing::error!("leader watcher recv failed {}", err.to_string());
                    }
                }
            }
            let svc_shutdown_rx_clone = svc_shutdown_rx.clone();

            // If not the leader, spawn a follower.
            let follower_handle: Option<JoinHandle<()>> = if !*is_leader_watcher.borrow() {
                let address_info_clone = address_info.clone();

                let election_client_ = election_client.clone();
                Some(tokio::spawn(async move {
                    let _ = tracing::span!(tracing::Level::INFO, "follower services").enter();
                    start_service_as_election_follower(
                        svc_shutdown_rx_clone,
                        follower_shutdown_rx,
                        address_info_clone,
                        Some(election_client_),
                    )
                    .await;
                }))
            } else {
                None
            };

            let mut svc_shutdown_rx_clone = svc_shutdown_rx.clone();
            while !*is_leader_watcher.borrow_and_update() {
                tokio::select! {
                    _ = svc_shutdown_rx_clone.changed() => {
                        return;
                    }
                    res = is_leader_watcher.changed() => {
                        if let Err(err) = res {
                            tracing::error!("leader watcher recv failed {}", err.to_string());
                        }
                    }
                }
            }

            if let Some(handle) = follower_handle {
                let _res = follower_shutdown_tx.send(());
                let _ = handle.await;
            }
        };

        start_service_as_election_leader(
            meta_store,
            address_info,
            max_heartbeat_interval,
            opts,
            init_system_params,
            election_client,
            svc_shutdown_rx,
        )
        .await
        .expect("Unable to start leader services");
    });

    Ok((join_handle, leader_lost_handle, svc_shutdown_tx))
}

/// Starts all services needed for the meta follower node
pub async fn start_service_as_election_follower(
    mut svc_shutdown_rx: WatchReceiver<()>,
    follower_shutdown_rx: OneReceiver<()>,
    address_info: AddressInfo,
    election_client: Option<ElectionClientRef>,
) {
    let meta_member_srv = MetaMemberServiceImpl::new(match election_client {
        None => Either::Right(address_info.clone()),
        Some(election_client) => Either::Left(election_client),
    });

    let health_srv = HealthServiceImpl::new();
    tonic::transport::Server::builder()
        .layer(MetricsMiddlewareLayer::new(Arc::new(MetaMetrics::new())))
        .add_service(MetaMemberServiceServer::new(meta_member_srv))
        .add_service(HealthServer::new(health_srv))
        .serve_with_shutdown(address_info.listen_addr, async move {
            tokio::select! {
                // shutdown service if all services should be shut down
                res = svc_shutdown_rx.changed() => {
                    match res {
                        Ok(_) => tracing::info!("Shutting down services"),
                        Err(_) => tracing::error!("Service shutdown sender dropped")
                    }
                },
                // shutdown service if follower becomes leader
                res = follower_shutdown_rx => {
                    match res {
                        Ok(_) => tracing::info!("Shutting down follower services"),
                        Err(_) => tracing::error!("Follower service shutdown sender dropped")
                    }
                },
            }
        })
        .await
        .unwrap();
}

/// Starts all services needed for the meta leader node
/// Only call this function once, since initializing the services multiple times will result in an
/// inconsistent state
///
/// ## Returns
/// Returns an error if the service initialization failed
pub async fn start_service_as_election_leader<S: MetaStore>(
    meta_store: Arc<S>,
    address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    opts: MetaOpts,
    init_system_params: SystemParams,
    election_client: Option<ElectionClientRef>,
    mut svc_shutdown_rx: WatchReceiver<()>,
) -> MetaResult<()> {
    tracing::info!("Defining leader services");
    let prometheus_endpoint = opts.prometheus_endpoint.clone();
    let env = MetaSrvEnv::<S>::new(opts, init_system_params, meta_store.clone()).await?;
    let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await.unwrap());
    let meta_metrics = Arc::new(MetaMetrics::new());
    let registry = meta_metrics.registry();
    monitor_process(registry).unwrap();

    let system_params_manager = env.system_params_manager_ref();
    let system_params_reader = system_params_manager.get_params().await;

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

    let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await.unwrap());
    let hummock_manager = hummock::HummockManager::new(
        env.clone(),
        cluster_manager.clone(),
        meta_metrics.clone(),
        compactor_manager.clone(),
        catalog_manager.clone(),
    )
    .await
    .unwrap();

    let meta_member_srv = MetaMemberServiceImpl::new(match election_client.clone() {
        None => Either::Right(address_info.clone()),
        Some(election_client) => Either::Left(election_client),
    });

    #[cfg(not(madsim))]
    if let Some(ref dashboard_addr) = address_info.dashboard_addr {
        let dashboard_service = crate::dashboard::DashboardService {
            dashboard_addr: *dashboard_addr,
            prometheus_endpoint: prometheus_endpoint.clone(),
            prometheus_client: prometheus_endpoint.as_ref().map(|x| {
                use std::str::FromStr;
                prometheus_http_query::Client::from_str(x).unwrap()
            }),
            cluster_manager: cluster_manager.clone(),
            fragment_manager: fragment_manager.clone(),
            compute_clients: ComputeClientPool::default(),
            meta_store: env.meta_store_ref(),
        };
        // TODO: join dashboard service back to local thread.
        tokio::spawn(dashboard_service.serve(address_info.ui_path));
    }

    let (barrier_scheduler, scheduled_barriers) = BarrierScheduler::new_pair(
        hummock_manager.clone(),
        meta_metrics.clone(),
        system_params_reader.checkpoint_frequency() as usize,
    );

    let source_manager = Arc::new(
        SourceManager::new(
            env.opts.connector_rpc_endpoint.clone(),
            barrier_scheduler.clone(),
            catalog_manager.clone(),
            fragment_manager.clone(),
            meta_metrics.clone(),
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
        .purge(
            &catalog_manager
                .list_tables()
                .await
                .into_iter()
                .map(|t| t.id)
                .collect_vec(),
        )
        .await?;

    // Initialize services.
    let backup_manager = BackupManager::new(
        env.clone(),
        hummock_manager.clone(),
        meta_metrics.clone(),
        system_params_reader.backup_storage_url(),
        system_params_reader.backup_storage_directory(),
    )
    .await?;
    let vacuum_manager = Arc::new(hummock::VacuumManager::new(
        env.clone(),
        hummock_manager.clone(),
        backup_manager.clone(),
        compactor_manager.clone(),
    ));

    let mut aws_cli = None;
    if let Some(my_vpc_id) = &env.opts.vpc_id && let Some(security_group_id) = &env.opts.security_group_id {
        let cli = AwsEc2Client::new(my_vpc_id, security_group_id).await;
        aws_cli = Some(cli);
    }

    let ddl_srv = DdlServiceImpl::<S>::new(
        env.clone(),
        aws_cli,
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
        stream_manager.clone(),
        catalog_manager.clone(),
        fragment_manager.clone(),
    );
    let hummock_srv = HummockServiceImpl::new(
        hummock_manager.clone(),
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
    let telemetry_srv = TelemetryInfoServiceImpl::new(meta_store.clone());
    let system_params_srv = SystemParamsServiceImpl::new(system_params_manager.clone());

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
    let mut sub_tasks = hummock::start_hummock_workers(
        hummock_manager.clone(),
        vacuum_manager,
        compaction_scheduler,
        &env.opts,
    );
    sub_tasks.push(
        start_worker_info_monitor(
            cluster_manager.clone(),
            election_client.clone(),
            Duration::from_secs(env.opts.node_num_monitor_interval_sec),
            meta_metrics.clone(),
        )
        .await,
    );
    sub_tasks.push(
        start_fragment_info_monitor(
            cluster_manager.clone(),
            fragment_manager.clone(),
            meta_metrics.clone(),
        )
        .await,
    );
    sub_tasks.push(SystemParamsManager::start_params_notifier(system_params_manager.clone()).await);
    sub_tasks.push(HummockManager::start_compaction_heartbeat(hummock_manager.clone()).await);
    sub_tasks.push(HummockManager::start_lsm_stat_report(hummock_manager).await);

    if cfg!(not(test)) {
        sub_tasks.push(
            ClusterManager::start_heartbeat_checker(
                cluster_manager.clone(),
                Duration::from_secs(1),
            )
            .await,
        );
        sub_tasks.push(GlobalBarrierManager::start(barrier_manager).await);
    }
    let (idle_send, idle_recv) = tokio::sync::oneshot::channel();
    sub_tasks.push(
        IdleManager::start_idle_checker(env.idle_manager_ref(), Duration::from_secs(30), idle_send)
            .await,
    );

    let (abort_sender, abort_recv) = tokio::sync::oneshot::channel();
    let notification_mgr = env.notification_manager_ref();
    let stream_abort_handler = tokio::spawn(async move {
        abort_recv.await.unwrap();
        notification_mgr.abort_all().await;
        compactor_manager.abort_all_compactors();
    });
    sub_tasks.push((stream_abort_handler, abort_sender));

    let local_system_params_manager = LocalSystemParamsManager::new(system_params_reader.clone());

    let mgr = TelemetryManager::new(
        local_system_params_manager.watch_params(),
        Arc::new(MetaTelemetryInfoFetcher::new(env.cluster_id().clone())),
        Arc::new(MetaReportCreator::new(
            cluster_manager,
            meta_store.meta_store_type(),
        )),
    );

    // May start telemetry reporting
    if let MetaBackend::Etcd = meta_store.meta_store_type() && env.opts.telemetry_enabled && telemetry_env_enabled(){
        if system_params_reader.telemetry_enabled(){
            mgr.start_telemetry_reporting().await;
        }
        sub_tasks.push(mgr.watch_params_change());
    } else {
        tracing::info!("Telemetry didn't start due to meta backend or config");
    }

    let shutdown_all = async move {
        let mut handles = Vec::with_capacity(sub_tasks.len());

        for (join_handle, shutdown_sender) in sub_tasks {
            if let Err(_err) = shutdown_sender.send(()) {
                continue;
            }

            handles.push(join_handle);
        }

        // The barrier manager can't be shutdown gracefully if it's under recovering, try to
        // abort it using timeout.
        match tokio::time::timeout(Duration::from_secs(1), join_all(handles)).await {
            Ok(results) => {
                for result in results {
                    if let Err(err) = result {
                        tracing::warn!("Failed to join shutdown: {:?}", err);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Join shutdown timeout: {:?}", e);
            }
        }
    };

    // Persist params before starting services so that invalid params that cause meta node
    // to crash will not be persisted.
    system_params_manager.flush_params().await?;
    env.cluster_id().put_at_meta_store(&meta_store).await?;

    tracing::info!("Assigned cluster id {:?}", *env.cluster_id());
    tracing::info!("Starting meta services");

    tonic::transport::Server::builder()
        .layer(MetricsMiddlewareLayer::new(meta_metrics))
        .add_service(HeartbeatServiceServer::new(heartbeat_srv))
        .add_service(ClusterServiceServer::new(cluster_srv))
        .add_service(StreamManagerServiceServer::new(stream_srv))
        .add_service(HummockManagerServiceServer::new(hummock_srv))
        .add_service(NotificationServiceServer::new(notification_srv))
        .add_service(MetaMemberServiceServer::new(meta_member_srv))
        .add_service(DdlServiceServer::new(ddl_srv))
        .add_service(UserServiceServer::new(user_srv))
        .add_service(ScaleServiceServer::new(scale_srv))
        .add_service(HealthServer::new(health_srv))
        .add_service(BackupServiceServer::new(backup_srv))
        .add_service(SystemParamsServiceServer::new(system_params_srv))
        .add_service(TelemetryInfoServiceServer::new(telemetry_srv))
        .serve_with_shutdown(address_info.listen_addr, async move {
            tokio::select! {
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
