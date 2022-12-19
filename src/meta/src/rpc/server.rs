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

use etcd_client::ConnectOptions;
use risingwave_backup::storage::ObjectStoreMetaSnapshotStorage;
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::backup_service::backup_service_server::BackupServiceServer;
use risingwave_pb::ddl_service::ddl_service_server::DdlServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::scale_service_server::ScaleServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use risingwave_pb::user::user_service_server::UserServiceServer;
use tokio::task::JoinHandle;

use super::elections::run_elections;
use super::intercept::MetricsMiddlewareLayer;
use super::leader_svs::get_leader_srv;
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
use crate::rpc::service::backup_service::BackupServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::rpc::service::user_service::UserServiceImpl;
use crate::storage::{EtcdMetaStore, MemStore, MetaStore, WrappedEtcdClient as EtcdClient};
use crate::stream::{GlobalStreamManager, SourceManager};
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
    pub addr: String,
    pub listen_addr: SocketAddr,
    pub prometheus_addr: Option<SocketAddr>,
    pub dashboard_addr: Option<SocketAddr>,
    pub ui_path: Option<String>,
}

impl Default for AddressInfo {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:0000".to_string(),
            listen_addr: SocketAddr::V4("127.0.0.1:0000".parse().unwrap()),
            prometheus_addr: None,
            dashboard_addr: None,
            ui_path: None,
        }
    }
}

// TODO: in imports do something like tokio::sync::watch::Sender<()> as watchSender
// and oneshot::sender as oneshotSender

pub async fn rpc_serve(
    address_info: AddressInfo,
    meta_store_backend: MetaStoreBackend,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
) -> MetaResult<(JoinHandle<()>, tokio::sync::watch::Sender<()>)> {
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
            let client = EtcdClient::connect(endpoints, Some(options), credentials.is_some())
                .await
                .map_err(|e| anyhow::anyhow!("failed to connect etcd {}", e))?;
            let meta_store = Arc::new(EtcdMetaStore::new(client));
            rpc_serve_with_store(
                meta_store,
                address_info,
                max_heartbeat_interval,
                lease_interval_secs,
                opts,
            )
            .await
        }
        MetaStoreBackend::Mem => {
            let meta_store = Arc::new(MemStore::new());
            rpc_serve_with_store(
                meta_store,
                address_info,
                max_heartbeat_interval,
                lease_interval_secs,
                opts,
            )
            .await
        }
    }
}

pub async fn rpc_serve_with_store<S: MetaStore>(
    meta_store: Arc<S>,
    address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
) -> MetaResult<(JoinHandle<()>, tokio::sync::watch::Sender<()>)> {
    // Contains address info with port
    //   address_info.listen_addr;

    // Initialize managers.
    let (_, lease_handle, lease_shutdown, leader_rx) = run_elections(
        address_info.listen_addr.clone().to_string(),
        meta_store.clone(),
        lease_interval_secs,
    )
    .await?;

    let mut services_leader_rx = leader_rx.clone();
    let mut note_status_leader_rx = leader_rx.clone();

    // FIXME: add fencing mechanism
    // https://github.com/risingwavelabs/risingwave/issues/6786

    // print current leader/follower status of this node
    tokio::spawn(async move {
        let span = tracing::span!(tracing::Level::INFO, "node_status");
        let _enter = span.enter();
        loop {
            if note_status_leader_rx.changed().await.is_err() {
                panic!("Leader sender dropped");
            }
            let (leader_info, is_leader) = note_status_leader_rx.borrow().clone();
            let leader_addr = HostAddr::from(leader_info);

            tracing::info!(
                "This node currently is a {} at {}:{}",
                if is_leader {
                    "leader. Serving"
                } else {
                    "follower. Leader serving"
                },
                leader_addr.host,
                leader_addr.port
            );
        }
    });

    // TODO: maybe do not use a channel here
    // What I need is basically a oneshot channel with one producer and multiple consumers
    let (svc_shutdown_tx, mut svc_shutdown_rx) = tokio::sync::watch::channel(());

    let join_handle = tokio::spawn(async move {
        let span = tracing::span!(tracing::Level::INFO, "services");
        let _enter = span.enter();

        // failover logic
        if services_leader_rx.changed().await.is_err() {
            panic!("Leader sender dropped");
        }

        let is_leader = services_leader_rx.borrow().clone().1;
        let was_follower = !is_leader;

        // FIXME: Add service discovery for follower
        // https://github.com/risingwavelabs/risingwave/issues/6755

        // run follower services until node becomes leader
        let mut svc_shutdown_rx_clone = svc_shutdown_rx.clone();
        let (follower_shutdown_tx, follower_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let (follower_finished_tx, follower_finished_rx) = tokio::sync::oneshot::channel::<()>();
        if !is_leader {
            tracing::info!("Starting follower services");
            tokio::spawn(async move {
                let health_srv = HealthServiceImpl::new();
                // TODO: Use prometheus endpoint in follower?
                tonic::transport::Server::builder()
                    .layer(MetricsMiddlewareLayer::new(Arc::new(MetaMetrics::new())))
                    .add_service(HealthServer::new(health_srv))
                    .serve_with_shutdown(address_info.listen_addr, async move {
                        tokio::select! {
                        _ = tokio::signal::ctrl_c() => {},
                        // shutdown service if all services should be shut down
                        res = svc_shutdown_rx_clone.changed() =>  {
                            if res.is_err() {
                                tracing::error!("service shutdown sender dropped");
                            }
                        },
                        // shutdown service if follower becomes leader
                        res = follower_shutdown_rx =>  {
                              if res.is_err() {
                                  tracing::error!("follower service shutdown sender dropped");
                              }
                            },
                          }
                    })
                    .await
                    .unwrap();
                match follower_finished_tx.send(()) {
                    Ok(_) => tracing::info!("Signaling that follower service is down"),
                    Err(_) => tracing::error!(
                        "Error when signaling that follower service is down. Receiver dropped"
                    ),
                }
            });
        }

        // wait until this node becomes a leader
        while !services_leader_rx.borrow().clone().1 {
            if services_leader_rx.changed().await.is_err() {
                panic!("Leader sender dropped");
            }
        }

        // shut down follower svc if node used to be follower
        if was_follower {
            match follower_shutdown_tx.send(()) {
                // TODO: Do I always use this error format?
                // Receiver, sender dropped?
                Ok(_) => tracing::info!("Shutting down follower services"),
                Err(_) => tracing::error!("Follower service receiver dropped"),
            }
            // Wait until follower service is down
            match follower_finished_rx.await {
                Ok(_) => tracing::info!("Follower services shut down"),
                Err(_) => tracing::error!("Follower service shutdown finished sender dropped"),
            };
        }

        tracing::info!("Starting leader services");

        let svc = get_leader_srv(
            meta_store,
            address_info.clone(),
            max_heartbeat_interval,
            opts,
            leader_rx,
            lease_handle,
            lease_shutdown,
        )
        .await
        .expect("Unable to create leader services");

        let shutdown_all = async move {
            for (join_handle, shutdown_sender) in svc.sub_tasks {
                if let Err(_err) = shutdown_sender.send(()) {
                    // Maybe it is already shut down
                    continue;
                }
                if let Err(err) = join_handle.await {
                    tracing::warn!("Failed to join shutdown: {:?}", err);
                }
            }
        };

        // FIXME: Add service discovery for leader
        // https://github.com/risingwavelabs/risingwave/issues/6755

        tonic::transport::Server::builder()
            .layer(MetricsMiddlewareLayer::new(svc.meta_metrics.clone()))
            .add_service(HeartbeatServiceServer::new(svc.heartbeat_srv))
            .add_service(ClusterServiceServer::new(svc.cluster_srv))
            .add_service(StreamManagerServiceServer::new(svc.stream_srv))
            .add_service(HummockManagerServiceServer::new(svc.hummock_srv))
            .add_service(NotificationServiceServer::new(svc.notification_srv))
            .add_service(DdlServiceServer::new(svc.ddl_srv))
            .add_service(UserServiceServer::new(svc.user_srv))
            .add_service(ScaleServiceServer::new(svc.scale_srv))
            .add_service(HealthServer::new(svc.health_srv))
            .add_service(BackupServiceServer::new(svc.backup_srv))
            .serve_with_shutdown(address_info.listen_addr, async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    res = svc_shutdown_rx.changed() => {
                        tracing::info!("svc_shutdown_rx.changed()");
                        if res.is_err() {
                            tracing::error!("service shutdown sender dropped");
                        }
                        shutdown_all.await;
                    },
                    _ = svc.idle_recv => {
                        tracing::info!("idle_recv");
                        shutdown_all.await;
                    },
                }
            })
            .await
            .unwrap();
    });

    Ok((join_handle, svc_shutdown_tx))
}
