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

use etcd_client::{Client as EtcdClient, ConnectOptions};
use prost::Message;
use risingwave_backup::storage::ObjectStoreMetaSnapshotStorage;
use risingwave_common::bail;
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::backup_service::backup_service_server::BackupServiceServer;
use risingwave_pb::common::HostAddress;
use risingwave_pb::ddl_service::ddl_service_server::DdlServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::leader_service_server::LeaderServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::scale_service_server::ScaleServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use risingwave_pb::meta::MetaLeaderInfo;
use risingwave_pb::user::user_service_server::UserServiceServer;
use tokio::sync::oneshot::Sender;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tonic::service::Interceptor;
use tonic::{Request, Status};

use super::elections::run_elections;
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
use crate::rpc::service::backup_service::BackupServiceImpl;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::leader_service::LeaderServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::rpc::service::user_service::UserServiceImpl;
use crate::storage::{EtcdMetaStore, MemStore, MetaStore};
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
            if let Some((username, password)) = credentials {
                options = options.with_user(username, password)
            }
            let client = EtcdClient::connect(endpoints, Some(options))
                .await
                .map_err(|e| anyhow::anyhow!("failed to connect etcd {}", e))?;
            let meta_store = Arc::new(EtcdMetaStore::new(client));
            let x = address_info.clone().addr;
            tracing::info!("meta_store addr {:?}", x);
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

/// Wrapper for `elections::run_elections`
/// Start single leader setup
///
/// ## Arguments
/// `addr`: Address of the current leader, e.g. "127.0.0.1"
/// `meta_store`: Store that will hold information about the leader
/// `lease_time_sec`: Time that a lease will be valid for.
/// If this `lease_time_sec` is large, elections will be less frequent, resulting in less traffic
/// for the meta store, but node failover may be slow If this `lease_time_sec` is small, elections
/// will be more frequent, resulting in more traffic for the meta store. Node failover will be fast
///
/// ## Returns
/// `MetaLeaderInfo` containing the leader who got initially elected
/// `JoinHandle` running all future elections concurrently
/// `Sender` for signaling a shutdown
/// `Receiver` receiving true if this node got elected as leader and false if it is a follower
pub async fn register_leader_for_meta<S: MetaStore>(
    addr: String,
    meta_store: Arc<S>,
    lease_time_sec: u64,
) -> MetaResult<(
    MetaLeaderInfo,
    JoinHandle<()>,
    Sender<()>,
    Receiver<(HostAddress, bool)>,
)> {
    run_elections(addr, meta_store, lease_time_sec).await
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
    let (info, lease_handle, lease_shutdown, leader_rx) = register_leader_for_meta(
        address_info.listen_addr.clone().to_string(),
        meta_store.clone(),
        lease_interval_secs,
    )
    .await?;

    let mut services_leader_rx = leader_rx.clone();
    let f_leader_svc_leader_rx = leader_rx.clone();
    let l_leader_svc_leader_rx = leader_rx.clone();
    let mut note_status_leader_rx = leader_rx.clone();

    // print current leader/follower status of this node + fencing mechanism
    tokio::spawn(async move {
        let span = tracing::span!(tracing::Level::INFO, "node_status");
        let _enter = span.enter();
        let mut was_leader = false;
        loop {
            if note_status_leader_rx.changed().await.is_err() {
                panic!("Leader sender dropped");
            }

            let (leader_addr, is_leader) = note_status_leader_rx.borrow().clone();

            // Implementation of naive fencing mechanism:
            // leader nodes should panic if they loose their leader position
            if was_leader {
                // && !is_leader {
                // TODO: enable this again to give leader chance to claim lease again?
                panic!(
                    "This node lost its leadership. New host address is {}:{}. Killing node",
                    leader_addr.host, leader_addr.port
                )
            }
            was_leader = is_leader;
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

    // TODO:
    // have a look at bug.txt

    // TODO:
    // Remove the interceptor pattern again

    // TODO:
    // we only can define leader services when they are needed. Otherwise they already do things
    // FIXME: Start leader services if follower becomes leader

    // TODO: maybe do not use a channel here
    // What I need is basically a oneshot channel with one producer and multiple consumers
    let (svc_shutdown_tx, mut svc_shutdown_rx) = tokio::sync::watch::channel(());

    let join_handle = tokio::spawn(async move {
        let span = tracing::span!(tracing::Level::INFO, "services");
        let _enter = span.enter();

        let intercept = InterceptorWrapper { leader_rx };

        // failover logic

        if services_leader_rx.changed().await.is_err() {
            panic!("Leader sender dropped");
        }

        let is_leader = services_leader_rx.borrow().clone().1;
        let was_follower = !is_leader;
        let intercept_clone = intercept.clone();

        // TODO: remove interceptor. Not needed if we have LeaderService
        let leader_srv = LeaderServiceImpl::new(f_leader_svc_leader_rx);

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
                    .add_service(HealthServer::with_interceptor(health_srv, intercept_clone))
                    .add_service(LeaderServiceServer::new(leader_srv))
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
            // Wait until follower service is
            match follower_finished_rx.await {
                Ok(_) => tracing::info!("Follower services shut down"),
                Err(_) => tracing::error!("Follower service shutdown finished sender dropped"),
            };
        }

        // leader services defined below
        tracing::info!("Starting leader services");

        // TODO: put leader service definition in separate function or maybe even separate file
        let prometheus_endpoint = opts.prometheus_endpoint.clone();
        let env = MetaSrvEnv::<S>::new(opts, meta_store.clone(), info).await;
        let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await.unwrap());
        let meta_metrics = Arc::new(MetaMetrics::new());
        let registry = meta_metrics.registry();
        monitor_process(registry).unwrap();
        let compactor_manager = Arc::new(
            hummock::CompactorManager::with_meta(env.clone(), max_heartbeat_interval.as_secs())
                .await
                .unwrap(),
        );

        let cluster_manager = Arc::new(
            ClusterManager::new(env.clone(), max_heartbeat_interval)
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
            )
            .await,
        );
        let backup_storage = Arc::new(
            ObjectStoreMetaSnapshotStorage::new(
                &env.opts.backup_storage_directory,
                backup_object_store,
            )
            .await
            .unwrap(),
            // FIXME: Do not use unwrap here
        );
        let backup_manager = Arc::new(BackupManager::new(
            env.clone(),
            hummock_manager.clone(),
            backup_storage,
        ));
        let vacuum_manager = Arc::new(hummock::VacuumManager::new(
            env.clone(),
            hummock_manager.clone(),
            backup_manager.clone(),
            compactor_manager.clone(),
        ));

        let heartbeat_srv = HeartbeatServiceImpl::new(cluster_manager.clone());
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
        );
        let health_srv = HealthServiceImpl::new();
        let backup_srv = BackupServiceImpl::new(backup_manager);

        if let Some(prometheus_addr) = address_info.prometheus_addr {
            MetricsManager::boot_metrics_service(
                prometheus_addr.to_string(),
                meta_metrics.registry().clone(),
            )
        }

        // Initialize sub-tasks.
        let compaction_scheduler = Arc::new(CompactionScheduler::new(
            env.clone(),
            hummock_manager.clone(),
            compactor_manager.clone(),
        ));
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
        sub_tasks.push((lease_handle, lease_shutdown));
        if cfg!(not(test)) {
            sub_tasks.push(
                ClusterManager::start_heartbeat_checker(cluster_manager, Duration::from_secs(1))
                    .await,
            );
            sub_tasks.push(GlobalBarrierManager::start(barrier_manager).await);
        }

        let (idle_send, mut idle_recv) = tokio::sync::oneshot::channel();
        sub_tasks.push(
            IdleManager::start_idle_checker(
                env.idle_manager_ref(),
                Duration::from_secs(30),
                idle_send,
            )
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

        let leader_srv = LeaderServiceImpl::new(l_leader_svc_leader_rx);

        // leader services defined above

        tonic::transport::Server::builder()
            .layer(MetricsMiddlewareLayer::new(meta_metrics.clone()))
            .add_service(LeaderServiceServer::new(leader_srv))
            .add_service(HeartbeatServiceServer::with_interceptor(
                heartbeat_srv,
                intercept.clone(),
            ))
            .add_service(ClusterServiceServer::with_interceptor(
                cluster_srv,
                intercept.clone(),
            ))
            .add_service(StreamManagerServiceServer::with_interceptor(
                stream_srv,
                intercept.clone(),
            ))
            .add_service(HummockManagerServiceServer::with_interceptor(
                hummock_srv,
                intercept.clone(),
            ))
            .add_service(NotificationServiceServer::with_interceptor(
                notification_srv,
                intercept.clone(),
            ))
            .add_service(DdlServiceServer::with_interceptor(
                ddl_srv,
                intercept.clone(),
            ))
            .add_service(UserServiceServer::with_interceptor(
                user_srv,
                intercept.clone(),
            ))
            .add_service(ScaleServiceServer::with_interceptor(
                scale_srv,
                intercept.clone(),
            ))
            .add_service(HealthServer::with_interceptor(
                health_srv,
                intercept.clone(),
            ))
            .add_service(BackupServiceServer::with_interceptor(
                backup_srv,
                intercept.clone(),
            ))
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
                    _ = idle_recv => {
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

#[derive(Clone)]
struct InterceptorWrapper {
    leader_rx: Receiver<(HostAddress, bool)>,
}

impl Interceptor for InterceptorWrapper {
    fn call(&mut self, req: Request<()>) -> std::result::Result<Request<()>, Status> {
        let (addr, is_leader) = self.leader_rx.borrow().clone();
        if !is_leader {
            return Err(Status::aborted(format!(
                "http://{}:{}",
                addr.get_host(),
                addr.get_port(),
            ))); // TODO: Do this as
                 // json
        }
        Ok(req)
    }
}
