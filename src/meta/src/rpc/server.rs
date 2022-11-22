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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client as EtcdClient, ConnectOptions};
use prost::Message;
use rand::Rng;
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common_service::metrics_manager::MetricsManager;
use risingwave_pb::ddl_service::ddl_service_server::DdlServiceServer;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerServiceServer;
use risingwave_pb::meta::cluster_service_server::ClusterServiceServer;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatServiceServer;
use risingwave_pb::meta::notification_service_server::NotificationServiceServer;
use risingwave_pb::meta::scale_service_server::ScaleServiceServer;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerServiceServer;
use risingwave_pb::meta::{MetaLeaderInfo, MetaLeaseInfo};
use risingwave_pb::user::user_service_server::UserServiceServer;
use tokio::sync::oneshot::Sender;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;

use super::intercept::MetricsMiddlewareLayer;
use super::service::health_service::HealthServiceImpl;
use super::service::notification_service::NotificationServiceImpl;
use super::service::scale_service::ScaleServiceImpl;
use super::DdlServiceImpl;
use crate::barrier::{BarrierScheduler, GlobalBarrierManager};
use crate::hummock::{CompactionScheduler, HummockManager};
use crate::manager::{
    CatalogManager, ClusterManager, FragmentManager, IdleManager, MetaOpts, MetaSrvEnv,
};
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::service::cluster_service::ClusterServiceImpl;
use crate::rpc::service::heartbeat_service::HeartbeatServiceImpl;
use crate::rpc::service::hummock_service::HummockServiceImpl;
use crate::rpc::service::stream_service::StreamServiceImpl;
use crate::rpc::service::user_service::UserServiceImpl;
use crate::rpc::{META_CF_NAME, META_LEADER_KEY, META_LEASE_KEY};
use crate::storage::{EtcdMetaStore, MemStore, MetaStore, MetaStoreError, Transaction};
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

pub async fn rpc_serve(
    address_info: AddressInfo,
    meta_store_backend: MetaStoreBackend,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
) -> MetaResult<(JoinHandle<()>, Sender<()>)> {
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

// get duration since epoch
fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

struct ElectionResult {
    pub meta_leader_info: MetaLeaderInfo,
    pub meta_lease_info: MetaLeaseInfo,

    // True if current node is leader
    pub is_leader: bool,
}

// Sets a leader. Does not renew leaders term
async fn run_election<S: MetaStore>(
    meta_store: &Arc<S>,
    addr: &String,
    lease_time: u64, // in sec
    lease_id: u64,   // random id identifying the leader via the lease
) -> Option<ElectionResult> {
    tracing::info!("running an election...");

    // below is old code
    // get old leader info and lease
    let (current_leader_info, current_leader_lease) = match get_infos(&meta_store).await {
        None => return None,
        Some(infos) => {
            let (leader, lease) = infos;
            (leader, lease)
        }
    };

    let now = since_epoch();
    if !current_leader_lease.is_empty() {
        // TODO: why do we need this part?
        let lease_info = MetaLeaseInfo::decode(&mut current_leader_lease.as_slice()).unwrap();

        // Lease did not yet expire
        if lease_info.lease_expire_time > now.as_secs()
            && lease_info.leader.as_ref().unwrap().node_address != *addr
        // TODO: all nodes have the same addr. Above line is useless.
        // Will this be different in prod?
        {
            tracing::error!(
                "We already have a leader with a lease that is valid for {} more sec",
                lease_info.lease_expire_time - now.as_secs(),
            );
            return Some(ElectionResult {
                meta_leader_info: MetaLeaderInfo::decode(&mut current_leader_info.as_slice())
                    .unwrap(),
                meta_lease_info: MetaLeaseInfo::decode(&mut current_leader_lease.as_slice())
                    .unwrap(),
                is_leader: lease_id == lease_info.leader.unwrap().lease_id,
            });
        }
    }

    tracing::info!("lease_id: {}", lease_id);

    let leader_info = MetaLeaderInfo {
        lease_id,
        node_address: addr.to_string(),
    };
    tracing::info!("leader_info: {:?}", leader_info);

    let lease_info = MetaLeaseInfo {
        leader: Some(leader_info.clone()),
        lease_register_time: now.as_secs(),
        lease_expire_time: now.as_secs() + lease_time,
    };
    tracing::info!("lease_info: {:?}", lease_info);

    // Initial leader election
    if current_leader_info.is_empty() {
        tracing::info!("We have no leader");

        // cluster has no leader
        if let Err(e) = meta_store
            .put_cf(
                META_CF_NAME,
                META_LEADER_KEY.as_bytes().to_vec(),
                leader_info.encode_to_vec(),
            )
            .await
        {
            tracing::warn!(
                "new cluster put leader info failed, MetaStoreError: {:?}",
                e
            );
            return None;
        }
        // Check if new leader was elected in the meantime
        return match try_acquire_renew_lease(&leader_info, lease_time, &meta_store).await {
            Some(val) => {
                if !val {
                    return None;
                }
                Some(ElectionResult {
                    meta_leader_info: leader_info,
                    meta_lease_info: lease_info,
                    is_leader: true,
                })
            }
            None => None,
        };
    }

    // follow-up election:
    // There has already been a leader before
    let is_leader = match try_acquire_renew_lease(&leader_info, lease_time, meta_store).await {
        None => return None,
        Some(val) => val,
    };

    Some(ElectionResult {
        meta_leader_info: leader_info,
        meta_lease_info: lease_info,
        is_leader,
    })
}

// TODO: write docstring
// Try to renew/acquire the lease of the current leader by lease_time
// Returns true if node was leader and was able to renew/acquire the lease
// Returns false if node was follower and thus could not renew/acquire lease
// Returns None if operation has to be repeated
async fn try_acquire_renew_lease<S: MetaStore>(
    leader_info: &MetaLeaderInfo,
    lease_time_sec: u64,
    meta_store: &Arc<S>,
) -> Option<bool> {
    let now = since_epoch();
    let mut txn = Transaction::default();
    let lease_info = MetaLeaseInfo {
        leader: Some(leader_info.clone()),
        lease_register_time: now.as_secs(),
        lease_expire_time: now.as_secs() + lease_time_sec,
    };
    txn.check_equal(
        META_CF_NAME.to_string(),
        META_LEADER_KEY.as_bytes().to_vec(),
        leader_info.encode_to_vec(),
    );
    txn.put(
        META_CF_NAME.to_string(),
        META_LEASE_KEY.as_bytes().to_vec(),
        lease_info.encode_to_vec(),
    );
    let is_leader = match meta_store.txn(txn).await {
        Err(e) => match e {
            MetaStoreError::TransactionAbort() => {
                // TODO: remove this
                //   tracing::info!("Renew/acquire lease: another node has become new leader");
                false
            }
            MetaStoreError::Internal(e) => {
                tracing::warn!(
                    "Renew/acquire lease: try again later, MetaStoreError: {:?}",
                    e
                );
                // operation has to be repeated
                return None;
            }
            MetaStoreError::ItemNotFound(e) => {
                tracing::warn!("Renew/acquire lease: MetaStoreError: {:?}", e);
                return None;
                // false // TODO: Should we repeat here or should we assume that we are not leader?
                // TODO: Do we have to go to elections if we run into this one?
            }
        },
        Ok(_) => true,
    };
    Some(is_leader)
}

type MetaLeaderInfoVec = Vec<u8>;
type MetaLeaseInfoVec = Vec<u8>;

// TODO: Write docstring
// TODO: return MetaLeaderInfo and MetaLeaderLease types. Or return a infos type that contains both
// of them getting leader_info and leader_lease or defaulting to none
async fn get_infos<S: MetaStore>(
    meta_store: &Arc<S>,
) -> Option<(MetaLeaderInfoVec, MetaLeaseInfoVec)> {
    let current_leader_info = match meta_store
        .get_cf(META_CF_NAME, META_LEADER_KEY.as_bytes())
        .await
    {
        Err(MetaStoreError::ItemNotFound(_)) => vec![],
        Ok(v) => v,
        _ => {
            return None;
        }
    };
    let current_leader_lease = match meta_store
        .get_cf(META_CF_NAME, META_LEASE_KEY.as_bytes())
        .await
    {
        Err(MetaStoreError::ItemNotFound(_)) => vec![],
        Ok(v) => v,
        _ => return None,
    };
    Some((current_leader_info, current_leader_lease))
}

fn gen_rand_lease_id() -> u64 {
    rand::thread_rng().gen_range(0..std::u64::MAX)
}

// TODO: write docstring
// returns true if current node is leader
pub async fn register_leader_for_meta<S: MetaStore>(
    addr: String, // Address of this node
    meta_store: Arc<S>,
    lease_time: u64, // seconds
) -> MetaResult<(MetaLeaderInfo, JoinHandle<()>, Sender<()>, Receiver<bool>)> {
    tracing::info!(
        "addr: {}, meta_store: ???, lease_time: {}",
        addr.clone(),
        lease_time
    );
    let addr_clone = addr.clone();

    // Randomize interval to reduce mitigate likelihood of simultaneous requests
    let rand_delay = (since_epoch().as_micros() % 100) as u64;
    tracing::info!("Pseudo random delay is {}ms", rand_delay);
    let mut ticker = tokio::time::interval(Duration::from_millis(rand_delay / 2 + rand_delay));

    'initial_election: loop {
        ticker.tick().await;

        // every lease gets a random ID to differentiate between leases/leaders
        let mut initial_election = true;
        let init_lease_id = gen_rand_lease_id();

        // run the initial election
        let election_outcome =
            run_election(&meta_store, &addr_clone, lease_time, init_lease_id).await;
        let (leader, is_leader) = match election_outcome {
            Some(infos) => {
                tracing::info!("initial election Succeeded");
                (infos.meta_leader_info, infos.is_leader)
            }
            None => {
                tracing::info!("initial election failed. Repeating election");
                continue 'initial_election;
            }
        };
        tracing::info!("current leader is {:?}", leader);

        // define all follow up elections and terms in handle
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(is_leader);
        let handle = tokio::spawn(async move {
            // election
            'election: loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        tracing::info!("Register leader info is stopped");
                        return;
                    }
                    _ = ticker.tick() => {},
                }

                // lease id of this node
                let lease_id = if initial_election {
                    initial_election = false;
                    init_lease_id
                } else {
                    gen_rand_lease_id()
                };

                let (leader_info, _, is_leader) =
                    match run_election(&meta_store, &addr_clone, lease_time, lease_id).await {
                        None => {
                            tracing::info!("Election failed. Repeating election");
                            continue 'election;
                        }
                        Some(infos) => {
                            tracing::info!("Election succeeded.");
                            (
                                infos.meta_leader_info,
                                infos.meta_lease_info,
                                infos.is_leader,
                            )
                        }
                    };

                // signal to observers if this node currently is leader
                leader_tx.send(is_leader).unwrap();

                // election done. Enter current term in loop below
                'term: loop {
                    // sleep OR abort if shutdown
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            tracing::info!("Register leader info is stopped");
                            return;
                        }
                        _ = ticker.tick() => {},
                    }

                    // renew the current lease if this is the leader
                    let attempt =
                        try_acquire_renew_lease(&leader_info, lease_time, &meta_store).await;
                    if attempt.is_none() {
                        // something went wrong
                        continue 'election;
                    }
                    if attempt.unwrap() {
                        // node is leader and lease was renewed
                        continue 'term;
                    }
                    // node is follower

                    // get leader info
                    let (_, lease_info) = get_infos(&meta_store).await.unwrap_or_default();
                    if lease_info.is_empty() {
                        // ETCD does not have leader lease. Elect new leader
                        tracing::info!("ETCD does not have leader lease. Running new election");
                        continue 'election;
                    }

                    // delete lease and run new election if lease is expired for some time
                    let some_time = lease_time / 2;
                    let lease_info = MetaLeaseInfo::decode(&mut lease_info.as_slice()).unwrap();
                    if lease_info.get_lease_expire_time() + some_time < since_epoch().as_secs() {
                        tracing::warn!("Detected that leader is down");
                        let mut txn = Transaction::default();
                        txn.delete(
                            META_CF_NAME.to_string(),
                            META_LEADER_KEY.as_bytes().to_vec(),
                        );
                        txn.delete(META_CF_NAME.to_string(), META_LEASE_KEY.as_bytes().to_vec());
                        match meta_store.txn(txn).await {
                            Err(e) => tracing::warn!("Unable to update lease. Error {}", e),
                            Ok(_) => tracing::info!("Deleted leader and lease"),
                        }
                        continue 'election;
                    }
                    // lease exists and leader continues term
                }
            }
        });
        return Ok((leader, handle, shutdown_tx, leader_rx));
    }
}

pub async fn rpc_serve_with_store<S: MetaStore>(
    meta_store: Arc<S>,
    mut address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
) -> MetaResult<(JoinHandle<()>, Sender<()>)> {
    // Initialize managers.
    let (info, lease_handle, lease_shutdown, mut leader_rx) = register_leader_for_meta(
        address_info.addr.clone(),
        meta_store.clone(),
        lease_interval_secs,
    )
    .await?;

    loop {
        // wait for leader update
        if !leader_rx.changed().await.is_ok() {
            tracing::error!("issue receiving leader value from channel");
        }

        let is_leader = *leader_rx.borrow();
        let node_state = if is_leader { "leader" } else { "follower" };
        tracing::info!("This node currently is a {}", node_state);

        // only start services if node is leader
        if !is_leader {
            continue;
        }

        tracing::info!("Node is leader. Preparing services...");

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
        // If node is not leader it should not start other services
        // probably this panic is caused because some other meta node does something

        let new_hm = hummock::HummockManager::new(
            env.clone(),
            cluster_manager.clone(),
            meta_metrics.clone(),
            compactor_manager.clone(),
        )
        .await;

        if new_hm.is_err() {
            tracing::error!("found error {}", new_hm.err().unwrap());
        }

        let hummock_manager = Arc::new(
            hummock::HummockManager::new(
                env.clone(),
                cluster_manager.clone(),
                meta_metrics.clone(),
                compactor_manager.clone(),
            )
            .await
            .unwrap(),
        );

        #[cfg(not(madsim))]
        if let Some(dashboard_addr) = address_info.dashboard_addr.take() {
            let dashboard_service = crate::dashboard::DashboardService {
                dashboard_addr,
                cluster_manager: cluster_manager.clone(),
                fragment_manager: fragment_manager.clone(),
                meta_store: env.meta_store_ref(),
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
        let vacuum_trigger = Arc::new(hummock::VacuumManager::new(
            env.clone(),
            hummock_manager.clone(),
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
            vacuum_trigger.clone(),
            fragment_manager.clone(),
        );
        let notification_manager = env.notification_manager_ref();
        let notification_srv = NotificationServiceImpl::new(
            env.clone(),
            catalog_manager,
            cluster_manager.clone(),
            hummock_manager.clone(),
            fragment_manager.clone(),
        );
        let health_srv = HealthServiceImpl::new();

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
        let mut sub_tasks = hummock::start_hummock_workers(
            hummock_manager.clone(),
            compactor_manager,
            vacuum_trigger,
            notification_manager,
            compaction_scheduler,
            &env.opts,
        )
        .await;
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

        // Start services.
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(MetricsMiddlewareLayer::new(meta_metrics.clone()))
                .add_service(HeartbeatServiceServer::new(heartbeat_srv))
                .add_service(ClusterServiceServer::new(cluster_srv))
                .add_service(StreamManagerServiceServer::new(stream_srv))
                .add_service(HummockManagerServiceServer::new(hummock_srv))
                .add_service(NotificationServiceServer::new(notification_srv))
                .add_service(DdlServiceServer::new(ddl_srv))
                .add_service(UserServiceServer::new(user_srv))
                .add_service(ScaleServiceServer::new(scale_srv))
                .add_service(HealthServer::new(health_srv))
                .serve(address_info.listen_addr)
                .await
                .unwrap();
        });

        // TODO: Use tonic's serve_with_shutdown for a graceful shutdown. Now it does not work,
        // as the graceful shutdown waits all connections to disconnect in order to finish the stop.
        let (shutdown_send, mut shutdown_recv) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = &mut shutdown_recv => {
                    shutdown_all.await;
                },
                _ = &mut idle_recv => {
                    shutdown_all.await;
                },
            }
        });

        return Ok((join_handle, shutdown_send));
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_leader_lease() {
        let info = AddressInfo {
            addr: "node1".to_string(),
            ..Default::default()
        };
        let meta_store = Arc::new(MemStore::default());
        let (handle, closer) = rpc_serve_with_store(
            meta_store.clone(),
            info,
            Duration::from_secs(10),
            2,
            MetaOpts::default(),
        )
        .await
        .unwrap();
        sleep(Duration::from_secs(4)).await;
        let info2 = AddressInfo {
            addr: "node2".to_string(),
            ..Default::default()
        };
        let ret = rpc_serve_with_store(
            meta_store.clone(),
            info2.clone(),
            Duration::from_secs(10),
            2,
            MetaOpts::default(),
        )
        .await;
        assert!(ret.is_err());
        closer.send(()).unwrap();
        handle.await.unwrap();
        sleep(Duration::from_secs(3)).await;
        let x = rpc_serve_with_store(
            meta_store.clone(),
            info2,
            Duration::from_secs(10),
            2,
            MetaOpts::default(),
        )
        .await
        .unwrap();
    }
}
