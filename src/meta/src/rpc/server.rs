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
use risingwave_common::util::addr::leader_info_to_host_addr;
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::oneshot::channel as OneChannel;
use tokio::sync::watch::{
    channel as WatchChannel, Receiver as WatchReceiver, Sender as WatchSender,
};
use tokio::task::JoinHandle;

use super::elections::run_elections;
use super::follower_svc::start_follower_srv;
use super::leader_svc::{start_leader_srv, ElectionCoordination};
use crate::manager::MetaOpts;
use crate::storage::{EtcdMetaStore, MemStore, MetaStore, WrappedEtcdClient as EtcdClient};
use crate::MetaResult;

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
) -> MetaResult<(JoinHandle<()>, WatchSender<()>)> {
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

fn node_is_leader(leader_rx: &WatchReceiver<(MetaLeaderInfo, bool)>) -> bool {
    leader_rx.borrow().clone().1
}

pub async fn rpc_serve_with_store<S: MetaStore>(
    meta_store: Arc<S>,
    address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
) -> MetaResult<(JoinHandle<()>, WatchSender<()>)> {
    // Used by election to signal that leader lost leadership and should exit immediately
    let (panic_tx, mut panic_rx) = WatchChannel(());
    let mut panic_rx_clone = panic_rx.clone();

    // Initialize managers
    let (_, election_handle, election_shutdown, mut leader_rx) = run_elections(
        address_info.listen_addr.clone().to_string(),
        meta_store.clone(),
        lease_interval_secs,
        panic_tx,
    )
    .await?;

    let mut services_leader_rx = leader_rx.clone();
    let mut note_status_leader_rx = leader_rx.clone();

    // print current leader/follower status of this node
    tokio::spawn(async move {
        let _ = tracing::span!(tracing::Level::INFO, "node_status").enter();
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    return;
                },
                _ = panic_rx_clone.changed() => {
                    return;
                },
                leader = note_status_leader_rx.changed() => {
                    if leader.is_err() {
                        tracing::error!("Leader sender dropped");
                        return;
                    }
                },
            }

            let (leader_info, is_leader) = note_status_leader_rx.borrow().clone();
            let leader_addr = leader_info_to_host_addr(leader_info);

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

    let (svc_shutdown_tx, mut svc_shutdown_rx) = WatchChannel(());
    let f_leader_rx = leader_rx.clone();

    let join_handle = tokio::spawn(async move {
        let span = tracing::span!(tracing::Level::INFO, "services");
        let _enter = span.enter();

        // failover logic
        services_leader_rx
            .changed()
            .await
            .expect("Leader sender dropped");

        // run follower services until node becomes leader
        let svc_shutdown_rx_clone = svc_shutdown_rx.clone();
        let (follower_shutdown_tx, follower_shutdown_rx) = OneChannel::<()>();
        let follower_handle: Option<JoinHandle<()>> = if !node_is_leader(&leader_rx) {
            let address_info_clone = address_info.clone();
            Some(tokio::spawn(async move {
                let _ = tracing::span!(tracing::Level::INFO, "follower services").enter();
                start_follower_srv(
                    svc_shutdown_rx_clone,
                    follower_shutdown_rx,
                    address_info_clone,
                    f_leader_rx,
                )
                .await;
            }))
        } else {
            None
        };

        // wait until this node becomes a leader
        while !node_is_leader(&leader_rx) {
            tokio::select! {
                _ = leader_rx.changed() => {}
                res = svc_shutdown_rx.changed() => {
                    match res {
                        Ok(_) => tracing::info!("Shutting down meta node"),
                        Err(_) => tracing::error!("Shutdown sender dropped"),
                    }
                    return;
                }
            }
        }

        // shut down follower svc if node used to be follower
        if let Some(handle) = follower_handle {
            match follower_shutdown_tx.send(()) {
                Ok(_) => tracing::info!("Shutting down follower services"),
                Err(_) => tracing::error!("Follower service receiver dropped"),
            }
            // Wait until follower service is down
            handle.await.unwrap();
        }

        let elect_coord = ElectionCoordination {
            election_handle,
            election_shutdown,
            leader_rx,
        };

        let current_leader = services_leader_rx.borrow().0.clone();
        start_leader_srv(
            meta_store,
            address_info,
            max_heartbeat_interval,
            opts,
            current_leader,
            elect_coord,
            svc_shutdown_rx,
        )
        .await
        .expect("Unable to start leader services");
    });

    // kill everything on panic signal
    let join_handle_with_trigger = tokio::spawn(async move {
        let _ = panic_rx.changed().await;
        join_handle.abort();
    });

    Ok((join_handle_with_trigger, svc_shutdown_tx))
}

#[cfg(test)]
mod tests {
    use core::panic;

    use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
    use risingwave_pb::common::HostAddress;
    use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
    use risingwave_pb::meta::LeaderRequest;
    use tokio::time::sleep;
    use tonic::transport::{Channel, Endpoint};

    use super::*;
    use crate::rpc::{META_CF_NAME, META_LEADER_KEY, META_LEASE_KEY};
    use crate::storage::Transaction;

    const WAIT_INTERVAL: Duration = Duration::from_secs(5);

    /// Start `n` meta nodes on localhost. First node will be started at `meta_port`, 2nd node on
    /// `meta_port + 1`, ...
    /// Call this, if you need more control over your `meta_store` in your test
    async fn setup_n_nodes_inner(
        n: u16,
        meta_port: u16,
        meta_store: &Arc<MemStore>,
    ) -> Vec<(JoinHandle<()>, WatchSender<()>)> {
        use std::net::{IpAddr, Ipv4Addr};

        let mut node_controllers: Vec<(JoinHandle<()>, WatchSender<()>)> = vec![];
        for i in 0..n {
            let addr = format!("http://127.0.0.1:{}", meta_port + i);

            let info = AddressInfo {
                addr,
                listen_addr: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    meta_port + i,
                ),
                ..AddressInfo::default()
            };
            node_controllers.push(
                rpc_serve_with_store(
                    meta_store.clone(),
                    info,
                    Duration::from_secs(4),
                    1,
                    MetaOpts::test(false),
                )
                .await
                .unwrap_or_else(|e| panic!("Meta node{} failed in setup. Err: {}", i, e)),
            );
        }
        sleep(WAIT_INTERVAL).await;
        node_controllers
    }

    /// wrapper for `setup_n_nodes_inner`
    async fn setup_n_nodes(n: u16, meta_port: u16) -> Vec<(JoinHandle<()>, WatchSender<()>)> {
        let meta_store = Arc::new(MemStore::default());
        setup_n_nodes_inner(n, meta_port, &meta_store).await
    }

    /// Get a Channel to a meat node without re-trying the connection.
    ///
    /// ### Returns
    /// Null on error, else the channel
    async fn get_meta_channel(meta_addr: String) -> Result<Channel, tonic::transport::Error> {
        let meta_addr_clone = meta_addr.clone();
        let endpoint = Endpoint::from_shared(meta_addr)
            .unwrap()
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE);
        endpoint
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .inspect_err(|e| {
                tracing::warn!(
                    "Failed to connect to meta server {}, wait for online: {}",
                    meta_addr_clone,
                    e
                );
            })
    }

    /// Check for `number_of_nodes` meta leader nodes, starting at `meta_port`, `meta_port + 1`, ...
    /// Simulates `number_of_nodes` compute nodes, starting at `meta_port`, `meta_port + 1`, ...
    ///
    /// ## Returns
    /// Number of nodes which currently are leaders. Number is not snapshoted. If there is a
    /// leader failover in process, you may get an incorrect result
    async fn number_of_leaders(number_of_nodes: u16, meta_port: u16, host_port: u16) -> u16 {
        use risingwave_common::util::addr::HostAddr;
        use risingwave_pb::common::WorkerType;
        use risingwave_pb::meta::AddWorkerNodeRequest;

        let mut leader_count = 0;
        for i in 0..number_of_nodes {
            let local = "127.0.0.1".to_owned();
            let port = meta_port + i;
            let meta_addr = format!("http://{}:{}", local, port);
            let host_addr = HostAddr {
                host: local,
                port: host_port + i,
            };

            // check if node is leader
            // Only leader nodes support adding worker nodes
            let channel = match get_meta_channel(meta_addr).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let cluster_client = ClusterServiceClient::new(channel);

            // check if node is leader
            // Only leader nodes support adding worker nodes
            let resp = cluster_client
                .to_owned()
                .add_worker_node(AddWorkerNodeRequest {
                    worker_type: WorkerType::ComputeNode as i32,
                    host: Some(HostAddress {
                        host: host_addr.host,
                        port: host_addr.port as i32,
                    }),
                    worker_node_parallelism: 5,
                })
                .await;

            if resp.is_ok() {
                leader_count += 1;
            }
        }
        leader_count
    }

    // Writing these tests as separate functions instead of one loop, because functions get executed
    // in parallel
    #[tokio::test]
    async fn test_single_leader_setup_1() {
        let node_controllers = setup_n_nodes(1, 1234).await;
        let leader_count = number_of_leaders(1, 1234, 5678).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for (join_handle, shutdown_tx) in node_controllers {
            shutdown_tx.send(()).unwrap();
            join_handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_single_leader_setup_3() {
        let node_controllers = setup_n_nodes(3, 2345).await;
        let leader_count = number_of_leaders(3, 2345, 6789).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for (join_handle, shutdown_tx) in node_controllers {
            shutdown_tx.send(()).unwrap();
            join_handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_single_leader_setup_5() {
        let node_controllers = setup_n_nodes(5, 3456).await;
        let leader_count = number_of_leaders(5, 3456, 7890).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for (join_handle, shutdown_tx) in node_controllers {
            shutdown_tx.send(()).unwrap();
            join_handle.await.unwrap();
        }
    }

    /// Get the current leader as reported by this node
    ///
    /// ## Return
    /// None if it can not reach the node at localhost: `meta_port`, else the reported leader
    /// address
    async fn get_leader_addr(meta_port: u16) -> Option<HostAddress> {
        use risingwave_pb::meta::leader_service_client::LeaderServiceClient;

        let port = meta_port;
        let meta_addr = format!("http://127.0.0.1:{}", port);

        let channel = match get_meta_channel(meta_addr).await {
            Ok(c) => c,
            Err(_) => return None,
        };
        let leader_client = LeaderServiceClient::new(channel);
        let reported_leader_addr: HostAddress = leader_client
            .to_owned()
            .leader(LeaderRequest {})
            .await
            .unwrap()
            .into_inner()
            .leader_addr
            .expect("Node should always know who leader is");
        Some(reported_leader_addr)
    }

    /// ## Returns
    /// The leader node on which all meta nodes agree
    /// Panics if not all nodes agree
    /// Not snapshotted. If there is a leader failover in process, you may get an incorrect result
    ///
    /// ## Arguments
    /// Will query `number_of_nodes` meta nodes, starting at localhost `meta_port`.
    /// Skips nodes that cannot be reached
    async fn get_agreed_leader(number_of_nodes: u16, meta_port: u16) -> HostAddress {
        let mut reported_leader_addr: Vec<HostAddress> = vec![];
        for i in 0..number_of_nodes {
            if let Some(leader_addr) = get_leader_addr(meta_port + i).await {
                reported_leader_addr.push(leader_addr);
            }
        }
        reported_leader_addr.dedup();
        assert_eq!(
            1,
            reported_leader_addr.len(),
            "Iteration 0: All nodes should agree on who leader is. Instead we got the following leaders {:?}",
            reported_leader_addr
        );
        reported_leader_addr.first().unwrap().clone()
    }

    // TODO
    // FIXME: Leader svc test: Delete lease and/or leader info after PR is merged
    // https://github.com/risingwavelabs/risingwave/pull/7022

    /// Adding nodes should not cause leader failover
    #[tokio::test]
    async fn test_leader_svc_add_nodes() {
        let number_of_nodes = 2;
        let meta_port = 1300;
        let node_controllers_1 = setup_n_nodes(number_of_nodes, meta_port).await;
        let original_leader = get_agreed_leader(number_of_nodes, meta_port).await;

        // add nodes
        let node_controllers_2 =
            setup_n_nodes(number_of_nodes, meta_port + number_of_nodes + 1).await;
        assert_eq!(
            original_leader,
            get_agreed_leader(number_of_nodes, meta_port).await,
            "1: Leader should stay the same if nodes are added"
        );

        // add nodes again
        let node_controllers_3 =
            setup_n_nodes(number_of_nodes, meta_port + number_of_nodes * 2 + 2).await;
        assert_eq!(
            original_leader,
            get_agreed_leader(number_of_nodes, meta_port).await,
            "2: Leader should stay the same if nodes are added"
        );

        // shut down all nodes
        for c in [node_controllers_1, node_controllers_2, node_controllers_3] {
            for (join_handle, shutdown_tx) in c {
                if shutdown_tx.send(()).is_ok() {
                    join_handle.await.unwrap();
                }
            }
        }
    }

    /// Deletes all leader nodes one after another
    /// Asserts that all nodes agree on who leader is
    /// Gets next leader to delete by using leader service from nodes
    #[tokio::test]
    async fn test_leader_svc_delete_everything() {
        let number_of_nodes = 5;
        let meta_port = 1250;
        let node_controllers = setup_n_nodes(number_of_nodes, meta_port).await;

        // All nodes should agree on who the leader is on beginning
        let mut current_leader = get_agreed_leader(number_of_nodes, meta_port).await;

        // delete all nodes on after another
        for _ in 1..number_of_nodes {
            // Shutdown current reported leader
            let leader_port = current_leader.port as u16;
            let offset = leader_port - meta_port;
            let _ = &node_controllers[offset as usize]
                .1
                .send(())
                .expect("Sending shutdown to leader should not fail");
            sleep(WAIT_INTERVAL).await;

            // Check if all nodes agree on who leader is
            current_leader = get_agreed_leader(number_of_nodes, meta_port).await;
        }

        // send shutdown to all nodes. There should only be one more node left
        let mut active_nodes = 0;
        for (join_handle, shutdown_tx) in node_controllers {
            active_nodes = match shutdown_tx.send(()) {
                Ok(_) => active_nodes + 1,
                Err(_) => active_nodes,
            };
            join_handle.await.unwrap();
        }
        assert_eq!(
            active_nodes, 1,
            "After test there should only be one meta node left, but there were {} nodes alive",
            active_nodes
        );
    }

    /// Deletes all leader nodes one after another by triggering fencing
    /// Asserts that all nodes agree on who leader is
    /// Gets next leader to delete by using leader service from nodes
    #[tokio::test]
    async fn test_leader_svc_fence_everything() {
        let number_of_nodes = 4;
        let meta_port = 2250;
        let meta_store = Arc::new(MemStore::default());
        let node_controllers = setup_n_nodes_inner(number_of_nodes, meta_port, &meta_store).await;

        // All nodes should agree on who the leader is on beginning
        let _ = get_agreed_leader(number_of_nodes, meta_port).await;

        // delete all nodes on after another
        let del = vec![(true, true), (true, false), (false, true)];

        for (delete_leader, delete_lease) in del {
            // trigger fencing on the current leader
            let mut txn = Transaction::default();
            if delete_leader {
                txn.delete(
                    META_CF_NAME.to_string(),
                    META_LEADER_KEY.as_bytes().to_vec(),
                );
            }
            if delete_lease {
                txn.delete(META_CF_NAME.to_string(), META_LEASE_KEY.as_bytes().to_vec());
            }
            meta_store.txn(txn).await.unwrap();
            sleep(WAIT_INTERVAL).await;

            // Check if all nodes agree on who leader is
            let _ = get_agreed_leader(number_of_nodes, meta_port).await;
        }

        // send shutdown to all nodes. There should only be one more node left
        let mut active_nodes = 0;
        for (join_handle, shutdown_tx) in node_controllers {
            active_nodes = match shutdown_tx.send(()) {
                Ok(_) => active_nodes + 1,
                Err(_) => active_nodes,
            };
            join_handle.await.unwrap();
        }
        assert_eq!(
            active_nodes, 1,
            "After test there should only be one meta node left, but there were {} nodes alive",
            active_nodes
        );
    }

    /// returns number of leaders after failover
    async fn test_failover(number_of_nodes: u16, meta_port: u16, compute_port: u16) -> u16 {
        let node_controllers = setup_n_nodes(number_of_nodes, meta_port).await;

        // we should have 1 leader on startup
        let leader_count = number_of_leaders(number_of_nodes, meta_port, compute_port).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );

        // kill leader to trigger failover
        let leader_shutdown_sender = &node_controllers[0].1;
        leader_shutdown_sender
            .send(())
            .expect("Sending shutdown to leader should not fail");
        sleep(WAIT_INTERVAL).await;

        // expect that we still have 1 leader
        // skipping first meta_port, since that node was former leader and got killed
        let leaders = number_of_leaders(number_of_nodes - 1, meta_port + 1, compute_port).await;
        for (join_handle, shutdown_tx) in node_controllers {
            if shutdown_tx.send(()).is_ok() {
                join_handle.await.unwrap();
            }
        }
        leaders
    }

    #[tokio::test]
    async fn test_failover_1() {
        let leader_count = test_failover(1, 9012, 1012).await;
        assert_eq!(
            leader_count, 0,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
    }

    #[tokio::test]
    async fn test_failover_3() {
        let leader_count = test_failover(3, 1100, 1200).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
    }

    /// Creates `number_of_nodes` meta nodes
    /// Deletes leader and or lease `number_of_nodes` times
    /// After each deletion asserts that we have the correct number of leader nodes
    #[tokio::test]
    async fn test_fencing() {
        let meta_port = 1600;
        let compute_port = 1700;
        let number_of_nodes = 4;

        let meta_store = Arc::new(MemStore::default());
        let vec_meta_handlers = setup_n_nodes_inner(number_of_nodes, meta_port, &meta_store).await;

        // we should have 1 leader on startup
        let leader_count = number_of_leaders(number_of_nodes, meta_port, compute_port).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader at beginning, instead got {} leaders",
            leader_count
        );

        let del = vec![(true, true), (true, false), (false, true)];

        for (delete_leader, delete_lease) in del {
            // delete leader/lease info in meta store
            let mut txn = Transaction::default();
            if delete_leader {
                txn.delete(
                    META_CF_NAME.to_string(),
                    META_LEADER_KEY.as_bytes().to_vec(),
                );
            }
            if delete_lease {
                txn.delete(META_CF_NAME.to_string(), META_LEASE_KEY.as_bytes().to_vec());
            }
            meta_store.txn(txn).await.unwrap();
            sleep(WAIT_INTERVAL).await;

            // assert that we still have 1 leader
            let leader_count = number_of_leaders(number_of_nodes, meta_port, compute_port).await;
            assert_eq!(
                leader_count, 1,
                "Expected to have 1 leader, instead got {} leaders",
                leader_count
            );
        }

        for ele in vec_meta_handlers {
            ele.0.abort();
        }
    }
}
