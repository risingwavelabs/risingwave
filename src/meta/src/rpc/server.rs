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
    // Initialize managers.
    let (_, election_handle, election_shutdown, mut leader_rx) = run_elections(
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
        let _ = tracing::span!(tracing::Level::INFO, "node_status").enter();
        loop {
            if note_status_leader_rx.changed().await.is_err() {
                tracing::error!("Leader sender dropped");
                return;
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

    // FIXME: maybe do not use a channel here
    // What I need is basically a oneshot channel with one producer and multiple consumers
    let (svc_shutdown_tx, svc_shutdown_rx) = WatchChannel(());

    let join_handle = tokio::spawn(async move {
        let span = tracing::span!(tracing::Level::INFO, "services");
        let _enter = span.enter();

        // failover logic
        services_leader_rx
            .changed()
            .await
            .expect("Leader sender dropped");

        let is_leader = services_leader_rx.borrow().clone().1;

        // run follower services until node becomes leader
        // FIXME: Add service discovery for follower
        // https://github.com/risingwavelabs/risingwave/issues/6755
        let svc_shutdown_rx_clone = svc_shutdown_rx.clone();
        let (follower_shutdown_tx, follower_shutdown_rx) = OneChannel::<()>();
        let follower_handle: Option<JoinHandle<()>> = if !is_leader {
            let address_info_clone = address_info.clone();
            Some(tokio::spawn(async move {
                let _ = tracing::span!(tracing::Level::INFO, "follower services").enter();
                start_follower_srv(
                    svc_shutdown_rx_clone,
                    follower_shutdown_rx,
                    address_info_clone,
                )
                .await;
            }))
        } else {
            None
        };

        // wait until this node becomes a leader
        while !node_is_leader(&leader_rx) {
            leader_rx.changed().await.expect("Leader sender dropped");
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
        };

        start_leader_srv(
            meta_store,
            address_info,
            max_heartbeat_interval,
            opts,
            leader_rx,
            elect_coord,
            svc_shutdown_rx,
        )
        .await
        .expect("Unable to start leader services");
    });

    Ok((join_handle, svc_shutdown_tx))
}

// TODO: repeat test 100 times. Start test with different sleep intervals and so on
// Print the sleep intervals
// use pseudo rand gen with seed
mod tests {
    use core::panic;
    use std::net::{IpAddr, Ipv4Addr};

    use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
    use risingwave_pb::meta::AddWorkerNodeRequest;
    use tokio::time::sleep;
    use tonic::transport::Endpoint;

    use super::*;

    static _SLEEP_SEC: u64 = 6;

    /// Start `n` meta nodes on localhost. First node will be started at `meta_port`, 2nd node on
    /// `meta_port + 1`, ...
    async fn _setup_n_nodes(
        n: u16,
        meta_port: u16,
        enable_recovery: bool, // TODO: remove. Is always false
    ) -> Vec<(JoinHandle<()>, WatchSender<()>)> {
        let meta_store = Arc::new(MemStore::default());

        let mut node_controllers: Vec<(JoinHandle<()>, WatchSender<()>)> = vec![];
        for i in 0..n {
            // TODO: use http or https here?
            let addr = format!("http://127.0.0.1:{}", meta_port + i);

            let info = AddressInfo {
                addr,
                listen_addr: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                    meta_port + i,
                ),
                prometheus_addr: None, // TODO: use default here
                dashboard_addr: None,
                ui_path: None,
            };
            node_controllers.push(
                rpc_serve_with_store(
                    meta_store.clone(),
                    info,
                    Duration::from_secs(4),
                    2,
                    MetaOpts::test(enable_recovery),
                )
                .await
                .unwrap_or_else(|e| panic!("Meta node{} failed in setup. Err: {}", i, e)),
            );
        }
        sleep(Duration::from_secs(_SLEEP_SEC)).await;
        node_controllers
    }

    /// Check for `number_of_nodes` meta leader nodes, starting at `meta_port`, `meta_port + 1`, ...
    /// Simulates `number_of_nodes` compute nodes, starting at `meta_port`, `meta_port + 1`, ...
    ///
    /// ## Returns
    /// Number of nodes which currently are leaders. Number is not snapshoted. If there is a
    /// leader failover in process, you may get an incorrect result
    async fn _number_of_leaders(number_of_nodes: u16, meta_port: u16, host_port: u16) -> u16 {
        let mut leader_count = 0;
        for i in 0..number_of_nodes {
            let local = "127.0.0.1".to_owned();
            let port = meta_port + i;
            let meta_addr = format!("http://{}:{}", local, port); // http, https, no protocol
            let host_addr = HostAddr {
                host: local,
                port: host_port + i,
            };
            // single leader test 1
            // should be http://127.0.0.1:1234
            // is        http://127.0.0.1:1234

            let endpoint = Endpoint::from_shared(meta_addr.to_string())
                .unwrap()
                .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE);
            let channel = endpoint
                .http2_keep_alive_interval(Duration::from_secs(60))
                .keep_alive_timeout(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .inspect_err(|e| {
                    tracing::warn!(
                        "Failed to connect to meta server {}, wait for online: {}",
                        meta_addr,
                        e
                    );
                })
                .unwrap();

            let cluster_client = ClusterServiceClient::new(channel);
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
            let is_leader = resp.is_ok();

            if is_leader {
                leader_count += 1;
            }
        }
        leader_count
    }

    // Writing these tests as separate functions instead of one loop, because functions get executed
    // in parallel
    #[tokio::test]
    async fn test_single_leader_setup_1() {
        let v = _setup_n_nodes(1, 1234, false).await; // TODO: parameter false not needed. Always false
        let leader_count = _number_of_leaders(1, 1234, 5678).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for element in v {
            element.0.abort();
        }
    }

    #[tokio::test]
    async fn test_single_leader_setup_3() {
        let v = _setup_n_nodes(3, 2345, false).await;
        let leader_count = _number_of_leaders(3, 2345, 6789).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for element in v {
            element.0.abort();
        }
    }

    #[tokio::test]
    async fn test_single_leader_setup_10() {
        let v = _setup_n_nodes(10, 3456, false).await;
        let leader_count = _number_of_leaders(10, 3456, 7890).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for element in v {
            element.0.abort();
        }
    }

    #[tokio::test]
    async fn test_single_leader_setup_100() {
        let v = _setup_n_nodes(100, 4567, false).await;
        let leader_count = _number_of_leaders(100, 4567, 8901).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
        for element in v {
            element.0.abort();
        }
    }

    /// returns number of leaders after failover
    async fn _test_failover(number_of_nodes: u16, meta_port: u16, compute_port: u16) -> u16 {
        // TODO: change to false?
        let vec_meta_handlers = _setup_n_nodes(number_of_nodes, meta_port, false).await;

        // we should have 1 leader on startup
        let leader_count = _number_of_leaders(number_of_nodes, meta_port, compute_port).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );

        // kill leader to trigger failover
        let leader_shutdown_sender = &vec_meta_handlers[0].1;
        leader_shutdown_sender
            .send(())
            .expect("Sending shutdown to leader should not fail");
        sleep(Duration::from_secs(_SLEEP_SEC)).await;

        // expect that we still have 1 leader
        _number_of_leaders(number_of_nodes, meta_port, compute_port).await
    }

    #[tokio::test]
    async fn test_failover_1() {
        let leader_count = _test_failover(1, 9012, 1012).await;
        assert_eq!(
            leader_count, 0,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
    }

    #[tokio::test]
    async fn test_failover_3() {
        let leader_count = _test_failover(3, 1100, 1200).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
    }

    #[tokio::test]
    async fn test_failover_10() {
        let leader_count = _test_failover(10, 1300, 1400).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
    }

    #[tokio::test]
    async fn test_failover_100() {
        let leader_count = _test_failover(100, 1500, 1600).await;
        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );
    }
}

// Tests fail, because address already in use. Test addresses overlap?
