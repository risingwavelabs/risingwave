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
            note_status_leader_rx
                .changed()
                .await
                .expect("Leader sender dropped");

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

    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::WorkerType;
    use risingwave_rpc_client::MetaClient;
    use tokio::time::sleep;

    use super::*;

    async fn setup_n_nodes(n: u16, meta_port: u16) -> Vec<(JoinHandle<()>, WatchSender<()>)> {
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
                    Duration::from_secs(4), // What values do we want to use here
                    2,                      // What values do we want to use here
                    MetaOpts::test(true),   // True or false?
                )
                .await
                .unwrap_or_else(|e| panic!("Meta node{} failed in setup. Err: {}", i, e)),
            );
        } // sleep duration of election cycle, not fixed duration
        sleep(Duration::from_secs(6)).await;
        node_controllers
    }

    #[tokio::test]
    async fn test_single_leader_setup() {
        // make n also pseudo random?
        let n = 5;
        let meta_port = 1234;
        let _node_controllers = setup_n_nodes(n, meta_port).await;

        let mut leader_count = 0;
        for i in 0..n {
            // create client connecting against meta_i
            let port = meta_port + i;
            // https or http?
            let meta_addr = format!("http://127.0.0.1:{}", port);
            let host_addr = "127.0.0.1:5688".parse::<HostAddr>().unwrap();
            let host_addr = HostAddr {
                host: host_addr.host,
                port: host_addr.port + i, // Do I need to change the port?
            };

            // register_new fails
            // Do I need to start some service first?
            // are the other services still running here?
            //      yes! Check via lsof -i @localhost
            // unspecified: Got error gRPC error (Internal error): worker_type
            // Compactor: Got error gRPC error (Operation is not implemented or not supported)
            // Frontend: (Operation is not implemented or not supported)
            // ComputeNode: Got error gRPC error (Operation is not implemented or not supported)
            // RiseCtl: Got error gRPC error (Operation is not implemented or not supported)

            // I get this error because I am talking to a non-leader?
            let is_leader = tokio::time::timeout(std::time::Duration::from_secs(1), async move {
                let client_i = MetaClient::register_new(
                    meta_addr.as_str(),
                    WorkerType::ComputeNode,
                    &host_addr,
                    1,
                )
                .await;
                match client_i {
                    Ok(client_i) => {
                        match client_i.send_heartbeat(client_i.worker_id(), vec![]).await {
                            Ok(_) => true,
                            Err(_) => false,
                        }
                    }
                    Err(_) => false,
                }
            })
            .await
            .unwrap_or(false);
            if is_leader {
                leader_count += 1;
            }
        }

        assert_eq!(
            leader_count, 1,
            "Expected to have 1 leader, instead got {} leaders",
            leader_count
        );

        // Also use pseudo random delays here?
        // https://github.com/risingwavelabs/risingwave/issues/6884

        // Use heartbeat to determine if node is leader
        // Use health service to determine if node is up

        // Tests:
        // Check if nodes conflict when they report leader
        // Check if the reported leader node is the actual leader node
        // Check if there are != 1 leader nodes
        // No failover should happen here, since system is idle
    }

    // Kill nodes via sender
    // Where do I write the logs to?
    // TODO: how do I know if a node is a leader or a follower?

    // TODO: implement failover test
}

// Old test below
// TODO: remove
// #[cfg(test)]
// mod testsdeprecated {
// use tokio::time::sleep;
//
// use super::*;
//
// #[tokio::test]
// async fn test_leader_lease() {
// let info = AddressInfo {
// addr: "node1".to_string(),
// ..Default::default()
// };
// let meta_store = Arc::new(MemStore::default());
// let (handle, closer) = rpc_serve_with_store(
// meta_store.clone(),
// info,
// Duration::from_secs(10),
// 2,
// MetaOpts::test(false),
// )
// .await
// .unwrap();
// sleep(Duration::from_secs(4)).await;
// let info2 = AddressInfo {
// addr: "node2".to_string(),
// ..Default::default()
// };
// let ret = rpc_serve_with_store(
// meta_store.clone(),
// info2.clone(),
// Duration::from_secs(10),
// 2,
// MetaOpts::test(false),
// )
// .await;
// assert!(ret.is_err());
// closer.send(()).unwrap();
// handle.await.unwrap();
// sleep(Duration::from_secs(3)).await;
// rpc_serve_with_store(
// meta_store.clone(),
// info2,
// Duration::from_secs(10),
// 2,
// MetaOpts::test(false),
// )
// .await
// .unwrap();
// }
// }
//
// Old test above
