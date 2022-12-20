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
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::meta::leader_service_server::LeaderServiceServer;
use tokio::sync::oneshot::channel as OneChannel;
use tokio::sync::watch::Sender as WatchSender;
use tokio::task::JoinHandle;

use super::elections::run_elections;
use super::intercept::MetricsMiddlewareLayer;
use super::leader_svs::start_leader_srv;
use super::service::health_service::HealthServiceImpl;
use crate::manager::MetaOpts;
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::service::leader_service::LeaderServiceImpl;
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

/// Starts a meta server with meta store
/// The server will start either as a leader or follower node, depending on the election outcome
/// Servers may failover from follower to leader, but not from leader to follower
/// Follower meta nodes start a limited range of services and should only be used to retrieve the
/// current leaders address
///
/// ## Arguments:
/// `meta_store`: Store in which to hold meta information
/// `address_info`: Address of this node
/// `max_heartbeat_interval`: TODO
/// `lease_interval_secs`: Timeout in seconds that a leader lease is valid. See election mod for
/// details
/// `opts`: MetaOpts TODO
///
/// ## Returns:
/// TODO
pub async fn rpc_serve_with_store<S: MetaStore>(
    meta_store: Arc<S>,
    address_info: AddressInfo,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
) -> MetaResult<(JoinHandle<()>, WatchSender<()>)> {
    // Initialize managers.
    let (_, election_handle, election_shutdown, leader_rx) = run_elections(
        address_info.listen_addr.clone().to_string(),
        meta_store.clone(),
        lease_interval_secs,
    )
    .await?;

    let mut services_leader_rx = leader_rx.clone();
    let mut note_status_leader_rx = leader_rx.clone();

    // print current leader/follower status of this node + fencing mechanism
    tokio::spawn(async move {
        let _ = tracing::span!(tracing::Level::INFO, "node_status").enter();
        let mut was_leader = false;
        loop {
            note_status_leader_rx
                .changed()
                .await
                .expect("Leader sender dropped");
            let (leader_info, is_leader) = note_status_leader_rx.borrow().clone();
            let leader_addr = leader_info_to_host_addr(leader_info);

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
    // we only can define leader services when they are needed. Otherwise they already do things
    // FIXME: Start leader services if follower becomes leader

    // TODO: maybe do not use a channel here
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
        let was_follower = !is_leader;
        let f_leader_svc_leader_rx = services_leader_rx.clone();
        let l_leader_svc_leader_rx = services_leader_rx.clone();

        let leader_srv = LeaderServiceImpl::new(f_leader_svc_leader_rx);

        // run follower services until node becomes leader
        let mut svc_shutdown_rx_clone = svc_shutdown_rx.clone();
        let (follower_shutdown_tx, follower_shutdown_rx) = OneChannel::<()>();
        let (follower_finished_tx, follower_finished_rx) = OneChannel::<()>();
        if !is_leader {
            tokio::spawn(async move {
                let _ = tracing::span!(tracing::Level::INFO, "follower services").enter();
                tracing::info!("Starting follower services");
                let health_srv = HealthServiceImpl::new();
                let err_msg = format!(
                    "Starting node at listen_addr {} failed",
                    address_info.listen_addr
                );
                tonic::transport::Server::builder()
                    .layer(MetricsMiddlewareLayer::new(Arc::new(MetaMetrics::new())))
                    .add_service(HealthServer::new(health_srv))
                    .add_service(LeaderServiceServer::new(leader_srv))
                    .serve_with_shutdown(address_info.listen_addr, async move {
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {},
                            // shutdown service if all services should be shut down
                            res = svc_shutdown_rx_clone.changed() =>  {
                                match res {
                                    Ok(_) => tracing::info!("Shutting down services"),
                                    Err(_) => tracing::error!("Service shutdown sender dropped")
                                }
                            },
                            // shutdown service if follower becomes leader
                            res = follower_shutdown_rx =>  {
                                match res {
                                    Ok(_) => tracing::info!("Shutting down follower services"),
                                    Err(_) => tracing::error!("Follower service shutdown sender dropped")
                                }
                            },
                        }
                    })
                    .await
                    .expect(&err_msg); // TODO: just use unwrap
                match follower_finished_tx.send(()) {
                    Ok(_) => tracing::info!("Shutting down follower services done"),
                    Err(_) => {
                        tracing::error!("Follower service shutdown done sender receiver dropped")
                    }
                }
            });
        }

        // wait until this node becomes a leader
        while !services_leader_rx.borrow().clone().1 {
            services_leader_rx
                .changed()
                .await
                .expect("Leader sender dropped");
        }

        // shut down follower svc if node used to be follower
        if was_follower {
            match follower_shutdown_tx.send(()) {
                Ok(_) => tracing::info!("Shutting down follower services"),
                Err(_) => tracing::error!("Follower service receiver dropped"),
            }
            // Wait until follower service is down
            match follower_finished_rx.await {
                Ok(_) => tracing::info!("Follower services shut down"),
                Err(_) => tracing::error!("Follower service shutdown finished sender dropped"),
            };
        }

        start_leader_srv(
            meta_store,
            address_info.clone(),
            max_heartbeat_interval,
            opts,
            leader_rx,
            election_handle,
            election_shutdown,
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
    use std::net::{IpAddr, Ipv4Addr};

    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::WorkerType;
    use risingwave_rpc_client::MetaClient;
    use tokio::time::sleep;

    use super::*;

    async fn setup_n_nodes(n: u16) -> Vec<(JoinHandle<()>, WatchSender<()>)> {
        let meta_store = Arc::new(MemStore::default());

        let mut node_controllers: Vec<(JoinHandle<()>, WatchSender<()>)> = vec![];
        for i in 0..n {
            // TODO: add pseudo random sleep here
            let node = format!("node{}", i);
            let err_msg = format!("Meta {} failed in setup", node);
            let err_msg = err_msg.as_str();

            let info = AddressInfo {
                addr: node,
                listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234 + i),
                prometheus_addr: None,
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
                .expect(err_msg),
            );
        } // sleep duration of election cycle, not fixed duration
        sleep(Duration::from_secs(10)).await;
        node_controllers
    }

    #[tokio::test]
    async fn test_single_leader_setup() {
        // make n also pseudo random?
        let n = 5;
        let _node_controllers = setup_n_nodes(n).await;

        let mut leader_count = 0;
        for i in 0..n {
            // create client connecting against meta_i
            let port = 5678 + i;
            let meta_addr = format!("127.0.0.1:{}", port);
            let host_addr = "127.0.0.1:5688".parse::<HostAddr>().unwrap();
            let host_addr = HostAddr {
                host: host_addr.host,
                port: host_addr.port + i, // Do I need to change the port?
            };

            // register_new fails
            // Do I need to start some service first?
            // are the other services still running here?
            //      yes! Check via lsof -i @localhost
            let res = MetaClient::register_new(
                meta_addr.as_str(),
                WorkerType::ComputeNode,
                &host_addr,
                0,
            )
            .await;
            if res.is_err() {
                panic!("Unable to connect against client {}", i);
            }
            let client_i = res.unwrap();

            match client_i.send_heartbeat(i as u32, vec![]).await {
                Ok(_) => {
                    leader_count += 1;
                    tracing::info!("Node {} is leader", i);
                }
                Err(_) => tracing::info!("Node {} is follower", i),
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
