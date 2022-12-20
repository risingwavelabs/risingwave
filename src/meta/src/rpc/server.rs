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
use tokio::sync::oneshot::channel as OneChannel;
use tokio::sync::watch::{channel as WatchChannel, Sender as WatchSender};
use tokio::task::JoinHandle;

use super::elections::run_elections;
use super::intercept::MetricsMiddlewareLayer;
use super::leader_svs::start_leader_srv;
use super::service::health_service::HealthServiceImpl;
use crate::manager::MetaOpts;
use crate::rpc::metrics::MetaMetrics;
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
        let was_follower = !is_leader;

        // run follower services until node becomes leader
        // FIXME: Add service discovery for follower
        // https://github.com/risingwavelabs/risingwave/issues/6755
        let mut svc_shutdown_rx_clone = svc_shutdown_rx.clone();
        let (follower_shutdown_tx, follower_shutdown_rx) = OneChannel::<()>();
        let (follower_finished_tx, follower_finished_rx) = OneChannel::<()>();
        if !is_leader {
            tokio::spawn(async move {
                let _ = tracing::span!(tracing::Level::INFO, "follower services").enter();
                tracing::info!("Starting follower services");
                let health_srv = HealthServiceImpl::new();
                tonic::transport::Server::builder()
                    .layer(MetricsMiddlewareLayer::new(Arc::new(MetaMetrics::new())))
                    .add_service(HealthServer::new(health_srv))
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
                    .unwrap();
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
