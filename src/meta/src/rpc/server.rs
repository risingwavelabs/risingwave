// Copyright 2023 Singularity Data
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

use etcd_client::ConnectOptions;
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::oneshot::channel as OneChannel;
use tokio::sync::watch;
use tokio::sync::watch::Sender as WatchSender;
use tokio::task::JoinHandle;
use tokio::time;

use super::follower_svc::start_follower_srv;
use crate::manager::MetaOpts;
use crate::rpc::election_client::{ElectionClient, EtcdElectionClient};
use crate::rpc::leader_svc::start_leader_srv;
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

pub type ElectionClientRef = Arc<dyn ElectionClient>;

pub async fn rpc_serve(
    address_info: AddressInfo,
    meta_store_backend: MetaStoreBackend,
    max_heartbeat_interval: Duration,
    lease_interval_secs: u64,
    opts: MetaOpts,
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
            let client = EtcdClient::connect(
                endpoints.clone(),
                Some(options.clone()),
                credentials.is_some(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect etcd {}", e))?;
            let meta_store = Arc::new(EtcdMetaStore::new(client));

            let election_client = Arc::new(
                EtcdElectionClient::new(
                    endpoints,
                    Some(options),
                    address_info.listen_addr.clone().to_string(),
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
            let svc_shutdown_rx_clone = svc_shutdown_rx.clone();
            let (follower_shutdown_tx, follower_shutdown_rx) = OneChannel::<()>();
            let follower_handle: Option<JoinHandle<()>> = if !election_client.is_leader().await {
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

            let mut ticker = time::interval(Duration::from_secs(1));

            while !election_client.is_leader().await {
                ticker.tick().await;
            }

            if let Some(handle) = follower_handle {
                let _res = follower_shutdown_tx.send(());
                let _ = handle.await;
            }
        };

        let current_leader = if let Some(election_client) = election_client.as_ref() {
            election_client.leader().await.unwrap().unwrap()
        } else {
            MetaLeaderInfo {
                node_address: address_info.listen_addr.clone().to_string(),
                lease_id: 0,
            }

        };

        start_leader_srv(
            meta_store,
            address_info,
            max_heartbeat_interval,
            opts,
            current_leader,
            svc_shutdown_rx,
        )
        .await
        .expect("Unable to start leader services");
    });

    Ok((join_handle, leader_lost_handle, svc_shutdown_tx))
}
