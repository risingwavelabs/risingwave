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

//! Wrapper gRPC clients, which help constructing the request and destructing the
//! response gRPC message structs.

#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(result_option_inspect)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(generators)]

use std::iter::repeat;
use std::sync::Arc;

#[cfg(not(madsim))]
use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::try_join_all;
#[cfg(not(madsim))]
use moka::future::Cache;
use rand::prelude::SliceRandom;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::heartbeat_request::extra_info;
pub mod error;
use error::{Result, RpcError};
mod compute_client;
mod connector_client;
mod hummock_meta_client;
mod meta_client;
// mod sink_client;
mod stream_client;

#[allow(unused_imports)]
use std::time::Duration;

pub use compute_client::{ComputeClient, ComputeClientPool, ComputeClientPoolRef};
pub use connector_client::ConnectorClient;
pub use hummock_meta_client::{CompactTaskItem, HummockMetaClient};
pub use meta_client::MetaClient;
pub use stream_client::{StreamClient, StreamClientPool, StreamClientPoolRef};

// TODO: Can I remove this import?
#[allow(unused_imports)]
use crate::meta_client::get_channel_no_retry;
// TODO: Can I remove this import?
#[allow(unused_imports)]
use crate::meta_client::get_channel_with_defaults;

#[async_trait]
pub trait RpcClient: Send + Sync + 'static + Clone {
    async fn new_client(host_addr: HostAddr) -> Result<Self>;

    async fn new_clients(host_addr: HostAddr, size: usize) -> Result<Vec<Self>> {
        try_join_all(repeat(host_addr).take(size).map(Self::new_client)).await
    }
}

#[derive(Clone)]
pub struct RpcClientPool<S> {
    connection_pool_size: u16,

    #[cfg(not(madsim))]
    clients: Cache<HostAddr, Vec<S>>,

    // moka::Cache internally uses system thread, so we can't use it in simulation
    #[cfg(madsim)]
    clients: Arc<Mutex<HashMap<HostAddr, S>>>,
}

impl<S> Default for RpcClientPool<S>
where
    S: RpcClient,
{
    fn default() -> Self {
        Self::new(1)
    }
}

impl<S> RpcClientPool<S>
where
    S: RpcClient,
{
    pub fn new(connection_pool_size: u16) -> Self {
        Self {
            connection_pool_size,
            #[cfg(not(madsim))]
            clients: Cache::new(u64::MAX),
            #[cfg(madsim)]
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Gets the RPC client for the given node. If the connection is not established, a
    /// new client will be created and returned.
    pub async fn get(&self, node: &WorkerNode) -> Result<S> {
        let addr: HostAddr = node.get_host().unwrap().into();
        self.get_by_addr(addr).await
    }

    /// Gets the RPC client for the given addr. If the connection is not established, a
    /// new client will be created and returned.
    #[cfg(not(madsim))]
    pub async fn get_by_addr(&self, addr: HostAddr) -> Result<S> {
        Ok(self
            .clients
            .try_get_with(
                addr.clone(),
                S::new_clients(addr, self.connection_pool_size as usize),
            )
            .await
            .map_err(|e| -> RpcError { anyhow!("failed to create RPC client: {:?}", e).into() })?
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone())
    }

    #[cfg(madsim)]
    pub async fn get_by_addr(&self, addr: HostAddr) -> Result<S> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&addr) {
            return Ok(client.clone());
        }
        let client = S::new_client(addr.clone()).await?;
        clients.insert(addr, client.clone());
        Ok(client)
    }
}

/// `ExtraInfoSource` is used by heartbeat worker to pull extra info that needs to be piggybacked.
#[async_trait::async_trait]
pub trait ExtraInfoSource: Send + Sync {
    /// None means the info is not available at the moment.
    async fn get_extra_info(&self) -> Option<extra_info::Info>;
}

pub type ExtraInfoSourceRef = Arc<dyn ExtraInfoSource>;

#[macro_export]
macro_rules! rpc_client_method_impl {
    ($( { $client:tt, $fn_name:ident, $req:ty, $resp:ty }),*) => {
        $(
            pub async fn $fn_name(&self, request: $req) -> $crate::Result<$resp> {
                Ok(self
                    .$client
                    .to_owned()
                    .$fn_name(request)
                    .await?
                    .into_inner())
            }
        )*
    }
}

// separate macro for connections against meta server to handle meta node failover
#[macro_export]
macro_rules! meta_rpc_client_method_impl {
    ($( { $client:tt, $fn_name:ident, $req:ty, $resp:ty }),*) => {
        $(
            // TODO: what is the difference between this and d97842031?
            // Why is the current version so spammy?
            // Cannot be only because I am using a request without a retry
            pub async fn $fn_name(&self, request: $req) -> $crate::Result<$resp> {
                let req_clone = request.clone();
                {
                    let response = self
                        .$client
                        .as_ref()
                        .lock()
                        .await
                        .$fn_name(request.clone())
                        .await;

                    // meta server is leader. Connection is correct
                    if response.is_ok() {
                        return Ok(response.unwrap().into_inner());
                    }

                    // TODO: use MetaClient::failover function

                    // It is not the below part that causes the spam: TODO: delete comment
                    // Use heartbeat request to check if we are connected against leader
                    let mut hc = self.heartbeat_client.as_ref().lock().await;
                    let h_response = hc.heartbeat(HeartbeatRequest {
                        node_id: u32::MAX,
                        info: vec![]
                    }).await;
                    let correct_connection = if h_response.is_ok() {
                        true
                    } else {
                        // { code: Unknown, message: "error reading a body from connection: broken pipe", source: Some(hyper::Error(Body, Error { kind: Io(Kind(BrokenPipe)) })) }
                        // TODO if unknown, then check if it is broken pipe
                        let err_code = h_response.err().unwrap().code();
                        err_code != tonic::Code::Unavailable
                            && err_code != tonic::Code::Unimplemented
                            && err_code != tonic::Code::Unknown
                   };
                    if correct_connection {
                        response?;
                    }
                } // release MutexGuard on $client

                tracing::info!("handle incorrect connection");

                // TODO: The entire approach is incorrect:
                // Response may also be error, even if connecting against live leader

                // On failover, we potentially have to wait for the election process
                // FIXME: reduce retry time after PR is merged
                // https://github.com/risingwavelabs/risingwave/pull/7179
                let sleep_duration = Duration::from_millis(1000);
                for _ in 0..25 {
                    // TODO: add indentation for loop

                // Invalid connection. Meta service is follower or down
                let resp = self.leader_client.as_ref().lock().await
                    .leader(LeaderRequest {})
                    .await;

                let leader_addr_response = match resp {
                    Ok(resp) => {
                        tracing::info!("Tried to connect against follower. Getting leader address");
                        resp
                    }
                    Err(_) => {
                        tracing::warn!("Meta node down. Getting leader info from different node");

                        // TODO: We do not need to sleep here. Just retry the connection below (recursion?)
                        // At some point we will get the up to date leader and connect against it
                        // Introduce some short async sleep to during retries


                        // We need to give time to all meta nodes to know who the new leader is
                        // How can we make this more resilient?
                        // tokio::time::sleep(std::time::Duration::from_secs(20)).await;

                        // TODO: this have to be the actual addresses from the cmd line arg
                        // TODO: order this again
                        let node_addresses = vec![5690, 15690, 25690];
                        let meta_channel = util(&node_addresses).await.unwrap_or_else(|| {
                            panic!("All meta nodes are down. Tried to connect against nodes at these addresses: {:?}",
                                node_addresses)
                        });
                        let mut leader_client = LeaderServiceClient::new(meta_channel);
                        leader_client.leader(LeaderRequest{}).await?
                    }
                };

                let leader_addr = leader_addr_response
                    .into_inner()
                    .leader_addr
                    .expect("Expect that leader service always knows who leader is");

                // TODO: If http or https should be decided by param that we pass to the image
                let addr = format!(
                    "http://{}:{}",
                    leader_addr.get_host(),
                    leader_addr.get_port()
                );
                tracing::info!("Connecting against meta leader node {}", addr);
                // TODO: remove comments
                // If I use get_channel_with_defaults, then I have no more spam
                let leader_channel = get_channel_no_retry(addr.as_str()).await;
                if leader_channel.is_err() {
                    tracing::warn!("Leader info seems to be outdated. Connection against {} failed. Try again...", addr);
                    tokio::time::sleep(sleep_duration).await;
                    continue;
                } else {
                    // TODO: Delete else branch. Debugging only
                    tracing::info!("established channel against {}",  addr);
                }
                // Probe node using heartbeat client?

                let leader_channel = leader_channel?;

                // Hold locks on all sub-clients, to update atomically
                {
                    let mut leader_c = self.leader_client.as_ref().lock().await;
                    let mut cluster_c = self.cluster_client.as_ref().lock().await;
                    let mut heartbeat_c = self.heartbeat_client.as_ref().lock().await;
                    let mut ddl_c = self.ddl_client.as_ref().lock().await;
                    let mut hummock_c = self.hummock_client.as_ref().lock().await;
                    let mut notification_c = self.notification_client.as_ref().lock().await;
                    let mut stream_c = self.stream_client.as_ref().lock().await;
                    let mut user_c = self.user_client.as_ref().lock().await;
                    let mut scale_c = self.scale_client.as_ref().lock().await;
                    let mut backup_c = self.backup_client.as_ref().lock().await;

                    *leader_c = LeaderServiceClient::new(leader_channel.clone());
                    *cluster_c = ClusterServiceClient::new(leader_channel.clone());
                    *heartbeat_c = HeartbeatServiceClient::new(leader_channel.clone());
                    *ddl_c = DdlServiceClient::new(leader_channel.clone());
                    *hummock_c = HummockManagerServiceClient::new(leader_channel.clone());
                    *notification_c = NotificationServiceClient::new(leader_channel.clone());
                    *stream_c = StreamManagerServiceClient::new(leader_channel.clone());
                    *user_c = UserServiceClient::new(leader_channel.clone());
                    *scale_c = ScaleServiceClient::new(leader_channel.clone());
                    *backup_c = BackupServiceClient::new(leader_channel);
                } // release MutexGuards on all clients

                // TODO: Recursive in case we have another faulty connection?
                // self.$fn_name(req_clone).await

                tracing::info!("updated client to new connection");

                let response = self
                    .$client
                    .as_ref()
                    .lock()
                    .await
                    .$fn_name(req_clone)
                    .await?;
                tracing::info!("Response send again and response succeeded");

                return Ok(response.into_inner());
            }
            panic!("unreachable code");
            }
        )*
    }
}
