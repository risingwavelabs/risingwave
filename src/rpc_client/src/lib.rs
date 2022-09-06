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

#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::unused_async)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![deny(rustdoc::broken_intra_doc_links)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(result_option_inspect)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(generators)]

mod meta_client;

#[cfg(madsim)]
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(not(madsim))]
use anyhow::anyhow;
use async_trait::async_trait;
pub use meta_client::{GrpcMetaClient, MetaClient};
#[cfg(not(madsim))]
use moka::future::Cache;
mod compute_client;
pub use compute_client::*;
mod hummock_meta_client;
pub use hummock_meta_client::HummockMetaClient;
pub mod error;
mod stream_client;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::heartbeat_request::extra_info;
pub use stream_client::*;
#[cfg(madsim)]
use tokio::sync::Mutex;

use crate::error::Result;

#[async_trait]
pub trait RpcClient: Send + Sync + 'static + Clone {
    async fn new_client(host_addr: HostAddr) -> Result<Self>;
}

#[derive(Clone)]
pub struct RpcClientPool<S> {
    #[cfg(not(madsim))]
    clients: Cache<HostAddr, S>,

    // moka::Cache internally uses system thread, so we can't use it in simulation
    #[cfg(madsim)]
    clients: Arc<Mutex<HashMap<HostAddr, S>>>,
}

impl<S> Default for RpcClientPool<S>
where
    S: RpcClient,
{
    fn default() -> Self {
        Self::new(u64::MAX)
    }
}

impl<S> RpcClientPool<S>
where
    S: RpcClient,
{
    pub fn new(cache_capacity: u64) -> Self {
        Self {
            #[cfg(not(madsim))]
            clients: Cache::new(cache_capacity),
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
        self.clients
            .try_get_with(addr.clone(), S::new_client(addr))
            .await
            .map_err(|e| anyhow!("failed to create RPC client: {:?}", e).into())
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
    ([], $( { $client:tt, $fn_name:ident, $req:ty, $resp:ty }),*) => {
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
