// Copyright 2024 RisingWave Labs
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
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(coroutines)]
#![feature(iterator_try_collect)]
#![feature(hash_extract_if)]
#![feature(try_blocks)]
#![feature(let_chains)]
#![feature(impl_trait_in_assoc_type)]
#![feature(error_generic_member_access)]
#![feature(panic_update_hook)]
#![feature(negative_impls)]

use std::any::type_name;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::iter::repeat;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::stream::{BoxStream, Peekable};
use futures::{Stream, StreamExt};
use moka::future::Cache;
use rand::prelude::SliceRandom;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::heartbeat_request::extra_info;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};

pub mod error;
use error::Result;
mod compactor_client;
mod compute_client;
mod connector_client;
mod hummock_meta_client;
mod meta_client;
mod sink_coordinate_client;
mod stream_client;
mod tracing;

pub use compactor_client::{CompactorClient, GrpcCompactorProxyClient};
pub use compute_client::{ComputeClient, ComputeClientPool, ComputeClientPoolRef};
pub use connector_client::{ConnectorClient, SinkCoordinatorStreamHandle, SinkWriterStreamHandle};
pub use hummock_meta_client::{CompactionEventItem, HummockMetaClient};
pub use meta_client::{MetaClient, SinkCoordinationRpcClient};
use rw_futures_util::await_future_with_monitor_error_stream;
pub use sink_coordinate_client::CoordinatorStreamHandle;
pub use stream_client::{
    StreamClient, StreamClientPool, StreamClientPoolRef, StreamingControlHandle,
};

#[async_trait]
pub trait RpcClient: Send + Sync + 'static + Clone {
    async fn new_client(host_addr: HostAddr) -> Result<Self>;

    async fn new_clients(host_addr: HostAddr, size: usize) -> Result<Arc<Vec<Self>>> {
        try_join_all(repeat(host_addr).take(size).map(Self::new_client))
            .await
            .map(Arc::new)
    }
}

#[derive(Clone)]
pub struct RpcClientPool<S> {
    connection_pool_size: u16,

    clients: Cache<HostAddr, Arc<Vec<S>>>,
}

impl<S> std::fmt::Debug for RpcClientPool<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClientPool")
            .field("connection_pool_size", &self.connection_pool_size)
            .field("type", &type_name::<S>())
            .field("len", &self.clients.entry_count())
            .finish()
    }
}

/// Intentionally not implementing `Default` to let callers be explicit about the pool size.
impl<S> !Default for RpcClientPool<S> {}

impl<S> RpcClientPool<S>
where
    S: RpcClient,
{
    /// Create a new pool with the given `connection_pool_size`, which is the number of
    /// connections to each node that will be reused.
    pub fn new(connection_pool_size: u16) -> Self {
        Self {
            connection_pool_size,
            clients: Cache::new(u64::MAX),
        }
    }

    /// Create a pool for testing purposes. Same as [`Self::adhoc`].
    pub fn for_test() -> Self {
        Self::adhoc()
    }

    /// Create a pool for ad-hoc usage, where the number of connections to each node is 1.
    pub fn adhoc() -> Self {
        Self::new(1)
    }

    /// Gets the RPC client for the given node. If the connection is not established, a
    /// new client will be created and returned.
    pub async fn get(&self, node: &WorkerNode) -> Result<S> {
        let addr: HostAddr = node.get_host().unwrap().into();
        self.get_by_addr(addr).await
    }

    /// Gets the RPC client for the given addr. If the connection is not established, a
    /// new client will be created and returned.
    pub async fn get_by_addr(&self, addr: HostAddr) -> Result<S> {
        Ok(self
            .clients
            .try_get_with(
                addr.clone(),
                S::new_clients(addr.clone(), self.connection_pool_size as usize),
            )
            .await
            .with_context(|| format!("failed to create RPC client to {addr}"))?
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone())
    }

    pub fn invalidate_all(&self) {
        self.clients.invalidate_all()
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
macro_rules! stream_rpc_client_method_impl {
    ($( { $client:tt, $fn_name:ident, $req:ty, $resp:ty }),*) => {
        $(
            pub async fn $fn_name(&self, request: $req) -> $crate::Result<$resp> {
                Ok(self
                    .$client
                    .to_owned()
                    .$fn_name(request)
                    .await
                    .map_err($crate::error::RpcError::from_stream_status)?
                    .into_inner())
            }
        )*
    }
}

#[macro_export]
macro_rules! meta_rpc_client_method_impl {
    ($( { $client:tt, $fn_name:ident, $req:ty, $resp:ty }),*) => {
        $(
            pub async fn $fn_name(&self, request: $req) -> $crate::Result<$resp> {
                let mut client = self.core.read().await.$client.to_owned();
                match client.$fn_name(request).await {
                    Ok(resp) => Ok(resp.into_inner()),
                    Err(e) => {
                        self.refresh_client_if_needed(e.code()).await;
                        Err($crate::error::RpcError::from_meta_status(e))
                    }
                }
            }
        )*
    }
}

pub const DEFAULT_BUFFER_SIZE: usize = 16;

pub struct BidiStreamSender<REQ> {
    tx: Sender<REQ>,
}

impl<REQ> BidiStreamSender<REQ> {
    pub async fn send_request<R: Into<REQ>>(&mut self, request: R) -> Result<()> {
        self.tx
            .send(request.into())
            .await
            .map_err(|_| anyhow!("unable to send request {}", type_name::<REQ>()).into())
    }
}

pub struct BidiStreamReceiver<RSP> {
    pub stream: Peekable<BoxStream<'static, Result<RSP>>>,
}

impl<RSP> BidiStreamReceiver<RSP> {
    pub async fn next_response(&mut self) -> Result<RSP> {
        self.stream
            .next()
            .await
            .ok_or_else(|| anyhow!("end of response stream"))?
    }
}

pub struct BidiStreamHandle<REQ, RSP> {
    pub request_sender: BidiStreamSender<REQ>,
    pub response_stream: BidiStreamReceiver<RSP>,
}

impl<REQ, RSP> Debug for BidiStreamHandle<REQ, RSP> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(type_name::<Self>())
    }
}

impl<REQ, RSP> BidiStreamHandle<REQ, RSP> {
    pub fn for_test(
        request_sender: Sender<REQ>,
        response_stream: BoxStream<'static, Result<RSP>>,
    ) -> Self {
        Self {
            request_sender: BidiStreamSender { tx: request_sender },
            response_stream: BidiStreamReceiver {
                stream: response_stream.peekable(),
            },
        }
    }

    pub async fn initialize<
        F: FnOnce(Receiver<REQ>) -> Fut,
        St: Stream<Item = Result<RSP>> + Send + Unpin + 'static,
        Fut: Future<Output = Result<St>> + Send,
        R: Into<REQ>,
    >(
        first_request: R,
        init_stream_fn: F,
    ) -> Result<(Self, RSP)> {
        let (request_sender, request_receiver) = channel(DEFAULT_BUFFER_SIZE);

        // Send initial request in case of the blocking receive call from creating streaming request
        request_sender
            .send(first_request.into())
            .await
            .map_err(|_err| anyhow!("unable to send first request of {}", type_name::<REQ>()))?;

        let mut response_stream = init_stream_fn(request_receiver).await?;

        let first_response = response_stream
            .next()
            .await
            .ok_or_else(|| anyhow!("get empty response from first request"))??;

        Ok((
            Self {
                request_sender: BidiStreamSender { tx: request_sender },
                response_stream: BidiStreamReceiver {
                    stream: response_stream.boxed().peekable(),
                },
            },
            first_response,
        ))
    }

    pub async fn next_response(&mut self) -> Result<RSP> {
        self.response_stream.next_response().await
    }

    pub async fn send_request(&mut self, request: REQ) -> Result<()> {
        match await_future_with_monitor_error_stream(
            &mut self.response_stream.stream,
            self.request_sender.send_request(request),
        )
        .await
        {
            Ok(send_result) => send_result,
            Err(None) => Err(anyhow!("end of response stream").into()),
            Err(Some(e)) => Err(e),
        }
    }
}

/// The handle of a bidi-stream started from the rpc client. It is similar to the `BidiStreamHandle`
/// except that its sender is unbounded.
pub struct UnboundedBidiStreamHandle<REQ, RSP> {
    pub request_sender: UnboundedSender<REQ>,
    pub response_stream: BoxStream<'static, Result<RSP>>,
}

impl<REQ, RSP> Debug for UnboundedBidiStreamHandle<REQ, RSP> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(type_name::<Self>())
    }
}

impl<REQ, RSP> UnboundedBidiStreamHandle<REQ, RSP> {
    pub async fn initialize<
        F: FnOnce(UnboundedReceiver<REQ>) -> Fut,
        St: Stream<Item = Result<RSP>> + Send + Unpin + 'static,
        Fut: Future<Output = Result<St>> + Send,
        R: Into<REQ>,
    >(
        first_request: R,
        init_stream_fn: F,
    ) -> Result<(Self, RSP)> {
        let (request_sender, request_receiver) = unbounded_channel();

        // Send initial request in case of the blocking receive call from creating streaming request
        request_sender
            .send(first_request.into())
            .map_err(|_err| anyhow!("unable to send first request of {}", type_name::<REQ>()))?;

        let mut response_stream = init_stream_fn(request_receiver).await?;

        let first_response = response_stream
            .next()
            .await
            .context("get empty response from first request")??;

        Ok((
            Self {
                request_sender,
                response_stream: response_stream.boxed(),
            },
            first_response,
        ))
    }

    pub async fn next_response(&mut self) -> Result<RSP> {
        self.response_stream
            .next()
            .await
            .ok_or_else(|| anyhow!("end of response stream"))?
    }

    pub fn send_request(&mut self, request: REQ) -> Result<()> {
        self.request_sender
            .send(request)
            .map_err(|_| anyhow!("unable to send request {}", type_name::<REQ>()).into())
    }
}
