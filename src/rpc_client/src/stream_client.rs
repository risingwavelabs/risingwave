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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use risingwave_pb::stream_service::*;
use tonic::transport::{Channel, Endpoint};

use crate::error::Result;
use crate::{rpc_client_method_impl, RpcClient, RpcClientPool};

#[derive(Clone)]
pub struct StreamClient(StreamServiceClient<Channel>);

#[async_trait]
impl RpcClient for StreamClient {
    async fn new_client(host_addr: HostAddr) -> Result<Self> {
        Self::new(host_addr).await
    }
}

impl StreamClient {
    async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self(StreamServiceClient::new(channel)))
    }
}

pub type StreamClientPool = RpcClientPool<StreamClient>;
pub type StreamClientPoolRef = Arc<StreamClientPool>;

macro_rules! for_all_stream_rpc {
    ($macro:ident) => {
        $macro! {
             { 0, update_actors, UpdateActorsRequest, UpdateActorsResponse }
            ,{ 0, build_actors, BuildActorsRequest, BuildActorsResponse }
            ,{ 0, broadcast_actor_info_table, BroadcastActorInfoTableRequest, BroadcastActorInfoTableResponse }
            ,{ 0, drop_actors, DropActorsRequest, DropActorsResponse }
            ,{ 0, force_stop_actors, ForceStopActorsRequest, ForceStopActorsResponse}
            ,{ 0, inject_barrier, InjectBarrierRequest, InjectBarrierResponse }
            ,{ 0, barrier_complete, BarrierCompleteRequest, BarrierCompleteResponse }
            ,{ 0, wait_epoch_commit, WaitEpochCommitRequest, WaitEpochCommitResponse }
        }
    };
}

impl StreamClient {
    for_all_stream_rpc! { rpc_client_method_impl }
}
