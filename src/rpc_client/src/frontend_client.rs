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

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_common::monitor::{EndpointExt, TcpConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::frontend_service::frontend_service_client::FrontendServiceClient;
use risingwave_pb::frontend_service::{GetTableReplacePlanRequest, GetTableReplacePlanResponse};
use tonic::transport::Endpoint;

use crate::error::{Result, RpcError};
use crate::tracing::{Channel, TracingInjectedChannelExt};
use crate::{frontend_rpc_client_method_impl, RpcClient, RpcClientPool};

#[derive(Clone)]
pub struct FrontendClient(FrontendServiceClient<Channel>);

#[async_trait]
impl RpcClient for FrontendClient {
    async fn new_client(host_addr: HostAddr) -> Result<Self> {
        Self::new(host_addr).await
    }
}

impl FrontendClient {
    async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(5))
            .monitored_connect(
                "grpc-frontend-client",
                TcpConfig {
                    tcp_nodelay: true,
                    keepalive_duration: None,
                },
            )
            .await?
            .tracing_injected();

        Ok(Self(
            FrontendServiceClient::new(channel).max_decoding_message_size(usize::MAX),
        ))
    }
}

// similar to the stream_client used in the Meta node
pub type FrontendClientPool = RpcClientPool<FrontendClient>;
pub type FrontendClientPoolRef = Arc<FrontendClientPool>;

macro_rules! for_all_frontend_rpc {
    ($macro:ident) => {
        $macro! {
             { 0, get_table_replace_plan, GetTableReplacePlanRequest, GetTableReplacePlanResponse }
        }
    };
}

impl FrontendClient {
    for_all_frontend_rpc! { frontend_rpc_client_method_impl }
}
