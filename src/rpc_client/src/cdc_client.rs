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

use std::time::Duration;

use async_trait::async_trait;
use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::cdc_service::cdc_service_client::CdcServiceClient;
use risingwave_pb::cdc_service::*;
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;

use crate::error::Result;
use crate::RpcClient;

#[derive(Clone)]
pub struct CdcClient(CdcServiceClient<Channel>);

impl CdcClient {
    pub async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self(CdcServiceClient::new(channel)))
    }

    pub async fn get_event_stream(
        &self,
        source_id: u64,
        props: DbConnectorProperties,
    ) -> Result<Streaming<GetEventStreamResponse>> {
        Ok(self
            .0
            .to_owned()
            .get_event_stream(GetEventStreamRequest {
                source_id,
                properties: Some(props),
            })
            .await
            .inspect_err(|_| {
                tracing::error!("failed to create event stream for CDC source {}", source_id)
            })?
            .into_inner())
    }
}

#[async_trait]
impl RpcClient for CdcClient {
    async fn new_client(host_addr: HostAddr) -> Result<Self> {
        Self::new(host_addr).await
    }
}
