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

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::connector_service::connector_service_client::ConnectorServiceClient;
use risingwave_pb::connector_service::get_event_stream_request::{
    Request, StartSource, ValidateProperties,
};
use risingwave_pb::connector_service::*;
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;

use crate::error::Result;
use crate::RpcClient;

#[derive(Clone)]
pub struct ConnectorClient(ConnectorServiceClient<Channel>);

impl ConnectorClient {
    pub async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self(ConnectorServiceClient::new(channel)))
    }

    /// Get source event stream
    pub async fn start_source_stream(
        &self,
        source_id: u64,
        source_type: SourceType,
        start_offset: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Streaming<GetEventStreamResponse>> {
        Ok(self
            .0
            .to_owned()
            .get_event_stream(GetEventStreamRequest {
                request: Some(Request::Start(StartSource {
                    source_id,
                    source_type: source_type as _,
                    start_offset: start_offset.unwrap_or_default(),
                    properties,
                })),
            })
            .await
            .inspect_err(|err| {
                tracing::error!(
                    "failed to start stream for CDC source {}: {}",
                    source_id,
                    err.message()
                )
            })?
            .into_inner())
    }

    /// Validate source properties
    pub async fn validate_properties(
        &self,
        source_id: u64,
        source_type: SourceType,
        properties: HashMap<String, String>,
        table_schema: Option<TableSchema>,
    ) -> Result<Streaming<GetEventStreamResponse>> {
        Ok(self
            .0
            .to_owned()
            .get_event_stream(GetEventStreamRequest {
                request: Some(Request::Validate(ValidateProperties {
                    source_id,
                    source_type: source_type as _,
                    properties,
                    table_schema,
                })),
            })
            .await
            .inspect_err(|err| {
                tracing::error!(
                    "failed to validate source {} properties: {}",
                    source_id,
                    err.message()
                )
            })?
            .into_inner())
    }
}

#[async_trait]
impl RpcClient for ConnectorClient {
    async fn new_client(host_addr: HostAddr) -> Result<Self> {
        Self::new(host_addr).await
    }
}
