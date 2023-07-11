// Copyright 2023 RisingWave Labs
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

use std::any::type_name;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::StreamExt;
use risingwave_common::config::{MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use risingwave_pb::catalog::SinkType;
use risingwave_pb::connector_service::connector_service_client::ConnectorServiceClient;
use risingwave_pb::connector_service::sink_stream_request::write_batch::Payload;
use risingwave_pb::connector_service::sink_stream_request::{
    Request as SinkRequest, StartEpoch, StartSink, SyncBatch, WriteBatch,
};
use risingwave_pb::connector_service::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};
use tracing::error;

use crate::error::{Result, RpcError};

#[derive(Clone, Debug)]
pub struct ConnectorClient {
    rpc_client: ConnectorServiceClient<Channel>,
    endpoint: String,
}

pub struct SinkWriterStreamHandle {
    request_sender: UnboundedSender<SinkStreamRequest>,
    response_stream: BoxStream<'static, std::result::Result<SinkResponse, Status>>,
}

impl Debug for SinkWriterStreamHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(type_name::<Self>())
    }
}

impl SinkWriterStreamHandle {
    pub fn new(
        request_sender: UnboundedSender<SinkStreamRequest>,
        response_stream: BoxStream<'static, std::result::Result<SinkResponse, Status>>,
    ) -> Self {
        Self {
            request_sender,
            response_stream,
        }
    }

    async fn next_response(&mut self) -> Result<SinkResponse> {
        Ok(self
            .response_stream
            .next()
            .await
            .ok_or(RpcError::Internal(anyhow!("end of response stream")))??)
    }

    pub async fn start_epoch(&mut self, epoch: u64) -> Result<()> {
        self.request_sender
            .send(SinkStreamRequest {
                request: Some(SinkRequest::StartEpoch(StartEpoch { epoch })),
            })
            .map_err(|_| RpcError::Internal(anyhow!("unable to send start epoch on {}", epoch)))?;
        match self.next_response().await? {
            SinkResponse {
                response: Some(sink_response::Response::StartEpoch(_)),
            } => Ok(()),
            msg => Err(RpcError::Internal(anyhow!(
                "should get StartEpoch response but get {:?}",
                msg
            ))),
        }
    }

    pub async fn write_batch(&mut self, epoch: u64, batch_id: u64, payload: Payload) -> Result<()> {
        self.request_sender
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Write(WriteBatch {
                    epoch,
                    batch_id,
                    payload: Some(payload),
                })),
            })
            .map_err(|_| {
                RpcError::Internal(anyhow!(
                    "unable to send write batch on {} with batch id {}",
                    epoch,
                    batch_id
                ))
            })?;
        match self.next_response().await? {
            SinkResponse {
                response: Some(sink_response::Response::Write(_)),
            } => Ok(()),
            msg => Err(RpcError::Internal(anyhow!(
                "should get Write response but get {:?}",
                msg
            ))),
        }
    }

    pub async fn commit(&mut self, epoch: u64) -> Result<()> {
        self.request_sender
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Sync(SyncBatch { epoch })),
            })
            .map_err(|_| RpcError::Internal(anyhow!("unable to send barrier on {}", epoch,)))?;
        match self.next_response().await? {
            SinkResponse {
                response: Some(sink_response::Response::Sync(_)),
            } => Ok(()),
            msg => Err(RpcError::Internal(anyhow!(
                "should get Sync response but get {:?}",
                msg
            ))),
        }
    }
}

impl ConnectorClient {
    pub async fn try_new(connector_endpoint: Option<&String>) -> Option<Self> {
        match connector_endpoint {
            None => None,
            Some(connector_endpoint) => match ConnectorClient::new(connector_endpoint).await {
                Ok(client) => Some(client),
                Err(e) => {
                    error!(
                        "invalid connector endpoint {:?}: {:?}",
                        connector_endpoint, e
                    );
                    None
                }
            },
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn new(connector_endpoint: &String) -> Result<Self> {
        let endpoint = Endpoint::from_shared(format!("http://{}", connector_endpoint))
            .map_err(|e| {
                RpcError::Internal(anyhow!(format!(
                    "invalid connector endpoint `{}`: {:?}",
                    &connector_endpoint, e
                )))
            })?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .initial_stream_window_size(STREAM_WINDOW_SIZE)
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5));

        let channel = {
            #[cfg(madsim)]
            {
                endpoint.connect().await?
            }
            #[cfg(not(madsim))]
            {
                endpoint.connect_lazy()
            }
        };
        Ok(Self {
            rpc_client: ConnectorServiceClient::new(channel),
            endpoint: connector_endpoint.to_string(),
        })
    }

    pub fn endpoint(&self) -> &String {
        &self.endpoint
    }

    /// Get source event stream
    pub async fn start_source_stream(
        &self,
        source_id: u64,
        source_type: SourceType,
        start_offset: Option<String>,
        properties: HashMap<String, String>,
        snapshot_done: bool,
    ) -> Result<Streaming<GetEventStreamResponse>> {
        tracing::info!(
            "start cdc source properties: {:?}, snapshot_done: {}",
            properties,
            snapshot_done
        );
        Ok(self
            .rpc_client
            .clone()
            .get_event_stream(GetEventStreamRequest {
                source_id,
                source_type: source_type as _,
                start_offset: start_offset.unwrap_or_default(),
                properties,
                snapshot_done,
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
    pub async fn validate_source_properties(
        &self,
        source_id: u64,
        source_type: SourceType,
        properties: HashMap<String, String>,
        table_schema: Option<TableSchema>,
    ) -> Result<()> {
        let response = self
            .rpc_client
            .clone()
            .validate_source(ValidateSourceRequest {
                source_id,
                source_type: source_type as _,
                properties,
                table_schema,
            })
            .await
            .inspect_err(|err| {
                tracing::error!("failed to validate source#{}: {}", source_id, err.message())
            })?
            .into_inner();

        response.error.map_or(Ok(()), |err| {
            Err(RpcError::Internal(anyhow!(format!(
                "source cannot pass validation: {}",
                err.error_message
            ))))
        })
    }

    pub async fn start_sink_writer_stream(
        &self,
        connector_type: String,
        sink_id: u64,
        properties: HashMap<String, String>,
        table_schema: Option<TableSchema>,
        sink_payload_format: SinkPayloadFormat,
    ) -> Result<SinkWriterStreamHandle> {
        let (request_sender, request_receiver) = unbounded_channel();

        // Send initial request in case of the blocking receive call from creating streaming request
        request_sender
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Start(StartSink {
                    format: sink_payload_format as i32,
                    sink_config: Some(SinkConfig {
                        connector_type,
                        properties,
                        table_schema,
                    }),
                    sink_id,
                })),
            })
            .map_err(|err| RpcError::Internal(anyhow!(err.to_string())))?;

        let mut response = self
            .rpc_client
            .clone()
            .sink_stream(Request::new(UnboundedReceiverStream::new(request_receiver)))
            .await
            .map_err(RpcError::GrpcStatus)?
            .into_inner();

        match response.next().await.ok_or(RpcError::Internal(anyhow!(
            "get empty response from start sink request"
        )))?? {
            SinkResponse {
                response: Some(sink_response::Response::Start(_)),
            } => Ok(SinkWriterStreamHandle {
                response_stream: response.boxed(),
                request_sender,
            }),
            msg => Err(RpcError::Internal(anyhow!(
                "should get start response but get {:?}",
                msg
            ))),
        }
    }

    pub async fn validate_sink_properties(
        &self,
        connector_type: String,
        properties: HashMap<String, String>,
        table_schema: Option<TableSchema>,
        sink_type: SinkType,
    ) -> Result<()> {
        let response = self
            .rpc_client
            .clone()
            .validate_sink(ValidateSinkRequest {
                sink_config: Some(SinkConfig {
                    connector_type,
                    properties,
                    table_schema,
                }),
                sink_type: sink_type as i32,
            })
            .await
            .inspect_err(|err| {
                tracing::error!("failed to validate sink properties: {}", err.message())
            })?
            .into_inner();
        response.error.map_or_else(
            || Ok(()), // If there is no error message, return Ok here.
            |err| {
                Err(RpcError::Internal(anyhow!(format!(
                    "sink cannot pass validation: {}",
                    err.error_message
                ))))
            },
        )
    }
}
