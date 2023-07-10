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
use risingwave_pb::connector_service::connector_service_client::ConnectorServiceClient;
use risingwave_pb::connector_service::sink_writer_request::write_batch::Payload;
use risingwave_pb::connector_service::sink_writer_request::{
    Barrier, BeginEpoch, Request as SinkRequest, StartSink, WriteBatch,
};
use risingwave_pb::connector_service::{
    sink_writer_response, GetEventStreamRequest, GetEventStreamResponse, SinkParam,
    SinkPayloadFormat, SinkWriterRequest, SinkWriterResponse, SourceType, TableSchema,
    ValidateSinkRequest, ValidateSourceRequest,
};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
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
    request_sender: Sender<SinkWriterRequest>,
    response_stream: BoxStream<'static, std::result::Result<SinkWriterResponse, Status>>,
}

impl Debug for SinkWriterStreamHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(type_name::<Self>())
    }
}

impl SinkWriterStreamHandle {
    pub fn new(
        request_sender: Sender<SinkWriterRequest>,
        response_stream: BoxStream<'static, std::result::Result<SinkWriterResponse, Status>>,
    ) -> Self {
        Self {
            request_sender,
            response_stream,
        }
    }

    pub async fn start_epoch(&mut self, epoch: u64) -> Result<()> {
        self.request_sender
            .send(SinkWriterRequest {
                request: Some(SinkRequest::BeginEpoch(BeginEpoch { epoch })),
            })
            .await
            .map_err(|_| RpcError::Internal(anyhow!("unable to send start epoch on {}", epoch)))
    }

    pub async fn write_batch(&mut self, epoch: u64, batch_id: u64, payload: Payload) -> Result<()> {
        self.request_sender
            .send(SinkWriterRequest {
                request: Some(SinkRequest::WriteBatch(WriteBatch {
                    epoch,
                    batch_id,
                    payload: Some(payload),
                })),
            })
            .await
            .map_err(|_| {
                RpcError::Internal(anyhow!(
                    "unable to send write batch on {} with batch id {}",
                    epoch,
                    batch_id
                ))
            })
    }

    pub async fn commit(&mut self, epoch: u64) -> Result<()> {
        self.request_sender
            .send(SinkWriterRequest {
                request: Some(SinkRequest::Barrier(Barrier {
                    epoch,
                    is_checkpoint: true,
                })),
            })
            .await
            .map_err(|_| RpcError::Internal(anyhow!("unable to send barrier on {}", epoch,)))?;
        match self
            .response_stream
            .next()
            .await
            .ok_or(RpcError::Internal(anyhow!("enable to get commit response")))??
        {
            SinkWriterResponse {
                response: Some(sink_writer_response::Response::Sync(_)),
            } => Ok(()),
            msg => Err(RpcError::Internal(anyhow!(
                "should get Sync response but get {:?}",
                msg
            ))),
        }
    }
}

impl ConnectorClient {
    pub async fn try_connect(connector_endpoint: Option<&String>) -> Option<Self> {
        match connector_endpoint {
            None => None,
            Some(connector_endpoint) => match ConnectorClient::new(connector_endpoint).await {
                Ok(client) => Some(client),
                Err(e) => {
                    error!(
                        "unable to connect to connector endpoint {:?}: {:?}",
                        connector_endpoint, e
                    );
                    None
                }
            },
        }
    }

    pub async fn new(connector_endpoint: &String) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", connector_endpoint))
            .map_err(|e| {
                RpcError::Internal(anyhow!(format!(
                    "invalid connector endpoint `{}`: {:?}",
                    &connector_endpoint, e
                )))
            })?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .initial_stream_window_size(STREAM_WINDOW_SIZE)
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
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
        sink_param: SinkParam,
        sink_payload_format: SinkPayloadFormat,
    ) -> Result<SinkWriterStreamHandle> {
        const SINK_WRITER_REQUEST_BUFFER_SIZE: usize = 16;
        let (request_sender, request_receiver) = channel(SINK_WRITER_REQUEST_BUFFER_SIZE);

        // Send initial request in case of the blocking receive call from creating streaming request
        request_sender
            .send(SinkWriterRequest {
                request: Some(SinkRequest::Start(StartSink {
                    sink_param: Some(sink_param),
                    format: sink_payload_format as i32,
                })),
            })
            .await
            .map_err(|err| RpcError::Internal(anyhow!(err.to_string())))?;

        let mut response = self
            .rpc_client
            .clone()
            .sink_stream(Request::new(ReceiverStream::new(request_receiver)))
            .await
            .map_err(RpcError::GrpcStatus)?
            .into_inner();

        response.next().await.ok_or(RpcError::Internal(anyhow!(
            "get empty response from start sink request"
        )))??;

        Ok(SinkWriterStreamHandle {
            response_stream: response.boxed(),
            request_sender,
        })
    }

    pub async fn validate_sink_properties(&self, sink_param: SinkParam) -> Result<()> {
        let response = self
            .rpc_client
            .clone()
            .validate_sink(ValidateSinkRequest {
                sink_param: Some(sink_param),
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
