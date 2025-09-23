// Copyright 2025 RisingWave Labs
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

use anyhow::anyhow;
use risingwave_pb::connector_service::sink_coordinator_stream_request::CommitMetadata;
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::Payload;
use risingwave_pb::connector_service::sink_writer_stream_request::{
    Barrier, Request as SinkRequest, WriteBatch,
};
use risingwave_pb::connector_service::sink_writer_stream_response::CommitResponse;
use risingwave_pb::connector_service::*;

use crate::error::{Result, RpcError};
use crate::{BidiStreamHandle, BidiStreamReceiver, BidiStreamSender};

pub type SinkWriterRequestSender<REQ = SinkWriterStreamRequest> = BidiStreamSender<REQ>;
pub type SinkWriterResponseReceiver = BidiStreamReceiver<SinkWriterStreamResponse>;

pub type SinkWriterStreamHandle<REQ = SinkWriterStreamRequest> =
    BidiStreamHandle<REQ, SinkWriterStreamResponse>;

impl<REQ: From<SinkWriterStreamRequest>> SinkWriterRequestSender<REQ> {
    pub async fn write_batch(&mut self, epoch: u64, batch_id: u64, payload: Payload) -> Result<()> {
        self.send_request(SinkWriterStreamRequest {
            request: Some(SinkRequest::WriteBatch(WriteBatch {
                epoch,
                batch_id,
                payload: Some(payload),
            })),
        })
        .await
    }

    pub async fn barrier(&mut self, epoch: u64, is_checkpoint: bool) -> Result<()> {
        self.send_request(SinkWriterStreamRequest {
            request: Some(SinkRequest::Barrier(Barrier {
                epoch,
                is_checkpoint,
            })),
        })
        .await
    }
}

impl SinkWriterResponseReceiver {
    pub async fn next_commit_response(&mut self) -> Result<CommitResponse> {
        loop {
            match self.next_response().await? {
                SinkWriterStreamResponse {
                    response: Some(sink_writer_stream_response::Response::Commit(rsp)),
                } => return Ok(rsp),
                SinkWriterStreamResponse {
                    response: Some(sink_writer_stream_response::Response::Batch(_)),
                } => continue,
                msg => {
                    return Err(RpcError::Internal(anyhow!(
                        "should get Sync response but get {:?}",
                        msg
                    )));
                }
            }
        }
    }
}

impl<REQ: From<SinkWriterStreamRequest>> SinkWriterStreamHandle<REQ> {
    pub async fn write_batch(&mut self, epoch: u64, batch_id: u64, payload: Payload) -> Result<()> {
        self.request_sender
            .write_batch(epoch, batch_id, payload)
            .await
    }

    pub async fn barrier(&mut self, epoch: u64) -> Result<()> {
        self.request_sender.barrier(epoch, false).await
    }

    pub async fn commit(&mut self, epoch: u64) -> Result<CommitResponse> {
        self.request_sender.barrier(epoch, true).await?;
        self.response_stream.next_commit_response().await
    }
}

pub type SinkCoordinatorStreamHandle =
    BidiStreamHandle<SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse>;

impl SinkCoordinatorStreamHandle {
    pub async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        self.send_request(SinkCoordinatorStreamRequest {
            request: Some(sink_coordinator_stream_request::Request::Commit(
                CommitMetadata { epoch, metadata },
            )),
        })
        .await?;
        match self.next_response().await? {
            SinkCoordinatorStreamResponse {
                response:
                    Some(sink_coordinator_stream_response::Response::Commit(
                        sink_coordinator_stream_response::CommitResponse {
                            epoch: response_epoch,
                        },
                    )),
            } => {
                if epoch == response_epoch {
                    Ok(())
                } else {
                    Err(RpcError::Internal(anyhow!(
                        "get different response epoch to commit epoch: {} {}",
                        epoch,
                        response_epoch
                    )))
                }
            }
            msg => Err(RpcError::Internal(anyhow!(
                "should get Commit response but get {:?}",
                msg
            ))),
        }
    }
}
