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

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use chrono::Utc;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_pb::connector_service::sink_writer_to_coordinator_msg::{
    CommitRequest, StartCoordinationRequest,
};
use risingwave_pb::connector_service::{
    sink_coordinator_to_writer_msg, sink_writer_to_coordinator_msg, SinkCoordinatorToWriterMsg,
    SinkMetadata, SinkWriterToCoordinatorMsg,
};
use risingwave_rpc_client::{BidiStreamHandle, SinkCoordinationRpcClient};
use tracing::{debug, info, warn};

use crate::sink::{Result, SinkError, SinkParam, SinkWriter};

struct CoordinatorStreamHandle(
    BidiStreamHandle<SinkWriterToCoordinatorMsg, SinkCoordinatorToWriterMsg>,
);

impl CoordinatorStreamHandle {
    async fn new(
        mut client: SinkCoordinationRpcClient,
        param: SinkParam,
        vnode_bitmap: Bitmap,
    ) -> Result<Self> {
        let (stream_handle, first_response) = BidiStreamHandle::initialize(
            SinkWriterToCoordinatorMsg {
                msg: Some(sink_writer_to_coordinator_msg::Msg::StartRequest(
                    StartCoordinationRequest {
                        vnode_bitmap: Some(vnode_bitmap.to_protobuf()),
                        param: Some(param.to_proto()),
                    },
                )),
            },
            |req_stream| async move { client.coordinate(req_stream).await },
        )
        .await?;
        match first_response {
            SinkCoordinatorToWriterMsg {
                msg: Some(sink_coordinator_to_writer_msg::Msg::StartResponse(_)),
            } => Ok(Self(stream_handle)),
            msg => Err(SinkError::Coordinator(anyhow!(
                "should get start response but get {:?}",
                msg
            ))),
        }
    }

    async fn commit(&mut self, epoch: u64, metadata: SinkMetadata) -> Result<()> {
        self.0
            .send_request(SinkWriterToCoordinatorMsg {
                msg: Some(sink_writer_to_coordinator_msg::Msg::CommitRequest(
                    CommitRequest {
                        epoch,
                        metadata: Some(metadata),
                    },
                )),
            })
            .await?;
        match self.0.next_response().await? {
            SinkCoordinatorToWriterMsg {
                msg: Some(sink_coordinator_to_writer_msg::Msg::CommitResponse(_)),
            } => Ok(()),
            msg => Err(SinkError::Coordinator(anyhow!(
                "should get commit response but get {:?}",
                msg
            ))),
        }
    }
}

pub struct CoordinatedSinkWriter<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> {
    epoch: u64,
    coordinator_stream_handle: CoordinatorStreamHandle,
    inner: W,
}

impl<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> CoordinatedSinkWriter<W> {
    pub async fn new(
        client: SinkCoordinationRpcClient,
        param: SinkParam,
        vnode_bitmap: Bitmap,
        inner: W,
    ) -> Result<Self> {
        Ok(Self {
            epoch: 0,
            coordinator_stream_handle: CoordinatorStreamHandle::new(client, param, vnode_bitmap)
                .await?,
            inner,
        })
    }
}

#[async_trait::async_trait]
impl<W: SinkWriter<CommitMetadata = Option<SinkMetadata>>> SinkWriter for CoordinatedSinkWriter<W> {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        self.inner.begin_epoch(epoch).await
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.inner.write_batch(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        let start_time = Instant::now();
        let metadata = self.inner.barrier(is_checkpoint).await?;
        if is_checkpoint {
            debug!("take {:?} to fetch metadata", start_time.elapsed());
        }
        if is_checkpoint {
            let metadata = metadata.ok_or(SinkError::Coordinator(anyhow!(
                "should get metadata on checkpoint barrier"
            )))?;
            let start_time = Instant::now();
            self.coordinator_stream_handle
                .commit(self.epoch, metadata)
                .await?;
            debug!("take {:?} to commit metadata", start_time.elapsed());
            Ok(())
        } else {
            if metadata.is_some() {
                warn!("get metadata on non-checkpoint barrier");
            }
            Ok(())
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Bitmap) -> Result<()> {
        self.inner.update_vnode_bitmap(vnode_bitmap).await
    }
}
