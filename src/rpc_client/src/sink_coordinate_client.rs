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

use std::future::Future;

use anyhow::anyhow;
use futures::Stream;
use risingwave_common::buffer::Bitmap;
use risingwave_pb::connector_service::sink_writer_to_coordinator_msg::{
    CommitRequest, StartCoordinationRequest,
};
use risingwave_pb::connector_service::{
    sink_coordinator_to_writer_msg, sink_writer_to_coordinator_msg, PbSinkParam,
    SinkCoordinatorToWriterMsg, SinkMetadata, SinkWriterToCoordinatorMsg,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{BidiStreamHandle, SinkCoordinationRpcClient};

pub type CoordinatorStreamHandle =
    BidiStreamHandle<SinkWriterToCoordinatorMsg, SinkCoordinatorToWriterMsg>;

impl CoordinatorStreamHandle {
    pub async fn new(
        mut client: SinkCoordinationRpcClient,
        param: PbSinkParam,
        vnode_bitmap: Bitmap,
    ) -> anyhow::Result<Self> {
        Self::new_with_init_stream(param, vnode_bitmap, |req_stream| async move {
            client.coordinate(req_stream).await
        })
        .await
    }

    pub async fn new_with_init_stream<F, St, Fut>(
        param: PbSinkParam,
        vnode_bitmap: Bitmap,
        init_stream: F,
    ) -> anyhow::Result<Self>
    where
        F: FnOnce(Request<ReceiverStream<SinkWriterToCoordinatorMsg>>) -> Fut,
        St: Stream<Item = Result<SinkCoordinatorToWriterMsg, Status>> + Send + Unpin + 'static,
        Fut: Future<Output = Result<Response<St>, Status>> + Send,
    {
        let (stream_handle, first_response) = BidiStreamHandle::initialize(
            SinkWriterToCoordinatorMsg {
                msg: Some(sink_writer_to_coordinator_msg::Msg::StartRequest(
                    StartCoordinationRequest {
                        vnode_bitmap: Some(vnode_bitmap.to_protobuf()),
                        param: Some(param),
                    },
                )),
            },
            init_stream,
        )
        .await?;
        match first_response {
            SinkCoordinatorToWriterMsg {
                msg: Some(sink_coordinator_to_writer_msg::Msg::StartResponse(_)),
            } => Ok(stream_handle),
            msg => Err(anyhow!("should get start response but get {:?}", msg)),
        }
    }

    pub async fn commit(&mut self, epoch: u64, metadata: SinkMetadata) -> anyhow::Result<()> {
        self.send_request(SinkWriterToCoordinatorMsg {
            msg: Some(sink_writer_to_coordinator_msg::Msg::CommitRequest(
                CommitRequest {
                    epoch,
                    metadata: Some(metadata),
                },
            )),
        })
        .await?;
        match self.next_response().await? {
            SinkCoordinatorToWriterMsg {
                msg: Some(sink_coordinator_to_writer_msg::Msg::CommitResponse(_)),
            } => Ok(()),
            msg => Err(anyhow!("should get commit response but get {:?}", msg)),
        }
    }
}
