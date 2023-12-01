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
use futures::{Stream, TryStreamExt};
use risingwave_common::buffer::Bitmap;
use risingwave_pb::connector_service::coordinate_request::{
    CommitRequest, StartCoordinationRequest,
};
use risingwave_pb::connector_service::{
    coordinate_request, coordinate_response, CoordinateRequest, CoordinateResponse, PbSinkParam,
    SinkMetadata,
};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use crate::error::RpcError;
use crate::{BidiStreamHandle, SinkCoordinationRpcClient};

pub type CoordinatorStreamHandle = BidiStreamHandle<CoordinateRequest, CoordinateResponse>;

impl CoordinatorStreamHandle {
    pub async fn new(
        mut client: SinkCoordinationRpcClient,
        param: PbSinkParam,
        vnode_bitmap: Bitmap,
    ) -> Result<Self, RpcError> {
        Self::new_with_init_stream(param, vnode_bitmap, |rx| async move {
            client.coordinate(ReceiverStream::new(rx)).await
        })
        .await
    }

    pub async fn new_with_init_stream<F, St, Fut>(
        param: PbSinkParam,
        vnode_bitmap: Bitmap,
        init_stream: F,
    ) -> Result<Self, RpcError>
    where
        F: FnOnce(Receiver<CoordinateRequest>) -> Fut + Send,
        St: Stream<Item = Result<CoordinateResponse, Status>> + Send + Unpin + 'static,
        Fut: Future<Output = Result<Response<St>, Status>> + Send,
    {
        let (stream_handle, first_response) = BidiStreamHandle::initialize(
            CoordinateRequest {
                msg: Some(coordinate_request::Msg::StartRequest(
                    StartCoordinationRequest {
                        vnode_bitmap: Some(vnode_bitmap.to_protobuf()),
                        param: Some(param),
                    },
                )),
            },
            move |rx| async move {
                init_stream(rx)
                    .await
                    .map(|response| response.into_inner().map_err(RpcError::from))
                    .map_err(RpcError::from)
            },
        )
        .await?;
        match first_response {
            CoordinateResponse {
                msg: Some(coordinate_response::Msg::StartResponse(_)),
            } => Ok(stream_handle),
            msg => Err(anyhow!("should get start response but get {:?}", msg).into()),
        }
    }

    pub async fn commit(&mut self, epoch: u64, metadata: SinkMetadata) -> anyhow::Result<()> {
        self.send_request(CoordinateRequest {
            msg: Some(coordinate_request::Msg::CommitRequest(CommitRequest {
                epoch,
                metadata: Some(metadata),
            })),
        })
        .await?;
        match self.next_response().await? {
            CoordinateResponse {
                msg: Some(coordinate_response::Msg::CommitResponse(_)),
            } => Ok(()),
            msg => Err(anyhow!("should get commit response but get {:?}", msg)),
        }
    }
}
