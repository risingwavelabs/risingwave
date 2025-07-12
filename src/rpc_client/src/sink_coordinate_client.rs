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

use std::future::Future;

use anyhow::anyhow;
use futures::{Stream, TryStreamExt};
use risingwave_common::bitmap::Bitmap;
use risingwave_pb::connector_service::coordinate_request::{
    CommitRequest, StartCoordinationRequest, UpdateVnodeBitmapRequest,
};
use risingwave_pb::connector_service::coordinate_response::StartCoordinationResponse;
use risingwave_pb::connector_service::{
    CoordinateRequest, CoordinateResponse, PbSinkParam, SinkMetadata, coordinate_request,
    coordinate_response,
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
    ) -> Result<(Self, Option<u64>), RpcError> {
        let (instance, log_store_rewind_start_epoch) =
            Self::new_with_init_stream(param, vnode_bitmap, |rx| async move {
                client.coordinate(ReceiverStream::new(rx)).await
            })
            .await?;

        Ok((instance, log_store_rewind_start_epoch))
    }

    pub async fn new_with_init_stream<F, St, Fut>(
        param: PbSinkParam,
        vnode_bitmap: Bitmap,
        init_stream: F,
    ) -> Result<(Self, Option<u64>), RpcError>
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
                    .map(|response| {
                        response
                            .into_inner()
                            .map_err(RpcError::from_connector_status)
                    })
                    .map_err(RpcError::from_connector_status)
            },
        )
        .await?;

        match first_response {
            CoordinateResponse {
                msg:
                    Some(coordinate_response::Msg::StartResponse(StartCoordinationResponse {
                        log_store_rewind_start_epoch,
                    })),
            } => Ok((stream_handle, log_store_rewind_start_epoch)),
            msg => Err(anyhow!("should get start response but get {:?}", msg).into()),
        }
    }

    pub async fn commit(
        &mut self,
        epoch: u64,
        metadata: Option<SinkMetadata>,
    ) -> anyhow::Result<()> {
        self.send_request(CoordinateRequest {
            msg: Some(coordinate_request::Msg::CommitRequest(CommitRequest {
                epoch,
                metadata,
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

    pub async fn update_vnode_bitmap(&mut self, vnode_bitmap: &Bitmap) -> anyhow::Result<u64> {
        self.send_request(CoordinateRequest {
            msg: Some(coordinate_request::Msg::UpdateVnodeRequest(
                UpdateVnodeBitmapRequest {
                    vnode_bitmap: Some(vnode_bitmap.to_protobuf()),
                },
            )),
        })
        .await?;
        match self.next_response().await? {
            CoordinateResponse {
                msg:
                    Some(coordinate_response::Msg::StartResponse(StartCoordinationResponse {
                        log_store_rewind_start_epoch,
                    })),
            } => Ok(log_store_rewind_start_epoch
                .ok_or_else(|| anyhow!("should get start epoch after update vnode bitmap"))?),
            msg => Err(anyhow!("should get start response but get {:?}", msg)),
        }
    }

    pub async fn stop(mut self) -> anyhow::Result<()> {
        self.send_request(CoordinateRequest {
            msg: Some(coordinate_request::Msg::Stop(true)),
        })
        .await?;
        match self.next_response().await? {
            CoordinateResponse {
                msg: Some(coordinate_response::Msg::Stopped(_)),
            } => Ok(()),
            msg => Err(anyhow!("should get Stopped but get {:?}", msg)),
        }
    }

    pub async fn align_initial_epoch(&mut self, initial_epoch: u64) -> anyhow::Result<u64> {
        self.send_request(CoordinateRequest {
            msg: Some(coordinate_request::Msg::AlignInitialEpochRequest(
                initial_epoch,
            )),
        })
        .await?;
        match self.next_response().await? {
            CoordinateResponse {
                msg: Some(coordinate_response::Msg::AlignInitialEpochResponse(epoch)),
            } => Ok(epoch),
            msg => Err(anyhow!(
                "should get AlignInitialEpochResponse but get {:?}",
                msg
            )),
        }
    }
}
