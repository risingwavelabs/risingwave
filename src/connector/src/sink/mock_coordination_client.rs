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

use risingwave_common::bitmap::Bitmap;
use risingwave_pb::connector_service::coordinate_response::{
    self, CommitResponse, StartCoordinationResponse,
};
use risingwave_pb::connector_service::{
    CoordinateRequest, CoordinateResponse, PbSinkParam, coordinate_request,
};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::{CoordinatorStreamHandle, SinkCoordinationRpcClient};
use tokio::sync::mpsc::{self, Receiver};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::boxed::BoxCoordinator;
use super::{BOUNDED_CHANNEL_SIZE, SinkParam};

#[derive(Clone)]
pub enum SinkCoordinationRpcClientEnum {
    SinkCoordinationRpcClient(SinkCoordinationRpcClient),
    MockSinkCoordinationRpcClient(MockSinkCoordinationRpcClient),
}

impl SinkCoordinationRpcClientEnum {
    pub async fn new_stream_handle(
        self,
        param: SinkParam,
        vnode_bitmap: Bitmap,
    ) -> super::Result<(CoordinatorStreamHandle, Option<u64>)> {
        match self {
            SinkCoordinationRpcClientEnum::SinkCoordinationRpcClient(
                sink_coordination_rpc_client,
            ) => {
                let (handle, log_store_rewind_start_epoch) = CoordinatorStreamHandle::new(
                    sink_coordination_rpc_client,
                    param.to_proto(),
                    vnode_bitmap,
                )
                .await?;
                Ok((handle, log_store_rewind_start_epoch))
            }
            SinkCoordinationRpcClientEnum::MockSinkCoordinationRpcClient(
                mock_sink_coordination_rpc_client,
            ) => {
                let handle = mock_sink_coordination_rpc_client
                    .new_stream_handle(param.to_proto(), vnode_bitmap)
                    .await?;
                Ok((handle, None))
            }
        }
    }
}

#[derive(Clone)]
pub struct MockMetaClient {
    mock_coordinator_committer: std::sync::Arc<tokio::sync::Mutex<BoxCoordinator>>,
}
impl MockMetaClient {
    pub fn new(mock_coordinator_committer: BoxCoordinator) -> Self {
        Self {
            mock_coordinator_committer: std::sync::Arc::new(tokio::sync::Mutex::new(
                mock_coordinator_committer,
            )),
        }
    }

    pub fn sink_coordinate_client(&self) -> MockSinkCoordinationRpcClient {
        MockSinkCoordinationRpcClient::new(self.mock_coordinator_committer.clone())
    }
}

#[derive(Clone)]
pub struct MockSinkCoordinationRpcClient {
    mock_coordinator_committer: std::sync::Arc<tokio::sync::Mutex<BoxCoordinator>>,
}

impl MockSinkCoordinationRpcClient {
    pub fn new(
        mock_coordinator_committer: std::sync::Arc<tokio::sync::Mutex<BoxCoordinator>>,
    ) -> Self {
        Self {
            mock_coordinator_committer,
        }
    }

    pub async fn new_stream_handle(
        &self,
        param: PbSinkParam,
        vnode_bitmap: Bitmap,
    ) -> std::result::Result<CoordinatorStreamHandle, RpcError> {
        let (res, _) =
            CoordinatorStreamHandle::new_with_init_stream(param, vnode_bitmap, |rx| async move {
                self.coordinate(rx).await
            })
            .await?;
        Ok(res)
    }

    pub async fn coordinate(
        &self,
        mut receiver_stream: Receiver<CoordinateRequest>,
    ) -> std::result::Result<
        tonic::Response<ReceiverStream<std::result::Result<CoordinateResponse, tonic::Status>>>,
        Status,
    > {
        match receiver_stream.try_recv() {
            Ok(CoordinateRequest {
                msg:
                    Some(risingwave_pb::connector_service::coordinate_request::Msg::StartRequest(
                        coordinate_request::StartCoordinationRequest {
                            param: Some(_param),
                            vnode_bitmap: Some(_vnode_bitmap),
                        },
                    )),
            }) => (),
            msg => {
                return Err(Status::invalid_argument(format!(
                    "expected CoordinateRequest::StartRequest in the first request, get {:?}",
                    msg
                )));
            }
        };

        let (response_tx, response_rx) =
            mpsc::channel::<std::result::Result<CoordinateResponse, Status>>(BOUNDED_CHANNEL_SIZE);
        let response_tx = std::sync::Arc::new(response_tx);
        response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::StartResponse(
                    StartCoordinationResponse {
                        log_store_rewind_start_epoch: None,
                    },
                )),
            }))
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;

        let mock_coordinator_committer = self.mock_coordinator_committer.clone();
        let response_tx_clone = response_tx.clone();
        tokio::spawn(async move {
            loop {
                match receiver_stream.recv().await {
                    Some(CoordinateRequest {
                        msg:
                            Some(risingwave_pb::connector_service::coordinate_request::Msg::CommitRequest(coordinate_request::CommitRequest {
                                epoch,
                                metadata,
                            })),
                    }) => {
                        mock_coordinator_committer.clone().lock().await.commit(epoch, vec![metadata.unwrap()]).await.map_err(|e| Status::from_error(Box::new(e)))?;
                        response_tx_clone.clone().send(Ok(CoordinateResponse {
                            msg: Some(coordinate_response::Msg::CommitResponse(CommitResponse{epoch})),
                        })).await.map_err(|e| Status::from_error(Box::new(e)))?;
                    },
                    msg => {
                        return Err::<ReceiverStream<CoordinateResponse>, tonic::Status>(Status::invalid_argument(format!(
                            "expected CoordinateRequest::CommitRequest , get {:?}",
                            msg
                        )));
                    }
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(response_rx)))
    }
}
