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

use std::net::SocketAddr;
use std::sync::Arc;

use futures::channel::mpsc::Receiver;
use futures::StreamExt;
use risingwave_batch::task::BatchManager;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::task_service::exchange_service_server::ExchangeService;
use risingwave_pb::task_service::{
    GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
};
use risingwave_stream::executor::Message;
use risingwave_stream::task::LocalStreamManager;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Buffer size of the receiver of the remote channel.
const EXCHANGE_BUFFER_SIZE: usize = 1024;

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    batch_mgr: Arc<BatchManager>,
    stream_mgr: Arc<LocalStreamManager>,
}

type ExchangeDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;

#[async_trait::async_trait]
impl ExchangeService for ExchangeServiceImpl {
    type GetDataStream = ExchangeDataStream;
    type GetStreamStream = ReceiverStream<std::result::Result<GetStreamResponse, Status>>;

    #[cfg_attr(coverage, no_coverage)]
    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let pb_task_output_id = request
            .into_inner()
            .task_output_id
            .expect("Failed to get task output id.");
        let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_BUFFER_SIZE);
        if let Err(e) = self.batch_mgr.get_data(tx, peer_addr, &pb_task_output_id) {
            error!("Failed to serve exchange RPC from {}: {}", peer_addr, e);
            return Err(e.to_grpc_status());
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream(
        &self,
        request: Request<GetStreamRequest>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;
        let req = request.into_inner();
        let up_down_ids = (req.up_fragment_id, req.down_fragment_id);
        let receiver = self
            .stream_mgr
            .take_receiver(up_down_ids)
            .map_err(|e| e.to_grpc_status())?;
        match self.get_stream_impl(peer_addr, receiver).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!(
                    "Failed to server stream exchange RPC from {}: {}",
                    peer_addr, e
                );
                Err(e.to_grpc_status())
            }
        }
    }
}

impl ExchangeServiceImpl {
    pub fn new(mgr: Arc<BatchManager>, stream_mgr: Arc<LocalStreamManager>) -> Self {
        ExchangeServiceImpl {
            batch_mgr: mgr,
            stream_mgr,
        }
    }

    async fn get_stream_impl(
        &self,
        peer_addr: SocketAddr,
        mut receiver: Receiver<Message>,
    ) -> Result<Response<<Self as ExchangeService>::GetStreamStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(EXCHANGE_BUFFER_SIZE);
        tracing::trace!(target: "events::compute::exchange", peer_addr = %peer_addr, "serve stream exchange RPC");
        tokio::spawn(async move {
            loop {
                let msg = receiver.next().await;
                match msg {
                    // the sender is closed, we close the receiver and stop forwarding message
                    None => break,
                    Some(msg) => {
                        let res = match msg.to_protobuf() {
                            Ok(stream_msg) => Ok(GetStreamResponse {
                                message: Some(stream_msg),
                            }),
                            Err(e) => Err(e.to_grpc_status()),
                        };
                        let _ = match tx.send(res).await.map_err(|e| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "failed to send stream data: {}",
                                e
                            )))
                        }) {
                            Ok(_) => Ok(()),
                            Err(e) => tx.send(Err(e.to_grpc_status())).await,
                        };
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
