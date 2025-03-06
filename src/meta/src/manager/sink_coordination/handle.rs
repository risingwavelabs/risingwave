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

use std::pin::pin;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::{Future, TryStreamExt};
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::SinkParam;
use risingwave_pb::connector_service::coordinate_response::{
    CommitResponse, StartCoordinationResponse,
};
use risingwave_pb::connector_service::{
    CoordinateResponse, SinkMetadata, coordinate_request, coordinate_response,
};
use tonic::Status;

use crate::manager::sink_coordination::{SinkCoordinatorResponseSender, SinkWriterRequestStream};

pub(super) struct SinkWriterCoordinationHandle {
    request_stream: SinkWriterRequestStream,
    response_tx: SinkCoordinatorResponseSender,
    param: SinkParam,
    vnode_bitmap: Bitmap,
    prev_epoch: Option<u64>,
}

impl SinkWriterCoordinationHandle {
    pub(super) fn new(
        request_stream: SinkWriterRequestStream,
        response_tx: SinkCoordinatorResponseSender,
        param: SinkParam,
        vnode_bitmap: Bitmap,
    ) -> Self {
        Self {
            request_stream,
            response_tx,
            param,
            vnode_bitmap,
            prev_epoch: None,
        }
    }

    pub(super) fn param(&self) -> &SinkParam {
        &self.param
    }

    pub(super) fn vnode_bitmap(&self) -> &Bitmap {
        &self.vnode_bitmap
    }

    pub(super) fn start(
        &mut self,
        log_store_rewind_start_epoch: Option<u64>,
    ) -> anyhow::Result<()> {
        self.response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::StartResponse(
                    StartCoordinationResponse {
                        log_store_rewind_start_epoch,
                    },
                )),
            }))
            .map_err(|_| anyhow!("fail to send start response"))
    }

    pub(super) fn abort(self, status: Status) {
        let _ = self.response_tx.send(Err(status));
    }

    pub(super) fn ack_commit(&mut self, epoch: u64) -> anyhow::Result<()> {
        self.response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::CommitResponse(CommitResponse {
                    epoch,
                })),
            }))
            .map_err(|_| anyhow!("fail to send commit response of epoch {}", epoch))
    }

    pub(super) fn poll_next_commit_request(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<anyhow::Result<Option<(u64, SinkMetadata)>>> {
        let future = self.next_commit_request();
        let future = pin!(future);
        future.poll(cx)
    }

    async fn next_commit_request(&mut self) -> anyhow::Result<Option<(u64, SinkMetadata)>> {
        loop {
            let request = self
                .request_stream
                .try_next()
                .await?
                .ok_or_else(|| anyhow!("end of request stream"))?;
            match request.msg.ok_or_else(|| anyhow!("None msg in request"))? {
                coordinate_request::Msg::StartRequest(_) => {
                    return Err(anyhow!("should have started"));
                }
                coordinate_request::Msg::CommitRequest(request) => {
                    if let Some(prev_epoch) = self.prev_epoch {
                        if request.epoch < prev_epoch {
                            return Err(anyhow!(
                                "invalid commit epoch {}, prev_epoch {}",
                                request.epoch,
                                prev_epoch
                            ));
                        }
                    }
                    let Some(metadata) = request.metadata else {
                        return Err(anyhow!("empty commit metadata"));
                    };
                    self.prev_epoch = Some(request.epoch);
                    return Ok(Some((request.epoch, metadata)));
                }
                coordinate_request::Msg::UpdateVnodeRequest(request) => {
                    let bitmap = Bitmap::from(
                        &request
                            .vnode_bitmap
                            .ok_or_else(|| anyhow!("empty vnode bitmap"))?,
                    );
                    self.vnode_bitmap = bitmap;
                    continue;
                }
                coordinate_request::Msg::Stop(_) => {
                    return Ok(None);
                }
            }
        }
    }
}
