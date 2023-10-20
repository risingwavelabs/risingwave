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

use std::collections::HashSet;
use std::pin::pin;

use anyhow::anyhow;
use futures::future::{select, Either};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::{build_sink, Sink, SinkCommitCoordinator, SinkParam};
use risingwave_pb::connector_service::coordinate_request::CommitRequest;
use risingwave_pb::connector_service::coordinate_response::{
    CommitResponse, StartCoordinationResponse,
};
use risingwave_pb::connector_service::{
    coordinate_request, coordinate_response, CoordinateRequest, CoordinateResponse, SinkMetadata,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::Status;
use tracing::{error, warn};

use crate::manager::sink_coordination::{
    NewSinkWriterRequest, SinkCoordinatorResponseSender, SinkWriterRequestStream,
};

macro_rules! send_await_with_err_check {
    ($tx:expr, $msg:expr) => {
        if $tx.send($msg).await.is_err() {
            error!("unable to send msg");
        }
    };
}

pub struct CoordinatorWorker {
    param: SinkParam,
    request_streams: Vec<SinkWriterRequestStream>,
    response_senders: Vec<SinkCoordinatorResponseSender>,
    request_rx: UnboundedReceiver<NewSinkWriterRequest>,
}

impl CoordinatorWorker {
    pub async fn run(
        first_writer_request: NewSinkWriterRequest,
        request_rx: UnboundedReceiver<NewSinkWriterRequest>,
    ) {
        let sink = match build_sink(first_writer_request.param.clone()) {
            Ok(sink) => sink,
            Err(e) => {
                error!(
                    "unable to build sink with param {:?}: {:?}",
                    first_writer_request.param, e
                );
                send_await_with_err_check!(
                    first_writer_request.response_tx,
                    Err(Status::invalid_argument("failed to build sink"))
                );
                return;
            }
        };
        dispatch_sink!(sink, sink, {
            let coordinator = match sink.new_coordinator().await {
                Ok(coordinator) => coordinator,
                Err(e) => {
                    error!(
                        "unable to build coordinator with param {:?}: {:?}",
                        first_writer_request.param, e
                    );
                    send_await_with_err_check!(
                        first_writer_request.response_tx,
                        Err(Status::invalid_argument("failed to build coordinator"))
                    );
                    return;
                }
            };
            Self::execute_coordinator(first_writer_request, request_rx, coordinator).await
        });
    }

    pub async fn execute_coordinator(
        first_writer_request: NewSinkWriterRequest,
        request_rx: UnboundedReceiver<NewSinkWriterRequest>,
        coordinator: impl SinkCommitCoordinator,
    ) {
        let mut worker = CoordinatorWorker {
            param: first_writer_request.param,
            request_streams: vec![first_writer_request.request_stream],
            response_senders: vec![first_writer_request.response_tx],
            request_rx,
        };

        if let Err(e) = worker
            .wait_for_writers(first_writer_request.vnode_bitmap)
            .await
        {
            error!("failed to wait for all writers: {:?}", e);
            worker
                .send_to_all_sink_writers(|| {
                    Err(Status::cancelled("failed to wait for all writers"))
                })
                .await;
        }

        worker.start_coordination(coordinator).await;
    }

    async fn send_to_all_sink_writers(
        &mut self,
        new_msg: impl Fn() -> Result<CoordinateResponse, Status>,
    ) {
        for sender in &self.response_senders {
            send_await_with_err_check!(sender, new_msg());
        }
    }

    async fn next_new_writer(&mut self) -> anyhow::Result<NewSinkWriterRequest> {
        // TODO: add timeout log
        match select(
            pin!(self.request_rx.recv()),
            pin!(FuturesUnordered::from_iter(
                self.request_streams
                    .iter_mut()
                    .map(|stream| stream.try_next()),
            )
            .next()),
        )
        .await
        {
            Either::Left((Some(req), _)) => Ok(req),
            Either::Left((None, _)) => Err(anyhow!("manager request stream reaches the end")),
            Either::Right((Some(Ok(Some(request))), _)) => Err(anyhow!(
                "get new request from sink writer before initialize: {:?}",
                request
            )),
            Either::Right((Some(Ok(None)), _)) => Err(anyhow!(
                "one sink writer stream reaches the end before initialize"
            )),
            Either::Right((Some(Err(e)), _)) => Err(anyhow!(
                "unable to poll from one sink writer stream: {:?}",
                e
            )),
            Either::Right((None, _)) => unreachable!("request_streams must not be empty"),
        }
    }

    async fn wait_for_writers(&mut self, first_vnode_bitmap: Bitmap) -> anyhow::Result<()> {
        let mut remaining_count = VirtualNode::COUNT;
        let mut registered_vnode = HashSet::with_capacity(VirtualNode::COUNT);

        for vnode in first_vnode_bitmap.iter_vnodes() {
            remaining_count -= 1;
            registered_vnode.insert(vnode);
        }

        while remaining_count > 0 {
            let new_writer_request = self.next_new_writer().await?;
            if self.param != new_writer_request.param {
                // TODO: may return error.
                warn!(
                    "get different param {:?} while current param {:?}",
                    new_writer_request.param, self.param
                );
            }
            self.request_streams.push(new_writer_request.request_stream);
            self.response_senders.push(new_writer_request.response_tx);

            for vnode in new_writer_request.vnode_bitmap.iter_vnodes() {
                if registered_vnode.contains(&vnode) {
                    return Err(anyhow!(
                        "get overlapped vnode: {}, current vnode {:?}",
                        vnode,
                        registered_vnode
                    ));
                }
                registered_vnode.insert(vnode);
                remaining_count -= 1;
            }
        }

        self.send_to_all_sink_writers(|| {
            Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::StartResponse(
                    StartCoordinationResponse {},
                )),
            })
        })
        .await;
        Ok(())
    }

    async fn collect_all_metadata(&mut self) -> anyhow::Result<(u64, Vec<SinkMetadata>)> {
        let mut epoch = None;
        let mut metadata_list = Vec::with_capacity(self.request_streams.len());
        let mut uncollected_futures = FuturesUnordered::from_iter(
            self.request_streams
                .iter_mut()
                .map(|stream| stream.try_next()),
        );

        loop {
            match select(
                pin!(self.request_rx.recv()),
                pin!(uncollected_futures.next()),
            )
            .await
            {
                Either::Left((Some(new_request), _)) => {
                    warn!("get new writer request while collecting metadata");
                    send_await_with_err_check!(
                        new_request.response_tx,
                        Err(Status::already_exists(
                            "coordinator already running, should not get new request"
                        ))
                    );
                    continue;
                }
                Either::Left((None, _)) => {
                    return Err(anyhow!(
                        "coordinator get notified to stop while collecting metadata"
                    ));
                }
                Either::Right((Some(next_result), _)) => match next_result {
                    Ok(Some(CoordinateRequest {
                        msg:
                            Some(coordinate_request::Msg::CommitRequest(CommitRequest {
                                epoch: request_epoch,
                                metadata: Some(metadata),
                            })),
                    })) => {
                        match &epoch {
                            Some(epoch) => {
                                if *epoch != request_epoch {
                                    warn!(
                                        "current epoch is {} but get request from {}",
                                        epoch, request_epoch
                                    );
                                }
                            }
                            None => {
                                epoch = Some(request_epoch);
                            }
                        }
                        metadata_list.push(metadata);
                    }
                    Ok(Some(req)) => {
                        return Err(anyhow!("expect commit request but get {:?}", req));
                    }
                    Ok(None) => {
                        return Err(anyhow!(
                            "sink writer input reaches the end while collecting metadata"
                        ));
                    }
                    Err(e) => {
                        return Err(anyhow!(
                            "failed to poll from one of the writer request streams: {:?}",
                            e
                        ));
                    }
                },
                Either::Right((None, _)) => {
                    break;
                }
            }
        }
        Ok((
            epoch.expect("should not be empty when have at least one writer"),
            metadata_list,
        ))
    }

    async fn start_coordination(&mut self, mut coordinator: impl SinkCommitCoordinator) {
        let result: Result<(), ()> = try {
            coordinator.init().await.map_err(|e| {
                error!("failed to initialize coordinator: {:?}", e);
            })?;
            loop {
                let (epoch, metadata_list) = self.collect_all_metadata().await.map_err(|e| {
                    error!("failed to collect all metadata: {:?}", e);
                })?;
                // TODO: measure commit time
                coordinator
                    .commit(epoch, metadata_list)
                    .await
                    .map_err(|e| error!("failed to commit metadata of epoch {}: {:?}", epoch, e))?;

                self.send_to_all_sink_writers(|| {
                    Ok(CoordinateResponse {
                        msg: Some(coordinate_response::Msg::CommitResponse(CommitResponse {
                            epoch,
                        })),
                    })
                })
                .await;
            }
        };

        if result.is_err() {
            self.send_to_all_sink_writers(|| Err(Status::aborted("failed to run coordination")))
                .await;
        }
    }
}
