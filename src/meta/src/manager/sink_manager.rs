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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::future::{pending, Future};
use std::iter::once;
use std::pin::pin;

use bytes::Bytes;
use futures::future::{join_all, select, BoxFuture, Either};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::{
    build_sink, BuildSinkParam, Sink, SinkCommitCoordinator, SinkConfig, SinkError, SinkImpl,
};
use risingwave_pb::connector_service::sink_coordinator_to_writer_msg::CommitResponse;
use risingwave_pb::connector_service::sink_writer_to_coordinator_msg::{CommitRequest, Msg};
use risingwave_pb::connector_service::{
    sink_coordinator_to_writer_msg, sink_writer_to_coordinator_msg, SinkCoordinatorToWriterMsg,
    SinkWriterToCoordinatorMsg,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Status;
use tracing::log::warn;
use tracing::{error, info};

macro_rules! send_with_err_check {
    ($tx:expr, $msg:expr, $loc:expr) => {
        if $tx.send($msg).is_err() {
            error!("unable to send msg when {:?}", $loc);
        }
    };
}

type SinkWriterInputStream = BoxStream<'static, Result<SinkWriterToCoordinatorMsg, Status>>;
type SinkCoordinatorResponseSender = UnboundedSender<Result<SinkCoordinatorToWriterMsg, Status>>;

struct NewSinkWriterRequest {
    request_stream: SinkWriterInputStream,
    response_tx: SinkCoordinatorResponseSender,
    param: BuildSinkParam,
    vnode_bitmap: Bitmap,
}

enum SinkManagerRequest {
    NewSinkWriter(NewSinkWriterRequest),
    Reset(Sender<()>),
}

#[derive(Clone)]
pub struct SinkManager {
    request_tx: UnboundedSender<SinkManagerRequest>,
}

impl SinkManager {
    pub(crate) fn start_worker() -> (Self, (JoinHandle<()>, Sender<()>)) {
        let (request_tx, request_rx) = unbounded_channel();
        let (shutdown_tx, shutdown_rx) = channel();
        let worker = SinkManagerWorker {
            request_rx,
            shutdown_rx: Some(shutdown_rx),
            running_coordinator_worker_join_handles: Default::default(),
            running_coordinator_worker: Default::default(),
        };
        let join_handle = tokio::spawn(worker.execute());
        (SinkManager { request_tx }, (join_handle, shutdown_tx))
    }

    pub(crate) async fn handle_new_request(
        &self,
        mut request_stream: SinkWriterInputStream,
    ) -> Result<impl Stream<Item = Result<SinkCoordinatorToWriterMsg, Status>>, Status> {
        let (param, vnode_bitmap) = match request_stream.try_next().await? {
            Some(SinkWriterToCoordinatorMsg {
                msg:
                    Some(sink_writer_to_coordinator_msg::Msg::StartRequest(
                        sink_writer_to_coordinator_msg::StartCoordinationRequest {
                            param: Some(param),
                            vnode_bitmap: Some(vnode_bitmap),
                        },
                    )),
            }) => (
                BuildSinkParam::from_proto(param),
                Bitmap::from(&vnode_bitmap),
            ),
            msg => {
                return Err(Status::invalid_argument(format!(
                    "expected SinkWriterToCoordinatorMsg::StartRequest in the first request, get {:?}",
                    msg
                )));
            }
        };
        let (response_tx, response_rx) = unbounded_channel();
        self.request_tx
            .send(SinkManagerRequest::NewSinkWriter(NewSinkWriterRequest {
                request_stream,
                response_tx,
                param,
                vnode_bitmap,
            }))
            .map_err(|_| {
                Status::unavailable(
                    "unable to send to sink manager worker. The worker may have stopped",
                )
            })?;

        Ok(UnboundedReceiverStream::new(response_rx))
    }

    pub(crate) async fn reset(&self) {
        let (tx, rx) = channel();
        let request_tx = &self.request_tx;
        send_with_err_check!(
            request_tx,
            SinkManagerRequest::Reset(tx),
            "sending reset to sink manager worker"
        );
        if rx.await.is_err() {
            error!("fail to wait for resetting sink manager worker");
        }
    }
}

struct SinkManagerWorker {
    request_rx: UnboundedReceiver<SinkManagerRequest>,
    // Make it option so that it can be polled with &mut SinkManagerWorker
    shutdown_rx: Option<Receiver<()>>,

    running_coordinator_worker_join_handles:
        FuturesUnordered<BoxFuture<'static, (SinkId, Result<(), JoinError>)>>,
    running_coordinator_worker: HashMap<SinkId, UnboundedSender<SinkCoordinatorWorkerRequest>>,
}

enum SinkManagerWorkerEvent {
    NewRequest(SinkManagerRequest),
    CoordinatorWorkerFinished {
        sink_id: SinkId,
        join_result: Result<(), JoinError>,
    },
}

fn pending_on_none<I>(future: impl Future<Output = Option<I>>) -> impl Future<Output = I> {
    future
        .map(|opt| opt.ok_or(()))
        .or_else(|()| pending::<Result<I, ()>>())
        .map(|result| result.expect("only err on pending, which is unlikely to reach here"))
}

impl SinkManagerWorker {
    async fn execute(mut self) {
        while let Some(event) = self.next_event().await {
            match event {
                SinkManagerWorkerEvent::NewRequest(request) => match request {
                    SinkManagerRequest::NewSinkWriter(request) => {
                        self.handle_new_sink_writer(request)
                    }
                    SinkManagerRequest::Reset(tx) => {
                        self.clean_up().await;
                        send_with_err_check!(tx, (), "send reset response");
                    }
                },
                SinkManagerWorkerEvent::CoordinatorWorkerFinished {
                    sink_id,
                    join_result,
                } => self.handle_coordinator_finished(sink_id, join_result),
            }
        }
        self.clean_up().await;
        info!("sink manager worker exited");
    }

    async fn next_event(&mut self) -> Option<SinkManagerWorkerEvent> {
        let shutdown_rx = self.shutdown_rx.take().expect("should not be empty");
        match select(
            select(
                pin!(self.request_rx.recv()),
                pin!(pending_on_none(
                    self.running_coordinator_worker_join_handles.next()
                )),
            ),
            shutdown_rx,
        )
        .await
        {
            Either::Left((either, shutdown_rx)) => {
                self.shutdown_rx = Some(shutdown_rx);
                match either {
                    Either::Left((Some(request), _)) => {
                        Some(SinkManagerWorkerEvent::NewRequest(request))
                    }
                    Either::Left((None, _)) => None,
                    Either::Right(((sink_id, join_result), _)) => {
                        Some(SinkManagerWorkerEvent::CoordinatorWorkerFinished {
                            sink_id,
                            join_result,
                        })
                    }
                }
            }
            Either::Right(_) => None,
        }
    }

    async fn clean_up(&mut self) {
        info!("sink manager worker start cleaning up");
        for sender in self.running_coordinator_worker.values() {
            send_with_err_check!(sender, SinkCoordinatorWorkerRequest::Stop, "send clean up");
        }
        while let Some((sink_id, join_result)) =
            self.running_coordinator_worker_join_handles.next().await
        {
            self.handle_coordinator_finished(sink_id, join_result);
        }
        info!("sink manager worker finished cleaning up");
    }

    fn handle_coordinator_finished(&mut self, sink_id: SinkId, join_result: Result<(), JoinError>) {
        self.running_coordinator_worker
            .remove(&sink_id)
            .expect("finished coordinator should have an associated sender");
        match join_result {
            Ok(()) => {
                info!(
                    "sink coordinator of {} has gracefully finished",
                    sink_id.sink_id
                );
            }
            Err(err) => {
                error!(
                    "sink coordinator of {} finished with error {:?}",
                    sink_id.sink_id, err
                );
            }
        }
    }

    fn handle_new_sink_writer(&mut self, request: NewSinkWriterRequest) {
        let param = &request.param;
        let sink_id = param.sink_id;

        // Launch the coordinator worker task if it is the first
        match self.running_coordinator_worker.entry(param.sink_id) {
            Entry::Occupied(entry) => {
                let sender = entry.into_mut();
                send_with_err_check!(
                    sender,
                    SinkCoordinatorWorkerRequest::NewWriter(request),
                    format!(
                        "failed to send new writer request to coordinator worker of {}",
                        sink_id.sink_id
                    )
                );
            }
            Entry::Vacant(entry) => {
                let (request_tx, request_rx) = unbounded_channel();
                let join_handle = tokio::spawn(async move {
                    if let Some(coordinator_worker) =
                        SinkCoordinatorWorker::initialize(request, request_rx).await
                    {
                        coordinator_worker.execute().await;
                    }
                });
                self.running_coordinator_worker_join_handles.push(
                    join_handle
                        .map(move |join_result| (sink_id, join_result))
                        .boxed(),
                );
                entry.insert(request_tx);
            }
        };
    }
}

enum SinkCoordinatorWorkerRequest {
    NewWriter(NewSinkWriterRequest),
    Stop,
}

struct SinkCoordinatorWorker {
    sink: Option<SinkImpl>,
    request_streams: Vec<SinkWriterInputStream>,
    response_response_senders: Vec<SinkCoordinatorResponseSender>,
    _request_rx: UnboundedReceiver<SinkCoordinatorWorkerRequest>,
}

impl SinkCoordinatorWorker {
    async fn initialize(
        first_writer_request: NewSinkWriterRequest,
        mut request_rx: UnboundedReceiver<SinkCoordinatorWorkerRequest>,
    ) -> Option<SinkCoordinatorWorker> {
        let param = first_writer_request.param;
        let sink = {
            let build_sink_result: Result<SinkImpl, SinkError> = try {
                let config = SinkConfig::from_hashmap(param.properties.clone())?;
                build_sink(
                    config,
                    &param.columns,
                    param.pk_indices.clone(),
                    param.sink_type,
                    param.sink_id,
                )?
            };
            match build_sink_result {
                Ok(sink) => sink,
                Err(e) => {
                    error!("failed to build sink with param {:?}: {:?}", param, e);
                    send_with_err_check!(
                        first_writer_request.response_tx,
                        Err(Status::invalid_argument("failed to build sink")),
                        "err in build sink"
                    );
                    return None;
                }
            }
        };

        let mut remaining_count = VirtualNode::COUNT;
        let mut registered_vnode = HashSet::with_capacity(VirtualNode::COUNT);
        let mut pending_request_streams = vec![first_writer_request.request_stream];
        let mut pending_response_senders = vec![first_writer_request.response_tx];

        for vnode in first_writer_request.vnode_bitmap.iter_vnodes() {
            remaining_count -= 1;
            registered_vnode.insert(vnode);
        }

        loop {
            // TODO: add timeout log
            if let Some(request) = request_rx.recv().await {
                match request {
                    SinkCoordinatorWorkerRequest::NewWriter(request) => {
                        for vnode in request.vnode_bitmap.iter_vnodes() {
                            if registered_vnode.contains(&vnode) {
                                error!(
                                    "get overlapped vnode: {}, current vnode {:?}",
                                    vnode, registered_vnode
                                );
                                for sender in pending_response_senders
                                    .into_iter()
                                    .chain(once(request.response_tx))
                                {
                                    send_with_err_check!(
                                        sender,
                                        Err(Status::cancelled("overlapped vnode")),
                                        "send overlapped vnode error"
                                    );
                                }
                                return None;
                            }
                            registered_vnode.insert(vnode);
                            remaining_count -= 1;
                        }
                        pending_request_streams.push(request.request_stream);
                        pending_response_senders.push(request.response_tx);

                        if remaining_count == 0 {
                            break;
                        }
                    }
                    SinkCoordinatorWorkerRequest::Stop => {
                        for sender in pending_response_senders {
                            send_with_err_check!(
                                sender,
                                Err(Status::cancelled("reset while initialization")),
                                "send reset while initialization"
                            );
                        }
                        return None;
                    }
                }
            } else {
                warn!("coordinator worker finished during initialization");
                return None;
            }
        }
        Some(Self {
            sink: Some(sink),
            request_streams: pending_request_streams,
            response_response_senders: pending_response_senders,
            _request_rx: request_rx,
        })
    }

    async fn execute(mut self) {
        let sink = self.sink.take().expect("should be Some when first execute");
        dispatch_sink!(sink, sink, {
            // TODO: pass the connector param
            let mut coordinator = match sink.new_coordinator(None).await {
                Ok(coordinator) => coordinator,
                Err(e) => {
                    error!("unable to create coordinator: {:?}", e);
                    for sender in self.response_response_senders {
                        send_with_err_check!(
                            sender,
                            Err(Status::unavailable("unable to create coordinator")),
                            "send coordinator create failure"
                        );
                    }
                    return;
                }
            };
            // TODO: return on error
            coordinator.init().await.unwrap();
            self.execute_inner(coordinator).await;
        })
    }

    async fn execute_inner(mut self, mut coordinator: impl SinkCommitCoordinator) {
        loop {
            // TODO: check epoch
            // TODO: poll request_tx for stop request
            // TODO: ensure type
            let commit_infos: Vec<(Vec<u8>, u64)> =
                join_all(self.request_streams.iter_mut().map(|stream| {
                    stream
                        .next()
                        .map(|event| match event.unwrap().unwrap().msg.unwrap() {
                            Msg::CommitRequest(CommitRequest { metadata, epoch }) => {
                                (metadata, epoch)
                            }
                            _ => unreachable!("should be commit"),
                        })
                }))
                .await;
            let epoch = commit_infos[0].1;
            let mut metadatas = Vec::with_capacity(commit_infos.len());
            for (metadata, other_epoch) in commit_infos {
                metadatas.push(Bytes::from(metadata));
                assert_eq!(epoch, other_epoch);
            }
            // TODO: return on error
            coordinator.commit(epoch, metadatas).await.unwrap();
            for sender in &self.response_response_senders {
                // TODO: return on error
                sender
                    .send(Ok(SinkCoordinatorToWriterMsg {
                        msg: Some(sink_coordinator_to_writer_msg::Msg::CommitResponse(
                            CommitResponse { epoch },
                        )),
                    }))
                    .unwrap();
            }
        }
    }
}
