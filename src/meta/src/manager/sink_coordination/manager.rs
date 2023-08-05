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
use std::collections::HashMap;
use std::pin::pin;

use futures::future::{select, BoxFuture, Either};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use risingwave_common::buffer::Bitmap;
use risingwave_common::util::pending_on_none;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::SinkParam;
use risingwave_pb::connector_service::sink_writer_to_coordinator_msg::Msg;
use risingwave_pb::connector_service::{
    sink_writer_to_coordinator_msg, SinkCoordinatorToWriterMsg, SinkWriterToCoordinatorMsg,
};
use risingwave_rpc_client::ConnectorClient;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel, Receiver, Sender};
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::manager::sink_coordination::coordinator_worker::CoordinatorWorker;
use crate::manager::sink_coordination::{NewSinkWriterRequest, SinkWriterRequestStream};

macro_rules! send_with_err_check {
    ($tx:expr, $msg:expr) => {
        if $tx.send($msg).is_err() {
            error!("unable to send msg");
        }
    };
}

macro_rules! send_await_with_err_check {
    ($tx:expr, $msg:expr) => {
        if $tx.send($msg).await.is_err() {
            error!("unable to send msg");
        }
    };
}

const BOUNDED_CHANNEL_SIZE: usize = 16;

enum ManagerRequest {
    NewSinkWriter(NewSinkWriterRequest),
    StopCoordinator {
        finish_notifier: Sender<()>,
        /// sink id to stop. When `None`, stop all sink coordinator
        sink_id: Option<SinkId>,
    },
}

#[derive(Clone)]
pub struct SinkCoordinatorManager {
    request_tx: mpsc::Sender<ManagerRequest>,
}

impl SinkCoordinatorManager {
    pub(crate) fn start_worker(
        connector_client: Option<ConnectorClient>,
    ) -> (Self, (JoinHandle<()>, Sender<()>)) {
        let (request_tx, request_rx) = mpsc::channel(BOUNDED_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = channel();
        let worker = ManagerWorker::new(request_rx, shutdown_rx, connector_client);
        let join_handle = tokio::spawn(worker.execute());
        (
            SinkCoordinatorManager { request_tx },
            (join_handle, shutdown_tx),
        )
    }

    pub(crate) async fn handle_new_request(
        &self,
        mut request_stream: SinkWriterRequestStream,
    ) -> Result<impl Stream<Item = Result<SinkCoordinatorToWriterMsg, Status>>, Status> {
        let (param, vnode_bitmap) = match request_stream.try_next().await? {
            Some(SinkWriterToCoordinatorMsg {
                msg:
                    Some(Msg::StartRequest(sink_writer_to_coordinator_msg::StartCoordinationRequest {
                        param: Some(param),
                        vnode_bitmap: Some(vnode_bitmap),
                    })),
            }) => (SinkParam::from_proto(param), Bitmap::from(&vnode_bitmap)),
            msg => {
                return Err(Status::invalid_argument(format!(
                    "expected SinkWriterToCoordinatorMsg::StartRequest in the first request, get {:?}",
                    msg
                )));
            }
        };
        let (response_tx, response_rx) = mpsc::channel(BOUNDED_CHANNEL_SIZE);
        self.request_tx
            .send(ManagerRequest::NewSinkWriter(NewSinkWriterRequest {
                request_stream,
                response_tx,
                param,
                vnode_bitmap,
            }))
            .await
            .map_err(|_| {
                Status::unavailable(
                    "unable to send to sink manager worker. The worker may have stopped",
                )
            })?;

        Ok(ReceiverStream::new(response_rx))
    }

    async fn stop_coordinator(&self, sink_id: Option<SinkId>) {
        let (tx, rx) = channel();
        send_await_with_err_check!(
            self.request_tx,
            ManagerRequest::StopCoordinator {
                finish_notifier: tx,
                sink_id,
            }
        );
        if rx.await.is_err() {
            error!("fail to wait for resetting sink manager worker");
        }
    }

    pub(crate) async fn reset(&self) {
        self.stop_coordinator(None).await;
    }

    pub(crate) async fn stop_sink_coordinator(&self, sink_id: SinkId) {
        self.stop_coordinator(Some(sink_id)).await;
    }
}

struct CoordinatorWorkerHandle {
    /// Sender to coordinator worker. Drop the sender as a stop signal
    request_sender: Option<UnboundedSender<NewSinkWriterRequest>>,
    /// Notify when the coordinator worker stops
    finish_notifiers: Vec<Sender<()>>,
}

struct ManagerWorker {
    connector_client: Option<ConnectorClient>,
    request_rx: mpsc::Receiver<ManagerRequest>,
    // Make it option so that it can be polled with &mut SinkManagerWorker
    shutdown_rx: Option<Receiver<()>>,

    running_coordinator_worker_join_handles:
        FuturesUnordered<BoxFuture<'static, (SinkId, Result<(), JoinError>)>>,
    running_coordinator_worker: HashMap<SinkId, CoordinatorWorkerHandle>,
}

enum ManagerEvent {
    NewRequest(ManagerRequest),
    CoordinatorWorkerFinished {
        sink_id: SinkId,
        join_result: Result<(), JoinError>,
    },
}

trait SpawnCoordinatorFn = FnMut(
    NewSinkWriterRequest,
    UnboundedReceiver<NewSinkWriterRequest>,
    Option<ConnectorClient>,
) -> JoinHandle<()>;

impl ManagerWorker {
    fn new(
        request_rx: mpsc::Receiver<ManagerRequest>,
        shutdown_rx: Receiver<()>,
        connector_client: Option<ConnectorClient>,
    ) -> Self {
        ManagerWorker {
            request_rx,
            shutdown_rx: Some(shutdown_rx),
            running_coordinator_worker_join_handles: Default::default(),
            running_coordinator_worker: Default::default(),
            connector_client,
        }
    }

    async fn execute(self) {
        self.execute_with_spawn_coordinator_worker(
            |writer_request, manager_request_stream, connector_client| {
                tokio::spawn(CoordinatorWorker::run(
                    writer_request,
                    manager_request_stream,
                    connector_client,
                ))
            },
        )
        .await
    }

    async fn execute_with_spawn_coordinator_worker(
        mut self,
        mut spawn_coordinator_worker: impl SpawnCoordinatorFn,
    ) {
        while let Some(event) = self.next_event().await {
            match event {
                ManagerEvent::NewRequest(request) => match request {
                    ManagerRequest::NewSinkWriter(request) => {
                        self.handle_new_sink_writer(request, &mut spawn_coordinator_worker)
                    }
                    ManagerRequest::StopCoordinator {
                        finish_notifier,
                        sink_id,
                    } => {
                        if let Some(sink_id) = sink_id {
                            if let Some(worker_handle) =
                                self.running_coordinator_worker.get_mut(&sink_id)
                            {
                                if let Some(sender) = worker_handle.request_sender.take() {
                                    // drop the sender as a signal to notify the coordinator worker
                                    // to stop
                                    drop(sender);
                                }
                                worker_handle.finish_notifiers.push(finish_notifier);
                            } else {
                                debug!(
                                    "sink coordinator of {} is not running. Notify finish directly",
                                    sink_id.sink_id
                                );
                                send_with_err_check!(finish_notifier, ());
                            }
                        } else {
                            self.clean_up().await;
                            send_with_err_check!(finish_notifier, ());
                        }
                    }
                },
                ManagerEvent::CoordinatorWorkerFinished {
                    sink_id,
                    join_result,
                } => self.handle_coordinator_finished(sink_id, join_result),
            }
        }
        self.clean_up().await;
        info!("sink manager worker exited");
    }

    async fn next_event(&mut self) -> Option<ManagerEvent> {
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
                    Either::Left((Some(request), _)) => Some(ManagerEvent::NewRequest(request)),
                    Either::Left((None, _)) => None,
                    Either::Right(((sink_id, join_result), _)) => {
                        Some(ManagerEvent::CoordinatorWorkerFinished {
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
        for worker_handle in self.running_coordinator_worker.values_mut() {
            if let Some(sender) = worker_handle.request_sender.take() {
                // drop the sender to notify the coordinator worker to stop
                drop(sender);
            }
        }
        while let Some((sink_id, join_result)) =
            self.running_coordinator_worker_join_handles.next().await
        {
            self.handle_coordinator_finished(sink_id, join_result);
        }
        info!("sink manager worker finished cleaning up");
    }

    fn handle_coordinator_finished(&mut self, sink_id: SinkId, join_result: Result<(), JoinError>) {
        let worker_handle = self
            .running_coordinator_worker
            .remove(&sink_id)
            .expect("finished coordinator should have an associated worker handle");
        for finish_notifier in worker_handle.finish_notifiers {
            send_with_err_check!(finish_notifier, ());
        }
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

    fn handle_new_sink_writer(
        &mut self,
        request: NewSinkWriterRequest,
        spawn_coordinator_worker: &mut impl SpawnCoordinatorFn,
    ) {
        let param = &request.param;
        let sink_id = param.sink_id;

        // Launch the coordinator worker task if it is the first
        match self.running_coordinator_worker.entry(param.sink_id) {
            Entry::Occupied(mut entry) => {
                if let Some(sender) = entry.get_mut().request_sender.as_mut() {
                    send_with_err_check!(sender, request);
                } else {
                    warn!(
                        "handle a new request while the sink coordinator is being stopped: {:?}",
                        param
                    );
                    drop(request.response_tx);
                }
            }
            Entry::Vacant(entry) => {
                let (request_tx, request_rx) = unbounded_channel();
                let connector_client = self.connector_client.clone();
                let join_handle = spawn_coordinator_worker(request, request_rx, connector_client);
                self.running_coordinator_worker_join_handles.push(
                    join_handle
                        .map(move |join_result| (sink_id, join_result))
                        .boxed(),
                );
                entry.insert(CoordinatorWorkerHandle {
                    request_sender: Some(request_tx),
                    finish_notifiers: Vec::new(),
                });
            }
        };
    }
}
