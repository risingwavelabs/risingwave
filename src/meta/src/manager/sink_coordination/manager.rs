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
use risingwave_pb::connector_service::coordinate_request::Msg;
use risingwave_pb::connector_service::{coordinate_request, CoordinateRequest, CoordinateResponse};
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
    pub fn start_worker() -> (Self, (JoinHandle<()>, Sender<()>)) {
        Self::start_worker_with_spawn_worker(|writer_request, manager_request_stream| {
            tokio::spawn(CoordinatorWorker::run(
                writer_request,
                manager_request_stream,
            ))
        })
    }

    fn start_worker_with_spawn_worker(
        spawn_coordinator_worker: impl SpawnCoordinatorFn,
    ) -> (Self, (JoinHandle<()>, Sender<()>)) {
        let (request_tx, request_rx) = mpsc::channel(BOUNDED_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = channel();
        let worker = ManagerWorker::new(request_rx, shutdown_rx);
        let join_handle = tokio::spawn(worker.execute(spawn_coordinator_worker));
        (
            SinkCoordinatorManager { request_tx },
            (join_handle, shutdown_tx),
        )
    }

    pub async fn handle_new_request(
        &self,
        mut request_stream: SinkWriterRequestStream,
    ) -> Result<impl Stream<Item = Result<CoordinateResponse, Status>>, Status> {
        let (param, vnode_bitmap) = match request_stream.try_next().await? {
            Some(CoordinateRequest {
                msg:
                    Some(Msg::StartRequest(coordinate_request::StartCoordinationRequest {
                        param: Some(param),
                        vnode_bitmap: Some(vnode_bitmap),
                    })),
            }) => (SinkParam::from_proto(param), Bitmap::from(&vnode_bitmap)),
            msg => {
                return Err(Status::invalid_argument(format!(
                    "expected CoordinateRequest::StartRequest in the first request, get {:?}",
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
        info!("successfully stop coordinator: {:?}", sink_id);
    }

    pub async fn reset(&self) {
        self.stop_coordinator(None).await;
    }

    pub async fn stop_sink_coordinator(&self, sink_id: SinkId) {
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

trait SpawnCoordinatorFn = FnMut(NewSinkWriterRequest, UnboundedReceiver<NewSinkWriterRequest>) -> JoinHandle<()>
    + Send
    + 'static;

impl ManagerWorker {
    fn new(request_rx: mpsc::Receiver<ManagerRequest>, shutdown_rx: Receiver<()>) -> Self {
        ManagerWorker {
            request_rx,
            shutdown_rx: Some(shutdown_rx),
            running_coordinator_worker_join_handles: Default::default(),
            running_coordinator_worker: Default::default(),
        }
    }

    async fn execute(mut self, mut spawn_coordinator_worker: impl SpawnCoordinatorFn) {
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
                let join_handle = spawn_coordinator_worker(request, request_rx);
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

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::pin::pin;
    use std::task::Poll;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use futures::future::join;
    use futures::{FutureExt, StreamExt};
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use risingwave_common::buffer::{Bitmap, BitmapBuilder};
    use risingwave_common::hash::VirtualNode;
    use risingwave_connector::sink::catalog::{SinkId, SinkType};
    use risingwave_connector::sink::{SinkCommitCoordinator, SinkError, SinkParam};
    use risingwave_pb::connector_service::sink_metadata::{Metadata, SerializedMetadata};
    use risingwave_pb::connector_service::SinkMetadata;
    use risingwave_rpc_client::CoordinatorStreamHandle;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::manager::sink_coordination::coordinator_worker::CoordinatorWorker;
    use crate::manager::sink_coordination::{NewSinkWriterRequest, SinkCoordinatorManager};

    struct MockCoordinator<C, F: FnMut(u64, Vec<SinkMetadata>, &mut C) -> Result<(), SinkError>> {
        context: C,
        f: F,
    }

    impl<C, F: FnMut(u64, Vec<SinkMetadata>, &mut C) -> Result<(), SinkError>> MockCoordinator<C, F> {
        fn new(context: C, f: F) -> Self {
            MockCoordinator { context, f }
        }
    }

    #[async_trait]
    impl<C: Send, F: FnMut(u64, Vec<SinkMetadata>, &mut C) -> Result<(), SinkError> + Send>
        SinkCommitCoordinator for MockCoordinator<C, F>
    {
        async fn init(&mut self) -> risingwave_connector::sink::Result<()> {
            Ok(())
        }

        async fn commit(
            &mut self,
            epoch: u64,
            metadata: Vec<SinkMetadata>,
        ) -> risingwave_connector::sink::Result<()> {
            (self.f)(epoch, metadata, &mut self.context)
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let sink_id = SinkId::from(1);
        let param = SinkParam {
            sink_id,
            properties: Default::default(),
            columns: vec![],
            downstream_pk: vec![],
            sink_type: SinkType::AppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
            sink_into_name: None,
        };

        let epoch1 = 233;
        let epoch2 = 234;

        let mut all_vnode = (0..VirtualNode::COUNT).collect_vec();
        all_vnode.shuffle(&mut rand::thread_rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);

        let metadata = [
            [vec![1u8, 2u8], vec![3u8, 4u8]],
            [vec![5u8, 6u8], vec![7u8, 8u8]],
        ];

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let param = param.clone();
                let metadata = metadata.clone();
                move |first_request: NewSinkWriterRequest, new_writer_rx| {
                    let param = param.clone();
                    let metadata = metadata.clone();
                    tokio::spawn(async move {
                        // validate the start request
                        assert_eq!(first_request.param, param);
                        CoordinatorWorker::execute_coordinator(
                            first_request,
                            new_writer_rx,
                            MockCoordinator::new(0, |epoch, metadata_list, count: &mut usize| {
                                *count += 1;
                                let mut metadata_list = metadata_list
                                    .into_iter()
                                    .map(|metadata| match metadata {
                                        SinkMetadata {
                                            metadata:
                                                Some(Metadata::Serialized(SerializedMetadata {
                                                    metadata,
                                                })),
                                        } => metadata,
                                        _ => unreachable!(),
                                    })
                                    .collect_vec();
                                metadata_list.sort();
                                match *count {
                                    1 => {
                                        assert_eq!(epoch, epoch1);
                                        assert_eq!(2, metadata_list.len());
                                        assert_eq!(metadata[0][0], metadata_list[0]);
                                        assert_eq!(metadata[0][1], metadata_list[1]);
                                    }
                                    2 => {
                                        assert_eq!(epoch, epoch2);
                                        assert_eq!(2, metadata_list.len());
                                        assert_eq!(metadata[1][0], metadata_list[0]);
                                        assert_eq!(metadata[1][1], metadata_list[1]);
                                    }
                                    _ => unreachable!(),
                                }
                                Ok(())
                            }),
                        )
                        .await;
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
        };

        let mut build_client_future1 = pin!(build_client(vnode1));
        assert!(
            poll_fn(|cx| Poll::Ready(build_client_future1.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let (mut client1, mut client2) =
            join(build_client_future1, pin!(build_client(vnode2))).await;

        {
            // commit epoch1
            let mut commit_future = pin!(client2
                .commit(
                    epoch1,
                    SinkMetadata {
                        metadata: Some(Metadata::Serialized(SerializedMetadata {
                            metadata: metadata[0][1].clone(),
                        })),
                    },
                )
                .map(|result| result.unwrap()));
            assert!(poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
                .await
                .is_pending());
            join(
                commit_future,
                client1
                    .commit(
                        epoch1,
                        SinkMetadata {
                            metadata: Some(Metadata::Serialized(SerializedMetadata {
                                metadata: metadata[0][0].clone(),
                            })),
                        },
                    )
                    .map(|result| result.unwrap()),
            )
            .await;
        }

        // commit epoch2
        let mut commit_future = pin!(client1
            .commit(
                epoch2,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata[1][0].clone(),
                    })),
                },
            )
            .map(|result| result.unwrap()));
        assert!(poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
            .await
            .is_pending());
        join(
            commit_future,
            client2
                .commit(
                    epoch2,
                    SinkMetadata {
                        metadata: Some(Metadata::Serialized(SerializedMetadata {
                            metadata: metadata[1][1].clone(),
                        })),
                    },
                )
                .map(|result| result.unwrap()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_single_writer() {
        let sink_id = SinkId::from(1);
        let param = SinkParam {
            sink_id,
            properties: Default::default(),
            columns: vec![],
            downstream_pk: vec![],
            sink_type: SinkType::AppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        };

        let epoch1 = 233;
        let epoch2 = 234;

        let all_vnode = (0..VirtualNode::COUNT).collect_vec();
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode = build_bitmap(&all_vnode);

        let metadata = [vec![1u8, 2u8], vec![3u8, 4u8]];

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let param = param.clone();
                let metadata = metadata.clone();
                move |first_request: NewSinkWriterRequest, new_writer_rx| {
                    let param = param.clone();
                    let metadata = metadata.clone();
                    tokio::spawn(async move {
                        // validate the start request
                        assert_eq!(first_request.param, param);
                        CoordinatorWorker::execute_coordinator(
                            first_request,
                            new_writer_rx,
                            MockCoordinator::new(0, |epoch, metadata_list, count: &mut usize| {
                                *count += 1;
                                let mut metadata_list = metadata_list
                                    .into_iter()
                                    .map(|metadata| match metadata {
                                        SinkMetadata {
                                            metadata:
                                                Some(Metadata::Serialized(SerializedMetadata {
                                                    metadata,
                                                })),
                                        } => metadata,
                                        _ => unreachable!(),
                                    })
                                    .collect_vec();
                                metadata_list.sort();
                                match *count {
                                    1 => {
                                        assert_eq!(epoch, epoch1);
                                        assert_eq!(1, metadata_list.len());
                                        assert_eq!(metadata[0], metadata_list[0]);
                                    }
                                    2 => {
                                        assert_eq!(epoch, epoch2);
                                        assert_eq!(1, metadata_list.len());
                                        assert_eq!(metadata[1], metadata_list[0]);
                                    }
                                    _ => unreachable!(),
                                }
                                Ok(())
                            }),
                        )
                        .await;
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
        };

        let mut client = build_client(vnode).await;

        client
            .commit(
                epoch1,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata[0].clone(),
                    })),
                },
            )
            .await
            .unwrap();

        client
            .commit(
                epoch2,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: metadata[1].clone(),
                    })),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_sink_while_init() {
        let sink_id = SinkId::from(1);
        let param = SinkParam {
            sink_id,
            properties: Default::default(),
            columns: vec![],
            downstream_pk: vec![],
            sink_type: SinkType::AppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
            sink_into_name: None,
        };

        let (manager, (_join_handle, _stop_tx)) = SinkCoordinatorManager::start_worker();

        let mut build_client_future1 = pin!(CoordinatorStreamHandle::new_with_init_stream(
            param.to_proto(),
            Bitmap::zeros(VirtualNode::COUNT),
            |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            },
        ));
        assert!(
            poll_fn(|cx| Poll::Ready(build_client_future1.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        manager.stop_sink_coordinator(sink_id).await;

        assert!(build_client_future1.await.is_err());
    }

    #[tokio::test]
    async fn test_partial_commit() {
        let sink_id = SinkId::from(1);
        let param = SinkParam {
            sink_id,
            properties: Default::default(),
            columns: vec![],
            downstream_pk: vec![],
            sink_type: SinkType::AppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
            sink_into_name: None,
        };

        let epoch = 233;

        let mut all_vnode = (0..VirtualNode::COUNT).collect_vec();
        all_vnode.shuffle(&mut rand::thread_rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let param = param.clone();
                move |first_request: NewSinkWriterRequest, new_writer_rx| {
                    let param = param.clone();
                    tokio::spawn(async move {
                        // validate the start request
                        assert_eq!(first_request.param, param);
                        CoordinatorWorker::execute_coordinator(
                            first_request,
                            new_writer_rx,
                            MockCoordinator::new((), |_, _, _| unreachable!()),
                        )
                        .await;
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
        };

        let (mut client1, client2) = join(build_client(vnode1), build_client(vnode2)).await;

        // commit epoch
        let mut commit_future = pin!(client1.commit(
            epoch,
            SinkMetadata {
                metadata: Some(Metadata::Serialized(SerializedMetadata {
                    metadata: vec![],
                })),
            },
        ));
        assert!(poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
            .await
            .is_pending());
        drop(client2);
        assert!(commit_future.await.is_err());
    }

    #[tokio::test]
    async fn test_fail_commit() {
        let sink_id = SinkId::from(1);
        let param = SinkParam {
            sink_id,
            properties: Default::default(),
            columns: vec![],
            downstream_pk: vec![],
            sink_type: SinkType::AppendOnly,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
            sink_into_name: None,
        };

        let epoch = 233;

        let mut all_vnode = (0..VirtualNode::COUNT).collect_vec();
        all_vnode.shuffle(&mut rand::thread_rng());
        let (first, second) = all_vnode.split_at(VirtualNode::COUNT / 2);
        let build_bitmap = |indexes: &[usize]| {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
            for i in indexes {
                builder.set(*i, true);
            }
            builder.finish()
        };
        let vnode1 = build_bitmap(first);
        let vnode2 = build_bitmap(second);

        let (manager, (_join_handle, _stop_tx)) =
            SinkCoordinatorManager::start_worker_with_spawn_worker({
                let param = param.clone();
                move |first_request: NewSinkWriterRequest, new_writer_rx| {
                    let param = param.clone();
                    tokio::spawn(async move {
                        // validate the start request
                        assert_eq!(first_request.param, param);
                        CoordinatorWorker::execute_coordinator(
                            first_request,
                            new_writer_rx,
                            MockCoordinator::new((), |_, _, _| {
                                Err(SinkError::Coordinator(anyhow!("failed to commit")))
                            }),
                        )
                        .await;
                    })
                }
            });

        let build_client = |vnode| async {
            CoordinatorStreamHandle::new_with_init_stream(param.to_proto(), vnode, |rx| async {
                Ok(tonic::Response::new(
                    manager
                        .handle_new_request(ReceiverStream::new(rx).map(Ok).boxed())
                        .await
                        .unwrap()
                        .boxed(),
                ))
            })
            .await
            .unwrap()
        };

        let (mut client1, mut client2) = join(build_client(vnode1), build_client(vnode2)).await;

        // commit epoch
        let mut commit_future = pin!(client1.commit(
            epoch,
            SinkMetadata {
                metadata: Some(Metadata::Serialized(SerializedMetadata {
                    metadata: vec![],
                })),
            },
        ));
        assert!(poll_fn(|cx| Poll::Ready(commit_future.as_mut().poll(cx)))
            .await
            .is_pending());
        let (result1, result2) = join(
            commit_future,
            client2.commit(
                epoch,
                SinkMetadata {
                    metadata: Some(Metadata::Serialized(SerializedMetadata {
                        metadata: vec![],
                    })),
                },
            ),
        )
        .await;
        assert!(result1.is_err());
        assert!(result2.is_err());
    }
}
