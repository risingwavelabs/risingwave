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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::{Either, select};
use futures::pin_mut;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::{
    Sink, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkParam, build_sink,
};
use risingwave_pb::connector_service::{SinkMetadata, coordinate_request};
use sea_orm::DatabaseConnection;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::sleep;
use tonic::Status;
use tracing::{error, warn};

use crate::manager::sink_coordination::handle::SinkWriterCoordinationHandle;

async fn run_future_with_periodic_fn<F: Future>(
    future: F,
    interval: Duration,
    mut f: impl FnMut(),
) -> F::Output {
    pin_mut!(future);
    loop {
        match select(&mut future, pin!(sleep(interval))).await {
            Either::Left((output, _)) => {
                break output;
            }
            Either::Right(_) => f(),
        }
    }
}

type HandleId = usize;

#[derive(Default)]
struct AligningRequests<R> {
    requests: Vec<R>,
    handle_ids: HashSet<HandleId>,
    committed_bitmap: Option<Bitmap>, // lazy-initialized on first request
}

impl<R> AligningRequests<R> {
    fn add_new_request(
        &mut self,
        handle_id: HandleId,
        request: R,
        vnode_bitmap: &Bitmap,
    ) -> anyhow::Result<()>
    where
        R: Debug,
    {
        let committed_bitmap = self
            .committed_bitmap
            .get_or_insert_with(|| Bitmap::zeros(vnode_bitmap.len()));
        assert_eq!(committed_bitmap.len(), vnode_bitmap.len());

        let check_bitmap = (&*committed_bitmap) & vnode_bitmap;
        if check_bitmap.count_ones() > 0 {
            return Err(anyhow!(
                "duplicate vnode {:?}. request vnode: {:?}, prev vnode: {:?}. pending request: {:?}, request: {:?}",
                check_bitmap.iter_ones().collect_vec(),
                vnode_bitmap,
                committed_bitmap,
                self.requests,
                request
            ));
        }
        *committed_bitmap |= vnode_bitmap;
        self.requests.push(request);
        assert!(self.handle_ids.insert(handle_id));
        Ok(())
    }

    fn aligned(&self) -> bool {
        self.committed_bitmap.as_ref().is_some_and(|b| b.all())
    }
}

struct CoordinationHandleManager {
    param: SinkParam,
    writer_handles: HashMap<HandleId, SinkWriterCoordinationHandle>,
    next_handle_id: HandleId,
    request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
}

impl CoordinationHandleManager {
    fn start(
        &mut self,
        log_store_rewind_start_epoch: Option<u64>,
        handle_ids: impl IntoIterator<Item = HandleId>,
    ) -> anyhow::Result<()> {
        for handle_id in handle_ids {
            let handle = self
                .writer_handles
                .get_mut(&handle_id)
                .ok_or_else(|| anyhow!("fail to find handle for {} to start", handle_id,))?;
            handle.start(log_store_rewind_start_epoch).map_err(|_| {
                anyhow!(
                    "fail to start {:?} for handle {}",
                    log_store_rewind_start_epoch,
                    handle_id
                )
            })?;
        }
        Ok(())
    }

    fn ack_commit(
        &mut self,
        epoch: u64,
        handle_ids: impl IntoIterator<Item = HandleId>,
    ) -> anyhow::Result<()> {
        for handle_id in handle_ids {
            let handle = self.writer_handles.get_mut(&handle_id).ok_or_else(|| {
                anyhow!(
                    "fail to find handle for {} when ack commit on epoch {}",
                    handle_id,
                    epoch
                )
            })?;
            handle.ack_commit(epoch).map_err(|_| {
                anyhow!(
                    "fail to ack commit on epoch {} for handle {}",
                    epoch,
                    handle_id
                )
            })?;
        }
        Ok(())
    }

    async fn next_request_inner(
        writer_handles: &mut HashMap<HandleId, SinkWriterCoordinationHandle>,
    ) -> anyhow::Result<(HandleId, coordinate_request::Msg)> {
        poll_fn(|cx| {
            for (handle_id, handle) in writer_handles.iter_mut() {
                if let Poll::Ready(result) = handle.poll_next_request(cx) {
                    return Poll::Ready(result.map(|request| (*handle_id, request)));
                }
            }
            Poll::Pending
        })
        .await
    }
}

enum CoordinationHandleManagerEvent {
    NewHandle,
    UpdateVnodeBitmap,
    Stop,
    CommitRequest { epoch: u64, metadata: SinkMetadata },
}

impl CoordinationHandleManager {
    async fn next_event(&mut self) -> anyhow::Result<(HandleId, CoordinationHandleManagerEvent)> {
        {
            select! {
                handle = self.request_rx.recv() => {
                    let handle = handle.ok_or_else(|| anyhow!("end of writer request stream"))?;
                    if handle.param() != &self.param {
                        warn!(prev_param = ?self.param, new_param = ?handle.param(), "sink param mismatch");
                    }
                    let handle_id = self.next_handle_id;
                    self.next_handle_id += 1;
                    self.writer_handles.insert(handle_id, handle);
                    Ok((handle_id, CoordinationHandleManagerEvent::NewHandle))
                }
                result = Self::next_request_inner(&mut self.writer_handles) => {
                    let (handle_id, request) = result?;
                    let event = match request {
                        coordinate_request::Msg::CommitRequest(request) => {
                            CoordinationHandleManagerEvent::CommitRequest {
                                epoch: request.epoch,
                                metadata: request.metadata.ok_or_else(|| anyhow!("empty sink metadata"))?,
                            }
                        }
                        coordinate_request::Msg::UpdateVnodeRequest(_) => {
                            CoordinationHandleManagerEvent::UpdateVnodeBitmap
                        }
                        coordinate_request::Msg::Stop(_) => {
                            CoordinationHandleManagerEvent::Stop
                        }
                        coordinate_request::Msg::StartRequest(_) => {
                            unreachable!("should have been handled");
                        }
                    };
                    Ok((handle_id, event))
                }
            }
        }
    }

    fn vnode_bitmap(&self, handle_id: HandleId) -> &Bitmap {
        self.writer_handles[&handle_id].vnode_bitmap()
    }

    fn stop_handle(&mut self, handle_id: HandleId) -> anyhow::Result<()> {
        self.writer_handles
            .remove(&handle_id)
            .expect("should exist")
            .stop()
    }

    async fn wait_init_handles(
        &mut self,
        log_store_rewind_start_epoch: Option<u64>,
    ) -> anyhow::Result<HashSet<HandleId>> {
        assert!(self.writer_handles.is_empty());
        let mut init_requests = AligningRequests::default();
        while !init_requests.aligned() {
            let (handle_id, event) = self.next_event().await?;
            let unexpected_event = match event {
                CoordinationHandleManagerEvent::NewHandle => {
                    init_requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
                    continue;
                }
                CoordinationHandleManagerEvent::UpdateVnodeBitmap => "UpdateVnodeBitmap",
                CoordinationHandleManagerEvent::CommitRequest { .. } => "CommitRequest",
                CoordinationHandleManagerEvent::Stop => "Stop",
            };
            return Err(anyhow!(
                "expect new handle during init, but got {}",
                unexpected_event
            ));
        }
        self.start(
            log_store_rewind_start_epoch,
            init_requests.handle_ids.iter().cloned(),
        )?;
        Ok(init_requests.handle_ids)
    }

    async fn alter_parallelisms(
        &mut self,
        altered_handles: impl Iterator<Item = HandleId>,
        prev_commit_epoch: u64,
    ) -> anyhow::Result<HashSet<HandleId>> {
        let mut requests = AligningRequests::default();
        for handle_id in altered_handles {
            requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
        }
        let mut remaining_handles: HashSet<_> = self
            .writer_handles
            .keys()
            .filter(|handle_id| !requests.handle_ids.contains(handle_id))
            .cloned()
            .collect();
        while !remaining_handles.is_empty() && !requests.aligned() {
            let (handle_id, event) = self.next_event().await?;
            match event {
                CoordinationHandleManagerEvent::NewHandle => {
                    requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
                }
                CoordinationHandleManagerEvent::UpdateVnodeBitmap => {
                    assert!(remaining_handles.remove(&handle_id));
                    requests.add_new_request(handle_id, (), self.vnode_bitmap(handle_id))?;
                }
                CoordinationHandleManagerEvent::Stop => {
                    assert!(remaining_handles.remove(&handle_id));
                    self.stop_handle(handle_id)?;
                }
                CoordinationHandleManagerEvent::CommitRequest { epoch, .. } => {
                    bail!(
                        "receive commit request on epoch {} from handle {} during alter parallelism",
                        epoch,
                        handle_id
                    );
                }
            }
        }
        self.start(Some(prev_commit_epoch), requests.handle_ids.iter().cloned())?;
        Ok(requests.handle_ids)
    }
}

pub struct CoordinatorWorker {
    handle_manager: CoordinationHandleManager,
}

impl CoordinatorWorker {
    pub async fn run(
        param: SinkParam,
        request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
        db: DatabaseConnection,
        subscriber: SinkCommittedEpochSubscriber,
    ) {
        let sink = match build_sink(param.clone()) {
            Ok(sink) => sink,
            Err(e) => {
                error!(
                    error = %e.as_report(),
                    "unable to build sink with param {:?}",
                    param
                );
                return;
            }
        };

        dispatch_sink!(sink, sink, {
            let coordinator = match sink.new_coordinator(db).await {
                Ok(coordinator) => coordinator,
                Err(e) => {
                    error!(
                        error = %e.as_report(),
                        "unable to build coordinator with param {:?}",
                        param
                    );
                    return;
                }
            };
            Self::execute_coordinator(param, request_rx, coordinator, subscriber).await
        });
    }

    pub async fn execute_coordinator(
        param: SinkParam,
        request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
        coordinator: impl SinkCommitCoordinator,
        subscriber: SinkCommittedEpochSubscriber,
    ) {
        let mut worker = CoordinatorWorker {
            handle_manager: CoordinationHandleManager {
                param,
                writer_handles: HashMap::new(),
                next_handle_id: 0,
                request_rx,
            },
        };

        if let Err(e) = worker.run_coordination(coordinator, subscriber).await {
            for handle in worker.handle_manager.writer_handles.into_values() {
                handle.abort(Status::internal(format!(
                    "failed to run coordination: {:?}",
                    e.as_report()
                )))
            }
        }
    }

    async fn run_coordination(
        &mut self,
        mut coordinator: impl SinkCommitCoordinator,
        subscriber: SinkCommittedEpochSubscriber,
    ) -> anyhow::Result<()> {
        let initial_log_store_rewind_start_epoch = coordinator.init(subscriber).await?;
        let sink_id = self.handle_manager.param.sink_id;
        let mut running_handles = self
            .handle_manager
            .wait_init_handles(initial_log_store_rewind_start_epoch)
            .await?;
        let mut pending_epochs: BTreeMap<u64, AligningRequests<SinkMetadata>> = BTreeMap::new();
        let mut pending_new_handles = vec![];
        let mut prev_commit_epoch = None;
        loop {
            let (handle_id, event) = self.handle_manager.next_event().await?;
            let (epoch, metadata) = match event {
                CoordinationHandleManagerEvent::NewHandle => {
                    pending_new_handles.push(handle_id);
                    continue;
                }
                CoordinationHandleManagerEvent::UpdateVnodeBitmap => {
                    running_handles = self
                        .handle_manager
                        .alter_parallelisms(
                            pending_new_handles.drain(..).chain([handle_id]),
                            prev_commit_epoch.ok_or_else(|| {
                                anyhow!("should have committed once on alter parallelisms")
                            })?,
                        )
                        .await?;
                    continue;
                }
                CoordinationHandleManagerEvent::Stop => {
                    self.handle_manager.stop_handle(handle_id)?;
                    running_handles = self
                        .handle_manager
                        .alter_parallelisms(
                            pending_new_handles.drain(..),
                            prev_commit_epoch.ok_or_else(|| {
                                anyhow!("should have committed once on alter parallelisms")
                            })?,
                        )
                        .await?;
                    continue;
                }
                CoordinationHandleManagerEvent::CommitRequest { epoch, metadata } => {
                    (epoch, metadata)
                }
            };
            if !running_handles.contains(&handle_id) {
                bail!(
                    "receiving commit request from non-running handle {}, running handles: {:?}",
                    handle_id,
                    running_handles
                );
            }
            pending_epochs.entry(epoch).or_default().add_new_request(
                handle_id,
                metadata,
                self.handle_manager.vnode_bitmap(handle_id),
            )?;
            if pending_epochs
                .first_key_value()
                .expect("non-empty")
                .1
                .aligned()
            {
                let (epoch, requests) = pending_epochs.pop_first().expect("non-empty");

                let start_time = Instant::now();
                run_future_with_periodic_fn(
                    coordinator.commit(epoch, requests.requests),
                    Duration::from_secs(5),
                    || {
                        warn!(
                            elapsed = ?start_time.elapsed(),
                            sink_id = sink_id.sink_id,
                            "committing"
                        );
                    },
                )
                .await
                .map_err(|e| anyhow!(e))?;
                self.handle_manager.ack_commit(epoch, requests.handle_ids)?;
                prev_commit_epoch = Some(epoch);
            }
        }
    }
}
