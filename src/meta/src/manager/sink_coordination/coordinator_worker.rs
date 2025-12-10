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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::{Either, pending, select};
use futures::pin_mut;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Field;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::boxed::BoxTwoPhaseCoordinator;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::{
    Sink, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkError, SinkParam, build_sink,
};
use risingwave_meta_model::pending_sink_state::SinkState;
use risingwave_pb::connector_service::{SinkMetadata, coordinate_request};
use sea_orm::DatabaseConnection;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::sleep;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::Status;
use tracing::{error, warn};

use crate::manager::sink_coordination::exactly_once_util::{
    clean_aborted_records, commit_and_prune_epoch, list_sink_states_ordered_by_epoch,
    persist_pre_commit_metadata,
};
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

type RetryBackoffFuture = std::pin::Pin<Box<tokio::time::Sleep>>;
type RetryBackoffStrategy = impl Iterator<Item = RetryBackoffFuture> + Send + 'static;

struct TwoPhaseCommitHandler {
    db: DatabaseConnection,
    sink_id: SinkId,
    curr_hummock_committed_epoch: u64,
    job_committed_epoch_rx: UnboundedReceiver<u64>,
    last_committed_epoch: Option<u64>,
    pending_epochs: VecDeque<(u64, Vec<u8>)>,
    prepared_epochs: VecDeque<(u64, Vec<u8>)>,
    backoff_state: Option<(RetryBackoffFuture, RetryBackoffStrategy)>,
}

impl TwoPhaseCommitHandler {
    fn new(
        db: DatabaseConnection,
        sink_id: SinkId,
        initial_hummock_committed_epoch: u64,
        job_committed_epoch_rx: UnboundedReceiver<u64>,
        last_committed_epoch: Option<u64>,
    ) -> Self {
        Self {
            db,
            sink_id,
            curr_hummock_committed_epoch: initial_hummock_committed_epoch,
            job_committed_epoch_rx,
            last_committed_epoch,
            pending_epochs: VecDeque::new(),
            prepared_epochs: VecDeque::new(),
            backoff_state: None,
        }
    }

    #[define_opaque(RetryBackoffStrategy)]
    fn get_retry_backoff_strategy() -> RetryBackoffStrategy {
        ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(60))
            .map(jitter)
            .map(|delay| Box::pin(tokio::time::sleep(delay)))
    }

    async fn next_to_commit(&mut self) -> anyhow::Result<(u64, Vec<u8>)> {
        loop {
            let wait_backoff = async {
                if self.prepared_epochs.is_empty() {
                    pending::<()>().await;
                } else if let Some((backoff_fut, _)) = &mut self.backoff_state {
                    backoff_fut.await;
                }
            };

            select! {
                _ = wait_backoff => {
                    let (epoch, metadata) = self.prepared_epochs.front().cloned().expect("non-empty");
                    return Ok((epoch, metadata));
                }

                recv_epoch = self.job_committed_epoch_rx.recv() => {
                    let Some(recv_epoch) = recv_epoch else {
                        return Err(anyhow!(
                            "Hummock committed epoch sender closed unexpectedly"
                        ));
                    };
                    self.curr_hummock_committed_epoch = recv_epoch;
                    while let Some((epoch, metadata)) = self.pending_epochs.pop_front_if(|(epoch, _)| *epoch <= recv_epoch) {
                        if let Some((last_epoch, _)) = self.prepared_epochs.back() {
                            assert!(epoch > *last_epoch, "prepared epochs must be in increasing order");
                        }
                        self.prepared_epochs.push_back((epoch, metadata));
                    }
                }
            }
        }
    }

    fn push_new_item(&mut self, epoch: u64, metadata: Vec<u8>) {
        if epoch > self.curr_hummock_committed_epoch {
            if let Some((last_epoch, _)) = self.pending_epochs.back() {
                assert!(
                    epoch > *last_epoch,
                    "pending epochs must be in increasing order"
                );
            }
            self.pending_epochs.push_back((epoch, metadata));
        } else {
            assert!(self.pending_epochs.is_empty());
            if let Some((last_epoch, _)) = self.prepared_epochs.back() {
                assert!(
                    epoch > *last_epoch,
                    "prepared epochs must be in increasing order"
                );
            }
            self.prepared_epochs.push_back((epoch, metadata));
        }
    }

    async fn ack_committed(&mut self, epoch: u64) -> anyhow::Result<()> {
        self.backoff_state = None;
        let (last_epoch, _) = self.prepared_epochs.pop_front().expect("non-empty");
        assert_eq!(last_epoch, epoch);

        commit_and_prune_epoch(&self.db, self.sink_id, epoch, self.last_committed_epoch).await?;
        self.last_committed_epoch = Some(epoch);
        Ok(())
    }

    fn failed_committed(&mut self, epoch: u64, err: SinkError) {
        assert_eq!(self.prepared_epochs.front().expect("non-empty").0, epoch,);
        if let Some((prev_fut, strategy)) = &mut self.backoff_state {
            let new_fut = strategy.next().expect("infinite");
            *prev_fut = new_fut;
        } else {
            let mut strategy = Self::get_retry_backoff_strategy();
            let backoff_fut = strategy.next().expect("infinite");
            self.backoff_state = Some((backoff_fut, strategy));
        }
        tracing::error!(
            error = %err.as_report(),
            %self.sink_id,
            "failed to commit epoch {}, Retrying after backoff",
            epoch,
        );
    }

    async fn try_commit(
        &mut self,
        coordinator: &mut BoxTwoPhaseCoordinator,
        epoch: u64,
        metadata: Vec<u8>,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let commit_res = run_future_with_periodic_fn(
            coordinator.commit(epoch, metadata),
            Duration::from_secs(5),
            || {
                warn!(
                    elapsed = ?start_time.elapsed(),
                    %self.sink_id,
                    "committing during try_commit"
                );
            },
        )
        .await;

        match commit_res {
            Ok(_) => {
                self.ack_committed(epoch).await?;
            }
            Err(e) => {
                self.failed_committed(epoch, e);
            }
        }

        Ok(())
    }

    async fn flush_all_pending_items(
        &mut self,
        coordinator: &mut BoxTwoPhaseCoordinator,
    ) -> anyhow::Result<()> {
        while !self.pending_epochs.is_empty() || !self.prepared_epochs.is_empty() {
            let (epoch, metadata) = self.next_to_commit().await?;
            self.try_commit(coordinator, epoch, metadata).await?;
        }
        Ok(())
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

    fn ack_aligned_initial_epoch(&mut self, aligned_initial_epoch: u64) -> anyhow::Result<()> {
        for (handle_id, handle) in &mut self.writer_handles {
            handle
                .ack_aligned_initial_epoch(aligned_initial_epoch)
                .map_err(|_| {
                    anyhow!(
                        "fail to ack_aligned_initial_epoch {:?} for handle {}",
                        aligned_initial_epoch,
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
    CommitRequest {
        epoch: u64,
        metadata: SinkMetadata,
        add_columns: Option<Vec<Field>>,
    },
    AlignInitialEpoch(u64),
}

impl CoordinationHandleManagerEvent {
    fn name(&self) -> &'static str {
        match self {
            CoordinationHandleManagerEvent::NewHandle => "NewHandle",
            CoordinationHandleManagerEvent::UpdateVnodeBitmap => "UpdateVnodeBitmap",
            CoordinationHandleManagerEvent::Stop => "Stop",
            CoordinationHandleManagerEvent::CommitRequest { .. } => "CommitRequest",
            CoordinationHandleManagerEvent::AlignInitialEpoch(_) => "AlignInitialEpoch",
        }
    }
}

impl CoordinationHandleManager {
    async fn next_event(&mut self) -> anyhow::Result<(HandleId, CoordinationHandleManagerEvent)> {
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
                            add_columns: request.add_columns.map(|add_columns| add_columns.fields.into_iter().map(|field| Field::from_prost(&field)).collect()),
                        }
                    }
                    coordinate_request::Msg::AlignInitialEpochRequest(epoch) => {
                        CoordinationHandleManagerEvent::AlignInitialEpoch(epoch)
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
                event => event.name(),
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
        if log_store_rewind_start_epoch.is_none() {
            let mut align_requests = AligningRequests::default();
            while !align_requests.aligned() {
                let (handle_id, event) = self.next_event().await?;
                match event {
                    CoordinationHandleManagerEvent::AlignInitialEpoch(initial_epoch) => {
                        align_requests.add_new_request(
                            handle_id,
                            initial_epoch,
                            self.vnode_bitmap(handle_id),
                        )?;
                    }
                    other => {
                        return Err(anyhow!("expect AlignInitialEpoch but got {}", other.name()));
                    }
                }
            }
            let aligned_initial_epoch = align_requests
                .requests
                .into_iter()
                .max()
                .expect("non-empty");
            self.ack_aligned_initial_epoch(aligned_initial_epoch)?;
        }
        Ok(init_requests.handle_ids)
    }

    async fn alter_parallelisms(
        &mut self,
        altered_handles: impl Iterator<Item = HandleId>,
        prev_commit_epoch: u64,
        two_phase_flush_fut: Option<impl Future<Output = anyhow::Result<()>>>,
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
        while !remaining_handles.is_empty() || !requests.aligned() {
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
                CoordinationHandleManagerEvent::AlignInitialEpoch(epoch) => {
                    bail!(
                        "receive AlignInitialEpoch on epoch {} from handle {} during alter parallelism",
                        epoch,
                        handle_id
                    );
                }
            }
        }
        // If it is two-phase commit, we need to flush all pending items before starting new handles.
        if let Some(two_phase_flush_fut) = two_phase_flush_fut {
            two_phase_flush_fut.await?;
        }
        self.start(Some(prev_commit_epoch), requests.handle_ids.iter().cloned())?;
        Ok(requests.handle_ids)
    }
}

pub struct CoordinatorWorker {
    handle_manager: CoordinationHandleManager,
}

enum CoordinatorWorkerEvent {
    HandleManagerEvent(HandleId, CoordinationHandleManagerEvent),
    ReadyToCommit(u64, Vec<u8>),
}

impl CoordinatorWorker {
    pub async fn run(
        param: SinkParam,
        request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
        db: DatabaseConnection,
        subscriber: SinkCommittedEpochSubscriber,
        iceberg_compact_stat_sender: UnboundedSender<IcebergSinkCompactionUpdate>,
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
            let coordinator = match sink
                .new_coordinator(Some(iceberg_compact_stat_sender))
                .await
            {
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
            Self::execute_coordinator(db, param, request_rx, coordinator, subscriber).await
        });
    }

    pub async fn execute_coordinator(
        db: DatabaseConnection,
        param: SinkParam,
        request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
        coordinator: SinkCommitCoordinator,
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

        if let Err(e) = worker.run_coordination(db, coordinator, subscriber).await {
            for handle in worker.handle_manager.writer_handles.into_values() {
                handle.abort(Status::internal(format!(
                    "failed to run coordination: {:?}",
                    e.as_report()
                )))
            }
        }
    }

    async fn next_event(
        &mut self,
        two_phase_handler: Option<&mut TwoPhaseCommitHandler>,
    ) -> anyhow::Result<CoordinatorWorkerEvent> {
        // For single-phase coordinator, there is no need to wait.
        let two_phase_next_fut = async {
            if let Some(handler) = two_phase_handler {
                handler.next_to_commit().await
            } else {
                pending().await
            }
        };
        select! {
            next_handle_event = self.handle_manager.next_event() => {
                let (handle_id, event) = next_handle_event?;
                Ok(CoordinatorWorkerEvent::HandleManagerEvent(handle_id, event))
            }

            next_item_to_commit = two_phase_next_fut => {
                let (epoch, metadata) = next_item_to_commit?;
                Ok(CoordinatorWorkerEvent::ReadyToCommit(epoch, metadata))
            }
        }
    }

    async fn run_coordination(
        &mut self,
        db: DatabaseConnection,
        mut coordinator: SinkCommitCoordinator,
        subscriber: SinkCommittedEpochSubscriber,
    ) -> anyhow::Result<()> {
        let sink_id = self.handle_manager.param.sink_id;

        let (initial_log_store_rewind_start_epoch, mut two_phase_handler) = match &mut coordinator {
            SinkCommitCoordinator::SinglePhase(coordinator) => {
                coordinator.init().await?;
                (None, None)
            }
            SinkCommitCoordinator::TwoPhase(coordinator) => {
                let (initial_log_store_rewind_start_epoch, two_phase_handler) = self
                    .init_state_from_store(&db, sink_id, subscriber, coordinator)
                    .await?;
                coordinator.init().await?;
                (
                    initial_log_store_rewind_start_epoch,
                    Some(two_phase_handler),
                )
            }
        };

        let mut running_handles = self
            .handle_manager
            .wait_init_handles(initial_log_store_rewind_start_epoch)
            .await?;

        let mut pending_epochs: BTreeMap<u64, AligningRequests<_>> = BTreeMap::new();
        let mut pending_new_handles = vec![];
        let mut prev_commit_epoch = None;
        loop {
            let event = self.next_event(two_phase_handler.as_mut()).await?;
            let (handle_id, epoch, commit_request) = match event {
                CoordinatorWorkerEvent::HandleManagerEvent(handle_id, event) => match event {
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
                                two_phase_handler.as_mut().map(|handler| {
                                    let SinkCommitCoordinator::TwoPhase(coordinator) =
                                        &mut coordinator
                                    else {
                                        unreachable!("should be two-phase commit coordinator");
                                    };
                                    handler.flush_all_pending_items(coordinator)
                                }),
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
                                two_phase_handler.as_mut().map(|handler| {
                                    let SinkCommitCoordinator::TwoPhase(coordinator) =
                                        &mut coordinator
                                    else {
                                        unreachable!("should be two-phase commit coordinator");
                                    };
                                    handler.flush_all_pending_items(coordinator)
                                }),
                            )
                            .await?;
                        continue;
                    }
                    CoordinationHandleManagerEvent::CommitRequest {
                        epoch,
                        metadata,
                        add_columns,
                    } => (handle_id, epoch, (metadata, add_columns)),
                    CoordinationHandleManagerEvent::AlignInitialEpoch(_) => {
                        bail!("receive AlignInitialEpoch after initialization")
                    }
                },
                CoordinatorWorkerEvent::ReadyToCommit(epoch, metadata) => {
                    let SinkCommitCoordinator::TwoPhase(coordinator) = &mut coordinator else {
                        unreachable!("should be two-phase commit coordinator");
                    };
                    let two_phase_handler = two_phase_handler.as_mut().expect("should exist");
                    two_phase_handler
                        .try_commit(coordinator, epoch, metadata)
                        .await?;

                    continue;
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
                commit_request,
                self.handle_manager.vnode_bitmap(handle_id),
            )?;
            if pending_epochs
                .first_key_value()
                .expect("non-empty")
                .1
                .aligned()
            {
                let (epoch, commit_requests) = pending_epochs.pop_first().expect("non-empty");
                let mut metadatas = Vec::with_capacity(commit_requests.requests.len());
                let mut requests = commit_requests.requests.into_iter();
                let (first_metadata, first_add_columns) = requests.next().expect("non-empty");
                metadatas.push(first_metadata);
                for (metadata, add_columns) in requests {
                    if first_add_columns != add_columns {
                        return Err(anyhow!(
                            "got different add columns {:?} to prev add columns {:?}",
                            add_columns,
                            first_add_columns
                        ));
                    }
                    metadatas.push(metadata);
                }

                match &mut coordinator {
                    SinkCommitCoordinator::SinglePhase(coordinator) => {
                        let start_time = Instant::now();
                        run_future_with_periodic_fn(
                            coordinator.commit(epoch, metadatas, first_add_columns),
                            Duration::from_secs(5),
                            || {
                                warn!(
                                    elapsed = ?start_time.elapsed(),
                                    %sink_id,
                                    "committing"
                                );
                            },
                        )
                        .await
                        .map_err(|e| anyhow!(e))?;
                        self.handle_manager
                            .ack_commit(epoch, commit_requests.handle_ids)?;
                    }
                    SinkCommitCoordinator::TwoPhase(coordinator) => {
                        let commit_metadata = coordinator
                            .pre_commit(epoch, metadatas, first_add_columns)
                            .await?;
                        persist_pre_commit_metadata(
                            &db,
                            sink_id as _,
                            epoch,
                            commit_metadata.clone(),
                        )
                        .await?;
                        self.handle_manager
                            .ack_commit(epoch, commit_requests.handle_ids)?;

                        let two_phase_handler = two_phase_handler.as_mut().expect("should exist");
                        two_phase_handler.push_new_item(epoch, commit_metadata);
                    }
                }
                prev_commit_epoch = Some(epoch);
            }
        }
    }

    /// Return the log store rewind start epoch if exists.
    async fn init_state_from_store(
        &mut self,
        db: &DatabaseConnection,
        sink_id: SinkId,
        subscriber: SinkCommittedEpochSubscriber,
        coordinator: &mut BoxTwoPhaseCoordinator,
    ) -> anyhow::Result<(Option<u64>, TwoPhaseCommitHandler)> {
        let ordered_metadata = list_sink_states_ordered_by_epoch(db, sink_id as _).await?;

        let mut metadata_iter = ordered_metadata.into_iter().peekable();
        let last_committed_epoch = metadata_iter
            .next_if(|(_, state, _)| matches!(state, SinkState::Committed))
            .map(|(epoch, _, _)| epoch);

        let pending_items = metadata_iter
            .peeking_take_while(|(_, state, _)| matches!(state, SinkState::Pending))
            .map(|(epoch, _, metadata)| (epoch, metadata))
            .collect_vec();

        let mut aborted_epochs = vec![];

        for (epoch, state, metadata) in metadata_iter {
            match state {
                SinkState::Aborted => {
                    coordinator.abort(epoch, metadata).await;
                    aborted_epochs.push(epoch);
                }
                other => {
                    unreachable!(
                        "unexpected state {:?} after pending items at epoch {}",
                        other, epoch
                    );
                }
            }
        }

        // Records for all aborted epochs and previously committed epochs are no longer needed.
        clean_aborted_records(db, sink_id, aborted_epochs).await?;

        let (initial_hummock_committed_epoch, job_committed_epoch_rx) = subscriber(sink_id).await?;
        let mut two_phase_handler = TwoPhaseCommitHandler::new(
            db.clone(),
            sink_id,
            initial_hummock_committed_epoch,
            job_committed_epoch_rx,
            last_committed_epoch,
        );

        for (epoch, metadata) in pending_items {
            two_phase_handler.push_new_item(epoch, metadata);
        }

        two_phase_handler
            .flush_all_pending_items(coordinator)
            .await?;

        Ok((last_committed_epoch, two_phase_handler))
    }
}
