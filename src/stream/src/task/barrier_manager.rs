// Copyright 2024 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::future::{pending, poll_fn};
use std::iter::once;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesOrdered};
use futures::{FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::error::tonic::extra::Score;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::{
    PbCreateMviewProgress, PbLocalSstableInfo,
};
use risingwave_rpc_client::error::{ToTonicStatus, TonicStatusWrapper};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::{Code, Status};

use self::managed_state::ManagedBarrierState;
use crate::error::{IntoUnexpectedExit, StreamError, StreamResult};
use crate::task::{
    ActorId, AtomicU64Ref, PartialGraphId, SharedContext, StreamEnvironment, UpDownActorIds,
};

mod managed_state;
mod progress;
#[cfg(test)]
mod tests;

pub use progress::CreateMviewProgressReporter;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::{LocalSstableInfo, SyncResult};
use risingwave_pb::stream_service::streaming_control_stream_request::{
    DatabaseInitialPartialGraph, InitRequest, Request,
};
use risingwave_pb::stream_service::streaming_control_stream_response::{
    InitResponse, ShutdownResponse,
};
use risingwave_pb::stream_service::{
    streaming_control_stream_response, BarrierCompleteResponse, InjectBarrierRequest,
    StreamingControlStreamRequest, StreamingControlStreamResponse,
};

use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Barrier, BarrierInner, StreamExecutorError};
use crate::task::barrier_manager::managed_state::{
    DatabaseManagedBarrierState, ManagedBarrierStateDebugInfo, ManagedBarrierStateEvent,
    PartialGraphManagedBarrierState,
};
use crate::task::barrier_manager::progress::BackfillState;

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

/// Collect result of some barrier on current compute node. Will be reported to the meta service.
#[derive(Debug)]
pub struct BarrierCompleteResult {
    /// The result returned from `sync` of `StateStore`.
    pub sync_result: Option<SyncResult>,

    /// The updated creation progress of materialized view after this barrier.
    pub create_mview_progress: Vec<PbCreateMviewProgress>,
}

pub(super) struct ControlStreamHandle {
    #[expect(clippy::type_complexity)]
    pair: Option<(
        UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
    )>,
}

impl ControlStreamHandle {
    fn empty() -> Self {
        Self { pair: None }
    }

    pub(super) fn new(
        sender: UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        request_stream: BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
    ) -> Self {
        Self {
            pair: Some((sender, request_stream)),
        }
    }

    pub(super) fn connected(&self) -> bool {
        self.pair.is_some()
    }

    fn reset_stream_with_err(&mut self, err: Status) {
        if let Some((sender, _)) = self.pair.take() {
            // Note: `TonicStatusWrapper` provides a better error report.
            let err = TonicStatusWrapper::new(err);
            warn!(error = %err.as_report(), "control stream reset with error");

            let err = err.into_inner();
            if sender.send(Err(err)).is_err() {
                warn!("failed to notify reset of control stream");
            }
        }
    }

    /// Send `Shutdown` message to the control stream and wait for the stream to be closed
    /// by the meta service.
    async fn shutdown_stream(&mut self) {
        if let Some((sender, _)) = self.pair.take() {
            if sender
                .send(Ok(StreamingControlStreamResponse {
                    response: Some(streaming_control_stream_response::Response::Shutdown(
                        ShutdownResponse::default(),
                    )),
                }))
                .is_err()
            {
                warn!("failed to notify shutdown of control stream");
            } else {
                tracing::info!("waiting for meta service to close control stream...");

                // Wait for the stream to be closed, to ensure that the `Shutdown` message has
                // been acknowledged by the meta service for more precise error report.
                //
                // This is because the meta service will reset the control stream manager and
                // drop the connection to us upon recovery. As a result, the receiver part of
                // this sender will also be dropped, causing the stream to close.
                sender.closed().await;
            }
        } else {
            debug!("control stream has been reset, ignore shutdown");
        }
    }

    fn send_response(&mut self, response: streaming_control_stream_response::Response) {
        if let Some((sender, _)) = self.pair.as_ref() {
            if sender
                .send(Ok(StreamingControlStreamResponse {
                    response: Some(response),
                }))
                .is_err()
            {
                self.pair = None;
                warn!("fail to send response. control stream reset");
            }
        } else {
            debug!(?response, "control stream has been reset. ignore response");
        }
    }

    async fn next_request(&mut self) -> StreamingControlStreamRequest {
        if let Some((_, stream)) = &mut self.pair {
            match stream.next().await {
                Some(Ok(request)) => {
                    return request;
                }
                Some(Err(e)) => self.reset_stream_with_err(
                    anyhow!(TonicStatusWrapper::new(e)) // wrap the status to provide better error report
                        .context("failed to get request")
                        .to_status_unnamed(Code::Internal),
                ),
                None => self.reset_stream_with_err(Status::internal("end of stream")),
            }
        }
        pending().await
    }
}

pub(super) enum LocalBarrierEvent {
    ReportActorCollected {
        actor_id: ActorId,
        epoch: EpochPair,
    },
    ReportCreateProgress {
        epoch: EpochPair,
        actor: ActorId,
        vnodes: Vec<usize>,
        state: BackfillState,
    },
    RegisterBarrierSender {
        actor_id: ActorId,
        barrier_sender: mpsc::UnboundedSender<Barrier>,
    },
}

#[derive(strum_macros::Display)]
pub(super) enum LocalActorOperation {
    NewControlStream {
        handle: ControlStreamHandle,
        init_request: InitRequest,
    },
    TakeReceiver {
        database_id: DatabaseId,
        ids: UpDownActorIds,
        result_sender: oneshot::Sender<StreamResult<Receiver>>,
    },
    #[cfg(test)]
    GetCurrentSharedContext(oneshot::Sender<(Arc<SharedContext>, LocalBarrierManager)>),
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
    InspectState {
        result_sender: oneshot::Sender<String>,
    },
    Shutdown {
        result_sender: oneshot::Sender<()>,
    },
}

pub(crate) struct StreamActorManager {
    pub(super) env: StreamEnvironment,
    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Watermark epoch number.
    pub(super) watermark_epoch: AtomicU64Ref,

    /// Manages the await-trees of all actors.
    pub(super) await_tree_reg: Option<await_tree::Registry>,

    /// Runtime for the streaming actors.
    pub(super) runtime: BackgroundShutdownRuntime,
}

pub(super) struct LocalBarrierWorkerDebugInfo<'a> {
    managed_barrier_state: HashMap<DatabaseId, ManagedBarrierStateDebugInfo<'a>>,
    has_control_stream_connected: bool,
}

impl Display for LocalBarrierWorkerDebugInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "\nhas_control_stream_connected: {}",
            self.has_control_stream_connected
        )?;

        for (database_id, managed_barrier_state) in &self.managed_barrier_state {
            writeln!(
                f,
                "database {} managed_barrier_state:\n{}",
                database_id.database_id, managed_barrier_state
            )?;
        }
        Ok(())
    }
}

/// [`LocalBarrierWorker`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierWorker`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub(super) struct LocalBarrierWorker {
    /// Current barrier collection state.
    pub(super) state: ManagedBarrierState,

    /// Futures will be finished in the order of epoch in ascending order.
    await_epoch_completed_futures: HashMap<DatabaseId, FuturesOrdered<AwaitEpochCompletedFuture>>,

    control_stream_handle: ControlStreamHandle,

    pub(super) actor_manager: Arc<StreamActorManager>,
}

impl LocalBarrierWorker {
    pub(super) fn new(
        actor_manager: Arc<StreamActorManager>,
        initial_partial_graphs: Vec<DatabaseInitialPartialGraph>,
    ) -> Self {
        let state = ManagedBarrierState::new(actor_manager.clone(), initial_partial_graphs);
        Self {
            state,
            await_epoch_completed_futures: Default::default(),
            control_stream_handle: ControlStreamHandle::empty(),
            actor_manager,
        }
    }

    fn to_debug_info(&self) -> LocalBarrierWorkerDebugInfo<'_> {
        LocalBarrierWorkerDebugInfo {
            managed_barrier_state: self
                .state
                .databases
                .iter()
                .map(|(database_id, state)| (*database_id, state.to_debug_info()))
                .collect(),
            has_control_stream_connected: self.control_stream_handle.connected(),
        }
    }

    pub(crate) fn get_or_insert_database_shared_context<'a>(
        current_shared_context: &'a mut HashMap<DatabaseId, Arc<SharedContext>>,
        database_id: DatabaseId,
        actor_manager: &StreamActorManager,
    ) -> &'a Arc<SharedContext> {
        current_shared_context
            .entry(database_id)
            .or_insert_with(|| Arc::new(SharedContext::new(database_id, &actor_manager.env)))
    }

    async fn next_completed_epoch(
        futures: &mut HashMap<DatabaseId, FuturesOrdered<AwaitEpochCompletedFuture>>,
    ) -> (
        DatabaseId,
        PartialGraphId,
        Barrier,
        StreamResult<BarrierCompleteResult>,
    ) {
        poll_fn(|cx| {
            for (database_id, futures) in &mut *futures {
                if let Poll::Ready(Some((partial_graph_id, barrier, result))) =
                    futures.poll_next_unpin(cx)
                {
                    return Poll::Ready((*database_id, partial_graph_id, barrier, result));
                }
            }
            Poll::Pending
        })
        .await
    }

    async fn run(mut self, mut actor_op_rx: UnboundedReceiver<LocalActorOperation>) {
        loop {
            select! {
                biased;
                (database_id, event) = self.state.next_event() => {
                    match event {
                        ManagedBarrierStateEvent::BarrierCollected{
                            partial_graph_id,
                            barrier,
                        } => {
                            self.complete_barrier(database_id, partial_graph_id, barrier.epoch.prev);
                        }
                        ManagedBarrierStateEvent::ActorError{
                            actor_id,
                            err,
                        } => {
                            self.notify_actor_failure(database_id, actor_id, err, "recv actor failure").await;
                        }
                    }
                }
                (database_id, partial_graph_id, barrier, result) = Self::next_completed_epoch(&mut self.await_epoch_completed_futures) => {
                    match result {
                        Ok(result) => {
                            self.on_epoch_completed(database_id, partial_graph_id, barrier.epoch.prev, result);
                        }
                        Err(err) => {
                            self.notify_other_failure(err, "failed to complete epoch").await;
                        }
                    }
                },
                actor_op = actor_op_rx.recv() => {
                    if let Some(actor_op) = actor_op {
                        match actor_op {
                            LocalActorOperation::NewControlStream { handle, init_request  } => {
                                self.control_stream_handle.reset_stream_with_err(Status::internal("control stream has been reset to a new one"));
                                self.reset(init_request.databases).await;
                                self.control_stream_handle = handle;
                                self.control_stream_handle.send_response(streaming_control_stream_response::Response::Init(InitResponse {}));
                            }
                            LocalActorOperation::Shutdown { result_sender } => {
                                if self.state.databases.values().any(|database| !database.actor_states.is_empty()) {
                                    tracing::warn!(
                                        "shutdown with running actors, scaling or migration will be triggered"
                                    );
                                }
                                self.control_stream_handle.shutdown_stream().await;
                                let _ = result_sender.send(());
                            }
                            actor_op => {
                                self.handle_actor_op(actor_op);
                            }
                        }
                    }
                    else {
                        break;
                    }
                },
                request = self.control_stream_handle.next_request() => {
                    let result = self.handle_streaming_control_request(request.request.expect("non empty"));
                    if let Err(err) = result {
                        self.notify_other_failure(err, "failed to inject barrier").await;
                    }
                },
            }
        }
    }

    fn handle_streaming_control_request(&mut self, request: Request) -> StreamResult<()> {
        match request {
            Request::InjectBarrier(req) => {
                let barrier = Barrier::from_protobuf(req.get_barrier().unwrap())?;
                self.update_actor_info(
                    DatabaseId::new(req.database_id),
                    req.broadcast_info.iter().cloned(),
                )?;
                self.send_barrier(&barrier, req)?;
                Ok(())
            }
            Request::RemovePartialGraph(req) => {
                self.remove_partial_graphs(
                    DatabaseId::new(req.database_id),
                    req.partial_graph_ids.into_iter().map(PartialGraphId::new),
                );
                Ok(())
            }
            Request::CreatePartialGraph(req) => {
                self.add_partial_graph(
                    DatabaseId::new(req.database_id),
                    PartialGraphId::new(req.partial_graph_id),
                );
                Ok(())
            }
            Request::Init(_) => {
                unreachable!()
            }
        }
    }

    fn handle_actor_op(&mut self, actor_op: LocalActorOperation) {
        match actor_op {
            LocalActorOperation::NewControlStream { .. } | LocalActorOperation::Shutdown { .. } => {
                unreachable!("event {actor_op} should be handled separately in async context")
            }
            LocalActorOperation::TakeReceiver {
                database_id,
                ids,
                result_sender,
            } => {
                let _ = result_sender.send(
                    LocalBarrierWorker::get_or_insert_database_shared_context(
                        &mut self.state.current_shared_context,
                        database_id,
                        &self.actor_manager,
                    )
                    .take_receiver(ids),
                );
            }
            #[cfg(test)]
            LocalActorOperation::GetCurrentSharedContext(sender) => {
                let database_state = self
                    .state
                    .databases
                    .get(&crate::task::TEST_DATABASE_ID)
                    .unwrap();
                let _ = sender.send((
                    database_state.current_shared_context.clone(),
                    database_state.local_barrier_manager.clone(),
                ));
            }
            #[cfg(test)]
            LocalActorOperation::Flush(sender) => {
                use futures::FutureExt;
                while let Some(request) = self.control_stream_handle.next_request().now_or_never() {
                    self.handle_streaming_control_request(
                        request.request.expect("should not be empty"),
                    )
                    .unwrap();
                }
                while let Some((database_id, event)) = self.state.next_event().now_or_never() {
                    match event {
                        ManagedBarrierStateEvent::BarrierCollected {
                            partial_graph_id,
                            barrier,
                        } => {
                            self.complete_barrier(
                                database_id,
                                partial_graph_id,
                                barrier.epoch.prev,
                            );
                        }
                        ManagedBarrierStateEvent::ActorError { .. } => {
                            unreachable!()
                        }
                    }
                }
                sender.send(()).unwrap()
            }
            LocalActorOperation::InspectState { result_sender } => {
                let debug_info = self.to_debug_info();
                let _ = result_sender.send(debug_info.to_string());
            }
        }
    }
}

mod await_epoch_completed_future {
    use std::future::Future;

    use futures::future::BoxFuture;
    use futures::FutureExt;
    use risingwave_hummock_sdk::SyncResult;
    use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;

    use crate::error::StreamResult;
    use crate::executor::Barrier;
    use crate::task::{await_tree_key, BarrierCompleteResult, PartialGraphId};

    pub(super) type AwaitEpochCompletedFuture = impl Future<Output = (PartialGraphId, Barrier, StreamResult<BarrierCompleteResult>)>
        + 'static;

    pub(super) fn instrument_complete_barrier_future(
        partial_graph_id: PartialGraphId,
        complete_barrier_future: Option<BoxFuture<'static, StreamResult<SyncResult>>>,
        barrier: Barrier,
        barrier_await_tree_reg: Option<&await_tree::Registry>,
        create_mview_progress: Vec<PbCreateMviewProgress>,
    ) -> AwaitEpochCompletedFuture {
        let prev_epoch = barrier.epoch.prev;
        let future = async move {
            if let Some(future) = complete_barrier_future {
                let result = future.await;
                result.map(Some)
            } else {
                Ok(None)
            }
        }
        .map(move |result| {
            (
                partial_graph_id,
                barrier,
                result.map(|sync_result| BarrierCompleteResult {
                    sync_result,
                    create_mview_progress,
                }),
            )
        });
        if let Some(reg) = barrier_await_tree_reg {
            reg.register(
                await_tree_key::BarrierAwait { prev_epoch },
                format!("SyncEpoch({})", prev_epoch),
            )
            .instrument(future)
            .left_future()
        } else {
            future.right_future()
        }
    }
}

use await_epoch_completed_future::*;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_storage::StateStoreImpl;

fn sync_epoch(
    state_store: &StateStoreImpl,
    streaming_metrics: &StreamingMetrics,
    prev_epoch: u64,
    table_ids: HashSet<TableId>,
) -> BoxFuture<'static, StreamResult<SyncResult>> {
    let timer = streaming_metrics.barrier_sync_latency.start_timer();
    let hummock = state_store.as_hummock().cloned();
    let future = async move {
        if let Some(hummock) = hummock {
            hummock.sync(vec![(prev_epoch, table_ids)]).await
        } else {
            Ok(SyncResult::default())
        }
    };
    future
        .instrument_await(format!("sync_epoch (epoch {})", prev_epoch))
        .inspect_ok(move |_| {
            timer.observe_duration();
        })
        .map_err(move |e| {
            tracing::error!(
                prev_epoch,
                error = %e.as_report(),
                "Failed to sync state store",
            );
            e.into()
        })
        .boxed()
}

impl LocalBarrierWorker {
    fn complete_barrier(
        &mut self,
        database_id: DatabaseId,
        partial_graph_id: PartialGraphId,
        prev_epoch: u64,
    ) {
        {
            let (barrier, table_ids, create_mview_progress) = self
                .state
                .databases
                .get_mut(&database_id)
                .expect("should exist")
                .pop_barrier_to_complete(partial_graph_id, prev_epoch);

            let complete_barrier_future = match &barrier.kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => {
                    tracing::info!(
                        epoch = prev_epoch,
                        "ignore sealing data for the first barrier"
                    );
                    tracing::info!(?prev_epoch, "ignored syncing data for the first barrier");
                    None
                }
                BarrierKind::Barrier => None,
                BarrierKind::Checkpoint => Some(sync_epoch(
                    &self.actor_manager.env.state_store(),
                    &self.actor_manager.streaming_metrics,
                    prev_epoch,
                    table_ids.expect("should be Some on BarrierKind::Checkpoint"),
                )),
            };

            self.await_epoch_completed_futures
                .entry(database_id)
                .or_default()
                .push_back({
                    instrument_complete_barrier_future(
                        partial_graph_id,
                        complete_barrier_future,
                        barrier,
                        self.actor_manager.await_tree_reg.as_ref(),
                        create_mview_progress,
                    )
                });
        }
    }

    fn on_epoch_completed(
        &mut self,
        database_id: DatabaseId,
        partial_graph_id: PartialGraphId,
        epoch: u64,
        result: BarrierCompleteResult,
    ) {
        let BarrierCompleteResult {
            create_mview_progress,
            sync_result,
        } = result;

        let (synced_sstables, table_watermarks, old_value_ssts) = sync_result
            .map(|sync_result| {
                (
                    sync_result.uncommitted_ssts,
                    sync_result.table_watermarks,
                    sync_result.old_value_ssts,
                )
            })
            .unwrap_or_default();

        let result = {
            {
                streaming_control_stream_response::Response::CompleteBarrier(
                    BarrierCompleteResponse {
                        request_id: "todo".to_string(),
                        partial_graph_id: partial_graph_id.into(),
                        epoch,
                        status: None,
                        create_mview_progress,
                        synced_sstables: synced_sstables
                            .into_iter()
                            .map(
                                |LocalSstableInfo {
                                     sst_info,
                                     table_stats,
                                     created_at,
                                 }| PbLocalSstableInfo {
                                    sst: Some(sst_info.into()),
                                    table_stats_map: to_prost_table_stats_map(table_stats),
                                    created_at,
                                },
                            )
                            .collect_vec(),
                        worker_id: self.actor_manager.env.worker_id(),
                        table_watermarks: table_watermarks
                            .into_iter()
                            .map(|(key, value)| (key.table_id, value.into()))
                            .collect(),
                        old_value_sstables: old_value_ssts
                            .into_iter()
                            .map(|sst| sst.sst_info.into())
                            .collect(),
                        database_id: database_id.database_id,
                    },
                )
            }
        };

        self.control_stream_handle.send_response(result);
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    ///
    /// Note that the error returned here is typically a [`StreamError::barrier_send`], which is not
    /// the root cause of the failure. The caller should then call [`Self::try_find_root_failure`]
    /// to find the root cause.
    fn send_barrier(
        &mut self,
        barrier: &Barrier,
        request: InjectBarrierRequest,
    ) -> StreamResult<()> {
        debug!(
            target: "events::stream::barrier::manager::send",
            "send barrier {:?}, actor_ids_to_collect = {:?}",
            barrier,
            request.actor_ids_to_collect
        );

        self.state
            .databases
            .get_mut(&DatabaseId::new(request.database_id))
            .expect("should exist")
            .transform_to_issued(barrier, request)?;
        Ok(())
    }

    fn remove_partial_graphs(
        &mut self,
        database_id: DatabaseId,
        partial_graph_ids: impl Iterator<Item = PartialGraphId>,
    ) {
        let Some(database_state) = self.state.databases.get_mut(&database_id) else {
            warn!(
                database_id = database_id.database_id,
                "database to remove partial graph not exist"
            );
            return;
        };
        for partial_graph_id in partial_graph_ids {
            if let Some(graph) = database_state.graph_states.remove(&partial_graph_id) {
                assert!(
                    graph.is_empty(),
                    "non empty graph to be removed: {}",
                    &graph
                );
            } else {
                warn!(
                    partial_graph_id = partial_graph_id.0,
                    "no partial graph to remove"
                );
            }
        }
    }

    pub(super) fn add_partial_graph(
        &mut self,
        database_id: DatabaseId,
        partial_graph_id: PartialGraphId,
    ) {
        assert!(self
            .state
            .databases
            .entry(database_id)
            .or_insert_with(|| {
                DatabaseManagedBarrierState::new(
                    self.actor_manager.clone(),
                    LocalBarrierWorker::get_or_insert_database_shared_context(
                        &mut self.state.current_shared_context,
                        database_id,
                        &self.actor_manager,
                    )
                    .clone(),
                    vec![],
                )
            })
            .graph_states
            .insert(
                partial_graph_id,
                PartialGraphManagedBarrierState::new(&self.actor_manager)
            )
            .is_none());
    }

    /// Reset all internal states.
    pub(super) fn reset_state(&mut self, initial_partial_graphs: Vec<DatabaseInitialPartialGraph>) {
        *self = Self::new(self.actor_manager.clone(), initial_partial_graphs);
    }

    /// When a actor exit unexpectedly, the error is reported using this function. The control stream
    /// will be reset and the meta service will then trigger recovery.
    async fn notify_actor_failure(
        &mut self,
        database_id: DatabaseId,
        actor_id: ActorId,
        err: StreamError,
        err_context: &'static str,
    ) {
        let root_err = self.try_find_root_failure(err).await;

        if let Some(database_state) = self.state.databases.get(&database_id)
            && let Some(actor_state) = database_state.actor_states.get(&actor_id)
            && (!actor_state.inflight_barriers.is_empty() || actor_state.is_running())
        {
            self.control_stream_handle.reset_stream_with_err(
                anyhow!(root_err)
                    .context(err_context)
                    .to_status_unnamed(Code::Internal),
            );
        }
    }

    /// When some other failure happens (like failed to send barrier), the error is reported using
    /// this function. The control stream will be reset and the meta service will then trigger recovery.
    ///
    /// This is similar to [`Self::notify_actor_failure`], but since there's not always an actor failure,
    /// the given `err` will be used if there's no root failure found.
    async fn notify_other_failure(&mut self, err: StreamError, message: impl Into<String>) {
        let root_err = self.try_find_root_failure(err).await;

        self.control_stream_handle.reset_stream_with_err(
            anyhow!(root_err)
                .context(message.into())
                .to_status_unnamed(Code::Internal),
        );
    }

    /// Collect actor errors for a while and find the one that might be the root cause.
    ///
    /// Returns `None` if there's no actor error received.
    async fn try_find_root_failure(&mut self, first_err: StreamError) -> ScoredStreamError {
        let mut later_errs = vec![];
        // fetch more actor errors within a timeout
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let error = poll_fn(|cx| {
                    for database in self.state.databases.values_mut() {
                        if let Poll::Ready(option) = database.actor_failure_rx.poll_recv(cx) {
                            let (_, err) = option
                                .expect("should not be none when tx in local_barrier_manager");
                            return Poll::Ready(err);
                        }
                    }
                    Poll::Pending
                })
                .await;
                later_errs.push(error);
            }
        })
        .await;

        once(first_err)
            .chain(later_errs.into_iter())
            .map(|e| ScoredStreamError::new(e.clone()))
            .max_by_key(|e| e.score)
            .expect("non-empty")
    }
}

#[derive(Clone)]
pub struct LocalBarrierManager {
    barrier_event_sender: UnboundedSender<LocalBarrierEvent>,
    actor_failure_sender: UnboundedSender<(ActorId, StreamError)>,
}

impl LocalBarrierWorker {
    /// Create a [`LocalBarrierWorker`] with managed mode.
    pub fn spawn(
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_reg: Option<await_tree::Registry>,
        watermark_epoch: AtomicU64Ref,
        actor_op_rx: UnboundedReceiver<LocalActorOperation>,
    ) -> JoinHandle<()> {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = env.config().actor_runtime_worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("rw-streaming")
                .enable_all()
                .build()
                .unwrap()
        };

        let actor_manager = Arc::new(StreamActorManager {
            env: env.clone(),
            streaming_metrics,
            watermark_epoch,
            await_tree_reg,
            runtime: runtime.into(),
        });
        let worker = LocalBarrierWorker::new(actor_manager, vec![]);
        tokio::spawn(worker.run(actor_op_rx))
    }
}

pub(super) struct EventSender<T>(pub(super) UnboundedSender<T>);

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> EventSender<T> {
    pub(super) fn send_event(&self, event: T) {
        self.0.send(event).expect("should be able to send event")
    }

    pub(super) async fn send_and_await<RSP>(
        &self,
        make_event: impl FnOnce(oneshot::Sender<RSP>) -> T,
    ) -> StreamResult<RSP> {
        let (tx, rx) = oneshot::channel();
        let event = make_event(tx);
        self.send_event(event);
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset").into())
    }
}

impl LocalBarrierManager {
    pub(super) fn new() -> (
        Self,
        UnboundedReceiver<LocalBarrierEvent>,
        UnboundedReceiver<(ActorId, StreamError)>,
    ) {
        let (event_tx, event_rx) = unbounded_channel();
        let (err_tx, err_rx) = unbounded_channel();
        (
            Self {
                barrier_event_sender: event_tx,
                actor_failure_sender: err_tx,
            },
            event_rx,
            err_rx,
        )
    }

    fn send_event(&self, event: LocalBarrierEvent) {
        // ignore error, because the current barrier manager maybe a stale one
        let _ = self.barrier_event_sender.send(event);
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect<M>(&self, actor_id: ActorId, barrier: &BarrierInner<M>) {
        self.send_event(LocalBarrierEvent::ReportActorCollected {
            actor_id,
            epoch: barrier.epoch,
        })
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    pub fn notify_failure(&self, actor_id: ActorId, err: StreamError) {
        let _ = self
            .actor_failure_sender
            .send((actor_id, err.into_unexpected_exit(actor_id)));
    }

    pub fn subscribe_barrier(&self, actor_id: ActorId) -> UnboundedReceiver<Barrier> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.send_event(LocalBarrierEvent::RegisterBarrierSender {
            actor_id,
            barrier_sender: tx,
        });
        rx
    }
}

/// A [`StreamError`] with a score, used to find the root cause of actor failures.
#[derive(Debug, Clone)]
struct ScoredStreamError {
    error: StreamError,
    score: Score,
}

impl std::fmt::Display for ScoredStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for ScoredStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }

    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        self.error.provide(request);
        // HIGHLIGHT: Provide the score to make it retrievable from meta service.
        request.provide_value(self.score);
    }
}

impl ScoredStreamError {
    /// Score the given error based on hard-coded rules.
    fn new(error: StreamError) -> Self {
        // Explicitly list all error kinds here to notice developers to update this function when
        // there are changes in error kinds.

        fn stream_executor_error_score(e: &StreamExecutorError) -> i32 {
            use crate::executor::error::ErrorKind;
            match e.inner() {
                // `ChannelClosed` or `ExchangeChannelClosed` is likely to be caused by actor exit
                // and not the root cause.
                ErrorKind::ChannelClosed(_) | ErrorKind::ExchangeChannelClosed(_) => 1,

                // Normal errors.
                ErrorKind::Uncategorized(_)
                | ErrorKind::Storage(_)
                | ErrorKind::ArrayError(_)
                | ErrorKind::ExprError(_)
                | ErrorKind::SerdeError(_)
                | ErrorKind::SinkError(_, _)
                | ErrorKind::RpcError(_)
                | ErrorKind::AlignBarrier(_, _)
                | ErrorKind::ConnectorError(_)
                | ErrorKind::DmlError(_)
                | ErrorKind::NotImplemented(_) => 999,
            }
        }

        fn stream_error_score(e: &StreamError) -> i32 {
            use crate::error::ErrorKind;
            match e.inner() {
                // `UnexpectedExit` wraps the original error. Score on the inner error.
                ErrorKind::UnexpectedExit { source, .. } => stream_error_score(source),

                // `BarrierSend` is likely to be caused by actor exit and not the root cause.
                ErrorKind::BarrierSend { .. } => 1,

                // Executor errors first.
                ErrorKind::Executor(ee) => 2000 + stream_executor_error_score(ee),

                // Then other errors.
                ErrorKind::Uncategorized(_)
                | ErrorKind::Storage(_)
                | ErrorKind::Expression(_)
                | ErrorKind::Array(_)
                | ErrorKind::Secret(_) => 1000,
            }
        }

        let score = Score(stream_error_score(&error));
        Self { error, score }
    }
}

#[cfg(test)]
impl LocalBarrierManager {
    fn spawn_for_test() -> EventSender<LocalActorOperation> {
        use std::sync::atomic::AtomicU64;
        let (tx, rx) = unbounded_channel();
        let _join_handle = LocalBarrierWorker::spawn(
            StreamEnvironment::for_test(),
            Arc::new(StreamingMetrics::unused()),
            None,
            Arc::new(AtomicU64::new(0)),
            rx,
        );
        EventSender(tx)
    }

    pub fn for_test() -> Self {
        let (tx, mut rx) = unbounded_channel();
        let (failure_tx, failure_rx) = unbounded_channel();
        let _join_handle = tokio::spawn(async move {
            let _failure_rx = failure_rx;
            while rx.recv().await.is_some() {}
        });
        Self {
            barrier_event_sender: tx,
            actor_failure_sender: failure_tx,
        }
    }
}

#[cfg(test)]
pub(crate) mod barrier_test_utils {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_pb::stream_service::streaming_control_stream_request::{
        InitRequest, PbDatabaseInitialPartialGraph, PbInitialPartialGraph,
    };
    use risingwave_pb::stream_service::{
        streaming_control_stream_request, streaming_control_stream_response, InjectBarrierRequest,
        StreamingControlStreamRequest, StreamingControlStreamResponse,
    };
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::Status;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::{ControlStreamHandle, EventSender, LocalActorOperation};
    use crate::task::{
        ActorId, LocalBarrierManager, SharedContext, TEST_DATABASE_ID, TEST_PARTIAL_GRAPH_ID,
    };

    pub(crate) struct LocalBarrierTestEnv {
        pub shared_context: Arc<SharedContext>,
        pub local_barrier_manager: LocalBarrierManager,
        pub(super) actor_op_tx: EventSender<LocalActorOperation>,
        pub request_tx: UnboundedSender<Result<StreamingControlStreamRequest, Status>>,
        pub response_rx: UnboundedReceiver<Result<StreamingControlStreamResponse, Status>>,
    }

    impl LocalBarrierTestEnv {
        pub(crate) async fn for_test() -> Self {
            let actor_op_tx = LocalBarrierManager::spawn_for_test();

            let (request_tx, request_rx) = unbounded_channel();
            let (response_tx, mut response_rx) = unbounded_channel();

            actor_op_tx.send_event(LocalActorOperation::NewControlStream {
                handle: ControlStreamHandle::new(
                    response_tx,
                    UnboundedReceiverStream::new(request_rx).boxed(),
                ),
                init_request: InitRequest {
                    databases: vec![PbDatabaseInitialPartialGraph {
                        database_id: TEST_DATABASE_ID.database_id,
                        graphs: vec![PbInitialPartialGraph {
                            partial_graph_id: TEST_PARTIAL_GRAPH_ID.into(),
                            subscriptions: vec![],
                        }],
                    }],
                },
            });

            assert_matches!(
                response_rx.recv().await.unwrap().unwrap().response.unwrap(),
                streaming_control_stream_response::Response::Init(_)
            );

            let (shared_context, local_barrier_manager) = actor_op_tx
                .send_and_await(LocalActorOperation::GetCurrentSharedContext)
                .await
                .unwrap();

            Self {
                shared_context,
                local_barrier_manager,
                actor_op_tx,
                request_tx,
                response_rx,
            }
        }

        pub(crate) fn inject_barrier(
            &self,
            barrier: &Barrier,
            actor_to_collect: impl IntoIterator<Item = ActorId>,
        ) {
            self.request_tx
                .send(Ok(StreamingControlStreamRequest {
                    request: Some(streaming_control_stream_request::Request::InjectBarrier(
                        InjectBarrierRequest {
                            request_id: "".to_string(),
                            barrier: Some(barrier.to_protobuf()),
                            database_id: TEST_DATABASE_ID.database_id,
                            actor_ids_to_collect: actor_to_collect.into_iter().collect(),
                            table_ids_to_sync: vec![],
                            partial_graph_id: TEST_PARTIAL_GRAPH_ID.into(),
                            broadcast_info: vec![],
                            actors_to_build: vec![],
                            subscriptions_to_add: vec![],
                            subscriptions_to_remove: vec![],
                        },
                    )),
                }))
                .unwrap();
        }

        pub(crate) async fn flush_all_events(&self) {
            Self::flush_all_events_impl(&self.actor_op_tx).await
        }

        pub(super) async fn flush_all_events_impl(actor_op_tx: &EventSender<LocalActorOperation>) {
            let (tx, rx) = oneshot::channel();
            actor_op_tx.send_event(LocalActorOperation::Flush(tx));
            rx.await.unwrap()
        }
    }
}
