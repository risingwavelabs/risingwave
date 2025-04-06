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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::future::{pending, poll_fn};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesOrdered};
use futures::{FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::error::tonic::extra::{Score, ScoredError};
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::{
    PbCreateMviewProgress, PbLocalSstableInfo,
};
use risingwave_rpc_client::error::{ToTonicStatus, TonicStatusWrapper};
use risingwave_storage::store_impl::AsHummock;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::{Code, Status};
use tracing::warn;

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
    DatabaseInitialPartialGraph, InitRequest, Request, ResetDatabaseRequest,
};
use risingwave_pb::stream_service::streaming_control_stream_response::{
    InitResponse, ReportDatabaseFailureResponse, ResetDatabaseResponse, Response, ShutdownResponse,
};
use risingwave_pb::stream_service::{
    BarrierCompleteResponse, InjectBarrierRequest, PbScoredError, StreamingControlStreamRequest,
    StreamingControlStreamResponse, streaming_control_stream_response,
};

use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Barrier, BarrierInner, StreamExecutorError};
use crate::task::barrier_manager::managed_state::{
    DatabaseManagedBarrierState, DatabaseStatus, ManagedBarrierStateDebugInfo,
    ManagedBarrierStateEvent, PartialGraphManagedBarrierState, ResetDatabaseOutput,
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

    pub(super) fn ack_reset_database(
        &mut self,
        database_id: DatabaseId,
        root_err: Option<ScoredStreamError>,
        reset_request_id: u32,
    ) {
        self.send_response(Response::ResetDatabase(ResetDatabaseResponse {
            database_id: database_id.database_id,
            root_err: root_err.map(|err| PbScoredError {
                err_msg: err.error.to_report_string(),
                score: err.score.0,
            }),
            reset_request_id,
        }));
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
        term_id: String,
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
    managed_barrier_state: HashMap<DatabaseId, (String, Option<ManagedBarrierStateDebugInfo<'a>>)>,
    has_control_stream_connected: bool,
}

impl Display for LocalBarrierWorkerDebugInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "\nhas_control_stream_connected: {}",
            self.has_control_stream_connected
        )?;

        for (database_id, (status, managed_barrier_state)) in &self.managed_barrier_state {
            writeln!(
                f,
                "database {} status: {} managed_barrier_state:\n{}",
                database_id.database_id,
                status,
                managed_barrier_state
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
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

    pub(super) term_id: String,
}

impl LocalBarrierWorker {
    pub(super) fn new(
        actor_manager: Arc<StreamActorManager>,
        initial_partial_graphs: Vec<DatabaseInitialPartialGraph>,
        term_id: String,
    ) -> Self {
        let state = ManagedBarrierState::new(
            actor_manager.clone(),
            initial_partial_graphs,
            term_id.clone(),
        );
        Self {
            state,
            await_epoch_completed_futures: Default::default(),
            control_stream_handle: ControlStreamHandle::empty(),
            actor_manager,
            term_id,
        }
    }

    fn to_debug_info(&self) -> LocalBarrierWorkerDebugInfo<'_> {
        LocalBarrierWorkerDebugInfo {
            managed_barrier_state: self
                .state
                .databases
                .iter()
                .map(|(database_id, status)| {
                    (*database_id, {
                        match status {
                            DatabaseStatus::ReceivedExchangeRequest(_) => {
                                ("ReceivedExchangeRequest".to_owned(), None)
                            }
                            DatabaseStatus::Running(state) => {
                                ("running".to_owned(), Some(state.to_debug_info()))
                            }
                            DatabaseStatus::Suspended(state) => {
                                (format!("suspended: {:?}", state.suspend_time), None)
                            }
                            DatabaseStatus::Resetting(_) => ("resetting".to_owned(), None),
                            DatabaseStatus::Unspecified => {
                                unreachable!()
                            }
                        }
                    })
                })
                .collect(),
            has_control_stream_connected: self.control_stream_handle.connected(),
        }
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
                            self.on_database_failure(database_id, Some(actor_id), err, "recv actor failure");
                        }
                        ManagedBarrierStateEvent::DatabaseReset(output, reset_request_id) => {
                            self.ack_database_reset(database_id, Some(output), reset_request_id);
                        }
                    }
                }
                (database_id, partial_graph_id, barrier, result) = Self::next_completed_epoch(&mut self.await_epoch_completed_futures) => {
                    match result {
                        Ok(result) => {
                            self.on_epoch_completed(database_id, partial_graph_id, barrier.epoch.prev, result);
                        }
                        Err(err) => {
                            // TODO: may only report as database failure instead of reset the stream
                            // when the HummockUploader support partial recovery. Currently the HummockUploader
                            // enter `Err` state and stop working until a global recovery to clear the uploader.
                            self.control_stream_handle.reset_stream_with_err(Status::internal(format!("failed to complete epoch: {} {} {:?} {:?}", database_id, partial_graph_id.0, barrier.epoch, err.as_report())));
                        }
                    }
                },
                actor_op = actor_op_rx.recv() => {
                    if let Some(actor_op) = actor_op {
                        match actor_op {
                            LocalActorOperation::NewControlStream { handle, init_request  } => {
                                self.control_stream_handle.reset_stream_with_err(Status::internal("control stream has been reset to a new one"));
                                self.reset(init_request).await;
                                self.control_stream_handle = handle;
                                self.control_stream_handle.send_response(streaming_control_stream_response::Response::Init(InitResponse {}));
                            }
                            LocalActorOperation::Shutdown { result_sender } => {
                                if self.state.databases.values().any(|database| {
                                    match database {
                                        DatabaseStatus::Running(database) => {
                                            !database.actor_states.is_empty()
                                        }
                                        DatabaseStatus::Suspended(_) | DatabaseStatus::Resetting(_) |
                                            DatabaseStatus::ReceivedExchangeRequest(_) => {
                                            false
                                        }
                                        DatabaseStatus::Unspecified => {
                                            unreachable!()
                                        }
                                    }
                                }) {
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
                    if let Err((database_id, err)) = result {
                        self.on_database_failure(database_id, None, err, "failed to inject barrier");
                    }
                },
            }
        }
    }

    fn handle_streaming_control_request(
        &mut self,
        request: Request,
    ) -> Result<(), (DatabaseId, StreamError)> {
        match request {
            Request::InjectBarrier(req) => {
                let database_id = DatabaseId::new(req.database_id);
                let result: StreamResult<()> = try {
                    let barrier = Barrier::from_protobuf(req.get_barrier().unwrap())?;
                    self.send_barrier(&barrier, req)?;
                };
                result.map_err(|e| (database_id, e))?;
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
            Request::ResetDatabase(req) => {
                self.reset_database(req);
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
                term_id,
                ids,
                result_sender,
            } => {
                let result = try {
                    if self.term_id != term_id {
                        warn!(
                            ?ids,
                            term_id,
                            current_term_id = self.term_id,
                            "take receiver on unmatched term_id"
                        );
                        Err(anyhow!(
                            "take receiver {:?} on unmatched term_id {} to current term_id {}",
                            ids,
                            term_id,
                            self.term_id
                        ))?;
                    }
                    let result = match self.state.databases.entry(database_id) {
                        Entry::Occupied(mut entry) => match entry.get_mut() {
                            DatabaseStatus::ReceivedExchangeRequest(pending_requests) => {
                                pending_requests.push((ids, result_sender));
                                return;
                            }
                            DatabaseStatus::Running(database) => database
                                .local_barrier_manager
                                .shared_context
                                .take_receiver(ids),
                            DatabaseStatus::Suspended(_) => {
                                Err(anyhow!("database suspended").into())
                            }
                            DatabaseStatus::Resetting(_) => {
                                Err(anyhow!("database resetting").into())
                            }
                            DatabaseStatus::Unspecified => {
                                unreachable!()
                            }
                        },
                        Entry::Vacant(entery) => {
                            entery.insert(DatabaseStatus::ReceivedExchangeRequest(vec![(
                                ids,
                                result_sender,
                            )]));
                            return;
                        }
                    };
                    result?
                };
                let _ = result_sender.send(result);
            }
            #[cfg(test)]
            LocalActorOperation::GetCurrentSharedContext(sender) => {
                let database_status = self
                    .state
                    .databases
                    .get(&crate::task::TEST_DATABASE_ID)
                    .unwrap();
                let database_state = risingwave_common::must_match!(database_status, DatabaseStatus::Running(database_state) => database_state);
                let _ = sender.send((
                    database_state.local_barrier_manager.shared_context.clone(),
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
                        ManagedBarrierStateEvent::ActorError { .. }
                        | ManagedBarrierStateEvent::DatabaseReset(..) => {
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

    use futures::FutureExt;
    use futures::future::BoxFuture;
    use risingwave_hummock_sdk::SyncResult;
    use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;

    use crate::error::StreamResult;
    use crate::executor::Barrier;
    use crate::task::{BarrierCompleteResult, PartialGraphId, await_tree_key};

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
use risingwave_storage::{StateStoreImpl, dispatch_state_store};

fn sync_epoch(
    state_store: &StateStoreImpl,
    streaming_metrics: &StreamingMetrics,
    prev_epoch: u64,
    table_ids: HashSet<TableId>,
) -> BoxFuture<'static, StreamResult<SyncResult>> {
    let timer = streaming_metrics.barrier_sync_latency.start_timer();

    let state_store = state_store.clone();
    let future = async move {
        dispatch_state_store!(state_store, hummock, {
            hummock.sync(vec![(prev_epoch, table_ids)]).await
        })
    };

    future
        .instrument_await(await_tree::span!("sync_epoch (epoch {})", prev_epoch))
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
            let Some(database_state) = self
                .state
                .databases
                .get_mut(&database_id)
                .expect("should exist")
                .state_for_request()
            else {
                return;
            };
            let (barrier, table_ids, create_mview_progress) =
                database_state.pop_barrier_to_complete(partial_graph_id, prev_epoch);

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
                        request_id: "todo".to_owned(),
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
    /// the root cause of the failure. The caller should then call `try_find_root_failure`
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

        let database_status = self
            .state
            .databases
            .get_mut(&DatabaseId::new(request.database_id))
            .expect("should exist");
        if let Some(state) = database_status.state_for_request() {
            state
                .local_barrier_manager
                .shared_context
                .add_actors(request.broadcast_info.iter().cloned());
            state.transform_to_issued(barrier, request)?;
        }
        Ok(())
    }

    fn remove_partial_graphs(
        &mut self,
        database_id: DatabaseId,
        partial_graph_ids: impl Iterator<Item = PartialGraphId>,
    ) {
        let Some(database_status) = self.state.databases.get_mut(&database_id) else {
            warn!(
                database_id = database_id.database_id,
                "database to remove partial graph not exist"
            );
            return;
        };
        let Some(database_state) = database_status.state_for_request() else {
            warn!(
                database_id = database_id.database_id,
                "ignore remove partial graph request on err database",
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

    fn add_partial_graph(&mut self, database_id: DatabaseId, partial_graph_id: PartialGraphId) {
        let status = match self.state.databases.entry(database_id) {
            Entry::Occupied(entry) => {
                let status = entry.into_mut();
                if let DatabaseStatus::ReceivedExchangeRequest(pending_requests) = status {
                    let database = DatabaseManagedBarrierState::new(
                        database_id,
                        self.term_id.clone(),
                        self.actor_manager.clone(),
                        vec![],
                    );
                    for (ids, result_sender) in pending_requests.drain(..) {
                        let result = database
                            .local_barrier_manager
                            .shared_context
                            .take_receiver(ids);
                        let _ = result_sender.send(result);
                    }
                    *status = DatabaseStatus::Running(database);
                }

                status
            }
            Entry::Vacant(entry) => {
                entry.insert(DatabaseStatus::Running(DatabaseManagedBarrierState::new(
                    database_id,
                    self.term_id.clone(),
                    self.actor_manager.clone(),
                    vec![],
                )))
            }
        };
        if let Some(state) = status.state_for_request() {
            assert!(
                state
                    .graph_states
                    .insert(
                        partial_graph_id,
                        PartialGraphManagedBarrierState::new(&self.actor_manager)
                    )
                    .is_none()
            );
        }
    }

    fn reset_database(&mut self, req: ResetDatabaseRequest) {
        let database_id = DatabaseId::new(req.database_id);
        if let Some(database_status) = self.state.databases.get_mut(&database_id) {
            database_status.start_reset(
                database_id,
                self.await_epoch_completed_futures.remove(&database_id),
                req.reset_request_id,
            );
        } else {
            self.ack_database_reset(database_id, None, req.reset_request_id);
        }
    }

    fn ack_database_reset(
        &mut self,
        database_id: DatabaseId,
        reset_output: Option<ResetDatabaseOutput>,
        reset_request_id: u32,
    ) {
        info!(
            database_id = database_id.database_id,
            "database reset successfully"
        );
        if let Some(reset_database) = self.state.databases.remove(&database_id) {
            match reset_database {
                DatabaseStatus::Resetting(_) => {}
                _ => {
                    unreachable!("must be resetting previously")
                }
            }
        }
        self.await_epoch_completed_futures.remove(&database_id);
        self.control_stream_handle.ack_reset_database(
            database_id,
            reset_output.and_then(|output| output.root_err),
            reset_request_id,
        );
    }

    /// When some other failure happens (like failed to send barrier), the error is reported using
    /// this function. The control stream will be responded with a message to notify about the error,
    /// and the global barrier worker will later reset and rerun the database.
    fn on_database_failure(
        &mut self,
        database_id: DatabaseId,
        failed_actor: Option<ActorId>,
        err: StreamError,
        message: impl Into<String>,
    ) {
        let message = message.into();
        error!(database_id = database_id.database_id, ?failed_actor, message, err = ?err.as_report(), "suspend database on error");
        let completing_futures = self.await_epoch_completed_futures.remove(&database_id);
        self.state
            .databases
            .get_mut(&database_id)
            .expect("should exist")
            .suspend(failed_actor, err, completing_futures);
        self.control_stream_handle
            .send_response(Response::ReportDatabaseFailure(
                ReportDatabaseFailureResponse {
                    database_id: database_id.database_id,
                },
            ));
    }
}

impl DatabaseManagedBarrierState {
    /// Collect actor errors for a while and find the one that might be the root cause.
    ///
    /// Returns `None` if there's no actor error received.
    async fn try_find_root_actor_failure(
        &mut self,
        first_failure: Option<(Option<ActorId>, StreamError)>,
    ) -> Option<ScoredStreamError> {
        let mut later_errs = vec![];
        // fetch more actor errors within a timeout
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            let mut uncollected_actors: HashSet<_> = self.actor_states.keys().cloned().collect();
            if let Some((Some(failed_actor), _)) = &first_failure {
                uncollected_actors.remove(failed_actor);
            }
            while !uncollected_actors.is_empty()
                && let Some((actor_id, error)) = self.actor_failure_rx.recv().await
            {
                uncollected_actors.remove(&actor_id);
                later_errs.push(error);
            }
        })
        .await;

        first_failure
            .into_iter()
            .map(|(_, err)| err)
            .chain(later_errs.into_iter())
            .map(|e| e.with_score())
            .max_by_key(|e| e.score)
    }
}

#[derive(Clone)]
pub struct LocalBarrierManager {
    barrier_event_sender: UnboundedSender<LocalBarrierEvent>,
    actor_failure_sender: UnboundedSender<(ActorId, StreamError)>,
    pub shared_context: Arc<SharedContext>,
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
        let worker = LocalBarrierWorker::new(actor_manager, vec![], "uninitialized".into());
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
    pub(super) fn new(
        shared_context: Arc<SharedContext>,
    ) -> (
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
                shared_context,
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
type ScoredStreamError = ScoredError<StreamError>;

impl StreamError {
    /// Score the given error based on hard-coded rules.
    fn with_score(self) -> ScoredStreamError {
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

        let score = Score(stream_error_score(&self));
        ScoredStreamError { error: self, score }
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
        InjectBarrierRequest, StreamingControlStreamRequest, StreamingControlStreamResponse,
        streaming_control_stream_request, streaming_control_stream_response,
    };
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
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
                            actor_infos: vec![],
                        }],
                    }],
                    term_id: "for_test".into(),
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
                            request_id: "".to_owned(),
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
