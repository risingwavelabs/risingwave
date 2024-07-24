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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_pb::stream_service::barrier_complete_response::{
    GroupedSstableInfo, PbCreateMviewProgress,
};
use risingwave_rpc_client::error::{ToTonicStatus, TonicStatusWrapper};
use rw_futures_util::{pending_on_none, AttachedFuture};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::{Code, Status};

use self::managed_state::ManagedBarrierState;
use crate::error::{IntoUnexpectedExit, StreamError, StreamResult};
use crate::task::{
    ActorHandle, ActorId, AtomicU64Ref, SharedContext, StreamEnvironment, UpDownActorIds,
};

mod managed_state;
mod progress;
#[cfg(test)]
mod tests;

pub use progress::CreateMviewProgress;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::{LocalSstableInfo, SyncResult};
use risingwave_pb::common::ActorInfo;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::streaming_control_stream_request::{InitRequest, Request};
use risingwave_pb::stream_service::streaming_control_stream_response::InitResponse;
use risingwave_pb::stream_service::{
    streaming_control_stream_response, BarrierCompleteResponse, BuildActorInfo,
    StreamingControlStreamRequest, StreamingControlStreamResponse,
};

use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Actor, Barrier, DispatchExecutor, Mutation, StreamExecutorError};
use crate::task::barrier_manager::managed_state::ManagedBarrierStateDebugInfo;
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
                warn!("failed to notify finish of control stream");
            }
        }
    }

    fn inspect_result(&mut self, result: StreamResult<()>) {
        if let Err(e) = result {
            self.reset_stream_with_err(e.to_status_unnamed(Code::Internal));
        }
    }

    fn send_response(&mut self, response: StreamingControlStreamResponse) {
        if let Some((sender, _)) = self.pair.as_ref() {
            if sender.send(Ok(response)).is_err() {
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

#[derive(Clone)]
pub struct CreateActorContext {
    #[expect(clippy::type_complexity)]
    barrier_sender: Arc<Mutex<Option<HashMap<ActorId, Vec<UnboundedSender<Barrier>>>>>>,
}

impl Default for CreateActorContext {
    fn default() -> Self {
        Self {
            barrier_sender: Arc::new(Mutex::new(Some(HashMap::new()))),
        }
    }
}

impl CreateActorContext {
    pub fn register_sender(&self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        self.barrier_sender
            .lock()
            .as_mut()
            .expect("should not register after collecting sender")
            .entry(actor_id)
            .or_default()
            .push(sender)
    }

    pub(super) fn collect_senders(&self) -> HashMap<ActorId, Vec<UnboundedSender<Barrier>>> {
        self.barrier_sender
            .lock()
            .take()
            .expect("should not collect senders for twice")
    }
}

pub(super) enum LocalBarrierEvent {
    ReportActorCollected {
        actor_id: ActorId,
        barrier: Barrier,
    },
    ReportCreateProgress {
        current_epoch: u64,
        actor: ActorId,
        state: BackfillState,
    },
    ReadBarrierMutation {
        barrier: Barrier,
        mutation_sender: oneshot::Sender<Option<Arc<Mutation>>>,
    },
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
}

pub(super) enum LocalActorOperation {
    NewControlStream {
        handle: ControlStreamHandle,
        init_request: InitRequest,
    },
    DropActors {
        actors: Vec<ActorId>,
        result_sender: oneshot::Sender<()>,
    },
    UpdateActors {
        actors: Vec<BuildActorInfo>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    BuildActors {
        actors: Vec<ActorId>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    UpdateActorInfo {
        new_actor_infos: Vec<ActorInfo>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    TakeReceiver {
        ids: UpDownActorIds,
        result_sender: oneshot::Sender<StreamResult<Receiver>>,
    },
    #[cfg(test)]
    GetCurrentSharedContext(oneshot::Sender<Arc<SharedContext>>),
    #[cfg(test)]
    RegisterSenders {
        actor_id: ActorId,
        senders: Vec<UnboundedSender<Barrier>>,
        result_sender: oneshot::Sender<()>,
    },
    InspectState {
        result_sender: oneshot::Sender<String>,
    },
}

pub(super) struct CreateActorOutput {
    pub(super) actors: Vec<Actor<DispatchExecutor>>,
    pub(super) senders: HashMap<ActorId, Vec<UnboundedSender<Barrier>>>,
}

pub(crate) struct StreamActorManagerState {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    pub(super) handles: HashMap<ActorId, ActorHandle>,

    /// Stores all actor information, taken after actor built.
    pub(super) actors: HashMap<ActorId, BuildActorInfo>,

    /// Stores all actor tokio runtime monitoring tasks.
    pub(super) actor_monitor_tasks: HashMap<ActorId, ActorHandle>,

    #[expect(clippy::type_complexity)]
    pub(super) creating_actors: FuturesUnordered<
        AttachedFuture<
            JoinHandle<StreamResult<CreateActorOutput>>,
            (BTreeSet<ActorId>, oneshot::Sender<StreamResult<()>>),
        >,
    >,
}

impl StreamActorManagerState {
    fn new() -> Self {
        Self {
            handles: HashMap::new(),
            actors: HashMap::new(),
            actor_monitor_tasks: HashMap::new(),
            creating_actors: FuturesUnordered::new(),
        }
    }

    async fn next_created_actors(
        &mut self,
    ) -> (
        oneshot::Sender<StreamResult<()>>,
        StreamResult<CreateActorOutput>,
    ) {
        let (join_result, (_, sender)) = pending_on_none(self.creating_actors.next()).await;
        (
            sender,
            try { join_result.context("failed to join creating actors futures")?? },
        )
    }
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

#[derive(Debug)]
#[expect(dead_code)]
pub(super) struct LocalBarrierWorkerDebugInfo<'a> {
    actor_to_send: BTreeSet<ActorId>,
    running_actors: BTreeSet<ActorId>,
    creating_actors: Vec<BTreeSet<ActorId>>,
    managed_barrier_state: ManagedBarrierStateDebugInfo<'a>,
    has_control_stream_connected: bool,
}

/// [`LocalBarrierWorker`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierWorker`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub(super) struct LocalBarrierWorker {
    /// Stores all streaming job source sender.
    barrier_senders: HashMap<ActorId, Vec<UnboundedSender<Barrier>>>,

    /// Current barrier collection state.
    state: ManagedBarrierState,

    /// Record all unexpected exited actors.
    failure_actors: HashMap<ActorId, StreamError>,

    control_stream_handle: ControlStreamHandle,

    pub(super) actor_manager: Arc<StreamActorManager>,

    pub(super) actor_manager_state: StreamActorManagerState,

    pub(super) current_shared_context: Arc<SharedContext>,

    barrier_event_rx: UnboundedReceiver<LocalBarrierEvent>,

    actor_failure_rx: UnboundedReceiver<(ActorId, StreamError)>,

    root_failure: Option<StreamError>,
}

impl LocalBarrierWorker {
    pub(super) fn new(actor_manager: Arc<StreamActorManager>) -> Self {
        let (event_tx, event_rx) = unbounded_channel();
        let (failure_tx, failure_rx) = unbounded_channel();
        let shared_context = Arc::new(SharedContext::new(
            &actor_manager.env,
            LocalBarrierManager {
                barrier_event_sender: event_tx,
                actor_failure_sender: failure_tx,
            },
        ));
        Self {
            barrier_senders: HashMap::new(),
            failure_actors: HashMap::default(),
            state: ManagedBarrierState::new(
                actor_manager.env.state_store(),
                actor_manager.streaming_metrics.clone(),
                actor_manager.await_tree_reg.clone(),
            ),
            control_stream_handle: ControlStreamHandle::empty(),
            actor_manager,
            actor_manager_state: StreamActorManagerState::new(),
            current_shared_context: shared_context,
            barrier_event_rx: event_rx,
            actor_failure_rx: failure_rx,
            root_failure: None,
        }
    }

    fn to_debug_info(&self) -> LocalBarrierWorkerDebugInfo<'_> {
        LocalBarrierWorkerDebugInfo {
            actor_to_send: self.barrier_senders.keys().cloned().collect(),
            running_actors: self.actor_manager_state.handles.keys().cloned().collect(),
            creating_actors: self
                .actor_manager_state
                .creating_actors
                .iter()
                .map(|fut| fut.item().0.clone())
                .collect(),
            managed_barrier_state: self.state.to_debug_info(),
            has_control_stream_connected: self.control_stream_handle.connected(),
        }
    }

    async fn run(mut self, mut actor_op_rx: UnboundedReceiver<LocalActorOperation>) {
        loop {
            select! {
                biased;
                (sender, create_actors_result) = self.actor_manager_state.next_created_actors() => {
                    self.handle_actor_created(sender, create_actors_result);
                }
                completed_epoch = self.state.next_completed_epoch() => {
                    let result = self.on_epoch_completed(completed_epoch);
                    self.control_stream_handle.inspect_result(result);
                },
                event = self.barrier_event_rx.recv() => {
                    self.handle_barrier_event(event.expect("should not be none"));
                },
                failure = self.actor_failure_rx.recv() => {
                    let (actor_id, err) = failure.unwrap();
                    self.notify_failure(actor_id, err).await;
                },
                actor_op = actor_op_rx.recv() => {
                    if let Some(actor_op) = actor_op {
                        match actor_op {
                            LocalActorOperation::NewControlStream { handle, init_request  } => {
                                self.control_stream_handle.reset_stream_with_err(Status::internal("control stream has been reset to a new one"));
                                self.reset(init_request.prev_epoch).await;
                                self.control_stream_handle = handle;
                                self.control_stream_handle.send_response(StreamingControlStreamResponse {
                                    response: Some(streaming_control_stream_response::Response::Init(InitResponse {}))
                                });
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
                    let result = self.handle_streaming_control_request(request);
                    self.control_stream_handle.inspect_result(result);
                },
            }
        }
    }

    fn handle_actor_created(
        &mut self,
        sender: oneshot::Sender<StreamResult<()>>,
        create_actor_result: StreamResult<CreateActorOutput>,
    ) {
        let result = create_actor_result.map(|output| {
            for (actor_id, senders) in output.senders {
                self.register_sender(actor_id, senders);
            }
            self.spawn_actors(output.actors);
        });

        let _ = sender.send(result);
    }

    fn handle_streaming_control_request(
        &mut self,
        request: StreamingControlStreamRequest,
    ) -> StreamResult<()> {
        match request.request.expect("should not be empty") {
            Request::InjectBarrier(req) => {
                let barrier = Barrier::from_protobuf(req.get_barrier().unwrap())?;
                self.send_barrier(
                    &barrier,
                    req.actor_ids_to_send.into_iter().collect(),
                    req.actor_ids_to_collect.into_iter().collect(),
                )?;
                Ok(())
            }
            Request::Init(_) => {
                unreachable!()
            }
        }
    }

    fn handle_barrier_event(&mut self, event: LocalBarrierEvent) {
        match event {
            LocalBarrierEvent::ReportActorCollected { actor_id, barrier } => {
                self.collect(actor_id, &barrier)
            }
            LocalBarrierEvent::ReportCreateProgress {
                current_epoch,
                actor,
                state,
            } => {
                self.update_create_mview_progress(current_epoch, actor, state);
            }
            LocalBarrierEvent::ReadBarrierMutation {
                barrier,
                mutation_sender,
            } => {
                self.read_barrier_mutation(barrier, mutation_sender);
            }
            #[cfg(test)]
            LocalBarrierEvent::Flush(sender) => sender.send(()).unwrap(),
        }
    }

    fn handle_actor_op(&mut self, actor_op: LocalActorOperation) {
        match actor_op {
            LocalActorOperation::NewControlStream { .. } => {
                unreachable!("NewControlStream event should be handled separately in async context")
            }
            LocalActorOperation::DropActors {
                actors,
                result_sender,
            } => {
                self.drop_actors(&actors);
                let _ = result_sender.send(());
            }
            LocalActorOperation::UpdateActors {
                actors,
                result_sender,
            } => {
                let result = self.update_actors(actors);
                let _ = result_sender.send(result);
            }
            LocalActorOperation::BuildActors {
                actors,
                result_sender,
            } => self.start_create_actors(&actors, result_sender),
            LocalActorOperation::UpdateActorInfo {
                new_actor_infos,
                result_sender,
            } => {
                let _ = result_sender.send(self.update_actor_info(new_actor_infos));
            }
            LocalActorOperation::TakeReceiver { ids, result_sender } => {
                let _ = result_sender.send(self.current_shared_context.take_receiver(ids));
            }
            #[cfg(test)]
            LocalActorOperation::GetCurrentSharedContext(sender) => {
                let _ = sender.send(self.current_shared_context.clone());
            }
            #[cfg(test)]
            LocalActorOperation::RegisterSenders {
                actor_id,
                senders,
                result_sender,
            } => {
                self.register_sender(actor_id, senders);
                let _ = result_sender.send(());
            }
            LocalActorOperation::InspectState { result_sender } => {
                let _ = result_sender.send(format!("{:#?}", self.to_debug_info()));
            }
        }
    }
}

// event handler
impl LocalBarrierWorker {
    fn on_epoch_completed(&mut self, epoch: u64) -> StreamResult<()> {
        let result = self
            .state
            .pop_completed_epoch(epoch)
            .expect("should exist")
            .expect("should have completed")?;

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

        let result = StreamingControlStreamResponse {
            response: Some(
                streaming_control_stream_response::Response::CompleteBarrier(
                    BarrierCompleteResponse {
                        request_id: "todo".to_string(),
                        status: None,
                        create_mview_progress,
                        synced_sstables: synced_sstables
                            .into_iter()
                            .map(
                                |LocalSstableInfo {
                                     compaction_group_id,
                                     sst_info,
                                     table_stats,
                                 }| GroupedSstableInfo {
                                    compaction_group_id,
                                    sst: Some(sst_info),
                                    table_stats_map: to_prost_table_stats_map(table_stats),
                                },
                            )
                            .collect_vec(),
                        worker_id: self.actor_manager.env.worker_id(),
                        table_watermarks: table_watermarks
                            .into_iter()
                            .map(|(key, value)| (key.table_id, value.to_protobuf()))
                            .collect(),
                        old_value_sstables: old_value_ssts
                            .into_iter()
                            .map(|sst| sst.sst_info)
                            .collect(),
                    },
                ),
            ),
        };

        self.control_stream_handle.send_response(result);
        Ok(())
    }

    /// Read mutation from barrier state.
    fn read_barrier_mutation(
        &mut self,
        barrier: Barrier,
        sender: oneshot::Sender<Option<Arc<Mutation>>>,
    ) {
        self.state.read_barrier_mutation(&barrier, sender);
    }

    /// Register sender for source actors, used to send barriers.
    fn register_sender(&mut self, actor_id: ActorId, senders: Vec<UnboundedSender<Barrier>>) {
        tracing::debug!(
            target: "events::stream::barrier::manager",
            actor_id = actor_id,
            "register sender"
        );
        self.barrier_senders
            .entry(actor_id)
            .or_default()
            .extend(senders);
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    fn send_barrier(
        &mut self,
        barrier: &Barrier,
        to_send: HashSet<ActorId>,
        to_collect: HashSet<ActorId>,
    ) -> StreamResult<()> {
        #[cfg(not(test))]
        {
            // The barrier might be outdated and been injected after recovery in some certain extreme
            // scenarios. So some newly creating actors in the barrier are possibly not rebuilt during
            // recovery. Check it here and return an error here if some actors are not found to
            // avoid collection hang. We need some refine in meta side to remove this workaround since
            // it will cause another round of unnecessary recovery.
            let missing_actor_ids = to_collect
                .iter()
                .filter(|id| !self.actor_manager_state.handles.contains_key(id))
                .collect_vec();
            if !missing_actor_ids.is_empty() {
                tracing::warn!(
                    "to collect actors not found, they should be cleaned when recovering: {:?}",
                    missing_actor_ids
                );
                return Err(anyhow!("to collect actors not found: {:?}", to_collect).into());
            }
        }

        if barrier.kind == BarrierKind::Initial {
            self.actor_manager
                .watermark_epoch
                .store(barrier.epoch.curr, std::sync::atomic::Ordering::SeqCst);
        }
        debug!(
            target: "events::stream::barrier::manager::send",
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier,
            to_send,
            to_collect
        );

        // There must be some actors to collect from.
        assert!(!to_collect.is_empty());

        for actor_id in &to_collect {
            if let Some(e) = self.failure_actors.get(actor_id) {
                // The failure actors could exit before the barrier is issued, while their
                // up-downstream actors could be stuck somehow. Return error directly to trigger the
                // recovery.
                // try_find_root_failure is not used merely because it requires async.
                return Err(self.root_failure.clone().unwrap_or(e.clone()));
            }
        }

        self.state.transform_to_issued(barrier, to_collect);

        for actor_id in to_send {
            match self.barrier_senders.get(&actor_id) {
                Some(senders) => {
                    for sender in senders {
                        if let Err(_err) = sender.send(barrier.clone()) {
                            // return err to trigger recovery.
                            return Err(StreamError::barrier_send(
                                barrier.clone(),
                                actor_id,
                                "channel closed",
                            ));
                        }
                    }
                }
                None => {
                    return Err(StreamError::barrier_send(
                        barrier.clone(),
                        actor_id,
                        "sender not found",
                    ));
                }
            }
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(actors) = barrier.all_stop_actors() {
            debug!(
                target: "events::stream::barrier::manager",
                "remove actors {:?} from senders",
                actors
            );
            for actor in actors {
                self.barrier_senders.remove(actor);
            }
        }
        Ok(())
    }

    /// Reset all internal states.
    pub(super) fn reset_state(&mut self) {
        *self = Self::new(self.actor_manager.clone());
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        self.state.collect(actor_id, barrier)
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    async fn notify_failure(&mut self, actor_id: ActorId, err: StreamError) {
        self.add_failure(actor_id, err.clone());
        let root_err = self.try_find_root_failure().await;

        let failed_epochs = self.state.epochs_await_on_actor(actor_id).collect_vec();
        if !failed_epochs.is_empty() {
            self.control_stream_handle.reset_stream_with_err(
                anyhow!(root_err)
                    .context(format!(
                        "failed to collect barrier for epoch {:?}",
                        failed_epochs
                    ))
                    .to_status_unnamed(Code::Internal),
            );
        }
    }

    fn add_failure(&mut self, actor_id: ActorId, err: StreamError) {
        if let Some(prev_err) = self.failure_actors.insert(actor_id, err) {
            warn!(
                actor_id,
                prev_err = %prev_err.as_report(),
                "actor error overwritten"
            );
        }
    }

    async fn try_find_root_failure(&mut self) -> StreamError {
        if let Some(root_failure) = &self.root_failure {
            return root_failure.clone();
        }
        // fetch more actor errors within a timeout
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some((actor_id, error)) = self.actor_failure_rx.recv().await {
                self.add_failure(actor_id, error);
            }
        })
        .await;
        self.root_failure = try_find_root_actor_failure(self.failure_actors.values());

        self.root_failure
            .clone()
            .expect("failure actors should not be empty")
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
        let worker = LocalBarrierWorker::new(actor_manager);
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
    fn send_event(&self, event: LocalBarrierEvent) {
        // ignore error, because the current barrier manager maybe a stale one
        let _ = self.barrier_event_sender.send(event);
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&self, actor_id: ActorId, barrier: &Barrier) {
        self.send_event(LocalBarrierEvent::ReportActorCollected {
            actor_id,
            barrier: barrier.clone(),
        })
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    pub fn notify_failure(&self, actor_id: ActorId, err: StreamError) {
        let _ = self
            .actor_failure_sender
            .send((actor_id, err.into_unexpected_exit(actor_id)));
    }

    /// When a `RemoteInput` get a barrier, it should wait and read the barrier mutation from the barrier manager.
    pub async fn read_barrier_mutation(
        &self,
        barrier: &Barrier,
    ) -> StreamResult<Option<Arc<Mutation>>> {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::ReadBarrierMutation {
            barrier: barrier.clone(),
            mutation_sender: tx,
        });
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset").into())
    }
}

/// Tries to find the root cause of actor failures, based on hard-coded rules.
pub fn try_find_root_actor_failure<'a>(
    actor_errors: impl IntoIterator<Item = &'a StreamError>,
) -> Option<StreamError> {
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
            | ErrorKind::SinkError(_)
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
            | ErrorKind::Sink(_) => 1000,
        }
    }

    actor_errors
        .into_iter()
        .max_by_key(|&e| stream_error_score(e))
        .cloned()
}

#[cfg(test)]
impl LocalBarrierManager {
    pub(super) fn spawn_for_test() -> EventSender<LocalActorOperation> {
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

    pub async fn flush_all_events(&self) {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::Flush(tx));
        rx.await.unwrap()
    }
}
