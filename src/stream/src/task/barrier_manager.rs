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
use std::pin::pin;
use std::sync::Arc;

use anyhow::anyhow;
use futures::future::{select, Either};
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use self::managed_state::ManagedBarrierState;
use crate::error::{IntoUnexpectedExit, StreamError, StreamResult};
use crate::task::ActorId;

mod managed_state;
mod progress;
#[cfg(test)]
mod tests;

pub use progress::CreateMviewProgress;
use risingwave_common::util::drop_either_future;
use risingwave_storage::store::SyncResult;
use risingwave_storage::StateStoreImpl;

use crate::executor::monitor::StreamingMetrics;
use crate::executor::Barrier;
use crate::task::barrier_manager::progress::BackfillState;
use crate::task::barrier_manager::LocalBarrierEvent::{ReportActorCollected, ReportActorFailure};

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

/// Collect result of some barrier on current compute node. Will be reported to the meta service.
#[derive(Debug)]
pub struct BarrierCompleteResult {
    pub sync_result: SyncResult,

    /// The updated creation progress of materialized view after this barrier.
    pub create_mview_progress: Vec<PbCreateMviewProgress>,
}

enum LocalBarrierEvent {
    RegisterSender {
        actor_id: ActorId,
        sender: UnboundedSender<Barrier>,
    },
    InjectBarrier {
        barrier: Barrier,
        actor_ids_to_send: HashSet<ActorId>,
        actor_ids_to_collect: HashSet<ActorId>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    Reset,
    ReportActorCollected {
        actor_id: ActorId,
        barrier: Barrier,
    },
    ReportActorFailure {
        actor_id: ActorId,
        err: StreamError,
    },
    AwaitEpochCompleted {
        epoch: u64,
        result_sender: oneshot::Sender<StreamResult<BarrierCompleteResult>>,
    },
    ReportCreateProgress {
        current_epoch: u64,
        actor: ActorId,
        state: BackfillState,
    },
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
}

/// [`LocalBarrierWorker`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierWorker`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
struct LocalBarrierWorker {
    /// Stores all streaming job source sender.
    barrier_senders: HashMap<ActorId, Vec<UnboundedSender<Barrier>>>,

    /// Current barrier collection state.
    state: ManagedBarrierState,

    /// Record all unexpected exited actors.
    failure_actors: HashMap<ActorId, StreamError>,

    epoch_result_sender: HashMap<u64, oneshot::Sender<StreamResult<BarrierCompleteResult>>>,
}

impl LocalBarrierWorker {
    fn new(state_store: StateStoreImpl, streaming_metrics: Arc<StreamingMetrics>) -> Self {
        Self {
            barrier_senders: HashMap::new(),
            failure_actors: HashMap::default(),
            state: ManagedBarrierState::new(state_store, streaming_metrics),
            epoch_result_sender: HashMap::default(),
        }
    }

    async fn run(mut self, mut event_rx: UnboundedReceiver<LocalBarrierEvent>) {
        loop {
            let item = drop_either_future(
                select(
                    pin!(self.state.next_completed_epoch()),
                    pin!(event_rx.recv()),
                )
                .await,
            );
            match item {
                Either::Left(epoch) => {
                    self.on_epoch_completed(epoch);
                }
                Either::Right(Some(event)) => match event {
                    LocalBarrierEvent::RegisterSender { actor_id, sender } => {
                        self.register_sender(actor_id, sender);
                    }
                    LocalBarrierEvent::InjectBarrier {
                        barrier,
                        actor_ids_to_send,
                        actor_ids_to_collect,
                        result_sender,
                    } => {
                        let result =
                            self.send_barrier(&barrier, actor_ids_to_send, actor_ids_to_collect);
                        let _ = result_sender.send(result).inspect_err(|e| {
                            warn!(err=?e, "fail to send inject barrier result");
                        });
                    }
                    LocalBarrierEvent::Reset => {
                        self.reset();
                    }
                    ReportActorCollected { actor_id, barrier } => self.collect(actor_id, &barrier),
                    ReportActorFailure { actor_id, err } => {
                        self.notify_failure(actor_id, err);
                    }
                    LocalBarrierEvent::AwaitEpochCompleted {
                        epoch,
                        result_sender,
                    } => {
                        self.await_epoch_completed(epoch, result_sender);
                    }
                    LocalBarrierEvent::ReportCreateProgress {
                        current_epoch,
                        actor,
                        state,
                    } => {
                        self.update_create_mview_progress(current_epoch, actor, state);
                    }
                    #[cfg(test)]
                    LocalBarrierEvent::Flush(sender) => sender.send(()).unwrap(),
                },
                Either::Right(None) => {
                    break;
                }
            }
        }
    }
}

// event handler
impl LocalBarrierWorker {
    fn on_epoch_completed(&mut self, epoch: u64) {
        if let Some(sender) = self.epoch_result_sender.remove(&epoch) {
            let result = self
                .state
                .pop_completed_epoch(epoch)
                .expect("should exist")
                .expect("should have completed");
            if sender.send(result).is_err() {
                warn!(epoch, "fail to send epoch complete result");
            }
        }
    }

    /// Register sender for source actors, used to send barriers.
    fn register_sender(&mut self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        tracing::debug!(
            target: "events::stream::barrier::manager",
            actor_id = actor_id,
            "register sender"
        );
        self.barrier_senders
            .entry(actor_id)
            .or_default()
            .push(sender);
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    fn send_barrier(
        &mut self,
        barrier: &Barrier,
        to_send: HashSet<ActorId>,
        to_collect: HashSet<ActorId>,
    ) -> StreamResult<()> {
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
                return Err(e.clone());
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

    /// Use `prev_epoch` to remove collect rx and return rx.
    fn await_epoch_completed(
        &mut self,
        prev_epoch: u64,
        result_sender: oneshot::Sender<StreamResult<BarrierCompleteResult>>,
    ) {
        match self.state.pop_completed_epoch(prev_epoch) {
            Err(e) => {
                let _ = result_sender.send(Err(e));
            }
            Ok(Some(result)) => {
                if result_sender.send(result).is_err() {
                    warn!(prev_epoch, "failed to send completed epoch result");
                }
            }
            Ok(None) => {
                if let Some(prev_sender) =
                    self.epoch_result_sender.insert(prev_epoch, result_sender)
                {
                    warn!(?prev_epoch, "duplicate await_collect_barrier on epoch");
                    let _ = prev_sender.send(Err(anyhow!(
                        "duplicate await_collect_barrier on epoch {}",
                        prev_epoch
                    )
                    .into()));
                }
            }
        }
    }

    /// Reset all internal states.
    fn reset(&mut self) {
        *self = Self::new(
            self.state.state_store.clone(),
            self.state.streaming_metrics.clone(),
        );
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        self.state.collect(actor_id, barrier)
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    fn notify_failure(&mut self, actor_id: ActorId, err: StreamError) {
        let err = err.into_unexpected_exit(actor_id);
        if let Some(prev_err) = self.failure_actors.insert(actor_id, err.clone()) {
            warn!(?actor_id, prev_err = %prev_err.as_report(), "actor error overwritten");
        }
        for fail_epoch in self.state.epochs_await_on_actor(actor_id) {
            if let Some(result_sender) = self.epoch_result_sender.remove(&fail_epoch) {
                if result_sender.send(Err(err.clone())).is_err() {
                    warn!(?fail_epoch, ?actor_id, err = ?err.as_report(), "fail to notify actor failure");
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct LocalBarrierManager {
    barrier_event_sender: UnboundedSender<LocalBarrierEvent>,
}

impl LocalBarrierManager {
    /// Create a [`LocalBarrierWorker`] with managed mode.
    pub fn new(state_store: StateStoreImpl, streaming_metrics: Arc<StreamingMetrics>) -> Self {
        let (tx, rx) = unbounded_channel();
        let worker = LocalBarrierWorker::new(state_store, streaming_metrics);
        let _join_handle = tokio::spawn(worker.run(rx));
        Self {
            barrier_event_sender: tx,
        }
    }

    fn send_event(&self, event: LocalBarrierEvent) {
        self.barrier_event_sender
            .send(event)
            .expect("should be able to send event")
    }
}

impl LocalBarrierManager {
    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        self.send_event(LocalBarrierEvent::RegisterSender { actor_id, sender });
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    pub async fn send_barrier(
        &self,
        barrier: Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> StreamResult<()> {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::InjectBarrier {
            barrier,
            actor_ids_to_send: actor_ids_to_send.into_iter().collect(),
            actor_ids_to_collect: actor_ids_to_collect.into_iter().collect(),
            result_sender: tx,
        });
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset"))?
    }

    /// Use `prev_epoch` to remove collect rx and return rx.
    pub async fn await_epoch_completed(
        &self,
        prev_epoch: u64,
    ) -> StreamResult<BarrierCompleteResult> {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::AwaitEpochCompleted {
            epoch: prev_epoch,
            result_sender: tx,
        });
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset"))?
    }

    /// Reset all internal states.
    pub fn reset(&self) {
        self.send_event(LocalBarrierEvent::Reset)
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&self, actor_id: ActorId, barrier: &Barrier) {
        self.send_event(ReportActorCollected {
            actor_id,
            barrier: barrier.clone(),
        })
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    pub fn notify_failure(&self, actor_id: ActorId, err: StreamError) {
        self.send_event(ReportActorFailure { actor_id, err })
    }
}

#[cfg(test)]
impl LocalBarrierManager {
    pub fn for_test() -> Self {
        Self::new(
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
        )
    }

    pub async fn flush_all_events(&self) {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::Flush(tx));
        rx.await.unwrap()
    }
}
