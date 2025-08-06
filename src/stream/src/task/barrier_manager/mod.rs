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

pub mod progress;
pub use progress::CreateMviewProgressReporter;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::util::epoch::EpochPair;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::error::{IntoUnexpectedExit, StreamError};
use crate::executor::exchange::permit::{self, channel_from_config};
use crate::executor::{Barrier, BarrierInner};
use crate::task::barrier_manager::progress::BackfillState;
use crate::task::{ActorId, StreamEnvironment};

/// Events sent from actors via [`LocalBarrierManager`] to [`super::barrier_worker::managed_state::DatabaseManagedBarrierState`].
///
/// See [`crate::task`] for architecture overview.
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
    ReportSourceLoadFinished {
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: u32,
        associated_source_id: u32,
    },
    RefreshFinished {
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: u32,
        staging_table_id: u32,
    },
    RegisterBarrierSender {
        actor_id: ActorId,
        barrier_sender: mpsc::UnboundedSender<Barrier>,
    },
    RegisterLocalUpstreamOutput {
        actor_id: ActorId,
        upstream_actor_id: ActorId,
        tx: permit::Sender,
    },
}

/// Can send [`LocalBarrierEvent`] to [`super::barrier_worker::managed_state::DatabaseManagedBarrierState::poll_next_event`]
///
/// See [`crate::task`] for architecture overview.
#[derive(Clone)]
pub struct LocalBarrierManager {
    barrier_event_sender: UnboundedSender<LocalBarrierEvent>,
    actor_failure_sender: UnboundedSender<(ActorId, StreamError)>,
    pub(crate) database_id: DatabaseId,
    pub(crate) term_id: String,
    pub(crate) env: StreamEnvironment,
}

impl LocalBarrierManager {
    pub(super) fn new(
        database_id: DatabaseId,
        term_id: String,
        env: StreamEnvironment,
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
                database_id,
                term_id,
                env,
            },
            event_rx,
            err_rx,
        )
    }

    pub fn for_test() -> Self {
        Self::new(
            DatabaseId {
                database_id: 114514,
            },
            "114514".to_owned(),
            StreamEnvironment::for_test(),
        )
        .0
    }

    /// Event is handled by [`super::barrier_worker::managed_state::DatabaseManagedBarrierState::poll_next_event`]
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

    pub fn register_local_upstream_output(
        &self,
        actor_id: ActorId,
        upstream_actor_id: ActorId,
    ) -> permit::Receiver {
        let (tx, rx) = channel_from_config(self.env.config());
        self.send_event(LocalBarrierEvent::RegisterLocalUpstreamOutput {
            actor_id,
            upstream_actor_id,
            tx,
        });
        rx
    }

    pub fn report_source_load_finished(
        &self,
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: u32,
        associated_source_id: u32,
    ) {
        self.send_event(LocalBarrierEvent::ReportSourceLoadFinished {
            epoch,
            actor_id,
            table_id,
            associated_source_id,
        });
    }

    pub fn report_refresh_finished(
        &self,
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: u32,
        staging_table_id: u32,
    ) {
        self.send_event(LocalBarrierEvent::RefreshFinished {
            epoch,
            actor_id,
            table_id,
            staging_table_id,
        });
    }
}

#[cfg(test)]
impl LocalBarrierManager {
    pub(super) fn spawn_for_test()
    -> crate::task::barrier_worker::EventSender<crate::task::barrier_worker::LocalActorOperation>
    {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;

        use crate::executor::monitor::StreamingMetrics;
        use crate::task::barrier_worker::{EventSender, LocalBarrierWorker};

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
