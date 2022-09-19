// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tokio::sync::oneshot;

use crate::{MetaError, MetaResult};

/// Notifier with checkpoint = true
pub(super) enum NotifierCheckpointBarrier {
    /// Get notified when scheduled barrier(checkpoint = true) is finished.
    Finished(Option<oneshot::Sender<()>>),
    /// Get notified when scheduled barrier(checkpoint = true) is collected.
    Collected(Option<oneshot::Sender<MetaResult<()>>>),
}

impl NotifierCheckpointBarrier {
    /// Notify when we collect a barrier(checkpoint = true)
    pub fn notify_checkpoint_barrier(&mut self) {
        match self {
            NotifierCheckpointBarrier::Collected(tx) => {
                if let Some(tx) = tx.take() {
                    tx.send(Ok(())).ok();
                }
            }
            NotifierCheckpointBarrier::Finished(tx) => {
                if let Some(tx) = tx.take() {
                    tx.send(()).ok();
                }
            }
        }
    }

    /// Notify when we failed to collect a barrier
    pub fn notify_chekpoint_barrier_failed(&mut self, err: MetaError) {
        if let NotifierCheckpointBarrier::Collected(tx) = self {
            if let Some(tx) = tx.take() {
                tx.send(Err(err)).ok();
            }
        }
    }
}
/// Used for notifying the status of a scheduled command/barrier.
#[derive(Debug, Default)]
pub(super) struct Notifier {
    /// Get notified when scheduled barrier is about to send.
    pub to_send: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled barrier is finished.
    pub finished: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled different barrier is collected.
    pub collected: Option<oneshot::Sender<MetaResult<()>>>,

    /// Whether this notifier is bound to a checkpoint barrier or not.
    pub checkpoint: bool,
}

impl Notifier {
    /// Notify when we are about to send a barrier.
    pub fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(()).ok();
        }
    }

    /// Take `collected_notifier` when we have collected a barrier from all actors.
    /// It will be use with checkpoint = true.
    pub fn take_collected_checkpoint_barrier(&mut self) -> NotifierCheckpointBarrier {
        NotifierCheckpointBarrier::Collected(self.collected.take())
    }

    /// Take `finished_notifier` when we have finished a barrier from all actors. This function
    /// consumes `self`.
    ///
    /// Generally when a barrier is collected, it's also finished since it does not require further
    /// report of finishing from actors.
    /// However for creating MV, this is only called when all `Chain` report it finished.
    pub fn take_finished_checkpoint_barrier(self) -> NotifierCheckpointBarrier {
        NotifierCheckpointBarrier::Finished(self.finished)
    }

    pub fn bound_to_checkpoint_barrier(&self) -> bool {
        self.checkpoint
    }

    /// Notify when we have collected a barrier from all actors.
    /// It will be use with checkpoint = false.
    pub fn notify_collected(&mut self) {
        if let Some(tx) = self.collected.take() {
            tx.send(Ok(())).ok();
        }
    }

    /// Notify when we failed to collect a barrier. This function consumes
    /// `self`.
    /// It will be use with checkpoint = false.
    pub fn notify_collected_failed(self, err: MetaError) {
        if let Some(tx) = self.collected {
            tx.send(Err(err)).ok();
        }
    }
}
