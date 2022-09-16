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

#[derive(Debug)]
pub(super) enum NotifierCollected {
    /// Get notified when scheduled barrier is collected.
    CollectedBarrier(oneshot::Sender<MetaResult<()>>),
    /// Get notified when scheduled barrier(checkpoint = true) is collected.
    CollectedCheckpointBarrier(oneshot::Sender<MetaResult<()>>),
}

/// Used for notifying the status of a scheduled command/barrier.
#[derive(Debug, Default)]
pub(super) struct Notifier {
    /// Get notified when scheduled barrier is about to send.
    pub to_send: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled barrier is finished.
    pub finished: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled different barrier is collected.
    pub collected: Option<NotifierCollected>,
}

impl Notifier {
    /// Notify when we are about to send a barrier.
    pub fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(()).ok();
        }
    }

    pub fn take_collected_checkpoint_barrier(&mut self) -> Option<oneshot::Sender<MetaResult<()>>> {
        let collected = self.collected.take();
        match collected {
            Some(NotifierCollected::CollectedCheckpointBarrier(tx)) => Some(tx),
            _ => {
                self.collected = collected;
                None
            }
        }
    }

    /// Notify when we have collected a barrier(checkpoint = true) from all actors.
    pub fn notify_checkpoint_barrier_collected(&mut self) {
        match self.collected.take() {
            Some(NotifierCollected::CollectedBarrier(tx)) => {
                tx.send(Ok(())).ok();
            }
            Some(NotifierCollected::CollectedCheckpointBarrier(tx)) => {
                tx.send(Ok(())).ok();
            }
            _ => {}
        };
    }

    /// Notify when we have collected a barrier(checkpoint = false) from all actors.
    pub fn notify_barrier_collected(&mut self) {
        let collected = self.collected.take();
        match collected {
            Some(NotifierCollected::CollectedBarrier(tx)) => {
                tx.send(Ok(())).ok();
            }
            _ => self.collected = collected,
        };
    }

    pub fn need_collect_checkpoint_barrier(&self) -> bool {
        matches!(
            self.collected,
            Some(NotifierCollected::CollectedCheckpointBarrier(_))
        )
    }

    /// Notify when we failed to collect(checkpoint = true) a barrier. This function consumes
    /// `self`.
    pub fn notify_checkpoint_barrier_collection_failed(self, err: MetaError) {
        match self.collected {
            Some(NotifierCollected::CollectedBarrier(tx)) => {
                tx.send(Err(err)).ok();
            }
            Some(NotifierCollected::CollectedCheckpointBarrier(tx)) => {
                tx.send(Err(err)).ok();
            }
            _ => {}
        };
    }

    /// Notify when we have finished a barrier from all actors. This function consumes `self`.
    ///
    /// Generally when a barrier is collected, it's also finished since it does not require further
    /// report of finishing from actors.
    /// However for creating MV, this is only called when all `Chain` report it finished.
    pub fn notify_finished(&mut self) {
        if let Some(tx) = self.finished.take() {
            tx.send(()).ok();
        }
    }
}
