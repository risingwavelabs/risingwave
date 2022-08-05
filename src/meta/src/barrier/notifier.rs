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

/// Used for notifying the status of a scheduled command/barrier.
#[derive(Debug, Default)]
pub(super) struct Notifier {
    /// Get notified when scheduled barrier is about to send.
    pub to_send: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled barrier is collected or failed.
    pub collected: Option<oneshot::Sender<MetaResult<()>>>,

    /// Get notified when scheduled barrier is finished.
    pub finished: Option<oneshot::Sender<()>>,
}

impl Notifier {
    /// Notify when we are about to send a barrier.
    pub fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(()).ok();
        }
    }

    /// Notify when we have collected a barrier from all actors.
    pub fn notify_collected(&mut self) {
        if let Some(tx) = self.collected.take() {
            tx.send(Ok(())).ok();
        }
    }

    /// Notify when we failed to collect a barrier. This function consumes `self`.
    pub fn notify_collection_failed(self, err: MetaError) {
        if let Some(tx) = self.collected {
            tx.send(Err(err)).ok();
        }
    }

    /// Notify when we have finished a barrier from all actors. This function consumes `self`.
    ///
    /// Generally when a barrier is collected, it's also finished since it does not require further
    /// report of finishing from actors.
    /// However for creating MV, this is only called when all `Chain` report it finished.
    pub fn notify_finished(self) {
        if let Some(tx) = self.finished {
            tx.send(()).ok();
        }
    }
}
