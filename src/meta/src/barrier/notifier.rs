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

use tokio::sync::oneshot;

use crate::{MetaError, MetaResult};

/// Used for notifying the status of a scheduled command/barrier.
#[derive(Debug, Default)]
pub(crate) struct Notifier {
    /// Get notified when scheduled barrier has started to be handled.
    pub started: Option<oneshot::Sender<MetaResult<()>>>,

    /// Get notified when scheduled barrier is collected or failed.
    pub collected: Option<oneshot::Sender<MetaResult<()>>>,
}

impl Notifier {
    /// Notify when we have injected a barrier to compute nodes.
    pub fn notify_started(&mut self) {
        if let Some(tx) = self.started.take() {
            tx.send(Ok(())).ok();
        }
    }

    pub fn notify_start_failed(self, err: MetaError) {
        if let Some(tx) = self.started {
            tx.send(Err(err)).ok();
        }
    }

    /// Notify when we have collected a barrier from all actors.
    pub fn notify_collected(self) {
        if let Some(tx) = self.collected {
            tx.send(Ok(())).ok();
        }
    }

    /// Notify when we failed to collect a barrier. This function consumes `self`.
    pub fn notify_collection_failed(self, err: MetaError) {
        if let Some(tx) = self.collected {
            tx.send(Err(err)).ok();
        }
    }

    /// Notify when we failed to collect or finish a barrier. This function consumes `self`.
    pub fn notify_failed(self, err: MetaError) {
        if let Some(tx) = self.collected {
            tx.send(Err(err.clone())).ok();
        }
    }
}
