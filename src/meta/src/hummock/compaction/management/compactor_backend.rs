// Copyright 2023 RisingWave Labs
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

use async_trait::async_trait;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::CompactionTaskEvent;
use crate::hummock::error::Error;
use crate::hummock::HummockManagerRef;
use crate::storage::MetaStore;

#[async_trait]
pub trait CompactorBackend {
    // The `submit` interface accepts a compact task, and assigns the compact task to a remote for
    // execution. The interface returns a unique id for the corresponding task, which is used for
    // subsequent cancel.
    async fn submit(
        &self,
        compaction_group: CompactionGroupId,
        compact_task: CompactTask,
        timeout: u64,
    ) -> std::result::Result<u64, Error>;

    // The `cancel` interface will receive the id of the task and will `cancel` the
    // corresponding task in the meta and remote execution.
    async fn cancel(
        &self,
        task_id: u64,
        compact_task: &CompactTask,
    ) -> std::result::Result<(), Error>;

    // The `event_channel` will try to return a `Receiver`, and will have only one external holder.
    // The `Receiver` will receive the event from the backend. The main purpose is to decouple
    // the backend from external implementations such as the task manager.
    fn event_channel(&mut self) -> Option<UnboundedReceiver<CompactionTaskEvent>>;
}

pub struct DedicatedCompactorBackend<S>
where
    S: MetaStore,
{
    hummock_manager: HummockManagerRef<S>,

    event_sender: UnboundedSender<CompactionTaskEvent>,

    event_receiver: Option<UnboundedReceiver<CompactionTaskEvent>>,
}

impl<S> DedicatedCompactorBackend<S>
where
    S: MetaStore,
{
    fn new(hummock_manager: HummockManagerRef<S>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            hummock_manager,
            event_sender: tx,
            event_receiver: Some(rx),
        }
    }
}

#[async_trait]
impl<S> CompactorBackend for DedicatedCompactorBackend<S>
where
    S: MetaStore,
{
    async fn submit(
        &self,
        compaction_group_id: CompactionGroupId,
        compact_task: CompactTask,
        timeout: u64,
    ) -> std::result::Result<u64, Error> {
        // how to trigger a new compaction when failed assign ?

        if let Some(compactor) = self.hummock_manager.get_idle_compactor().await {
            // 2. Assign the compaction task to a compactor.
            match self
                .hummock_manager
                .assign_compaction_task(&compact_task, compactor.context_id())
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    // handle error
                    // log
                    return Err(Error::Internal(anyhow::anyhow!("error")));
                }
            };

            // 3. Send the compaction task.
            if let Err(e) = compactor
                .send_task(Task::CompactTask(compact_task.clone()))
                .await
            {
                // handle error
                // log

                return Err(Error::Internal(anyhow::anyhow!("error")));
            }

            let task_id = compact_task.task_id;
            // TODO: timeout configuration
            let _ = self.event_sender.send(CompactionTaskEvent::Register(
                compaction_group_id,
                compact_task,
                timeout,
            ));

            return Ok(task_id);
        } else {
            return Err(Error::Internal(anyhow::anyhow!("error")));
        }
    }

    async fn cancel(
        &self,
        task_id: u64,
        compact_task: &CompactTask,
    ) -> std::result::Result<(), Error> {
        todo!()
    }

    fn event_channel(&mut self) -> Option<UnboundedReceiver<CompactionTaskEvent>> {
        self.event_receiver.take()
    }
}
