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
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::CompactionTaskEvent;
use crate::hummock::error::Error;
use crate::hummock::HummockManagerRef;
use crate::manager::LocalNotification;
use crate::storage::MetaStore;

#[async_trait]
pub trait CompactorBackend {
    async fn submit(
        &self,
        compaction_group: CompactionGroupId,
        compact_task: CompactTask,
        timeout: u64,
    ) -> bool;

    async fn cancel(&self, compact_task: &mut CompactTask) -> bool;

    fn event_channel(
        &mut self,
        compact_task: &CompactTask,
    ) -> Option<UnboundedReceiver<CompactionTaskEvent>>;
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
    ) -> bool {
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
                    tracing::warn!(
                        "Failed to assign {:?} compaction task to compactor {} : {:#?}",
                        compact_task.task_type().as_str_name(),
                        compactor.context_id(),
                        err
                    );
                    match err {
                        Error::CompactionTaskAlreadyAssigned(_, _) => {
                            panic!("Compaction task manager is the only tokio task that can assign task.");
                        }
                        Error::InvalidContext(context_id) => {
                            self.hummock_manager
                                .compactor_manager
                                .remove_compactor(context_id);
                            return false;
                        }
                        _ => {
                            return false;
                        }
                    }
                }
            };

            // 3. Send the compaction task.
            if let Err(e) = compactor
                .send_task(Task::CompactTask(compact_task.clone()))
                .await
            {
                tracing::warn!(
                    "Failed to send task {} to {}. {:#?}",
                    compact_task.task_id,
                    compactor.context_id(),
                    e
                );

                // todo: add timeout to remove compactor
                self.hummock_manager
                    .compactor_manager
                    .pause_compactor(compactor.context_id());

                return false;
            }

            // TODO: timeout configuration
            let _ = self.event_sender.send(CompactionTaskEvent::Register(
                compaction_group_id,
                compact_task,
                timeout,
            ));

            return true;
        } else {
            return false;
        }
    }

    async fn cancel(&self, compact_task: &mut CompactTask) -> bool {
        if let Err(err) = self
            .hummock_manager
            .cancel_compact_task(compact_task, TaskStatus::HeartbeatCanceled)
            .await
        {
            // Cancel task asynchronously.
            tracing::warn!(
                "Failed to cancel task {}. {}. {:?} It will be cancelled asynchronously.",
                compact_task.task_id,
                err,
                TaskStatus::HeartbeatCanceled
            );
            self.hummock_manager
                .env
                .notification_manager()
                .notify_local_subscribers(LocalNotification::CompactionTaskNeedCancel(
                    compact_task.clone(),
                ))
                .await;
        }
        true
    }

    fn event_channel(
        &mut self,
        compact_task: &CompactTask,
    ) -> Option<UnboundedReceiver<CompactionTaskEvent>> {
        self.event_receiver.take()
    }
}
