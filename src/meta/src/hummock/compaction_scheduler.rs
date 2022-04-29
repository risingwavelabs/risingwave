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

use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::hummock::compaction_group::CompactionGroupId;
use crate::hummock::{CompactorManagerRef, HummockManagerRef};
use crate::storage::MetaStore;

pub type CompactionSchedulerRef<S> = Arc<CompactionScheduler<S>>;

/// Schedules compaction task picking and assignment.
pub struct CompactionScheduler<S>
where
    S: MetaStore,
{
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
    shutdown_tx: UnboundedSender<()>,
    shutdown_rx: Mutex<Option<UnboundedReceiver<()>>>,
    request_tx: UnboundedSender<CompactionGroupId>,
    request_rx: Mutex<Option<UnboundedReceiver<CompactionGroupId>>>,
}

impl<S> CompactionScheduler<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel::<CompactionGroupId>();
        Self {
            hummock_manager,
            compactor_manager,
            shutdown_tx,
            shutdown_rx: Mutex::new(Some(shutdown_rx)),
            request_tx,
            request_rx: Mutex::new(Some(request_rx)),
        }
    }

    pub async fn start(&self) {
        let (mut shutdown_rx, mut request_rx) = match (
            self.shutdown_rx.lock().take(),
            self.request_rx.lock().take(),
        ) {
            (Some(shutdown_rx), Some(request_rx)) => (shutdown_rx, request_rx),
            _ => {
                tracing::warn!("Compaction scheduler is already started");
                return;
            }
        };
        self.hummock_manager
            .set_compaction_scheduler(self.request_tx.clone());
        tracing::info!("Start compaction scheduler.");
        'compaction_trigger: loop {
            let compaction_group: CompactionGroupId = tokio::select! {
                compaction_group = request_rx.recv() => {
                    match compaction_group {
                        Some(compaction_group) => compaction_group,
                        None => {
                            break 'compaction_trigger;
                        }
                    }
                },
                // Shutdown compactor
                _ = shutdown_rx.recv() => {
                    break 'compaction_trigger;
                }
            };
            self.pick_and_assign(compaction_group).await;
        }
        tracing::info!("Compaction scheduler is stopped");
    }

    async fn pick_and_assign(&self, compaction_group: CompactionGroupId) {
        // 1. Select a compactor.
        let compactor = match self.compactor_manager.next_compactor() {
            None => {
                tracing::warn!("No compactor is available.");
                return;
            }
            Some(compactor) => compactor,
        };

        // 2. Pick a compact task and assign to the compactor.
        // TODO: specify compaction_group in get_compact_task
        let mut compact_task = match self
            .hummock_manager
            .get_compact_task(compactor.context_id())
            .await
        {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                // No compact task available.
                return;
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to get compact task for compactor {}. {}",
                    compactor.context_id(),
                    err
                );
                self.compactor_manager
                    .remove_compactor(compactor.context_id());
                self.reschedule_compaction_group(compaction_group);
                return;
            }
        };

        // 3. Send the compact task to the compactor.
        match compactor.send_task(Some(compact_task.clone()), None).await {
            Ok(_) => {
                tracing::debug!(
                    "Sent compaction task {} to worker {}.",
                    compact_task_to_string(&compact_task),
                    compactor.context_id()
                );
                // TODO: decide if more compaction task available in compaction_group, then either
                // reschedule or unset compaction_group's is_scheduled.
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to send compaction task to worker {}: {}",
                    compactor.context_id(),
                    err
                );
                compact_task.task_status = false;
                if let Err(err) = self.hummock_manager.report_compact_task(compact_task).await {
                    tracing::warn!(
                        "Failed to cancel compaction task for worker {}: {}",
                        compactor.context_id(),
                        err
                    );
                    // Either the compactor will reestablish the stream and fetch this unfinished
                    // compact task, or the compactor will lose connection and
                    // its assigned compact task will be cancelled eventually.
                }
                self.compactor_manager
                    .remove_compactor(compactor.context_id());
                self.reschedule_compaction_group(compaction_group);
            }
        }
    }

    pub fn shutdown_sender(&self) -> UnboundedSender<()> {
        self.shutdown_tx.clone()
    }

    pub fn request_sender(&self) -> UnboundedSender<CompactionGroupId> {
        self.request_tx.clone()
    }

    fn reschedule_compaction_group(&self, compaction_group: CompactionGroupId) {
        if let Err(err) = self.request_tx.send(compaction_group.clone()) {
            tracing::warn!(
                "Failed to schedule compaction_group {:?}: {}",
                compaction_group,
                err
            );
        }
    }
}
