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
use std::time::Duration;

use parking_lot::Mutex;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::hummock::compaction::compaction_picker::CompactionPicker;
use crate::hummock::manager::compaction::CompactionGroupRef;
use crate::hummock::utils::RetryableError;
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
    request_tx: UnboundedSender<CompactionGroupRef>,
    request_rx: Mutex<Option<UnboundedReceiver<CompactionGroupRef>>>,
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
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel::<CompactionGroupRef>();
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
            let compaction_group: CompactionGroupRef = tokio::select! {
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

    async fn pick_and_assign(&self, compaction_group: CompactionGroupRef) {
        // 1. Select a compactor.
        let compactor = match self.compactor_manager.next_compactor() {
            None => {
                tracing::warn!("No compactor is available.");
                self.reschedule_compaction_group(compaction_group);
                // Sleep to avoid spin as we've rescheduled the request.
                tokio::time::sleep(Duration::from_secs(10)).await;
                return;
            }
            Some(compactor) => compactor,
        };
        let max_pending_compact_task = 2usize;
        if self
            .hummock_manager
            .get_ongoing_compact_task_count(compactor.context_id())
            .await
            > max_pending_compact_task
        {
            tracing::warn!("Compactor is busy.");
            self.reschedule_compaction_group(compaction_group);
            // The selected compactor is busy. We use this to roughly indicate the whole scheduler
            // is overloaded.
            tokio::time::sleep(Duration::from_secs(10)).await;
            return;
        }

        // 2. Pick a compact task and assign to the compactor.
        let mut compact_task = match self
            .hummock_manager
            .get_compact_task_by_compaction_group(compactor.context_id(), compaction_group.clone())
            .await
        {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                // No compact task available.
                compaction_group.write().set_is_scheduled(false);
                return;
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to get compact task for compactor {}. {:?}",
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
                // Decide if more compaction task available in compaction_group, then either
                // reschedule or unset compaction_group's is_scheduled.
                let levels = self.hummock_manager.get_current_version().await.levels;
                if compaction_group
                    .read()
                    .compaction_picker()
                    .need_compaction(levels)
                {
                    self.reschedule_compaction_group(compaction_group);
                } else {
                    compaction_group.write().set_is_scheduled(false);
                }
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to send compaction task to worker {}: {}",
                    compactor.context_id(),
                    err
                );
                compact_task.task_status = false;
                self.compactor_manager
                    .remove_compactor(compactor.context_id());
                self.reschedule_compaction_group(compaction_group);

                // Retry only happens when meta store is undergoing failure.
                let retry_strategy = ExponentialBackoff::from_millis(10)
                    .max_delay(Duration::from_secs(60))
                    .map(jitter);
                tokio_retry::RetryIf::spawn(retry_strategy, || async {
                    if let Err(err) = self.hummock_manager.report_compact_task(&compact_task).await {
                        tracing::warn!(
                            "Failed to cancel compaction task for worker {}: {:?}. Will retry.",
                            compactor.context_id(),
                            err
                        );
                        return Err(err);
                    }
                    Ok(())
                }, RetryableError::default()).await
                    .unwrap_or_else(|err|{
                        tracing::warn!(
                            "Failed to cancel compaction task for worker {}: unretryable error {:?}. The compaction task will be unassigned after the worker is removed from cluster",
                            compactor.context_id(),
                            err
                        );
                    });
            }
        }
    }

    pub fn shutdown_sender(&self) -> UnboundedSender<()> {
        self.shutdown_tx.clone()
    }

    pub fn request_sender(&self) -> UnboundedSender<CompactionGroupRef> {
        self.request_tx.clone()
    }

    fn reschedule_compaction_group(&self, compaction_group: CompactionGroupRef) {
        if let Err(err) = self.request_tx.send(compaction_group.clone()) {
            tracing::warn!(
                "Failed to schedule compaction_group {}: {}",
                compaction_group.read().group_id(),
                err
            );
        }
    }
}
