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

mod compaction;
mod compactor_manager;
mod hummock_manager;
#[cfg(test)]
mod hummock_manager_tests;
mod level_handler;
mod model;
#[cfg(test)]
pub mod test_utils;
mod vacuum;

use std::sync::Arc;
use std::time::Duration;

pub use compactor_manager::*;
pub use hummock_manager::*;
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
pub use vacuum::*;

use crate::manager::{LocalNotification, NotificationManagerRef};
use crate::storage::MetaStore;

/// Start hummock's asynchronous tasks.
pub async fn start_hummock_workers<S>(
    hummock_manager_ref: Arc<HummockManager<S>>,
    compactor_manager_ref: Arc<CompactorManager>,
    vacuum_trigger_ref: Arc<VacuumTrigger<S>>,
    notification_manager_ref: NotificationManagerRef,
) -> Vec<(JoinHandle<()>, UnboundedSender<()>)>
where
    S: MetaStore,
{
    vec![
        start_compaction_trigger(hummock_manager_ref.clone(), compactor_manager_ref.clone()),
        VacuumTrigger::start_vacuum_trigger(vacuum_trigger_ref),
        subscribe_cluster_membership_change(
            hummock_manager_ref,
            compactor_manager_ref,
            notification_manager_ref,
        )
        .await,
    ]
}

/// Start a task to handle cluster membership change.
pub async fn subscribe_cluster_membership_change<S>(
    hummock_manager_ref: Arc<HummockManager<S>>,
    _compactor_manager_ref: Arc<CompactorManager>,
    notification_manager_ref: NotificationManagerRef,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    notification_manager_ref.insert_local_sender(tx).await;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                notification = rx.recv() => {
                    match notification {
                        None => {
                            return;
                        }
                        Some(LocalNotification::WorkerDeletion(worker_node)) => {
                            // TODO: #93 retry instead of unwrap
                            hummock_manager_ref.release_contexts(vec![worker_node.id]).await.unwrap();
                            // TODO: #93 notify CompactorManager to remove stale compactor
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    return;
                }
            }
        }
    });
    (join_handle, shutdown_tx)
}

const COMPACT_TRIGGER_INTERVAL: Duration = Duration::from_secs(10);
/// Starts a worker to conditionally trigger compaction.
pub fn start_compaction_trigger<S>(
    hummock_manager_ref: Arc<HummockManager<S>>,
    compactor_manager_ref: Arc<CompactorManager>,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        let mut min_interval = tokio::time::interval(COMPACT_TRIGGER_INTERVAL);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_interval.tick() => {},
                // Shutdown compactor
                _ = shutdown_rx.recv() => {
                    tracing::info!("Compaction trigger is shutting down");
                    return;
                }
            }

            // 1. Pick a compactor.
            let compactor = match compactor_manager_ref.next_compactor() {
                None => {
                    continue;
                }
                Some(compactor) => compactor,
            };

            // 2. Assign a compact task to the compactor.
            let compact_task = match hummock_manager_ref
                .get_compact_task(compactor.context_id())
                .await
            {
                Ok(Some(compact_task)) => compact_task,
                Ok(None) => {
                    // No compact task available.
                    continue;
                }
                Err(err) => {
                    tracing::warn!("Failed to get compact task. {}", err);
                    continue;
                }
            };

            // 3. Send the compact task to the compactor.
            match compactor.send_task(Some(compact_task.clone()), None).await {
                Ok(_) => {
                    let input_ssts = compact_task
                        .input_ssts
                        .iter()
                        .flat_map(|v| v.level.as_ref().unwrap().table_ids.clone())
                        .collect_vec();
                    tracing::debug!(
                        "Try to compact SSTs {:?} in worker {}.",
                        input_ssts,
                        compactor.context_id()
                    );
                }
                Err(err) => {
                    tracing::warn!("Failed to send compaction task. {}", err);
                    // Cancel the compact task and remove the compactor.
                    // TODO: #93 retry
                    hummock_manager_ref
                        .report_compact_task(compact_task.clone(), false)
                        .await
                        .unwrap();
                    compactor_manager_ref.remove_compactor(compactor.context_id());
                }
            }
        }
    });

    (join_handle, shutdown_tx)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::hummock::test_utils::setup_compute_env;
    use crate::hummock::{start_compaction_trigger, CompactorManager};

    #[tokio::test]
    async fn test_shutdown_compaction_trigger() {
        let (_, hummock_manager_ref, _, _) = setup_compute_env(80).await;
        let compactor_manager_ref = Arc::new(CompactorManager::new());
        let (join_handle, shutdown_sender) =
            start_compaction_trigger(hummock_manager_ref, compactor_manager_ref);
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
