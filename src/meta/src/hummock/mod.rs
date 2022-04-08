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
mod metrics_utils;
#[cfg(any(test, feature = "test"))]
pub mod mock_hummock_meta_client;
mod model;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
mod vacuum;
use std::sync::Arc;
use std::time::Duration;

pub use compactor_manager::*;
pub use hummock_manager::*;
use itertools::Itertools;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
pub use vacuum::*;

use crate::manager::{LocalNotification, NotificationManagerRef};
use crate::storage::MetaStore;

/// Start hummock's asynchronous tasks.
pub async fn start_hummock_workers<S>(
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: Arc<CompactorManager>,
    vacuum_trigger: Arc<VacuumTrigger<S>>,
    notification_manager: NotificationManagerRef,
) -> Vec<(JoinHandle<()>, UnboundedSender<()>)>
where
    S: MetaStore,
{
    vec![
        start_compaction_trigger(hummock_manager.clone(), compactor_manager.clone()),
        VacuumTrigger::start_vacuum_trigger(vacuum_trigger),
        subscribe_cluster_membership_change(
            hummock_manager,
            compactor_manager,
            notification_manager,
        )
        .await,
    ]
}

/// Start a task to handle cluster membership change.
pub async fn subscribe_cluster_membership_change<S>(
    hummock_manager: Arc<HummockManager<S>>,
    compactor_manager: Arc<CompactorManager>,
    notification_manager: NotificationManagerRef,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    notification_manager.insert_local_sender(tx).await;
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
                            let retry_strategy = ExponentialBackoff::from_millis(10).max_delay(Duration::from_secs(60)).map(jitter);
                            tokio_retry::Retry::spawn(retry_strategy, || async {
                                if let Err(err) = hummock_manager.release_contexts(vec![worker_node.id]).await {
                                    tracing::warn!("Failed to release_contexts {}. Will retry.", err);
                                    return Err(err);
                                }
                                Ok(())
                            }).await.expect("Should retry until release_contexts succeeds");
                            compactor_manager.remove_compactor(worker_node.id);
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
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: Arc<CompactorManager>,
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
            let compactor = match compactor_manager.next_compactor() {
                None => {
                    continue;
                }
                Some(compactor) => compactor,
            };

            // 2. Get a compact task and assign to the compactor.
            let compact_task = match hummock_manager
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
                        .flat_map(|v| {
                            v.level
                                .as_ref()
                                .unwrap()
                                .table_infos
                                .iter()
                                .map(|sst| sst.id)
                                .collect_vec()
                        })
                        .collect_vec();
                    tracing::debug!(
                        "Try to compact SSTs {:?} in worker {}.",
                        input_ssts,
                        compactor.context_id()
                    );
                }
                Err(err) => {
                    tracing::warn!("Failed to send compaction task. {}", err);
                    compactor_manager.remove_compactor(compactor.context_id());
                    // We don't need to explicitly cancel the compact task here.
                    // Either the compactor will reestablish the stream and fetch this unfinished
                    // compact task, or the compactor will lose connection and
                    // its assigned compact task will be cancelled.
                    // TODO: Currently the reestablished compactor won't retrieve the on-going
                    // compact task until it is picked by next_compactor. This can leave the compact
                    // task remain unfinished for some time.
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
        let (_, hummock_manager, _, _) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::new());
        let (join_handle, shutdown_sender) =
            start_compaction_trigger(hummock_manager, compactor_manager);
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
