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

pub mod compaction;
pub mod compaction_group;
mod compaction_scheduler;
mod compactor_manager;
pub mod error;
mod manager;
pub use manager::*;

mod level_handler;
mod metrics_utils;
#[cfg(any(test, feature = "test"))]
pub mod mock_hummock_meta_client;
mod model;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
mod utils;
mod vacuum;

use std::sync::Arc;
use std::time::Duration;

pub use compaction_scheduler::CompactionScheduler;
pub use compactor_manager::*;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
pub use vacuum::*;

use crate::hummock::compaction_scheduler::CompactionSchedulerRef;
use crate::hummock::utils::RetryableError;
use crate::manager::{LocalNotification, NotificationManagerRef};
use crate::storage::MetaStore;
use crate::MetaOpts;

/// Start hummock's asynchronous tasks.
pub async fn start_hummock_workers<S>(
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
    vacuum_manager: Arc<VacuumManager<S>>,
    notification_manager: NotificationManagerRef,
    compaction_scheduler: CompactionSchedulerRef<S>,
    meta_opts: &MetaOpts,
) -> Vec<(JoinHandle<()>, Sender<()>)>
where
    S: MetaStore,
{
    vec![
        start_compaction_scheduler(compaction_scheduler),
        start_vacuum_scheduler(
            vacuum_manager.clone(),
            Duration::from_secs(meta_opts.vacuum_interval_sec),
        ),
        local_notification_receiver(hummock_manager, compactor_manager, notification_manager).await,
    ]
}

/// Starts a task to handle meta local notification.
pub async fn local_notification_receiver<S>(
    hummock_manager: Arc<HummockManager<S>>,
    compactor_manager: Arc<CompactorManager>,
    notification_manager: NotificationManagerRef,
) -> (JoinHandle<()>, Sender<()>)
where
    S: MetaStore,
{
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    notification_manager.insert_local_sender(tx).await;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(60))
            .map(jitter);
        loop {
            tokio::select! {
                notification = rx.recv() => {
                    match notification {
                        None => {
                            return;
                        },
                        Some(LocalNotification::WorkerNodeIsDeleted(worker_node)) => {
                            compactor_manager.remove_compactor(worker_node.id);
                            tokio_retry::RetryIf::spawn(
                                retry_strategy.clone(),
                                || async {
                                    if let Err(err) = hummock_manager.release_contexts(vec![worker_node.id]).await {
                                        tracing::warn!("Failed to release hummock context {}. {}. Will retry.", worker_node.id, err);
                                        return Err(err);
                                    }
                                    Ok(())
                                }, RetryableError::default())
                                .await
                                .expect("retry until success");
                            tracing::info!("Released hummock context {}", worker_node.id);
                        },
                        Some(LocalNotification::CompactionTaskNeedCancel(mut compact_task)) => {
                            compact_task.set_task_status(risingwave_pb::hummock::compact_task::TaskStatus::Canceled);
                            tokio_retry::RetryIf::spawn(
                                retry_strategy.clone(),
                                || async {
                                    if let Err(err) = hummock_manager.cancel_compact_task_impl(&compact_task).await {
                                        tracing::warn!("Failed to cancel compaction task {}. {}. Will retry.", compact_task.task_id, err);
                                        return Err(err);
                                    }
                                    Ok(())
                                }, RetryableError::default())
                                .await
                                .expect("retry until success");
                            tracing::info!("Cancelled compaction task {}", compact_task.task_id);
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("Hummock local notification receiver is stopped");
                    return;
                }
            };
        }
    });
    (join_handle, shutdown_tx)
}

/// Starts a task to accept compaction request.
fn start_compaction_scheduler<S>(
    compaction_scheduler: CompactionSchedulerRef<S>,
) -> (JoinHandle<()>, Sender<()>)
where
    S: MetaStore,
{
    // Start compaction scheduler
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join_handle = tokio::spawn(async move {
        compaction_scheduler.start(shutdown_rx).await;
    });

    (join_handle, shutdown_tx)
}

/// Starts a task to periodically vacuum hummock.
fn start_vacuum_scheduler<S>(
    vacuum: Arc<VacuumManager<S>>,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>)
where
    S: MetaStore,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = &mut shutdown_rx => {
                    tracing::info!("Vacuum is stopped");
                    return;
                }
            }
            // May metadata vacuum and SST vacuum split into two tasks.
            if let Err(err) = vacuum.vacuum_metadata().await {
                tracing::warn!("Vacuum metadata error {:#?}", err);
            }
            if let Err(err) = vacuum.vacuum_sst_data().await {
                tracing::warn!("Vacuum SST error {:#?}", err);
            }
            sync_point::on("AFTER_SCHEDULE_VACUUM").await;
        }
    });
    (join_handle, shutdown_tx)
}

// As we have supported manual full GC in risectl,
// this auto full GC scheduler is not used for now.
#[allow(unused)]
pub fn start_full_gc_scheduler<S>(
    vacuum: Arc<VacuumManager<S>>,
    interval: Duration,
    sst_retention_time: Duration,
) -> (JoinHandle<()>, Sender<()>)
where
    S: MetaStore,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        min_trigger_interval.tick().await;
        loop {
            tokio::select! {
                _ = min_trigger_interval.tick() => {},
                _ = &mut shutdown_rx => {
                    tracing::info!("Full GC scheduler is stopped");
                    return;
                }
            }
            if let Err(err) = vacuum.start_full_gc(sst_retention_time).await {
                tracing::warn!("Full GC error {:#?}", err);
            }
        }
    });
    (join_handle, shutdown_tx)
}
