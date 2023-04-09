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

use std::time::Duration;

use risingwave_hummock_sdk::HummockVersionId;
use risingwave_pb::common::WorkerType;
use sync_point::sync_point;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::hummock::utils::RetryableError;
use crate::hummock::{HummockManager, HummockManagerRef};
use crate::manager::LocalNotification;
use crate::storage::MetaStore;

pub type HummockManagerEventSender = tokio::sync::mpsc::UnboundedSender<HummockManagerEvent>;
pub type HummockManagerEventReceiver = tokio::sync::mpsc::UnboundedReceiver<HummockManagerEvent>;

pub enum HummockManagerEvent {
    DropSafePoint(HummockVersionId),
    #[allow(dead_code)]
    Shutdown,
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub(crate) async fn start_worker(
        self: &HummockManagerRef<S>,
        mut receiver: HummockManagerEventReceiver,
    ) -> JoinHandle<()> {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;
        let hummock_manager = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    notification = local_notification_rx.recv() => {
                        match notification {
                            Some(notification) => {
                                hummock_manager
                                    .handle_local_notification(notification)
                                    .await;
                            }
                            None => {
                                return;
                            }
                        }
                    }
                    hummock_manager_event = receiver.recv() => {
                        match hummock_manager_event {
                            Some(hummock_manager_event) => {
                                if !hummock_manager
                                    .handle_hummock_manager_event(hummock_manager_event)
                                    .await {
                                    return;
                                }
                            }
                            None => {
                                return;
                            }
                        }
                    }
                }
            }
        })
    }

    /// Returns false indicates to shutdown worker
    async fn handle_hummock_manager_event(&self, event: HummockManagerEvent) -> bool {
        match event {
            HummockManagerEvent::DropSafePoint(id) => {
                self.unregister_safe_point(id).await;
                sync_point!("UNREGISTER_HUMMOCK_VERSION_SAFE_POINT");
            }
            HummockManagerEvent::Shutdown => {
                tracing::info!("Hummock manager worker is stopped");
                return false;
            }
        }
        true
    }

    async fn handle_local_notification(&self, notification: LocalNotification) {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(60))
            .map(jitter);
        match notification {
            LocalNotification::WorkerNodeIsDeleted(worker_node) => {
                if worker_node.get_type().unwrap() == WorkerType::Compactor {
                    self.compactor_manager.remove_compactor(worker_node.id);
                }
                tokio_retry::RetryIf::spawn(
                    retry_strategy.clone(),
                    || async {
                        if let Err(err) = self.release_contexts(vec![worker_node.id]).await {
                            tracing::warn!(
                                "Failed to release hummock context {}. {}. Will retry.",
                                worker_node.id,
                                err
                            );
                            return Err(err);
                        }
                        Ok(())
                    },
                    RetryableError::default(),
                )
                .await
                .expect("retry until success");
                tracing::info!("Released hummock context {}", worker_node.id);
                sync_point!("AFTER_RELEASE_HUMMOCK_CONTEXTS_ASYNC");
            }
            // TODO move `CompactionTaskNeedCancel` to `handle_hummock_manager_event`
            // TODO extract retry boilerplate code
            LocalNotification::CompactionTaskNeedCancel(compact_task) => {
                let task_id = compact_task.task_id;
                tokio_retry::RetryIf::spawn(
                    retry_strategy.clone(),
                    || async {
                        let mut compact_task_mut = compact_task.clone();
                        if let Err(err) = self.cancel_compact_task_impl(&mut compact_task_mut).await
                        {
                            tracing::warn!(
                                "Failed to cancel compaction task {}. {}. Will retry.",
                                compact_task.task_id,
                                err
                            );
                            return Err(err);
                        }
                        Ok(())
                    },
                    RetryableError::default(),
                )
                .await
                .expect("retry until success");
                tracing::info!("Cancelled compaction task {}", task_id);
                sync_point!("AFTER_CANCEL_COMPACTION_TASK_ASYNC");
            }
            LocalNotification::SystemParamsChange(_) => {}
        }
    }
}
