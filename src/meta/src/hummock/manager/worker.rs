// Copyright 2025 RisingWave Labs
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

use risingwave_hummock_sdk::HummockVersionId;
use sync_point::sync_point;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;

use crate::hummock::{HummockManager, HummockManagerRef};
use crate::manager::LocalNotification;

pub type HummockManagerEventSender = tokio::sync::mpsc::UnboundedSender<HummockManagerEvent>;
pub type HummockManagerEventReceiver = tokio::sync::mpsc::UnboundedReceiver<HummockManagerEvent>;

pub enum HummockManagerEvent {
    DropSafePoint(HummockVersionId),
    #[allow(dead_code)]
    Shutdown,
}

impl HummockManager {
    pub async fn start_worker(
        self: &HummockManagerRef,
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
        if let LocalNotification::WorkerNodeDeleted(worker_node) = notification {
            self.release_contexts(vec![worker_node.id])
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to release hummock context {}, error={}",
                        worker_node.id,
                        err.as_report()
                    )
                });
            tracing::info!("Released hummock context {}", worker_node.id);
            sync_point!("AFTER_RELEASE_HUMMOCK_CONTEXTS_ASYNC");
        }
    }
}
