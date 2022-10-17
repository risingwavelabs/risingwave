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

use std::collections::HashMap;
use std::iter::once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::{select, try_join_all, Either};
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::HummockEpoch;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::hummock::event_handler::HummockEvent;
use crate::hummock::local_version::local_version_manager::LocalVersionManager;
use crate::hummock::local_version::upload_handle_manager::UploadHandleManager;
use crate::hummock::local_version::SyncUncommittedDataStage;
use crate::hummock::store::version::{HummockReadVersion, VersionUpdate};
use crate::hummock::{HummockError, HummockResult, MemoryLimiter, SstableIdManagerRef, TrackerId};
use crate::store::SyncResult;

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    global_buffer: Arc<MemoryLimiter>,
    pub(crate) global_upload_task_size: Arc<AtomicUsize>,

    pub(crate) buffer_event_sender: mpsc::UnboundedSender<HummockEvent>,
}

impl BufferTracker {
    pub fn new(
        flush_threshold: usize,
        memory_limit: Arc<MemoryLimiter>,
        buffer_event_sender: mpsc::UnboundedSender<HummockEvent>,
    ) -> Self {
        Self {
            flush_threshold,
            global_buffer: memory_limit,
            global_upload_task_size: Arc::new(AtomicUsize::new(0)),
            buffer_event_sender,
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer.get_memory_usage() as usize
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_buffer
    }

    pub fn get_upload_task_size(&self) -> usize {
        self.global_upload_task_size.load(Ordering::Relaxed)
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_more_flush(&self) -> bool {
        self.get_buffer_size() > self.flush_threshold + self.get_upload_task_size()
    }

    pub fn send_event(&self, event: HummockEvent) {
        self.buffer_event_sender.send(event).unwrap();
    }
}

pub struct HummockEventHandler {
    local_version_manager: Arc<LocalVersionManager>,
    buffer_tracker: BufferTracker,
    sstable_id_manager: SstableIdManagerRef,
    shared_buffer_event_receiver: mpsc::UnboundedReceiver<HummockEvent>,
    upload_handle_manager: UploadHandleManager,
    pending_sync_requests: HashMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,

    // TODO: replace it with hashmap<id, read_version>
    read_version: Arc<RwLock<HummockReadVersion>>,
}

impl HummockEventHandler {
    pub fn new(
        local_version_manager: Arc<LocalVersionManager>,
        shared_buffer_event_receiver: mpsc::UnboundedReceiver<HummockEvent>,
        read_version: Arc<RwLock<HummockReadVersion>>,
    ) -> Self {
        Self {
            buffer_tracker: local_version_manager.buffer_tracker().clone(),
            sstable_id_manager: local_version_manager.sstable_id_manager(),
            local_version_manager,
            shared_buffer_event_receiver,
            upload_handle_manager: UploadHandleManager::new(),
            pending_sync_requests: Default::default(),
            read_version,
        }
    }

    fn try_flush_shared_buffer(&mut self) {
        // Keep issuing new flush task until flush is not needed or we can issue
        // no more task
        while self.buffer_tracker.need_more_flush() {
            if let Some((epoch, join_handle)) =
                self.local_version_manager.clone().flush_shared_buffer()
            {
                self.upload_handle_manager
                    .add_epoch_handle(epoch, once(join_handle));
            } else {
                break;
            }
        }
    }

    fn send_sync_result(&mut self, epoch: HummockEpoch, result: HummockResult<SyncResult>) {
        if let Some(tx) = self.pending_sync_requests.remove(&epoch) {
            let _ = tx.send(result).inspect_err(|e| {
                error!("unable to send sync result. Epoch: {}. Err: {:?}", epoch, e);
            });
        } else {
            panic!("send sync result to non-requested epoch: {}", epoch);
        }
    }
}

// Handler for different events
impl HummockEventHandler {
    fn handle_epoch_finished(&mut self, epoch: HummockEpoch) {
        // TODO: in some case we may only need the read guard.
        let mut local_version_guard = self.local_version_manager.local_version.write();
        if epoch > local_version_guard.get_max_sync_epoch() {
            // The finished flush task does not belong to any syncing epoch.
            return;
        }
        let sync_epoch = epoch;
        let sync_data = local_version_guard
            .sync_uncommitted_data
            .get_mut(&sync_epoch)
            .expect("should find");
        match sync_data.stage() {
            SyncUncommittedDataStage::CheckpointEpochSealed(_) => {
                let (payload, sync_size) = sync_data.start_syncing();
                let local_version_manager = self.local_version_manager.clone();
                let join_handle = tokio::spawn(async move {
                    let _ = local_version_manager
                        .run_sync_upload_task(payload, sync_size, sync_epoch)
                        .await
                        .inspect_err(|e| {
                            error!("sync upload task failed: {}, err: {:?}", sync_epoch, e);
                        });
                });
                self.upload_handle_manager
                    .add_epoch_handle(sync_epoch, once(join_handle));
            }
            SyncUncommittedDataStage::Syncing(_) => {
                unreachable!("when a join handle is finished, the stage should not be at syncing");
            }
            SyncUncommittedDataStage::Failed(_) => {
                drop(local_version_guard);
                self.send_sync_result(sync_epoch, Err(HummockError::other("sync task failed")));
            }
            SyncUncommittedDataStage::Synced(ssts, sync_size) => {
                let ssts = ssts.clone();
                let sync_size = *sync_size;
                drop(local_version_guard);
                self.send_sync_result(
                    sync_epoch,
                    Ok(SyncResult {
                        sync_size,
                        uncommitted_ssts: ssts,
                    }),
                );
            }
        }
    }

    fn handle_sync_epoch(
        &mut self,
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    ) {
        if let Some(old_sync_result_sender) = self
            .pending_sync_requests
            .insert(new_sync_epoch, sync_result_sender)
        {
            let _ = old_sync_result_sender
                .send(Err(HummockError::other(
                    "the sync rx is overwritten by an new rx",
                )))
                .inspect_err(|e| {
                    error!(
                        "unable to send sync result: {}. Err: {:?}",
                        new_sync_epoch, e
                    );
                });
        }
        let mut local_version_guard = self.local_version_manager.local_version.write();
        let prev_max_sync_epoch =
            if let Some(epoch) = local_version_guard.get_prev_max_sync_epoch(new_sync_epoch) {
                epoch
            } else {
                drop(local_version_guard);
                self.send_sync_result(
                    new_sync_epoch,
                    Err(HummockError::other(format!(
                        "no sync task on epoch: {}. May have been cleared",
                        new_sync_epoch
                    ))),
                );
                return;
            };
        let flush_join_handles = self
            .upload_handle_manager
            .drain_epoch_handle(prev_max_sync_epoch + 1..=new_sync_epoch);
        if flush_join_handles.is_empty() {
            // no pending flush to wait. Start syncing

            let (payload, sync_size) = local_version_guard.start_syncing(new_sync_epoch);
            let local_version_manager = self.local_version_manager.clone();
            let join_handle = tokio::spawn(async move {
                let _ = local_version_manager
                    .run_sync_upload_task(payload, sync_size, new_sync_epoch)
                    .await
                    .inspect_err(|e| {
                        error!("sync upload task failed: {}, err: {:?}", new_sync_epoch, e);
                    });
            });
            self.upload_handle_manager
                .add_epoch_handle(new_sync_epoch, once(join_handle));
        } else {
            // some pending flush task. waiting for flush to finish.
            // Note: the flush join handle of some previous epoch is now attached to
            // the new sync epoch
            self.upload_handle_manager
                .add_epoch_handle(new_sync_epoch, flush_join_handles.into_iter());
        }
    }

    async fn handle_clear(&mut self, notifier: oneshot::Sender<()>) {
        // Wait for all ongoing flush to finish.
        let ongoing_flush_handles: Vec<_> = self.upload_handle_manager.drain_epoch_handle(..);
        if let Err(e) = try_join_all(ongoing_flush_handles).await {
            error!("Failed to join flush handle {:?}", e)
        }

        // There cannot be any pending write requests since we should only clear
        // shared buffer after all actors stop processing data.
        let pending_epochs = self.pending_sync_requests.keys().cloned().collect_vec();
        pending_epochs.into_iter().for_each(|epoch| {
            self.send_sync_result(
                epoch,
                Err(HummockError::other("the pending sync is cleared")),
            );
        });

        // Clear shared buffer
        self.local_version_manager
            .local_version
            .write()
            .clear_shared_buffer();
        self.sstable_id_manager
            .remove_watermark_sst_id(TrackerId::Epoch(HummockEpoch::MAX));

        // Notify completion of the Clear event.
        notifier.send(()).unwrap();
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        loop {
            let select_result = match select(
                self.upload_handle_manager.next_finished_epoch(),
                self.shared_buffer_event_receiver.recv().boxed(),
            )
            .await
            {
                Either::Left((epoch_result, _)) => Either::Left(epoch_result),
                Either::Right((event, _)) => Either::Right(event),
            };
            match select_result {
                Either::Left(epoch_result) => {
                    let epoch = epoch_result.expect(
                        "now we don't cancel the join handle. So join is expected to be success",
                    );
                    self.handle_epoch_finished(epoch);
                }
                Either::Right(Some(event)) => match event {
                    HummockEvent::BufferMayFlush => {
                        // Only check and flush shared buffer after batch has been added to shared
                        // buffer.
                        self.try_flush_shared_buffer();
                    }
                    HummockEvent::SyncEpoch {
                        new_sync_epoch,
                        sync_result_sender,
                    } => {
                        self.handle_sync_epoch(new_sync_epoch, sync_result_sender);
                    }
                    HummockEvent::Clear(notifier) => {
                        self.handle_clear(notifier).await;
                    }
                    HummockEvent::Shutdown => {
                        info!("buffer tracker shutdown");
                        break;
                    }

                    HummockEvent::VersionUpdate(version_payload) => {
                        if let Some(new_version) = self
                            .local_version_manager
                            .try_update_pinned_version(version_payload)
                        {
                            self.read_version
                                .write()
                                .update(VersionUpdate::CommittedSnapshot(new_version));
                        }
                    }

                    HummockEvent::ImmToUploader(imm) => {
                        self.local_version_manager.write_shared_buffer_batch(imm);
                    }

                    HummockEvent::SealEpoch {
                        epoch,
                        is_checkpoint,
                    } => self
                        .local_version_manager
                        .local_version
                        .write()
                        .seal_epoch(epoch, is_checkpoint),
                },
                Either::Right(None) => {
                    break;
                }
            };
        }
    }
}
