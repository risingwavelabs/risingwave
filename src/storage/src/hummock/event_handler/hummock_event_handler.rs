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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::{select, Either};
use futures::FutureExt;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch};
use risingwave_pb::hummock::pin_version_response::Payload;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::hummock::compactor::Context;
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::event_handler::uploader::{HummockUploader, UploaderEvent};
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::store::version::{
    HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{HummockError, HummockResult, MemoryLimiter, SstableIdManagerRef, TrackerId};
use crate::store::SyncResult;

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    global_buffer: Arc<MemoryLimiter>,
    global_upload_task_size: Arc<AtomicUsize>,
}

impl BufferTracker {
    pub fn from_storage_config(config: &StorageConfig) -> Self {
        let capacity = config.shared_buffer_capacity_mb as usize * (1 << 20);
        let flush_threshold = capacity * 4 / 5;
        Self {
            flush_threshold,
            global_buffer: Arc::new(MemoryLimiter::new(capacity as u64)),
            global_upload_task_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer.get_memory_usage() as usize
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_buffer
    }

    pub fn global_upload_task_size(&self) -> Arc<AtomicUsize> {
        self.global_upload_task_size.clone()
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_more_flush(&self) -> bool {
        self.get_buffer_size()
            > self.flush_threshold + self.global_upload_task_size.load(Ordering::Relaxed)
    }
}

pub struct HummockEventHandler {
    sstable_id_manager: SstableIdManagerRef,
    hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
    pending_sync_requests: BTreeMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,

    // TODO: replace it with hashmap<id, read_version>
    read_version: Arc<RwLock<HummockReadVersion>>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,
    seal_epoch: Arc<AtomicU64>,
    pinned_version: PinnedVersion,
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    uploader: HummockUploader,
}

impl HummockEventHandler {
    pub fn new(
        hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
        pinned_version: PinnedVersion,
        compactor_context: Arc<Context>,
    ) -> Self {
        let read_version = Arc::new(RwLock::new(HummockReadVersion::new(pinned_version.clone())));
        let seal_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(pinned_version.max_committed_epoch());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let sstable_id_manager = compactor_context.sstable_id_manager.clone();
        let buffer_tracker = BufferTracker::from_storage_config(&compactor_context.options);
        let write_conflict_detector = ConflictDetector::new_from_config(&compactor_context.options);
        let uploader =
            HummockUploader::new(pinned_version.clone(), compactor_context, buffer_tracker);
        Self {
            sstable_id_manager,
            hummock_event_rx,
            pending_sync_requests: Default::default(),
            read_version,
            version_update_notifier_tx,
            seal_epoch,
            pinned_version,
            write_conflict_detector,
            uploader,
        }
    }

    pub fn sealed_epoch(&self) -> Arc<AtomicU64> {
        self.seal_epoch.clone()
    }

    pub fn version_update_notifier_tx(&self) -> Arc<tokio::sync::watch::Sender<HummockEpoch>> {
        self.version_update_notifier_tx.clone()
    }

    pub fn read_version(&self) -> Arc<RwLock<HummockReadVersion>> {
        self.read_version.clone()
    }

    pub fn buffer_tracker(&self) -> &BufferTracker {
        self.uploader.buffer_tracker()
    }
}

impl HummockEventHandler {
    async fn next_event(&mut self) -> Option<Either<UploaderEvent, HummockEvent>> {
        match select(
            self.uploader.next_event(),
            self.hummock_event_rx.recv().boxed(),
        )
        .await
        {
            Either::Left((event, _)) => Some(Either::Left(event)),
            Either::Right((event, _)) => event.map(Either::Right),
        }
    }
}

// Handler for different events
impl HummockEventHandler {
    fn handle_epoch_sync(&mut self, epoch: HummockEpoch) {
        let result = self
            .uploader
            .get_synced_data(epoch)
            .expect("data just synced. must exist");
        if let Ok(staging_sstable_infos) = &result {
            let mut read_version_guard = self.read_version.write();
            staging_sstable_infos
                .iter()
                // Take rev because newer data come first in `staging_sstable_infos` but we apply
                // older data first
                .rev()
                .for_each(|staging_sstable_info| {
                    read_version_guard.update(VersionUpdate::Staging(StagingData::Sst(
                        staging_sstable_info.clone(),
                    )))
                });
        }
        while let Some((smallest_pending_sync_epoch, _)) =
            self.pending_sync_requests.first_key_value()
        {
            if *smallest_pending_sync_epoch > epoch {
                // The smallest pending sync epoch has not synced yet. Wait later
                break;
            }
            let (pending_sync_epoch, result_sender) =
                self.pending_sync_requests.pop_first().expect("must exist");
            if pending_sync_epoch == epoch {
                send_sync_result(result_sender, to_sync_result(result));
                break;
            } else {
                send_sync_result(
                    result_sender,
                    Err(HummockError::other(format!(
                        "epoch {} is not a checkpoint epoch",
                        pending_sync_epoch
                    ))),
                );
            }
        }
    }

    fn handle_data_spilled(&mut self, staging_sstable_info: StagingSstableInfo) {
        self.read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::Sst(
                staging_sstable_info,
            )));
    }

    fn handle_sync_epoch(
        &mut self,
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    ) {
        // The epoch to
        if new_sync_epoch <= self.uploader.max_committed_epoch() {
            send_sync_result(
                sync_result_sender,
                Err(HummockError::other(format!(
                    "epoch {} has been committed. {}",
                    new_sync_epoch,
                    self.uploader.max_committed_epoch()
                ))),
            );
            return;
        }
        if new_sync_epoch <= self.uploader.max_synced_epoch() {
            if let Some(result) = self.uploader.get_synced_data(new_sync_epoch) {
                let result = to_sync_result(result);
                send_sync_result(sync_result_sender, result);
            } else {
                send_sync_result(
                    sync_result_sender,
                    Err(HummockError::other(
                        "the requested sync epoch is not a checkpoint epoch",
                    )),
                );
            }
            return;
        }

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
    }

    fn handle_clear(&mut self, notifier: oneshot::Sender<()>) {
        self.uploader.clear();

        for (epoch, result_sender) in self.pending_sync_requests.drain_filter(|_, _| true) {
            send_sync_result(
                result_sender,
                Err(HummockError::other(format!(
                    "the sync epoch {} has been cleared",
                    epoch
                ))),
            );
        }

        // Clear read version
        self.read_version.write().clear_uncommitted();
        self.sstable_id_manager
            .remove_watermark_sst_id(TrackerId::Epoch(HummockEpoch::MAX));

        // Notify completion of the Clear event.
        notifier.send(()).unwrap();
    }

    fn handle_version_update(&mut self, version_payload: Payload) {
        let prev_max_committed_epoch = self.pinned_version.max_committed_epoch();
        // TODO: after local version manager is removed, we can match version_payload directly
        // instead of taking a reference
        let newly_pinned_version = match &version_payload {
            Payload::VersionDeltas(version_deltas) => {
                let mut version_to_apply = self.pinned_version.version();
                for version_delta in &version_deltas.version_deltas {
                    assert_eq!(version_to_apply.id, version_delta.prev_id);
                    version_to_apply.apply_version_delta(version_delta);
                }
                version_to_apply
            }
            Payload::PinnedVersion(version) => version.clone(),
        };

        validate_table_key_range(&newly_pinned_version);

        self.pinned_version = self.pinned_version.new_pin_version(newly_pinned_version);

        self.read_version
            .write()
            .update(VersionUpdate::CommittedSnapshot(
                self.pinned_version.clone(),
            ));

        let max_committed_epoch = self.pinned_version.max_committed_epoch();

        // only notify local_version_manager when MCE change
        self.version_update_notifier_tx.send_if_modified(|state| {
            assert_eq!(prev_max_committed_epoch, *state);
            if max_committed_epoch > *state {
                *state = max_committed_epoch;
                true
            } else {
                false
            }
        });

        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.set_watermark(self.pinned_version.max_committed_epoch());
        }
        self.sstable_id_manager
            .remove_watermark_sst_id(TrackerId::Epoch(self.pinned_version.max_committed_epoch()));

        self.uploader
            .update_pinned_version(self.pinned_version.clone());
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        while let Some(event) = self.next_event().await {
            match event {
                Either::Left(event) => match event {
                    UploaderEvent::SyncFinish(epoch) => {
                        self.handle_epoch_sync(epoch);
                    }
                    UploaderEvent::DataSpilled(staging_sstable_info) => {
                        self.handle_data_spilled(staging_sstable_info);
                    }
                },
                Either::Right(event) => match event {
                    HummockEvent::BufferMayFlush => {
                        self.uploader.try_flush();
                    }
                    HummockEvent::SyncEpoch {
                        new_sync_epoch,
                        sync_result_sender,
                    } => {
                        self.handle_sync_epoch(new_sync_epoch, sync_result_sender);
                    }
                    HummockEvent::Clear(notifier) => {
                        self.handle_clear(notifier);
                    }
                    HummockEvent::Shutdown => {
                        info!("buffer tracker shutdown");
                        break;
                    }

                    HummockEvent::VersionUpdate(version_payload) => {
                        self.handle_version_update(version_payload);
                    }

                    HummockEvent::ImmToUploader(imm) => {
                        self.uploader.add_imm(imm);
                    }

                    HummockEvent::SealEpoch {
                        epoch,
                        is_checkpoint,
                    } => {
                        self.uploader.seal_epoch(epoch);
                        if is_checkpoint {
                            self.uploader.start_sync_epoch(epoch);
                        }
                        self.seal_epoch.store(epoch, Ordering::SeqCst);
                    }
                },
            };
        }
    }
}

fn send_sync_result(
    sender: oneshot::Sender<HummockResult<SyncResult>>,
    result: HummockResult<SyncResult>,
) {
    let _ = sender.send(result).inspect_err(|e| {
        error!("unable to send sync result. Err: {:?}", e);
    });
}

fn to_sync_result(
    staging_sstable_infos: &HummockResult<Vec<StagingSstableInfo>>,
) -> HummockResult<SyncResult> {
    match staging_sstable_infos {
        Ok(staging_sstable_infos) => Ok(SyncResult {
            // TODO: use the accurate sync size
            sync_size: 0,
            uncommitted_ssts: staging_sstable_infos
                .iter()
                .flat_map(|staging_sstable_info| staging_sstable_info.sstable_infos().clone())
                .map(|sstable_info| {
                    (
                        StaticCompactionGroupId::StateDefault as CompactionGroupId,
                        sstable_info,
                    )
                })
                .collect(),
        }),
        Err(e) => Err(HummockError::other(format!("sync task failed for {:?}", e))),
    }
}
