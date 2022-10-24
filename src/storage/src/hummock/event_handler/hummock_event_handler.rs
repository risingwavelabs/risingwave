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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::future::{select, Either};
use futures::FutureExt;
use itertools::Itertools;
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

pub struct HummockEventHandler {
    sstable_id_manager: SstableIdManagerRef,
    hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
    pending_sync_requests: HashMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,

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
        storage_config: Arc<StorageConfig>,
        hummock_event_rx: mpsc::UnboundedReceiver<HummockEvent>,
        read_version: Arc<RwLock<HummockReadVersion>>,
        sstable_id_manager: SstableIdManagerRef,
        compactor_context: Arc<Context>,
    ) -> Self {
        let pinned_version = read_version.read().committed().clone();
        let seal_epoch = Arc::new(AtomicU64::new(pinned_version.max_committed_epoch()));
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(pinned_version.max_committed_epoch());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let memory_limiter = Arc::new(MemoryLimiter::new(
            storage_config.shared_buffer_memory_limiter_quota() as u64,
        ));
        let uploader = HummockUploader::new(
            pinned_version.clone(),
            compactor_context,
            storage_config.flush_threshold(),
            memory_limiter,
        );
        Self {
            sstable_id_manager,
            hummock_event_rx,
            pending_sync_requests: Default::default(),
            read_version,
            version_update_notifier_tx,
            seal_epoch,
            pinned_version,
            write_conflict_detector: ConflictDetector::new_from_config(storage_config),
            uploader,
        }
    }

    pub fn sealed_epoch(&self) -> Arc<AtomicU64> {
        self.seal_epoch.clone()
    }

    pub fn version_update_notifier_tx(&self) -> Arc<tokio::sync::watch::Sender<HummockEpoch>> {
        self.version_update_notifier_tx.clone()
    }

    pub fn memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.uploader.memory_limiter()
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
    fn handle_epoch_sync(
        &mut self,
        epoch: HummockEpoch,
        result: HummockResult<Vec<StagingSstableInfo>>,
    ) {
        if let Ok(staging_sstable_infos) = &result {
            let mut read_version_guard = self.read_version.write();
            staging_sstable_infos
                .iter()
                .for_each(|staging_sstable_info| {
                    read_version_guard.update(VersionUpdate::Staging(StagingData::Sst(
                        staging_sstable_info.clone(),
                    )))
                });
        }
        if let Some(result_sender) = self.pending_sync_requests.remove(&epoch) {
            let result = result.map(|staging_sstable_infos| {
                SyncResult {
                    // TODO: use the accurate sync size
                    sync_size: 0,
                    uncommitted_ssts: staging_sstable_infos
                        .iter()
                        .flat_map(|staging_sstable_info| {
                            staging_sstable_info.sstable_infos().clone()
                        })
                        .map(|sstable_info| {
                            (
                                StaticCompactionGroupId::StateDefault as CompactionGroupId,
                                sstable_info,
                            )
                        })
                        .collect(),
                }
            });
            let _ = result_sender.send(result).inspect_err(|e| {
                error!("unable to send sync result: {} {:?}", epoch, e);
            });
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

        // There cannot be any pending write requests since we should only clear
        // shared buffer after all actors stop processing data.
        let pending_epochs = self.pending_sync_requests.keys().cloned().collect_vec();
        pending_epochs.into_iter().for_each(|epoch| {
            self.send_sync_result(
                epoch,
                Err(HummockError::other("the pending sync is cleared")),
            );
        });

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
        while let Some(select_result) = self.next_event().await {
            match select_result {
                Either::Left(event) => match event {
                    UploaderEvent::SyncFinish { epoch, result } => {
                        self.handle_epoch_sync(epoch, result);
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
                        self.uploader.seal_epoch(epoch, is_checkpoint);
                        self.seal_epoch.store(epoch, Ordering::SeqCst);
                    }
                },
            };
        }
    }
}
