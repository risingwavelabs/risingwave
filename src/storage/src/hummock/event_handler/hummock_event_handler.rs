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
use std::sync::Arc;

use futures::future::{select, Either};
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch};
use risingwave_pb::hummock::pin_version_response::Payload;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::hummock::event_handler::uploader::{HummockUploader, UploaderEvent};
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::local_version::local_version_manager::LocalVersionManager;
use crate::hummock::store::version::{
    HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use crate::hummock::{HummockError, HummockResult, SstableIdManagerRef, TrackerId};
use crate::store::SyncResult;

pub struct HummockEventHandler {
    // TODO: remove the use of local version manager
    local_version_manager: Arc<LocalVersionManager>,
    sstable_id_manager: SstableIdManagerRef,
    shared_buffer_event_receiver: mpsc::UnboundedReceiver<HummockEvent>,
    pending_sync_requests: HashMap<HummockEpoch, oneshot::Sender<HummockResult<SyncResult>>>,

    // TODO: replace it with hashmap<id, read_version>
    read_version: Arc<RwLock<HummockReadVersion>>,

    uploader: HummockUploader,
}

impl HummockEventHandler {
    pub fn new(
        local_version_manager: Arc<LocalVersionManager>,
        shared_buffer_event_receiver: mpsc::UnboundedReceiver<HummockEvent>,
        read_version: Arc<RwLock<HummockReadVersion>>,
    ) -> Self {
        // TODO: avoid initializing from local version manager
        let uploader = HummockUploader::new(
            local_version_manager.get_pinned_version(),
            local_version_manager
                .shared_buffer_uploader
                .compactor_context
                .clone(),
            local_version_manager.buffer_tracker.flush_threshold,
            local_version_manager
                .buffer_tracker
                .get_memory_limiter()
                .clone(),
        );
        Self {
            sstable_id_manager: local_version_manager.sstable_id_manager(),
            local_version_manager,
            shared_buffer_event_receiver,
            pending_sync_requests: Default::default(),
            read_version,
            uploader,
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

    async fn next_event(&mut self) -> Option<Either<UploaderEvent, HummockEvent>> {
        match select(
            self.uploader.next_event(),
            self.shared_buffer_event_receiver.recv().boxed(),
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

    fn handle_version_update(&mut self, version_payload: Payload) {
        if let (Some(new_version), mce_change) = self
            .local_version_manager
            .try_update_pinned_version(version_payload)
        {
            let new_version_id = new_version.id();
            // update the read_version of hummock instance
            self.read_version
                .write()
                .update(VersionUpdate::CommittedSnapshot(new_version.clone()));

            self.uploader.update_pinned_version(new_version);

            if mce_change {
                // only notify local_version_manager when MCE change
                // TODO: use MCE to replace new_version_id
                self.local_version_manager
                    .notify_version_id_to_worker_context(new_version_id);
            }
        }
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
                    } => self.uploader.seal_epoch(epoch, is_checkpoint),
                },
            };
        }
    }
}
