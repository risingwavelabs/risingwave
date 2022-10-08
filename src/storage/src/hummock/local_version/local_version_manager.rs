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

use std::ops::{Deref, RangeBounds};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::{RwLock, RwLockWriteGuard};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
#[cfg(any(test, feature = "test"))]
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::{CompactionGroupId, HummockReadEpoch};
use risingwave_pb::hummock::pin_version_response::Payload;
use risingwave_pb::hummock::{pin_version_response, HummockVersion};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::local_version::flush_controller::{BufferTracker, FlushController};
use crate::hummock::local_version::pinned_version::{start_pinned_version_worker, PinnedVersion};
use crate::hummock::local_version::{LocalVersion, ReadVersion};
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferItem};
use crate::hummock::shared_buffer::shared_buffer_uploader::{
    SharedBufferUploader, UploadTaskPayload,
};
use crate::hummock::shared_buffer::SharedBufferEvent;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, SstableIdManagerRef, TrackerId,
    INVALID_VERSION_ID,
};
use crate::monitor::StateStoreMetrics;
use crate::storage_value::StorageValue;
use crate::store::SyncResult;

struct WorkerContext {
    version_update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
}

/// A holder for any external reference to `LocalVersionManager`.
///
/// For the term `external`, it means any external usage of `LocalVersionManager` other than some
/// worker tasks related to `LocalVersionManager` and holding a reference to it.
///
/// Upon dropping such holder, it means there is no any external usage of the `LocalVersionManager`,
/// and we can send the shutdown message to the `LocalVersionRelatedWorker` to gracefully shutdown
/// the worker.
pub struct LocalVersionManagerExternalHolder {
    local_version_manager: Arc<LocalVersionManager>,
    shutdown_sender: mpsc::UnboundedSender<SharedBufferEvent>,
}

impl Drop for LocalVersionManagerExternalHolder {
    fn drop(&mut self) {
        let _ = self
            .shutdown_sender
            .send(SharedBufferEvent::Shutdown)
            .inspect_err(|e| {
                error!("unable to send shutdown. Err: {}", e);
            });
    }
}

impl Deref for LocalVersionManagerExternalHolder {
    type Target = LocalVersionManager;

    fn deref(&self) -> &Self::Target {
        self.local_version_manager.deref()
    }
}

pub type LocalVersionManagerRef = Arc<LocalVersionManagerExternalHolder>;

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `Sstables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    pub(crate) local_version: RwLock<LocalVersion>,
    worker_context: WorkerContext,
    buffer_tracker: BufferTracker,
    write_conflict_detector: Option<Arc<ConflictDetector>>,
    shared_buffer_uploader: Arc<SharedBufferUploader>,
    sstable_id_manager: SstableIdManagerRef,
}

impl LocalVersionManager {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
        sstable_id_manager: SstableIdManagerRef,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> LocalVersionManagerRef {
        let (pinned_version_manager_tx, pinned_version_manager_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (version_update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);

        // This version cannot be used in query. It must be replaced by valid version.
        let pinned_version = HummockVersion {
            id: INVALID_VERSION_ID,
            ..Default::default()
        };

        let (buffer_event_sender, buffer_event_receiver) = mpsc::unbounded_channel();

        let capacity = (options.shared_buffer_capacity_mb as usize) * (1 << 20);

        let buffer_tracker = BufferTracker::new(
            // 0.8 * capacity
            // TODO: enable setting the ratio with config
            capacity * 4 / 5,
            capacity,
            buffer_event_sender.clone(),
        );

        let local_version_manager = Arc::new(LocalVersionManager {
            local_version: RwLock::new(LocalVersion::new(
                pinned_version,
                pinned_version_manager_tx,
            )),
            worker_context: WorkerContext {
                version_update_notifier_tx,
            },
            buffer_tracker: buffer_tracker.clone(),
            write_conflict_detector: write_conflict_detector.clone(),

            shared_buffer_uploader: Arc::new(SharedBufferUploader::new(
                options,
                sstable_store,
                hummock_meta_client.clone(),
                stats,
                write_conflict_detector,
                sstable_id_manager.clone(),
                filter_key_extractor_manager,
            )),
            sstable_id_manager: sstable_id_manager.clone(),
        });

        // Unpin unused version.
        tokio::spawn(start_pinned_version_worker(
            pinned_version_manager_rx,
            hummock_meta_client,
        ));

        let flush_controller = FlushController::new(
            local_version_manager.clone(),
            buffer_tracker,
            sstable_id_manager,
            buffer_event_receiver,
        );

        // Buffer size manager.
        tokio::spawn(flush_controller.start_flush_controller_worker());

        Arc::new(LocalVersionManagerExternalHolder {
            local_version_manager,
            shutdown_sender: buffer_event_sender,
        })
    }

    #[cfg(any(test, feature = "test"))]
    pub fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
    ) -> LocalVersionManagerRef {
        Self::new(
            options.clone(),
            sstable_store,
            Arc::new(StateStoreMetrics::unused()),
            hummock_meta_client.clone(),
            write_conflict_detector,
            Arc::new(crate::hummock::SstableIdManager::new(
                hummock_meta_client,
                options.sstable_id_remote_fetch_number,
            )),
            Arc::new(FilterKeyExtractorManager::default()),
        )
    }

    /// Updates cached version if the new version is of greater id.
    /// You shouldn't unpin even the method returns false, as it is possible `hummock_version` is
    /// being referenced by some readers.
    pub fn try_update_pinned_version(
        &self,
        pin_resp_payload: pin_version_response::Payload,
    ) -> bool {
        let old_version = self.local_version.read();
        let new_version_id = match &pin_resp_payload {
            Payload::VersionDeltas(version_deltas) => match version_deltas.delta.last() {
                Some(version_delta) => version_delta.id,
                None => old_version.pinned_version().id(),
            },
            Payload::PinnedVersion(version) => version.id,
        };

        if old_version.pinned_version().id() >= new_version_id {
            return false;
        }

        let (newly_pinned_version, version_deltas) = match pin_resp_payload {
            Payload::VersionDeltas(version_deltas) => {
                let mut version_to_apply = old_version.pinned_version().version();
                for version_delta in &version_deltas.delta {
                    assert_eq!(version_to_apply.id, version_delta.prev_id);
                    version_to_apply.apply_version_delta(version_delta);
                }
                (version_to_apply, Some(version_deltas.delta))
            }
            Payload::PinnedVersion(version) => (version, None),
        };

        for levels in newly_pinned_version.levels.values() {
            if validate_table_key_range(&levels.levels).is_err() {
                error!("invalid table key range: {:?}", levels.levels);
                return false;
            }
        }

        drop(old_version);
        let mut new_version = self.local_version.write();
        // check again to prevent other thread changes new_version.
        if new_version.pinned_version().id() >= new_version_id {
            return false;
        }

        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.set_watermark(newly_pinned_version.max_committed_epoch);
        }

        let cleaned_epochs = new_version.set_pinned_version(newly_pinned_version, version_deltas);
        RwLockWriteGuard::unlock_fair(new_version);
        for cleaned_epoch in cleaned_epochs {
            self.sstable_id_manager
                .remove_watermark_sst_id(TrackerId::Epoch(cleaned_epoch));
        }
        self.worker_context
            .version_update_notifier_tx
            .send(new_version_id)
            .ok();
        true
    }

    /// Waits until the local hummock version contains the epoch. If `wait_epoch` is `Current`,
    /// we will only check whether it is le `sealed_epoch` and won't wait.
    pub async fn try_wait_epoch(&self, wait_epoch: HummockReadEpoch) -> HummockResult<()> {
        let wait_epoch = match wait_epoch {
            HummockReadEpoch::Committed(epoch) => epoch,
            HummockReadEpoch::Current(epoch) => {
                let sealed_epoch = self.local_version.read().get_sealed_epoch();
                assert!(
                    epoch <= sealed_epoch
                        && epoch != HummockEpoch::MAX
                    ,
                    "current epoch can't read, because the epoch in storage is not updated, epoch{}, sealed epoch{}"
                    ,epoch
                    ,sealed_epoch
                );
                return Ok(());
            }
            HummockReadEpoch::NoWait(_) => return Ok(()),
        };
        if wait_epoch == HummockEpoch::MAX {
            panic!("epoch should not be u64::MAX");
        }
        let mut receiver = self.worker_context.version_update_notifier_tx.subscribe();
        loop {
            let (pinned_version_id, pinned_version_epoch) = {
                let current_version = self.local_version.read();
                if current_version.pinned_version().max_committed_epoch() >= wait_epoch {
                    return Ok(());
                }
                (
                    current_version.pinned_version().id(),
                    current_version.pinned_version().max_committed_epoch(),
                )
            };
            match tokio::time::timeout(Duration::from_secs(10), receiver.changed()).await {
                Err(_) => {
                    // The reason that we need to retry here is batch scan in chain/rearrange_chain
                    // is waiting for an uncommitted epoch carried by the CreateMV barrier, which
                    // can take unbounded time to become committed and propagate
                    // to the CN. We should consider removing the retry as well as wait_epoch for
                    // chain/rearrange_chain if we enforce chain/rearrange_chain to be
                    // scheduled on the same CN with the same distribution as
                    // the upstream MV. See #3845 for more details.
                    tracing::warn!(
                        "wait_epoch {:?} timeout when waiting for version update. pinned_version_id {}, pinned_version_epoch {}.",
                        wait_epoch, pinned_version_id, pinned_version_epoch
                    );
                    continue;
                }
                Ok(Err(_)) => {
                    return Err(HummockError::wait_epoch("tx dropped"));
                }
                Ok(Ok(_)) => {}
            }
        }
    }

    pub fn build_shared_buffer_item_batches(
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: HummockEpoch,
    ) -> Vec<SharedBufferItem> {
        kv_pairs
            .into_iter()
            .map(|(key, value)| {
                (
                    Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                    value.into(),
                )
            })
            .collect_vec()
    }

    pub async fn write_shared_buffer(
        &self,
        epoch: HummockEpoch,
        compaction_group_id: CompactionGroupId,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        table_id: u32,
    ) -> HummockResult<usize> {
        let sorted_items = Self::build_shared_buffer_item_batches(kv_pairs, epoch);
        let batch_size = SharedBufferBatch::measure_batch_size(&sorted_items);
        let tracker = self
            .buffer_tracker
            .limiter
            .require_memory(batch_size as u64)
            .await
            .unwrap();
        let batch = SharedBufferBatch::new(
            sorted_items,
            epoch,
            tracker,
            compaction_group_id,
            table_id,
            batch_size,
        );
        self.write_shared_buffer_inner(epoch, batch);
        Ok(batch_size)
    }

    pub(crate) fn write_shared_buffer_inner(&self, epoch: HummockEpoch, batch: SharedBufferBatch) {
        let mut local_version_guard = self.local_version.write();
        let sealed_epoch = local_version_guard.get_sealed_epoch();
        assert!(
            epoch > sealed_epoch,
            "write epoch must greater than max current epoch, write epoch{}, sealed epoch{}",
            epoch,
            sealed_epoch
        );
        // Write into shared buffer

        let shared_buffer = match local_version_guard.get_mut_shared_buffer(epoch) {
            Some(shared_buffer) => shared_buffer,
            None => local_version_guard.new_shared_buffer(epoch),
        };
        // The batch will be synced to S3 asynchronously if it is a local batch
        shared_buffer.write_batch(batch);
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn sync_shared_buffer(&self, epoch: HummockEpoch) -> HummockResult<SyncResult> {
        self.seal_epoch(epoch, true);
        self.await_sync_shared_buffer(epoch).await
    }

    /// seal epoch in local version.
    pub fn seal_epoch(&self, epoch: HummockEpoch, is_checkpoint: bool) {
        if let Some(merge_task) = self.local_version.write().seal_epoch(epoch, is_checkpoint) {
            self.buffer_tracker
                .send_event(SharedBufferEvent::CompactMemory(epoch, merge_task));
        }
    }

    pub async fn await_sync_shared_buffer(&self, epoch: HummockEpoch) -> HummockResult<SyncResult> {
        tracing::trace!("sync epoch {}", epoch);

        // Wait all epochs' task that less than epoch.
        let (tx, rx) = oneshot::channel();
        self.buffer_tracker
            .send_event(SharedBufferEvent::SyncEpoch {
                new_sync_epoch: epoch,
                sync_result_sender: tx,
            });

        // TODO: re-enable it when conflict detector has enough information to do conflict detection
        // if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
        //     conflict_detector.archive_epoch(epochs.clone());
        // }

        rx.await.expect("should be able to get result")
    }

    pub async fn run_sync_upload_task(
        &self,
        task_payload: UploadTaskPayload,
        epochs: Vec<HummockEpoch>,
        sync_size: usize,
    ) {
        let epoch = *epochs.last().unwrap();
        match self.shared_buffer_uploader.flush(task_payload, epoch).await {
            Ok(ssts) => {
                self.local_version
                    .write()
                    .data_synced(epochs, ssts, sync_size);
                self.buffer_tracker
                    .global_upload_task_size
                    .fetch_sub(sync_size, Ordering::Relaxed);
                let _ = self
                    .buffer_tracker
                    .buffer_event_sender
                    .send(SharedBufferEvent::FlushEnd(epoch));
            }
            Err(e) => {
                error!("sync upload task failed: {}, err: {:?}", epoch, e);
                self.local_version.write().fail_epoch_sync(epoch, e);
            }
        }
    }

    pub fn read_filter<R, B>(
        self: &LocalVersionManager,
        read_epoch: HummockEpoch,
        key_range: &R,
    ) -> ReadVersion
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        LocalVersion::read_filter(&self.local_version, read_epoch, key_range)
    }

    pub fn get_pinned_version(&self) -> PinnedVersion {
        self.local_version.read().pinned_version().clone()
    }
}

// concurrent worker thread of `LocalVersionManager`
impl LocalVersionManager {
    pub async fn clear_shared_buffer(&self) {
        let (tx, rx) = oneshot::channel();
        self.buffer_tracker.send_event(SharedBufferEvent::Clear(tx));
        rx.await.unwrap();
    }
}

#[cfg(any(test, feature = "test"))]
// Some method specially for tests of `LocalVersionManager`
impl LocalVersionManager {
    pub fn local_version(&self) -> &RwLock<LocalVersion> {
        &self.local_version
    }

    pub fn get_local_version(&self) -> LocalVersion {
        // self.local_version.read().clone()
        unimplemented!("todo")
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }

    pub fn get_sstable_id_manager(&self) -> SstableIdManagerRef {
        self.sstable_id_manager.clone()
    }
}
