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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::RangeBounds;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::{join_all, try_join_all};
use itertools::Itertools;
use parking_lot::{RwLock, RwLockWriteGuard};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
#[cfg(any(test, feature = "test"))]
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::{CompactionGroupId, HummockReadEpoch, LocalSstableInfo};
use risingwave_pb::hummock::pin_version_response;
use risingwave_pb::hummock::pin_version_response::Payload;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_retry::strategy::jitter;
use tracing::{error, info};

use super::local_version::{LocalVersion, PinVersionAction, PinnedVersion, ReadVersion};
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::shared_buffer::shared_buffer_uploader::SharedBufferUploader;
use super::SstableStoreRef;
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::local_version::SyncUncommittedData::{Synced, Syncing};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferItem;
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::shared_buffer::{to_order_sorted, OrderIndex, SharedBufferEvent, WriteRequest};
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, SstableIdManagerRef, TrackerId,
    INVALID_VERSION_ID,
};
use crate::monitor::StateStoreMetrics;
use crate::storage_value::StorageValue;

#[derive(Default)]
pub struct SyncResult {
    /// The size of all synced shared buffers.
    pub sync_size: usize,
    /// The sst_info of sync.
    pub uncommitted_ssts: Vec<LocalSstableInfo>,
    /// If this epoch had been synced, it will be false. So it may be false, if we sync multiple
    /// epochs in one shot.
    pub sync_succeed: bool,
}

struct WorkerContext {
    version_update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
}

struct BufferTracker {
    flush_threshold: usize,
    block_write_threshold: usize,
    global_buffer_size: Arc<AtomicUsize>,
    global_upload_task_size: Arc<AtomicUsize>,

    buffer_event_sender: mpsc::UnboundedSender<SharedBufferEvent>,
}

impl BufferTracker {
    pub fn new(
        flush_threshold: usize,
        block_write_threshold: usize,
        buffer_event_sender: mpsc::UnboundedSender<SharedBufferEvent>,
    ) -> Self {
        assert!(
            flush_threshold <= block_write_threshold,
            "flush threshold {} is not less than block write threshold {}",
            flush_threshold,
            block_write_threshold
        );
        info!(
            "buffer tracker init: flush threshold {}, block write threshold {}",
            flush_threshold, block_write_threshold
        );
        Self {
            flush_threshold,
            block_write_threshold,
            global_buffer_size: Arc::new(AtomicUsize::new(0)),
            global_upload_task_size: Arc::new(AtomicUsize::new(0)),
            buffer_event_sender,
        }
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer_size.load(Relaxed)
    }

    pub fn get_upload_task_size(&self) -> usize {
        self.global_upload_task_size.load(Relaxed)
    }

    pub fn can_write(&self) -> bool {
        self.get_buffer_size() <= self.block_write_threshold
    }

    pub fn try_write(&self, size: usize) -> bool {
        loop {
            let current_size = self.global_buffer_size.load(Acquire);
            if current_size > self.block_write_threshold {
                return false;
            }
            if self
                .global_buffer_size
                .compare_exchange(current_size, current_size + size, Acquire, Acquire)
                .is_ok()
            {
                break true;
            }
        }
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_more_flush(&self) -> bool {
        self.get_buffer_size() > self.flush_threshold + self.get_upload_task_size()
    }

    pub fn send_event(&self, event: SharedBufferEvent) {
        self.buffer_event_sender.send(event).unwrap();
    }
}

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `Sstables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    local_version: RwLock<LocalVersion>,
    worker_context: WorkerContext,
    buffer_tracker: BufferTracker,
    write_conflict_detector: Option<Arc<ConflictDetector>>,
    shared_buffer_uploader: Arc<SharedBufferUploader>,
    sstable_id_manager: SstableIdManagerRef,
}

impl LocalVersionManager {
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
        sstable_id_manager: SstableIdManagerRef,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> Arc<LocalVersionManager> {
        let (pinned_version_manager_tx, pinned_version_manager_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (version_update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);

        let pinned_version = match Self::pin_version_with_retry(
            hummock_meta_client.clone(),
            INVALID_VERSION_ID,
            10,
            // never break until max retry
            || false,
        )
        .await
        .expect("should be `Some` since `break_condition` is always false")
        .expect("should be able to pinned the first version")
        {
            Payload::VersionDeltas(_) => {
                unreachable!("should fetch the full hummock version in initialization")
            }
            Payload::PinnedVersion(version) => version,
        };

        let (buffer_event_sender, buffer_event_receiver) = mpsc::unbounded_channel();

        let capacity = (options.shared_buffer_capacity_mb as usize) * (1 << 20);

        let local_version_manager = Arc::new(LocalVersionManager {
            local_version: RwLock::new(LocalVersion::new(
                pinned_version,
                pinned_version_manager_tx,
            )),
            worker_context: WorkerContext {
                version_update_notifier_tx,
            },
            buffer_tracker: BufferTracker::new(
                // 0.8 * capacity
                // TODO: enable setting the ratio with config
                capacity * 4 / 5,
                capacity,
                buffer_event_sender,
            ),
            write_conflict_detector: write_conflict_detector.clone(),

            shared_buffer_uploader: Arc::new(SharedBufferUploader::new(
                options.clone(),
                sstable_store,
                hummock_meta_client.clone(),
                stats,
                write_conflict_detector,
                sstable_id_manager.clone(),
                filter_key_extractor_manager.clone(),
            )),
            sstable_id_manager,
        });

        // Unpin unused version.
        tokio::spawn(LocalVersionManager::start_pinned_version_worker(
            pinned_version_manager_rx,
            hummock_meta_client,
        ));

        // Buffer size manager.
        tokio::spawn(LocalVersionManager::start_buffer_tracker_worker(
            local_version_manager.clone(),
            buffer_event_receiver,
        ));

        local_version_manager
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
    ) -> Arc<LocalVersionManager> {
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
        .await
    }

    /// Updates cached version if the new version is of greater id.
    /// You shouldn't unpin even the method returns false, as it is possible `hummock_version` is
    /// being referenced by some readers.
    pub fn try_update_pinned_version(
        &self,
        last_pinned: Option<u64>,
        pin_resp_payload: pin_version_response::Payload,
    ) -> bool {
        let old_version = self.local_version.read();
        if let Some(last_pinned_id) = last_pinned {
            if old_version.pinned_version().id() != last_pinned_id {
                return false;
            }
        }

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

    /// Waits until the local hummock version contains the epoch. We will wait for different epochs
    /// according to `HummockReadEpoch`'s type
    pub async fn wait_epoch(&self, wait_epoch: HummockReadEpoch) -> HummockResult<()> {
        if wait_epoch.get_epoch() == HummockEpoch::MAX {
            panic!("epoch should not be u64::MAX");
        }
        let mut receiver = self.worker_context.version_update_notifier_tx.subscribe();
        loop {
            let (pinned_version_id, pinned_version_epoch) = {
                let current_version = self.local_version.read();
                match wait_epoch {
                    HummockReadEpoch::Committed(epoch) => {
                        if current_version.pinned_version().max_committed_epoch() >= epoch {
                            return Ok(());
                        }
                        (
                            current_version.pinned_version().id(),
                            current_version.pinned_version().max_committed_epoch(),
                        )
                    }
                    HummockReadEpoch::Current(epoch) => {
                        if current_version.pinned_version().max_current_epoch() >= epoch {
                            return Ok(());
                        }
                        (
                            current_version.pinned_version().id(),
                            current_version.pinned_version().max_current_epoch(),
                        )
                    }
                    HummockReadEpoch::NoWait(_) => panic!("No wait can't wait epoch"),
                }
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
        is_remote_batch: bool,
        table_id: u32,
    ) -> HummockResult<usize> {
        let sorted_items = Self::build_shared_buffer_item_batches(kv_pairs, epoch);
        let batch = SharedBufferBatch::new(
            sorted_items,
            epoch,
            self.buffer_tracker.buffer_event_sender.clone(),
            compaction_group_id,
            table_id,
        );
        let batch_size = batch.size();
        if self.buffer_tracker.try_write(batch_size) {
            self.write_shared_buffer_inner(epoch, batch, is_remote_batch);
            self.buffer_tracker.send_event(SharedBufferEvent::MayFlush);
        } else {
            let (tx, rx) = oneshot::channel();
            self.buffer_tracker
                .send_event(SharedBufferEvent::WriteRequest(WriteRequest {
                    batch,
                    epoch,
                    is_remote_batch,
                    grant_sender: tx,
                }));
            rx.await.unwrap();
        }
        Ok(batch_size)
    }

    fn write_shared_buffer_inner(
        &self,
        epoch: HummockEpoch,
        batch: SharedBufferBatch,
        is_remote_batch: bool,
    ) {
        let mut local_version_guard = self.local_version.write();
        let max_sync_epoch = local_version_guard.get_max_sync_epoch();
        assert!(
            epoch > max_sync_epoch,
            "write epoch must greater than sync epoch, write epoch{}, sync epoch{}",
            epoch,
            max_sync_epoch
        );
        // Write into shared buffer
        if is_remote_batch {
            // The batch won't be synced to S3 asynchronously if it is a remote batch
            local_version_guard.replicate_batch(epoch, batch);
        } else {
            let shared_buffer = match local_version_guard.get_mut_shared_buffer(epoch) {
                Some(shared_buffer) => shared_buffer,
                None => local_version_guard
                    .new_shared_buffer(epoch, self.buffer_tracker.global_upload_task_size.clone()),
            };
            // The batch will be synced to S3 asynchronously if it is a local batch
            shared_buffer.write_batch(batch);
        }

        // Notify the buffer tracker after the batch has been added to shared buffer.
        self.buffer_tracker.send_event(SharedBufferEvent::MayFlush);
    }

    /// Issue a concurrent upload task to flush some local shared buffer batch to object store.
    ///
    /// This method should only be called in the buffer tracker worker.
    ///
    /// Return:
    ///   - Some(task join handle) when there is new upload task
    ///   - None when there is no new task
    pub fn flush_shared_buffer(
        self: Arc<Self>,
        syncing_epoch: &HashSet<HummockEpoch>,
    ) -> Option<(HummockEpoch, JoinHandle<()>)> {
        // The current implementation is a trivial one, which issue only one flush task and wait for
        // the task to finish.
        let mut task = None;
        for (epoch, shared_buffer) in self.local_version.write().iter_mut_shared_buffer() {
            // skip the epoch that is being synced
            if syncing_epoch.get(epoch).is_some() {
                continue;
            }
            if let Some(upload_task) = shared_buffer.new_upload_task() {
                task = Some((*epoch, upload_task));
                break;
            }
        }
        let (epoch, (order_index, payload, task_write_batch_size)) = match task {
            Some(task) => task,
            None => return None,
        };

        let join_handle = tokio::spawn(async move {
            info!(
                "running flush task in epoch {} of size {}",
                epoch, task_write_batch_size
            );
            // TODO: may apply different `is_local` according to whether local spill is enabled.
            let _ = self
                .run_flush_upload_task(order_index, epoch, payload)
                .await
                .inspect_err(|err| {
                    error!(
                        "upload task fail. epoch: {}, order_index: {}. Err: {:?}",
                        epoch, order_index, err
                    );
                });
            info!(
                "flush task in epoch {} of size {} finished",
                epoch, task_write_batch_size
            );
        });
        Some((epoch, join_handle))
    }

    pub async fn sync_shared_buffer(&self, epoch: HummockEpoch) -> HummockResult<SyncResult> {
        tracing::trace!("sync epoch {}", epoch);
        // Wait all epochs' task that less than epoch.
        let epochs = self
            .local_version
            .read()
            .scan_shared_buffer(..=epoch)
            .map(|(&key, _)| key)
            .collect_vec();
        let (tx, rx) = oneshot::channel();
        self.buffer_tracker
            .send_event(SharedBufferEvent::SyncEpoch(epochs, tx));
        let join_handles = rx.await.unwrap();
        for result in join_all(join_handles).await {
            result.expect("should be able to join the flush handle")
        }
        self.sync_shared_buffer_le_epoch(epoch).await
    }

    /// Sync all shared buffer that less than or equal to the `epoch`.
    pub async fn sync_shared_buffer_le_epoch(
        &self,
        epoch: HummockEpoch,
    ) -> HummockResult<SyncResult> {
        // Get epochs that less than epoch and add to sync_uncommitted_data, we must keep the write
        // lock.
        let (task_payload, epochs, sync_size) = {
            let mut local_version_guard = self.local_version.write();
            let prev_max_sync_epoch = match local_version_guard.swap_max_sync_epoch(epoch) {
                Some(epoch) => epoch,
                None => {
                    return Ok(SyncResult::default());
                }
            };
            let mut sync_size = 0;
            let mut all_uncommitted_data = vec![];
            let mut epochs = local_version_guard
                .drain_shared_buffer(..=epoch)
                .into_iter()
                .map(|(key, value)| {
                    if let Some((uncommitted_data, size)) = value.into_uncommitted_data() {
                        all_uncommitted_data.push(uncommitted_data);
                        sync_size += size;
                    };
                    key
                })
                .collect_vec();
            assert!(
                epochs
                    .iter()
                    .all(|epoch| *epoch > prev_max_sync_epoch),
                "data is written to shared buffer of older than the max_sync_epoch {}. Synced epoch: {:?}",
                prev_max_sync_epoch,
                epochs,
            );
            if epochs.is_empty() {
                tracing::trace!("sync epoch {} has no more task to do", epoch);
                return Ok(SyncResult {
                    sync_succeed: true,
                    ..Default::default()
                });
            }
            // Data of smaller epoch was added first. Take a `reverse` to make the data of greater
            // epoch appear first.
            all_uncommitted_data.reverse();
            epochs.reverse();
            let task_payload = all_uncommitted_data
                .into_iter()
                .flat_map(to_order_sorted)
                .collect_vec();
            local_version_guard.add_sync_state(epochs.clone(), Syncing(task_payload.clone()));
            (task_payload, epochs, sync_size)
        };
        let uncommitted_ssts = self
            .run_sync_upload_task(task_payload, epochs.clone(), epoch)
            .await?;
        tracing::trace!("sync epoch {} finished. Task size {}", epoch, sync_size);
        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.archive_epoch(epochs.clone());
        }
        self.buffer_tracker
            .send_event(SharedBufferEvent::EpochSynced(epochs));
        Ok(SyncResult {
            sync_size,
            uncommitted_ssts,
            sync_succeed: true,
        })
    }

    pub async fn run_sync_upload_task(
        &self,
        task_payload: UploadTaskPayload,
        epochs: Vec<HummockEpoch>,
        epoch: HummockEpoch,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        let ssts = self
            .shared_buffer_uploader
            .flush(task_payload, *epochs.last().unwrap(), epoch)
            .await?;
        self.local_version
            .write()
            .add_sync_state(epochs.clone(), Synced(ssts.clone()));
        Ok(ssts)
    }

    async fn run_flush_upload_task(
        &self,
        order_index: OrderIndex,
        epoch: HummockEpoch,
        task_payload: UploadTaskPayload,
    ) -> HummockResult<()> {
        let task_result = self
            .shared_buffer_uploader
            .flush(task_payload, epoch, epoch)
            .await;

        let mut local_version_guard = self.local_version.write();
        let shared_buffer_guard = local_version_guard
            .get_mut_shared_buffer(epoch)
            .expect("shared buffer should exist since some uncommitted data is not committed yet");

        let ret = match task_result {
            Ok(ssts) => {
                shared_buffer_guard.succeed_upload_task(order_index, ssts);
                Ok(())
            }
            Err(e) => {
                shared_buffer_guard.fail_upload_task(order_index);
                Err(e)
            }
        };
        self.buffer_tracker.send_event(SharedBufferEvent::MayFlush);
        ret
    }

    pub fn read_filter<R, B>(
        self: &Arc<LocalVersionManager>,
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

    /// Pin a version with retry.
    ///
    /// Return:
    ///   - `Some(Ok(pinned_version))` if success
    ///   - `Some(Err(err))` if exceed the retry limit
    ///   - `None` if meet the break condition
    async fn pin_version_with_retry(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        last_pinned: HummockVersionId,
        max_retry: usize,
        break_condition: impl Fn() -> bool,
    ) -> Option<HummockResult<pin_version_response::Payload>> {
        let max_retry_interval = Duration::from_secs(10);
        let mut retry_backoff = tokio_retry::strategy::ExponentialBackoff::from_millis(10)
            .max_delay(max_retry_interval)
            .map(jitter);

        let mut retry_count = 0;
        loop {
            if retry_count > max_retry {
                break Some(Err(HummockError::meta_error(format!(
                    "pin_version max retry reached: {}.",
                    max_retry
                ))));
            }
            if break_condition() {
                break None;
            }
            match hummock_meta_client.pin_version(last_pinned).await {
                Ok(version) => {
                    break Some(Ok(version));
                }
                Err(err) => {
                    let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                    tracing::warn!(
                        "Failed to pin version {:?}. Will retry after about {} milliseconds",
                        err,
                        retry_after.as_millis()
                    );
                    tokio::time::sleep(retry_after).await;
                }
            }
            retry_count += 1;
        }
    }
}

// concurrent worker thread of `LocalVersionManager`
impl LocalVersionManager {
    async fn start_pinned_version_worker(
        mut rx: UnboundedReceiver<PinVersionAction>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let min_execute_interval = Duration::from_millis(1000);
        let max_retry_interval = Duration::from_secs(10);
        let get_backoff_strategy = || {
            tokio_retry::strategy::ExponentialBackoff::from_millis(10)
                .max_delay(max_retry_interval)
                .map(jitter)
        };
        let mut retry_backoff = get_backoff_strategy();
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut need_unpin = false;

        let mut version_ids_in_use: BTreeMap<u64, usize> = BTreeMap::new();

        // For each run in the loop, accumulate versions to unpin and call unpin RPC once.
        loop {
            min_execute_interval_tick.tick().await;
            // 1. Collect new versions to unpin.
            let mut versions_to_unpin = vec![];
            'collect: loop {
                match rx.try_recv() {
                    Ok(version_action) => match version_action {
                        PinVersionAction::Pin(version_id) => {
                            version_ids_in_use
                                .entry(version_id)
                                .and_modify(|counter| *counter += 1)
                                .or_insert(1);
                        }
                        PinVersionAction::Unpin(version_id) => {
                            versions_to_unpin.push(version_id);
                        }
                    },
                    Err(err) => match err {
                        TryRecvError::Empty => {
                            break 'collect;
                        }
                        TryRecvError::Disconnected => {
                            tracing::info!("Shutdown hummock unpin worker");
                            return;
                        }
                    },
                }
            }
            if !versions_to_unpin.is_empty() {
                need_unpin = true;
            }
            if !need_unpin {
                continue;
            }

            for version in &versions_to_unpin {
                match version_ids_in_use.get_mut(version) {
                    Some(counter) => {
                        *counter -= 1;
                        if *counter == 0 {
                            version_ids_in_use.remove(version);
                        }
                    }
                    None => tracing::warn!("version {} to unpin dose not exist", version),
                }
            }

            match version_ids_in_use.first_entry() {
                Some(unpin_before) => {
                    // 2. Call unpin RPC, including versions failed to unpin in previous RPC calls.
                    match hummock_meta_client
                        .unpin_version_before(*unpin_before.key())
                        .await
                    {
                        Ok(_) => {
                            versions_to_unpin.clear();
                            need_unpin = false;
                            retry_backoff = get_backoff_strategy();
                        }
                        Err(err) => {
                            let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                            tracing::warn!(
                            "Failed to unpin version {:?}. Will retry after about {} milliseconds",
                            err,
                            retry_after.as_millis()
                        );
                            tokio::time::sleep(retry_after).await;
                        }
                    }
                }
                None => tracing::warn!("version_ids_in_use is empty!"),
            }
        }
    }

    pub async fn start_buffer_tracker_worker(
        local_version_manager: Arc<LocalVersionManager>,
        mut buffer_size_change_receiver: mpsc::UnboundedReceiver<SharedBufferEvent>,
    ) {
        let mut syncing_epoch = HashSet::new();
        let mut epoch_join_handle = HashMap::new();
        let try_flush_shared_buffer =
            |syncing_epoch: &HashSet<HummockEpoch>,
             epoch_join_handle: &mut HashMap<HummockEpoch, Vec<JoinHandle<()>>>| {
                // Keep issuing new flush task until flush is not needed or we can issue
                // no more task
                while local_version_manager.buffer_tracker.need_more_flush() {
                    if let Some((epoch, join_handle)) = local_version_manager
                        .clone()
                        .flush_shared_buffer(syncing_epoch)
                    {
                        epoch_join_handle
                            .entry(epoch)
                            .or_insert_with(Vec::new)
                            .push(join_handle);
                    } else {
                        break;
                    }
                }
            };

        let grant_write_request = |request| {
            let WriteRequest {
                batch,
                epoch,
                is_remote_batch,
                grant_sender: sender,
            } = request;
            let size = batch.size();
            local_version_manager.write_shared_buffer_inner(epoch, batch, is_remote_batch);
            local_version_manager
                .buffer_tracker
                .global_buffer_size
                .fetch_add(size, Relaxed);
            let _ = sender.send(()).inspect_err(|err| {
                error!("unable to send write request response: {:?}", err);
            });
        };

        let mut pending_write_requests: VecDeque<_> = VecDeque::new();

        // While the current Arc is not the only strong reference to the local version manager
        while Arc::strong_count(&local_version_manager) > 1 {
            if let Some(event) = buffer_size_change_receiver.recv().await {
                match event {
                    SharedBufferEvent::WriteRequest(request) => {
                        if local_version_manager.buffer_tracker.can_write() {
                            grant_write_request(request);
                            try_flush_shared_buffer(&syncing_epoch, &mut epoch_join_handle);
                        } else {
                            info!(
                                "write request is blocked: epoch {}, size: {}",
                                request.epoch,
                                request.batch.size()
                            );
                            pending_write_requests.push_back(request);
                        }
                    }
                    SharedBufferEvent::MayFlush => {
                        // Only check and flush shared buffer after batch has been added to shared
                        // buffer.
                        try_flush_shared_buffer(&syncing_epoch, &mut epoch_join_handle);
                    }
                    SharedBufferEvent::BufferRelease(size) => {
                        local_version_manager
                            .buffer_tracker
                            .global_buffer_size
                            .fetch_sub(size, Relaxed);
                        let mut has_granted = false;
                        while !pending_write_requests.is_empty()
                            && local_version_manager.buffer_tracker.can_write()
                        {
                            let request = pending_write_requests.pop_front().unwrap();
                            info!(
                                "write request is granted: epoch {}, size: {}",
                                request.epoch,
                                request.batch.size()
                            );
                            grant_write_request(request);
                            has_granted = true;
                        }
                        if has_granted {
                            try_flush_shared_buffer(&syncing_epoch, &mut epoch_join_handle);
                        }
                    }
                    SharedBufferEvent::SyncEpoch(epochs, join_handle_sender) => {
                        let join_handle = epochs
                            .into_iter()
                            .flat_map(|epoch| {
                                syncing_epoch.insert(epoch);
                                epoch_join_handle.remove(&epoch).unwrap_or_default()
                            })
                            .collect_vec();
                        let _ = join_handle_sender.send(join_handle).inspect_err(|e| {
                            error!("unable to send join handles Err {:?}", e);
                        });
                    }
                    SharedBufferEvent::EpochSynced(epochs) => {
                        epochs.into_iter().for_each(|epoch| {
                            assert!(
                                syncing_epoch.remove(&epoch),
                                "removing a previous not synced epoch {}",
                                epoch
                            )
                        });
                    }

                    SharedBufferEvent::Clear(notifier) => {
                        // Wait for all ongoing flush to finish.
                        let ongoing_flush_handles: Vec<_> =
                            epoch_join_handle.drain().flat_map(|e| e.1).collect();
                        if let Err(e) = try_join_all(ongoing_flush_handles).await {
                            error!("Failed to join flush handle {:?}", e)
                        }

                        // There cannot be any pending write requests since we should only clear
                        // shared buffer after all actors stop processing data.
                        assert!(pending_write_requests.is_empty());

                        // Clear shared buffer
                        let cleaned_epochs = local_version_manager
                            .local_version
                            .write()
                            .clear_shared_buffer();
                        for cleaned_epoch in cleaned_epochs {
                            local_version_manager
                                .sstable_id_manager
                                .remove_watermark_sst_id(TrackerId::Epoch(cleaned_epoch));
                        }

                        // Notify completion of the Clear event.
                        notifier.send(()).unwrap();
                    }
                };
            } else {
                break;
            }
        }
    }

    pub async fn clear_shared_buffer(&self) {
        let (tx, rx) = oneshot::channel();
        self.buffer_tracker.send_event(SharedBufferEvent::Clear(tx));
        rx.await.unwrap();
    }
}

#[cfg(any(test, feature = "test"))]
// Some method specially for tests of `LocalVersionManager`
impl LocalVersionManager {
    pub async fn refresh_version(&self, hummock_meta_client: &dyn HummockMetaClient) -> bool {
        let last_pinned = self.get_pinned_version().id();
        let version = hummock_meta_client.pin_version(last_pinned).await.unwrap();
        self.try_update_pinned_version(Some(last_pinned), version)
    }

    pub fn local_version(&self) -> &RwLock<LocalVersion> {
        &self.local_version
    }

    pub fn get_local_version(&self) -> LocalVersion {
        self.local_version.read().clone()
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }

    pub fn get_sstable_id_manager(&self) -> SstableIdManagerRef {
        self.sstable_id_manager.clone()
    }
}
