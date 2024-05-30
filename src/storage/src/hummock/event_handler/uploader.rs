// Copyright 2024 RisingWave Labs
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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::future::{poll_fn, Future};
use std::mem::take;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures::future::{try_join_all, TryJoinAll};
use futures::FutureExt;
use itertools::Itertools;
use more_asserts::{assert_ge, assert_gt};
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::{HistogramTimer, IntGauge};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarks, VnodeWatermark, WatermarkDirection,
};
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, LocalSstableInfo};
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::store::version::StagingSstableInfo;
use crate::hummock::{HummockError, HummockResult, ImmutableMemtable};
use crate::mem_table::ImmId;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

pub type UploadTaskInput = HashMap<LocalInstanceId, Vec<ImmutableMemtable>>;
pub type UploadTaskPayload = Vec<ImmutableMemtable>;

#[derive(Debug)]
pub struct UploadTaskOutput {
    pub new_value_ssts: Vec<LocalSstableInfo>,
    pub old_value_ssts: Vec<LocalSstableInfo>,
    pub wait_poll_timer: Option<HistogramTimer>,
}
pub type SpawnUploadTask = Arc<
    dyn Fn(UploadTaskInput, UploadTaskInfo) -> JoinHandle<HummockResult<UploadTaskOutput>>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone)]
pub struct UploadTaskInfo {
    pub task_size: usize,
    pub epochs: Vec<HummockEpoch>,
    pub imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>,
    pub compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
}

impl Display for UploadTaskInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadTaskInfo")
            .field("task_size", &self.task_size)
            .field("epochs", &self.epochs)
            .field("len(imm_ids)", &self.imm_ids.len())
            .finish()
    }
}

impl Debug for UploadTaskInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadTaskInfo")
            .field("task_size", &self.task_size)
            .field("epochs", &self.epochs)
            .field("imm_ids", &self.imm_ids)
            .finish()
    }
}

/// A wrapper for a uploading task that compacts and uploads the imm payload. Task context are
/// stored so that when the task fails, it can be re-tried.
struct UploadingTask {
    // newer data at the front
    payload: UploadTaskInput,
    join_handle: JoinHandle<HummockResult<UploadTaskOutput>>,
    task_info: UploadTaskInfo,
    spawn_upload_task: SpawnUploadTask,
    task_size_guard: GenericGauge<AtomicU64>,
    task_count_guard: IntGauge,
}

impl Drop for UploadingTask {
    fn drop(&mut self) {
        self.task_size_guard.sub(self.task_info.task_size as u64);
        self.task_count_guard.dec();
    }
}

impl Debug for UploadingTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadingTask")
            .field("payload", &self.payload)
            .field("task_info", &self.task_info)
            .finish()
    }
}

impl UploadingTask {
    // INFO logs will be enabled for task with size exceeding 50MB.
    const LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE: usize = 50 * (1 << 20);

    fn new(payload: UploadTaskInput, context: &UploaderContext) -> Self {
        assert!(!payload.is_empty());
        let mut epochs = payload
            .iter()
            .flat_map(|(_, imms)| imms.iter().flat_map(|imm| imm.epochs().iter().cloned()))
            .sorted()
            .dedup()
            .collect_vec();

        // reverse to make newer epochs comes first
        epochs.reverse();
        let imm_ids = payload
            .iter()
            .map(|(instance_id, imms)| {
                (
                    *instance_id,
                    imms.iter().map(|imm| imm.batch_id()).collect_vec(),
                )
            })
            .collect();
        let task_size = payload
            .values()
            .map(|imms| imms.iter().map(|imm| imm.size()).sum::<usize>())
            .sum();
        let task_info = UploadTaskInfo {
            task_size,
            epochs,
            imm_ids,
            compaction_group_index: context.pinned_version.compaction_group_index(),
        };
        context
            .buffer_tracker
            .global_upload_task_size()
            .add(task_size as u64);
        if task_info.task_size > Self::LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE {
            info!("start upload task: {:?}", task_info);
        } else {
            debug!("start upload task: {:?}", task_info);
        }
        let join_handle = (context.spawn_upload_task)(payload.clone(), task_info.clone());
        context.stats.uploader_uploading_task_count.inc();
        Self {
            payload,
            join_handle,
            task_info,
            spawn_upload_task: context.spawn_upload_task.clone(),
            task_size_guard: context.buffer_tracker.global_upload_task_size().clone(),
            task_count_guard: context.stats.uploader_uploading_task_count.clone(),
        }
    }

    /// Poll the result of the uploading task
    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<HummockResult<StagingSstableInfo>> {
        Poll::Ready(match ready!(self.join_handle.poll_unpin(cx)) {
            Ok(task_result) => task_result
                .inspect(|_| {
                    if self.task_info.task_size > Self::LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE {
                        info!(task_info = ?self.task_info, "upload task finish");
                    } else {
                        debug!(task_info = ?self.task_info, "upload task finish");
                    }
                })
                .inspect_err(|e| error!(task_info = ?self.task_info, err = ?e.as_report(), "upload task failed"))
                .map(|output| {
                    StagingSstableInfo::new(
                        output.new_value_ssts,
                        output.old_value_ssts,
                        self.task_info.epochs.clone(),
                        self.task_info.imm_ids.clone(),
                        self.task_info.task_size,
                    )
                }),

            Err(err) => Err(HummockError::other(format!(
                "fail to join upload join handle: {}",
                err.as_report()
            ))),
        })
    }

    /// Poll the uploading task until it succeeds. If it fails, we will retry it.
    fn poll_ok_with_retry(&mut self, cx: &mut Context<'_>) -> Poll<StagingSstableInfo> {
        loop {
            let result = ready!(self.poll_result(cx));
            match result {
                Ok(sstables) => return Poll::Ready(sstables),
                Err(e) => {
                    error!(
                        error = %e.as_report(),
                        task_info = ?self.task_info,
                        "a flush task failed, start retry",
                    );
                    self.join_handle =
                        (self.spawn_upload_task)(self.payload.clone(), self.task_info.clone());
                    // It is important not to return Poll::pending here immediately, because the new
                    // join_handle is not polled yet, and will not awake the current task when
                    // succeed. It will be polled in the next loop iteration.
                }
            }
        }
    }

    pub fn get_task_info(&self) -> &UploadTaskInfo {
        &self.task_info
    }
}

impl Future for UploadingTask {
    type Output = HummockResult<StagingSstableInfo>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_result(cx)
    }
}

#[derive(Default, Debug)]
/// Manage the spilled data. Task and uploaded data at the front is newer data. Task data are
/// always newer than uploaded data. Task holding oldest data is always collected first.
struct SpilledData {
    // ordered spilling tasks. Task at the back is spilling older data.
    uploading_tasks: VecDeque<UploadingTask>,
    // ordered spilled data. Data at the back is older.
    uploaded_data: VecDeque<StagingSstableInfo>,
}

impl SpilledData {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.uploading_tasks.is_empty() && self.uploaded_data.is_empty()
    }

    fn add_task(&mut self, task: UploadingTask) {
        self.uploading_tasks.push_front(task);
    }

    /// Poll the successful spill of the oldest uploading task. Return `Poll::Ready(None)` is there
    /// is no uploading task
    fn poll_success_spill(&mut self, cx: &mut Context<'_>) -> Poll<Option<StagingSstableInfo>> {
        // only poll the oldest uploading task if there is any
        if let Some(task) = self.uploading_tasks.back_mut() {
            let staging_sstable_info = ready!(task.poll_ok_with_retry(cx));
            self.uploaded_data.push_front(staging_sstable_info.clone());
            self.uploading_tasks.pop_back();
            Poll::Ready(Some(staging_sstable_info))
        } else {
            Poll::Ready(None)
        }
    }

    fn clear(&mut self) {
        for task in self.uploading_tasks.drain(..) {
            task.join_handle.abort();
        }
        self.uploaded_data.clear();
    }
}

#[derive(Default, Debug)]
struct UnsealedEpochData {
    // newer data at the front
    imms: HashMap<LocalInstanceId, VecDeque<ImmutableMemtable>>,
    spilled_data: SpilledData,

    table_watermarks: HashMap<TableId, (WatermarkDirection, Vec<VnodeWatermark>, BitmapBuilder)>,
}

impl UnsealedEpochData {
    fn flush(&mut self, context: &UploaderContext) {
        let imms: HashMap<_, _> = take(&mut self.imms)
            .into_iter()
            .map(|(id, imms)| (id, imms.into_iter().collect_vec()))
            .collect();
        if !imms.is_empty() {
            let task = UploadingTask::new(imms, context);
            context.stats.spill_task_counts_from_unsealed.inc();
            context
                .stats
                .spill_task_size_from_unsealed
                .inc_by(task.task_info.task_size as u64);
            info!("Spill unsealed data. Task: {}", task.get_task_info());
            self.spilled_data.add_task(task);
        }
    }

    fn add_table_watermarks(
        &mut self,
        table_id: TableId,
        table_watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) {
        fn apply_new_vnodes(
            vnode_bitmap: &mut BitmapBuilder,
            vnode_watermarks: &Vec<VnodeWatermark>,
        ) {
            for vnode_watermark in vnode_watermarks {
                for vnode in vnode_watermark.vnode_bitmap().iter_ones() {
                    assert!(
                        !vnode_bitmap.is_set(vnode),
                        "vnode {} write multiple table watermarks",
                        vnode
                    );
                    vnode_bitmap.set(vnode, true);
                }
            }
        }
        match self.table_watermarks.entry(table_id) {
            Entry::Occupied(mut entry) => {
                let (prev_direction, prev_watermarks, vnode_bitmap) = entry.get_mut();
                assert_eq!(
                    *prev_direction, direction,
                    "table id {} new watermark direction not match with previous",
                    table_id
                );
                apply_new_vnodes(vnode_bitmap, &table_watermarks);
                prev_watermarks.extend(table_watermarks);
            }
            Entry::Vacant(entry) => {
                let mut vnode_bitmap = BitmapBuilder::zeroed(VirtualNode::COUNT);
                apply_new_vnodes(&mut vnode_bitmap, &table_watermarks);
                entry.insert((direction, table_watermarks, vnode_bitmap));
            }
        }
    }
}

#[derive(Default)]
/// Data at the sealed stage. We will ensure that data in `imms` are newer than the data in the
/// `spilled_data`, and that data in the `uploading_tasks` in `spilled_data` are newer than data in
/// the `uploaded_data` in `spilled_data`.
struct SealedData {
    // newer epochs come first
    epochs: VecDeque<HummockEpoch>,

    // Sealed imms grouped by table shard.
    // newer data (larger imm id) at the front
    imms_by_table_shard: HashMap<LocalInstanceId, VecDeque<ImmutableMemtable>>,

    spilled_data: SpilledData,

    table_watermarks: HashMap<TableId, TableWatermarks>,
}

impl SealedData {
    fn clear(&mut self) {
        self.spilled_data.clear();
        *self = Self::default();
    }

    /// Add the data of a newly sealed epoch.
    ///
    /// Note: it may happen that, for example, currently we hold `imms` and `spilled_data` of epoch
    /// 3,  and after we add the spilled data of epoch 4, both `imms` and `spilled_data` hold data
    /// of both epoch 3 and 4, which seems breaking the rules that data in `imms` are
    /// always newer than data in `spilled_data`, because epoch 3 data of `imms`
    /// seems older than epoch 4 data of `spilled_data`. However, if this happens, the epoch 3
    /// data of `imms` must not overlap with the epoch 4 data of `spilled_data`. The explanation is
    /// as followed:
    ///
    /// First, unsealed data has 3 stages, from earlier to later, imms, uploading task, and
    /// uploaded. When we try to spill unsealed data, we first pick the imms of older epoch until
    /// the imms of older epoch are all picked. When we try to poll the uploading tasks of unsealed
    /// data, we first poll the task of older epoch, until there is no uploading task in older
    /// epoch. Therefore, we can reach that, if two data are in the same stage, but
    /// different epochs, data in the older epoch will always enter the next stage earlier than data
    /// in the newer epoch.
    ///
    /// Second, we have an assumption that, if a key has been written in a newer epoch, e.g. epoch4,
    /// it will no longer be written in an older epoch, e.g. epoch3, and then, if two data of the
    /// same key are at the imm stage, the data of older epoch must appear earlier than the data
    /// of newer epoch.
    ///
    /// Based on the two points above, we can reach that, if two data of a same key appear in
    /// different epochs, the data of older epoch will not appear at a later stage than the data
    /// of newer epoch. Therefore, we can safely merge the data of each stage when we seal an epoch.
    fn seal_new_epoch(&mut self, epoch: HummockEpoch, mut unseal_epoch_data: UnsealedEpochData) {
        if let Some(prev_max_sealed_epoch) = self.epochs.front() {
            assert!(
                epoch > *prev_max_sealed_epoch,
                "epoch {} to seal not greater than prev max sealed epoch {}",
                epoch,
                prev_max_sealed_epoch
            );
        }

        // rearrange sealed imms by table shard and in epoch descending order
        for (instance_id, imms) in unseal_epoch_data.imms {
            let queue = self.imms_by_table_shard.entry(instance_id).or_default();
            for imm in imms.into_iter().rev() {
                if let Some(front) = queue.front() {
                    assert_gt!(imm.batch_id(), front.batch_id());
                }
                queue.push_front(imm);
            }
        }

        self.epochs.push_front(epoch);
        unseal_epoch_data
            .spilled_data
            .uploading_tasks
            .append(&mut self.spilled_data.uploading_tasks);
        unseal_epoch_data
            .spilled_data
            .uploaded_data
            .append(&mut self.spilled_data.uploaded_data);
        self.spilled_data.uploading_tasks = unseal_epoch_data.spilled_data.uploading_tasks;
        self.spilled_data.uploaded_data = unseal_epoch_data.spilled_data.uploaded_data;
        for (table_id, (direction, watermarks, _)) in unseal_epoch_data.table_watermarks {
            match self.table_watermarks.entry(table_id) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().add_new_epoch_watermarks(
                        epoch,
                        Arc::from(watermarks),
                        direction,
                    );
                }
                Entry::Vacant(entry) => {
                    entry.insert(TableWatermarks::single_epoch(epoch, watermarks, direction));
                }
            };
        }
    }

    // Flush can be triggered by either a sync_epoch or a spill (`may_flush`) request.
    fn flush(&mut self, context: &UploaderContext, is_spilled: bool) {
        let payload: HashMap<_, _> = take(&mut self.imms_by_table_shard)
            .into_iter()
            .map(|(id, imms)| (id, imms.into_iter().collect()))
            .collect();

        if !payload.is_empty() {
            let task = UploadingTask::new(payload, context);
            if is_spilled {
                context.stats.spill_task_counts_from_sealed.inc();
                context
                    .stats
                    .spill_task_size_from_sealed
                    .inc_by(task.task_info.task_size as u64);
                info!("Spill sealed data. Task: {}", task.get_task_info());
            }
            self.spilled_data.add_task(task);
        }
    }

    /// Clear self and return the current sealed data
    fn drain(&mut self) -> SealedData {
        take(self)
    }

    #[cfg(test)]
    fn imm_count(&self) -> usize {
        self.imms_by_table_shard
            .values()
            .map(|imms| imms.len())
            .sum()
    }
}

struct SyncingData {
    // newer epochs come first
    epochs: Vec<HummockEpoch>,
    // TODO: may replace `TryJoinAll` with a future that will abort other join handles once
    // one join handle failed.
    // None means there is no pending uploading tasks
    uploading_tasks: Option<TryJoinAll<UploadingTask>>,
    // newer data at the front
    uploaded: VecDeque<StagingSstableInfo>,
    table_watermarks: HashMap<TableId, TableWatermarks>,
}

impl SyncingData {
    fn sync_epoch(&self) -> HummockEpoch {
        *self.epochs.first().expect("non-empty")
    }
}

pub struct SyncedData {
    pub staging_ssts: Vec<StagingSstableInfo>,
    pub table_watermarks: HashMap<TableId, TableWatermarks>,
}

// newer staging sstable info at the front
type SyncedDataState = HummockResult<SyncedData>;

struct UploaderContext {
    pinned_version: PinnedVersion,
    /// When called, it will spawn a task to flush the imm into sst and return the join handle.
    spawn_upload_task: SpawnUploadTask,
    buffer_tracker: BufferTracker,

    stats: Arc<HummockStateStoreMetrics>,
}

impl UploaderContext {
    fn new(
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
        _config: &StorageOpts,
        stats: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        UploaderContext {
            pinned_version,
            spawn_upload_task,
            buffer_tracker,
            stats,
        }
    }
}

/// An uploader for hummock data.
///
/// Data have 4 sequential stages: unsealed, sealed, syncing, synced.
///
/// The 4 stages are divided by 3 marginal epochs: `max_sealed_epoch`, `max_syncing_epoch`,
/// `max_synced_epoch`. Epochs satisfy the following inequality.
///
/// (epochs of `synced_data`) <= `max_synced_epoch` < (epochs of `syncing_data`) <=
/// `max_syncing_epoch` < (epochs of `sealed_data`) <= `max_sealed_epoch` < (epochs of
/// `unsealed_data`)
///
/// Data are mostly stored in `VecDeque`, and the order stored in the `VecDeque` indicates the data
/// order. Data at the front represents ***newer*** data.
pub struct HummockUploader {
    /// The maximum epoch that is sealed
    max_sealed_epoch: HummockEpoch,
    /// The maximum epoch that has started syncing
    max_syncing_epoch: HummockEpoch,
    /// The maximum epoch that has been synced
    max_synced_epoch: HummockEpoch,

    /// Data that are not sealed yet. `epoch` satisfies `epoch > max_sealed_epoch`.
    unsealed_data: BTreeMap<HummockEpoch, UnsealedEpochData>,

    /// Data that are sealed but not synced yet. `epoch` satisfies
    /// `max_syncing_epoch < epoch <= max_sealed_epoch`.
    sealed_data: SealedData,

    /// Data that has started syncing but not synced yet. `epoch` satisfies
    /// `max_synced_epoch < epoch <= max_syncing_epoch`.
    /// Newer epoch at the front
    syncing_data: VecDeque<SyncingData>,

    /// Data that has been synced already. `epoch` satisfies
    /// `epoch <= max_synced_epoch`.
    synced_data: BTreeMap<HummockEpoch, SyncedDataState>,

    context: UploaderContext,
}

impl HummockUploader {
    pub(crate) fn new(
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
        config: &StorageOpts,
    ) -> Self {
        let initial_epoch = pinned_version.version().max_committed_epoch;
        Self {
            max_sealed_epoch: initial_epoch,
            max_syncing_epoch: initial_epoch,
            max_synced_epoch: initial_epoch,
            unsealed_data: Default::default(),
            sealed_data: Default::default(),
            syncing_data: Default::default(),
            synced_data: Default::default(),
            context: UploaderContext::new(
                pinned_version,
                spawn_upload_task,
                buffer_tracker,
                config,
                state_store_metrics,
            ),
        }
    }

    pub(crate) fn buffer_tracker(&self) -> &BufferTracker {
        &self.context.buffer_tracker
    }

    pub(crate) fn max_sealed_epoch(&self) -> HummockEpoch {
        self.max_sealed_epoch
    }

    pub(crate) fn max_synced_epoch(&self) -> HummockEpoch {
        self.max_synced_epoch
    }

    pub(crate) fn max_committed_epoch(&self) -> HummockEpoch {
        self.context.pinned_version.max_committed_epoch()
    }

    pub(crate) fn hummock_version(&self) -> &PinnedVersion {
        &self.context.pinned_version
    }

    pub(crate) fn get_synced_data(&self, epoch: HummockEpoch) -> Option<&SyncedDataState> {
        assert!(self.max_committed_epoch() < epoch && epoch <= self.max_synced_epoch);
        self.synced_data.get(&epoch)
    }

    pub(crate) fn add_imm(&mut self, imm: ImmutableMemtable) {
        let epoch = imm.min_epoch();
        assert!(
            epoch > self.max_sealed_epoch,
            "imm epoch {} older than max sealed epoch {}",
            epoch,
            self.max_sealed_epoch
        );
        let unsealed_data = self.unsealed_data.entry(epoch).or_default();
        unsealed_data
            .imms
            .entry(imm.instance_id)
            .or_default()
            .push_front(imm);
    }

    pub(crate) fn add_table_watermarks(
        &mut self,
        epoch: u64,
        table_id: TableId,
        table_watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
    ) {
        assert!(
            epoch > self.max_sealed_epoch,
            "imm epoch {} older than max sealed epoch {}",
            epoch,
            self.max_sealed_epoch
        );
        self.unsealed_data
            .entry(epoch)
            .or_default()
            .add_table_watermarks(table_id, table_watermarks, direction);
    }

    pub(crate) fn seal_epoch(&mut self, epoch: HummockEpoch) {
        debug!("epoch {} is sealed", epoch);
        assert!(
            epoch > self.max_sealed_epoch,
            "sealing a sealed epoch {}. {}",
            epoch,
            self.max_sealed_epoch
        );
        self.max_sealed_epoch = epoch;
        let unsealed_data =
            if let Some((&smallest_unsealed_epoch, _)) = self.unsealed_data.first_key_value() {
                assert!(
                    smallest_unsealed_epoch >= epoch,
                    "some epoch {} older than epoch to seal {}",
                    smallest_unsealed_epoch,
                    epoch
                );
                if smallest_unsealed_epoch == epoch {
                    let (_, unsealed_data) = self
                        .unsealed_data
                        .pop_first()
                        .expect("we have checked non-empty");
                    unsealed_data
                } else {
                    debug!("epoch {} to seal has no data", epoch);
                    UnsealedEpochData::default()
                }
            } else {
                debug!("epoch {} to seal has no data", epoch);
                UnsealedEpochData::default()
            };
        self.sealed_data.seal_new_epoch(epoch, unsealed_data);
    }

    pub(crate) fn start_sync_epoch(&mut self, epoch: HummockEpoch) {
        debug!("start sync epoch: {}", epoch);
        assert!(
            epoch > self.max_syncing_epoch,
            "the epoch {} has started syncing already: {}",
            epoch,
            self.max_syncing_epoch
        );
        assert_eq!(
            epoch, self.max_sealed_epoch,
            "we must start syncing all the sealed data",
        );

        self.max_syncing_epoch = epoch;

        // flush imms to SST file, the output SSTs will be uploaded to object store
        // return unfinished merging task
        self.sealed_data.flush(&self.context, false);

        let SealedData {
            epochs,
            imms_by_table_shard,
            spilled_data:
                SpilledData {
                    uploading_tasks,
                    uploaded_data,
                },
            table_watermarks,
            ..
        } = self.sealed_data.drain();

        assert!(
            imms_by_table_shard.is_empty(),
            "after flush, imms must be empty"
        );

        assert_eq!(epoch, *epochs.front().expect("non-empty epoch"));

        let try_join_all_upload_task = if uploading_tasks.is_empty() {
            None
        } else {
            Some(try_join_all(uploading_tasks))
        };

        self.syncing_data.push_front(SyncingData {
            epochs: epochs.into_iter().collect(),
            uploading_tasks: try_join_all_upload_task,
            uploaded: uploaded_data,
            table_watermarks,
        });

        self.context
            .stats
            .uploader_syncing_epoch_count
            .set(self.syncing_data.len() as _);
    }

    fn add_synced_data(&mut self, epoch: HummockEpoch, synced_state: SyncedDataState) {
        assert!(
            epoch <= self.max_syncing_epoch,
            "epoch {} that has been synced has not started syncing yet.  previous max syncing epoch {}",
            epoch,
            self.max_syncing_epoch
        );
        assert!(
            epoch > self.max_synced_epoch,
            "epoch {} has been synced. previous max synced epoch: {}",
            epoch,
            self.max_synced_epoch
        );
        self.max_synced_epoch = epoch;
        assert!(self.synced_data.insert(epoch, synced_state).is_none());
    }

    pub(crate) fn update_pinned_version(&mut self, pinned_version: PinnedVersion) {
        assert_ge!(
            pinned_version.max_committed_epoch(),
            self.context.pinned_version.max_committed_epoch()
        );
        let max_committed_epoch = pinned_version.max_committed_epoch();
        self.context.pinned_version = pinned_version;
        self.synced_data
            .retain(|epoch, _| *epoch > max_committed_epoch);
        if self.max_synced_epoch < max_committed_epoch {
            self.max_synced_epoch = max_committed_epoch;
            if let Some(syncing_data) = self.syncing_data.back() {
                // there must not be any syncing data below MCE
                assert_gt!(
                    *syncing_data
                        .epochs
                        .last()
                        .expect("epoch should not be empty"),
                    max_committed_epoch
                );
            }
        }
        if self.max_syncing_epoch < max_committed_epoch {
            self.max_syncing_epoch = max_committed_epoch;
            // there must not be any sealed data below MCE
            if let Some(&epoch) = self.sealed_data.epochs.back() {
                assert_gt!(epoch, max_committed_epoch);
            }
        }
        if self.max_sealed_epoch < max_committed_epoch {
            self.max_sealed_epoch = max_committed_epoch;
            // there must not be any unsealed data below MCE
            if let Some((&epoch, _)) = self.unsealed_data.first_key_value() {
                assert_gt!(epoch, max_committed_epoch);
            }
        }
    }

    pub(crate) fn may_flush(&mut self) {
        if self.context.buffer_tracker.need_more_flush() {
            self.sealed_data.flush(&self.context, true);
        }

        if self.context.buffer_tracker.need_more_flush() {
            // iterate from older epoch to newer epoch
            for unsealed_data in self.unsealed_data.values_mut() {
                unsealed_data.flush(&self.context);
                if !self.context.buffer_tracker.need_more_flush() {
                    break;
                }
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        let max_committed_epoch = self.context.pinned_version.max_committed_epoch();
        self.max_synced_epoch = max_committed_epoch;
        self.max_syncing_epoch = max_committed_epoch;
        self.max_sealed_epoch = max_committed_epoch;
        self.synced_data.clear();
        self.syncing_data.clear();
        self.sealed_data.clear();
        self.unsealed_data.clear();

        self.context.stats.uploader_syncing_epoch_count.set(0);

        // TODO: call `abort` on the uploading task join handle
    }
}

impl HummockUploader {
    /// Poll the syncing task of the syncing data of the oldest epoch. Return `Poll::Ready(None)` if
    /// there is no syncing data.
    fn poll_syncing_task(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(HummockEpoch, Vec<StagingSstableInfo>)>> {
        // Only poll the oldest epoch if there is any so that the syncing epoch are finished in
        // order
        if let Some(syncing_data) = self.syncing_data.back_mut() {
            // The syncing task has finished
            let result = if let Some(all_tasks) = &mut syncing_data.uploading_tasks {
                ready!(all_tasks.poll_unpin(cx))
            } else {
                Ok(Vec::new())
            };
            let syncing_data = self.syncing_data.pop_back().expect("must exist");
            self.context
                .stats
                .uploader_syncing_epoch_count
                .set(self.syncing_data.len() as _);
            let epoch = syncing_data.sync_epoch();

            let newly_uploaded_sstable_infos = match &result {
                Ok(sstable_infos) => sstable_infos.clone(),
                Err(_) => vec![],
            };

            let result = result.map(|mut sstable_infos| {
                // The newly uploaded `sstable_infos` contains newer data. Therefore,
                // `sstable_infos` at the front
                sstable_infos.extend(syncing_data.uploaded);
                SyncedData {
                    staging_ssts: sstable_infos,
                    table_watermarks: syncing_data.table_watermarks,
                }
            });
            self.add_synced_data(epoch, result);
            Poll::Ready(Some((epoch, newly_uploaded_sstable_infos)))
        } else {
            Poll::Ready(None)
        }
    }

    /// Poll the success of the oldest spilled task of sealed data. Return `Poll::Ready(None)` if
    /// there is no spilling task.
    fn poll_sealed_spill_task(&mut self, cx: &mut Context<'_>) -> Poll<Option<StagingSstableInfo>> {
        self.sealed_data.spilled_data.poll_success_spill(cx)
    }

    /// Poll the success of the oldest spilled task of unsealed data. Return `Poll::Ready(None)` if
    /// there is no spilling task.
    fn poll_unsealed_spill_task(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<StagingSstableInfo>> {
        // iterator from older epoch to new epoch so that the spill task are finished in epoch order
        for unsealed_data in self.unsealed_data.values_mut() {
            // if None, there is no spilling task. Search for the unsealed data of the next epoch in
            // the next iteration.
            if let Some(sstable_info) = ready!(unsealed_data.spilled_data.poll_success_spill(cx)) {
                return Poll::Ready(Some(sstable_info));
            }
        }
        Poll::Ready(None)
    }
}

pub(crate) enum UploaderEvent {
    // staging sstable info of newer data comes first
    SyncFinish(HummockEpoch, Vec<StagingSstableInfo>),
    DataSpilled(StagingSstableInfo),
}

impl HummockUploader {
    pub(crate) fn next_event(&mut self) -> impl Future<Output = UploaderEvent> + '_ {
        poll_fn(|cx| {
            if let Some((epoch, newly_uploaded_sstables)) = ready!(self.poll_syncing_task(cx)) {
                return Poll::Ready(UploaderEvent::SyncFinish(epoch, newly_uploaded_sstables));
            }

            if let Some(sstable_info) = ready!(self.poll_sealed_spill_task(cx)) {
                return Poll::Ready(UploaderEvent::DataSpilled(sstable_info));
            }

            if let Some(sstable_info) = ready!(self.poll_unsealed_spill_task(cx)) {
                return Poll::Ready(UploaderEvent::DataSpilled(sstable_info));
            }

            Poll::Pending
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::future::{poll_fn, Future};
    use std::ops::Deref;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use prometheus::core::GenericGauge;
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::{test_epoch, EpochExt};
    use risingwave_hummock_sdk::key::{FullKey, TableKey};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
    use risingwave_pb::hummock::{KeyRange, SstableInfo};
    use spin::Mutex;
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
    use crate::hummock::event_handler::uploader::{
        HummockUploader, UploadTaskInfo, UploadTaskInput, UploadTaskOutput, UploaderContext,
        UploaderEvent, UploadingTask,
    };
    use crate::hummock::event_handler::{LocalInstanceId, TEST_LOCAL_INSTANCE_ID};
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::shared_buffer::shared_buffer_batch::{
        SharedBufferBatch, SharedBufferBatchId, SharedBufferValue,
    };
    use crate::hummock::{HummockError, HummockResult, MemoryLimiter};
    use crate::mem_table::{ImmId, ImmutableMemtable};
    use crate::monitor::HummockStateStoreMetrics;
    use crate::opts::StorageOpts;

    const INITIAL_EPOCH: HummockEpoch = test_epoch(5);
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

    pub trait UploadOutputFuture =
        Future<Output = HummockResult<UploadTaskOutput>> + Send + 'static;
    pub trait UploadFn<Fut: UploadOutputFuture> =
        Fn(UploadTaskInput, UploadTaskInfo) -> Fut + Send + Sync + 'static;

    fn test_hummock_version(epoch: HummockEpoch) -> HummockVersion {
        HummockVersion {
            id: epoch,
            levels: Default::default(),
            max_committed_epoch: epoch,
            safe_epoch: 0,
            table_watermarks: HashMap::new(),
            table_change_log: HashMap::new(),
        }
    }

    fn initial_pinned_version() -> PinnedVersion {
        PinnedVersion::new(test_hummock_version(INITIAL_EPOCH), unbounded_channel().0)
    }

    fn dummy_table_key() -> Vec<u8> {
        vec![b't', b'e', b's', b't']
    }

    async fn gen_imm_with_limiter(
        epoch: HummockEpoch,
        limiter: Option<&MemoryLimiter>,
    ) -> ImmutableMemtable {
        let sorted_items = vec![(
            TableKey(Bytes::from(dummy_table_key())),
            SharedBufferValue::Delete,
        )];
        let size = SharedBufferBatch::measure_batch_size(&sorted_items, None).0;
        let tracker = match limiter {
            Some(limiter) => Some(limiter.require_memory(size as u64).await),
            None => None,
        };
        SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            0,
            sorted_items,
            None,
            size,
            TEST_TABLE_ID,
            TEST_LOCAL_INSTANCE_ID,
            tracker,
        )
    }

    async fn gen_imm(epoch: HummockEpoch) -> ImmutableMemtable {
        gen_imm_with_limiter(epoch, None).await
    }

    fn gen_sstable_info(
        start_epoch: HummockEpoch,
        end_epoch: HummockEpoch,
    ) -> Vec<LocalSstableInfo> {
        let start_full_key = FullKey::new(TEST_TABLE_ID, TableKey(dummy_table_key()), start_epoch);
        let end_full_key = FullKey::new(TEST_TABLE_ID, TableKey(dummy_table_key()), end_epoch);
        let gen_sst_object_id = (start_epoch << 8) + end_epoch;
        vec![LocalSstableInfo::for_test(SstableInfo {
            object_id: gen_sst_object_id,
            sst_id: gen_sst_object_id,
            key_range: Some(KeyRange {
                left: start_full_key.encode(),
                right: end_full_key.encode(),
                right_exclusive: true,
            }),
            table_ids: vec![TEST_TABLE_ID.table_id],
            ..Default::default()
        })]
    }

    fn test_uploader_context<F, Fut>(upload_fn: F) -> UploaderContext
    where
        Fut: UploadOutputFuture,
        F: UploadFn<Fut>,
    {
        let config = StorageOpts::default();
        UploaderContext::new(
            initial_pinned_version(),
            Arc::new(move |payload, task_info| spawn(upload_fn(payload, task_info))),
            BufferTracker::for_test(),
            &config,
            Arc::new(HummockStateStoreMetrics::unused()),
        )
    }

    fn test_uploader<F, Fut>(upload_fn: F) -> HummockUploader
    where
        Fut: UploadOutputFuture,
        F: UploadFn<Fut>,
    {
        let config = StorageOpts {
            ..Default::default()
        };
        HummockUploader::new(
            Arc::new(HummockStateStoreMetrics::unused()),
            initial_pinned_version(),
            Arc::new(move |payload, task_info| spawn(upload_fn(payload, task_info))),
            BufferTracker::for_test(),
            &config,
        )
    }

    fn dummy_success_upload_output() -> UploadTaskOutput {
        UploadTaskOutput {
            new_value_ssts: gen_sstable_info(INITIAL_EPOCH, INITIAL_EPOCH),
            old_value_ssts: vec![],
            wait_poll_timer: None,
        }
    }

    #[allow(clippy::unused_async)]
    async fn dummy_success_upload_future(
        _: UploadTaskInput,
        _: UploadTaskInfo,
    ) -> HummockResult<UploadTaskOutput> {
        Ok(dummy_success_upload_output())
    }

    #[allow(clippy::unused_async)]
    async fn dummy_fail_upload_future(
        _: UploadTaskInput,
        _: UploadTaskInfo,
    ) -> HummockResult<UploadTaskOutput> {
        Err(HummockError::other("failed"))
    }

    impl UploadingTask {
        fn from_vec(imms: Vec<ImmutableMemtable>, context: &UploaderContext) -> Self {
            let mut input: HashMap<_, Vec<_>> = HashMap::new();
            for imm in imms {
                input.entry(imm.instance_id).or_default().push(imm);
            }
            Self::new(input, context)
        }
    }

    fn get_imm_ids<'a>(
        imms: impl IntoIterator<Item = &'a ImmutableMemtable>,
    ) -> HashMap<LocalInstanceId, Vec<SharedBufferBatchId>> {
        let mut ret: HashMap<_, Vec<_>> = HashMap::new();
        for imm in imms {
            ret.entry(imm.instance_id).or_default().push(imm.batch_id())
        }
        ret
    }

    #[tokio::test]
    pub async fn test_uploading_task_future() {
        let uploader_context = test_uploader_context(dummy_success_upload_future);

        let imm = gen_imm(INITIAL_EPOCH).await;
        let imm_size = imm.size();
        let imm_ids = get_imm_ids(vec![&imm]);
        let task = UploadingTask::from_vec(vec![imm], &uploader_context);
        assert_eq!(imm_size, task.task_info.task_size);
        assert_eq!(imm_ids, task.task_info.imm_ids);
        assert_eq!(vec![INITIAL_EPOCH], task.task_info.epochs);
        let output = task.await.unwrap();
        assert_eq!(
            output.sstable_infos(),
            &dummy_success_upload_output().new_value_ssts
        );
        assert_eq!(imm_size, output.imm_size());
        assert_eq!(&imm_ids, output.imm_ids());
        assert_eq!(&vec![INITIAL_EPOCH], output.epochs());

        let uploader_context = test_uploader_context(dummy_fail_upload_future);
        let imm = gen_imm(INITIAL_EPOCH).await;
        let task = UploadingTask::from_vec(vec![imm], &uploader_context);
        let _ = task.await.unwrap_err();
    }

    #[tokio::test]
    pub async fn test_uploading_task_poll_result() {
        let uploader_context = test_uploader_context(dummy_success_upload_future);
        let mut task =
            UploadingTask::from_vec(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let output = poll_fn(|cx| task.poll_result(cx)).await.unwrap();
        assert_eq!(
            output.sstable_infos(),
            &dummy_success_upload_output().new_value_ssts
        );

        let uploader_context = test_uploader_context(dummy_fail_upload_future);
        let mut task =
            UploadingTask::from_vec(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let _ = poll_fn(|cx| task.poll_result(cx)).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_uploading_task_poll_ok_with_retry() {
        let run_count = Arc::new(AtomicUsize::new(0));
        let fail_num = 10;
        let run_count_clone = run_count.clone();
        let uploader_context = test_uploader_context(move |payload, info| {
            let run_count = run_count.clone();
            async move {
                // fail in the first `fail_num` run, and success at the end
                let ret = if run_count.load(SeqCst) < fail_num {
                    Err(HummockError::other("fail"))
                } else {
                    dummy_success_upload_future(payload, info).await
                };
                run_count.fetch_add(1, SeqCst);
                ret
            }
        });
        let mut task =
            UploadingTask::from_vec(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let output = poll_fn(|cx| task.poll_ok_with_retry(cx)).await;
        assert_eq!(fail_num + 1, run_count_clone.load(SeqCst));
        assert_eq!(
            output.sstable_infos(),
            &dummy_success_upload_output().new_value_ssts
        );
    }

    #[tokio::test]
    async fn test_uploader_basic() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH.next_epoch();
        let imm = gen_imm(epoch1).await;
        uploader.add_imm(imm.clone());
        assert_eq!(1, uploader.unsealed_data.len());
        assert_eq!(
            epoch1 as HummockEpoch,
            *uploader.unsealed_data.first_key_value().unwrap().0
        );
        assert_eq!(
            1,
            uploader
                .unsealed_data
                .first_key_value()
                .unwrap()
                .1
                .imms
                .len()
        );
        uploader.seal_epoch(epoch1);
        assert_eq!(epoch1, uploader.max_sealed_epoch);
        assert!(uploader.unsealed_data.is_empty());
        assert_eq!(1, uploader.sealed_data.imm_count());

        uploader.start_sync_epoch(epoch1);
        assert_eq!(epoch1 as HummockEpoch, uploader.max_syncing_epoch);
        assert_eq!(0, uploader.sealed_data.imm_count());
        assert!(uploader.sealed_data.spilled_data.is_empty());
        assert_eq!(1, uploader.syncing_data.len());
        let syncing_data = uploader.syncing_data.front().unwrap();
        assert_eq!(epoch1 as HummockEpoch, syncing_data.sync_epoch());
        assert!(syncing_data.uploaded.is_empty());
        assert!(syncing_data.uploading_tasks.is_some());

        match uploader.next_event().await {
            UploaderEvent::SyncFinish(finished_epoch, ssts) => {
                assert_eq!(epoch1, finished_epoch);
                assert_eq!(1, ssts.len());
                let staging_sst = ssts.first().unwrap();
                assert_eq!(&vec![epoch1], staging_sst.epochs());
                assert_eq!(&get_imm_ids([&imm]), staging_sst.imm_ids());
                assert_eq!(
                    &dummy_success_upload_output().new_value_ssts,
                    staging_sst.sstable_infos()
                );
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.max_synced_epoch());
        let synced_data = uploader.get_synced_data(epoch1).unwrap();
        let ssts = &synced_data.as_ref().unwrap().staging_ssts;
        assert_eq!(1, ssts.len());
        let staging_sst = ssts.first().unwrap();
        assert_eq!(&vec![epoch1], staging_sst.epochs());
        assert_eq!(&get_imm_ids([&imm]), staging_sst.imm_ids());
        assert_eq!(
            &dummy_success_upload_output().new_value_ssts,
            staging_sst.sstable_infos()
        );

        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1));
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.synced_data.is_empty());
        assert_eq!(epoch1, uploader.max_committed_epoch());
    }

    #[tokio::test]
    async fn test_uploader_empty_epoch() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let imm = gen_imm(epoch2).await;
        // epoch1 is empty while epoch2 is not. Going to seal empty epoch1.
        uploader.add_imm(imm);
        uploader.seal_epoch(epoch1);
        assert_eq!(epoch1, uploader.max_sealed_epoch);

        uploader.start_sync_epoch(epoch1);
        assert_eq!(epoch1, uploader.max_syncing_epoch);

        match uploader.next_event().await {
            UploaderEvent::SyncFinish(finished_epoch, ssts) => {
                assert_eq!(epoch1, finished_epoch);
                assert!(ssts.is_empty());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.max_synced_epoch());
        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1));
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.synced_data.is_empty());
        assert_eq!(epoch1, uploader.max_committed_epoch());
    }

    #[tokio::test]
    async fn test_uploader_poll_empty() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        assert!(poll_fn(|cx| uploader.poll_syncing_task(cx)).await.is_none());
        assert!(poll_fn(|cx| uploader.poll_sealed_spill_task(cx))
            .await
            .is_none());
        assert!(poll_fn(|cx| uploader.poll_unsealed_spill_task(cx))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn test_uploader_empty_advance_mce() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let initial_pinned_version = uploader.context.pinned_version.clone();
        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();
        let epoch4 = epoch3.next_epoch();
        let epoch5 = epoch4.next_epoch();
        let epoch6 = epoch5.next_epoch();
        let version1 = initial_pinned_version.new_pin_version(test_hummock_version(epoch1));
        let version2 = initial_pinned_version.new_pin_version(test_hummock_version(epoch2));
        let version3 = initial_pinned_version.new_pin_version(test_hummock_version(epoch3));
        let version4 = initial_pinned_version.new_pin_version(test_hummock_version(epoch4));
        let version5 = initial_pinned_version.new_pin_version(test_hummock_version(epoch5));
        uploader.update_pinned_version(version1);
        assert_eq!(epoch1, uploader.max_synced_epoch);
        assert_eq!(epoch1, uploader.max_syncing_epoch);
        assert_eq!(epoch1, uploader.max_sealed_epoch);

        uploader.add_imm(gen_imm(epoch6).await);
        uploader.update_pinned_version(version2);
        assert_eq!(epoch2, uploader.max_synced_epoch);
        assert_eq!(epoch2, uploader.max_syncing_epoch);
        assert_eq!(epoch2, uploader.max_sealed_epoch);

        uploader.seal_epoch(epoch6);
        assert_eq!(epoch6, uploader.max_sealed_epoch);
        uploader.update_pinned_version(version3);
        assert_eq!(epoch3, uploader.max_synced_epoch);
        assert_eq!(epoch3, uploader.max_syncing_epoch);
        assert_eq!(epoch6, uploader.max_sealed_epoch);

        uploader.start_sync_epoch(epoch6);
        assert_eq!(epoch6, uploader.max_syncing_epoch);
        uploader.update_pinned_version(version4);
        assert_eq!(epoch4, uploader.max_synced_epoch);
        assert_eq!(epoch6, uploader.max_syncing_epoch);
        assert_eq!(epoch6, uploader.max_sealed_epoch);

        match uploader.next_event().await {
            UploaderEvent::SyncFinish(epoch, _) => {
                assert_eq!(epoch6, epoch);
            }
            UploaderEvent::DataSpilled(_) => unreachable!(),
        }
        uploader.update_pinned_version(version5);
        assert_eq!(epoch6, uploader.max_synced_epoch);
        assert_eq!(epoch6, uploader.max_syncing_epoch);
        assert_eq!(epoch6, uploader.max_sealed_epoch);
    }

    fn prepare_uploader_order_test() -> (
        BufferTracker,
        HummockUploader,
        impl Fn(HashMap<LocalInstanceId, Vec<ImmId>>) -> (BoxFuture<'static, ()>, oneshot::Sender<()>),
    ) {
        // flush threshold is 0. Flush anyway
        let buffer_tracker =
            BufferTracker::new(usize::MAX, 0, GenericGauge::new("test", "test").unwrap());
        // (the started task send the imm ids of payload, the started task wait for finish notify)
        #[allow(clippy::type_complexity)]
        let task_notifier_holder: Arc<
            Mutex<VecDeque<(oneshot::Sender<UploadTaskInfo>, oneshot::Receiver<()>)>>,
        > = Arc::new(Mutex::new(VecDeque::new()));

        let new_task_notifier = {
            let task_notifier_holder = task_notifier_holder.clone();
            move |imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>| {
                let (start_tx, start_rx) = oneshot::channel();
                let (finish_tx, finish_rx) = oneshot::channel();
                task_notifier_holder
                    .lock()
                    .push_front((start_tx, finish_rx));
                let await_start_future = async move {
                    let task_info = start_rx.await.unwrap();
                    assert_eq!(imm_ids, task_info.imm_ids);
                }
                .boxed();
                (await_start_future, finish_tx)
            }
        };

        let config = StorageOpts::default();
        let uploader = HummockUploader::new(
            Arc::new(HummockStateStoreMetrics::unused()),
            initial_pinned_version(),
            Arc::new({
                move |_, task_info: UploadTaskInfo| {
                    let task_notifier_holder = task_notifier_holder.clone();
                    let (start_tx, finish_rx) = task_notifier_holder.lock().pop_back().unwrap();
                    let start_epoch = *task_info.epochs.last().unwrap();
                    let end_epoch = *task_info.epochs.first().unwrap();
                    assert!(end_epoch >= start_epoch);
                    spawn(async move {
                        let ssts = gen_sstable_info(start_epoch, end_epoch);
                        start_tx.send(task_info).unwrap();
                        finish_rx.await.unwrap();
                        Ok(UploadTaskOutput {
                            new_value_ssts: ssts,
                            old_value_ssts: vec![],
                            wait_poll_timer: None,
                        })
                    })
                }
            }),
            buffer_tracker.clone(),
            &config,
        );
        (buffer_tracker, uploader, new_task_notifier)
    }

    async fn assert_uploader_pending(uploader: &mut HummockUploader) {
        for _ in 0..10 {
            yield_now().await;
        }
        assert!(
            poll_fn(|cx| Poll::Ready(uploader.next_event().poll_unpin(cx)))
                .await
                .is_pending()
        )
    }

    #[tokio::test]
    async fn test_uploader_finish_in_order() {
        let (buffer_tracker, mut uploader, new_task_notifier) = prepare_uploader_order_test();

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();
        let memory_limiter = Some(memory_limiter.deref());

        // imm2 contains data in newer epoch, but added first
        let imm2 = gen_imm_with_limiter(epoch2, memory_limiter).await;
        uploader.add_imm(imm2.clone());
        let imm1_1 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(imm1_1.clone());
        let imm1_2 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(imm1_2.clone());

        // imm1 will be spilled first
        let (await_start1, finish_tx1) = new_task_notifier(get_imm_ids([&imm1_2, &imm1_1]));
        let (await_start2, finish_tx2) = new_task_notifier(get_imm_ids([&imm2]));
        uploader.may_flush();
        await_start1.await;
        await_start2.await;

        assert_uploader_pending(&mut uploader).await;

        finish_tx2.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;

        finish_tx1.send(()).unwrap();
        if let UploaderEvent::DataSpilled(sst) = uploader.next_event().await {
            assert_eq!(&get_imm_ids([&imm1_2, &imm1_1]), sst.imm_ids());
            assert_eq!(&vec![epoch1], sst.epochs());
        } else {
            unreachable!("")
        }

        if let UploaderEvent::DataSpilled(sst) = uploader.next_event().await {
            assert_eq!(&get_imm_ids([&imm2]), sst.imm_ids());
            assert_eq!(&vec![epoch2], sst.epochs());
        } else {
            unreachable!("")
        }

        let imm1_3 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(imm1_3.clone());
        let (await_start1_3, finish_tx1_3) = new_task_notifier(get_imm_ids([&imm1_3]));
        uploader.may_flush();
        await_start1_3.await;
        let imm1_4 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(imm1_4.clone());
        let (await_start1_4, finish_tx1_4) = new_task_notifier(get_imm_ids([&imm1_4]));
        uploader.seal_epoch(epoch1);
        uploader.start_sync_epoch(epoch1);
        await_start1_4.await;

        uploader.seal_epoch(epoch2);

        // current uploader state:
        // unsealed: empty
        // sealed: epoch2: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let epoch3 = epoch2.next_epoch();
        let imm3_1 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(imm3_1.clone());
        let (await_start3_1, finish_tx3_1) = new_task_notifier(get_imm_ids([&imm3_1]));
        uploader.may_flush();
        await_start3_1.await;
        let imm3_2 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(imm3_2.clone());
        let (await_start3_2, finish_tx3_2) = new_task_notifier(get_imm_ids([&imm3_2]));
        uploader.may_flush();
        await_start3_2.await;
        let imm3_3 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(imm3_3.clone());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let epoch4 = epoch3.next_epoch();
        let imm4 = gen_imm_with_limiter(epoch4, memory_limiter).await;
        uploader.add_imm(imm4.clone());
        assert_uploader_pending(&mut uploader).await;

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        finish_tx3_1.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx1_4.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx1_3.send(()).unwrap();

        if let UploaderEvent::SyncFinish(epoch, newly_upload_sst) = uploader.next_event().await {
            assert_eq!(epoch1, epoch);
            assert_eq!(2, newly_upload_sst.len());
            assert_eq!(&get_imm_ids([&imm1_4]), newly_upload_sst[0].imm_ids());
            assert_eq!(&get_imm_ids([&imm1_3]), newly_upload_sst[1].imm_ids());
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch1, uploader.max_synced_epoch);
        let synced_data1 = &uploader
            .get_synced_data(epoch1)
            .unwrap()
            .as_ref()
            .unwrap()
            .staging_ssts;
        assert_eq!(3, synced_data1.len());
        assert_eq!(&get_imm_ids([&imm1_4]), synced_data1[0].imm_ids());
        assert_eq!(&get_imm_ids([&imm1_3]), synced_data1[1].imm_ids());
        assert_eq!(&get_imm_ids([&imm1_2, &imm1_1]), synced_data1[2].imm_ids());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])

        uploader.start_sync_epoch(epoch2);
        if let UploaderEvent::SyncFinish(epoch, newly_upload_sst) = uploader.next_event().await {
            assert_eq!(epoch2, epoch);
            assert!(newly_upload_sst.is_empty());
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch2, uploader.max_synced_epoch);
        let synced_data2 = &uploader
            .get_synced_data(epoch2)
            .unwrap()
            .as_ref()
            .unwrap()
            .staging_ssts;
        assert_eq!(1, synced_data2.len());
        assert_eq!(&get_imm_ids([&imm2]), synced_data2[0].imm_ids());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        uploader.seal_epoch(epoch3);
        if let UploaderEvent::DataSpilled(sst) = uploader.next_event().await {
            assert_eq!(&get_imm_ids([&imm3_1]), sst.imm_ids());
        } else {
            unreachable!("should be data spilled");
        }

        // current uploader state:
        // unsealed: epoch4: imm: imm4
        // sealed: imm: imm3_3, uploading: [imm3_2], uploaded: sst([imm3_1])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        uploader.seal_epoch(epoch4);
        let (await_start4_with_3_3, finish_tx4_with_3_3) =
            new_task_notifier(get_imm_ids([&imm4, &imm3_3]));
        uploader.start_sync_epoch(epoch4);
        await_start4_with_3_3.await;

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: epoch4: uploading: [imm4, imm3_3], [imm3_2], uploaded: sst([imm3_1])
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        assert_uploader_pending(&mut uploader).await;
        finish_tx3_2.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx4_with_3_3.send(()).unwrap();

        if let UploaderEvent::SyncFinish(epoch, newly_upload_sst) = uploader.next_event().await {
            assert_eq!(epoch4, epoch);
            assert_eq!(2, newly_upload_sst.len());
            assert_eq!(
                &get_imm_ids([&imm4, &imm3_3]),
                newly_upload_sst[0].imm_ids()
            );
            assert_eq!(&get_imm_ids([&imm3_2]), newly_upload_sst[1].imm_ids());
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch4, uploader.max_synced_epoch);
        let synced_data4 = &uploader
            .get_synced_data(epoch4)
            .unwrap()
            .as_ref()
            .unwrap()
            .staging_ssts;
        assert_eq!(3, synced_data4.len());
        assert_eq!(&vec![epoch4, epoch3], synced_data4[0].epochs());
        assert_eq!(&get_imm_ids([&imm4, &imm3_3]), synced_data4[0].imm_ids());
        assert_eq!(&get_imm_ids([&imm3_2]), synced_data4[1].imm_ids());
        assert_eq!(&get_imm_ids([&imm3_1]), synced_data4[2].imm_ids());

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])
        //         epoch4: sst([imm4, imm3_3]), sst([imm3_2]), sst([imm3_1])
    }
}
