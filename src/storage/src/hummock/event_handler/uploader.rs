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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::mem::swap;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures::future::{try_join_all, TryJoinAll};
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::EPOCH_LEN;
use risingwave_hummock_sdk::{info_in_release, CompactionGroupId, HummockEpoch, LocalSstableInfo};
use tokio::task::JoinHandle;
use tracing::error;

use crate::hummock::compactor::{merge_imms_in_memory, CompactionExecutor};
use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::store::version::StagingSstableInfo;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

pub type UploadTaskPayload = Vec<ImmutableMemtable>;

pub type UploadTaskOutput = Vec<LocalSstableInfo>;
pub type SpawnUploadTask = Arc<
    dyn Fn(UploadTaskPayload, UploadTaskInfo) -> JoinHandle<HummockResult<UploadTaskOutput>>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone)]
pub struct UploadTaskInfo {
    pub task_size: usize,
    pub epochs: Vec<HummockEpoch>,
    pub imm_ids: Vec<ImmId>,
    pub compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
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
    payload: UploadTaskPayload,
    join_handle: JoinHandle<HummockResult<UploadTaskOutput>>,
    task_info: UploadTaskInfo,
    spawn_upload_task: SpawnUploadTask,
    task_size_guard: Arc<AtomicUsize>,
}

pub struct MergeImmTaskOutput {
    pub table_id: TableId,
    pub instance_id: LocalInstanceId,
    pub merged_imm: ImmutableMemtable,
}

// A future that merges multiple immutable memtables to a single immutable memtable.
struct MergingImmTask {
    table_id: TableId,
    instance_id: LocalInstanceId,
    input_imms: Vec<ImmutableMemtable>,
    join_handle: JoinHandle<HummockResult<ImmutableMemtable>>,
}

impl MergingImmTask {
    fn new(
        table_id: TableId,
        instance_id: LocalInstanceId,
        imms: Vec<ImmutableMemtable>,
        memory_tracker: Option<MemoryTracker>,
        context: &UploaderContext,
    ) -> Self {
        let input_imms = imms.clone();
        let join_handle = context.compaction_executor.spawn(async move {
            merge_imms_in_memory(table_id, instance_id, imms, memory_tracker).await
        });

        MergingImmTask {
            table_id,
            instance_id,
            input_imms,
            join_handle,
        }
    }

    /// Poll the result of the merge task
    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<HummockResult<ImmutableMemtable>> {
        Poll::Ready(match ready!(self.join_handle.poll_unpin(cx)) {
            Ok(task_result) => task_result,
            Err(err) => Err(HummockError::other(format!(
                "fail to join imm merge join handle: {:?}",
                err
            ))),
        })
    }
}

impl Future for MergingImmTask {
    type Output = HummockResult<ImmutableMemtable>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_result(cx)
    }
}

impl Drop for UploadingTask {
    fn drop(&mut self) {
        self.task_size_guard
            .fetch_sub(self.task_info.task_size, Relaxed);
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
    fn new(payload: UploadTaskPayload, context: &UploaderContext) -> Self {
        assert!(!payload.is_empty());
        let mut epochs = payload
            .iter()
            .flat_map(|imm| imm.epochs().clone())
            .sorted()
            .dedup()
            .collect_vec();

        // reverse to make newer epochs comes first
        epochs.reverse();
        let imm_ids = payload.iter().map(|imm| imm.batch_id()).collect_vec();
        let task_size = payload.iter().map(|imm| imm.size()).sum();
        let task_info = UploadTaskInfo {
            task_size,
            epochs,
            imm_ids,
            compaction_group_index: context.pinned_version.compaction_group_index(),
        };
        context
            .buffer_tracker
            .global_upload_task_size()
            .fetch_add(task_size, Relaxed);
        info_in_release!("start upload task: {:?}", task_info);
        let join_handle = (context.spawn_upload_task)(payload.clone(), task_info.clone());
        Self {
            payload,
            join_handle,
            task_info,
            spawn_upload_task: context.spawn_upload_task.clone(),
            task_size_guard: context.buffer_tracker.global_upload_task_size().clone(),
        }
    }

    /// Poll the result of the uploading task
    fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<HummockResult<StagingSstableInfo>> {
        Poll::Ready(match ready!(self.join_handle.poll_unpin(cx)) {
            Ok(task_result) => task_result
                .inspect(|_| info_in_release!("upload task finish {:?}", self.task_info))
                .map(|ssts| {
                    StagingSstableInfo::new(
                        ssts,
                        self.task_info.epochs.clone(),
                        self.task_info.imm_ids.clone(),
                        self.task_info.task_size,
                    )
                }),

            Err(err) => Err(HummockError::other(format!(
                "fail to join upload join handle: {:?}",
                err
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
                        "a flush task {:?} failed, start retry. Task info: {:?}",
                        self.task_info, e
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
    imms: VecDeque<ImmutableMemtable>,
    spilled_data: SpilledData,
}

impl UnsealedEpochData {
    fn flush(&mut self, context: &UploaderContext) {
        let imms = self.imms.drain(..).collect_vec();
        if !imms.is_empty() {
            self.spilled_data
                .add_task(UploadingTask::new(imms, context));
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

    // store the output of merge task that will be feed into `flush_imms` procedure
    merged_imms: VecDeque<ImmutableMemtable>,

    // Sealed imms grouped by table shard.
    imms_by_table_shard: HashMap<(TableId, LocalInstanceId), VecDeque<ImmutableMemtable>>,

    // Merging tasks generated from sealed imms
    // it should be safe to directly drop these tasks
    merging_tasks: VecDeque<MergingImmTask>,

    spilled_data: SpilledData,
}

impl SealedData {
    fn clear(&mut self) {
        self.epochs.clear();

        self.spilled_data.clear();
        self.imms_by_table_shard.clear();
        self.merged_imms.clear();
        self.merging_tasks
            .iter()
            .for_each(|task| task.join_handle.abort());
        self.merging_tasks.clear();
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
        for imm in unseal_epoch_data.imms.into_iter().rev() {
            self.imms_by_table_shard
                .entry((imm.table_id, imm.instance_id))
                .or_default()
                .push_front(imm);
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
    }

    fn add_merged_imm(&mut self, merged_imm: &ImmutableMemtable) {
        debug_assert!(merged_imm.is_merged_imm());
        // add merged_imm to merged_imms
        self.merged_imms.push_front(merged_imm.clone());
    }

    fn drop_merging_tasks(&mut self) {
        // pop from newest merging task to restore candidate imms back
        while let Some(task) = self.merging_tasks.pop_front() {
            // cancel the task
            task.join_handle.abort();
            self.imms_by_table_shard
                .get_mut(&(task.table_id, task.instance_id))
                .unwrap()
                .extend(task.input_imms.into_iter());
        }
    }

    // Flush can be triggered by either a sync_epoch or a spill (`may_flush`) request.
    fn flush(&mut self, context: &UploaderContext) {
        // drop unfinished merging tasks
        self.drop_merging_tasks();

        // group imms by epoch and order by epoch
        let mut imms_by_epoch: BTreeMap<HummockEpoch, Vec<ImmutableMemtable>> = BTreeMap::new();
        self.imms_by_table_shard.drain().for_each(|(_, imms)| {
            for imm in imms {
                debug_assert!(imm.max_epoch() == imm.min_epoch());
                imms_by_epoch.entry(imm.max_epoch()).or_default().push(imm);
            }
        });

        // When ImmMerged, we have removed those imms that have been merged from the
        // `self.imms`, thus we need to feed merged_imms into the `flush` procedure
        // to complete a checkpoint.
        let merged_imms = self.merged_imms.drain(..);

        // newer epoch comes first, the order is matter for cleaning the staging imms
        // when adding a staging sst to the `StagingVersion`
        let payload = imms_by_epoch
            .into_iter()
            .rev()
            // in `imms`, newer data comes first
            .flat_map(|(_epoch, imms)| imms)
            .chain(merged_imms.into_iter())
            .collect_vec();

        if !payload.is_empty() {
            self.spilled_data
                .add_task(UploadingTask::new(payload, context));
        }
    }

    fn poll_success_merge_imm(&mut self, cx: &mut Context<'_>) -> Poll<Option<MergeImmTaskOutput>> {
        // only poll the oldest merge task if there is any
        if let Some(task) = self.merging_tasks.back_mut() {
            let merge_result = ready!(task.poll_unpin(cx));

            // pop the finished task
            let task = self.merging_tasks.pop_back().expect("must exist");

            Poll::Ready(Some(MergeImmTaskOutput {
                table_id: task.table_id,
                instance_id: task.instance_id,
                merged_imm: merge_result.unwrap(),
            }))
        } else {
            Poll::Ready(None)
        }
    }

    /// Clear self and return the current sealed data
    fn drain(&mut self) -> SealedData {
        let mut ret = SealedData::default();
        swap(&mut ret, self);
        ret
    }

    #[cfg(test)]
    fn imm_count(&self) -> usize {
        self.imms_by_table_shard
            .values()
            .map(|imms| imms.len())
            .sum()
    }

    #[cfg(test)]
    fn imms_by_epoch(&self) -> BTreeMap<HummockEpoch, Vec<ImmutableMemtable>> {
        let mut imms_by_epoch: BTreeMap<HummockEpoch, Vec<ImmutableMemtable>> = BTreeMap::new();
        self.imms_by_table_shard.iter().for_each(|(_, imms)| {
            for imm in imms {
                debug_assert!(imm.max_epoch() == imm.min_epoch());
                imms_by_epoch
                    .entry(imm.max_epoch())
                    .or_default()
                    .push(imm.clone());
            }
        });
        imms_by_epoch
    }
}

struct SyncingData {
    sync_epoch: HummockEpoch,
    // newer epochs come first
    epochs: Vec<HummockEpoch>,
    // TODO: may replace `TryJoinAll` with a future that will abort other join handles once
    // one join handle failed.
    // None means there is no pending uploading tasks
    uploading_tasks: Option<TryJoinAll<UploadingTask>>,
    // newer data at the front
    uploaded: VecDeque<StagingSstableInfo>,
}

// newer staging sstable info at the front
type SyncedDataState = HummockResult<Vec<StagingSstableInfo>>;

struct UploaderContext {
    pinned_version: PinnedVersion,
    /// When called, it will spawn a task to flush the imm into sst and return the join handle.
    spawn_upload_task: SpawnUploadTask,
    buffer_tracker: BufferTracker,
    /// The number of immutable memtables that will be merged into a new imm.
    /// When the number of imms of a table shard exceeds this threshold, uploader will generate
    /// merging tasks to merge them.
    imm_merge_threshold: usize,

    compaction_executor: Arc<CompactionExecutor>,
}

impl UploaderContext {
    fn new(
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
        config: &StorageOpts,
        compaction_executor: Arc<CompactionExecutor>,
    ) -> Self {
        UploaderContext {
            pinned_version,
            spawn_upload_task,
            buffer_tracker,
            imm_merge_threshold: config.imm_merge_threshold,
            compaction_executor,
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
    stats: Arc<HummockStateStoreMetrics>,
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
        compaction_executor: Arc<CompactionExecutor>,
    ) -> Self {
        let initial_epoch = pinned_version.version().max_committed_epoch;
        Self {
            stats: state_store_metrics,
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
                compaction_executor,
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn imm_merge_threshold(&self) -> usize {
        self.context.imm_merge_threshold
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
        self.unsealed_data
            .entry(epoch)
            .or_default()
            .imms
            .push_front(imm);
    }

    pub(crate) fn seal_epoch(&mut self, epoch: HummockEpoch) {
        info_in_release!("epoch {} is sealed", epoch);
        assert!(
            epoch > self.max_sealed_epoch,
            "sealing a sealed epoch {}. {}",
            epoch,
            self.max_sealed_epoch
        );
        self.max_sealed_epoch = epoch;
        if let Some((&smallest_unsealed_epoch, _)) = self.unsealed_data.first_key_value() {
            assert!(
                smallest_unsealed_epoch >= epoch,
                "some epoch {} older than epoch to seal {}",
                smallest_unsealed_epoch,
                epoch
            );
            if smallest_unsealed_epoch == epoch {
                let (epoch, unsealed_data) = self
                    .unsealed_data
                    .pop_first()
                    .expect("we have checked non-empty");
                self.sealed_data.seal_new_epoch(epoch, unsealed_data);
            } else {
                info_in_release!("epoch {} to seal has no data", epoch);
            }
        } else {
            info_in_release!("epoch {} to seal has no data", epoch);
        }
    }

    pub(crate) fn start_merge_imms(&mut self, sealed_epoch: HummockEpoch) {
        // skip merging if merge threshold is 1
        if self.context.imm_merge_threshold <= 1 {
            return;
        }

        let memory_limiter = self.context.buffer_tracker.get_memory_limiter();
        // scan imms of each table shard to generate merging task
        // when the number of imms exceeds the merge threshold
        for ((table_id, shard_id), imms) in self
            .sealed_data
            .imms_by_table_shard
            .iter_mut()
            .filter(|(_, imms)| imms.len() >= self.context.imm_merge_threshold)
        {
            let imms_to_merge = imms.drain(..).collect_vec();
            let mut kv_count = 0;
            let mut imm_size = 0;
            imms_to_merge.iter().for_each(|imm| {
                // ensure imms are sealed
                assert!(imm.max_epoch() <= sealed_epoch);
                kv_count += imm.kv_count();
                imm_size += imm.size();
            });

            // acquire memory before generate merge task
            // if acquire memory failed, the task will not be generated
            let memory_sz = (imm_size + kv_count * EPOCH_LEN) as u64;
            if let Some(tracker) = memory_limiter.try_require_memory(memory_sz) {
                self.sealed_data
                    .merging_tasks
                    .push_front(MergingImmTask::new(
                        *table_id,
                        *shard_id,
                        imms_to_merge,
                        Some(tracker),
                        &self.context,
                    ));
            } else {
                tracing::warn!(
                    "fail to acqiure memory {} B, skip merging imms for table {}, shard {}",
                    memory_sz,
                    table_id,
                    shard_id
                );
                imms.extend(imms_to_merge);
            }
        }
    }

    pub(crate) fn update_sealed_data(&mut self, merged_imm: &ImmutableMemtable) {
        self.sealed_data.add_merged_imm(merged_imm);
    }

    pub(crate) fn start_sync_epoch(&mut self, epoch: HummockEpoch) {
        info_in_release!("start sync epoch: {}", epoch);
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
        self.sealed_data.flush(&self.context);

        let SealedData {
            epochs,
            imms_by_table_shard,
            spilled_data:
                SpilledData {
                    uploading_tasks,
                    uploaded_data,
                },
            ..
        } = self.sealed_data.drain();

        assert!(
            imms_by_table_shard.is_empty(),
            "after flush, imms must be empty"
        );

        let try_join_all_upload_task = if uploading_tasks.is_empty() {
            None
        } else {
            Some(try_join_all(uploading_tasks))
        };

        self.syncing_data.push_front(SyncingData {
            epochs: epochs.into_iter().collect(),
            sync_epoch: epoch,
            uploading_tasks: try_join_all_upload_task,
            uploaded: uploaded_data,
        });
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
        assert!(
            pinned_version.max_committed_epoch()
                >= self.context.pinned_version.max_committed_epoch()
        );
        let max_committed_epoch = pinned_version.max_committed_epoch();
        self.context.pinned_version = pinned_version;
        self.synced_data
            .retain(|epoch, _| *epoch > max_committed_epoch);
        if self.max_synced_epoch < max_committed_epoch {
            self.max_synced_epoch = max_committed_epoch;
            if let Some(syncing_data) = self.syncing_data.back() {
                // there must not be any syncing data below MCE
                assert!(
                    *syncing_data
                        .epochs
                        .last()
                        .expect("epoch should not be empty")
                        > max_committed_epoch
                );
            }
        }
        if self.max_syncing_epoch < max_committed_epoch {
            self.max_syncing_epoch = max_committed_epoch;
            // there must not be any sealed data below MCE
            if let Some(&epoch) = self.sealed_data.epochs.back() {
                assert!(epoch > max_committed_epoch);
            }
        }
        if self.max_sealed_epoch < max_committed_epoch {
            self.max_sealed_epoch = max_committed_epoch;
            // there must not be any unsealed data below MCE
            if let Some((&epoch, _)) = self.unsealed_data.first_key_value() {
                assert!(epoch > max_committed_epoch);
            }
        }
    }

    pub(crate) fn may_flush(&mut self) {
        if self.context.buffer_tracker.need_more_flush() {
            self.sealed_data.flush(&self.context);
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

        // TODO: call `abort` on the uploading task join handle
    }

    pub(crate) fn next_event(&mut self) -> NextUploaderEvent<'_> {
        NextUploaderEvent { uploader: self }
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
            let epoch = syncing_data.sync_epoch;

            let newly_uploaded_sstable_infos = match &result {
                Ok(sstable_infos) => sstable_infos.clone(),
                Err(_) => vec![],
            };

            let result = result.map(|mut sstable_infos| {
                // The newly uploaded `sstable_infos` contains newer data. Therefore,
                // `sstable_infos` at the front
                sstable_infos.extend(syncing_data.uploaded);
                sstable_infos
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

    fn poll_sealed_merge_imm_task(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<MergeImmTaskOutput>> {
        let poll_ret = self.sealed_data.poll_success_merge_imm(cx);
        if let Poll::Ready(Some(output)) = &poll_ret {
            let table_id_label = output.table_id.to_string();
            let shard_id_label = output.instance_id.to_string();

            // monitor finished task
            self.stats
                .merge_imm_task_counts
                .with_label_values(&[table_id_label.as_str(), shard_id_label.as_str()])
                .inc();
            // monitor merged imms
            self.stats
                .merge_imm_batch_counts
                .with_label_values(&[table_id_label.as_str(), shard_id_label.as_str()])
                .inc_by(output.merged_imm.get_imm_ids().len() as _);
        }
        poll_ret
    }
}

pub(crate) struct NextUploaderEvent<'a> {
    uploader: &'a mut HummockUploader,
}

pub(crate) enum UploaderEvent {
    // staging sstable info of newer data comes first
    SyncFinish(HummockEpoch, Vec<StagingSstableInfo>),
    DataSpilled(StagingSstableInfo),
    ImmMerged(MergeImmTaskOutput),
}

impl<'a> Future for NextUploaderEvent<'a> {
    type Output = UploaderEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let uploader = &mut self.deref_mut().uploader;

        if let Some((epoch, newly_uploaded_sstables)) = ready!(uploader.poll_syncing_task(cx)) {
            return Poll::Ready(UploaderEvent::SyncFinish(epoch, newly_uploaded_sstables));
        }

        if let Some(sstable_info) = ready!(uploader.poll_sealed_spill_task(cx)) {
            return Poll::Ready(UploaderEvent::DataSpilled(sstable_info));
        }

        if let Some(sstable_info) = ready!(uploader.poll_unsealed_spill_task(cx)) {
            return Poll::Ready(UploaderEvent::DataSpilled(sstable_info));
        }

        if let Some(merge_output) = ready!(uploader.poll_sealed_merge_imm_task(cx)) {
            // add the merged imm into sealed data
            uploader.update_sealed_data(&merge_output.merged_imm);
            return Poll::Ready(UploaderEvent::ImmMerged(merge_output));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{poll_fn, Future};
    use std::ops::Deref;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};
    use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
    use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo};
    use spin::Mutex;
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    use crate::hummock::compactor::CompactionExecutor;
    use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
    use crate::hummock::event_handler::uploader::{
        HummockUploader, MergingImmTask, UploadTaskInfo, UploadTaskOutput, UploadTaskPayload,
        UploaderContext, UploaderEvent, UploadingTask,
    };
    use crate::hummock::event_handler::LocalInstanceId;
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, transform_shared_buffer,
    };
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
    use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
    use crate::hummock::value::HummockValue;
    use crate::hummock::{HummockError, HummockResult, MemoryLimiter};
    use crate::opts::StorageOpts;
    use crate::storage_value::StorageValue;

    const INITIAL_EPOCH: HummockEpoch = 5;
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

    pub trait UploadOutputFuture =
        Future<Output = HummockResult<UploadTaskOutput>> + Send + 'static;
    pub trait UploadFn<Fut: UploadOutputFuture> =
        Fn(UploadTaskPayload, UploadTaskInfo) -> Fut + Send + Sync + 'static;

    fn test_hummock_version(epoch: HummockEpoch) -> HummockVersion {
        HummockVersion {
            id: epoch,
            levels: Default::default(),
            max_committed_epoch: epoch,
            safe_epoch: 0,
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
        let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(vec![(
            Bytes::from(dummy_table_key()),
            StorageValue::new_delete(),
        )]);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items);
        let tracker = match limiter {
            Some(limiter) => Some(limiter.require_memory(size as u64).await),
            None => None,
        };
        SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            sorted_items,
            size,
            vec![],
            TEST_TABLE_ID,
            None,
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
        let compaction_executor = Arc::new(CompactionExecutor::new(None));
        UploaderContext::new(
            initial_pinned_version(),
            Arc::new(move |payload, task_info| spawn(upload_fn(payload, task_info))),
            BufferTracker::for_test(),
            &config,
            compaction_executor,
        )
    }

    fn test_uploader<F, Fut>(upload_fn: F) -> HummockUploader
    where
        Fut: UploadOutputFuture,
        F: UploadFn<Fut>,
    {
        let config = StorageOpts::default();
        let compaction_executor = Arc::new(CompactionExecutor::new(None));
        HummockUploader::new(
            initial_pinned_version(),
            Arc::new(move |payload, task_info| spawn(upload_fn(payload, task_info))),
            BufferTracker::for_test(),
            &config,
            compaction_executor,
        )
    }

    fn dummy_success_upload_output() -> Vec<LocalSstableInfo> {
        gen_sstable_info(INITIAL_EPOCH, INITIAL_EPOCH)
    }

    #[allow(clippy::unused_async)]
    async fn dummy_success_upload_future(
        _: UploadTaskPayload,
        _: UploadTaskInfo,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        Ok(dummy_success_upload_output())
    }

    #[allow(clippy::unused_async)]
    async fn dummy_fail_upload_future(
        _: UploadTaskPayload,
        _: UploadTaskInfo,
    ) -> HummockResult<Vec<LocalSstableInfo>> {
        Err(HummockError::other("failed"))
    }

    #[tokio::test]
    pub async fn test_uploading_task_future() {
        let uploader_context = test_uploader_context(dummy_success_upload_future);
        let imm = gen_imm(INITIAL_EPOCH).await;
        let imm_size = imm.size();
        let imm_id = imm.batch_id();
        let task = UploadingTask::new(vec![imm], &uploader_context);
        assert_eq!(imm_size, task.task_info.task_size);
        assert_eq!(vec![imm_id], task.task_info.imm_ids);
        assert_eq!(vec![INITIAL_EPOCH as HummockEpoch], task.task_info.epochs);
        let output = task.await.unwrap();
        assert_eq!(output.sstable_infos(), &dummy_success_upload_output());
        assert_eq!(imm_size, output.imm_size());
        assert_eq!(&vec![imm_id], output.imm_ids());
        assert_eq!(&vec![INITIAL_EPOCH as HummockEpoch], output.epochs());

        let uploader_context = test_uploader_context(dummy_fail_upload_future);
        let task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let _ = task.await.unwrap_err();
    }

    #[tokio::test]
    pub async fn test_uploading_task_poll_result() {
        let uploader_context = test_uploader_context(dummy_success_upload_future);
        let mut task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let output = poll_fn(|cx| task.poll_result(cx)).await.unwrap();
        assert_eq!(output.sstable_infos(), &dummy_success_upload_output());

        let uploader_context = test_uploader_context(dummy_fail_upload_future);
        let mut task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let _ = poll_fn(|cx| task.poll_result(cx)).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_uploading_task_poll_ok_with_retry() {
        let run_count = Arc::new(AtomicUsize::new(0));
        let fail_num = 10;
        let run_count_clone = run_count.clone();
        let uploader_context = test_uploader_context(move |_, _| {
            let run_count = run_count.clone();
            async move {
                // fail in the first `fail_num` run, and success at the end
                let ret = if run_count.load(SeqCst) < fail_num {
                    Err(HummockError::other("fail"))
                } else {
                    Ok(dummy_success_upload_output())
                };
                run_count.fetch_add(1, SeqCst);
                ret
            }
        });
        let mut task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH).await], &uploader_context);
        let output = poll_fn(|cx| task.poll_ok_with_retry(cx)).await;
        assert_eq!(fail_num + 1, run_count_clone.load(SeqCst));
        assert_eq!(output.sstable_infos(), &dummy_success_upload_output());
    }

    #[tokio::test]
    async fn test_uploader_basic() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH + 1;
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
        assert_eq!(epoch1 as HummockEpoch, syncing_data.sync_epoch);
        assert!(syncing_data.uploaded.is_empty());
        assert!(syncing_data.uploading_tasks.is_some());

        match uploader.next_event().await {
            UploaderEvent::SyncFinish(finished_epoch, ssts) => {
                assert_eq!(epoch1, finished_epoch);
                assert_eq!(1, ssts.len());
                let staging_sst = ssts.first().unwrap();
                assert_eq!(&vec![epoch1], staging_sst.epochs());
                assert_eq!(&vec![imm.batch_id()], staging_sst.imm_ids());
                assert_eq!(&dummy_success_upload_output(), staging_sst.sstable_infos());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.max_synced_epoch());
        let synced_data = uploader.get_synced_data(epoch1).unwrap();
        let ssts = synced_data.as_ref().unwrap();
        assert_eq!(1, ssts.len());
        let staging_sst = ssts.first().unwrap();
        assert_eq!(&vec![epoch1], staging_sst.epochs());
        assert_eq!(&vec![imm.batch_id()], staging_sst.imm_ids());
        assert_eq!(&dummy_success_upload_output(), staging_sst.sstable_infos());

        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1));
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.synced_data.is_empty());
        assert_eq!(epoch1, uploader.max_committed_epoch());
    }

    #[tokio::test]
    async fn test_uploader_merge_imms_without_flush() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let mut all_imms = VecDeque::new();
        // assume a chckpoint consists of 11 epochs
        let ckpt_intervals = 11;
        let imm_merge_threshold: usize = uploader.imm_merge_threshold();

        // For each epoch, we gen imm for 2 shards and add them to uploader and seal the epoch
        // afterward. check uploader's state after each epoch has been sealed
        // When we get IMM_MERGE_THRESHOLD epochs, there should be merging task started for sealed
        // data. Then we await the merging task and check the uploader's state again.
        let mut merged_imms = VecDeque::new();
        for i in 1..=ckpt_intervals {
            let epoch = INITIAL_EPOCH + i;
            let mut imm1 = gen_imm(epoch).await;
            let mut imm2 = gen_imm(epoch).await;

            imm1.instance_id = 1 as LocalInstanceId;
            imm2.instance_id = 2 as LocalInstanceId;

            uploader.add_imm(imm1.clone());
            uploader.add_imm(imm2.clone());

            // newer imm comes in front
            all_imms.push_front(imm1);
            all_imms.push_front(imm2);

            uploader.seal_epoch(epoch);

            assert_eq!(epoch, uploader.max_sealed_epoch);
            // check sealed data has two imms
            let imms_by_epoch = uploader.sealed_data.imms_by_epoch();
            imms_by_epoch.last_key_value().map_or((), |(e, imms)| {
                if *e == epoch {
                    assert_eq!(2, imms.len());
                }
            });

            let epoch_cnt = (epoch - INITIAL_EPOCH) as usize;
            if epoch_cnt < imm_merge_threshold {
                assert!(uploader.sealed_data.merging_tasks.is_empty());
                assert!(uploader.sealed_data.spilled_data.is_empty());
                assert_eq!(epoch_cnt, uploader.sealed_data.epochs.len());
            } else {
                assert_eq!(epoch_cnt, uploader.sealed_data.epochs.len());

                let unmerged_imm_cnt: usize = epoch_cnt - imm_merge_threshold * merged_imms.len();

                if unmerged_imm_cnt < imm_merge_threshold {
                    continue;
                }

                let imms_by_shard = &mut uploader.sealed_data.imms_by_table_shard;
                // check shard 1
                imms_by_shard
                    .get(&(TEST_TABLE_ID, 1 as LocalInstanceId))
                    .map_or((), |imms| {
                        assert_eq!(imm_merge_threshold, imms.len());
                    });

                // check shard 2
                imms_by_shard
                    .get(&(TEST_TABLE_ID, 2 as LocalInstanceId))
                    .map_or((), |imms| {
                        assert_eq!(imm_merge_threshold, imms.len());
                    });

                // we have enough sealed imms, start merging task
                println!("start merging task for epoch {}", epoch);
                uploader.start_merge_imms(epoch);
                assert!(!uploader.sealed_data.merging_tasks.is_empty());
                assert!(uploader.sealed_data.spilled_data.is_empty());

                // check after generate merging task
                uploader
                    .sealed_data
                    .imms_by_table_shard
                    .get(&(TEST_TABLE_ID, 1 as LocalInstanceId))
                    .map_or((), |imms| {
                        assert_eq!(0, imms.len());
                    });
                uploader
                    .sealed_data
                    .imms_by_table_shard
                    .get(&(TEST_TABLE_ID, 2 as LocalInstanceId))
                    .map_or((), |imms| {
                        assert_eq!(0, imms.len());
                    });

                // poll the merging task and check the result
                match uploader.next_event().await {
                    UploaderEvent::ImmMerged(output) => {
                        println!("merging task success for epoch {}", epoch);
                        merged_imms.push_front(output.merged_imm);
                    }
                    _ => unreachable!(),
                };
            }
        }
    }

    #[tokio::test]
    async fn test_uploader_empty_epoch() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH + 1;
        let epoch2 = INITIAL_EPOCH + 2;
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
    async fn test_drop_success_merging_task() {
        let table_id = TableId { table_id: 1004 };
        let shared_buffer_items1: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value2")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value3")),
            ),
        ];
        let epoch = 1;
        let imm1 = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items1.clone()),
            epoch,
            table_id,
        );
        let shared_buffer_items2: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value12")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value22")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value32")),
            ),
        ];
        let epoch = 2;
        let imm2 = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items2.clone()),
            epoch,
            table_id,
        );

        let shared_buffer_items3: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value13")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value23")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value33")),
            ),
        ];
        let epoch = 3;
        let imm3 = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items3.clone()),
            epoch,
            table_id,
        );

        // newer data comes first
        let imms = vec![imm3, imm2, imm1];
        let context = test_uploader_context(dummy_success_upload_future);
        let mut task = MergingImmTask::new(table_id, 0, imms, None, &context);
        let sleep = tokio::time::sleep(Duration::from_millis(500));
        tokio::select! {
            _ = sleep => {
                println!("sleep timeout")
            }
            res = &mut task => {
                match res {
                    Ok(imm) => {
                        println!("merging task success");
                        assert_eq!(table_id, imm.table_id);
                        assert_eq!(9, imm.kv_count());
                    }
                    Err(err) => {
                        println!("merging task failed: {:?}", err);
                    }
                }
            }
        }
        task.join_handle.abort();
        println!("merging task abort success");
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
        let epoch1 = INITIAL_EPOCH + 1;
        let epoch2 = INITIAL_EPOCH + 2;
        let epoch3 = INITIAL_EPOCH + 3;
        let epoch4 = INITIAL_EPOCH + 4;
        let epoch5 = INITIAL_EPOCH + 5;
        let epoch6 = INITIAL_EPOCH + 6;
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
            UploaderEvent::ImmMerged(_) => unreachable!(),
        }
        uploader.update_pinned_version(version5);
        assert_eq!(epoch6, uploader.max_synced_epoch);
        assert_eq!(epoch6, uploader.max_syncing_epoch);
        assert_eq!(epoch6, uploader.max_sealed_epoch);
    }

    fn prepare_uploader_order_test() -> (
        BufferTracker,
        HummockUploader,
        impl Fn(Vec<ImmId>) -> (BoxFuture<'static, ()>, oneshot::Sender<()>),
    ) {
        // flush threshold is 0. Flush anyway
        let buffer_tracker = BufferTracker::new(usize::MAX, 0);
        // (the started task send the imm ids of payload, the started task wait for finish notify)
        #[allow(clippy::type_complexity)]
        let task_notifier_holder: Arc<
            Mutex<VecDeque<(oneshot::Sender<UploadTaskInfo>, oneshot::Receiver<()>)>>,
        > = Arc::new(Mutex::new(VecDeque::new()));

        let new_task_notifier = {
            let task_notifier_holder = task_notifier_holder.clone();
            move |imm_ids: Vec<ImmId>| {
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
        let compaction_executor = Arc::new(CompactionExecutor::new(None));
        let uploader = HummockUploader::new(
            initial_pinned_version(),
            Arc::new({
                move |_: UploadTaskPayload, task_info: UploadTaskInfo| {
                    let task_notifier_holder = task_notifier_holder.clone();
                    let (start_tx, finish_rx) = task_notifier_holder.lock().pop_back().unwrap();
                    let start_epoch = *task_info.epochs.last().unwrap();
                    let end_epoch = *task_info.epochs.first().unwrap();
                    assert!(end_epoch >= start_epoch);
                    spawn(async move {
                        let ret = gen_sstable_info(start_epoch, end_epoch);
                        start_tx.send(task_info).unwrap();
                        finish_rx.await.unwrap();
                        Ok(ret)
                    })
                }
            }),
            buffer_tracker.clone(),
            &config,
            compaction_executor,
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

        let epoch1 = INITIAL_EPOCH + 1;
        let epoch2 = INITIAL_EPOCH + 2;
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
        let (await_start1, finish_tx1) =
            new_task_notifier(vec![imm1_2.batch_id(), imm1_1.batch_id()]);
        let (await_start2, finish_tx2) = new_task_notifier(vec![imm2.batch_id()]);
        uploader.may_flush();
        await_start1.await;
        await_start2.await;

        assert_uploader_pending(&mut uploader).await;

        finish_tx2.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;

        finish_tx1.send(()).unwrap();
        if let UploaderEvent::DataSpilled(sst) = uploader.next_event().await {
            assert_eq!(&vec![imm1_2.batch_id(), imm1_1.batch_id()], sst.imm_ids());
            assert_eq!(&vec![epoch1], sst.epochs());
        } else {
            unreachable!("")
        }

        if let UploaderEvent::DataSpilled(sst) = uploader.next_event().await {
            assert_eq!(&vec![imm2.batch_id()], sst.imm_ids());
            assert_eq!(&vec![epoch2], sst.epochs());
        } else {
            unreachable!("")
        }

        let imm1_3 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(imm1_3.clone());
        let (await_start1_3, finish_tx1_3) = new_task_notifier(vec![imm1_3.batch_id()]);
        uploader.may_flush();
        await_start1_3.await;
        let imm1_4 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(imm1_4.clone());
        let (await_start1_4, finish_tx1_4) = new_task_notifier(vec![imm1_4.batch_id()]);
        uploader.seal_epoch(epoch1);
        uploader.start_sync_epoch(epoch1);
        await_start1_4.await;

        uploader.seal_epoch(epoch2);

        // current uploader state:
        // unsealed: empty
        // sealed: epoch2: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let epoch3 = INITIAL_EPOCH + 3;
        let imm3_1 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(imm3_1.clone());
        let (await_start3_1, finish_tx3_1) = new_task_notifier(vec![imm3_1.batch_id()]);
        uploader.may_flush();
        await_start3_1.await;
        let imm3_2 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(imm3_2.clone());
        let (await_start3_2, finish_tx3_2) = new_task_notifier(vec![imm3_2.batch_id()]);
        uploader.may_flush();
        await_start3_2.await;
        let imm3_3 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(imm3_3.clone());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let epoch4 = INITIAL_EPOCH + 4;
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
            assert_eq!(&vec![imm1_4.batch_id()], newly_upload_sst[0].imm_ids());
            assert_eq!(&vec![imm1_3.batch_id()], newly_upload_sst[1].imm_ids());
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch1, uploader.max_synced_epoch);
        let synced_data1 = uploader.get_synced_data(epoch1).unwrap().as_ref().unwrap();
        assert_eq!(3, synced_data1.len());
        assert_eq!(&vec![imm1_4.batch_id()], synced_data1[0].imm_ids());
        assert_eq!(&vec![imm1_3.batch_id()], synced_data1[1].imm_ids());
        assert_eq!(
            &vec![imm1_2.batch_id(), imm1_1.batch_id()],
            synced_data1[2].imm_ids()
        );

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
        let synced_data2 = uploader.get_synced_data(epoch2).unwrap().as_ref().unwrap();
        assert_eq!(1, synced_data2.len());
        assert_eq!(&vec![imm2.batch_id()], synced_data2[0].imm_ids());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        uploader.seal_epoch(epoch3);
        if let UploaderEvent::DataSpilled(sst) = uploader.next_event().await {
            assert_eq!(&vec![imm3_1.batch_id()], sst.imm_ids());
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
            new_task_notifier(vec![imm4.batch_id(), imm3_3.batch_id()]);
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
                &vec![imm4.batch_id(), imm3_3.batch_id()],
                newly_upload_sst[0].imm_ids()
            );
            assert_eq!(&vec![imm3_2.batch_id()], newly_upload_sst[1].imm_ids());
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch4, uploader.max_synced_epoch);
        let synced_data4 = uploader.get_synced_data(epoch4).unwrap().as_ref().unwrap();
        assert_eq!(3, synced_data4.len());
        assert_eq!(&vec![epoch4, epoch3], synced_data4[0].epochs());
        assert_eq!(
            &vec![imm4.batch_id(), imm3_3.batch_id()],
            synced_data4[0].imm_ids()
        );
        assert_eq!(&vec![imm3_2.batch_id()], synced_data4[1].imm_ids());
        assert_eq!(&vec![imm3_1.batch_id()], synced_data4[2].imm_ids());

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])
        //         epoch4: sst([imm4, imm3_3]), sst([imm3_2]), sst([imm3_1])
    }
}
