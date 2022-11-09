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
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch};
use tokio::task::JoinHandle;
use tracing::{error, warn};

use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::store::version::StagingSstableInfo;
use crate::hummock::{HummockError, HummockResult};

pub type TaskPayload = Vec<ImmutableMemtable>;
pub type SpawnUploadTask = Arc<
    dyn Fn(TaskPayload, TaskInfo) -> JoinHandle<HummockResult<StagingSstableInfo>>
        + Send
        + Sync
        + 'static,
>;

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub task_size: usize,
    pub epochs: Vec<HummockEpoch>,
    pub imm_ids: Vec<ImmId>,
    pub compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
}

/// A wrapper for a uploading task that compacts and uploads the imm payload. Task context are
/// stored so that when the task fails, it can be re-tried.
struct UploadingTask {
    payload: TaskPayload,
    join_handle: JoinHandle<HummockResult<StagingSstableInfo>>,
    task_info: TaskInfo,
    spawn_upload_task: SpawnUploadTask,
    task_size_guard: Arc<AtomicUsize>,
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
    fn new(payload: TaskPayload, context: &UploaderContext) -> Self {
        let mut epochs = payload
            .iter()
            .map(|imm| imm.epoch())
            .sorted()
            .dedup()
            .collect_vec();
        // reverse to make newer epochs comes first
        epochs.reverse();
        let imm_ids = payload.iter().map(|imm| imm.batch_id()).collect_vec();
        let task_size = payload.iter().map(|imm| imm.size()).sum();
        let task_info = TaskInfo {
            task_size,
            epochs,
            imm_ids,
            compaction_group_index: context.pinned_version.compaction_group_index(),
        };
        context
            .buffer_tracker
            .global_upload_task_size()
            .fetch_add(task_size, Relaxed);
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
        self.join_handle
            .poll_unpin(cx)
            .map(|join_result| match join_result {
                Ok(task_result) => task_result,
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
                    error!("a flush task failed. {:?}", e);
                    self.join_handle =
                        (self.spawn_upload_task)(self.payload.clone(), self.task_info.clone());
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

#[derive(Default)]
/// Manage the spilled data. Task and uploaded data at the front is newer data. Task data are
/// always newer than uploaded data. Task holding oldest data is always collected first.
struct SpilledData {
    // ordered spilling tasks. Task at the back is spilling older data.
    uploading_tasks: VecDeque<UploadingTask>,
    // ordered spilled data. Data at the back is older.
    uploaded_data: VecDeque<StagingSstableInfo>,
}

impl SpilledData {
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

#[derive(Default)]
struct UnsealedEpochData {
    // newer data at the front
    imms: VecDeque<ImmutableMemtable>,
    spilled_data: SpilledData,
}

impl UnsealedEpochData {
    fn flush(&mut self, context: &UploaderContext) {
        let imms = self.imms.drain(..).collect_vec();
        self.spilled_data
            .add_task(UploadingTask::new(imms, context));
    }
}

#[derive(Default)]
/// Data at the sealed stage. We will ensure that data in `imms` are newer than the data in the
/// `spilled_data`, and that data in the `uploading_tasks` in `spilled_data` are newer than data in
/// the `uploaded_data` in `spilled_data`.
struct SealedData {
    // newer epoch at the front and newer data at the front in the `VecDeque`
    imms: VecDeque<(HummockEpoch, VecDeque<ImmutableMemtable>)>,
    spilled_data: SpilledData,
}

impl SealedData {
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
        if let Some((prev_max_sealed_epoch, _)) = self.imms.front() {
            assert!(
                epoch > *prev_max_sealed_epoch,
                "epoch {} to seal not greater than prev max sealed epoch {}",
                epoch,
                prev_max_sealed_epoch
            );
        }
        // the newly added data are at the front
        self.imms.push_front((epoch, unseal_epoch_data.imms));
        unseal_epoch_data
            .spilled_data
            .uploading_tasks
            .extend(self.spilled_data.uploading_tasks.drain(..));
        unseal_epoch_data
            .spilled_data
            .uploaded_data
            .extend(self.spilled_data.uploaded_data.drain(..));
        self.spilled_data.uploading_tasks = unseal_epoch_data.spilled_data.uploading_tasks;
        self.spilled_data.uploaded_data = unseal_epoch_data.spilled_data.uploaded_data;
    }

    fn flush(&mut self, context: &UploaderContext) {
        let imms = self.imms.drain(..);

        let payload = imms
            .into_iter()
            // in `imms`, newer data comes first
            .flat_map(|(_epoch, imms)| imms)
            .collect_vec();

        if !payload.is_empty() {
            self.spilled_data
                .add_task(UploadingTask::new(payload, context));
        }
    }

    /// Clear self and return the current sealed data
    fn drain(&mut self) -> SealedData {
        let mut ret = SealedData::default();
        swap(&mut ret, self);
        ret
    }
}

struct SyncingData {
    sync_epoch: HummockEpoch,
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
    spawn_upload_task: SpawnUploadTask,
    buffer_tracker: BufferTracker,
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
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
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
            context: UploaderContext {
                pinned_version,
                spawn_upload_task,
                buffer_tracker,
            },
        }
    }

    pub(crate) fn buffer_tracker(&self) -> &BufferTracker {
        &self.context.buffer_tracker
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
        let epoch = imm.epoch();
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
                warn!("epoch {} to seal has no data", epoch);
            }
        }
    }

    pub(crate) fn start_sync_epoch(&mut self, epoch: HummockEpoch) {
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

        self.sealed_data.flush(&self.context);

        let SealedData {
            imms,
            spilled_data:
                SpilledData {
                    uploading_tasks,
                    uploaded_data,
                },
        } = self.sealed_data.drain();

        assert!(imms.is_empty(), "after flush, imms must be empty");

        let try_join_all_upload_task = if uploading_tasks.is_empty() {
            None
        } else {
            Some(try_join_all(uploading_tasks))
        };

        if let Some(SyncingData {
            sync_epoch: prev_max_syncing_epoch,
            ..
        }) = self.syncing_data.front()
        {
            assert!(
                epoch > *prev_max_syncing_epoch,
                "epoch {} to sync not greater than prev max syncing epoch: {}",
                epoch,
                prev_max_syncing_epoch
            );
        }

        self.syncing_data.push_front(SyncingData {
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
        assert!(pinned_version.version().id > self.context.pinned_version.version().id);
        assert!(self.max_synced_epoch >= pinned_version.max_committed_epoch());
        self.synced_data
            .retain(|epoch, _| *epoch > pinned_version.max_committed_epoch());
        self.context.pinned_version = pinned_version;
    }

    pub(crate) fn try_flush(&mut self) {
        if self.buffer_tracker().need_more_flush() {
            self.sealed_data.flush(&self.context);
        }

        if self.context.buffer_tracker.need_more_flush() {
            // iterate from older epoch to newer epoch
            for unsealed_data in self.unsealed_data.values_mut() {
                if !self.context.buffer_tracker.need_more_flush() {
                    break;
                }
                unsealed_data.flush(&self.context);
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
        self.sealed_data.spilled_data.clear();
        self.sealed_data.imms.clear();
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
}

pub(crate) struct NextUploaderEvent<'a> {
    uploader: &'a mut HummockUploader,
}

pub(crate) enum UploaderEvent {
    // staging sstable info of newer data comes first
    SyncFinish(HummockEpoch, Vec<StagingSstableInfo>),
    DataSpilled(StagingSstableInfo),
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

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::{BufMut, BytesMut};
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::HummockEpoch;
    use risingwave_pb::hummock::{HummockVersion, SstableInfo};
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::task::yield_now;

    use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
    use crate::hummock::event_handler::uploader::{UploaderContext, UploadingTask};
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
    use crate::hummock::store::memtable::ImmutableMemtable;
    use crate::hummock::store::version::StagingSstableInfo;
    use crate::hummock::HummockError;
    use crate::storage_value::StorageValue;

    const INITIAL_EPOCH: HummockEpoch = 100;
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    fn initial_pinned_version() -> PinnedVersion {
        let initial_version = HummockVersion {
            id: 0,
            levels: Default::default(),
            max_committed_epoch: INITIAL_EPOCH,
            safe_epoch: 0,
        };
        PinnedVersion::new(initial_version, unbounded_channel().0)
    }

    async fn gen_imm(epoch: HummockEpoch, test_batch_id: u8) -> ImmutableMemtable {
        let mut key_builder = BytesMut::new();
        key_builder.put_u32(TEST_TABLE_ID.table_id);
        key_builder.put_u8(test_batch_id);
        let key = key_builder.freeze();
        SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            vec![(key, StorageValue::new_delete())],
            TEST_TABLE_ID,
            None,
        )
        .await
    }

    fn gen_sstable_info(test_sst_id: u64) -> SstableInfo {
        SstableInfo {
            id: test_sst_id,
            key_range: None,
            file_size: 0,
            table_ids: vec![TEST_TABLE_ID.table_id],
            meta_offset: 0,
            stale_key_count: 0,
            total_key_count: 0,
            divide_version: 0,
        }
    }

    #[tokio::test]
    pub async fn test_uploading_task_future() {
        let uploader_context = UploaderContext {
            pinned_version: initial_pinned_version(),
            spawn_upload_task: Arc::new(move |_, _| {
                tokio::spawn(async move {
                    Ok(StagingSstableInfo::new(
                        vec![gen_sstable_info(1), gen_sstable_info(2)],
                        vec![INITIAL_EPOCH],
                        vec![1],
                        1,
                    ))
                })
            }),
            buffer_tracker: BufferTracker::for_test(),
        };
        let task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH, 1).await], &uploader_context);
        let output = task.await.unwrap();
        assert_eq!(
            output.sstable_infos(),
            &vec![gen_sstable_info(1), gen_sstable_info(2)]
        );

        let uploader_context = UploaderContext {
            pinned_version: initial_pinned_version(),
            spawn_upload_task: Arc::new(move |_, _| {
                tokio::spawn(async move { Err(HummockError::other("failed")) })
            }),
            buffer_tracker: BufferTracker::for_test(),
        };
        let task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH, 1).await], &uploader_context);
        let _ = task.await.unwrap_err();
    }

    #[tokio::test]
    pub async fn test_uploading_task_poll_result() {
        let uploader_context = UploaderContext {
            pinned_version: initial_pinned_version(),
            spawn_upload_task: Arc::new(move |_, _| {
                tokio::spawn(async move {
                    Ok(StagingSstableInfo::new(
                        vec![gen_sstable_info(1), gen_sstable_info(2)],
                        vec![INITIAL_EPOCH],
                        vec![1],
                        1,
                    ))
                })
            }),
            buffer_tracker: BufferTracker::for_test(),
        };
        let mut task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH, 1).await], &uploader_context);
        let output = loop {
            if let Poll::Ready(result) = poll_fn(|cx| Poll::Ready(task.poll_result(cx))).await {
                break result;
            }
            yield_now().await;
        }
        .unwrap();
        assert_eq!(
            output.sstable_infos(),
            &vec![gen_sstable_info(1), gen_sstable_info(2)]
        );

        let uploader_context = UploaderContext {
            pinned_version: initial_pinned_version(),
            spawn_upload_task: Arc::new(move |_, _| {
                tokio::spawn(async move { Err(HummockError::other("failed")) })
            }),
            buffer_tracker: BufferTracker::for_test(),
        };
        let mut task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH, 1).await], &uploader_context);
        let _ = loop {
            if let Poll::Ready(result) = poll_fn(|cx| Poll::Ready(task.poll_result(cx))).await {
                break result;
            }
            yield_now().await;
        }
        .unwrap_err();
    }

    #[tokio::test]
    async fn test_uploading_task_poll_ok_with_retry() {
        let run_count = Arc::new(AtomicUsize::new(0));
        let fail_num = 10;
        let run_count_clone = run_count.clone();
        let uploader_context = UploaderContext {
            pinned_version: initial_pinned_version(),
            spawn_upload_task: Arc::new(move |_, _| {
                let run_count = run_count.clone();
                tokio::spawn(async move {
                    // fail in the first `fail_num` run, and success at the end
                    let ret = if run_count.load(SeqCst) < fail_num {
                        Err(HummockError::other("fail"))
                    } else {
                        Ok(StagingSstableInfo::new(
                            vec![gen_sstable_info(1), gen_sstable_info(2)],
                            vec![INITIAL_EPOCH],
                            vec![1],
                            1,
                        ))
                    };
                    run_count.fetch_add(1, SeqCst);
                    ret
                })
            }),
            buffer_tracker: BufferTracker::for_test(),
        };
        let mut task = UploadingTask::new(vec![gen_imm(INITIAL_EPOCH, 1).await], &uploader_context);
        let output = loop {
            if let Poll::Ready(result) =
                poll_fn(|cx| Poll::Ready(task.poll_ok_with_retry(cx))).await
            {
                break result;
            }
            yield_now().await;
        };
        assert_eq!(fail_num + 1, run_count_clone.load(SeqCst));
        assert_eq!(
            output.sstable_infos(),
            &vec![gen_sstable_info(1), gen_sstable_info(2)]
        );
    }
}
