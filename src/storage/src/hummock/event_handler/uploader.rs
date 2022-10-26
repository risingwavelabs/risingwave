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

use futures::future::{try_join_all, BoxFuture, TryJoinAll};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch};
use tokio::spawn;
use tokio::task::JoinHandle;
use tracing::{error, warn};

use crate::hummock::compactor::compact;
use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::UncommittedData;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::store::version::StagingSstableInfo;
use crate::hummock::{HummockError, HummockResult};

type TaskPayload = Vec<ImmutableMemtable>;

async fn flush_imms(
    payload: TaskPayload,
    epochs: Vec<HummockEpoch>,
    imm_ids: Vec<ImmId>,
    compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
    compactor_context: Arc<crate::hummock::compactor::Context>,
) -> HummockResult<StagingSstableInfo> {
    for epoch in &epochs {
        let _ = compactor_context
            .sstable_id_manager
            .add_watermark_sst_id(Some(*epoch))
            .await
            .inspect_err(|e| {
                error!("unable to set watermark sst id. epoch: {}, {:?}", epoch, e);
            });
    }
    let sstable_infos = compact(
        compactor_context,
        payload
            .into_iter()
            .map(|imm| vec![UncommittedData::Batch(imm)])
            .collect(),
        compaction_group_index,
    )
    .await?
    .into_iter()
    .map(|(_, sstable_info)| sstable_info)
    .collect();
    Ok(StagingSstableInfo::new(sstable_infos, epochs, imm_ids))
}

struct UploadingTask {
    payload: TaskPayload,
    join_handle: JoinHandle<HummockResult<StagingSstableInfo>>,
    task_size: usize,
    epochs: Vec<HummockEpoch>,
    imm_ids: Vec<ImmId>,
    compactor_context: Arc<crate::hummock::compactor::Context>,
    compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
    task_size_guard: Arc<AtomicUsize>,
}

impl Drop for UploadingTask {
    fn drop(&mut self) {
        self.task_size_guard.fetch_sub(self.task_size, Relaxed);
    }
}

impl Debug for UploadingTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadingTask")
            .field("payload", &self.payload)
            .field("task_size", &self.task_size)
            .field("epochs", &self.epochs)
            .field("imm_ids", &self.imm_ids)
            .field("compaction_group_index", &self.compaction_group_index)
            .finish()
    }
}

impl UploadingTask {
    fn new(
        payload: TaskPayload,
        epochs: Vec<HummockEpoch>,
        compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
        task_size_guard: Arc<AtomicUsize>,
        compactor_context: Arc<crate::hummock::compactor::Context>,
    ) -> Self {
        let imm_ids = payload.iter().map(|imm| imm.batch_id()).collect_vec();
        let task_size = payload.iter().map(|imm| imm.size()).sum();
        task_size_guard.fetch_add(task_size, Relaxed);
        let join_handle = spawn(flush_imms(
            payload.clone(),
            epochs.clone(),
            imm_ids.clone(),
            compaction_group_index.clone(),
            compactor_context.clone(),
        ));
        Self {
            payload,
            join_handle,
            task_size,
            epochs,
            imm_ids,
            compactor_context,
            compaction_group_index,
            task_size_guard,
        }
    }

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

    fn poll_ok_with_retry(&mut self, cx: &mut Context<'_>) -> Poll<StagingSstableInfo> {
        loop {
            let result = ready!(self.poll_result(cx));
            match result {
                Ok(sstables) => return Poll::Ready(sstables),
                Err(e) => {
                    error!("a flush task failed. {:?}", e);
                    self.join_handle = spawn(flush_imms(
                        self.payload.clone(),
                        self.epochs.clone(),
                        self.imm_ids.clone(),
                        self.compaction_group_index.clone(),
                        self.compactor_context.clone(),
                    ));
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
struct SpilledData {
    // ordered spilling tasks. Task at the back is spilling older data.
    uploading_tasks: VecDeque<UploadingTask>,
    // ordered spilled data. Data at the back is older.
    uploaded_data: VecDeque<StagingSstableInfo>,
}

impl SpilledData {
    fn add_new(&mut self, mut other: Self) {
        // the newly added data are at the front
        other.uploading_tasks.extend(self.uploading_tasks.drain(..));
        other.uploaded_data.extend(self.uploaded_data.drain(..));
        self.uploading_tasks = other.uploading_tasks;
        self.uploaded_data = other.uploaded_data;
    }

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

#[derive(Default)]
struct SealedData {
    // newer epoch at the front and newer data at the front in the `VecDeque`
    imms: VecDeque<(HummockEpoch, VecDeque<ImmutableMemtable>)>,
    spilled_data: SpilledData,
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

    aborted_futures: FuturesUnordered<BoxFuture<'static, ()>>,

    pinned_version: PinnedVersion,

    compactor_context: Arc<crate::hummock::compactor::Context>,

    buffer_tracker: BufferTracker,
}

impl HummockUploader {
    pub(crate) fn new(
        pinned_version: PinnedVersion,
        compactor_context: Arc<crate::hummock::compactor::Context>,
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
            aborted_futures: Default::default(),
            pinned_version,
            compactor_context,
            buffer_tracker,
        }
    }

    pub(crate) fn buffer_tracker(&self) -> &BufferTracker {
        &self.buffer_tracker
    }

    pub(crate) fn max_synced_epoch(&self) -> HummockEpoch {
        self.max_synced_epoch
    }

    pub(crate) fn max_committed_epoch(&self) -> HummockEpoch {
        self.pinned_version.max_committed_epoch()
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
                if let Some((prev_max_sealed_epoch, _)) = self.sealed_data.imms.front() {
                    assert!(
                        epoch > *prev_max_sealed_epoch,
                        "epoch {} to seal not greater than prev max sealed epoch {}",
                        epoch,
                        prev_max_sealed_epoch
                    );
                }
                self.sealed_data
                    .imms
                    .push_front((epoch, unsealed_data.imms));
                self.sealed_data
                    .spilled_data
                    .add_new(unsealed_data.spilled_data);
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
        assert!(
            epoch <= self.max_sealed_epoch,
            "the epoch {} to start syncing has not sealed yet: {}",
            epoch,
            self.max_sealed_epoch,
        );

        self.max_syncing_epoch = epoch;

        // Take the current sealed data out
        let sealed_data = {
            let mut sealed_data = SealedData::default();
            swap(&mut sealed_data, &mut self.sealed_data);
            sealed_data
        };

        let SpilledData {
            mut uploading_tasks,
            uploaded_data,
        } = sealed_data.spilled_data;

        // newer epoch comes first
        let epochs = sealed_data
            .imms
            .iter()
            .map(|(epoch, _)| *epoch)
            .collect_vec();

        let payload = sealed_data
            .imms
            .into_iter()
            // in `imms`, newer data comes first
            .flat_map(|(_epoch, imms)| imms)
            .collect_vec();

        if !payload.is_empty() {
            uploading_tasks.push_front(UploadingTask::new(
                payload,
                epochs,
                self.pinned_version.compaction_group_index(),
                self.buffer_tracker.global_upload_task_size(),
                self.compactor_context.clone(),
            ));
        }

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
        assert!(pinned_version.version().id > self.pinned_version.version().id);
        assert!(self.max_synced_epoch >= pinned_version.max_committed_epoch());
        self.synced_data
            .retain(|epoch, _| *epoch > pinned_version.max_committed_epoch());
        self.pinned_version = pinned_version;
    }

    pub(crate) fn try_flush(&mut self) {
        todo!()
    }

    pub(crate) fn clear(&mut self) {
        let max_committed_epoch = self.pinned_version.max_committed_epoch();
        self.max_synced_epoch = max_committed_epoch;
        self.max_syncing_epoch = max_committed_epoch;
        self.max_sealed_epoch = max_committed_epoch;
        self.synced_data.clear();
        self.syncing_data.clear();
        self.sealed_data.spilled_data.clear();
        self.sealed_data.imms.clear();
        self.unsealed_data.clear();

        // TODO: call `abort_task` on the uploading task join handle
    }

    pub(crate) fn next_event(&mut self) -> NextUploaderEvent<'_> {
        NextUploaderEvent { uploader: self }
    }
}

impl HummockUploader {
    #[expect(dead_code)]
    fn abort_task<T: Send + 'static>(&mut self, join_handle: JoinHandle<T>) {
        join_handle.abort();
        self.aborted_futures.push(
            async move {
                match join_handle.await {
                    Ok(_) => {
                        warn!("a task finished successfully after being cancelled");
                    }
                    Err(e) => {
                        if let Ok(panic_err) = e.try_into_panic() {
                            error!("a cancelled task panics: {:?}", panic_err);
                        }
                    }
                }
            }
            .boxed(),
        );
    }

    fn poll_syncing_task(&mut self, cx: &mut Context<'_>) -> Poll<Option<HummockEpoch>> {
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

            let result = result.map(|mut sstable_infos| {
                // The newly uploaded `sstable_infos` contains newer data. Therefore,
                // `sstable_infos` at the front
                sstable_infos.extend(syncing_data.uploaded);
                sstable_infos
            });
            self.add_synced_data(epoch, result);
            Poll::Ready(Some(epoch))
        } else {
            Poll::Ready(None)
        }
    }

    fn poll_sealed_spill_task(&mut self, cx: &mut Context<'_>) -> Poll<Option<StagingSstableInfo>> {
        self.sealed_data.spilled_data.poll_success_spill(cx)
    }

    fn poll_unsealed_spill_task(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<StagingSstableInfo>> {
        // iterator from older epoch to new epoch so that the spill task are finished in epoch order
        for unsealed_data in self.unsealed_data.values_mut() {
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
    SyncFinish(HummockEpoch),
    DataSpilled(StagingSstableInfo),
}

impl<'a> Future for NextUploaderEvent<'a> {
    type Output = UploaderEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let uploader = &mut self.deref_mut().uploader;

        if let Some(epoch) = ready!(uploader.poll_syncing_task(cx)) {
            return Poll::Ready(UploaderEvent::SyncFinish(epoch));
        }

        if let Some(sstable_info) = ready!(uploader.poll_sealed_spill_task(cx)) {
            return Poll::Ready(UploaderEvent::DataSpilled(sstable_info));
        }

        if let Some(sstable_info) = ready!(uploader.poll_unsealed_spill_task(cx)) {
            return Poll::Ready(UploaderEvent::DataSpilled(sstable_info));
        }

        // collect and clear aborted futures.
        while let Poll::Ready(Some(_)) = uploader.aborted_futures.poll_next_unpin(cx) {}

        Poll::Pending
    }
}
