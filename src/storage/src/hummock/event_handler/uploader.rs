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

use std::any::Any;
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
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, StreamExt, TryFuture};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::SstableInfo;
use tokio::spawn;
use tokio::task::{JoinError, JoinHandle};
use tracing::{error, warn};

use crate::hummock::compactor::compact;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::UncommittedData;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::store::version::{StagingData, StagingSstableInfo};
use crate::hummock::{HummockError, HummockResult};

type TaskPayload = Vec<ImmutableMemtable>;

async fn flush_imms(
    payload: TaskPayload,
    epochs: Vec<HummockEpoch>,
    imm_ids: Vec<ImmId>,
    compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
    compactor_context: Arc<crate::hummock::compactor::Context>,
) -> HummockResult<StagingSstableInfo> {
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
            compaction_group_index,
            task_size_guard,
            compactor_context,
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
    // ordered spilling tasks. Task at the front is spilling older data.
    uploading_tasks: VecDeque<UploadingTask>,
    // ordered spilled data. Data at the front is older.
    uploaded_data: Vec<StagingSstableInfo>,
}

impl SpilledData {
    fn extend(&mut self, other: Self) {
        self.uploading_tasks.extend(other.uploading_tasks);
        self.uploaded_data.extend(other.uploaded_data);
    }

    fn poll_success_spill(&mut self, cx: &mut Context<'_>) -> Poll<Option<StagingSstableInfo>> {
        // only poll the oldest uploading task if there is any
        if let Some(task) = self.uploading_tasks.front_mut() {
            let staging_sstable_info = ready!(task.poll_ok_with_retry(cx));
            self.uploaded_data.push(staging_sstable_info.clone());
            self.uploading_tasks.pop_front();
            Poll::Ready(Some(staging_sstable_info))
        } else {
            Poll::Ready(None)
        }
    }

    fn add_task(&mut self, task: UploadingTask) {
        self.uploading_tasks.push_back(task);
    }

    fn clear(&mut self) {
        self.uploading_tasks.clear();
        self.uploaded_data.clear();
    }
}

#[derive(Default)]
struct UnsealedEpochData {
    // newer data comes first
    imms: VecDeque<ImmutableMemtable>,
    spilled_data: SpilledData,
}

#[derive(Default)]
struct SealedData {
    // newer data comes first in the `VecDeque`
    imms: BTreeMap<HummockEpoch, VecDeque<ImmutableMemtable>>,
    spilled_data: SpilledData,
}

struct SyncingData {
    // TODO: may replace `TryJoinAll` with a future that will abort other join handles once
    // one join handle failed.
    uploading_tasks: TryJoinAll<UploadingTask>,
    uploaded: Vec<StagingSstableInfo>,
}

type SyncedDataState = HummockResult<Vec<StagingSstableInfo>>;

pub(crate) struct Uploader {
    // max_synced_epoch <= max_syncing_epoch <= max_sealed_epoch
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
    syncing_data: BTreeMap<HummockEpoch, SyncingData>,

    /// Data that has been synced already. `epoch` satisfies
    /// `epoch <= max_synced_epoch`.
    synced_data: BTreeMap<HummockEpoch, SyncedDataState>,

    aborted_futures: FuturesUnordered<BoxFuture<'static, ()>>,

    pinned_version: PinnedVersion,

    uploading_task_size: Arc<AtomicUsize>,

    compactor_context: Arc<crate::hummock::compactor::Context>,
}

impl Uploader {
    pub(crate) fn new(
        pinned_version: PinnedVersion,
        compactor_context: Arc<crate::hummock::compactor::Context>,
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
            uploading_task_size: Arc::new(AtomicUsize::new(0)),
            compactor_context,
        }
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

    pub(crate) fn seal_epoch(&mut self, epoch: HummockEpoch, is_checkpoint: bool) {
        assert!(
            epoch > self.max_sealed_epoch,
            "new sealed epoch {} older than max sealed epoch {}",
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
                assert!(self
                    .sealed_data
                    .imms
                    .insert(epoch, unsealed_data.imms)
                    .is_none());
                self.sealed_data
                    .spilled_data
                    .extend(unsealed_data.spilled_data);
            } else {
                warn!("epoch {} to seal has no data", epoch);
            }
        }

        if is_checkpoint {
            assert!(
                epoch > self.max_syncing_epoch,
                "epoch to sync {} is not greater than previous max syncing epoch {}",
                epoch,
                self.max_syncing_epoch
            );

            self.max_syncing_epoch = epoch;

            // Take the
            let sealed_data = {
                let mut sealed_data = SealedData::default();
                swap(&mut sealed_data, &mut self.sealed_data);
                sealed_data
            };

            // Take rev to let newer epoch comes first
            let epochs = sealed_data.imms.keys().cloned().rev().collect_vec();

            let SpilledData {
                mut uploading_tasks,
                uploaded_data,
            } = sealed_data.spilled_data;

            let payload = sealed_data
                .imms
                .into_iter()
                .rev() // Take rev so that newer epochs comes first
                // in `imms`, newer data comes first
                .flat_map(|(epoch, imms)| imms)
                .collect_vec();

            if !payload.is_empty() {
                uploading_tasks.push_back(UploadingTask::new(
                    payload,
                    epochs,
                    self.pinned_version.compaction_group_index().clone(),
                    self.uploading_task_size.clone(),
                    self.compactor_context.clone(),
                ));
            }

            if uploading_tasks.is_empty() {
                self.add_synced_data(epoch, Ok(uploaded_data));
                return;
            }

            let try_join_all = try_join_all(uploading_tasks);

            assert!(self
                .syncing_data
                .insert(
                    epoch,
                    SyncingData {
                        uploading_tasks: try_join_all,
                        uploaded: uploaded_data
                    }
                )
                .is_none());
        }
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

    pub(crate) fn get_task_size(&self) -> usize {
        self.uploading_task_size.load(Relaxed)
    }
}

impl Uploader {
    fn abort_task<T: Send + 'static>(&mut self, join_handle: JoinHandle<T>) {
        join_handle.abort();
        self.aborted_futures.push(
            async move {
                match join_handle.await {
                    Ok(_) => {
                        warn!("a task finished successfully after being cancelled");
                    }
                    Err(e) => match e.try_into_panic() {
                        Ok(panic_err) => {
                            error!("a cancelled task panics: {:?}", panic_err);
                        }
                        Err(_) => {}
                    },
                }
            }
            .boxed(),
        );
    }

    fn poll_syncing_task(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(HummockEpoch, HummockResult<Vec<StagingSstableInfo>>)>> {
        // Only poll the oldest epoch if there is any so that the syncing epoch are finished in
        // order
        if let Some(mut entry) = self.syncing_data.first_entry() {
            // The syncing task has finished
            let result = ready!(entry.get_mut().uploading_tasks.poll_unpin(cx));
            let (epoch, _) = self
                .syncing_data
                .pop_first()
                .expect("first entry must exist");
            let ret = match &result {
                Ok(sstable_infos) => Ok(sstable_infos.clone()),
                Err(e) => {
                    let err_str = format!("syncing data of epoch {} failed. {:?}", epoch, e);
                    error!("{}", err_str);
                    Err(HummockError::other(err_str))
                }
            };
            self.add_synced_data(epoch, result);
            Poll::Ready(Some((epoch, ret)))
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
    uploader: &'a mut Uploader,
}

pub(crate) enum UploaderEvent {
    SyncFinish {
        epoch: HummockEpoch,
        result: HummockResult<Vec<StagingSstableInfo>>,
    },
    DataSpilled(StagingSstableInfo),
}

impl<'a> Future for NextUploaderEvent<'a> {
    type Output = UploaderEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let uploader = &mut self.deref_mut().uploader;

        if let Some((epoch, result)) = ready!(uploader.poll_syncing_task(cx)) {
            return Poll::Ready(UploaderEvent::SyncFinish { epoch, result });
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
