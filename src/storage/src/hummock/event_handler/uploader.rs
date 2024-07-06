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

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::future::{poll_fn, Future};
use std::mem::{replace, swap, take};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures::FutureExt;
use itertools::Itertools;
use more_asserts::{assert_ge, assert_gt};
use prometheus::core::{AtomicU64, GenericGauge};
use prometheus::{HistogramTimer, IntGauge};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::must_match;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarks, VnodeWatermark, WatermarkDirection,
};
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, LocalSstableInfo};
use thiserror_ext::AsReport;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::hummock::event_handler::hummock_event_handler::{send_sync_result, BufferTracker};
use crate::hummock::event_handler::uploader::uploader_imm::UploaderImm;
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchId;
use crate::hummock::store::version::StagingSstableInfo;
use crate::hummock::{HummockError, HummockResult, ImmutableMemtable};
use crate::mem_table::ImmId;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;
use crate::store::SealCurrentEpochOptions;

/// Take epoch data inclusively before `epoch` out from `data`
fn take_before_epoch<T>(
    data: &mut BTreeMap<HummockEpoch, T>,
    epoch: HummockEpoch,
) -> BTreeMap<HummockEpoch, T> {
    let mut before_epoch_data = data.split_off(&(epoch + 1));
    swap(&mut before_epoch_data, data);
    before_epoch_data
}

type UploadTaskInput = HashMap<LocalInstanceId, Vec<UploaderImm>>;
pub type UploadTaskPayload = HashMap<LocalInstanceId, Vec<ImmutableMemtable>>;

#[derive(Debug)]
pub struct UploadTaskOutput {
    pub new_value_ssts: Vec<LocalSstableInfo>,
    pub old_value_ssts: Vec<LocalSstableInfo>,
    pub wait_poll_timer: Option<HistogramTimer>,
}
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

mod uploader_imm {
    use std::fmt::Formatter;
    use std::ops::Deref;

    use prometheus::core::{AtomicU64, GenericGauge};

    use crate::hummock::event_handler::uploader::UploaderContext;
    use crate::mem_table::ImmutableMemtable;

    pub(super) struct UploaderImm {
        inner: ImmutableMemtable,
        size_guard: GenericGauge<AtomicU64>,
    }

    impl UploaderImm {
        pub(super) fn new(imm: ImmutableMemtable, context: &UploaderContext) -> Self {
            let size = imm.size();
            let size_guard = context.stats.uploader_imm_size.clone();
            size_guard.add(size as _);
            Self {
                inner: imm,
                size_guard,
            }
        }

        #[cfg(test)]
        pub(super) fn for_test(imm: ImmutableMemtable) -> Self {
            Self {
                inner: imm,
                size_guard: GenericGauge::new("test", "test").unwrap(),
            }
        }
    }

    impl std::fmt::Debug for UploaderImm {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.inner.fmt(f)
        }
    }

    impl Deref for UploaderImm {
        type Target = ImmutableMemtable;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl Drop for UploaderImm {
        fn drop(&mut self) {
            self.size_guard.sub(self.inner.size() as _);
        }
    }
}

/// A wrapper for a uploading task that compacts and uploads the imm payload. Task context are
/// stored so that when the task fails, it can be re-tried.
struct UploadingTask {
    // newer data at the front
    input: UploadTaskInput,
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
            .field("input", &self.input)
            .field("task_info", &self.task_info)
            .finish()
    }
}

fn get_payload_imm_ids(
    payload: &UploadTaskPayload,
) -> HashMap<LocalInstanceId, Vec<SharedBufferBatchId>> {
    payload
        .iter()
        .map(|(instance_id, imms)| {
            (
                *instance_id,
                imms.iter().map(|imm| imm.batch_id()).collect_vec(),
            )
        })
        .collect()
}

impl UploadingTask {
    // INFO logs will be enabled for task with size exceeding 50MB.
    const LOG_THRESHOLD_FOR_UPLOAD_TASK_SIZE: usize = 50 * (1 << 20);

    fn input_to_payload(input: &UploadTaskInput) -> UploadTaskPayload {
        input
            .iter()
            .map(|(instance_id, imms)| {
                (
                    *instance_id,
                    imms.iter().map(|imm| (**imm).clone()).collect(),
                )
            })
            .collect()
    }

    fn new(input: UploadTaskInput, context: &UploaderContext) -> Self {
        assert!(!input.is_empty());
        let mut epochs = input
            .iter()
            .flat_map(|(_, imms)| imms.iter().flat_map(|imm| imm.epochs().iter().cloned()))
            .sorted()
            .dedup()
            .collect_vec();

        // reverse to make newer epochs comes first
        epochs.reverse();
        let payload = Self::input_to_payload(&input);
        let imm_ids = get_payload_imm_ids(&payload);
        let task_size = input
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
        let join_handle = (context.spawn_upload_task)(payload, task_info.clone());
        context.stats.uploader_uploading_task_count.inc();
        Self {
            input,
            join_handle,
            task_info,
            spawn_upload_task: context.spawn_upload_task.clone(),
            task_size_guard: context.buffer_tracker.global_upload_task_size().clone(),
            task_count_guard: context.stats.uploader_uploading_task_count.clone(),
        }
    }

    /// Poll the result of the uploading task
    fn poll_result(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<HummockResult<Arc<StagingSstableInfo>>> {
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
                    Arc::new(StagingSstableInfo::new(
                        output.new_value_ssts,
                        output.old_value_ssts,
                        self.task_info.epochs.clone(),
                        self.task_info.imm_ids.clone(),
                        self.task_info.task_size,
                    ))
                }),

            Err(err) => Err(HummockError::other(format!(
                "fail to join upload join handle: {}",
                err.as_report()
            ))),
        })
    }

    /// Poll the uploading task until it succeeds. If it fails, we will retry it.
    fn poll_ok_with_retry(&mut self, cx: &mut Context<'_>) -> Poll<Arc<StagingSstableInfo>> {
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
                    self.join_handle = (self.spawn_upload_task)(
                        Self::input_to_payload(&self.input),
                        self.task_info.clone(),
                    );
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
    type Output = HummockResult<Arc<StagingSstableInfo>>;

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
    uploaded_data: VecDeque<Arc<StagingSstableInfo>>,
}

impl SpilledData {
    fn add_task(&mut self, task: UploadingTask) {
        self.uploading_tasks.push_front(task);
    }

    /// Poll the successful spill of the oldest uploading task. Return `Poll::Ready(None)` is there
    /// is no uploading task
    fn poll_success_spill(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Arc<StagingSstableInfo>>> {
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

    fn abort(self) {
        for task in self.uploading_tasks {
            task.join_handle.abort();
        }
    }
}

#[derive(Default, Debug)]
struct EpochData {
    spilled_data: SpilledData,
}

impl EpochData {
    fn flush(
        &mut self,
        context: &UploaderContext,
        imms: HashMap<LocalInstanceId, Vec<UploaderImm>>,
    ) -> usize {
        if !imms.is_empty() {
            let task = UploadingTask::new(imms, context);
            context.stats.spill_task_counts_from_unsealed.inc();
            context
                .stats
                .spill_task_size_from_unsealed
                .inc_by(task.task_info.task_size as u64);
            info!("Spill unsealed data. Task: {}", task.get_task_info());
            let size = task.task_info.task_size;
            self.spilled_data.add_task(task);
            size
        } else {
            0
        }
    }
}

impl TableUnsyncData {
    fn add_table_watermarks(
        &mut self,
        epoch: HummockEpoch,
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
        match &mut self.table_watermarks {
            Some((prev_direction, prev_watermarks)) => {
                assert_eq!(
                    *prev_direction, direction,
                    "table id {} new watermark direction not match with previous",
                    self.table_id
                );
                match prev_watermarks.entry(epoch) {
                    Entry::Occupied(mut entry) => {
                        let (prev_watermarks, vnode_bitmap) = entry.get_mut();
                        apply_new_vnodes(vnode_bitmap, &table_watermarks);
                        prev_watermarks.extend(table_watermarks);
                    }
                    Entry::Vacant(entry) => {
                        let mut vnode_bitmap = BitmapBuilder::zeroed(VirtualNode::COUNT);
                        apply_new_vnodes(&mut vnode_bitmap, &table_watermarks);
                        entry.insert((table_watermarks, vnode_bitmap));
                    }
                }
            }
            None => {
                let mut vnode_bitmap = BitmapBuilder::zeroed(VirtualNode::COUNT);
                apply_new_vnodes(&mut vnode_bitmap, &table_watermarks);
                self.table_watermarks = Some((
                    direction,
                    BTreeMap::from_iter([(epoch, (table_watermarks, vnode_bitmap))]),
                ));
            }
        }
    }
}

#[derive(Default)]
struct SyncDataBuilder {
    // newer epochs come first
    epochs: VecDeque<HummockEpoch>,

    spilled_data: SpilledData,

    table_watermarks: HashMap<TableId, TableWatermarks>,
}

impl SyncDataBuilder {
    /// Add the data of a new epoch.
    ///
    /// Note: it may happen that, for example, currently we hold `imms` and `spilled_data` of epoch
    /// 3,  and after we add the spilled data of epoch 4, both `imms` and `spilled_data` hold data
    /// of both epoch 3 and 4, which seems breaking the rules that data in `imms` are
    /// always newer than data in `spilled_data`, because epoch 3 data of `imms`
    /// seems older than epoch 4 data of `spilled_data`. However, if this happens, the epoch 3
    /// data of `imms` must not overlap with the epoch 4 data of `spilled_data`. The explanation is
    /// as followed:
    ///
    /// First, unsync data has 3 stages, from earlier to later, imms, uploading task, and
    /// uploaded. When we try to spill unsync data, we first pick the imms of older epoch until
    /// the imms of older epoch are all picked. When we try to poll the uploading tasks of unsync
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
    fn add_new_epoch(&mut self, epoch: HummockEpoch, mut unseal_epoch_data: EpochData) {
        if let Some(prev_max_epoch) = self.epochs.front() {
            assert!(
                epoch > *prev_max_epoch,
                "epoch {} to seal not greater than prev max epoch {}",
                epoch,
                prev_max_epoch
            );
        }

        self.epochs.push_front(epoch);
        // for each local instance, earlier data must be spilled at earlier epoch. Therefore, since we add spill data from old epoch
        // to new epoch,
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

    fn add_table_watermarks(
        &mut self,
        table_id: TableId,
        direction: WatermarkDirection,
        watermarks: impl Iterator<Item = (HummockEpoch, Vec<VnodeWatermark>)>,
    ) {
        let mut table_watermarks: Option<TableWatermarks> = None;
        for (epoch, watermarks) in watermarks {
            match &mut table_watermarks {
                Some(prev_watermarks) => {
                    prev_watermarks.add_new_epoch_watermarks(
                        epoch,
                        Arc::from(watermarks),
                        direction,
                    );
                }
                None => {
                    table_watermarks =
                        Some(TableWatermarks::single_epoch(epoch, watermarks, direction));
                }
            }
        }
        if let Some(table_watermarks) = table_watermarks {
            assert!(self
                .table_watermarks
                .insert(table_id, table_watermarks)
                .is_none());
        }
    }

    fn flush(&mut self, context: &UploaderContext, payload: UploadTaskInput) {
        if !payload.is_empty() {
            let task = UploadingTask::new(payload, context);
            self.spilled_data.add_task(task);
        }
    }
}

struct LocalInstanceEpochData {
    epoch: HummockEpoch,
    // newer data comes first.
    imms: VecDeque<UploaderImm>,
    has_spilled: bool,
}

impl LocalInstanceEpochData {
    fn new(epoch: HummockEpoch) -> Self {
        Self {
            epoch,
            imms: VecDeque::new(),
            has_spilled: false,
        }
    }

    fn epoch(&self) -> HummockEpoch {
        self.epoch
    }

    fn add_imm(&mut self, imm: UploaderImm) {
        assert_eq!(imm.max_epoch(), imm.min_epoch());
        assert_eq!(self.epoch, imm.min_epoch());
        if let Some(prev_imm) = self.imms.front() {
            assert_gt!(imm.batch_id(), prev_imm.batch_id());
        }
        self.imms.push_front(imm);
    }

    fn is_empty(&self) -> bool {
        self.imms.is_empty()
    }
}

struct LocalInstanceUnsyncData {
    table_id: TableId,
    instance_id: LocalInstanceId,
    // None means that the current instance should have stopped advancing
    current_epoch_data: Option<LocalInstanceEpochData>,
    // newer data comes first.
    sealed_data: VecDeque<LocalInstanceEpochData>,
    // newer data comes first
    flushing_imms: VecDeque<SharedBufferBatchId>,
}

impl LocalInstanceUnsyncData {
    fn new(table_id: TableId, instance_id: LocalInstanceId, init_epoch: HummockEpoch) -> Self {
        Self {
            table_id,
            instance_id,
            current_epoch_data: Some(LocalInstanceEpochData::new(init_epoch)),
            sealed_data: VecDeque::new(),
            flushing_imms: Default::default(),
        }
    }

    fn add_imm(&mut self, imm: UploaderImm) {
        assert_eq!(self.table_id, imm.table_id);
        self.current_epoch_data
            .as_mut()
            .expect("should be Some when adding new imm")
            .add_imm(imm);
    }

    fn local_seal_epoch(&mut self, next_epoch: HummockEpoch) -> HummockEpoch {
        let data = self
            .current_epoch_data
            .as_mut()
            .expect("should be Some when seal new epoch");
        let current_epoch = data.epoch;
        debug!(
            instance_id = self.instance_id,
            next_epoch, current_epoch, "local seal epoch"
        );
        assert_gt!(next_epoch, current_epoch);
        let epoch_data = replace(data, LocalInstanceEpochData::new(next_epoch));
        if !epoch_data.is_empty() {
            self.sealed_data.push_front(epoch_data);
        }
        current_epoch
    }

    // imm_ids from old to new, which means in ascending order
    fn ack_flushed(&mut self, imm_ids: impl Iterator<Item = SharedBufferBatchId>) {
        for imm_id in imm_ids {
            assert_eq!(self.flushing_imms.pop_back().expect("should exist"), imm_id);
        }
    }

    fn spill(&mut self, epoch: HummockEpoch) -> Vec<UploaderImm> {
        let imms = if let Some(oldest_sealed_epoch) = self.sealed_data.back() {
            match oldest_sealed_epoch.epoch.cmp(&epoch) {
                Ordering::Less => {
                    unreachable!(
                        "should not spill at this epoch because there \
                    is unspilled data in previous epoch: prev epoch {}, spill epoch {}",
                        oldest_sealed_epoch.epoch, epoch
                    );
                }
                Ordering::Equal => {
                    let epoch_data = self.sealed_data.pop_back().unwrap();
                    assert_eq!(epoch, epoch_data.epoch);
                    epoch_data.imms
                }
                Ordering::Greater => VecDeque::new(),
            }
        } else {
            let Some(current_epoch_data) = &mut self.current_epoch_data else {
                return Vec::new();
            };
            match current_epoch_data.epoch.cmp(&epoch) {
                Ordering::Less => {
                    assert!(
                        current_epoch_data.imms.is_empty(),
                        "should not spill at this epoch because there \
                    is unspilled data in current epoch epoch {}, spill epoch {}",
                        current_epoch_data.epoch,
                        epoch
                    );
                    VecDeque::new()
                }
                Ordering::Equal => {
                    if !current_epoch_data.imms.is_empty() {
                        current_epoch_data.has_spilled = true;
                        take(&mut current_epoch_data.imms)
                    } else {
                        VecDeque::new()
                    }
                }
                Ordering::Greater => VecDeque::new(),
            }
        };
        self.add_flushing_imm(imms.iter().rev().map(|imm| imm.batch_id()));
        imms.into_iter().collect()
    }

    fn add_flushing_imm(&mut self, imm_ids: impl Iterator<Item = SharedBufferBatchId>) {
        for imm_id in imm_ids {
            if let Some(prev_imm_id) = self.flushing_imms.front() {
                assert_gt!(imm_id, *prev_imm_id);
            }
            self.flushing_imms.push_front(imm_id);
        }
    }

    // start syncing the imm inclusively before the `epoch`
    // returning data with newer data coming first
    fn sync(&mut self, epoch: HummockEpoch) -> Vec<UploaderImm> {
        // firstly added from old to new
        let mut ret = Vec::new();
        while let Some(epoch_data) = self.sealed_data.back()
            && epoch_data.epoch() <= epoch
        {
            let imms = self.sealed_data.pop_back().expect("checked exist").imms;
            self.add_flushing_imm(imms.iter().rev().map(|imm| imm.batch_id()));
            ret.extend(imms.into_iter().rev());
        }
        // reverse so that newer data comes first
        ret.reverse();
        if let Some(latest_epoch_data) = &self.current_epoch_data {
            if latest_epoch_data.epoch <= epoch {
                assert!(self.sealed_data.is_empty());
                assert!(latest_epoch_data.is_empty());
                assert!(!latest_epoch_data.has_spilled);
                if cfg!(debug_assertions) {
                    panic!("sync epoch exceeds latest epoch, and the current instance should have be archived");
                }
                warn!(
                    instance_id = self.instance_id,
                    table_id = self.table_id.table_id,
                    "sync epoch exceeds latest epoch, and the current instance should have be archived"
                );
                self.current_epoch_data = None;
            }
        }
        ret
    }

    fn assert_after_epoch(&self, epoch: HummockEpoch) {
        if let Some(oldest_sealed_data) = self.sealed_data.back() {
            assert!(!oldest_sealed_data.imms.is_empty());
            assert_gt!(oldest_sealed_data.epoch, epoch);
        } else if let Some(current_data) = &self.current_epoch_data {
            if current_data.epoch <= epoch {
                assert!(current_data.imms.is_empty() && !current_data.has_spilled);
            }
        }
    }
}

struct TableUnsyncData {
    table_id: TableId,
    instance_data: HashMap<LocalInstanceId, LocalInstanceUnsyncData>,
    #[expect(clippy::type_complexity)]
    table_watermarks: Option<(
        WatermarkDirection,
        BTreeMap<HummockEpoch, (Vec<VnodeWatermark>, BitmapBuilder)>,
    )>,
}

impl TableUnsyncData {
    fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            instance_data: Default::default(),
            table_watermarks: None,
        }
    }

    fn sync(
        &mut self,
        epoch: HummockEpoch,
    ) -> (
        impl Iterator<Item = (LocalInstanceId, Vec<UploaderImm>)> + '_,
        Option<(
            WatermarkDirection,
            impl Iterator<Item = (HummockEpoch, Vec<VnodeWatermark>)>,
        )>,
    ) {
        (
            self.instance_data
                .iter_mut()
                .map(move |(instance_id, data)| (*instance_id, data.sync(epoch))),
            self.table_watermarks
                .as_mut()
                .map(|(direction, watermarks)| {
                    let watermarks = take_before_epoch(watermarks, epoch);
                    (
                        *direction,
                        watermarks
                            .into_iter()
                            .map(|(epoch, (watermarks, _))| (epoch, watermarks)),
                    )
                }),
        )
    }

    fn assert_after_epoch(&self, epoch: HummockEpoch) {
        self.instance_data
            .values()
            .for_each(|instance_data| instance_data.assert_after_epoch(epoch));
        if let Some((_, watermarks)) = &self.table_watermarks
            && let Some((oldest_epoch, _)) = watermarks.first_key_value()
        {
            assert_gt!(*oldest_epoch, epoch);
        }
    }
}

#[derive(Default)]
/// Unsync data, can be either imm or spilled sst, and some aggregated epoch information.
///
/// `instance_data` holds the imm of each individual local instance, and data are first added here.
/// The aggregated epoch information (table watermarks, etc.) and the spilled sst will be added to `epoch_data`.
struct UnsyncData {
    table_data: HashMap<TableId, TableUnsyncData>,
    // An index as a mapping from instance id to its table id
    instance_table_id: HashMap<LocalInstanceId, TableId>,
    epoch_data: BTreeMap<HummockEpoch, EpochData>,
}

impl UnsyncData {
    fn init_instance(
        &mut self,
        table_id: TableId,
        instance_id: LocalInstanceId,
        init_epoch: HummockEpoch,
    ) {
        debug!(
            table_id = table_id.table_id,
            instance_id, init_epoch, "init epoch"
        );
        let table_data = self
            .table_data
            .entry(table_id)
            .or_insert_with(|| TableUnsyncData::new(table_id));
        assert!(table_data
            .instance_data
            .insert(
                instance_id,
                LocalInstanceUnsyncData::new(table_id, instance_id, init_epoch)
            )
            .is_none());
        assert!(self
            .instance_table_id
            .insert(instance_id, table_id)
            .is_none());
        self.epoch_data.entry(init_epoch).or_default();
    }

    fn instance_data(
        &mut self,
        instance_id: LocalInstanceId,
    ) -> Option<&mut LocalInstanceUnsyncData> {
        self.instance_table_id
            .get_mut(&instance_id)
            .cloned()
            .map(move |table_id| {
                self.table_data
                    .get_mut(&table_id)
                    .expect("should exist")
                    .instance_data
                    .get_mut(&instance_id)
                    .expect("should exist")
            })
    }

    fn add_imm(&mut self, instance_id: LocalInstanceId, imm: UploaderImm) {
        self.instance_data(instance_id)
            .expect("should exist")
            .add_imm(imm);
    }

    fn local_seal_epoch(
        &mut self,
        instance_id: LocalInstanceId,
        next_epoch: HummockEpoch,
        opts: SealCurrentEpochOptions,
    ) {
        let table_id = self.instance_table_id[&instance_id];
        let table_data = self.table_data.get_mut(&table_id).expect("should exist");
        let instance_data = table_data
            .instance_data
            .get_mut(&instance_id)
            .expect("should exist");
        let epoch = instance_data.local_seal_epoch(next_epoch);
        self.epoch_data.entry(next_epoch).or_default();
        if let Some((direction, table_watermarks)) = opts.table_watermarks {
            table_data.add_table_watermarks(epoch, table_watermarks, direction);
        }
    }

    fn may_destroy_instance(&mut self, instance_id: LocalInstanceId) {
        if let Some(table_id) = self.instance_table_id.remove(&instance_id) {
            debug!(instance_id, "destroy instance");
            let table_data = self.table_data.get_mut(&table_id).expect("should exist");
            assert!(table_data.instance_data.remove(&instance_id).is_some());
            if table_data.instance_data.is_empty() {
                self.table_data.remove(&table_id);
            }
        }
    }

    fn sync(
        &mut self,
        epoch: HummockEpoch,
        context: &UploaderContext,
        table_ids: HashSet<TableId>,
    ) -> SyncDataBuilder {
        let sync_epoch_data = take_before_epoch(&mut self.epoch_data, epoch);

        let mut sync_data = SyncDataBuilder::default();
        for (epoch, epoch_data) in sync_epoch_data {
            sync_data.add_new_epoch(epoch, epoch_data);
        }

        let mut flush_payload = HashMap::new();
        for (table_id, table_data) in &mut self.table_data {
            if !table_ids.contains(table_id) {
                table_data.assert_after_epoch(epoch);
                continue;
            }
            let (unflushed_payload, table_watermarks) = table_data.sync(epoch);
            for (instance_id, payload) in unflushed_payload {
                if !payload.is_empty() {
                    flush_payload.insert(instance_id, payload);
                }
            }
            if let Some((direction, watermarks)) = table_watermarks {
                sync_data.add_table_watermarks(*table_id, direction, watermarks);
            }
        }
        sync_data.flush(context, flush_payload);
        sync_data
    }

    fn ack_flushed(&mut self, sstable_info: &StagingSstableInfo) {
        for (instance_id, imm_ids) in sstable_info.imm_ids() {
            if let Some(instance_data) = self.instance_data(*instance_id) {
                // take `rev` to let old imm id goes first
                instance_data.ack_flushed(imm_ids.iter().rev().cloned());
            }
        }
    }
}

struct SyncingData {
    sync_epoch: HummockEpoch,
    // task of newer data at the front
    uploading_tasks: VecDeque<UploadingTask>,
    // newer data at the front
    uploaded: VecDeque<Arc<StagingSstableInfo>>,
    table_watermarks: HashMap<TableId, TableWatermarks>,
    sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
}

#[derive(Debug)]
pub struct SyncedData {
    pub uploaded_ssts: VecDeque<Arc<StagingSstableInfo>>,
    pub table_watermarks: HashMap<TableId, TableWatermarks>,
}

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

#[derive(Default)]
struct UploaderData {
    unsync_data: UnsyncData,

    /// Data that has started syncing but not synced yet. `epoch` satisfies
    /// `max_synced_epoch < epoch <= max_syncing_epoch`.
    /// Newer epoch at the front
    syncing_data: VecDeque<SyncingData>,
}

impl UploaderData {
    fn abort(self, err: impl Fn() -> HummockError) {
        for (_, epoch_data) in self.unsync_data.epoch_data {
            epoch_data.spilled_data.abort();
        }
        for syncing_data in self.syncing_data {
            for task in syncing_data.uploading_tasks {
                task.join_handle.abort();
            }
            send_sync_result(syncing_data.sync_result_sender, Err(err()));
        }
    }
}

struct ErrState {
    failed_epoch: HummockEpoch,
    reason: String,
}

enum UploaderState {
    Working(UploaderData),
    Err(ErrState),
}

/// An uploader for hummock data.
///
/// Data have 3 sequential stages: unsync (inside each local instance, data can be unsealed, sealed), syncing, synced.
///
/// The 3 stages are divided by 2 marginal epochs: `max_syncing_epoch`,
/// `max_synced_epoch`. Epochs satisfy the following inequality.
///
/// (epochs of `synced_data`) <= `max_synced_epoch` < (epochs of `syncing_data`) <=
/// `max_syncing_epoch` < (epochs of `unsync_data`)
///
/// Data are mostly stored in `VecDeque`, and the order stored in the `VecDeque` indicates the data
/// order. Data at the front represents ***newer*** data.
pub struct HummockUploader {
    /// The maximum epoch that has started syncing
    max_syncing_epoch: HummockEpoch,
    /// The maximum epoch that has been synced
    max_synced_epoch: HummockEpoch,

    state: UploaderState,

    context: UploaderContext,
}

impl HummockUploader {
    pub(super) fn new(
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        pinned_version: PinnedVersion,
        spawn_upload_task: SpawnUploadTask,
        buffer_tracker: BufferTracker,
        config: &StorageOpts,
    ) -> Self {
        let initial_epoch = pinned_version.version().max_committed_epoch;
        Self {
            max_syncing_epoch: initial_epoch,
            max_synced_epoch: initial_epoch,
            state: UploaderState::Working(UploaderData {
                unsync_data: Default::default(),
                syncing_data: Default::default(),
            }),
            context: UploaderContext::new(
                pinned_version,
                spawn_upload_task,
                buffer_tracker,
                config,
                state_store_metrics,
            ),
        }
    }

    pub(super) fn buffer_tracker(&self) -> &BufferTracker {
        &self.context.buffer_tracker
    }

    pub(super) fn max_synced_epoch(&self) -> HummockEpoch {
        self.max_synced_epoch
    }

    pub(super) fn max_committed_epoch(&self) -> HummockEpoch {
        self.context.pinned_version.max_committed_epoch()
    }

    pub(super) fn hummock_version(&self) -> &PinnedVersion {
        &self.context.pinned_version
    }

    pub(super) fn add_imm(&mut self, instance_id: LocalInstanceId, imm: ImmutableMemtable) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        let imm = UploaderImm::new(imm, &self.context);
        data.unsync_data.add_imm(instance_id, imm);
    }

    pub(super) fn init_instance(
        &mut self,
        instance_id: LocalInstanceId,
        table_id: TableId,
        init_epoch: HummockEpoch,
    ) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        assert_gt!(init_epoch, self.max_syncing_epoch);
        data.unsync_data
            .init_instance(table_id, instance_id, init_epoch);
    }

    pub(super) fn local_seal_epoch(
        &mut self,
        instance_id: LocalInstanceId,
        next_epoch: HummockEpoch,
        opts: SealCurrentEpochOptions,
    ) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        assert_gt!(next_epoch, self.max_syncing_epoch);
        data.unsync_data
            .local_seal_epoch(instance_id, next_epoch, opts);
    }

    pub(super) fn start_sync_epoch(
        &mut self,
        epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
        table_ids: HashSet<TableId>,
    ) {
        let data = match &mut self.state {
            UploaderState::Working(data) => data,
            UploaderState::Err(ErrState {
                failed_epoch,
                reason,
            }) => {
                let result = Err(HummockError::other(format!(
                    "previous epoch {} failed due to [{}]",
                    failed_epoch, reason
                )));
                send_sync_result(sync_result_sender, result);
                return;
            }
        };
        debug!("start sync epoch: {}", epoch);
        assert!(
            epoch > self.max_syncing_epoch,
            "the epoch {} has started syncing already: {}",
            epoch,
            self.max_syncing_epoch
        );

        self.max_syncing_epoch = epoch;

        let sync_data = data.unsync_data.sync(epoch, &self.context, table_ids);

        let SyncDataBuilder {
            spilled_data:
                SpilledData {
                    uploading_tasks,
                    uploaded_data,
                },
            table_watermarks,
            ..
        } = sync_data;

        data.syncing_data.push_front(SyncingData {
            sync_epoch: epoch,
            uploading_tasks,
            uploaded: uploaded_data,
            table_watermarks,
            sync_result_sender,
        });

        self.context
            .stats
            .uploader_syncing_epoch_count
            .set(data.syncing_data.len() as _);
    }

    fn set_max_synced_epoch(
        max_synced_epoch: &mut HummockEpoch,
        max_syncing_epoch: HummockEpoch,
        epoch: HummockEpoch,
    ) {
        assert!(
            epoch <= max_syncing_epoch,
            "epoch {} that has been synced has not started syncing yet.  previous max syncing epoch {}",
            epoch,
            max_syncing_epoch
        );
        assert!(
            epoch > *max_synced_epoch,
            "epoch {} has been synced. previous max synced epoch: {}",
            epoch,
            max_synced_epoch
        );
        *max_synced_epoch = epoch;
    }

    pub(crate) fn update_pinned_version(&mut self, pinned_version: PinnedVersion) {
        assert_ge!(
            pinned_version.max_committed_epoch(),
            self.context.pinned_version.max_committed_epoch()
        );
        let max_committed_epoch = pinned_version.max_committed_epoch();
        self.context.pinned_version = pinned_version;
        if self.max_synced_epoch < max_committed_epoch {
            self.max_synced_epoch = max_committed_epoch;
        }
        if self.max_syncing_epoch < max_committed_epoch {
            self.max_syncing_epoch = max_committed_epoch;
            if let UploaderState::Working(data) = &self.state {
                for instance_data in data
                    .unsync_data
                    .table_data
                    .values()
                    .flat_map(|data| data.instance_data.values())
                {
                    if let Some(oldest_epoch) = instance_data.sealed_data.back() {
                        assert_gt!(oldest_epoch.epoch, max_committed_epoch);
                    } else if let Some(current_epoch) = &instance_data.current_epoch_data {
                        assert_gt!(current_epoch.epoch, max_committed_epoch);
                    }
                }
            }
        }
    }

    pub(crate) fn may_flush(&mut self) -> bool {
        let UploaderState::Working(data) = &mut self.state else {
            return false;
        };
        if self.context.buffer_tracker.need_flush() {
            let mut curr_batch_flush_size = 0;
            // iterate from older epoch to newer epoch
            for (epoch, epoch_data) in &mut data.unsync_data.epoch_data {
                if !self
                    .context
                    .buffer_tracker
                    .need_more_flush(curr_batch_flush_size)
                {
                    break;
                }
                let mut payload = HashMap::new();
                for (instance_id, instance_data) in data
                    .unsync_data
                    .table_data
                    .values_mut()
                    .flat_map(|data| data.instance_data.iter_mut())
                {
                    let instance_payload = instance_data.spill(*epoch);
                    if !instance_payload.is_empty() {
                        payload.insert(*instance_id, instance_payload);
                    }
                }
                curr_batch_flush_size += epoch_data.flush(&self.context, payload);
            }
            curr_batch_flush_size > 0
        } else {
            false
        }
    }

    pub(crate) fn clear(&mut self) {
        let max_committed_epoch = self.context.pinned_version.max_committed_epoch();
        self.max_synced_epoch = max_committed_epoch;
        self.max_syncing_epoch = max_committed_epoch;
        if let UploaderState::Working(data) = replace(
            &mut self.state,
            UploaderState::Working(UploaderData::default()),
        ) {
            data.abort(|| {
                HummockError::other(format!("uploader is reset to {}", max_committed_epoch))
            });
        }

        self.context.stats.uploader_syncing_epoch_count.set(0);
    }

    pub(crate) fn may_destroy_instance(&mut self, instance_id: LocalInstanceId) {
        let UploaderState::Working(data) = &mut self.state else {
            return;
        };
        data.unsync_data.may_destroy_instance(instance_id);
    }
}

impl UploaderData {
    /// Poll the syncing task of the syncing data of the oldest epoch. Return `Poll::Ready(None)` if
    /// there is no syncing data.
    fn poll_syncing_task(
        &mut self,
        cx: &mut Context<'_>,
        context: &UploaderContext,
        mut set_max_synced_epoch: impl FnMut(u64),
    ) -> Poll<Option<Result<Arc<StagingSstableInfo>, ErrState>>> {
        while let Some(syncing_data) = self.syncing_data.back_mut() {
            let sstable_info = if let Some(task) = syncing_data.uploading_tasks.back_mut() {
                let result = ready!(task.poll_result(cx));
                let _task = syncing_data.uploading_tasks.pop_back().expect("non-empty");
                let sstable_info = match result {
                    Ok(sstable_info) => sstable_info,
                    Err(e) => {
                        let SyncingData {
                            sync_epoch,
                            uploading_tasks,
                            sync_result_sender,
                            ..
                        } = self.syncing_data.pop_back().expect("non-empty");
                        for task in uploading_tasks {
                            task.join_handle.abort();
                        }
                        send_sync_result(
                            sync_result_sender,
                            Err(HummockError::other(format!(
                                "failed sync task: {:?}",
                                e.as_report()
                            ))),
                        );

                        return Poll::Ready(Some(Err(ErrState {
                            failed_epoch: sync_epoch,
                            reason: format!("{:?}", e.as_report()),
                        })));
                    }
                };
                syncing_data.uploaded.push_front(sstable_info.clone());
                self.unsync_data.ack_flushed(&sstable_info);
                Some(sstable_info)
            } else {
                None
            };

            if syncing_data.uploading_tasks.is_empty() {
                let syncing_data = self.syncing_data.pop_back().expect("non-empty");
                let SyncingData {
                    sync_epoch,
                    uploading_tasks,
                    uploaded,
                    table_watermarks,
                    sync_result_sender,
                } = syncing_data;
                assert!(uploading_tasks.is_empty());
                context
                    .stats
                    .uploader_syncing_epoch_count
                    .set(self.syncing_data.len() as _);
                set_max_synced_epoch(sync_epoch);
                send_sync_result(
                    sync_result_sender,
                    Ok(SyncedData {
                        uploaded_ssts: uploaded,
                        table_watermarks,
                    }),
                )
            }

            if let Some(sstable_info) = sstable_info {
                return Poll::Ready(Some(Ok(sstable_info)));
            }
        }
        Poll::Ready(None)
    }

    /// Poll the success of the oldest spilled task of unsync spill data. Return `Poll::Ready(None)` if
    /// there is no spilling task.
    fn poll_spill_task(&mut self, cx: &mut Context<'_>) -> Poll<Option<Arc<StagingSstableInfo>>> {
        // iterator from older epoch to new epoch so that the spill task are finished in epoch order
        for epoch_data in self.unsync_data.epoch_data.values_mut() {
            // if None, there is no spilling task. Search for the unsync data of the next epoch in
            // the next iteration.
            if let Some(sstable_info) = ready!(epoch_data.spilled_data.poll_success_spill(cx)) {
                self.unsync_data.ack_flushed(&sstable_info);
                return Poll::Ready(Some(sstable_info));
            }
        }
        Poll::Ready(None)
    }
}

impl HummockUploader {
    pub(super) fn next_uploaded_sst(
        &mut self,
    ) -> impl Future<Output = Arc<StagingSstableInfo>> + '_ {
        poll_fn(|cx| {
            let UploaderState::Working(data) = &mut self.state else {
                return Poll::Pending;
            };

            if let Some(result) =
                ready!(
                    data.poll_syncing_task(cx, &self.context, |new_synced_epoch| {
                        Self::set_max_synced_epoch(
                            &mut self.max_synced_epoch,
                            self.max_syncing_epoch,
                            new_synced_epoch,
                        )
                    })
                )
            {
                match result {
                    Ok(data) => {
                        return Poll::Ready(data);
                    }
                    Err(e) => {
                        let failed_epoch = e.failed_epoch;
                        let data = must_match!(replace(
                            &mut self.state,
                            UploaderState::Err(e),
                        ), UploaderState::Working(data) => data);

                        data.abort(|| {
                            HummockError::other(format!(
                                "previous epoch {} failed to sync",
                                failed_epoch
                            ))
                        });
                        return Poll::Pending;
                    }
                }
            }

            if let Some(sstable_info) = ready!(data.poll_spill_task(cx)) {
                return Poll::Ready(sstable_info);
            }

            Poll::Pending
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::future::{poll_fn, Future};
    use std::ops::Deref;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::future::BoxFuture;
    use futures::FutureExt;
    use itertools::Itertools;
    use prometheus::core::GenericGauge;
    use risingwave_common::catalog::TableId;
    use risingwave_common::must_match;
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
    use crate::hummock::event_handler::uploader::uploader_imm::UploaderImm;
    use crate::hummock::event_handler::uploader::{
        get_payload_imm_ids, HummockUploader, SyncedData, UploadTaskInfo, UploadTaskOutput,
        UploadTaskPayload, UploaderContext, UploaderData, UploaderState, UploadingTask,
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
    use crate::store::SealCurrentEpochOptions;

    const INITIAL_EPOCH: HummockEpoch = test_epoch(5);
    pub(crate) const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

    pub trait UploadOutputFuture =
        Future<Output = HummockResult<UploadTaskOutput>> + Send + 'static;
    pub trait UploadFn<Fut: UploadOutputFuture> =
        Fn(UploadTaskPayload, UploadTaskInfo) -> Fut + Send + Sync + 'static;

    impl HummockUploader {
        fn data(&self) -> &UploaderData {
            must_match!(&self.state, UploaderState::Working(data) => data)
        }
    }

    fn test_hummock_version(epoch: HummockEpoch) -> HummockVersion {
        let mut version = HummockVersion::default();
        version.id = epoch;
        version.max_committed_epoch = epoch;
        version
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
            tracker,
        )
    }

    pub(crate) async fn gen_imm(epoch: HummockEpoch) -> ImmutableMemtable {
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
        _: UploadTaskPayload,
        _: UploadTaskInfo,
    ) -> HummockResult<UploadTaskOutput> {
        Ok(dummy_success_upload_output())
    }

    #[allow(clippy::unused_async)]
    async fn dummy_fail_upload_future(
        _: UploadTaskPayload,
        _: UploadTaskInfo,
    ) -> HummockResult<UploadTaskOutput> {
        Err(HummockError::other("failed"))
    }

    impl UploadingTask {
        fn from_vec(imms: Vec<ImmutableMemtable>, context: &UploaderContext) -> Self {
            let input = HashMap::from_iter([(
                TEST_LOCAL_INSTANCE_ID,
                imms.into_iter().map(UploaderImm::for_test).collect_vec(),
            )]);
            Self::new(input, context)
        }
    }

    fn get_imm_ids<'a>(
        imms: impl IntoIterator<Item = &'a ImmutableMemtable>,
    ) -> HashMap<LocalInstanceId, Vec<SharedBufferBatchId>> {
        HashMap::from_iter([(
            TEST_LOCAL_INSTANCE_ID,
            imms.into_iter().map(|imm| imm.batch_id()).collect_vec(),
        )])
    }

    impl HummockUploader {
        fn local_seal_epoch_for_test(&mut self, instance_id: LocalInstanceId, epoch: HummockEpoch) {
            self.local_seal_epoch(
                instance_id,
                epoch.next_epoch(),
                SealCurrentEpochOptions::for_test(),
            );
        }
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
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch1);
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, imm.clone());
        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch1);

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_sync_epoch(epoch1, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch1 as HummockEpoch, uploader.max_syncing_epoch);
        assert_eq!(1, uploader.data().syncing_data.len());
        let syncing_data = uploader.data().syncing_data.front().unwrap();
        assert_eq!(epoch1 as HummockEpoch, syncing_data.sync_epoch);
        assert!(syncing_data.uploaded.is_empty());
        assert!(!syncing_data.uploading_tasks.is_empty());

        let staging_sst = uploader.next_uploaded_sst().await;
        assert_eq!(&vec![epoch1], staging_sst.epochs());
        assert_eq!(
            &HashMap::from_iter([(TEST_LOCAL_INSTANCE_ID, vec![imm.batch_id()])]),
            staging_sst.imm_ids()
        );
        assert_eq!(
            &dummy_success_upload_output().new_value_ssts,
            staging_sst.sstable_infos()
        );

        match sync_rx.await {
            Ok(Ok(data)) => {
                let SyncedData {
                    uploaded_ssts,
                    table_watermarks,
                } = data;
                assert_eq!(1, uploaded_ssts.len());
                let staging_sst = &uploaded_ssts[0];
                assert_eq!(&vec![epoch1], staging_sst.epochs());
                assert_eq!(
                    &HashMap::from_iter([(TEST_LOCAL_INSTANCE_ID, vec![imm.batch_id()])]),
                    staging_sst.imm_ids()
                );
                assert_eq!(
                    &dummy_success_upload_output().new_value_ssts,
                    staging_sst.sstable_infos()
                );
                assert!(table_watermarks.is_empty());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.max_synced_epoch());

        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1));
        uploader.update_pinned_version(new_pinned_version);
        assert_eq!(epoch1, uploader.max_committed_epoch());
    }

    #[tokio::test]
    async fn test_empty_uploader_sync() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH.next_epoch();

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_sync_epoch(epoch1, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch1, uploader.max_syncing_epoch);

        assert_uploader_pending(&mut uploader).await;

        match sync_rx.await {
            Ok(Ok(data)) => {
                assert!(data.uploaded_ssts.is_empty());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.max_synced_epoch());
        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1));
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.data().syncing_data.is_empty());
        assert_eq!(epoch1, uploader.max_committed_epoch());
    }

    #[tokio::test]
    async fn test_uploader_empty_epoch() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let imm = gen_imm(epoch2).await;
        // epoch1 is empty while epoch2 is not. Going to seal empty epoch1.
        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch1);
        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch1);
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, imm);

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_sync_epoch(epoch1, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch1, uploader.max_syncing_epoch);

        assert_uploader_pending(&mut uploader).await;

        match sync_rx.await {
            Ok(Ok(data)) => {
                assert!(data.uploaded_ssts.is_empty());
            }
            _ => unreachable!(),
        };
        assert_eq!(epoch1, uploader.max_synced_epoch());
        let new_pinned_version = uploader
            .context
            .pinned_version
            .new_pin_version(test_hummock_version(epoch1));
        uploader.update_pinned_version(new_pinned_version);
        assert!(uploader.data().syncing_data.is_empty());
        assert_eq!(epoch1, uploader.max_committed_epoch());
    }

    #[tokio::test]
    async fn test_uploader_poll_empty() {
        let mut uploader = test_uploader(dummy_success_upload_future);
        let data = must_match!(&mut uploader.state, UploaderState::Working(data) => data);
        assert!(
            poll_fn(|cx| data.poll_syncing_task(cx, &uploader.context, |_| unreachable!()))
                .await
                .is_none()
        );
        assert!(poll_fn(|cx| data.poll_spill_task(cx)).await.is_none());
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

        uploader.init_instance(TEST_LOCAL_INSTANCE_ID, TEST_TABLE_ID, epoch6);
        let imm = gen_imm(epoch6).await;
        uploader.add_imm(TEST_LOCAL_INSTANCE_ID, imm.clone());
        uploader.update_pinned_version(version2);
        assert_eq!(epoch2, uploader.max_synced_epoch);
        assert_eq!(epoch2, uploader.max_syncing_epoch);

        uploader.local_seal_epoch_for_test(TEST_LOCAL_INSTANCE_ID, epoch6);
        uploader.update_pinned_version(version3);
        assert_eq!(epoch3, uploader.max_synced_epoch);
        assert_eq!(epoch3, uploader.max_syncing_epoch);

        let (sync_tx, sync_rx) = oneshot::channel();
        uploader.start_sync_epoch(epoch6, sync_tx, HashSet::from_iter([TEST_TABLE_ID]));
        assert_eq!(epoch6, uploader.max_syncing_epoch);
        uploader.update_pinned_version(version4);
        assert_eq!(epoch4, uploader.max_synced_epoch);
        assert_eq!(epoch6, uploader.max_syncing_epoch);

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_imm_ids([&imm]), sst.imm_ids());

        match sync_rx.await {
            Ok(Ok(data)) => {
                assert!(data.table_watermarks.is_empty());
                assert_eq!(1, data.uploaded_ssts.len());
                assert_eq!(&get_imm_ids([&imm]), data.uploaded_ssts[0].imm_ids());
            }
            _ => unreachable!(),
        }

        uploader.update_pinned_version(version5);
        assert_eq!(epoch6, uploader.max_synced_epoch);
        assert_eq!(epoch6, uploader.max_syncing_epoch);
    }

    fn prepare_uploader_order_test(
        config: &StorageOpts,
        skip_schedule: bool,
    ) -> (
        BufferTracker,
        HummockUploader,
        impl Fn(HashMap<LocalInstanceId, Vec<ImmId>>) -> (BoxFuture<'static, ()>, oneshot::Sender<()>),
    ) {
        let gauge = GenericGauge::new("test", "test").unwrap();
        let buffer_tracker = BufferTracker::from_storage_opts(config, gauge);
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
                    let task_item = task_notifier_holder.lock().pop_back();
                    let start_epoch = *task_info.epochs.last().unwrap();
                    let end_epoch = *task_info.epochs.first().unwrap();
                    assert!(end_epoch >= start_epoch);
                    spawn(async move {
                        let ssts = gen_sstable_info(start_epoch, end_epoch);
                        if !skip_schedule {
                            let (start_tx, finish_rx) = task_item.unwrap();
                            start_tx.send(task_info).unwrap();
                            finish_rx.await.unwrap();
                        }
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
            poll_fn(|cx| Poll::Ready(uploader.next_uploaded_sst().poll_unpin(cx)))
                .await
                .is_pending()
        )
    }

    #[tokio::test]
    async fn test_uploader_finish_in_order() {
        let config = StorageOpts {
            shared_buffer_capacity_mb: 1024 * 1024,
            shared_buffer_flush_ratio: 0.0,
            ..Default::default()
        };
        let (buffer_tracker, mut uploader, new_task_notifier) =
            prepare_uploader_order_test(&config, false);

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();
        let memory_limiter = Some(memory_limiter.deref());

        let instance_id1 = 1;
        let instance_id2 = 2;

        uploader.init_instance(instance_id1, TEST_TABLE_ID, epoch1);
        uploader.init_instance(instance_id2, TEST_TABLE_ID, epoch2);

        // imm2 contains data in newer epoch, but added first
        let imm2 = gen_imm_with_limiter(epoch2, memory_limiter).await;
        uploader.add_imm(instance_id2, imm2.clone());
        let imm1_1 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_1.clone());
        let imm1_2 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_2.clone());

        // imm1 will be spilled first
        let epoch1_spill_payload12 =
            HashMap::from_iter([(instance_id1, vec![imm1_2.clone(), imm1_1.clone()])]);
        let epoch2_spill_payload = HashMap::from_iter([(instance_id2, vec![imm2.clone()])]);
        let (await_start1, finish_tx1) =
            new_task_notifier(get_payload_imm_ids(&epoch1_spill_payload12));
        let (await_start2, finish_tx2) =
            new_task_notifier(get_payload_imm_ids(&epoch2_spill_payload));
        uploader.may_flush();
        await_start1.await;
        await_start2.await;

        assert_uploader_pending(&mut uploader).await;

        finish_tx2.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;

        finish_tx1.send(()).unwrap();
        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch1_spill_payload12), sst.imm_ids());
        assert_eq!(&vec![epoch1], sst.epochs());

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch2_spill_payload), sst.imm_ids());
        assert_eq!(&vec![epoch2], sst.epochs());

        let imm1_3 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_3.clone());
        let epoch1_spill_payload3 = HashMap::from_iter([(instance_id1, vec![imm1_3.clone()])]);
        let (await_start1_3, finish_tx1_3) =
            new_task_notifier(get_payload_imm_ids(&epoch1_spill_payload3));
        uploader.may_flush();
        await_start1_3.await;
        let imm1_4 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_4.clone());
        let epoch1_sync_payload = HashMap::from_iter([(instance_id1, vec![imm1_4.clone()])]);
        let (await_start1_4, finish_tx1_4) =
            new_task_notifier(get_payload_imm_ids(&epoch1_sync_payload));
        uploader.local_seal_epoch_for_test(instance_id1, epoch1);
        let (sync_tx1, mut sync_rx1) = oneshot::channel();
        uploader.start_sync_epoch(epoch1, sync_tx1, HashSet::from_iter([TEST_TABLE_ID]));
        await_start1_4.await;
        let epoch3 = epoch2.next_epoch();

        uploader.local_seal_epoch_for_test(instance_id1, epoch2);
        uploader.local_seal_epoch_for_test(instance_id2, epoch2);

        // current uploader state:
        // unsealed: empty
        // sealed: epoch2: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let imm3_1 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        let epoch3_spill_payload1 = HashMap::from_iter([(instance_id1, vec![imm3_1.clone()])]);
        uploader.add_imm(instance_id1, imm3_1.clone());
        let (await_start3_1, finish_tx3_1) =
            new_task_notifier(get_payload_imm_ids(&epoch3_spill_payload1));
        uploader.may_flush();
        await_start3_1.await;
        let imm3_2 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        let epoch3_spill_payload2 = HashMap::from_iter([(instance_id2, vec![imm3_2.clone()])]);
        uploader.add_imm(instance_id2, imm3_2.clone());
        let (await_start3_2, finish_tx3_2) =
            new_task_notifier(get_payload_imm_ids(&epoch3_spill_payload2));
        uploader.may_flush();
        await_start3_2.await;
        let imm3_3 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(instance_id1, imm3_3.clone());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let epoch4 = epoch3.next_epoch();
        uploader.local_seal_epoch_for_test(instance_id1, epoch3);
        let imm4 = gen_imm_with_limiter(epoch4, memory_limiter).await;
        uploader.add_imm(instance_id1, imm4.clone());
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

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch1_spill_payload3), sst.imm_ids());

        assert!(poll_fn(|cx| Poll::Ready(sync_rx1.poll_unpin(cx).is_pending())).await);

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch1_sync_payload), sst.imm_ids());

        if let Ok(Ok(data)) = sync_rx1.await {
            assert_eq!(3, data.uploaded_ssts.len());
            assert_eq!(
                &get_payload_imm_ids(&epoch1_sync_payload),
                data.uploaded_ssts[0].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch1_spill_payload3),
                data.uploaded_ssts[1].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch1_spill_payload12),
                data.uploaded_ssts[2].imm_ids()
            );
        } else {
            unreachable!()
        }

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])

        let (sync_tx2, sync_rx2) = oneshot::channel();
        uploader.start_sync_epoch(epoch2, sync_tx2, HashSet::from_iter([TEST_TABLE_ID]));
        uploader.local_seal_epoch_for_test(instance_id2, epoch3);
        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch3_spill_payload1), sst.imm_ids());

        if let Ok(Ok(data)) = sync_rx2.await {
            assert_eq!(data.uploaded_ssts.len(), 1);
            assert_eq!(
                &get_payload_imm_ids(&epoch2_spill_payload),
                data.uploaded_ssts[0].imm_ids()
            );
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch2, uploader.max_synced_epoch);

        // current uploader state:
        // unsealed: epoch4: imm: imm4
        // sealed: imm: imm3_3, uploading: [imm3_2], uploaded: sst([imm3_1])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        uploader.local_seal_epoch_for_test(instance_id1, epoch4);
        uploader.local_seal_epoch_for_test(instance_id2, epoch4);
        let epoch4_sync_payload = HashMap::from_iter([(instance_id1, vec![imm4, imm3_3])]);
        let (await_start4_with_3_3, finish_tx4_with_3_3) =
            new_task_notifier(get_payload_imm_ids(&epoch4_sync_payload));
        let (sync_tx4, mut sync_rx4) = oneshot::channel();
        uploader.start_sync_epoch(epoch4, sync_tx4, HashSet::from_iter([TEST_TABLE_ID]));
        await_start4_with_3_3.await;

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: epoch4: uploading: [imm4, imm3_3], [imm3_2], uploaded: sst([imm3_1])
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        assert_uploader_pending(&mut uploader).await;

        finish_tx3_2.send(()).unwrap();
        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch3_spill_payload2), sst.imm_ids());

        finish_tx4_with_3_3.send(()).unwrap();
        assert!(poll_fn(|cx| Poll::Ready(sync_rx4.poll_unpin(cx).is_pending())).await);

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch4_sync_payload), sst.imm_ids());

        if let Ok(Ok(data)) = sync_rx4.await {
            assert_eq!(3, data.uploaded_ssts.len());
            assert_eq!(
                &get_payload_imm_ids(&epoch4_sync_payload),
                data.uploaded_ssts[0].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch3_spill_payload2),
                data.uploaded_ssts[1].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch3_spill_payload1),
                data.uploaded_ssts[2].imm_ids(),
            )
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch4, uploader.max_synced_epoch);

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])
        //         epoch4: sst([imm4, imm3_3]), sst([imm3_2]), sst([imm3_1])
    }

    #[tokio::test]
    async fn test_uploader_frequently_flush() {
        let config = StorageOpts {
            shared_buffer_capacity_mb: 10,
            shared_buffer_flush_ratio: 0.8,
            // This test will fail when we set it to 0
            shared_buffer_min_batch_flush_size_mb: 1,
            ..Default::default()
        };
        let (buffer_tracker, mut uploader, _new_task_notifier) =
            prepare_uploader_order_test(&config, true);

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let instance_id1 = 1;
        let instance_id2 = 2;
        let flush_threshold = buffer_tracker.flush_threshold();
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();

        uploader.init_instance(instance_id1, TEST_TABLE_ID, epoch1);
        uploader.init_instance(instance_id2, TEST_TABLE_ID, epoch2);

        // imm2 contains data in newer epoch, but added first
        let mut total_memory = 0;
        while total_memory < flush_threshold {
            let imm = gen_imm_with_limiter(epoch2, Some(memory_limiter.as_ref())).await;
            total_memory += imm.size();
            if total_memory > flush_threshold {
                break;
            }
            uploader.add_imm(instance_id2, imm);
        }
        let imm = gen_imm_with_limiter(epoch1, Some(memory_limiter.as_ref())).await;
        uploader.add_imm(instance_id1, imm);
        assert!(uploader.may_flush());

        for _ in 0..10 {
            let imm = gen_imm_with_limiter(epoch1, Some(memory_limiter.as_ref())).await;
            uploader.add_imm(instance_id1, imm);
            assert!(!uploader.may_flush());
        }
    }
}
