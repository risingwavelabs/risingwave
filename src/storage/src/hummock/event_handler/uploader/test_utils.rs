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

#![cfg(test)]

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::{test_epoch, EpochExt};
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::{KeyRange, SstableInfo, StateTableInfoDelta};
use tokio::spawn;
use tokio::sync::mpsc::unbounded_channel;

use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::event_handler::uploader::uploader_imm::UploaderImm;
use crate::hummock::event_handler::uploader::{
    HummockUploader, TableUnsyncData, UploadTaskInfo, UploadTaskOutput, UploadTaskPayload,
    UploaderContext, UploaderData, UploaderState, UploadingTask, UploadingTaskId,
};
use crate::hummock::event_handler::{LocalInstanceId, TEST_LOCAL_INSTANCE_ID};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchId, SharedBufferValue,
};
use crate::hummock::{HummockError, HummockResult, MemoryLimiter};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;
use crate::store::SealCurrentEpochOptions;

pub(crate) const INITIAL_EPOCH: HummockEpoch = test_epoch(5);
pub(crate) const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

pub trait UploadOutputFuture = Future<Output = HummockResult<UploadTaskOutput>> + Send + 'static;
pub trait UploadFn<Fut: UploadOutputFuture> =
    Fn(UploadTaskPayload, UploadTaskInfo) -> Fut + Send + Sync + 'static;

impl HummockUploader {
    pub(super) fn data(&self) -> &UploaderData {
        must_match!(&self.state, UploaderState::Working(data) => data)
    }

    pub(super) fn table_data(&self) -> &TableUnsyncData {
        self.data()
            .unsync_data
            .table_data
            .get(&TEST_TABLE_ID)
            .expect("should exist")
    }

    pub(super) fn test_max_syncing_epoch(&self) -> HummockEpoch {
        self.table_data().max_sync_epoch().unwrap()
    }

    pub(super) fn test_max_synced_epoch(&self) -> HummockEpoch {
        self.table_data().max_synced_epoch.unwrap()
    }
}

pub(super) fn test_hummock_version(epoch: HummockEpoch) -> HummockVersion {
    let mut version = HummockVersion::default();
    version.id = epoch;
    version.max_committed_epoch = epoch;
    version.state_table_info.apply_delta(
        &HashMap::from_iter([(
            TEST_TABLE_ID,
            StateTableInfoDelta {
                committed_epoch: epoch,
                safe_epoch: epoch,
                compaction_group_id: StaticCompactionGroupId::StateDefault as _,
            },
        )]),
        &HashSet::new(),
    );
    version
}

pub(super) fn initial_pinned_version() -> PinnedVersion {
    PinnedVersion::new(test_hummock_version(INITIAL_EPOCH), unbounded_channel().0)
}

pub(super) fn dummy_table_key() -> Vec<u8> {
    vec![b't', b'e', b's', b't']
}

pub(super) async fn gen_imm_with_limiter(
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

pub(super) fn gen_sstable_info(
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

pub(super) fn test_uploader_context<F, Fut>(upload_fn: F) -> UploaderContext
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

pub(super) fn test_uploader<F, Fut>(upload_fn: F) -> HummockUploader
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

pub(super) fn dummy_success_upload_output() -> UploadTaskOutput {
    UploadTaskOutput {
        new_value_ssts: gen_sstable_info(INITIAL_EPOCH, INITIAL_EPOCH),
        old_value_ssts: vec![],
        wait_poll_timer: None,
    }
}

#[allow(clippy::unused_async)]
pub(super) async fn dummy_success_upload_future(
    _: UploadTaskPayload,
    _: UploadTaskInfo,
) -> HummockResult<UploadTaskOutput> {
    Ok(dummy_success_upload_output())
}

#[allow(clippy::unused_async)]
pub(super) async fn dummy_fail_upload_future(
    _: UploadTaskPayload,
    _: UploadTaskInfo,
) -> HummockResult<UploadTaskOutput> {
    Err(HummockError::other("failed"))
}

impl UploadingTask {
    pub(super) fn from_vec(imms: Vec<ImmutableMemtable>, context: &UploaderContext) -> Self {
        let input = HashMap::from_iter([(
            TEST_LOCAL_INSTANCE_ID,
            imms.into_iter().map(UploaderImm::for_test).collect_vec(),
        )]);
        static NEXT_TASK_ID: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
        Self::new(
            UploadingTaskId(NEXT_TASK_ID.fetch_add(1, Relaxed)),
            input,
            context,
        )
    }
}

pub(super) fn get_imm_ids<'a>(
    imms: impl IntoIterator<Item = &'a ImmutableMemtable>,
) -> HashMap<LocalInstanceId, Vec<SharedBufferBatchId>> {
    HashMap::from_iter([(
        TEST_LOCAL_INSTANCE_ID,
        imms.into_iter().map(|imm| imm.batch_id()).collect_vec(),
    )])
}

impl HummockUploader {
    pub(super) fn local_seal_epoch_for_test(
        &mut self,
        instance_id: LocalInstanceId,
        epoch: HummockEpoch,
    ) {
        self.local_seal_epoch(
            instance_id,
            epoch.next_epoch(),
            SealCurrentEpochOptions::for_test(),
        );
    }

    pub(super) fn start_epochs_for_test(&mut self, epochs: impl IntoIterator<Item = HummockEpoch>) {
        let mut last_epoch = None;
        for epoch in epochs {
            last_epoch = Some(epoch);
            self.start_epoch(epoch, HashSet::from_iter([TEST_TABLE_ID]));
        }
        self.start_epoch(
            last_epoch.unwrap().next_epoch(),
            HashSet::from_iter([TEST_TABLE_ID]),
        );
    }
}
