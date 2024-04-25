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

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use foyer::memory::CacheContext;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::EvictionConfig;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::{FullKey, PointRange, TableKey, UserKey};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch, HummockSstableObjectId};
use risingwave_pb::hummock::{KeyRange, SstableInfo};

use super::iterator::test_utils::iterator_test_table_key_of;
use super::{
    HummockResult, InMemWriter, MonotonicDeleteEvent, SstableMeta, SstableWriterOptions,
    DEFAULT_RESTART_INTERVAL,
};
use crate::filter_key_extractor::{FilterKeyExtractorImpl, FullKeyFilterKeyExtractor};
use crate::hummock::iterator::ForwardMergeRangeIterator;
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferItem, SharedBufferValue,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, CompactionDeleteRangeIterator, DeleteRangeTombstone,
    FilterBuilder, LruCache, Sstable, SstableBuilder, SstableBuilderOptions, SstableStoreRef,
    SstableWriter, TableHolder, Xor16FilterBuilder,
};
use crate::monitor::StoreLocalStatistic;
use crate::opts::StorageOpts;
use crate::storage_value::StorageValue;
use crate::StateStoreIter;

pub fn default_opts_for_test() -> StorageOpts {
    StorageOpts {
        sstable_size_mb: 4,
        block_size_kb: 64,
        bloom_false_positive: 0.1,
        share_buffers_sync_parallelism: 2,
        share_buffer_compaction_worker_threads_number: 1,
        shared_buffer_capacity_mb: 64,
        data_directory: "hummock_001".to_string(),
        write_conflict_detection_enabled: true,
        block_cache_capacity_mb: 64,
        meta_cache_capacity_mb: 64,
        block_cache_eviction_config: EvictionConfig::for_test(),
        disable_remote_compactor: false,
        share_buffer_upload_concurrency: 1,
        compactor_memory_limit_mb: 64,
        sstable_id_remote_fetch_number: 1,
        ..Default::default()
    }
}

pub fn gen_dummy_batch(n: u64) -> Vec<SharedBufferItem> {
    vec![(
        TableKey(Bytes::from(iterator_test_table_key_of(n as usize))),
        SharedBufferValue::Insert(Bytes::copy_from_slice(&b"value1"[..])),
    )]
}

pub fn gen_dummy_batch_several_keys(n: usize) -> Vec<(TableKey<Bytes>, StorageValue)> {
    let mut kvs = vec![];
    let v = Bytes::from(b"value1".to_vec().repeat(100));
    for idx in 0..n {
        kvs.push((
            TableKey(Bytes::from(iterator_test_table_key_of(idx))),
            StorageValue::new_put(v.clone()),
        ));
    }
    kvs
}

pub fn gen_dummy_sst_info(
    id: HummockSstableObjectId,
    batches: Vec<SharedBufferBatch>,
    table_id: TableId,
    epoch: HummockEpoch,
) -> SstableInfo {
    let mut min_table_key: Vec<u8> = batches[0].start_table_key().to_vec();
    let mut max_table_key: Vec<u8> = batches[0].end_table_key().to_vec();
    let mut file_size = 0;
    for batch in batches.iter().skip(1) {
        if min_table_key.as_slice() > *batch.start_table_key() {
            min_table_key = batch.start_table_key().to_vec();
        }
        if max_table_key.as_slice() < *batch.end_table_key() {
            max_table_key = batch.end_table_key().to_vec();
        }
        file_size += batch.size() as u64;
    }
    SstableInfo {
        object_id: id,
        sst_id: id,
        key_range: Some(KeyRange {
            left: FullKey::for_test(table_id, min_table_key, epoch).encode(),
            right: FullKey::for_test(table_id, max_table_key, epoch).encode(),
            right_exclusive: false,
        }),
        file_size,
        table_ids: vec![table_id.table_id],
        uncompressed_file_size: file_size,
        min_epoch: epoch,
        max_epoch: epoch,
        ..Default::default()
    }
}

/// Number of keys in table generated in `generate_table`.
pub const TEST_KEYS_COUNT: usize = 10000;

pub fn default_builder_opt_for_test() -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: 256 * (1 << 20), // 256MB
        block_capacity: 4096,      // 4KB
        restart_interval: DEFAULT_RESTART_INTERVAL,
        bloom_false_positive: 0.1,
        ..Default::default()
    }
}

pub fn default_writer_opt_for_test() -> SstableWriterOptions {
    SstableWriterOptions {
        capacity_hint: None,
        tracker: None,
        policy: CachePolicy::Disable,
    }
}

pub fn mock_sst_writer(opt: &SstableBuilderOptions) -> InMemWriter {
    InMemWriter::from(opt)
}

/// Generates sstable data and metadata from given `kv_iter`
pub async fn gen_test_sstable_data(
    opts: SstableBuilderOptions,
    kv_iter: impl Iterator<Item = (FullKey<Vec<u8>>, HummockValue<Vec<u8>>)>,
) -> (Bytes, SstableMeta) {
    let mut b = SstableBuilder::for_test(0, mock_sst_writer(&opts), opts);
    for (key, value) in kv_iter {
        b.add_for_test(key.to_ref(), value.as_slice())
            .await
            .unwrap();
    }
    let output = b.finish().await.unwrap();
    output.writer_output
}

/// Write the data and meta to `sstable_store`.
pub async fn put_sst(
    sst_object_id: HummockSstableObjectId,
    data: Bytes,
    mut meta: SstableMeta,
    sstable_store: SstableStoreRef,
    mut options: SstableWriterOptions,
) -> HummockResult<SstableInfo> {
    options.policy = CachePolicy::NotFill;
    let mut writer = sstable_store
        .clone()
        .create_sst_writer(sst_object_id, options);
    for block_meta in &meta.block_metas {
        let offset = block_meta.offset as usize;
        let end_offset = offset + block_meta.len as usize;
        writer
            .write_block(&data[offset..end_offset], block_meta)
            .await?;
    }
    meta.meta_offset = writer.data_len() as u64;
    let sst = SstableInfo {
        object_id: sst_object_id,
        sst_id: sst_object_id,
        key_range: Some(KeyRange {
            left: meta.smallest_key.clone(),
            right: meta.largest_key.clone(),
            right_exclusive: false,
        }),
        file_size: meta.estimated_size as u64,
        meta_offset: meta.meta_offset,
        uncompressed_file_size: meta.estimated_size as u64,
        ..Default::default()
    };
    let writer_output = writer.finish(meta).await?;
    writer_output.await.unwrap()?;
    Ok(sst)
}

/// Generates a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_impl<B: AsRef<[u8]> + Clone + Default + Eq, F: FilterBuilder>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl IntoIterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
) -> SstableInfo {
    let writer_opts = SstableWriterOptions {
        capacity_hint: None,
        tracker: None,
        policy,
    };
    let writer = sstable_store
        .clone()
        .create_sst_writer(object_id, writer_opts);
    let mut b = SstableBuilder::<_, F>::new(
        object_id,
        writer,
        F::create(opts.bloom_false_positive, opts.capacity / 16),
        opts,
        Arc::new(FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)),
        None,
    );

    let mut last_key = FullKey::<B>::default();
    for (key, value) in kv_iter {
        let is_new_user_key =
            last_key.is_empty() || key.user_key.as_ref() != last_key.user_key.as_ref();
        if is_new_user_key {
            last_key = key.clone();
        }

        b.add(key.to_ref(), value.as_slice()).await.unwrap();
    }
    let output = b.finish().await.unwrap();
    output.writer_output.await.unwrap().unwrap();
    output.sst_info.sst_info
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl Iterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
) -> TableHolder {
    let sst_info = gen_test_sstable_impl::<_, Xor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store.clone(),
        CachePolicy::NotFill,
    )
    .await;
    sstable_store
        .sstable(&sst_info, &mut StoreLocalStatistic::default())
        .await
        .unwrap()
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_info<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl IntoIterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
) -> SstableInfo {
    gen_test_sstable_impl::<_, BlockedXor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store,
        CachePolicy::NotFill,
    )
    .await
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_with_range_tombstone(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl Iterator<Item = (FullKey<Vec<u8>>, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
) -> SstableInfo {
    gen_test_sstable_impl::<_, Xor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store.clone(),
        CachePolicy::Fill(CacheContext::Default),
    )
    .await
}

/// Generates a user key with table id 0 and the given `table_key`
pub fn test_user_key(table_key: impl AsRef<[u8]>) -> UserKey<Vec<u8>> {
    UserKey::for_test(TableId::default(), table_key.as_ref().to_vec())
}

/// Generates a user key with table id 0 and table key format of `key_test_{idx * 2}`
pub fn test_user_key_of(idx: usize) -> UserKey<Vec<u8>> {
    let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
    table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
    UserKey::for_test(TableId::default(), table_key)
}

/// Generates a full key with table id 0 and epoch 123. User key is created with `test_user_key_of`.
pub fn test_key_of(idx: usize) -> FullKey<Vec<u8>> {
    FullKey {
        user_key: test_user_key_of(idx),
        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
    }
}

/// The value of an index in the test table
pub fn test_value_of(idx: usize) -> Vec<u8> {
    "23332333"
        .as_bytes()
        .iter()
        .cycle()
        .cloned()
        .take(idx % 100 + 1) // so that the table is not too big
        .collect_vec()
}

/// Generates a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_default_test_sstable(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    sstable_store: SstableStoreRef,
) -> TableHolder {
    gen_test_sstable(
        opts,
        object_id,
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
        sstable_store,
    )
    .await
}

pub async fn count_stream(mut i: impl StateStoreIter) -> usize {
    let mut c: usize = 0;
    while i.try_next().await.unwrap().is_some() {
        c += 1
    }
    c
}

pub fn create_small_table_cache() -> Arc<LruCache<HummockSstableObjectId, Box<Sstable>>> {
    Arc::new(LruCache::new(1, 4, 0))
}

pub mod delete_range {
    use super::*;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferDeleteRangeIterator;

    #[derive(Default)]
    pub struct CompactionDeleteRangesBuilder {
        iter: ForwardMergeRangeIterator,
    }

    impl CompactionDeleteRangesBuilder {
        pub fn add_delete_events_for_test(
            &mut self,
            epoch: HummockEpoch,
            table_id: TableId,
            delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
        ) {
            self.iter
                .add_batch_iter(SharedBufferDeleteRangeIterator::new(
                    test_epoch(epoch),
                    table_id,
                    delete_ranges,
                ));
        }

        pub fn build_for_compaction(self) -> CompactionDeleteRangeIterator {
            CompactionDeleteRangeIterator::new(self.iter)
        }
    }

    /// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
    /// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
    /// can be transformed into events below:
    /// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
    /// -epoch3> }`
    fn build_events(
        delete_tombstones: &Vec<DeleteRangeTombstone>,
    ) -> Vec<CompactionDeleteRangeEvent> {
        let tombstone_len = delete_tombstones.len();
        let mut events = Vec::with_capacity(tombstone_len * 2);
        for DeleteRangeTombstone {
            start_user_key,
            end_user_key,
            sequence,
        } in delete_tombstones
        {
            events.push((start_user_key, 1, *sequence));
            events.push((end_user_key, 0, *sequence));
        }
        events.sort();

        let mut result = Vec::with_capacity(events.len());
        for (user_key, group) in &events.into_iter().group_by(|(user_key, _, _)| *user_key) {
            let (mut exit, mut enter) = (vec![], vec![]);
            for (_, op, sequence) in group {
                match op {
                    0 => exit.push(TombstoneEnterExitEvent {
                        tombstone_epoch: sequence,
                    }),
                    1 => {
                        enter.push(TombstoneEnterExitEvent {
                            tombstone_epoch: sequence,
                        });
                    }
                    _ => unreachable!(),
                }
            }
            result.push((user_key.clone(), exit, enter));
        }

        result
    }

    pub fn create_monotonic_events(
        mut delete_range_tombstones: Vec<DeleteRangeTombstone>,
    ) -> Vec<MonotonicDeleteEvent> {
        delete_range_tombstones.sort();
        let events = build_events(&delete_range_tombstones);
        create_monotonic_events_from_compaction_delete_events(events)
    }

    fn create_monotonic_events_from_compaction_delete_events(
        compaction_delete_range_events: Vec<CompactionDeleteRangeEvent>,
    ) -> Vec<MonotonicDeleteEvent> {
        let mut epochs = BTreeSet::new();
        let mut monotonic_tombstone_events =
            Vec::with_capacity(compaction_delete_range_events.len());
        for event in compaction_delete_range_events {
            apply_event(&mut epochs, &event);
            monotonic_tombstone_events.push(MonotonicDeleteEvent {
                event_key: event.0,
                new_epoch: epochs.first().map_or(HummockEpoch::MAX, |epoch| *epoch),
            });
        }
        monotonic_tombstone_events.dedup_by(|a, b| {
            a.event_key.left_user_key.table_id == b.event_key.left_user_key.table_id
                && a.new_epoch == b.new_epoch
        });
        monotonic_tombstone_events
    }

    #[derive(Clone)]
    #[cfg(any(test, feature = "test"))]
    pub struct TombstoneEnterExitEvent {
        pub(crate) tombstone_epoch: HummockEpoch,
    }

    #[cfg(any(test, feature = "test"))]
    pub type CompactionDeleteRangeEvent = (
        // event key
        PointRange<Vec<u8>>,
        // Old tombstones which exits at the event key
        Vec<TombstoneEnterExitEvent>,
        // New tombstones which enters at the event key
        Vec<TombstoneEnterExitEvent>,
    );
    /// We introduce `event` to avoid storing excessive range tombstones after compaction if there are
    /// overlaps among range tombstones among different SSTs/batchs in compaction.
    /// The core idea contains two parts:
    /// 1) we only need to keep the smallest epoch of the overlapping
    /// range tomstone intervals since the key covered by the range tombstone in lower level must have
    /// smaller epoches;
    /// 2) due to 1), we lose the information to delete a key by tombstone in a single
    /// SST so we add a tombstone key in the data block.
    /// We leverage `events` to calculate the epoch information mentioned above.
    /// e.g. Delete range [1, 5) at epoch1, delete range [3, 7) at epoch2 and delete range [10, 12) at
    /// epoch3 will first be transformed into `events` below:
    /// `<1, +epoch1> <5, -epoch1> <3, +epoch2> <7, -epoch2> <10, +epoch3> <12, -epoch3>`
    /// Then `events` are sorted by user key:
    /// `<1, +epoch1> <3, +epoch2> <5, -epoch1> <7, -epoch2> <10, +epoch3> <12, -epoch3>`
    /// We rely on the fact that keys met in compaction are in order.
    /// When user key 0 comes, no events have happened yet so no range delete epoch. (will be
    /// represented as range delete epoch MAX EPOCH)
    /// When user key 1 comes, event `<1, +epoch1>` happens so there is currently one range delete
    /// epoch: epoch1.
    /// When user key 2 comes, no more events happen so the set remains `{epoch1}`.
    /// When user key 3 comes, event `<3, +epoch2>` happens so the range delete epoch set is now
    /// `{epoch1, epoch2}`.
    /// When user key 5 comes, event `<5, -epoch1>` happens so epoch1 exits the set,
    /// therefore the current range delete epoch set is `{epoch2}`.
    /// When user key 11 comes, events `<7, -epoch2>` and `<10, +epoch3>`
    /// both happen, one after another. The set changes to `{epoch3}` from `{epoch2}`.
    pub fn apply_event(epochs: &mut BTreeSet<HummockEpoch>, event: &CompactionDeleteRangeEvent) {
        let (_, exit, enter) = event;
        // Correct because ranges in an epoch won't intersect.
        for TombstoneEnterExitEvent { tombstone_epoch } in exit {
            epochs.remove(tombstone_epoch);
        }
        for TombstoneEnterExitEvent { tombstone_epoch } in enter {
            epochs.insert(*tombstone_epoch);
        }
    }
}
