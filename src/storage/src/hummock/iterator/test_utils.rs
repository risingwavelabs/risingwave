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

use std::iter::Iterator;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::{EvictionConfig, MetricLevel, ObjectStoreConfig};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::{prefix_slice_with_vnode, FullKey, TableKey, UserKey};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch, HummockSstableObjectId};
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
};
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferValue;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStore;
pub use crate::hummock::test_utils::default_builder_opt_for_test;
use crate::hummock::test_utils::{
    gen_test_sstable, gen_test_sstable_info, gen_test_sstable_with_range_tombstone,
};
use crate::hummock::{
    FileCache, HummockValue, SstableBuilderOptions, SstableIterator, SstableIteratorType,
    SstableStoreConfig, SstableStoreRef, TableHolder,
};
use crate::monitor::{global_hummock_state_store_metrics, ObjectStoreMetrics};

/// `assert_eq` two `Vec<u8>` with human-readable format.
#[macro_export]
macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {{
        use bytes::Bytes;
        assert_eq!(
            Bytes::copy_from_slice(&$left),
            Bytes::copy_from_slice(&$right)
        )
    }};
}

pub const TEST_KEYS_COUNT: usize = 10;

pub fn mock_sstable_store() -> SstableStoreRef {
    mock_sstable_store_with_object_store(Arc::new(ObjectStoreImpl::InMem(
        InMemObjectStore::new().monitored(
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ),
    )))
}

pub fn mock_sstable_store_with_object_store(store: ObjectStoreRef) -> SstableStoreRef {
    let path = "test".to_string();
    Arc::new(SstableStore::new(SstableStoreConfig {
        store,
        path,
        block_cache_capacity: 64 << 20,
        block_cache_shard_num: 2,
        block_cache_eviction: EvictionConfig::for_test(),
        meta_cache_capacity: 64 << 20,
        meta_cache_shard_num: 2,
        meta_cache_eviction: EvictionConfig::for_test(),
        prefetch_buffer_capacity: 64 << 20,
        max_prefetch_block_number: 16,

        data_file_cache: FileCache::none(),
        meta_file_cache: FileCache::none(),
        recent_filter: None,
        state_store_metrics: Arc::new(global_hummock_state_store_metrics(MetricLevel::Disabled)),
    }))
}

// Generate test table key with vnode 0
pub fn iterator_test_table_key_of(idx: usize) -> Vec<u8> {
    prefix_slice_with_vnode(VirtualNode::ZERO, format!("key_test_{:05}", idx).as_bytes()).to_vec()
}

pub fn iterator_test_user_key_of(idx: usize) -> UserKey<Vec<u8>> {
    UserKey::for_test(TableId::default(), iterator_test_table_key_of(idx))
}

pub fn iterator_test_bytes_user_key_of(idx: usize) -> UserKey<Bytes> {
    UserKey::for_test(
        TableId::default(),
        Bytes::from(iterator_test_table_key_of(idx)),
    )
}

/// Generates keys like `{table_id=0}key_test_00002` with epoch 233.
pub fn iterator_test_key_of(idx: usize) -> FullKey<Vec<u8>> {
    FullKey {
        user_key: iterator_test_user_key_of(idx),
        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
    }
}

/// Generates keys like `{table_id=0}key_test_00002` with epoch 233.
pub fn iterator_test_bytes_key_of(idx: usize) -> FullKey<Bytes> {
    iterator_test_key_of(idx).into_bytes()
}

/// Generates keys like `{table_id=0}key_test_00002` with epoch `epoch` .
pub fn iterator_test_key_of_epoch(idx: usize, epoch: HummockEpoch) -> FullKey<Vec<u8>> {
    FullKey {
        user_key: iterator_test_user_key_of(idx),
        epoch_with_gap: EpochWithGap::new_from_epoch(epoch),
    }
}

/// Generates keys like `{table_id=0}key_test_00002` with epoch `epoch` .
pub fn iterator_test_bytes_key_of_epoch(idx: usize, epoch: HummockEpoch) -> FullKey<Bytes> {
    iterator_test_key_of_epoch(idx, test_epoch(epoch)).into_bytes()
}

/// The value of an index, like `value_test_00002` without value meta
pub fn iterator_test_value_of(idx: usize) -> Vec<u8> {
    format!("value_test_{:05}", idx).as_bytes().to_vec()
}

pub fn transform_shared_buffer(
    batches: Vec<(Vec<u8>, SharedBufferValue<Bytes>)>,
) -> Vec<(TableKey<Bytes>, SharedBufferValue<Bytes>)> {
    batches
        .into_iter()
        .map(|(k, v)| (TableKey(k.into()), v))
        .collect_vec()
}

/// Generates a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_iterator_test_sstable_info(
    object_id: HummockSstableObjectId,
    opts: SstableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
) -> SstableInfo {
    gen_test_sstable_info(
        opts,
        object_id,
        (0..total).map(|i| {
            (
                iterator_test_key_of(idx_mapping(i)),
                HummockValue::put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}

/// Generates a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_iterator_test_sstable_base(
    object_id: HummockSstableObjectId,
    opts: SstableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
) -> TableHolder {
    gen_test_sstable(
        opts,
        object_id,
        (0..total).map(|i| {
            (
                iterator_test_key_of(idx_mapping(i)),
                HummockValue::put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}

// key=[idx, epoch], value
pub async fn gen_iterator_test_sstable_from_kv_pair(
    object_id: HummockSstableObjectId,
    kv_pairs: Vec<(usize, u64, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
) -> TableHolder {
    gen_test_sstable(
        default_builder_opt_for_test(),
        object_id,
        kv_pairs
            .into_iter()
            .map(|kv| (iterator_test_key_of_epoch(kv.0, test_epoch(kv.1)), kv.2)),
        sstable_store,
    )
    .await
}

// key=[idx, epoch], value
pub async fn gen_iterator_test_sstable_with_range_tombstones(
    object_id: HummockSstableObjectId,
    kv_pairs: Vec<(usize, u64, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
) -> SstableInfo {
    gen_test_sstable_with_range_tombstone(
        default_builder_opt_for_test(),
        object_id,
        kv_pairs
            .into_iter()
            .map(|kv| (iterator_test_key_of_epoch(kv.0, test_epoch(kv.1)), kv.2)),
        sstable_store,
    )
    .await
}

pub async fn gen_merge_iterator_interleave_test_sstable_iters(
    key_count: usize,
    count: usize,
) -> Vec<SstableIterator> {
    let sstable_store = mock_sstable_store();
    let mut result = vec![];
    for i in 0..count {
        let table = gen_iterator_test_sstable_base(
            i as HummockSstableObjectId,
            default_builder_opt_for_test(),
            |x| x * count + i,
            sstable_store.clone(),
            key_count,
        )
        .await;
        result.push(SstableIterator::create(
            table,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions::default()),
        ));
    }
    result
}

pub async fn gen_iterator_test_sstable_with_incr_epoch(
    object_id: HummockSstableObjectId,
    opts: SstableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
    epoch_base: u64,
) -> TableHolder {
    gen_test_sstable(
        opts,
        object_id,
        (0..total).map(|i| {
            (
                iterator_test_key_of_epoch(idx_mapping(i), test_epoch(epoch_base + i as u64)),
                HummockValue::put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}
