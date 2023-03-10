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

use std::iter::Iterator;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableId};
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
};

use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStore;
pub use crate::hummock::test_utils::default_builder_opt_for_test;
use crate::hummock::test_utils::{
    create_small_table_cache, gen_test_sstable, gen_test_sstable_with_range_tombstone,
};
use crate::hummock::{
    DeleteRangeTombstone, HummockValue, Sstable, SstableBuilderOptions, SstableIterator,
    SstableIteratorType, SstableStoreRef, TieredCache,
};
use crate::monitor::ObjectStoreMetrics;

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
    mock_sstable_store_with_object_store(Arc::new(ObjectStoreImpl::Hybrid {
        local: Box::new(ObjectStoreImpl::InMem(
            InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
        )),
        remote: Box::new(ObjectStoreImpl::InMem(
            InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
        )),
    }))
}

pub fn mock_sstable_store_with_object_store(store: ObjectStoreRef) -> SstableStoreRef {
    let path = "test".to_string();
    Arc::new(SstableStore::new(
        store,
        path,
        64 << 20,
        64 << 20,
        TieredCache::none(),
    ))
}

pub fn iterator_test_table_key_of(idx: usize) -> Vec<u8> {
    format!("key_test_{:05}", idx).as_bytes().to_vec()
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
        epoch: 233,
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
        epoch,
    }
}

/// Generates keys like `{table_id=0}key_test_00002` with epoch `epoch` .
pub fn iterator_test_bytes_key_of_epoch(idx: usize, epoch: HummockEpoch) -> FullKey<Bytes> {
    iterator_test_key_of_epoch(idx, epoch).into_bytes()
}

/// The value of an index, like `value_test_00002` without value meta
pub fn iterator_test_value_of(idx: usize) -> Vec<u8> {
    format!("value_test_{:05}", idx).as_bytes().to_vec()
}

/// Generates a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_iterator_test_sstable_base(
    sst_id: HummockSstableId,
    opts: SstableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
) -> Sstable {
    gen_test_sstable(
        opts,
        sst_id,
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
    sst_id: HummockSstableId,
    kv_pairs: Vec<(usize, u64, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
) -> Sstable {
    gen_test_sstable(
        default_builder_opt_for_test(),
        sst_id,
        kv_pairs
            .into_iter()
            .map(|kv| (iterator_test_key_of_epoch(kv.0, kv.1), kv.2)),
        sstable_store,
    )
    .await
}

// key=[idx, epoch], value
pub async fn gen_iterator_test_sstable_with_range_tombstones(
    sst_id: HummockSstableId,
    kv_pairs: Vec<(usize, u64, HummockValue<Vec<u8>>)>,
    delete_ranges: Vec<(usize, usize, u64)>,
    sstable_store: SstableStoreRef,
) -> Sstable {
    let range_tombstones = delete_ranges
        .into_iter()
        .map(|(start, end, epoch)| {
            DeleteRangeTombstone::new(
                TableId::default(),
                iterator_test_table_key_of(start),
                iterator_test_table_key_of(end),
                epoch,
            )
        })
        .collect_vec();
    gen_test_sstable_with_range_tombstone(
        default_builder_opt_for_test(),
        sst_id,
        kv_pairs
            .into_iter()
            .map(|kv| (iterator_test_key_of_epoch(kv.0, kv.1), kv.2)),
        range_tombstones,
        sstable_store,
    )
    .await
}

pub async fn gen_merge_iterator_interleave_test_sstable_iters(
    key_count: usize,
    count: usize,
) -> Vec<SstableIterator> {
    let sstable_store = mock_sstable_store();
    let cache = create_small_table_cache();
    let mut result = vec![];
    for i in 0..count {
        let table = gen_iterator_test_sstable_base(
            i as HummockSstableId,
            default_builder_opt_for_test(),
            |x| x * count + i,
            sstable_store.clone(),
            key_count,
        )
        .await;
        let handle = cache.insert(table.id, table.id, 1, Box::new(table), false);
        result.push(SstableIterator::create(
            handle,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions::default()),
        ));
    }
    result
}

pub async fn gen_iterator_test_sstable_with_incr_epoch(
    sst_id: HummockSstableId,
    opts: SstableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
    epoch_base: u64,
) -> Sstable {
    gen_test_sstable(
        opts,
        sst_id,
        (0..total).map(|i| {
            (
                iterator_test_key_of_epoch(idx_mapping(i), epoch_base + i as u64),
                HummockValue::put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}
