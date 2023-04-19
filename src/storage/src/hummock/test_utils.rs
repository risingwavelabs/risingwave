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

use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableObjectId};
use risingwave_pb::hummock::{KeyRange, SstableInfo};

use super::iterator::test_utils::iterator_test_table_key_of;
use super::{
    create_monotonic_events, CompressionAlgorithm, HummockResult, InMemWriter, SstableMeta,
    SstableWriterOptions, DEFAULT_RESTART_INTERVAL,
};
use crate::error::StorageResult;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    CachePolicy, DeleteRangeTombstone, LruCache, Sstable, SstableBuilder, SstableBuilderOptions,
    SstableStoreRef, SstableWriter,
};
use crate::monitor::StoreLocalStatistic;
use crate::opts::StorageOpts;
use crate::storage_value::StorageValue;

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
        high_priority_ratio: 0,
        disable_remote_compactor: false,
        enable_local_spill: false,
        local_object_store: "memory".to_string(),
        share_buffer_upload_concurrency: 1,
        compactor_memory_limit_mb: 64,
        sstable_id_remote_fetch_number: 1,
        ..Default::default()
    }
}

pub fn gen_dummy_batch(n: u64) -> Vec<(Bytes, StorageValue)> {
    vec![(
        Bytes::from(iterator_test_table_key_of(n as usize)),
        StorageValue::new_put(b"value1".to_vec()),
    )]
}

pub fn gen_dummy_batch_several_keys(n: usize) -> Vec<(Bytes, StorageValue)> {
    let mut kvs = vec![];
    let v = Bytes::from(b"value1".to_vec().repeat(100));
    for idx in 0..n {
        kvs.push((
            Bytes::from(iterator_test_table_key_of(idx)),
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
        table_ids: vec![],
        meta_offset: 0,
        stale_key_count: 0,
        total_key_count: 0,
        uncompressed_file_size: file_size,
        min_epoch: 0,
        max_epoch: 0,
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
        compression_algorithm: CompressionAlgorithm::None,
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
        b.add_for_test(key.to_ref(), value.as_slice(), true)
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
        table_ids: vec![],
        meta_offset: meta.meta_offset,
        stale_key_count: 0,
        total_key_count: 0,
        uncompressed_file_size: meta.estimated_size as u64,
        min_epoch: 0,
        max_epoch: 0,
    };
    let writer_output = writer.finish(meta).await?;
    writer_output.await.unwrap()?;
    Ok(sst)
}

/// Generates a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_inner<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl Iterator<Item = (FullKey<B>, HummockValue<B>)>,
    range_tombstones: Vec<DeleteRangeTombstone>,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
) -> (Sstable, SstableInfo) {
    let writer_opts = SstableWriterOptions {
        capacity_hint: None,
        tracker: None,
        policy,
    };
    let writer = sstable_store
        .clone()
        .create_sst_writer(object_id, writer_opts);
    let mut b = SstableBuilder::for_test(object_id, writer, opts);

    let mut last_key = FullKey::<B>::default();
    let mut user_key_last_delete = HummockEpoch::MAX;
    for (mut key, value) in kv_iter {
        let mut is_new_user_key =
            last_key.is_empty() || key.user_key.as_ref() != last_key.user_key.as_ref();
        let epoch = key.epoch;
        if is_new_user_key {
            last_key = key.clone();
            user_key_last_delete = HummockEpoch::MAX;
        }

        let mut earliest_delete_epoch = HummockEpoch::MAX;
        for range_tombstone in &range_tombstones {
            if range_tombstone
                .start_user_key
                .as_ref()
                .le(&key.user_key.as_ref())
                && range_tombstone
                    .end_user_key
                    .as_ref()
                    .gt(&key.user_key.as_ref())
                && range_tombstone.sequence >= key.epoch
                && range_tombstone.sequence < earliest_delete_epoch
            {
                earliest_delete_epoch = range_tombstone.sequence;
            }
        }

        if value.is_delete() {
            user_key_last_delete = epoch;
        } else if earliest_delete_epoch < user_key_last_delete {
            user_key_last_delete = earliest_delete_epoch;

            key.epoch = earliest_delete_epoch;
            b.add(key.to_ref(), HummockValue::Delete, is_new_user_key)
                .await
                .unwrap();
            key.epoch = epoch;
            is_new_user_key = false;
        }

        b.add(key.to_ref(), value.as_slice(), is_new_user_key)
            .await
            .unwrap();
    }
    b.add_monotonic_deletes(create_monotonic_events(&range_tombstones));
    let output = b.finish().await.unwrap();
    output.writer_output.await.unwrap().unwrap();
    let table = sstable_store
        .sstable(
            &output.sst_info.sst_info,
            &mut StoreLocalStatistic::default(),
        )
        .await
        .unwrap();
    (table.value().as_ref().clone(), output.sst_info.sst_info)
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl Iterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
) -> Sstable {
    gen_test_sstable_inner(
        opts,
        object_id,
        kv_iter,
        vec![],
        sstable_store,
        CachePolicy::NotFill,
    )
    .await
    .0
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_and_info<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl Iterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
) -> (Sstable, SstableInfo) {
    gen_test_sstable_inner(
        opts,
        object_id,
        kv_iter,
        vec![],
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
    range_tombstones: Vec<DeleteRangeTombstone>,
    sstable_store: SstableStoreRef,
) -> Sstable {
    gen_test_sstable_inner(
        opts,
        object_id,
        kv_iter,
        range_tombstones,
        sstable_store,
        CachePolicy::NotFill,
    )
    .await
    .0
}

/// Generates a user key with table id 0 and the given `table_key`
pub fn test_user_key(table_key: impl AsRef<[u8]>) -> UserKey<Vec<u8>> {
    UserKey::for_test(TableId::default(), table_key.as_ref().to_vec())
}

/// Generates a user key with table id 0 and table key format of `key_test_{idx * 2}`
pub fn test_user_key_of(idx: usize) -> UserKey<Vec<u8>> {
    let table_key = format!("key_test_{:05}", idx * 2).as_bytes().to_vec();
    UserKey::for_test(TableId::default(), table_key)
}

/// Generates a full key with table id 0 and epoch 123. User key is created with `test_user_key_of`.
pub fn test_key_of(idx: usize) -> FullKey<Vec<u8>> {
    FullKey {
        user_key: test_user_key_of(idx),
        epoch: 233,
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
) -> Sstable {
    gen_test_sstable(
        opts,
        object_id,
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
        sstable_store,
    )
    .await
}

pub async fn count_stream<T>(s: impl Stream<Item = StorageResult<T>> + Send) -> usize {
    futures::pin_mut!(s);
    let mut c: usize = 0;
    while s.try_next().await.unwrap().is_some() {
        c += 1
    }
    c
}

pub fn create_small_table_cache() -> Arc<LruCache<HummockSstableObjectId, Box<Sstable>>> {
    Arc::new(LruCache::new(1, 4, 0))
}
