// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use foyer::{
    CacheHint, Engine, HybridCache, HybridCacheBuilder, StorageKey as HybridKey,
    StorageValue as HybridValue,
};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::EvictionConfig;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch, HummockSstableObjectId};

use super::iterator::test_utils::iterator_test_table_key_of;
use super::{
    HummockResult, InMemWriter, SstableMeta, SstableWriterOptions, DEFAULT_RESTART_INTERVAL,
};
use crate::compaction_catalog_manager::{
    CompactionCatalogAgent, FilterKeyExtractorImpl, FullKeyFilterKeyExtractor,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferItem, SharedBufferValue,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, FilterBuilder, LruCache, Sstable, SstableBuilder,
    SstableBuilderOptions, SstableStoreRef, SstableWriter, TableHolder, Xor16FilterBuilder,
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
        data_directory: "hummock_001".to_owned(),
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
    SstableInfoInner {
        object_id: id,
        sst_id: id,
        key_range: KeyRange {
            left: Bytes::from(FullKey::for_test(table_id, min_table_key, epoch).encode()),
            right: Bytes::from(FullKey::for_test(table_id, max_table_key, epoch).encode()),
            right_exclusive: false,
        },
        file_size,
        table_ids: vec![table_id.table_id],
        uncompressed_file_size: file_size,
        min_epoch: epoch,
        max_epoch: epoch,
        sst_size: file_size,
        ..Default::default()
    }
    .into()
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
    let table_id_to_vnode = HashMap::from_iter(vec![(
        TableId::default().table_id(),
        VirtualNode::COUNT_FOR_TEST,
    )]);
    let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
    let mut b = SstableBuilder::for_test(
        0,
        mock_sst_writer(&opts),
        opts,
        table_id_to_vnode,
        table_id_to_watermark_serde,
    );
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
    table_ids: Vec<u32>,
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

    // dummy
    let bloom_filter = {
        let mut filter_builder = BlockedXor16FilterBuilder::new(100);
        for _ in &meta.block_metas {
            filter_builder.switch_block(None);
        }

        filter_builder.finish(None)
    };

    meta.meta_offset = writer.data_len() as u64;
    meta.bloom_filter = bloom_filter;
    let sst = SstableInfoInner {
        object_id: sst_object_id,
        sst_id: sst_object_id,
        key_range: KeyRange {
            left: Bytes::from(meta.smallest_key.clone()),
            right: Bytes::from(meta.largest_key.clone()),
            right_exclusive: false,
        },
        file_size: meta.estimated_size as u64,
        meta_offset: meta.meta_offset,
        uncompressed_file_size: meta.estimated_size as u64,
        table_ids,
        ..Default::default()
    }
    .into();
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
    table_id_to_vnode: HashMap<u32, usize>,
    table_id_to_watermark_serde: HashMap<u32, Option<(OrderedRowSerde, OrderedRowSerde, usize)>>,
) -> SstableInfo {
    let writer_opts = SstableWriterOptions {
        capacity_hint: None,
        tracker: None,
        policy,
    };
    let writer = sstable_store
        .clone()
        .create_sst_writer(object_id, writer_opts);

    let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
        FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
        table_id_to_vnode,
        table_id_to_watermark_serde,
    ));

    let mut b = SstableBuilder::<_, F>::new(
        object_id,
        writer,
        F::create(opts.bloom_false_positive, opts.capacity / 16),
        opts,
        compaction_catalog_agent_ref,
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
) -> (TableHolder, SstableInfo) {
    let table_id_to_vnode = HashMap::from_iter(vec![(
        TableId::default().table_id(),
        VirtualNode::COUNT_FOR_TEST,
    )]);

    let table_id_to_watermark_serde =
        HashMap::from_iter(vec![(TableId::default().table_id(), None)]);

    let sst_info = gen_test_sstable_impl::<_, Xor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store.clone(),
        CachePolicy::NotFill,
        table_id_to_vnode,
        table_id_to_watermark_serde,
    )
    .await;

    (
        sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap(),
        sst_info,
    )
}

pub async fn gen_test_sstable_with_table_ids<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl Iterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
    table_ids: Vec<StateTableId>,
) -> (TableHolder, SstableInfo) {
    let table_id_to_vnode = table_ids
        .iter()
        .map(|table_id| (*table_id, VirtualNode::COUNT_FOR_TEST))
        .collect();
    let table_id_to_watermark_serde = table_ids.iter().map(|table_id| (*table_id, None)).collect();

    let sst_info = gen_test_sstable_impl::<_, Xor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store.clone(),
        CachePolicy::NotFill,
        table_id_to_vnode,
        table_id_to_watermark_serde,
    )
    .await;

    (
        sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap(),
        sst_info,
    )
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_info<B: AsRef<[u8]> + Clone + Default + Eq>(
    opts: SstableBuilderOptions,
    object_id: HummockSstableObjectId,
    kv_iter: impl IntoIterator<Item = (FullKey<B>, HummockValue<B>)>,
    sstable_store: SstableStoreRef,
) -> SstableInfo {
    let table_id_to_vnode = HashMap::from_iter(vec![(
        TableId::default().table_id(),
        VirtualNode::COUNT_FOR_TEST,
    )]);

    let table_id_to_watermark_serde =
        HashMap::from_iter(vec![(TableId::default().table_id(), None)]);

    gen_test_sstable_impl::<_, BlockedXor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store,
        CachePolicy::NotFill,
        table_id_to_vnode,
        table_id_to_watermark_serde,
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
    let table_id_to_vnode = HashMap::from_iter(vec![(
        TableId::default().table_id(),
        VirtualNode::COUNT_FOR_TEST,
    )]);

    let table_id_to_watermark_serde =
        HashMap::from_iter(vec![(TableId::default().table_id(), None)]);

    gen_test_sstable_impl::<_, Xor16FilterBuilder>(
        opts,
        object_id,
        kv_iter,
        sstable_store.clone(),
        CachePolicy::Fill(CacheHint::Normal),
        table_id_to_vnode,
        table_id_to_watermark_serde,
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
) -> (TableHolder, SstableInfo) {
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

pub async fn hybrid_cache_for_test<K, V>() -> HybridCache<K, V>
where
    K: HybridKey,
    V: HybridValue,
{
    HybridCacheBuilder::new()
        .memory(10)
        .storage(Engine::Large)
        .build()
        .await
        .unwrap()
}
