// Copyright 2022 RisingWave Labs
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

use std::ops::Bound::*;
use std::sync::Arc;

use await_tree::{InstrumentAwait, SpanExt};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use thiserror_ext::AsReport;

use super::super::{HummockResult, HummockValue};
use crate::hummock::block_stream::BlockStream;
use crate::hummock::iterator::{Forward, HummockIterator, ValueMeta};
use crate::hummock::sstable::{BlockMeta, MetaShardDesc, SstableIteratorReadOptions};
use crate::hummock::{
    BlockIterator, HummockError, MetaShardHolder, PartitionedSstableMetaHolder, SstableStoreRef,
};
use crate::monitor::StoreLocalStatistic;

pub trait SstableIteratorType: HummockIterator + 'static {
    fn create(
        sstable: PartitionedSstableMetaHolder,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self;
}

/// Iterates on a sstable.
pub struct SstableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    preload_stream: Option<Box<dyn BlockStream>>,
    preload_end_block_idx: usize,
    preload_retry_times: usize,

    /// Reference to the sst
    pub sst: PartitionedSstableMetaHolder,

    sstable_store: SstableStoreRef,
    stats: StoreLocalStatistic,
    options: Arc<SstableIteratorReadOptions>,

    // used for checking if the block is valid, filter out the block that is not in the table-id range
    block_start_idx_inclusive: usize,
    block_end_idx_inclusive: usize,
    read_table_id_range: (TableId, TableId),

    partitioned_shard_idx: Option<u32>,
    partitioned_shard: Option<MetaShardHolder>,
}

impl SstableIterator {
    pub fn new(
        sstable: PartitionedSstableMetaHolder,
        sstable_store: SstableStoreRef,
        options: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self {
        assert!(
            !sstable_info_ref.table_ids.is_empty(),
            "SstableIterator: SST {} (object {}) has empty table_ids",
            sstable_info_ref.sst_id,
            sstable_info_ref.object_id,
        );
        let read_table_id_range = (
            *sstable_info_ref.table_ids.first().unwrap(),
            *sstable_info_ref.table_ids.last().unwrap(),
        );
        assert!(
            read_table_id_range.0 <= read_table_id_range.1,
            "invalid table id range {} - {}",
            read_table_id_range.0,
            read_table_id_range.1
        );

        let index = &sstable.index;
        let block_meta_count = index.block_count as usize;
        assert!(block_meta_count > 0);
        let (block_start_idx_inclusive, block_end_idx_inclusive) = index
            .block_range_for_table_id_range(read_table_id_range)
            .unwrap_or_else(|| {
                panic!(
                    "table id range {} - {} not found in partitioned meta shard descs",
                    read_table_id_range.0, read_table_id_range.1
                )
            });

        Self {
            block_iter: None,
            cur_idx: 0,
            preload_stream: None,
            preload_end_block_idx: 0,
            preload_retry_times: 0,
            sst: sstable,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            options,
            block_start_idx_inclusive,
            block_end_idx_inclusive,
            read_table_id_range,
            partitioned_shard_idx: None,
            partitioned_shard: None,
        }
    }

    fn init_block_prefetch_range(&mut self, start_idx: usize) {
        assert!(
            start_idx >= self.block_start_idx_inclusive
                && start_idx <= self.block_end_idx_inclusive
        );

        self.preload_stream.take();
        self.preload_end_block_idx = 0;
        self.preload_retry_times = 0;
        let Some(bound) = self.options.must_iterated_end_user_key.as_ref() else {
            return;
        };

        let next_to_start_idx = start_idx + 1;
        if next_to_start_idx > self.block_end_idx_inclusive {
            return;
        }

        let end_idx = match bound {
            Unbounded => self.block_end_idx_inclusive + 1,
            Included(dest_key) => {
                let dest_key = dest_key.as_ref();
                let shard_idx = self.sst.index.shards.partition_point(|shard| {
                    FullKey::decode(&shard.smallest_key).user_key <= dest_key
                });
                self.prefetch_end_block_idx_by_shard_idx(shard_idx)
            }
            Excluded(end_key) => {
                let end_key = end_key.as_ref();
                let shard_idx = self.sst.index.shards.partition_point(|shard| {
                    FullKey::decode(&shard.smallest_key).user_key < end_key
                });
                self.prefetch_end_block_idx_by_shard_idx(shard_idx)
            }
        };

        let end_idx = std::cmp::min(end_idx, self.block_end_idx_inclusive + 1);
        if next_to_start_idx < end_idx {
            self.preload_end_block_idx = end_idx;
        }
    }

    fn prefetch_end_block_idx_by_shard_idx(&self, shard_idx: usize) -> usize {
        if shard_idx == 0 {
            return self.block_start_idx_inclusive;
        }
        let desc =
            &self.sst.index.shards[std::cmp::min(shard_idx, self.sst.index.shards.len()) - 1];
        desc.first_block_idx as usize + desc.block_count as usize
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(
        &mut self,
        idx: usize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        tracing::debug!(
            target: "events::storage::sstable::block_seek",
            "table iterator seek: sstable_object_id = {}, block_id = {}",
            self.sst.id,
            idx,
        );

        // When all data are in block cache, it is highly possible that this iterator will stay on a
        // worker thread for a full time. Therefore, we use tokio's unstable API consume_budget to
        // do cooperative scheduling.
        tokio::task::consume_budget().await;

        if idx > self.block_end_idx_inclusive {
            self.block_iter = None;
            return Ok(());
        }

        let mut hit_cache = false;
        if self.preload_stream.is_none() && idx + 1 < self.preload_end_block_idx {
            match self
                .create_preload_stream(idx)
                .instrument_await("prefetch_blocks".verbose())
                .await
            {
                Ok(preload_stream) => self.preload_stream = Some(preload_stream),
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "failed to create stream for prefetch data, fall back to block get")
                }
            }
        }

        if self
            .preload_stream
            .as_ref()
            .map(|preload_stream| preload_stream.next_block_index() <= idx)
            .unwrap_or(false)
        {
            while let Some(preload_stream) = self.preload_stream.as_mut() {
                let mut ret = Ok(());
                while preload_stream.next_block_index() < idx {
                    if let Err(e) = preload_stream.next_block().await {
                        ret = Err(e);
                        break;
                    }
                }
                assert_eq!(preload_stream.next_block_index(), idx);
                if ret.is_ok() {
                    match preload_stream.next_block().await {
                        Ok(Some(block)) => {
                            hit_cache = true;
                            self.block_iter = Some(BlockIterator::new(block));
                            break;
                        }
                        Ok(None) => {
                            self.preload_stream.take();
                        }
                        Err(e) => {
                            self.preload_stream.take();
                            ret = Err(e);
                        }
                    }
                } else {
                    self.preload_stream.take();
                }
                if self.preload_stream.is_none() && idx + 1 < self.preload_end_block_idx {
                    if let Err(e) = ret {
                        tracing::warn!(error = %e.as_report(), "recreate stream because the connection to remote storage has closed");
                        if self.preload_retry_times >= self.options.max_preload_retry_times {
                            break;
                        }
                        self.preload_retry_times += 1;
                    }

                    match self
                        .create_preload_stream(idx)
                        .instrument_await("prefetch_blocks".verbose())
                        .await
                    {
                        Ok(stream) => {
                            self.preload_stream = Some(stream);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "failed to recreate stream meet IO error");
                            break;
                        }
                    }
                }
            }
        }

        if !hit_cache {
            self.seek_partitioned_idx(idx, seek_key).await?;
        } else {
            let block_iter = self.block_iter.as_mut().unwrap();
            if let Some(key) = seek_key {
                block_iter.seek(key);
            } else {
                block_iter.seek_to_first();
            }
            self.cur_idx = idx;
        }
        Ok(())
    }

    fn partitioned_desc_by_block_idx(&self, idx: usize) -> Option<MetaShardDesc> {
        let index = &self.sst.index;
        let shard_idx = index
            .shards
            .partition_point(|shard| shard.first_block_idx as usize <= idx)
            .saturating_sub(1);
        let desc = index.shards.get(shard_idx)?;
        if idx < desc.first_block_idx as usize + desc.block_count as usize {
            Some(desc.clone())
        } else {
            None
        }
    }

    async fn ensure_partitioned_shard_loaded(&mut self, desc: &MetaShardDesc) -> HummockResult<()> {
        if self.partitioned_shard_idx == Some(desc.shard_idx) {
            return Ok(());
        }

        let filter_type = self.sst.index.filter_type;
        let shard = self
            .sstable_store
            .get_meta_shard_holder(self.sst.id, filter_type, desc, &mut self.stats)
            .await?;
        self.partitioned_shard_idx = Some(desc.shard_idx);
        self.partitioned_shard = Some(shard);
        Ok(())
    }

    async fn partitioned_block_meta(&mut self, idx: usize) -> HummockResult<Option<BlockMeta>> {
        let Some(desc) = self.partitioned_desc_by_block_idx(idx) else {
            return Ok(None);
        };
        self.ensure_partitioned_shard_loaded(&desc).await?;
        let shard = self
            .partitioned_shard
            .as_ref()
            .expect("partitioned shard should be loaded");
        let local_idx = idx.saturating_sub(shard.first_block_idx as usize);
        Ok(shard.block_metas.get(local_idx).cloned())
    }

    async fn partitioned_block_metas_in_range(
        &mut self,
        start_idx: usize,
        end_idx_exclusive: usize,
    ) -> HummockResult<Vec<BlockMeta>> {
        let mut block_metas = Vec::with_capacity(end_idx_exclusive.saturating_sub(start_idx));
        let mut idx = start_idx;
        while idx < end_idx_exclusive {
            let Some(desc) = self.partitioned_desc_by_block_idx(idx) else {
                break;
            };
            self.ensure_partitioned_shard_loaded(&desc).await?;
            let shard = self
                .partitioned_shard
                .as_ref()
                .expect("partitioned shard should be loaded");
            let shard_first_idx = shard.first_block_idx as usize;
            let local_start_idx = idx.saturating_sub(shard_first_idx);
            let local_end_idx = std::cmp::min(
                shard.block_metas.len(),
                end_idx_exclusive.saturating_sub(shard_first_idx),
            );
            block_metas.extend_from_slice(&shard.block_metas[local_start_idx..local_end_idx]);
            idx = shard_first_idx + local_end_idx;
        }
        Ok(block_metas)
    }

    async fn create_preload_stream(&mut self, idx: usize) -> HummockResult<Box<dyn BlockStream>> {
        let prefetch_end_idx = std::cmp::min(
            self.preload_end_block_idx,
            idx + self.sstable_store.max_prefetch_block_number().max(1),
        );
        let block_metas = self
            .partitioned_block_metas_in_range(idx, prefetch_end_idx)
            .await?;
        if block_metas.is_empty() {
            return Err(HummockError::other(format!(
                "no partitioned block metas found for prefetch sst {} block {}..{}",
                self.sst.id, idx, prefetch_end_idx
            )));
        }
        self.sstable_store
            .prefetch_blocks_by_block_metas(
                self.sst.id,
                self.sst.meta.estimated_size,
                idx,
                &block_metas,
                self.options.cache_policy,
                &mut self.stats,
            )
            .await
    }

    async fn seek_partitioned_idx(
        &mut self,
        idx: usize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        if idx > self.block_end_idx_inclusive {
            self.block_iter = None;
            return Ok(());
        }

        let Some(block_meta) = self.partitioned_block_meta(idx).await? else {
            self.block_iter = None;
            return Ok(());
        };
        let block = self
            .sstable_store
            .get_by_block_meta(
                self.sst.id,
                self.sst.meta.estimated_size,
                idx,
                &block_meta,
                self.options.cache_policy,
                &mut self.stats,
            )
            .await?;
        let mut block_iter = BlockIterator::new(block);
        if let Some(key) = seek_key {
            block_iter.seek(key);
        } else {
            block_iter.seek_to_first();
        }
        self.block_iter = Some(block_iter);
        self.cur_idx = idx;
        Ok(())
    }

    async fn skip_partitioned_table_ids_outside_range(&mut self) -> HummockResult<()> {
        while self.is_valid() {
            let table_id = self.key().user_key.table_id;
            if table_id < self.read_table_id_range.0 {
                self.seek_idx(self.cur_idx + 1, None).await?;
            } else if table_id > self.read_table_id_range.1 {
                self.block_iter = None;
                break;
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn calculate_partitioned_block_idx_by_key(
        &mut self,
        key: FullKey<&[u8]>,
    ) -> HummockResult<usize> {
        let Some(desc) = self.sst.index.locate_shard_by_key(key).cloned() else {
            return Ok(self.block_start_idx_inclusive);
        };
        self.ensure_partitioned_shard_loaded(&desc).await?;
        let shard = self
            .partitioned_shard
            .as_ref()
            .expect("partitioned shard should be loaded");
        let local_idx = shard.locate_block_by_key(key);
        Ok(shard.first_block_idx as usize + local_idx)
    }
}

impl HummockIterator for SstableIterator {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        self.stats.total_key_count += 1;
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        if !block_iter.try_next() {
            // seek to next block
            self.seek_idx(self.cur_idx + 1, None).await?;
        }
        self.skip_partitioned_table_ids_outside_range().await?;

        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.block_iter.as_ref().expect("no block iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self.block_iter.as_ref().expect("no block iter").value();

        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        self.block_iter.as_ref().is_some_and(|i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        let start_key = FullKey {
            user_key: UserKey::new(self.read_table_id_range.0, TableKey(Vec::new())),
            epoch_with_gap: EpochWithGap::new_max_epoch(),
        };
        self.seek(start_key.to_ref()).await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        if key.user_key.table_id > self.read_table_id_range.1 {
            self.block_iter = None;
            return Ok(());
        }

        let partitioned_start_key;
        let key = if key.user_key.table_id < self.read_table_id_range.0 {
            partitioned_start_key = FullKey {
                user_key: UserKey::new(self.read_table_id_range.0, TableKey(Vec::new())),
                epoch_with_gap: EpochWithGap::new_max_epoch(),
            };
            partitioned_start_key.to_ref()
        } else {
            key
        };

        let block_idx = self.calculate_partitioned_block_idx_by_key(key).await?;
        self.init_block_prefetch_range(block_idx);

        self.seek_idx(block_idx, Some(key)).await?;
        while !self.is_valid() && self.cur_idx < self.block_end_idx_inclusive {
            self.seek_idx(self.cur_idx + 1, None).await?;
        }
        self.skip_partitioned_table_ids_outside_range().await?;
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats);
    }

    fn value_meta(&self) -> ValueMeta {
        ValueMeta {
            object_id: Some(self.sst.id),
            block_id: Some(self.cur_idx as _),
        }
    }
}

impl SstableIteratorType for SstableIterator {
    fn create(
        sstable: PartitionedSstableMetaHolder,
        sstable_store: SstableStoreRef,
        options: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self {
        SstableIterator::new(sstable, sstable_store, options, sstable_info_ref)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;

    use bytes::Bytes;
    use foyer::Hint;
    use itertools::Itertools;
    use rand::prelude::*;
    use rand::rng as thread_rng;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::{TableKey, UserKey};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::CachePolicy;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_default_test_sstable,
        gen_test_sstable_info, gen_test_sstable_with_table_ids, test_key_of, test_value_of,
    };

    async fn inner_test_forward_iterator(
        sstable_store: SstableStoreRef,
        handle: PartitionedSstableMetaHolder,
        sstable_info: SstableInfo,
    ) {
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        let mut sstable_iter = SstableIterator::create(
            handle,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
            &sstable_info,
        );
        let mut cnt = 0;
        sstable_iter.rewind().await.unwrap();

        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(key, test_key_of(cnt).to_ref());
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }

        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_table_iterator() {
        // Build remote sstable
        let sstable_store = mock_sstable_store().await;
        let (sstable, sstable_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.block_count() > 10);

        inner_test_forward_iterator(sstable_store.clone(), sstable, sstable_info).await;
    }

    #[tokio::test]
    async fn test_table_seek() {
        let sstable_store = mock_sstable_store().await;
        let (sstable, sstable_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.block_count() > 10);
        let mut sstable_iter = SstableIterator::create(
            sstable,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
            &sstable_info,
        );
        let mut all_key_to_test = (0..TEST_KEYS_COUNT).collect_vec();
        let mut rng = thread_rng();
        all_key_to_test.shuffle(&mut rng);

        // We seek and access all the keys in random order
        for i in all_key_to_test {
            sstable_iter.seek(test_key_of(i).to_ref()).await.unwrap();
            // sstable_iter.next().await.unwrap();
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i).to_ref());
        }

        // Seek to key #500 and start iterating.
        sstable_iter.seek(test_key_of(500).to_ref()).await.unwrap();
        for i in 500..TEST_KEYS_COUNT {
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i).to_ref());
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());

        // Seek to < first key
        let smallest_key = FullKey::for_test(
            TableId::default(),
            [
                VirtualNode::ZERO.to_be_bytes().as_slice(),
                format!("key_aaaa_{:05}", 0).as_bytes(),
            ]
            .concat(),
            test_epoch(233),
        );
        sstable_iter.seek(smallest_key.to_ref()).await.unwrap();
        let key = sstable_iter.key();
        assert_eq!(key, test_key_of(0).to_ref());

        // Seek to > last key
        let largest_key = FullKey::for_test(
            TableId::default(),
            [
                VirtualNode::ZERO.to_be_bytes().as_slice(),
                format!("key_zzzz_{:05}", 0).as_bytes(),
            ]
            .concat(),
            test_epoch(233),
        );
        sstable_iter.seek(largest_key.to_ref()).await.unwrap();
        assert!(!sstable_iter.is_valid());

        // Seek to non-existing key
        for idx in 1..TEST_KEYS_COUNT {
            // Seek to the previous key of each existing key. e.g.,
            // Our key space is `key_test_00000`, `key_test_00002`, `key_test_00004`, ...
            // And we seek to `key_test_00001` (will produce `key_test_00002`), `key_test_00003`
            // (will produce `key_test_00004`).
            sstable_iter
                .seek(
                    FullKey::for_test(
                        TableId::default(),
                        [
                            VirtualNode::ZERO.to_be_bytes().as_slice(),
                            format!("key_test_{:05}", idx * 2 - 1).as_bytes(),
                        ]
                        .concat(),
                        0,
                    )
                    .to_ref(),
                )
                .await
                .unwrap();

            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(idx).to_ref());
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());
    }

    #[tokio::test]
    async fn test_prefetch_table_read() {
        let sstable_store = mock_sstable_store().await;
        // when upload data is successful, but upload meta is fail and delete is fail
        let kv_iter =
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
        let sst_info = gen_test_sstable_info(
            default_builder_opt_for_test(),
            0,
            kv_iter,
            sstable_store.clone(),
        )
        .await;

        let end_key = test_key_of(TEST_KEYS_COUNT);
        let uk = UserKey::new(
            end_key.user_key.table_id,
            TableKey(Bytes::from(end_key.user_key.table_key.0)),
        );
        let options = Arc::new(SstableIteratorReadOptions {
            cache_policy: CachePolicy::Fill(Hint::Normal),
            must_iterated_end_user_key: Some(Bound::Included(uk.clone())),
            max_preload_retry_times: 0,
            prefetch_for_large_query: false,
        });
        let mut stats = StoreLocalStatistic::default();
        let mut sstable_iter = SstableIterator::create(
            sstable_store
                .meta_index(&sst_info, &mut stats)
                .await
                .unwrap(),
            sstable_store.clone(),
            options.clone(),
            &sst_info,
        );
        let mut cnt = 1000;
        sstable_iter.seek(test_key_of(cnt).to_ref()).await.unwrap();
        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(
                key,
                test_key_of(cnt).to_ref(),
                "fail at {}, get key :{:?}",
                cnt,
                String::from_utf8(key.user_key.table_key.key_part().to_vec()).unwrap()
            );
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
        sstable_iter.collect_local_statistic(&mut stats);
        assert!(stats.cache_data_prefetch_count > 0);
        assert!(stats.sst_store_data_prefetch_read_count > 0);

        let mut sstable_iter = SstableIterator::create(
            sstable_store
                .meta_index(&sst_info, &mut stats)
                .await
                .unwrap(),
            sstable_store,
            options.clone(),
            &sst_info,
        );
        let mut cnt = 1000;
        sstable_iter.seek(test_key_of(cnt).to_ref()).await.unwrap();
        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(key, test_key_of(cnt).to_ref());
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_read_table_id_range() {
        {
            let sstable_store = mock_sstable_store().await;
            let (sstable, sstable_info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                    .await;
            let mut sstable_iter = SstableIterator::create(
                sstable,
                sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions::default()),
                &sstable_info,
            );
            sstable_iter.rewind().await.unwrap();
            assert!(sstable_iter.is_valid());
            assert_eq!(sstable_iter.key(), test_key_of(0).to_ref());
        }

        {
            let sstable_store = mock_sstable_store().await;
            // test key_range right
            let k1 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 1).as_bytes());
                let uk = UserKey::for_test(TableId::from(1), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            let k2 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 2).as_bytes());
                let uk = UserKey::for_test(TableId::from(2), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            let k3 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 3).as_bytes());
                let uk = UserKey::for_test(TableId::from(3), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;
                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![1.into(), 2.into(), 3.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k1.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(3, cnt);
                assert_eq!(last_key, k3.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![1.into(), 2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k1.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(2, cnt);
                assert_eq!(last_key, k2.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into(), 3.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(2, cnt);
                assert_eq!(last_key, k3.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(1, cnt);
                assert_eq!(last_key, k2.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];
                let mut opt = default_builder_opt_for_test();
                opt.partitioned_meta_block_count = 8;

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    opt,
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(1, cnt);
                assert_eq!(last_key, k2.clone());
            }
        }
    }
}
