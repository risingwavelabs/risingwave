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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_hummock_sdk::key::{FullKey, MAX_KEY_LEN, TABLE_PREFIX_LEN, UserKey, user_key};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner, VnodeStatistics};
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableObjectId, LocalSstableInfo};
use risingwave_pb::hummock::{BloomFilterType, PbSstableFilterType};

use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, DEFAULT_BLOCK_SIZE, DEFAULT_ENTRY_SIZE,
    DEFAULT_RESTART_INTERVAL, META_SHARD_POLICY_FIXED_BLOCK_COUNT, MetaPartitionIndex, MetaShard,
    MetaShardDesc, PARTITIONED_META_VERSION, SstableMeta, SstableWriter,
};
use crate::compaction_catalog_manager::{
    CompactionCatalogAgent, CompactionCatalogAgentRef, FilterKeyExtractorImpl,
    FullKeyFilterKeyExtractor,
};
use crate::hummock::sstable::{
    DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP, FilterBuilder, FilterBuilderOptions, utils,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    HummockResult, MemoryLimiter, Xor16FilterBuilder, try_shorten_block_smallest_key,
};
use crate::monitor::CompactorMetrics;
use crate::opts::StorageOpts;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.001;
pub const DEFAULT_MAX_SST_SIZE: u64 = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// Deprecated and ignored by SST filter builders; kept for backward compatibility.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    pub max_sst_size: u64,
    /// If set, block metadata keys will be shortened when their length exceeds this threshold.
    pub shorten_block_meta_key_threshold: Option<usize>,
    /// Max bytes for vnode key-range hints in SST metadata. None disables collection.
    pub max_vnode_key_range_bytes: Option<usize>,
    /// Number of data blocks per v3 metadata shard.
    pub partitioned_meta_block_count: usize,
    /// Estimated key count for one output SST. Used only as a filter-builder capacity hint.
    pub estimated_output_key_count: Option<usize>,
    /// Upper bound for the initial key-hash buffer allocation in plain filter builders.
    pub filter_hash_prealloc_key_count_cap: usize,
}

impl From<&StorageOpts> for SstableBuilderOptions {
    fn from(options: &StorageOpts) -> SstableBuilderOptions {
        let capacity: usize = (options.sstable_size_mb as usize) * (1 << 20);
        SstableBuilderOptions {
            capacity,
            block_capacity: (options.block_size_kb as usize) * (1 << 10),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: options.compactor_max_sst_size,
            shorten_block_meta_key_threshold: options.shorten_block_meta_key_threshold,
            max_vnode_key_range_bytes: None,
            partitioned_meta_block_count: std::env::var("RW_SSTABLE_META_SHARD_BLOCK_COUNT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(8),
            estimated_output_key_count: None,
            filter_hash_prealloc_key_count_cap: DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
        }
    }
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: DEFAULT_MAX_SST_SIZE,
            shorten_block_meta_key_threshold: None,
            max_vnode_key_range_bytes: None,
            partitioned_meta_block_count: 8,
            estimated_output_key_count: None,
            filter_hash_prealloc_key_count_cap: DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
        }
    }
}

impl SstableBuilderOptions {
    pub fn estimated_output_key_count(&self) -> usize {
        self.estimated_output_key_count
            .unwrap_or(self.capacity / DEFAULT_ENTRY_SIZE + 1)
    }

    pub fn filter_builder_options(&self) -> FilterBuilderOptions {
        FilterBuilderOptions {
            estimated_key_count: self.estimated_output_key_count(),
            estimated_block_count: self.capacity / self.block_capacity + 1,
            hash_prealloc_key_count_cap: self.filter_hash_prealloc_key_count_cap,
        }
    }
}

struct PartitionedMetaBuilder {
    shard_block_count: usize,
    filter_capacity: usize,
    current_first_block_idx: usize,
    current_filter_builder: Xor16FilterBuilder,
    shard_filters: Vec<Vec<u8>>,
    shard_block_counts: Vec<usize>,
}

impl PartitionedMetaBuilder {
    fn new(shard_block_count: usize, capacity: usize) -> Self {
        let filter_capacity = capacity / DEFAULT_ENTRY_SIZE / shard_block_count.max(1) + 1;
        Self {
            shard_block_count,
            filter_capacity,
            current_first_block_idx: 0,
            current_filter_builder: Xor16FilterBuilder::new(filter_capacity),
            shard_filters: Vec::new(),
            shard_block_counts: Vec::new(),
        }
    }

    fn add_key(&mut self, dist_key: &[u8], table_id: u32) {
        self.current_filter_builder.add_key(dist_key, table_id);
    }

    fn maybe_finish_shard(
        &mut self,
        total_block_count: usize,
        force: bool,
        memory_limiter: Option<Arc<MemoryLimiter>>,
    ) {
        let block_count = total_block_count.saturating_sub(self.current_first_block_idx);
        if block_count == 0 {
            return;
        }

        if force || block_count >= self.shard_block_count {
            self.shard_filters
                .push(self.current_filter_builder.finish(memory_limiter));
            self.shard_block_counts.push(block_count);
            self.current_first_block_idx = total_block_count;
            self.current_filter_builder = Xor16FilterBuilder::new(self.filter_capacity);
        }
    }

    fn approximate_len(&self) -> usize {
        self.current_filter_builder.approximate_len()
            + self.shard_filters.iter().map(Vec::len).sum::<usize>()
    }

    fn can_add_raw_shard(&self, total_block_count: usize, block_count: usize) -> bool {
        self.current_first_block_idx == total_block_count
            && block_count == self.shard_block_count
            && block_count > 0
    }

    fn add_raw_shard(&mut self, total_block_count: usize, block_count: usize, filter: Vec<u8>) {
        assert!(self.can_add_raw_shard(total_block_count - block_count, block_count));
        self.shard_filters.push(filter);
        self.shard_block_counts.push(block_count);
        self.current_first_block_idx = total_block_count;
        self.current_filter_builder = Xor16FilterBuilder::new(self.filter_capacity);
    }
}

pub struct SstableBuilderOutput<WO> {
    pub sst_info: LocalSstableInfo,
    pub writer_output: WO,
    pub stats: SstableBuilderOutputStats,
}

pub struct SstableBuilder<W: SstableWriter, F: FilterBuilder> {
    /// Options.
    options: SstableBuilderOptions,
    /// Data writer.
    writer: W,
    /// Current block builder.
    block_builder: BlockBuilder,

    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,

    /// `table_id` of added keys.
    table_ids: BTreeSet<TableId>,
    last_full_key: Vec<u8>,
    /// Buffer for encoded key and value to avoid allocation.
    raw_key: BytesMut,
    raw_value: BytesMut,
    last_table_id: Option<TableId>,
    sst_object_id: HummockSstableObjectId,

    /// Per table stats.
    table_stats: TableStatsMap,
    /// `last_table_stats` accumulates stats for `last_table_id` and finalizes it in `table_stats`
    /// by `finalize_last_table_stats`
    last_table_stats: TableStats,

    filter_builder: F,
    partitioned_meta_builder: PartitionedMetaBuilder,

    epoch_set: BTreeSet<u64>,
    memory_limiter: Option<Arc<MemoryLimiter>>,

    block_size_vec: Vec<usize>, // for statistics
    vnode_range_collector: Option<VnodeUserKeyRangeCollector>,
}

impl<W: SstableWriter> SstableBuilder<W, Xor16FilterBuilder> {
    pub fn for_test(
        sstable_id: u64,
        writer: W,
        options: SstableBuilderOptions,
        table_id_to_vnode: HashMap<impl Into<TableId>, usize>,
        table_id_to_watermark_serde: HashMap<
            impl Into<TableId>,
            Option<(OrderedRowSerde, OrderedRowSerde, usize)>,
        >,
    ) -> Self {
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
            table_id_to_vnode
                .into_iter()
                .map(|(table_id, v)| (table_id.into(), v))
                .collect(),
            table_id_to_watermark_serde
                .into_iter()
                .map(|(table_id, v)| (table_id.into(), v))
                .collect(),
            HashMap::default(),
        ));

        Self::new(
            sstable_id,
            writer,
            Xor16FilterBuilder::create(options.filter_builder_options()),
            options,
            compaction_catalog_agent_ref,
            None,
        )
    }
}

impl<W: SstableWriter, F: FilterBuilder> SstableBuilder<W, F> {
    pub fn new(
        sst_object_id: impl Into<HummockSstableObjectId>,
        writer: W,
        filter_builder: F,
        options: SstableBuilderOptions,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        memory_limiter: Option<Arc<MemoryLimiter>>,
    ) -> Self {
        let sst_object_id = sst_object_id.into();
        Self {
            vnode_range_collector: VnodeUserKeyRangeCollector::with_limit(
                options.max_vnode_key_range_bytes,
            ),
            options: options.clone(),
            writer,
            block_builder: BlockBuilder::new(BlockBuilderOptions {
                capacity: options.block_capacity,
                restart_interval: options.restart_interval,
                compression_algorithm: options.compression_algorithm,
            }),
            filter_builder,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            table_ids: BTreeSet::new(),
            last_table_id: None,
            raw_key: BytesMut::new(),
            raw_value: BytesMut::new(),
            last_full_key: vec![],
            sst_object_id,
            compaction_catalog_agent_ref,
            table_stats: Default::default(),
            last_table_stats: Default::default(),
            partitioned_meta_builder: PartitionedMetaBuilder::new(
                options.partitioned_meta_block_count,
                options.capacity,
            ),
            epoch_set: BTreeSet::default(),
            memory_limiter,
            block_size_vec: Vec::new(),
        }
    }

    /// Add kv pair to sstable.
    pub async fn add_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> HummockResult<()> {
        self.add(full_key, value).await
    }

    pub fn current_block_size(&self) -> usize {
        self.block_builder.approximate_len()
    }

    pub fn can_add_raw_meta_shard(&self, block_count: usize) -> bool {
        self.block_builder.is_empty()
            && self
                .partitioned_meta_builder
                .can_add_raw_shard(self.block_metas.len(), block_count)
    }

    fn record_raw_block_stats(&mut self, block_meta: &BlockMeta, block_len: usize) {
        self.last_table_stats.total_key_count += block_meta.total_key_count as i64;
        self.epoch_set.insert(
            FullKey::decode(&block_meta.smallest_key)
                .epoch_with_gap
                .pure_epoch(),
        );
        self.block_size_vec.push(block_len);
    }

    pub async fn add_raw_meta_shard(
        &mut self,
        blocks: Vec<(Bytes, BlockMeta)>,
        filter_data: Vec<u8>,
        largest_key: Vec<u8>,
    ) -> HummockResult<bool> {
        let block_count = blocks.len();
        if !self.can_add_raw_meta_shard(block_count) {
            return Ok(false);
        }

        let table_id = blocks
            .first()
            .expect("raw meta shard should not be empty")
            .1
            .table_id();
        if blocks.iter().any(|(_, meta)| meta.table_id() != table_id) {
            return Ok(false);
        }

        if self.last_table_id != Some(table_id) {
            self.table_ids.insert(table_id);
            self.finalize_last_table_stats();
            self.last_table_id = Some(table_id);
        }

        self.last_full_key = largest_key;
        for (block, mut meta) in blocks {
            self.record_raw_block_stats(&meta, block.len());
            meta.offset = self.writer.data_len() as u32;
            meta.len = block.len() as u32;
            self.block_metas.push(meta);
            let block_meta = self.block_metas.last_mut().unwrap();
            self.writer.write_block_bytes(block, block_meta).await?;
        }
        self.epoch_set.insert(
            FullKey::decode(&self.last_full_key)
                .epoch_with_gap
                .pure_epoch(),
        );
        self.partitioned_meta_builder.add_raw_shard(
            self.block_metas.len(),
            block_count,
            filter_data,
        );

        Ok(true)
    }

    /// Add kv pair to sstable.
    pub async fn add(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> HummockResult<()> {
        self.add_impl(full_key, value, true).await
    }

    /// Add kv pair to sstable.
    async fn add_impl(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        could_switch_block: bool,
    ) -> HummockResult<()> {
        const LARGE_KEY_LEN: usize = MAX_KEY_LEN >> 1;

        let table_key_len = full_key.user_key.table_key.as_ref().len();
        let table_value_len = match &value {
            HummockValue::Put(t) => t.len(),
            HummockValue::Delete => 0,
        };
        let large_value_len = self.options.max_sst_size as usize / 10;
        let large_key_value_len = self.options.max_sst_size as usize / 2;
        if table_key_len >= LARGE_KEY_LEN
            || table_value_len > large_value_len
            || table_key_len + table_value_len > large_key_value_len
        {
            let table_id = full_key.user_key.table_id;
            tracing::warn!(
                "A large key/value (table_id={}, key len={}, value len={}, epoch={}, spill offset={}) is added to block",
                table_id,
                table_key_len,
                table_value_len,
                full_key.epoch_with_gap.pure_epoch(),
                full_key.epoch_with_gap.offset(),
            );
        }

        // TODO: refine me
        full_key.encode_into(&mut self.raw_key);
        value.encode(&mut self.raw_value);
        let is_new_user_key = self.last_full_key.is_empty()
            || !user_key(&self.raw_key).eq(user_key(self.last_full_key.as_slice()));
        let table_id = full_key.user_key.table_id;
        let is_new_table = self.last_table_id != Some(table_id);
        let current_block_size = self.current_block_size();
        let is_block_full = current_block_size >= self.options.block_capacity
            || (current_block_size > self.options.block_capacity / 4 * 3
                && current_block_size + self.raw_value.len() + self.raw_key.len()
                    > self.options.block_capacity);

        if is_new_table {
            assert!(
                could_switch_block,
                "is_new_user_key {} sst_id {} block_idx {} table_id {} last_table_id {:?} full_key {:?}",
                is_new_user_key,
                self.sst_object_id,
                self.block_metas.len(),
                table_id,
                self.last_table_id,
                full_key
            );
            self.table_ids.insert(table_id);
            self.finalize_last_table_stats();
            self.last_table_id = Some(table_id);
            if !self.block_builder.is_empty() {
                self.build_block().await?;
            }
            self.partitioned_meta_builder.maybe_finish_shard(
                self.block_metas.len(),
                true,
                self.memory_limiter.clone(),
            );
        } else if is_block_full && could_switch_block {
            self.build_block().await?;
        }
        self.last_table_stats.total_key_count += 1;
        self.epoch_set.insert(full_key.epoch_with_gap.pure_epoch());

        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_empty() {
            let smallest_key = if let Some(threshold) =
                self.options.shorten_block_meta_key_threshold
                && !self.last_full_key.is_empty()
                && full_key.encoded_len() >= threshold
            {
                let prev = FullKey::decode(&self.last_full_key);
                if let Some(shortened) = try_shorten_block_smallest_key(&prev, &full_key) {
                    shortened.encode()
                } else {
                    full_key.encode()
                }
            } else {
                full_key.encode()
            };

            self.block_metas.push(BlockMeta {
                offset: utils::checked_into_u32(self.writer.data_len()).unwrap_or_else(|_| {
                    panic!(
                        "WARN overflow can't convert writer_data_len {} into u32 sst_id {} block_idx {} tables {:?}",
                        self.writer.data_len(),
                        self.sst_object_id,
                        self.block_metas.len(),
                        self.table_ids,
                    )
                }),
                len: 0,
                smallest_key,
                uncompressed_size: 0,
                total_key_count: 0,
                stale_key_count: 0,
            });
        }

        let filter_key = self
            .compaction_catalog_agent_ref
            .extract(user_key(&self.raw_key));
        // Add SST filter check.
        if !filter_key.is_empty() {
            self.partitioned_meta_builder
                .add_key(filter_key, table_id.as_raw_id());
        }
        // Use pre-encoded key to avoid redundant encoding
        self.block_builder.add(
            table_id,
            &self.raw_key[TABLE_PREFIX_LEN..],
            self.raw_value.as_ref(),
        );
        self.block_metas.last_mut().unwrap().total_key_count += 1;
        if !is_new_user_key || value.is_delete() {
            self.block_metas.last_mut().unwrap().stale_key_count += 1;
        }
        self.last_table_stats.total_key_size += full_key.encoded_len() as i64;
        self.last_table_stats.total_value_size += value.encoded_len() as i64;

        if let Some(collector) = self.vnode_range_collector.as_mut() {
            collector.observe_key(
                VirtualNode::from_index(full_key.user_key.get_vnode_id()),
                &self.raw_key,
                &self.last_full_key,
            );
        }

        self.last_full_key.clear();
        self.last_full_key.extend_from_slice(&self.raw_key);

        self.raw_key.clear();
        self.raw_value.clear();
        Ok(())
    }

    /// Finish building sst.
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub async fn finish(mut self) -> HummockResult<SstableBuilderOutput<W::Output>> {
        let smallest_key = if self.block_metas.is_empty() {
            vec![]
        } else {
            self.block_metas[0].smallest_key.clone()
        };
        let largest_key = self.last_full_key.clone();
        self.finalize_last_table_stats();

        // Vnode key-range hints are only supported for single-table SSTs.
        // Multi-table SST scenarios should not enable max_vnode_key_range_bytes in config.
        assert!(
            self.table_ids.len() <= 1 || self.vnode_range_collector.is_none(),
            "vnode key-range hints are only supported for single-table SSTs, found {} tables",
            self.table_ids.len()
        );

        self.build_block().await?;
        self.partitioned_meta_builder.maybe_finish_shard(
            self.block_metas.len(),
            true,
            self.memory_limiter.clone(),
        );
        let right_exclusive = false;
        let data_end_offset = self.writer.data_len() as u64;

        let total_key_count = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.total_key_count as u64)
            .sum::<u64>();
        let stale_key_count = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.stale_key_count as u64)
            .sum::<u64>();
        let uncompressed_file_size = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.uncompressed_size as u64)
            .sum::<u64>();
        let key_count = utils::checked_into_u32(total_key_count).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert total_key_count {} into u32 tables {:?}",
                total_key_count, self.table_ids,
            )
        });

        let partitioned_meta_builder = self.partitioned_meta_builder;
        let (
            meta,
            partitioned_index,
            partitioned_meta_shards,
            partitioned_encoded_meta,
            filter_size,
            metadata_encoded_size,
        ) = {
            assert_eq!(
                partitioned_meta_builder.shard_filters.len(),
                partitioned_meta_builder.shard_block_counts.len()
            );
            let mut shard_payload = Vec::new();
            let mut shards = Vec::with_capacity(partitioned_meta_builder.shard_filters.len());
            let mut meta_shards = Vec::with_capacity(partitioned_meta_builder.shard_filters.len());
            let mut first_block_idx = 0;
            let mut total_filter_size = 0;

            for (shard_idx, (filter, block_count)) in partitioned_meta_builder
                .shard_filters
                .into_iter()
                .zip(partitioned_meta_builder.shard_block_counts.into_iter())
                .enumerate()
            {
                let end_block_idx = first_block_idx + block_count;
                let shard = MetaShard {
                    shard_idx: shard_idx as u32,
                    first_block_idx: first_block_idx as u32,
                    block_metas: self.block_metas[first_block_idx..end_block_idx].to_vec(),
                    filter_type: PbSstableFilterType::SstableFilterXor16 as u32,
                    filter,
                };
                total_filter_size += shard.filter.len();
                let shard_body = shard.encode_body_to_bytes();
                let offset = data_end_offset + shard_payload.len() as u64;
                let checksum = super::xxhash64_checksum(&shard_body);
                let len = match utils::checked_into_u32(shard_body.len()) {
                    Ok(len) => len,
                    Err(_) => panic!(
                        "WARN overflow can't convert meta_shard_body_len {} into u32 tables {:?}",
                        shard_body.len(),
                        self.table_ids,
                    ),
                };
                shard_payload.extend_from_slice(&shard_body);
                shards.push(MetaShardDesc {
                    shard_idx: shard_idx as u32,
                    first_block_idx: first_block_idx as u32,
                    block_count: block_count as u32,
                    smallest_key: self.block_metas[first_block_idx].smallest_key.clone(),
                    offset,
                    len,
                    checksum,
                });
                meta_shards.push(shard);
                first_block_idx = end_block_idx;
            }
            assert_eq!(first_block_idx, self.block_metas.len());

            let meta_offset = data_end_offset + shard_payload.len() as u64;
            let mut index = MetaPartitionIndex {
                estimated_size: 0,
                key_count,
                smallest_key: smallest_key.clone(),
                largest_key: largest_key.clone(),
                block_count: self.block_metas.len() as u32,
                shard_count: shards.len() as u32,
                filter_type: PbSstableFilterType::SstableFilterXor16 as u32,
                shard_policy: META_SHARD_POLICY_FIXED_BLOCK_COUNT as u32,
                shards,
            };
            let index_encoded_size = index.encoded_size();
            let file_size =
                data_end_offset + shard_payload.len() as u64 + index_encoded_size as u64;
            index.estimated_size = utils::checked_into_u32(file_size).unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert partitioned file_size {} into u32 tables {:?}",
                    file_size, self.table_ids,
                )
            });
            let mut encoded_meta = shard_payload;
            index.encode_to(&mut encoded_meta);
            let metadata_encoded_size = encoded_meta.len();

            #[expect(deprecated)]
            let meta = SstableMeta {
                block_metas: vec![],
                bloom_filter: vec![],
                estimated_size: index.estimated_size,
                key_count,
                smallest_key,
                largest_key,
                version: PARTITIONED_META_VERSION,
                meta_offset,
                monotonic_tombstone_events: vec![],
            };

            (
                meta,
                index,
                meta_shards,
                encoded_meta,
                total_filter_size,
                metadata_encoded_size,
            )
        };

        let (avg_key_size, avg_value_size) = if self.table_stats.is_empty() {
            (0, 0)
        } else {
            let total_key_count: usize = self
                .table_stats
                .values()
                .map(|s| s.total_key_count as usize)
                .sum();

            if total_key_count == 0 {
                (0, 0)
            } else {
                let total_key_size: usize = self
                    .table_stats
                    .values()
                    .map(|s| s.total_key_size as usize)
                    .sum();

                let total_value_size: usize = self
                    .table_stats
                    .values()
                    .map(|s| s.total_value_size as usize)
                    .sum();

                (
                    total_key_size / total_key_count,
                    total_value_size / total_key_count,
                )
            }
        };

        let (min_epoch, max_epoch) = {
            if self.epoch_set.is_empty() {
                (HummockEpoch::MAX, u64::MIN)
            } else {
                (
                    *self.epoch_set.first().unwrap(),
                    *self.epoch_set.last().unwrap(),
                )
            }
        };

        let vnode_user_key_ranges = self
            .vnode_range_collector
            .take()
            .and_then(|collector| collector.finish(&self.last_full_key));

        let sst_info: SstableInfo = SstableInfoInner {
            object_id: self.sst_object_id,
            // use the same sst_id as object_id for initial sst
            sst_id: self.sst_object_id.as_raw_id().into(),
            bloom_filter_kind: BloomFilterType::Sstable,
            key_range: KeyRange {
                left: Bytes::from(meta.smallest_key.clone()),
                right: Bytes::from(meta.largest_key.clone()),
                right_exclusive,
            },
            file_size: meta.estimated_size as u64,
            table_ids: self.table_ids.into_iter().collect(),
            meta_offset: meta.meta_offset,
            stale_key_count,
            total_key_count,
            uncompressed_file_size: uncompressed_file_size + metadata_encoded_size as u64,
            min_epoch,
            max_epoch,
            range_tombstone_count: 0,
            sst_size: meta.estimated_size as u64,
            filter_type: PbSstableFilterType::SstableFilterXor16,
            vnode_statistics: vnode_user_key_ranges,
        }
        .into();

        tracing::trace!(
            "meta_size {} filter_size {} add_key_counts {} stale_key_count {} min_epoch {} max_epoch {} epoch_count {}",
            metadata_encoded_size,
            filter_size,
            total_key_count,
            stale_key_count,
            min_epoch,
            max_epoch,
            self.epoch_set.len()
        );
        let sstable_file_size = sst_info.file_size as usize;

        if !self.block_metas.is_empty() {
            // fill total_compressed_size
            let mut last_table_id = self.block_metas[0].table_id();
            let mut last_table_stats = self.table_stats.get_mut(&last_table_id).unwrap();
            for block_meta in &self.block_metas {
                let block_table_id = block_meta.table_id();
                if last_table_id != block_table_id {
                    last_table_id = block_table_id;
                    last_table_stats = self.table_stats.get_mut(&last_table_id).unwrap();
                }

                last_table_stats.total_compressed_size += block_meta.len as u64;
            }
        }

        let writer_output = self
            .writer
            .finish_partitioned_meta(
                meta,
                partitioned_index,
                partitioned_meta_shards,
                partitioned_encoded_meta,
            )
            .await?;
        // The timestamp is only used during full GC.
        //
        // Ideally object store object's last_modified should be used.
        // However, it'll incur additional IO overhead since S3 lacks an interface to retrieve the last_modified timestamp after the PUT operation on an object.
        //
        // The local timestamp below is expected to precede the last_modified of object store object, given that the object store object is created afterward.
        // It should help alleviate the clock drift issue.

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        Ok(SstableBuilderOutput::<W::Output> {
            sst_info: LocalSstableInfo::new(sst_info, self.table_stats, now),
            writer_output,
            stats: SstableBuilderOutputStats {
                filter_size,
                avg_key_size,
                avg_value_size,
                epoch_count: self.epoch_set.len(),
                block_size_vec: self.block_size_vec,
                sstable_file_size,
            },
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.writer.data_len()
            + self.block_builder.approximate_len()
            + self.filter_builder.approximate_len()
            + self.partitioned_meta_builder.approximate_len()
    }

    pub async fn build_block(&mut self) -> HummockResult<()> {
        // Skip empty block.
        if self.block_builder.is_empty() {
            return Ok(());
        }

        let block_meta = self.block_metas.last_mut().unwrap();
        let uncompressed_block_size = self.block_builder.uncompressed_block_size();
        block_meta.uncompressed_size = utils::checked_into_u32(uncompressed_block_size)
            .unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert uncompressed_block_size {} into u32 table {:?}",
                    uncompressed_block_size,
                    self.block_builder.table_id(),
                )
            });
        let block = self.block_builder.build();
        self.writer.write_block(block, block_meta).await?;
        self.block_size_vec.push(block.len());
        let data_len = utils::checked_into_u32(self.writer.data_len()).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert writer_data_len {} into u32 table {:?}",
                self.writer.data_len(),
                self.block_builder.table_id(),
            )
        });
        block_meta.len = data_len.checked_sub(block_meta.offset).unwrap_or_else(|| {
            panic!(
                "data_len should >= meta_offset, found data_len={}, meta_offset={}",
                data_len, block_meta.offset
            )
        });

        self.filter_builder
            .switch_block(self.memory_limiter.clone());

        if data_len as usize > self.options.capacity * 2 {
            tracing::warn!(
                "WARN unexpected block size {} table {:?}",
                data_len,
                self.block_builder.table_id()
            );
        }

        self.block_builder.clear();
        self.partitioned_meta_builder.maybe_finish_shard(
            self.block_metas.len(),
            false,
            self.memory_limiter.clone(),
        );
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.writer.data_len() > 0
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }

    fn finalize_last_table_stats(&mut self) {
        if self.table_ids.is_empty() || self.last_table_id.is_none() {
            return;
        }
        self.table_stats.insert(
            self.last_table_id.unwrap(),
            std::mem::take(&mut self.last_table_stats),
        );
    }
}

/// Collects vnode key-range hints during SST building.
struct VnodeUserKeyRangeCollector {
    max_bytes: usize,
    current_size: usize,
    ranges: BTreeMap<VirtualNode, (UserKey<Bytes>, UserKey<Bytes>)>,
    current_vnode: VirtualNode,
    range_start_key: Vec<u8>,
}

impl VnodeUserKeyRangeCollector {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_size: 0,
            ranges: BTreeMap::new(),
            current_vnode: VirtualNode::ZERO,
            range_start_key: Vec::new(),
        }
    }

    fn with_limit(max_bytes: Option<usize>) -> Option<Self> {
        max_bytes.filter(|&n| n > 0).map(Self::new)
    }

    /// Track vnode boundaries. On vnode switch, seals previous range with `prev_key` as right bound.
    /// Range: `[first_key_of_vnode, last_key_of_vnode]` (inclusive).
    fn observe_key(&mut self, vnode: VirtualNode, key: &[u8], prev_key: &[u8]) {
        if self.current_size >= self.max_bytes {
            return;
        }

        // First key
        if self.range_start_key.is_empty() {
            self.current_vnode = vnode;
            self.range_start_key = key.to_vec();
            return;
        }

        // Same vnode, nothing to do
        if vnode == self.current_vnode {
            return;
        }

        // Vnode changed: seal previous range
        self.seal_range(prev_key);

        // Check if budget exhausted after sealing
        if self.current_size >= self.max_bytes {
            return;
        }

        // Start new vnode
        self.current_vnode = vnode;
        self.range_start_key = key.to_vec();
    }

    /// Seal current range. Asserts `vnode/table_id` consistency between left and right keys.
    fn seal_range(&mut self, right_key: &[u8]) {
        let left_key = mem::take(&mut self.range_start_key);
        self.current_size += mem::size_of::<VirtualNode>() + left_key.len() + right_key.len();

        let left_full_key = FullKey::decode(&left_key);
        let right_full_key = FullKey::decode(right_key);
        let left_user_key = left_full_key.user_key.copy_into();
        let right_user_key = right_full_key.user_key.copy_into();

        // Sanity checks for data correctness:
        // 1. left and right keys have same `vnode`
        // 2. vnode matches `current_vnode` being tracked
        // 3. left and right keys have same `table_id`
        assert_eq!(
            left_user_key.get_vnode_id(),
            right_user_key.get_vnode_id(),
            "vnode changed within range: left_user {:?}, right_user {:?}",
            left_user_key,
            right_user_key
        );
        assert_eq!(
            left_user_key.get_vnode_id(),
            self.current_vnode.to_index(),
            "vnode mismatch: left {:?}, right {:?}, expected vnode {}",
            left_user_key,
            right_user_key,
            self.current_vnode.to_index()
        );
        assert_eq!(
            left_user_key.table_id, right_user_key.table_id,
            "table_id changed within range: left {:?}, right {:?}",
            left_user_key, right_user_key
        );

        self.ranges
            .insert(self.current_vnode, (left_user_key, right_user_key));
    }

    /// Returns `Some` if >1 vnodes collected, `None` otherwise (single-vnode needs no hints).
    fn finish(mut self, last_key: &[u8]) -> Option<VnodeStatistics> {
        if !self.range_start_key.is_empty() {
            self.seal_range(last_key);
        }

        if self.ranges.len() > 1 {
            // Validate all ranges belong to the same table_id before building VnodeStatistics
            let mut table_ids = self
                .ranges
                .values()
                .flat_map(|(left, right)| [left.table_id, right.table_id]);
            if let Some(first_table_id) = table_ids.next() {
                for table_id in table_ids {
                    assert_eq!(
                        table_id, first_table_id,
                        "all vnode ranges must belong to the same table_id, found {:?} and {:?}",
                        table_id, first_table_id
                    );
                }
            }

            Some(VnodeStatistics::from_map(self.ranges))
        } else {
            None
        }
    }
}

pub struct SstableBuilderOutputStats {
    filter_size: usize,
    avg_key_size: usize,
    avg_value_size: usize,
    epoch_count: usize,
    block_size_vec: Vec<usize>, // for statistics
    sstable_file_size: usize,
}

impl SstableBuilderOutputStats {
    pub fn report_stats(&self, metrics: &Arc<CompactorMetrics>) {
        if self.filter_size != 0 {
            metrics
                .sstable_bloom_filter_size
                .observe(self.filter_size as _);
        }

        if self.sstable_file_size != 0 {
            metrics
                .sstable_file_size
                .observe(self.sstable_file_size as _);
        }

        if self.avg_key_size != 0 {
            metrics.sstable_avg_key_size.observe(self.avg_key_size as _);
        }

        if self.avg_value_size != 0 {
            metrics
                .sstable_avg_value_size
                .observe(self.avg_value_size as _);
        }

        if self.epoch_count != 0 {
            metrics
                .sstable_distinct_epoch_count
                .observe(self.epoch_count as _);
        }

        if !self.block_size_vec.is_empty() {
            for block_size in &self.block_size_vec {
                metrics.sstable_block_size.observe(*block_size as _);
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::{Bound, HashMap};

    use risingwave_common::catalog::{ColumnDesc, TableId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::row_serde::OrderedRowSerde;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::UserKey;
    use risingwave_pb::catalog::table::{PbEngine, TableType};
    use risingwave_pb::catalog::{PbCreateType, PbStreamJobStatus, PbTable};
    use risingwave_pb::common::{PbColumnOrder, PbDirection, PbNullsAre, PbOrderType};
    use risingwave_pb::plan_common::ColumnCatalog as PbColumnCatalog;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::compaction_catalog_manager::{
        CompactionCatalogAgent, DummyFilterKeyExtractor, FilterKeyExtractorImpl,
        MultiFilterKeyExtractor,
    };
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::xor_filter::BlockedXor16FilterBuilder;
    use crate::hummock::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_test_sstable_impl, mock_sst_writer,
        test_key_of, test_value_of,
    };
    use crate::hummock::{
        CachePolicy, PartitionedSstableMetaHolder, ReadOptions, Sstable, SstableStoreRef,
        SstableWriterOptions, Xor8FilterBuilder, get_from_sstable_info,
        hit_sstable_filter_with_partitioned_meta,
    };
    use crate::monitor::StoreLocalStatistic;

    fn partitioned_meta_schema_filter_test_table(table_id: TableId) -> PbTable {
        PbTable {
            id: table_id,
            schema_id: 0.into(),
            database_id: 0.into(),
            name: "test".to_owned(),
            table_type: TableType::Table as i32,
            columns: vec![
                PbColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc::named("pk_0", 0.into(), DataType::Int64)).into(),
                    ),
                    is_hidden: false,
                },
                PbColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc::named("pk_1", 1.into(), DataType::Int64)).into(),
                    ),
                    is_hidden: false,
                },
            ],
            pk: vec![
                PbColumnOrder {
                    column_index: 0,
                    order_type: Some(PbOrderType {
                        direction: PbDirection::Ascending as _,
                        nulls_are: PbNullsAre::Largest as _,
                    }),
                },
                PbColumnOrder {
                    column_index: 1,
                    order_type: Some(PbOrderType {
                        direction: PbDirection::Ascending as _,
                        nulls_are: PbNullsAre::Largest as _,
                    }),
                },
            ],
            stream_key: vec![0],
            distribution_key: vec![0],
            value_indices: vec![0, 1],
            read_prefix_len_hint: 1,
            stream_job_status: PbStreamJobStatus::Created.into(),
            create_type: PbCreateType::Foreground.into(),
            engine: Some(PbEngine::Hummock as i32),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_empty() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.001,
            ..Default::default()
        };

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let b = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        b.finish().await.unwrap();
    }

    fn encode_full_key(vnode: VirtualNode, table_key_suffix: &[u8]) -> Vec<u8> {
        let mut table_key = vnode.to_be_bytes().to_vec();
        table_key.extend_from_slice(table_key_suffix);
        FullKey::for_test(TableId::default(), table_key, 0).encode()
    }

    fn table_key_of(vnode: VirtualNode, suffix: &[u8]) -> Vec<u8> {
        let mut key = vnode.to_be_bytes().to_vec();
        key.extend_from_slice(suffix);
        key
    }

    #[test]
    fn test_vnode_user_key_range_basic_collection() {
        // Test basic multi-vnode collection with boundary semantics verification.
        // Validates: vnode switching triggers range sealing, boundaries are inclusive (right_exclusive=false).
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        let vnode_1 = VirtualNode::from_index(1);
        let vnode_2 = VirtualNode::from_index(2);

        let k1 = encode_full_key(vnode_1, b"k1");
        let k2 = encode_full_key(vnode_1, b"k2");
        let k3 = encode_full_key(vnode_2, b"k3");
        let k4 = encode_full_key(vnode_2, b"k4");

        collector.observe_key(vnode_1, &k1, &[]);
        collector.observe_key(vnode_1, &k2, &k1);
        collector.observe_key(vnode_2, &k3, &k2);
        collector.observe_key(vnode_2, &k4, &k3);

        let info = collector.finish(&k4).unwrap();
        assert_eq!(info.vnode_user_key_ranges().len(), 2);

        // Verify vnode_1: left = first key, right = last key before switch
        let (range1_left, range1_right) = info.get_vnode_user_key_range(vnode_1).unwrap();
        assert_eq!(range1_left.table_key.as_ref(), table_key_of(vnode_1, b"k1"));
        assert_eq!(
            range1_right.table_key.as_ref(),
            table_key_of(vnode_1, b"k2")
        );

        // Verify vnode_2: left = first key, right = SST's last key
        let (range2_left, range2_right) = info.get_vnode_user_key_range(vnode_2).unwrap();
        assert_eq!(range2_left.table_key.as_ref(), table_key_of(vnode_2, b"k3"));
        assert_eq!(
            range2_right.table_key.as_ref(),
            table_key_of(vnode_2, b"k4")
        );
    }

    #[test]
    fn test_vnode_user_key_range_capacity_limit() {
        // Test "allow over-limit write" semantics: inserting a range may exceed capacity,
        // but stops before starting the next range if it would exceed the limit.
        //
        // Calculation based on encoded key sizes:
        // Each range = sizeof(VirtualNode) + left.len() + right.len().
        // Use a limit that:
        //   - allows writing vnode_1 (range_size),
        //   - allows over-limit write of vnode_2 (range_size + estimated_next),
        //   - stops before vnode_3 (2 * range_size + estimated_next > limit).
        let vnode_1 = VirtualNode::from_index(1);
        let vnode_2 = VirtualNode::from_index(2);
        let vnode_3 = VirtualNode::from_index(3);
        let vnode_4 = VirtualNode::from_index(4);

        let k1 = encode_full_key(vnode_1, b"k1");
        let k2 = encode_full_key(vnode_1, b"k2");
        let k3 = encode_full_key(vnode_2, b"k3");
        let k4 = encode_full_key(vnode_2, b"k4");
        let k5 = encode_full_key(vnode_3, b"k5");
        let k6 = encode_full_key(vnode_3, b"k6");
        let k7 = encode_full_key(vnode_4, b"k7");

        let range_size = mem::size_of::<VirtualNode>() + k1.len() + k2.len();
        let estimated_next_size = mem::size_of::<VirtualNode>() + k3.len();
        let limit = range_size + estimated_next_size; // allow second range, stop before third
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(limit)).unwrap();

        collector.observe_key(vnode_1, &k1, &[]);
        collector.observe_key(vnode_1, &k2, &k1);
        collector.observe_key(vnode_2, &k3, &k2); // Seals vnode_1 (6 bytes)
        collector.observe_key(vnode_2, &k4, &k3);
        collector.observe_key(vnode_3, &k5, &k4); // Seals vnode_2 (12 bytes total), stops before vnode_3
        collector.observe_key(vnode_3, &k6, &k5); // Ignored
        collector.observe_key(vnode_4, &k7, &k6); // Ignored

        let info = collector.finish(&k7).unwrap();
        assert_eq!(
            info.vnode_user_key_ranges().len(),
            2,
            "Should collect exactly 2 vnodes"
        );

        // Verify collected vnodes have correct boundaries
        let (range1_left, range1_right) = info.get_vnode_user_key_range(vnode_1).unwrap();
        assert_eq!(range1_left.table_key.as_ref(), table_key_of(vnode_1, b"k1"));
        assert_eq!(
            range1_right.table_key.as_ref(),
            table_key_of(vnode_1, b"k2")
        );

        let (range2_left, range2_right) = info.get_vnode_user_key_range(vnode_2).unwrap();
        assert_eq!(range2_left.table_key.as_ref(), table_key_of(vnode_2, b"k3"));
        assert_eq!(
            range2_right.table_key.as_ref(),
            table_key_of(vnode_2, b"k4")
        );

        // Verify stopped vnodes are not collected
        assert!(info.get_vnode_user_key_range(vnode_3).is_none());
        assert!(info.get_vnode_user_key_range(vnode_4).is_none());
    }

    #[test]
    fn test_vnode_user_key_range_sparse_distribution() {
        // Test non-consecutive vnodes (production scenario: hash-based data distribution).
        // Validates: collector handles sparse vnode indices correctly, gaps are not filled.
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        let vnode_5 = VirtualNode::from_index(5);
        let vnode_10 = VirtualNode::from_index(10);
        let vnode_100 = VirtualNode::from_index(100);

        let keys = vec![
            (vnode_5, encode_full_key(vnode_5, b"key_005_001")),
            (vnode_5, encode_full_key(vnode_5, b"key_005_002")),
            (vnode_5, encode_full_key(vnode_5, b"key_005_999")),
            (vnode_10, encode_full_key(vnode_10, b"key_010_001")),
            (vnode_10, encode_full_key(vnode_10, b"key_010_100")),
            (vnode_100, encode_full_key(vnode_100, b"key_100_001")),
            (vnode_100, encode_full_key(vnode_100, b"key_100_999")),
        ];

        let mut prev_key = Vec::new();
        for (vnode, key) in &keys {
            collector.observe_key(*vnode, key, &prev_key);
            prev_key = key.clone();
        }

        let info = collector.finish(&prev_key).unwrap();
        assert_eq!(info.vnode_user_key_ranges().len(), 3);

        // Verify each collected vnode has correct boundaries
        let (range5_left, range5_right) = info.get_vnode_user_key_range(vnode_5).unwrap();
        assert_eq!(
            range5_left.table_key.as_ref(),
            table_key_of(vnode_5, b"key_005_001")
        );
        assert_eq!(
            range5_right.table_key.as_ref(),
            table_key_of(vnode_5, b"key_005_999")
        );

        let (range10_left, range10_right) = info.get_vnode_user_key_range(vnode_10).unwrap();
        assert_eq!(
            range10_left.table_key.as_ref(),
            table_key_of(vnode_10, b"key_010_001")
        );
        assert_eq!(
            range10_right.table_key.as_ref(),
            table_key_of(vnode_10, b"key_010_100")
        );

        let (range100_left, range100_right) = info.get_vnode_user_key_range(vnode_100).unwrap();
        assert_eq!(
            range100_left.table_key.as_ref(),
            table_key_of(vnode_100, b"key_100_001")
        );
        assert_eq!(
            range100_right.table_key.as_ref(),
            table_key_of(vnode_100, b"key_100_999")
        );

        // Verify gaps are not filled (no data for vnodes 0, 7, 50)
        assert!(
            info.get_vnode_user_key_range(VirtualNode::from_index(0))
                .is_none()
        );
        assert!(
            info.get_vnode_user_key_range(VirtualNode::from_index(7))
                .is_none()
        );
        assert!(
            info.get_vnode_user_key_range(VirtualNode::from_index(50))
                .is_none()
        );
    }

    #[test]
    fn test_vnode_user_key_range_edge_cases() {
        // Test 1: Configuration disabled (None or 0) should return None
        assert!(VnodeUserKeyRangeCollector::with_limit(None).is_none());
        assert!(VnodeUserKeyRangeCollector::with_limit(Some(0)).is_none());

        // Test 2: Empty collector (no keys) should return None
        let collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        assert!(
            collector
                .finish(&encode_full_key(VirtualNode::ZERO, b"any"))
                .is_none()
        );

        // Test 3: Single vnode (optimization: don't emit hints for single-vnode SSTs)
        let vnode = VirtualNode::from_index(5);
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1024)).unwrap();
        let a = encode_full_key(vnode, b"a");
        let b = encode_full_key(vnode, b"b");
        let c = encode_full_key(vnode, b"c");
        collector.observe_key(vnode, &a, &[]);
        collector.observe_key(vnode, &b, &a);
        collector.observe_key(vnode, &c, &b);
        assert!(
            collector.finish(&c).is_none(),
            "Single-vnode SST should not emit hints"
        );

        // Test 4: Extreme limit (1 byte) - first vnode exceeds, becomes single-vnode
        // Range size = sizeof(VirtualNode=2) + encoded_key_len * 2
        // With 1-byte limit: vnode_1 range exceeds but allowed (over-limit write)
        // vnode_2 check: current_size >= max_bytes, stop. Result: only vnode_1, returns None (single-vnode)
        let mut collector = VnodeUserKeyRangeCollector::with_limit(Some(1)).unwrap();
        let vnode_1 = VirtualNode::from_index(1);
        let vnode_2 = VirtualNode::from_index(2);
        let key1 = encode_full_key(vnode_1, b"key1");
        let key2 = encode_full_key(vnode_1, b"key2");
        let key3 = encode_full_key(vnode_2, b"key3");
        collector.observe_key(vnode_1, &key1, &[]);
        collector.observe_key(vnode_1, &key2, &key1);
        collector.observe_key(vnode_2, &key3, &key2); // Seals vnode_1, stops before vnode_2
        assert!(
            collector.finish(&key3).is_none(),
            "Only 1 vnode collected, should return None"
        );
    }

    #[tokio::test]
    async fn test_basic() {
        let opt = default_builder_opt_for_test();

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let mut b = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        for i in 0..TEST_KEYS_COUNT {
            b.add_for_test(
                test_key_of(i).to_ref(),
                HummockValue::put(&test_value_of(i)),
            )
            .await
            .unwrap();
        }

        let output = b.finish().await.unwrap();
        let info = output.sst_info.sst_info;

        assert_bytes_eq!(test_key_of(0).encode(), info.key_range.left);
        assert_bytes_eq!(
            test_key_of(TEST_KEYS_COUNT - 1).encode(),
            info.key_range.right
        );
        let (data, meta) = output.writer_output;
        assert_eq!(info.file_size, meta.estimated_size as u64);
        let offset = info.meta_offset as usize;
        let index = MetaPartitionIndex::decode(&data[offset..]).unwrap();
        assert_eq!(index.estimated_size, meta.estimated_size);
        assert_eq!(index.key_count, meta.key_count);
        assert_bytes_eq!(index.smallest_key, meta.smallest_key);
        assert_bytes_eq!(index.largest_key, meta.largest_key);
    }

    #[tokio::test]
    async fn test_partitioned_meta_builder_output() {
        let mut opt = default_builder_opt_for_test();
        opt.block_capacity = 128;
        opt.partitioned_meta_block_count = 2;

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let mut builder = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        for i in 0..100 {
            builder
                .add_for_test(
                    test_key_of(i).to_ref(),
                    HummockValue::put(&test_value_of(i)),
                )
                .await
                .unwrap();
        }

        let output = builder.finish().await.unwrap();
        let info = output.sst_info.sst_info;
        let (data, meta) = output.writer_output;
        assert_eq!(meta.version, PARTITIONED_META_VERSION);
        assert!(meta.block_metas.is_empty());
        assert_eq!(info.meta_offset, meta.meta_offset);
        assert_eq!(info.file_size, meta.estimated_size as u64);

        let index = MetaPartitionIndex::decode(&data[info.meta_offset as usize..]).unwrap();
        assert_eq!(index.estimated_size as u64, info.file_size);
        assert_eq!(index.shard_count as usize, index.shards.len());
        assert!(index.shard_count > 1);

        let first_desc = &index.shards[0];
        let shard_body =
            &data[first_desc.offset as usize..first_desc.offset as usize + first_desc.len as usize];
        crate::hummock::sstable::xxhash64_verify(shard_body, first_desc.checksum).unwrap();
        let shard = MetaShard::decode_body(
            first_desc.shard_idx,
            first_desc.first_block_idx,
            first_desc.block_count,
            &first_desc.smallest_key,
            index.filter_type,
            shard_body,
        )
        .unwrap();
        assert_eq!(shard.block_metas.len(), first_desc.block_count as usize);
        assert_eq!(shard.block_metas.len(), 2);
        assert!(!shard.filter.is_empty());
    }

    #[tokio::test]
    async fn test_partitioned_meta_point_get() {
        let mut opts = default_builder_opt_for_test();
        opts.block_capacity = 128;
        opts.partitioned_meta_block_count = 2;

        let sstable_store = mock_sstable_store().await;
        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let sst_info = gen_test_sstable_impl::<_, Xor16FilterBuilder>(
            opts,
            1,
            (0..100).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )
        .await;

        let key = test_key_of(42);
        let encoded_key = key.encode();
        let dist_key_hash =
            Sstable::hash_for_filter(user_key(&encoded_key), key.user_key.table_id.as_raw_id());
        let table_holder = sstable_store
            .meta_index(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let user_key_range = (
            Bound::Included(key.to_ref().user_key),
            Bound::Included(key.to_ref().user_key),
        );
        let mut stats = StoreLocalStatistic::default();
        let read_options = ReadOptions::default();
        {
            let iter = get_from_sstable_info(
                sstable_store.clone(),
                &sst_info,
                key.to_ref(),
                &read_options,
                Some(dist_key_hash),
                &mut stats,
            )
            .await
            .unwrap()
            .unwrap();

            assert_eq!(iter.key(), key.to_ref());
            assert_bytes_eq!(iter.value().into_user_value().unwrap(), test_value_of(42));
        }
        assert!(stats.cache_meta_block_total >= 2);
        assert!(stats.bloom_filter_check_counts >= 1);
        assert_eq!(stats.partitioned_meta_index_cache_total, 1);
        assert_eq!(stats.partitioned_meta_index_cache_miss, 0);
        assert!(stats.partitioned_meta_shard_cache_total >= 1);
        assert_eq!(stats.partitioned_meta_shard_cache_miss, 0);
        assert_eq!(stats.partitioned_meta_shard_read_bytes, 0);
        assert_eq!(stats.partitioned_meta_shard_filter_positive_counts, 1);

        sstable_store.clear_meta_cache().await.unwrap();
        let mut cold_stats = StoreLocalStatistic::default();
        {
            let iter = get_from_sstable_info(
                sstable_store.clone(),
                &sst_info,
                key.to_ref(),
                &read_options,
                Some(dist_key_hash),
                &mut cold_stats,
            )
            .await
            .unwrap()
            .unwrap();

            assert_eq!(iter.key(), key.to_ref());
        }
        assert_eq!(cold_stats.partitioned_meta_index_cache_total, 1);
        assert_eq!(cold_stats.partitioned_meta_index_cache_miss, 1);
        assert!(cold_stats.partitioned_meta_shard_cache_total >= 1);
        assert!(cold_stats.partitioned_meta_shard_cache_miss >= 1);
        assert!(cold_stats.partitioned_meta_shard_read_bytes > 0);

        let mut hit_stats = StoreLocalStatistic::default();
        {
            let iter = get_from_sstable_info(
                sstable_store.clone(),
                &sst_info,
                key.to_ref(),
                &read_options,
                Some(dist_key_hash),
                &mut hit_stats,
            )
            .await
            .unwrap()
            .unwrap();

            assert_eq!(iter.key(), key.to_ref());
        }
        assert!(hit_stats.partitioned_meta_shard_cache_total >= 1);
        assert_eq!(hit_stats.partitioned_meta_shard_cache_miss, 0);
        assert_eq!(hit_stats.partitioned_meta_shard_read_bytes, 0);

        let mut negative_filter_stats = StoreLocalStatistic::default();
        assert!(
            !hit_sstable_filter_with_partitioned_meta(
                &sstable_store,
                &table_holder,
                &user_key_range,
                u64::MAX,
                &mut negative_filter_stats
            )
            .await
            .unwrap()
        );
        assert_eq!(negative_filter_stats.bloom_filter_true_negative_counts, 1);
        assert_eq!(
            negative_filter_stats.partitioned_meta_shard_filter_positive_counts,
            0
        );
        assert!(negative_filter_stats.partitioned_meta_shard_filter_negative_counts >= 1);

        let mut positive_filter_stats = StoreLocalStatistic::default();
        assert!(
            hit_sstable_filter_with_partitioned_meta(
                &sstable_store,
                &table_holder,
                &user_key_range,
                dist_key_hash,
                &mut positive_filter_stats
            )
            .await
            .unwrap()
        );
        assert_eq!(
            positive_filter_stats.partitioned_meta_shard_filter_positive_counts,
            1
        );

        for i in 0..100 {
            let data_key = test_key_of(i);
            let read_key = FullKey::from_user_key(data_key.user_key.clone(), test_epoch(2));
            let encoded_key = read_key.encode();
            let dist_key_hash = Sstable::hash_for_filter(
                user_key(&encoded_key),
                read_key.user_key.table_id.as_raw_id(),
            );
            let mut read_stats = StoreLocalStatistic::default();
            let iter = get_from_sstable_info(
                sstable_store.clone(),
                &sst_info,
                read_key.to_ref(),
                &read_options,
                Some(dist_key_hash),
                &mut read_stats,
            )
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("partitioned meta point get missed key {i}"));

            assert_eq!(iter.key().user_key, read_key.to_ref().user_key);
            assert_bytes_eq!(iter.value().into_user_value().unwrap(), test_value_of(i));
        }
    }

    #[tokio::test]
    async fn test_partitioned_meta_schema_prefix_filter_across_shards() {
        let table_id = TableId::new(233);
        let table = partitioned_meta_schema_filter_test_table(table_id);
        let mut opts = default_builder_opt_for_test();
        opts.block_capacity = 128;
        opts.partitioned_meta_block_count = 2;

        let sstable_store = mock_sstable_store().await;
        let writer = sstable_store
            .clone()
            .create_sst_writer(10, SstableWriterOptions::default());
        let mut multi_filter = MultiFilterKeyExtractor::default();
        multi_filter.register(table_id, FilterKeyExtractorImpl::from_table(&table));
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(multi_filter),
            HashMap::from_iter([(table_id, VirtualNode::COUNT_FOR_TEST)]),
            HashMap::from_iter([(table_id, None)]),
            HashMap::default(),
        ));
        let mut builder = SstableBuilder::new(
            10,
            writer,
            Xor16FilterBuilder::new(1024),
            opts,
            compaction_catalog_agent_ref,
            None,
        );

        let pk_serde = OrderedRowSerde::new(
            vec![DataType::Int64, DataType::Int64],
            vec![OrderType::ascending(), OrderType::ascending()],
        );
        let mut prefix_hint = vec![];
        OrderedRowSerde::new(vec![DataType::Int64], vec![OrderType::ascending()]).serialize(
            &OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
            &mut prefix_hint,
        );

        let make_key = |prefix: i64, suffix: i64| {
            let mut pk = vec![];
            pk_serde.serialize(
                &OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(prefix)),
                    Some(ScalarImpl::Int64(suffix)),
                ]),
                &mut pk,
            );
            let table_key = [VirtualNode::ZERO.to_be_bytes().as_slice(), pk.as_slice()].concat();
            FullKey::from_user_key(UserKey::for_test(table_id, table_key), test_epoch(1))
        };

        for suffix in 0..200 {
            builder
                .add(
                    make_key(7, suffix).to_ref(),
                    HummockValue::put(format!("value-{suffix}").as_bytes()),
                )
                .await
                .unwrap();
        }
        for suffix in 0..200 {
            builder
                .add(
                    make_key(8, suffix).to_ref(),
                    HummockValue::put(format!("other-{suffix}").as_bytes()),
                )
                .await
                .unwrap();
        }

        let output = builder.finish().await.unwrap();
        output.writer_output.await.unwrap().unwrap();
        let sst_info = output.sst_info.sst_info;
        let sstable = sstable_store
            .meta_index(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        assert!(sstable.index.shard_count > 1);

        let prefix_hash = Sstable::hash_for_filter(&prefix_hint, table_id.as_raw_id());
        let mut stats = StoreLocalStatistic::default();
        assert!(
            hit_sstable_filter_with_partitioned_meta(
                &sstable_store,
                &sstable,
                &(Bound::Unbounded, Bound::Unbounded),
                prefix_hash,
                &mut stats,
            )
            .await
            .unwrap(),
            "schema prefix filter missed prefix across partitioned meta shards; stats={stats:?}"
        );
    }

    #[tokio::test]
    async fn test_partitioned_meta_point_get_multi_version_across_shards() {
        let mut opts = default_builder_opt_for_test();
        opts.block_capacity = 128;
        opts.partitioned_meta_block_count = 2;

        let table_key = [
            VirtualNode::ZERO.to_be_bytes().as_slice(),
            b"multi_version_key".as_slice(),
        ]
        .concat();
        let value_of_epoch = |epoch| format!("value_epoch_{epoch}").into_bytes();
        let kv_iter = (1..=5).rev().map(|epoch| {
            (
                FullKey::for_test(TableId::default(), table_key.clone(), test_epoch(epoch)),
                HummockValue::put(value_of_epoch(epoch)),
            )
        });

        let sstable_store = mock_sstable_store().await;
        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let sst_info = gen_test_sstable_impl::<_, Xor16FilterBuilder>(
            opts,
            2,
            kv_iter,
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )
        .await;

        let read_options = ReadOptions::default();
        for (read_epoch, expected_epoch) in [(6, 5), (4, 4), (2, 2)] {
            let read_key = FullKey::for_test(
                TableId::default(),
                table_key.clone(),
                test_epoch(read_epoch),
            );
            let encoded_key = read_key.encode();
            let dist_key_hash = Sstable::hash_for_filter(
                user_key(&encoded_key),
                read_key.user_key.table_id.as_raw_id(),
            );
            let mut stats = StoreLocalStatistic::default();
            let iter = get_from_sstable_info(
                sstable_store.clone(),
                &sst_info,
                read_key.to_ref(),
                &read_options,
                Some(dist_key_hash),
                &mut stats,
            )
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("missed read_epoch {read_epoch}"));

            assert_eq!(
                iter.key().epoch_with_gap.pure_epoch(),
                test_epoch(expected_epoch)
            );
            assert_bytes_eq!(
                iter.value().into_user_value().unwrap(),
                value_of_epoch(expected_epoch)
            );
        }

        let read_key = FullKey::for_test(TableId::default(), table_key, test_epoch(0));
        let encoded_key = read_key.encode();
        let dist_key_hash = Sstable::hash_for_filter(
            user_key(&encoded_key),
            read_key.user_key.table_id.as_raw_id(),
        );
        let mut stats = StoreLocalStatistic::default();
        let iter = get_from_sstable_info(
            sstable_store,
            &sst_info,
            read_key.to_ref(),
            &read_options,
            Some(dist_key_hash),
            &mut stats,
        )
        .await
        .unwrap();
        assert!(iter.is_none());
    }

    async fn may_match_partitioned_filter(
        sstable_store: &SstableStoreRef,
        table: &PartitionedSstableMetaHolder,
        key: UserKey<&[u8]>,
        hash: u64,
    ) -> bool {
        let user_key_range = (Bound::Included(key), Bound::Included(key));
        let full_key = FullKey {
            user_key: key,
            epoch_with_gap: EpochWithGap::new_from_epoch(HummockEpoch::MAX),
        };
        let mut stats = StoreLocalStatistic::default();
        for desc in table.index.filter_candidate_shards_by_user_key(full_key) {
            let shard = sstable_store
                .get_meta_shard_holder(table.id, table.index.filter_type, desc, &mut stats)
                .await
                .unwrap();
            if shard.may_match(&user_key_range, hash) {
                return true;
            }
        }
        false
    }

    async fn test_with_xor_filter_builder<F: FilterBuilder>(
        bloom_false_positive: f64,
        expected_filter_type: PbSstableFilterType,
    ) {
        let key_count = 1000;

        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive,
            ..Default::default()
        };

        // build remote table
        let sstable_store = mock_sstable_store().await;
        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let sst_info = gen_test_sstable_impl::<Vec<u8>, F>(
            opts,
            0,
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )
        .await;
        assert_eq!(sst_info.filter_type, expected_filter_type);
        let table = sstable_store
            .meta_index(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();

        for i in 0..key_count {
            let full_key = test_key_of(i);
            let hash = Sstable::hash_for_filter(full_key.user_key.encode().as_slice(), 0);
            let key_ref = full_key.user_key.as_ref();
            assert!(
                may_match_partitioned_filter(&sstable_store, &table, key_ref, hash).await,
                "failed at {}",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_xor_filter_builder_output() {
        test_with_xor_filter_builder::<Xor16FilterBuilder>(
            0.0,
            PbSstableFilterType::SstableFilterXor16,
        )
        .await;
        test_with_xor_filter_builder::<Xor16FilterBuilder>(
            0.01,
            PbSstableFilterType::SstableFilterXor16,
        )
        .await;
        test_with_xor_filter_builder::<Xor8FilterBuilder>(
            0.01,
            PbSstableFilterType::SstableFilterXor16,
        )
        .await;
        test_with_xor_filter_builder::<BlockedXor16FilterBuilder>(
            0.01,
            PbSstableFilterType::SstableFilterXor16,
        )
        .await;
    }

    #[tokio::test]
    async fn test_no_xor_filter_block() {
        let opts = SstableBuilderOptions::default();
        // build remote table
        let sstable_store = mock_sstable_store().await;
        let writer_opts = SstableWriterOptions::default();
        let object_id = 1;
        let writer = sstable_store
            .clone()
            .create_sst_writer(object_id, writer_opts);
        let mut filter = MultiFilterKeyExtractor::default();
        filter.register(
            1.into(),
            FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor),
        );
        filter.register(
            2.into(),
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
        );
        filter.register(
            3.into(),
            FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor),
        );

        let table_id_to_vnode = HashMap::from_iter(vec![
            (1.into(), VirtualNode::COUNT_FOR_TEST),
            (2.into(), VirtualNode::COUNT_FOR_TEST),
            (3.into(), VirtualNode::COUNT_FOR_TEST),
        ]);
        let table_id_to_watermark_serde =
            HashMap::from_iter(vec![(1.into(), None), (2.into(), None), (3.into(), None)]);

        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(filter),
            table_id_to_vnode,
            table_id_to_watermark_serde,
            HashMap::default(),
        ));

        let mut builder = SstableBuilder::new(
            object_id,
            writer,
            BlockedXor16FilterBuilder::create(opts.filter_builder_options()),
            opts,
            compaction_catalog_agent_ref,
            None,
        );

        let key_count: usize = 10000;
        for table_id in 1..4 {
            let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
            for idx in 0..key_count {
                table_key.resize(VirtualNode::SIZE, 0);
                table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
                let k = UserKey::for_test(TableId::new(table_id), table_key.as_ref());
                let v = test_value_of(idx);
                builder
                    .add(
                        FullKey::from_user_key(k, test_epoch(1)),
                        HummockValue::put(v.as_ref()),
                    )
                    .await
                    .unwrap();
            }
        }
        let ret = builder.finish().await.unwrap();
        let sst_info = ret.sst_info.sst_info.clone();
        ret.writer_output.await.unwrap().unwrap();
        let table = sstable_store
            .meta_index(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
        for idx in 0..key_count {
            table_key.resize(VirtualNode::SIZE, 0);
            table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
            let k = UserKey::for_test(TableId::new(2), table_key.as_slice());
            let hash = Sstable::hash_for_filter(&k.encode(), 2);
            let key_ref = k.as_ref();
            assert!(may_match_partitioned_filter(&sstable_store, &table, key_ref, hash).await);
        }
    }
}
