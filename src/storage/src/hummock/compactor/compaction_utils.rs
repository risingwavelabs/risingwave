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

use std::collections::{BinaryHeap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::vec;

use itertools::Itertools;
use minstant::Instant;
use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{HummockEpoch, KeyComparator};
use risingwave_pb::hummock::{compact_task, CompactTask, KeyRange as KeyRange_vec, SstableInfo};

pub use super::context::CompactorContext;
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::{
    MultiCompactionFilter, StateCleanUpCompactionFilter, TtlCompactionFilter,
};
use crate::hummock::multi_builder::TableBuilderFactory;
use crate::hummock::sstable::DEFAULT_ENTRY_SIZE;
use crate::hummock::{
    BlockMeta, CachePolicy, FilterBuilder, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableObjectIdManagerRef, SstableWriterFactory, SstableWriterOptions,
};
use crate::monitor::StoreLocalStatistic;

pub struct RemoteBuilderFactory<W: SstableWriterFactory, F: FilterBuilder> {
    pub sstable_object_id_manager: SstableObjectIdManagerRef,
    pub limiter: Arc<MemoryLimiter>,
    pub options: SstableBuilderOptions,
    pub policy: CachePolicy,
    pub remote_rpc_cost: Arc<AtomicU64>,
    pub filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    pub sstable_writer_factory: W,
    pub _phantom: PhantomData<F>,
}

#[async_trait::async_trait]
impl<W: SstableWriterFactory, F: FilterBuilder> TableBuilderFactory for RemoteBuilderFactory<W, F> {
    type Filter = F;
    type Writer = W::Writer;

    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>> {
        // TODO: memory consumption may vary based on `SstableWriter`, `ObjectStore` and cache
        let tracker = self
            .limiter
            .require_memory((self.options.capacity + self.options.block_capacity) as u64)
            .await;
        let timer = Instant::now();
        let table_id = self
            .sstable_object_id_manager
            .get_new_sst_object_id()
            .await?;
        let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
        self.remote_rpc_cost.fetch_add(cost, Ordering::Relaxed);
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity + self.options.block_capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .sstable_writer_factory
            .create_sst_writer(table_id, writer_options)
            .await?;
        let builder = SstableBuilder::new(
            table_id,
            writer,
            Self::Filter::create(
                self.options.bloom_false_positive,
                self.options.capacity / DEFAULT_ENTRY_SIZE + 1,
            ),
            self.options.clone(),
            self.filter_key_extractor.clone(),
        );
        Ok(builder)
    }
}

/// `CompactionStatistics` will count the results of each compact split
#[derive(Default, Debug)]
pub struct CompactionStatistics {
    // to report per-table metrics
    pub delta_drop_stat: TableStatsMap,

    // to calculate delete ratio
    pub iter_total_key_counts: u64,
    pub iter_drop_key_counts: u64,
}

impl CompactionStatistics {
    #[allow(dead_code)]
    fn delete_ratio(&self) -> Option<u64> {
        if self.iter_total_key_counts == 0 {
            return None;
        }

        Some(self.iter_drop_key_counts / self.iter_total_key_counts)
    }
}

#[derive(Clone)]
pub struct TaskConfig {
    pub key_range: KeyRange,
    pub cache_policy: CachePolicy,
    pub gc_delete_keys: bool,
    pub watermark: u64,
    /// `stats_target_table_ids` decides whether a dropped key should be counted as table stats
    /// change. For an divided SST as input, a dropped key shouldn't be counted if its table id
    /// doesn't belong to this divided SST. See `Compactor::compact_and_build_sst`.
    pub stats_target_table_ids: Option<HashSet<u32>>,
    pub task_type: compact_task::TaskType,
    pub is_target_l0_or_lbase: bool,
    pub split_by_table: bool,
    pub split_weight_by_vnode: u32,
}

pub fn build_multi_compaction_filter(compact_task: &CompactTask) -> MultiCompactionFilter {
    use risingwave_common::catalog::TableOption;
    let mut multi_filter = MultiCompactionFilter::default();
    let compaction_filter_flag =
        CompactionFilterFlag::from_bits(compact_task.compaction_filter_mask).unwrap_or_default();
    if compaction_filter_flag.contains(CompactionFilterFlag::STATE_CLEAN) {
        let state_clean_up_filter = Box::new(StateCleanUpCompactionFilter::new(
            HashSet::from_iter(compact_task.existing_table_ids.clone()),
        ));

        multi_filter.register(state_clean_up_filter);
    }

    if compaction_filter_flag.contains(CompactionFilterFlag::TTL) {
        let id_to_ttl = compact_task
            .table_options
            .iter()
            .filter(|id_to_option| {
                let table_option: TableOption = id_to_option.1.into();
                table_option.retention_seconds.is_some()
            })
            .map(|id_to_option| (*id_to_option.0, id_to_option.1.retention_seconds))
            .collect();

        let ttl_filter = Box::new(TtlCompactionFilter::new(
            id_to_ttl,
            compact_task.current_epoch_time,
        ));
        multi_filter.register(ttl_filter);
    }

    multi_filter
}

pub async fn generate_splits(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: Arc<CompactorContext>,
) -> HummockResult<Vec<KeyRange_vec>> {
    let sstable_size = (context.storage_opts.sstable_size_mb as u64) << 20;
    if compaction_size > sstable_size * 2 {
        let mut indexes = vec![];
        // preload the meta and get the smallest key to split sub_compaction
        for sstable_info in sstable_infos {
            indexes.extend(
                context
                    .sstable_store
                    .sstable(sstable_info, &mut StoreLocalStatistic::default())
                    .await?
                    .value()
                    .meta
                    .block_metas
                    .iter()
                    .map(|block| {
                        let data_size = block.len;
                        let full_key = FullKey {
                            user_key: FullKey::decode(&block.smallest_key).user_key,
                            epoch: HummockEpoch::MAX,
                        }
                        .encode();
                        (data_size as u64, full_key)
                    })
                    .collect_vec(),
            );
        }
        // sort by key, as for every data block has the same size;
        indexes.sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.1.as_ref(), b.1.as_ref()));
        let mut splits = vec![];
        splits.push(KeyRange_vec::new(vec![], vec![]));
        let parallelism = std::cmp::min(
            indexes.len() as u64,
            context.storage_opts.max_sub_compaction as u64,
        );
        let sub_compaction_data_size = std::cmp::max(compaction_size / parallelism, sstable_size);
        let parallelism = compaction_size / sub_compaction_data_size;

        if parallelism > 1 {
            let mut last_buffer_size = 0;
            let mut last_key: Vec<u8> = vec![];
            let mut remaining_size = indexes.iter().map(|block| block.0).sum::<u64>();
            for (data_size, key) in indexes {
                if last_buffer_size >= sub_compaction_data_size
                    && !last_key.eq(&key)
                    && remaining_size > sstable_size
                {
                    splits.last_mut().unwrap().right = key.clone();
                    splits.push(KeyRange_vec::new(key.clone(), vec![]));
                    last_buffer_size = data_size;
                } else {
                    last_buffer_size += data_size;
                }
                remaining_size -= data_size;
                last_key = key;
            }
            return Ok(splits);
        }
    }

    Ok(vec![])
}

pub async fn block_overlap_info(
    compact_task: &CompactTask,
    context: Arc<CompactorContext>,
) -> HummockResult<(usize, usize)> {
    if compact_task.target_level == 0 || compact_task.target_level == compact_task.base_level {
        return Ok((0, 0));
    }
    let mut block_meta_vec = vec![vec![]; compact_task.input_ssts.len()];
    let mut total_block_count = 0;

    for (idx, input_level) in compact_task.input_ssts.iter().enumerate() {
        for sstable_info in &input_level.table_infos {
            let block_metas = context
                .sstable_store
                .sstable(sstable_info, &mut StoreLocalStatistic::default())
                .await?
                .value()
                .meta
                .block_metas
                .clone();

            total_block_count += block_metas.len();
            block_meta_vec[idx].extend(block_metas);
        }

        let last_key = input_level
            .table_infos
            .last()
            .unwrap()
            .get_key_range()
            .as_ref()
            .unwrap()
            .get_right();
        let end_block_meta = BlockMeta {
            smallest_key: last_key.clone(),
            ..Default::default()
        };
        block_meta_vec[idx].push(end_block_meta);
    }

    let copy_block_count = check_copy_block(&block_meta_vec);

    Ok((copy_block_count, total_block_count))
}

fn check_copy_block(block_meta_vec: &Vec<Vec<BlockMeta>>) -> usize {
    #[derive(Eq)]
    struct Item<'a> {
        arr: &'a Vec<BlockMeta>,
        idx: usize,
    }

    impl<'a> PartialEq for Item<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.arr[self.idx].smallest_key == other.arr[self.idx].smallest_key
        }
    }

    impl<'a> PartialOrd for Item<'a> {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.arr[self.idx]
                .smallest_key
                .partial_cmp(&other.arr[self.idx].smallest_key)
        }
    }

    impl<'a> Ord for Item<'a> {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.arr[self.idx]
                .smallest_key
                .cmp(&other.arr[self.idx].smallest_key)
        }
    }

    let mut heap = BinaryHeap::default();
    use std::cmp::Reverse;
    for block_meta_list in block_meta_vec {
        let first_item = Item {
            arr: block_meta_list,
            idx: 0,
        };
        heap.push(Reverse(first_item));
    }

    let mut copy_block_count = 0;
    while !heap.is_empty() {
        let top_item = heap.pop().unwrap().0;
        let next_idx = top_item.idx + 1;

        if next_idx < top_item.arr.len() {
            let next_block = &top_item.arr[next_idx];
            match heap.peek() {
                Some(top_block) => {
                    if next_block.smallest_key < top_block.0.arr[top_block.0.idx].smallest_key {
                        copy_block_count += 1;
                    }

                    heap.push(Reverse(Item {
                        arr: top_item.arr,
                        idx: next_idx,
                    }))
                }

                None => {
                    copy_block_count += top_item.arr.len() - top_item.idx;
                }
            }
        }
    }

    copy_block_count
}

pub fn estimate_task_memory_capacity(context: Arc<CompactorContext>, task: &CompactTask) -> usize {
    let max_target_file_size = context.storage_opts.sstable_size_mb as usize * (1 << 20);
    let total_file_size = task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .map(|table| table.file_size)
        .sum::<u64>();

    let capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
    let total_file_size = (total_file_size as f64 * 1.2).round() as usize;

    match task.compression_algorithm {
        0 => std::cmp::min(capacity, total_file_size),
        _ => capacity,
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::hummock::compactor::compaction_utils::check_copy_block;
    use crate::hummock::BlockMeta;

    #[tokio::test]
    async fn test_block_overlap() {
        #[derive(Eq)]
        struct Item<'a> {
            arr: &'a Vec<BlockMeta>,
            idx: usize,
        }

        impl<'a> PartialEq for Item<'a> {
            fn eq(&self, other: &Self) -> bool {
                self.arr[self.idx].smallest_key == other.arr[self.idx].smallest_key
            }
        }

        impl<'a> PartialOrd for Item<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.arr[self.idx]
                    .smallest_key
                    .partial_cmp(&other.arr[self.idx].smallest_key)
            }
        }

        impl<'a> Ord for Item<'a> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.arr[self.idx]
                    .smallest_key
                    .cmp(&other.arr[self.idx].smallest_key)
            }
        }

        let block_meta_test_data = vec![
            vec![
                BlockMeta {
                    smallest_key: "a".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "b".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "c".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "d".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "j".as_bytes().to_vec(),
                    ..Default::default()
                },
            ],
            vec![
                BlockMeta {
                    smallest_key: "i".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "j".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "o".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "p".as_bytes().to_vec(),
                    ..Default::default()
                },
            ],
            vec![
                BlockMeta {
                    smallest_key: "i".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "j".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "k".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "q".as_bytes().to_vec(),
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: "r".as_bytes().to_vec(),
                    ..Default::default()
                },
            ],
        ];

        let mut block_meta_vec = vec![vec![]; block_meta_test_data.len()];
        let mut total_block_count = 0;

        for (idx, block_metas) in block_meta_test_data.into_iter().enumerate() {
            total_block_count += block_metas.len();
            block_meta_vec[idx].extend(block_metas);
        }

        let copy_block_count = check_copy_block(&block_meta_vec);

        println!(
            "copy_block_count {} total_block_count {}",
            copy_block_count, total_block_count
        );
    }
}
