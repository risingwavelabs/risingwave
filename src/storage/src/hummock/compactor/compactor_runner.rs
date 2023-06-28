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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{can_concat, HummockEpoch, KeyComparator, LocalSstableInfo};
use risingwave_pb::hummock::{CompactTask, LevelType, SstableInfo};

use super::compaction_utils::estimate_task_memory_capacity;
use super::task_progress::TaskProgress;
use super::TaskConfig;
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::iterator::ConcatSstableIterator;
use crate::hummock::compactor::{
    CompactOutput, CompactionFilter, CompactionStatistics, Compactor, CompactorContext,
    MultiCompactionFilter,
};
use crate::hummock::iterator::{Forward, HummockIterator, UnorderedMergeIteratorInner};
use crate::hummock::sstable::CompactionDeleteRangesBuilder;
use crate::hummock::{
    CachePolicy, CompactionDeleteRanges, CompressionAlgorithm, HummockResult,
    SstableBuilderOptions, SstableStoreRef,
};
use crate::monitor::StoreLocalStatistic;

pub struct CompactorRunner {
    compact_task: CompactTask,
    compactor: Compactor,
    sstable_store: SstableStoreRef,
    key_range: KeyRange,
    split_index: usize,
}

impl CompactorRunner {
    pub fn new(split_index: usize, context: Arc<CompactorContext>, task: CompactTask) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        options.capacity = estimate_task_memory_capacity(context.clone(), &task);

        let key_range = KeyRange {
            left: Bytes::copy_from_slice(task.splits[split_index].get_left()),
            right: Bytes::copy_from_slice(task.splits[split_index].get_right()),
            right_exclusive: true,
        };

        let compactor = Compactor::new(
            context.clone(),
            options,
            TaskConfig {
                key_range: key_range.clone(),
                cache_policy: CachePolicy::NotFill,
                gc_delete_keys: task.gc_delete_keys,
                watermark: task.watermark,
                stats_target_table_ids: Some(HashSet::from_iter(task.existing_table_ids.clone())),
                task_type: task.task_type(),
                is_target_l0_or_lbase: task.target_level == 0
                    || task.target_level == task.base_level,
                split_by_table: task.split_by_state_table,
                split_weight_by_vnode: task.split_weight_by_vnode,
            },
        );

        Self {
            compactor,
            compact_task: task,
            sstable_store: context.sstable_store.clone(),
            key_range,
            split_index,
        }
    }

    async fn compact_sstables(
        &self,
        input_ssts: Vec<SstableInfo>,
        compaction_filter: impl CompactionFilter,
        del_agg: Arc<CompactionDeleteRanges>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        let mut table_iters = Vec::with_capacity(input_ssts.len());
        for table_info in &input_ssts {
            let table_ids = &table_info.table_ids;
            let exist_table = table_ids
                .iter()
                .any(|table_id| self.compact_task.existing_table_ids.contains(table_id));
            if !exist_table {
                continue;
            }
            table_iters.push(ConcatSstableIterator::new(
                self.compact_task.existing_table_ids.clone(),
                vec![table_info.clone()],
                self.compactor.task_config.key_range.clone(),
                self.sstable_store.clone(),
                task_progress.clone(),
            ));
        }
        let iter = UnorderedMergeIteratorInner::for_compactor(table_iters);
        let (ssts, compaction_stat) = self
            .compactor
            .compact_key_range(
                iter,
                compaction_filter,
                del_agg.clone(),
                filter_key_extractor.clone(),
                Some(task_progress.clone()),
                Some(self.compact_task.task_id),
                Some(self.split_index),
            )
            .await?;
        Ok((ssts, compaction_stat))
    }

    pub async fn run_with_trivial_move(
        &self,
        compaction_filter: MultiCompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        del_agg: Arc<CompactionDeleteRanges>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<CompactOutput> {
        let mut output_ssts = vec![];
        let mut input_ssts = vec![];
        let mut max_largest_key = vec![];
        let input_level = &self.compact_task.input_ssts[0];
        let mut compaction_stat = CompactionStatistics::default();
        let mut trivial_move_count = 0;
        for sst in input_level.table_infos.iter() {
            if KeyComparator::encoded_full_key_less_than(
                &max_largest_key,
                &sst.key_range.as_ref().unwrap().left,
            ) {
                if input_ssts.len() == 1 {
                    let sst = input_ssts.pop().unwrap();
                    compaction_stat.iter_total_key_counts += sst.total_key_count;
                    trivial_move_count += 1;
                    output_ssts.push(LocalSstableInfo::with_stats(sst, TableStatsMap::default()));
                } else if input_ssts.len() > 1 {
                    let (ssts, stat) = self
                        .compact_sstables(
                            std::mem::take(&mut input_ssts),
                            compaction_filter.clone(),
                            del_agg.clone(),
                            filter_key_extractor.clone(),
                            task_progress.clone(),
                        )
                        .await?;
                    output_ssts.extend(ssts);
                    compaction_stat.iter_total_key_counts += stat.iter_total_key_counts;
                    compaction_stat.iter_drop_key_counts += stat.iter_drop_key_counts;
                    for (k, v) in stat.delta_drop_stat {
                        compaction_stat
                            .delta_drop_stat
                            .entry(k)
                            .or_default()
                            .add(&v);
                    }
                }
                max_largest_key = sst.key_range.as_ref().unwrap().right.clone();
            } else if KeyComparator::encoded_full_key_less_than(
                &max_largest_key,
                &sst.key_range.as_ref().unwrap().right,
            ) {
                max_largest_key = sst.key_range.as_ref().unwrap().right.clone();
            }
            input_ssts.push(sst.clone());
        }
        if input_ssts.len() == 1 {
            let sst = input_ssts.pop().unwrap();
            compaction_stat.iter_total_key_counts += sst.total_key_count;
            trivial_move_count += 1;
            output_ssts.push(LocalSstableInfo::with_stats(sst, TableStatsMap::default()));
        } else if input_ssts.len() > 1 {
            let (ssts, stat) = self
                .compact_sstables(
                    input_ssts,
                    compaction_filter.clone(),
                    del_agg.clone(),
                    filter_key_extractor.clone(),
                    task_progress.clone(),
                )
                .await?;
            output_ssts.extend(ssts);
            compaction_stat.iter_total_key_counts += stat.iter_total_key_counts;
            compaction_stat.iter_drop_key_counts += stat.iter_drop_key_counts;
            for (k, v) in stat.delta_drop_stat {
                compaction_stat
                    .delta_drop_stat
                    .entry(k)
                    .or_default()
                    .add(&v);
            }
        }
        tracing::info!("use trivial move optimize for compact-task: {}. trivial move file count: {}, total file count: {}",
            self.compact_task.task_id,
            trivial_move_count,
            self.compact_task.input_ssts[0].table_infos.len()
        );
        Ok((self.split_index, output_ssts, compaction_stat))
    }

    pub async fn run(
        &self,
        compaction_filter: impl CompactionFilter,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
        del_agg: Arc<CompactionDeleteRanges>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<CompactOutput> {
        let iter = self.build_sst_iter(task_progress.clone())?;
        let (ssts, compaction_stat) = self
            .compactor
            .compact_key_range(
                iter,
                compaction_filter,
                del_agg,
                filter_key_extractor,
                Some(task_progress),
                Some(self.compact_task.task_id),
                Some(self.split_index),
            )
            .await?;
        Ok((self.split_index, ssts, compaction_stat))
    }

    pub async fn build_delete_range_iter<F: CompactionFilter>(
        sstable_infos: &Vec<SstableInfo>,
        gc_delete_keys: bool,
        sstable_store: &SstableStoreRef,
        filter: &mut F,
    ) -> HummockResult<Arc<CompactionDeleteRanges>> {
        let mut builder = CompactionDeleteRangesBuilder::default();
        let mut local_stats = StoreLocalStatistic::default();

        for table_info in sstable_infos {
            let table = sstable_store.sstable(table_info, &mut local_stats).await?;
            let mut range_tombstone_list = table.value().meta.monotonic_tombstone_events.clone();
            range_tombstone_list.iter_mut().for_each(|tombstone| {
                if filter.should_delete(FullKey::from_user_key(
                    tombstone.event_key.left_user_key.as_ref(),
                    tombstone.new_epoch,
                )) {
                    tombstone.new_epoch = HummockEpoch::MAX;
                }
            });
            builder.add_delete_events(range_tombstone_list);
        }

        let aggregator = builder.build_for_compaction(gc_delete_keys);
        Ok(aggregator)
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(
        &self,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let mut table_iters = Vec::new();

        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            // Do not need to filter the table because manager has done it.
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&level.table_infos));
                let tables = level
                    .table_infos
                    .iter()
                    .filter(|table_info| {
                        let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                        let table_ids = &table_info.table_ids;
                        let exist_table = table_ids.iter().any(|table_id| {
                            self.compact_task.existing_table_ids.contains(table_id)
                        });

                        self.key_range.full_key_overlap(&key_range) && exist_table
                    })
                    .cloned()
                    .collect_vec();
                table_iters.push(ConcatSstableIterator::new(
                    self.compact_task.existing_table_ids.clone(),
                    tables,
                    self.compactor.task_config.key_range.clone(),
                    self.sstable_store.clone(),
                    task_progress.clone(),
                ));
            } else {
                for table_info in &level.table_infos {
                    let key_range = KeyRange::from(table_info.key_range.as_ref().unwrap());
                    let table_ids = &table_info.table_ids;
                    let exist_table = table_ids
                        .iter()
                        .any(|table_id| self.compact_task.existing_table_ids.contains(table_id));

                    if !self.key_range.full_key_overlap(&key_range) || !exist_table {
                        continue;
                    }
                    table_iters.push(ConcatSstableIterator::new(
                        self.compact_task.existing_table_ids.clone(),
                        vec![table_info.clone()],
                        self.compactor.task_config.key_range.clone(),
                        self.sstable_store.clone(),
                        task_progress.clone(),
                    ));
                }
            }
        }
        Ok(UnorderedMergeIteratorInner::for_compactor(table_iters))
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::UserKey;
    use risingwave_pb::hummock::InputLevel;

    use super::*;
    use crate::hummock::compactor::StateCleanUpCompactionFilter;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_range_tombstone,
    };
    use crate::hummock::{create_monotonic_events, DeleteRangeTombstone};

    #[tokio::test]
    async fn test_delete_range_aggregator_with_filter() {
        let sstable_store = mock_sstable_store();
        let kv_pairs = vec![];
        let range_tombstones = vec![
            DeleteRangeTombstone::new_for_test(
                TableId::new(1),
                b"abc".to_vec(),
                b"cde".to_vec(),
                1,
            ),
            DeleteRangeTombstone::new_for_test(
                TableId::new(2),
                b"abc".to_vec(),
                b"def".to_vec(),
                1,
            ),
        ];
        let mut sstable_info_1 = gen_test_sstable_with_range_tombstone(
            default_builder_opt_for_test(),
            1,
            kv_pairs.clone().into_iter(),
            range_tombstones.clone(),
            sstable_store.clone(),
        )
        .await
        .get_sstable_info();
        sstable_info_1.table_ids = vec![1];

        let mut sstable_info_2 = gen_test_sstable_with_range_tombstone(
            default_builder_opt_for_test(),
            2,
            kv_pairs.into_iter(),
            range_tombstones.clone(),
            sstable_store.clone(),
        )
        .await
        .get_sstable_info();
        sstable_info_2.table_ids = vec![2];

        let compact_task = CompactTask {
            input_ssts: vec![InputLevel {
                level_idx: 0,
                level_type: 0,
                table_infos: vec![sstable_info_1, sstable_info_2],
            }],
            existing_table_ids: vec![2],
            ..Default::default()
        };
        let mut state_clean_up_filter = StateCleanUpCompactionFilter::new(HashSet::from_iter(
            compact_task.existing_table_ids.clone(),
        ));

        let sstable_infos = compact_task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .filter(|table_info| {
                let table_ids = &table_info.table_ids;
                table_ids
                    .iter()
                    .any(|table_id| compact_task.existing_table_ids.contains(table_id))
            })
            .cloned()
            .collect_vec();

        let collector = CompactorRunner::build_delete_range_iter(
            &sstable_infos,
            compact_task.gc_delete_keys,
            &sstable_store,
            &mut state_clean_up_filter,
        )
        .await
        .unwrap();
        let ret = collector.get_tombstone_between(
            UserKey::<Bytes>::default().as_ref(),
            UserKey::<Bytes>::default().as_ref(),
        );

        assert_eq!(
            ret,
            create_monotonic_events(vec![range_tombstones[1].clone()])
        );
    }
}
