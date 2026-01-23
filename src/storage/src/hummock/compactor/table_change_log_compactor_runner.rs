// Copyright 2026 RisingWave Labs
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

use futures::{FutureExt, StreamExt, stream};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
use risingwave_hummock_sdk::compact_task::{CompactTask, TableChangeLogCompactionOutput};
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap};
use risingwave_hummock_sdk::{
    HummockSstableObjectId, can_concat, compact_task_output_to_string, compact_task_to_string,
    estimate_memory_for_compact_task, statistics_compact_task,
};
use risingwave_pb::hummock::compact_task::TaskStatus;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::TaskConfig;
use super::iterator::MonitoredCompactorIterator;
use super::task_progress::TaskProgress;
use crate::compaction_catalog_manager::CompactionCatalogAgent;
use crate::hummock::compactor::iterator::ConcatSstableIterator;
use crate::hummock::compactor::task_progress::TaskProgressGuard;
use crate::hummock::compactor::{
    CompactOutput, Compactor, CompactorContext, MultiCompactionFilter, await_tree_key,
};
use crate::hummock::iterator::{Forward, HummockIterator, MergeIterator};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    CachePolicy, CompressionAlgorithm, GetObjectId, HummockResult, SstableBuilderOptions,
    SstableStoreRef,
};

pub(crate) async fn compact_table_change_log(
    context: CompactorContext,
    compact_task: CompactTask,
    mut shutdown_rx: Receiver<()>,
    object_id_getter: Arc<dyn GetObjectId>,
) -> (
    (
        CompactTask,
        HashMap<TableId, TableStats>,
        HashMap<HummockSstableObjectId, u64>,
    ),
    Option<MemoryTracker>,
) {
    let parallelism = compact_task.splits.len();
    let compact_task_statistics = statistics_compact_task(&compact_task);
    let estimated_output_capacity =
        estimate_table_change_log_task_output_capacity(context.clone(), &compact_task);
    let task_memory_capacity_with_parallelism = estimate_memory_for_compact_task(
        &compact_task,
        (context.storage_opts.block_size_kb as u64) * (1 << 10),
        context
            .storage_opts
            .object_store_config
            .s3
            .recv_buffer_size
            .unwrap_or(6 * 1024 * 1024) as u64,
        estimated_output_capacity as u64,
    ) * compact_task.splits.len() as u64;
    tracing::info!(
        "Ready to handle task: {} compact_task_statistics {:?} compression_algorithm {:?}  parallelism {} task_memory_capacity_with_parallelism {}, {}",
        compact_task.task_id,
        compact_task_statistics,
        compact_task.compression_algorithm,
        parallelism,
        task_memory_capacity_with_parallelism,
        compact_task_to_string(&compact_task),
    );
    let group_label = compact_task.compaction_group_id.to_string();
    let mut task_status = TaskStatus::Success;
    let timer = context
        .compactor_metrics
        .compact_task_duration
        .with_label_values(&[&group_label, &"0".to_owned()])
        .start_timer();
    let memory_detector = context
        .memory_limiter
        .try_require_memory(task_memory_capacity_with_parallelism);
    if memory_detector.is_none() {
        tracing::warn!(
            "Not enough memory to serve the task {} task_memory_capacity_with_parallelism {}  memory_usage {} memory_quota {}",
            compact_task.task_id,
            task_memory_capacity_with_parallelism,
            context.memory_limiter.get_memory_usage(),
            context.memory_limiter.quota()
        );
        task_status = TaskStatus::NoAvailMemoryResourceCanceled;
        return (
            seal_table_change_log_compaction_task(
                compact_task,
                context.clone(),
                vec![],
                vec![],
                task_status,
            ),
            memory_detector,
        );
    }

    context.compactor_metrics.compact_task_pending_num.inc();
    context
        .compactor_metrics
        .compact_task_pending_parallelism
        .add(parallelism as _);
    let _release_metrics_guard =
        scopeguard::guard((parallelism, context.clone()), |(parallelism, context)| {
            context.compactor_metrics.compact_task_pending_num.dec();
            context
                .compactor_metrics
                .compact_task_pending_parallelism
                .sub(parallelism as _);
        });
    let mut compaction_futures = vec![];
    let mut abort_handles = vec![];
    let task_progress_guard =
        TaskProgressGuard::new(compact_task.task_id, context.task_progress_manager.clone());
    let mut output_ssts_new_values = vec![];
    let mut output_ssts_old_values = vec![];
    for (split_index, _) in compact_task.splits.iter().enumerate() {
        let runner = TableChangeLogCompactorRunner::new(
            split_index,
            context.clone(),
            compact_task.clone(),
            object_id_getter.clone(),
            estimated_output_capacity,
        );
        let task_progress = task_progress_guard.progress.clone();
        let runner = async move { runner.run(task_progress).await };
        let traced = match context.await_tree_reg.as_ref() {
            None => runner.right_future(),
            Some(await_tree_reg) => await_tree_reg
                .register(
                    await_tree_key::CompactRunner {
                        task_id: compact_task.task_id,
                        split_index,
                    },
                    format!(
                        "Compaction Task {} Split {} ",
                        compact_task.task_id, split_index
                    ),
                )
                .instrument(runner)
                .left_future(),
        };
        let handle = tokio::spawn(traced);
        abort_handles.push(handle.abort_handle());
        compaction_futures.push(handle);
    }
    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::warn!("Compaction task cancelled externally:\n{}", compact_task_to_string(&compact_task));
                task_status = TaskStatus::ManualCanceled;
                break;
            }
            future_result = buffered.next() => {
                match future_result {
                    Some(Ok(Ok((new, old)))) => {
                        output_ssts_new_values.push(new);
                        output_ssts_old_values.push(old);
                    }
                    Some(Ok(Err(e))) => {
                        task_status = TaskStatus::ExecuteFailed;
                        tracing::warn!(
                            error = %e.as_report(),
                            "Compaction task {} failed with error",
                            compact_task.task_id,
                        );
                        break;
                    }
                    Some(Err(e)) => {
                        task_status = TaskStatus::JoinHandleFailed;
                        tracing::warn!(
                            error = %e.as_report(),
                            "Compaction task {} failed with join handle error",
                            compact_task.task_id,
                        );
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    if task_status != TaskStatus::Success {
        for abort_handle in abort_handles {
            abort_handle.abort();
        }
        output_ssts_new_values.clear();
        output_ssts_old_values.clear();
    }
    if !output_ssts_new_values.is_empty() {
        output_ssts_new_values.sort_by_key(|(split_index, ..)| *split_index);
    }
    if !output_ssts_old_values.is_empty() {
        output_ssts_old_values.sort_by_key(|(split_index, ..)| *split_index);
    }

    let (compact_task, table_stats, object_timestamps) = seal_table_change_log_compaction_task(
        compact_task,
        context.clone(),
        output_ssts_new_values,
        output_ssts_old_values,
        task_status,
    );
    let cost_time = timer.stop_and_record() * 1000.0;
    tracing::info!(
        "Finished compaction task in {:?}ms: {}",
        cost_time,
        compact_task_output_to_string(&compact_task)
    );
    (
        (compact_task, table_stats, object_timestamps),
        memory_detector,
    )
}

fn estimate_table_change_log_task_output_capacity(
    context: CompactorContext,
    task: &CompactTask,
) -> usize {
    let max_target_file_size = context.storage_opts.sstable_size_mb as usize * (1 << 20);
    let total_input_uncompressed_file_size = task
        .table_change_log_input
        .as_ref()
        .map(|i| {
            i.clean_part
                .iter()
                .chain(i.dirty_part.iter())
                .flat_map(|a| a.new_value.iter().chain(a.old_value.iter()))
                .map(|sstable_info| sstable_info.uncompressed_file_size)
                .sum::<u64>()
        })
        .unwrap_or(0);
    let capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
    std::cmp::min(capacity, total_input_uncompressed_file_size as usize)
}

pub struct TableChangeLogCompactorRunner {
    compact_task: CompactTask,
    old_value_compactor: Compactor,
    new_value_compactor: Compactor,
    sstable_store: SstableStoreRef,
    key_range: KeyRange,
    split_index: usize,
}

impl TableChangeLogCompactorRunner {
    pub fn new(
        split_index: usize,
        context: CompactorContext,
        task: CompactTask,
        object_id_getter: Arc<dyn GetObjectId>,
        estimated_output_capacity: usize,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };
        options.capacity = estimated_output_capacity;
        let use_block_based_filter = task.should_use_block_based_filter();

        let key_range = KeyRange {
            left: task.splits[split_index].left.clone(),
            right: task.splits[split_index].right.clone(),
            right_exclusive: true,
        };

        let task_config = TaskConfig {
            key_range: key_range.clone(),
            cache_policy: CachePolicy::NotFill,
            gc_delete_keys: task.gc_delete_keys,
            retain_multiple_version: false,
            task_type: task.task_type,
            use_block_based_filter,
            preserve_earliest_key_version: true,
            ..Default::default()
        };
        let old_value_compactor = Compactor::new(
            context.clone(),
            options.clone(),
            task_config.clone(),
            object_id_getter.clone(),
        );

        let new_value_compactor = Compactor::new(
            context.clone(),
            options,
            TaskConfig {
                preserve_earliest_key_version: false,
                ..task_config
            },
            object_id_getter,
        );

        Self {
            split_index,
            old_value_compactor,
            new_value_compactor,
            compact_task: task,
            sstable_store: context.sstable_store,
            key_range,
        }
    }

    pub async fn run(
        &self,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<(CompactOutput, CompactOutput)> {
        let empty_catalog_agent = Arc::new(CompactionCatalogAgent::dummy());
        let empty_compaction_filter = MultiCompactionFilter::default();

        let (clean_part, dirty_part) = self
            .compact_task
            .table_change_log_input
            .as_ref()
            .map(|i| (&i.clean_part, &i.dirty_part))
            .unwrap();
        // TODO(ZW): Maybe parallelize new values and old values compaction. The memory estimation should be adjusted accordingly.
        let (new_value_ssts, new_value_compaction_stat) = {
            let new_value_iter =
                self.build_sst_iter(clean_part.iter(), dirty_part.iter(), task_progress.clone())?;
            self.new_value_compactor
                .compact_key_range(
                    new_value_iter,
                    empty_compaction_filter.clone(),
                    empty_catalog_agent.clone(),
                    Some(task_progress.clone()),
                    Some(self.compact_task.task_id),
                    Some(self.split_index),
                )
                .await?
        };

        let (old_value_ssts, old_value_compaction_stat) = {
            let old_value_iter =
                self.build_sst_iter(clean_part.iter(), dirty_part.iter(), task_progress.clone())?;
            self.old_value_compactor
                .compact_key_range(
                    old_value_iter,
                    empty_compaction_filter,
                    empty_catalog_agent,
                    Some(task_progress),
                    Some(self.compact_task.task_id),
                    Some(self.split_index),
                )
                .await?
        };

        Ok((
            (self.split_index, new_value_ssts, new_value_compaction_stat),
            (self.split_index, old_value_ssts, old_value_compaction_stat),
        ))
    }

    fn build_sst_iter<'a>(
        &self,
        clean_part: impl Iterator<Item = &'a EpochNewChangeLog>,
        dirty_part: impl Iterator<Item = &'a EpochNewChangeLog>,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let compactor_iter_max_io_retry_times = self
            .old_value_compactor
            .context
            .storage_opts
            .compactor_iter_max_io_retry_times;
        let mut table_iters: Vec<ConcatSstableIterator> = Vec::new();
        let filter_sstable_infos = |sstable_info: &&SstableInfo| -> bool {
            let table_ids = &sstable_info.table_ids;
            let exist_table = table_ids
                .iter()
                .any(|table_id| self.compact_task.existing_table_ids.contains(table_id));

            self.key_range.full_key_overlap(&sstable_info.key_range) && exist_table
        };

        for change_log in clean_part {
            let filtered_clean_part_sstable_infos = change_log
                .new_value
                .iter()
                .filter(filter_sstable_infos)
                .cloned()
                .collect::<Vec<_>>();
            debug_assert!(can_concat(&filtered_clean_part_sstable_infos));
            table_iters.push(ConcatSstableIterator::new(
                self.compact_task.existing_table_ids.clone(),
                filtered_clean_part_sstable_infos,
                self.old_value_compactor.task_config.key_range.clone(),
                self.sstable_store.clone(),
                task_progress.clone(),
                compactor_iter_max_io_retry_times,
            ));
        }
        let filtered_dirty_part_sstable_infos = dirty_part
            .flat_map(|change_log| change_log.old_value.iter())
            .filter(filter_sstable_infos)
            .cloned()
            .collect::<Vec<_>>();
        // Avoid excessive loading of dirty SSTables in the compactor: limit the number of dirty SSTables selected per compaction task at the meta node.
        for sstable_info in filtered_dirty_part_sstable_infos {
            table_iters.push(ConcatSstableIterator::new(
                self.compact_task.existing_table_ids.clone(),
                vec![sstable_info],
                self.old_value_compactor.task_config.key_range.clone(),
                self.sstable_store.clone(),
                task_progress.clone(),
                compactor_iter_max_io_retry_times,
            ));
        }

        Ok(MonitoredCompactorIterator::new(
            MergeIterator::for_compactor(table_iters),
            task_progress,
        ))
    }
}

fn seal_table_change_log_compaction_task(
    mut compact_task: CompactTask,
    context: CompactorContext,
    new_value_output_ssts: Vec<CompactOutput>,
    old_value_output_ssts: Vec<CompactOutput>,
    task_status: TaskStatus,
) -> (
    CompactTask,
    HashMap<TableId, TableStats>,
    HashMap<HummockSstableObjectId, u64>,
) {
    // table change log compaction task doesn't generate table stats change.
    let table_stats_map = TableStatsMap::default();
    let mut object_timestamps = HashMap::default();
    compact_task.task_status = task_status;
    let mut sorted_new_values_ssts = vec![];
    let mut sorted_old_values_ssts = vec![];
    let mut compaction_write_bytes = 0;
    for (_, ssts, _) in new_value_output_ssts {
        for sst_info in ssts {
            compaction_write_bytes += sst_info.file_size();
            object_timestamps.insert(sst_info.sst_info.object_id, sst_info.created_at);
            sorted_new_values_ssts.push(sst_info.sst_info);
        }
    }

    for (_, ssts, _) in old_value_output_ssts {
        for sst_info in ssts {
            compaction_write_bytes += sst_info.file_size();
            object_timestamps.insert(sst_info.sst_info.object_id, sst_info.created_at);
            sorted_old_values_ssts.push(sst_info.sst_info);
        }
    }

    compact_task.table_change_log_output = Some(TableChangeLogCompactionOutput {
        sorted_new_values_ssts,
        sorted_old_values_ssts,
    });

    let group_label = compact_task.compaction_group_id.to_string();
    let level_label = compact_task.target_level.to_string();
    context
        .compactor_metrics
        .compact_write_bytes
        .with_label_values(&[&group_label, &level_label])
        .inc_by(compaction_write_bytes);
    let compact_write_sstn = compact_task
        .table_change_log_output
        .as_ref()
        .map(|output| output.sorted_new_values_ssts.len() + output.sorted_old_values_ssts.len())
        .unwrap_or(0) as u64;
    context
        .compactor_metrics
        .compact_write_sstn
        .with_label_values(&[&group_label, &level_label])
        .inc_by(compact_write_sstn);

    // Always set table_change_log_output to Some, even if the task fails, to indicate that this is a table change log compaction task.
    // This is necessary because ReportTask lacks a task type field.
    assert!(compact_task.table_change_log_output.is_some());
    (compact_task, table_stats_map, object_timestamps)
}
