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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures::future::Either;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::get_compaction_group_ids;
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
use risingwave_pb::hummock::level_handler::RunningCompactTask;
use rw_futures_util::select_all;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::IntervalStream;
use tracing::warn;

use crate::hummock::manager::HISTORY_TABLE_INFO_STATISTIC_TIME;
use crate::hummock::metrics_utils::{trigger_lsm_stat, trigger_mv_stat};
use crate::hummock::{HummockManager, TASK_NORMAL};

// This structure describes how hummock handles sst switching in a compaction group. A better sst cut will result in better data alignment, which in turn will improve the efficiency of the compaction.
// By adopting certain rules, a better sst cut will lead to better data alignment and thus improve the efficiency of the compaction.
enum TableAlignRule {
    // The table_id is not optimized for alignment.
    NoOptimization,
    // Move the table_id to a separate compaction group. Currently, the system only supports separate compaction with one table.
    SplitToDedicatedCg((CompactionGroupId, BTreeMap<StateTableId, u32>)),
    // In the current group, partition the table's data according to the granularity of the vnode.
    SplitByVnode((StateTableId, u32)),
    // In the current group, partition the table's data at the granularity of the table.
    SplitByTable(StateTableId),
}

impl HummockManager {
    pub fn hummock_timer_task(hummock_manager: Arc<Self>) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            const CHECK_PENDING_TASK_PERIOD_SEC: u64 = 300;
            const STAT_REPORT_PERIOD_SEC: u64 = 20;
            const COMPACTION_HEARTBEAT_PERIOD_SEC: u64 = 1;

            pub enum HummockTimerEvent {
                GroupSplit,
                CheckDeadTask,
                Report,
                CompactionHeartBeatExpiredCheck,

                DynamicCompactionTrigger,
                SpaceReclaimCompactionTrigger,
                TtlCompactionTrigger,
                TombstoneCompactionTrigger,

                FullGc,
            }
            let mut check_compact_trigger_interval =
                tokio::time::interval(Duration::from_secs(CHECK_PENDING_TASK_PERIOD_SEC));
            check_compact_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            check_compact_trigger_interval.reset();

            let check_compact_trigger = IntervalStream::new(check_compact_trigger_interval)
                .map(|_| HummockTimerEvent::CheckDeadTask);

            let mut stat_report_interval =
                tokio::time::interval(std::time::Duration::from_secs(STAT_REPORT_PERIOD_SEC));
            stat_report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            stat_report_interval.reset();
            let stat_report_trigger =
                IntervalStream::new(stat_report_interval).map(|_| HummockTimerEvent::Report);

            let mut compaction_heartbeat_interval = tokio::time::interval(
                std::time::Duration::from_secs(COMPACTION_HEARTBEAT_PERIOD_SEC),
            );
            compaction_heartbeat_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            compaction_heartbeat_interval.reset();
            let compaction_heartbeat_trigger = IntervalStream::new(compaction_heartbeat_interval)
                .map(|_| HummockTimerEvent::CompactionHeartBeatExpiredCheck);

            let mut min_trigger_interval = tokio::time::interval(Duration::from_secs(
                hummock_manager.env.opts.periodic_compaction_interval_sec,
            ));
            min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            min_trigger_interval.reset();
            let dynamic_tick_trigger = IntervalStream::new(min_trigger_interval)
                .map(|_| HummockTimerEvent::DynamicCompactionTrigger);

            let mut min_space_reclaim_trigger_interval =
                tokio::time::interval(Duration::from_secs(
                    hummock_manager
                        .env
                        .opts
                        .periodic_space_reclaim_compaction_interval_sec,
                ));
            min_space_reclaim_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            min_space_reclaim_trigger_interval.reset();
            let space_reclaim_trigger = IntervalStream::new(min_space_reclaim_trigger_interval)
                .map(|_| HummockTimerEvent::SpaceReclaimCompactionTrigger);

            let mut min_ttl_reclaim_trigger_interval = tokio::time::interval(Duration::from_secs(
                hummock_manager
                    .env
                    .opts
                    .periodic_ttl_reclaim_compaction_interval_sec,
            ));
            min_ttl_reclaim_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            min_ttl_reclaim_trigger_interval.reset();
            let ttl_reclaim_trigger = IntervalStream::new(min_ttl_reclaim_trigger_interval)
                .map(|_| HummockTimerEvent::TtlCompactionTrigger);

            let mut full_gc_interval = tokio::time::interval(Duration::from_secs(
                hummock_manager.env.opts.full_gc_interval_sec,
            ));
            full_gc_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            full_gc_interval.reset();
            let full_gc_trigger =
                IntervalStream::new(full_gc_interval).map(|_| HummockTimerEvent::FullGc);

            let mut tombstone_reclaim_trigger_interval =
                tokio::time::interval(Duration::from_secs(
                    hummock_manager
                        .env
                        .opts
                        .periodic_tombstone_reclaim_compaction_interval_sec,
                ));
            tombstone_reclaim_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tombstone_reclaim_trigger_interval.reset();
            let tombstone_reclaim_trigger = IntervalStream::new(tombstone_reclaim_trigger_interval)
                .map(|_| HummockTimerEvent::TombstoneCompactionTrigger);

            let mut triggers: Vec<BoxStream<'static, HummockTimerEvent>> = vec![
                Box::pin(check_compact_trigger),
                Box::pin(stat_report_trigger),
                Box::pin(compaction_heartbeat_trigger),
                Box::pin(dynamic_tick_trigger),
                Box::pin(space_reclaim_trigger),
                Box::pin(ttl_reclaim_trigger),
                Box::pin(full_gc_trigger),
                Box::pin(tombstone_reclaim_trigger),
            ];

            let periodic_check_split_group_interval_sec = hummock_manager
                .env
                .opts
                .periodic_split_compact_group_interval_sec;

            if periodic_check_split_group_interval_sec > 0 {
                let mut split_group_trigger_interval = tokio::time::interval(Duration::from_secs(
                    periodic_check_split_group_interval_sec,
                ));
                split_group_trigger_interval
                    .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                let split_group_trigger = IntervalStream::new(split_group_trigger_interval)
                    .map(|_| HummockTimerEvent::GroupSplit);
                triggers.push(Box::pin(split_group_trigger));
            }

            let event_stream = select_all(triggers);
            use futures::pin_mut;
            pin_mut!(event_stream);

            let shutdown_rx_shared = shutdown_rx.shared();

            tracing::info!(
                "Hummock timer task tracing [GroupSplit interval {} sec] [CheckDeadTask interval {} sec] [Report interval {} sec] [CompactionHeartBeat interval {} sec]",
                    periodic_check_split_group_interval_sec, CHECK_PENDING_TASK_PERIOD_SEC, STAT_REPORT_PERIOD_SEC, COMPACTION_HEARTBEAT_PERIOD_SEC
            );

            loop {
                let item =
                    futures::future::select(event_stream.next(), shutdown_rx_shared.clone()).await;

                match item {
                    Either::Left((event, _)) => {
                        if let Some(event) = event {
                            match event {
                                HummockTimerEvent::CheckDeadTask => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.check_dead_task().await;
                                }

                                HummockTimerEvent::GroupSplit => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.on_handle_check_split_multi_group().await;
                                }

                                HummockTimerEvent::Report => {
                                    let (current_version, id_to_config, version_stats) = {
                                        let versioning_guard =
                                            hummock_manager.versioning.read().await;

                                        let configs =
                                            hummock_manager.get_compaction_group_map().await;
                                        let versioning_deref = versioning_guard;
                                        (
                                            versioning_deref.current_version.clone(),
                                            configs,
                                            versioning_deref.version_stats.clone(),
                                        )
                                    };

                                    if let Some(mv_id_to_all_table_ids) = hummock_manager
                                        .metadata_manager
                                        .get_job_id_to_internal_table_ids_mapping()
                                        .await
                                    {
                                        trigger_mv_stat(
                                            &hummock_manager.metrics,
                                            &version_stats,
                                            mv_id_to_all_table_ids,
                                        );
                                    }

                                    for compaction_group_id in
                                        get_compaction_group_ids(&current_version)
                                    {
                                        let compaction_group_config =
                                            &id_to_config[&compaction_group_id];

                                        let group_levels = current_version
                                            .get_compaction_group_levels(
                                                compaction_group_config.group_id(),
                                            );

                                        trigger_lsm_stat(
                                            &hummock_manager.metrics,
                                            compaction_group_config.compaction_config(),
                                            group_levels,
                                            compaction_group_config.group_id(),
                                        )
                                    }
                                }

                                HummockTimerEvent::CompactionHeartBeatExpiredCheck => {
                                    let compactor_manager =
                                        hummock_manager.compactor_manager.clone();

                                    // TODO: add metrics to track expired tasks
                                    // The cancel task has two paths
                                    // 1. compactor heartbeat cancels the expired task based on task
                                    // progress (meta + compactor)
                                    // 2. meta periodically scans the task and performs a cancel on
                                    // the meta side for tasks that are not updated by heartbeat
                                    for task in compactor_manager.get_heartbeat_expired_tasks() {
                                        if let Err(e) = hummock_manager
                                            .cancel_compact_task(
                                                task.task_id,
                                                TaskStatus::HeartbeatCanceled,
                                            )
                                            .await
                                        {
                                            tracing::error!(
                                                task_id = task.task_id,
                                                error = %e.as_report(),
                                                "Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                                                until we can successfully report its status",
                                            );
                                        }
                                    }
                                }

                                HummockTimerEvent::DynamicCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::Dynamic,
                                        )
                                        .await;
                                }

                                HummockTimerEvent::SpaceReclaimCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::SpaceReclaim,
                                        )
                                        .await;
                                }

                                HummockTimerEvent::TtlCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(compact_task::TaskType::Ttl)
                                        .await;
                                }

                                HummockTimerEvent::TombstoneCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::Tombstone,
                                        )
                                        .await;
                                }

                                HummockTimerEvent::FullGc => {
                                    if hummock_manager
                                        .start_full_gc(Duration::from_secs(3600))
                                        .is_ok()
                                    {
                                        tracing::info!("Start full GC from meta node.");
                                    }
                                }
                            }
                        }
                    }

                    Either::Right((_, _shutdown)) => {
                        tracing::info!("Hummock timer loop is stopped");
                        break;
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }
}

impl HummockManager {
    async fn check_dead_task(&self) {
        const MAX_COMPACTION_L0_MULTIPLIER: u64 = 32;
        const MAX_COMPACTION_DURATION_SEC: u64 = 20 * 60;
        let (groups, configs) = {
            let versioning_guard = self.versioning.read().await;
            let g = versioning_guard
                .current_version
                .levels
                .iter()
                .map(|(id, group)| {
                    (
                        *id,
                        group
                            .l0
                            .as_ref()
                            .unwrap()
                            .sub_levels
                            .iter()
                            .map(|level| level.total_file_size)
                            .sum::<u64>(),
                    )
                })
                .collect_vec();
            let c = self.get_compaction_group_map().await;
            (g, c)
        };
        let mut slowdown_groups: HashMap<u64, u64> = HashMap::default();
        {
            for (group_id, l0_file_size) in groups {
                let group = &configs[&group_id];
                if l0_file_size
                    > MAX_COMPACTION_L0_MULTIPLIER
                        * group.compaction_config.max_bytes_for_level_base
                {
                    slowdown_groups.insert(group_id, l0_file_size);
                }
            }
        }
        if slowdown_groups.is_empty() {
            return;
        }
        let mut pending_tasks: HashMap<u64, (u64, usize, RunningCompactTask)> = HashMap::default();
        {
            let compaction_guard = self.compaction.read().await;
            for group_id in slowdown_groups.keys() {
                if let Some(status) = compaction_guard.compaction_statuses.get(group_id) {
                    for (idx, level_handler) in status.level_handlers.iter().enumerate() {
                        let tasks = level_handler.get_pending_tasks().to_vec();
                        if tasks.is_empty() {
                            continue;
                        }
                        for task in tasks {
                            pending_tasks.insert(task.task_id, (*group_id, idx, task));
                        }
                    }
                }
            }
        }
        let task_ids = pending_tasks.keys().cloned().collect_vec();
        let task_infos = self
            .compactor_manager
            .check_tasks_status(&task_ids, Duration::from_secs(MAX_COMPACTION_DURATION_SEC));
        for (task_id, (compact_time, status)) in task_infos {
            if status == TASK_NORMAL {
                continue;
            }
            if let Some((group_id, level_id, task)) = pending_tasks.get(&task_id) {
                let group_size = *slowdown_groups.get(group_id).unwrap();
                warn!("COMPACTION SLOW: the task-{} of group-{}(size: {}MB) level-{} has not finished after {:?}, {}, it may cause pending sstable files({:?}) blocking other task.",
                    task_id, *group_id,group_size / 1024 / 1024,*level_id, compact_time, status, task.ssts);
            }
        }
    }

    /// * For compaction group with only one single state-table, do not change it again.
    /// * For state-table which only write less than `HISTORY_TABLE_INFO_WINDOW_SIZE` times, do not
    ///   change it. Because we need more statistic data to decide split strategy.
    /// * For state-table with low throughput which write no more than
    ///   `min_table_split_write_throughput` data, never split it.
    /// * For state-table whose size less than `min_table_split_size`, do not split it unless its
    ///   throughput keep larger than `table_write_throughput_threshold` for a long time.
    /// * For state-table whose throughput less than `min_table_split_write_throughput`, do not
    ///   increase it size of base-level.
    async fn on_handle_check_split_multi_group(&self) {
        let params = self.env.system_params_reader().await;
        let barrier_interval_ms = params.barrier_interval_ms() as u64;
        let checkpoint_secs = std::cmp::max(
            1,
            params.checkpoint_frequency() * barrier_interval_ms / 1000,
        );
        let created_tables = match self.metadata_manager.get_created_table_ids().await {
            Ok(created_tables) => created_tables,
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to fetch created table ids");
                return;
            }
        };
        let created_tables: HashSet<u32> = HashSet::from_iter(created_tables);
        let table_write_throughput = self.history_table_throughput.read().clone();
        let mut group_infos = self.calculate_compaction_group_statistic().await;
        group_infos.sort_by_key(|group| group.group_size);
        group_infos.reverse();
        const SPLIT_BY_TABLE: u32 = 1;

        let mut group_to_table_vnode_partition = self.group_to_table_vnode_partition.read().clone();
        for group in &group_infos {
            if group.table_statistic.len() == 1 {
                // no need to handle the separate compaciton group
                continue;
            }

            let mut table_vnode_partition_mappoing = group_to_table_vnode_partition
                .entry(group.group_id)
                .or_default();

            for (table_id, table_size) in &group.table_statistic {
                let rule = self
                    .calculate_table_align_rule(
                        &table_write_throughput,
                        table_id,
                        table_size,
                        !created_tables.contains(table_id),
                        checkpoint_secs,
                        group.group_id,
                        group.group_size,
                    )
                    .await;

                match rule {
                    TableAlignRule::NoOptimization => {
                        table_vnode_partition_mappoing.remove(table_id);
                        continue;
                    }

                    TableAlignRule::SplitByTable(table_id) => {
                        if self.env.opts.hybird_partition_vnode_count > 0 {
                            table_vnode_partition_mappoing.insert(table_id, SPLIT_BY_TABLE);
                        } else {
                            table_vnode_partition_mappoing.remove(&table_id);
                        }
                    }

                    TableAlignRule::SplitByVnode((table_id, vnode)) => {
                        if self.env.opts.hybird_partition_vnode_count > 0 {
                            table_vnode_partition_mappoing.insert(table_id, vnode);
                        } else {
                            table_vnode_partition_mappoing.remove(&table_id);
                        }
                    }

                    TableAlignRule::SplitToDedicatedCg((
                        new_group_id,
                        table_vnode_partition_count,
                    )) => {
                        let _ = table_vnode_partition_mappoing; // drop
                        group_to_table_vnode_partition
                            .insert(new_group_id, table_vnode_partition_count);

                        table_vnode_partition_mappoing = group_to_table_vnode_partition
                            .entry(group.group_id)
                            .or_default();
                    }
                }
            }
        }

        tracing::trace!(
            "group_to_table_vnode_partition {:?}",
            group_to_table_vnode_partition
        );

        // batch update group_to_table_vnode_partition
        *self.group_to_table_vnode_partition.write() = group_to_table_vnode_partition;
    }

    async fn on_handle_trigger_multi_group(&self, task_type: compact_task::TaskType) {
        for cg_id in self.compaction_group_ids().await {
            if let Err(e) = self.compaction_state.try_sched_compaction(cg_id, task_type) {
                tracing::warn!(
                    error = %e.as_report(),
                    "Failed to schedule {:?} compaction for compaction group {}",
                    task_type,
                    cg_id,
                );
            }
        }
    }

    async fn calculate_table_align_rule(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        table_id: &u32,
        table_size: &u64,
        is_creating_table: bool,
        checkpoint_secs: u64,
        parent_group_id: u64,
        group_size: u64,
    ) -> TableAlignRule {
        let default_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault.into();
        let mv_group_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView.into();
        let partition_vnode_count = self.env.opts.partition_vnode_count;
        let hybrid_vnode_count: u32 = self.env.opts.hybird_partition_vnode_count;
        let window_size = HISTORY_TABLE_INFO_STATISTIC_TIME / (checkpoint_secs as usize);

        let mut is_high_write_throughput = false;
        let mut is_low_write_throughput = true;
        if let Some(history) = table_write_throughput.get(table_id) {
            if !is_creating_table {
                if history.len() >= window_size {
                    is_high_write_throughput = history.iter().all(|throughput| {
                        *throughput / checkpoint_secs
                            > self.env.opts.table_write_throughput_threshold
                    });
                    is_low_write_throughput = history.iter().any(|throughput| {
                        *throughput / checkpoint_secs
                            < self.env.opts.min_table_split_write_throughput
                    });
                }
            } else {
                // For creating table, relax the checking restrictions to make the data alignment behavior more sensitive.
                let sum = history.iter().sum::<u64>();
                is_low_write_throughput = sum
                    < self.env.opts.min_table_split_write_throughput
                        * history.len() as u64
                        * checkpoint_secs;
            }
        }

        let state_table_size = *table_size;
        let result = {
            // When in a hybrid compaction group, data from multiple state tables may exist in a single sst, and in order to make the data in the sub level more aligned, a proactive cut is made for the data.
            // https://github.com/risingwavelabs/risingwave/issues/13037
            // 1. In some scenario (like backfill), the creating state_table / mv may have high write throughput (creating table ). Therefore, we relax the limit of `is_low_write_throughput` and partition the table with high write throughput by vnode to improve the parallel efficiency of compaction.
            // Add: creating table is not allowed to be split
            // 2. For table with low throughput, partition by table_id to minimize amplification.
            // 3. When the write mode is changed (the above conditions are not met), the default behavior is restored
            if !is_low_write_throughput {
                TableAlignRule::SplitByVnode((*table_id, hybrid_vnode_count))
            } else if state_table_size > self.env.opts.cut_table_size_limit {
                TableAlignRule::SplitByTable(*table_id)
            } else {
                TableAlignRule::NoOptimization
            }
        };

        // 1. Avoid splitting a creating table
        // 2. Avoid splitting a is_low_write_throughput creating table
        // 3. Avoid splitting a non-high throughput medium-sized table
        if is_creating_table
            || (is_low_write_throughput)
            || (state_table_size < self.env.opts.min_table_split_size && !is_high_write_throughput)
        {
            return result;
        }

        // do not split a large table and a small table because it would increase IOPS
        // of small table.
        if parent_group_id != default_group_id && parent_group_id != mv_group_id {
            let rest_group_size = group_size - state_table_size;
            if rest_group_size < state_table_size
                && rest_group_size < self.env.opts.min_table_split_size
            {
                return result;
            }
        }

        let ret = self
            .move_state_table_to_compaction_group(
                parent_group_id,
                &[*table_id],
                partition_vnode_count,
            )
            .await;
        match ret {
            Ok((new_group_id, table_vnode_partition_count)) => {
                tracing::info!("move state table [{}] from group-{} to group-{} success table_vnode_partition_count {:?}", table_id, parent_group_id, new_group_id, table_vnode_partition_count);
                return TableAlignRule::SplitToDedicatedCg((
                    new_group_id,
                    table_vnode_partition_count,
                ));
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to move state table [{}] from group-{}",
                    table_id,
                    parent_group_id,
                )
            }
        }

        TableAlignRule::NoOptimization
    }
}
