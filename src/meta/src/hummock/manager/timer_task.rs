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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::Either;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::get_compaction_group_ids;
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
use risingwave_pb::hummock::level_handler::RunningCompactTask;
use rw_futures_util::select_all;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::IntervalStream;
use tracing::{info, warn};

use crate::backup_restore::BackupManagerRef;
use crate::hummock::metrics_utils::{trigger_lsm_stat, trigger_mv_stat};
use crate::hummock::{HummockManager, TASK_NORMAL};

impl HummockManager {
    pub fn hummock_timer_task(
        hummock_manager: Arc<Self>,
        backup_manager: Option<BackupManagerRef>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            const CHECK_PENDING_TASK_PERIOD_SEC: u64 = 300;
            const STAT_REPORT_PERIOD_SEC: u64 = 20;
            const COMPACTION_HEARTBEAT_PERIOD_SEC: u64 = 1;

            pub enum HummockTimerEvent {
                GroupScheduleSplit,
                CheckDeadTask,
                Report,
                CompactionHeartBeatExpiredCheck,

                DynamicCompactionTrigger,
                SpaceReclaimCompactionTrigger,
                TtlCompactionTrigger,
                TombstoneCompactionTrigger,

                FullGc,

                GroupScheduleMerge,
                PurgeStaleCompactionGroup,
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

            let periodic_scheduling_compaction_group_split_interval_sec = hummock_manager
                .env
                .opts
                .periodic_scheduling_compaction_group_split_interval_sec;

            if periodic_scheduling_compaction_group_split_interval_sec > 0 {
                let mut scheduling_compaction_group_trigger_interval = tokio::time::interval(
                    Duration::from_secs(periodic_scheduling_compaction_group_split_interval_sec),
                );
                scheduling_compaction_group_trigger_interval
                    .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                scheduling_compaction_group_trigger_interval.reset();
                let group_scheduling_split_trigger =
                    IntervalStream::new(scheduling_compaction_group_trigger_interval)
                        .map(|_| HummockTimerEvent::GroupScheduleSplit);
                triggers.push(Box::pin(group_scheduling_split_trigger));
            }

            let periodic_scheduling_compaction_group_merge_interval_sec = hummock_manager
                .env
                .opts
                .periodic_scheduling_compaction_group_merge_interval_sec;

            if periodic_scheduling_compaction_group_merge_interval_sec > 0 {
                let mut scheduling_compaction_group_merge_trigger_interval = tokio::time::interval(
                    Duration::from_secs(periodic_scheduling_compaction_group_merge_interval_sec),
                );
                scheduling_compaction_group_merge_trigger_interval
                    .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                scheduling_compaction_group_merge_trigger_interval.reset();
                let group_scheduling_merge_trigger =
                    IntervalStream::new(scheduling_compaction_group_merge_trigger_interval)
                        .map(|_| HummockTimerEvent::GroupScheduleMerge);
                triggers.push(Box::pin(group_scheduling_merge_trigger));
            }

            let periodic_purge_stale_compaction_group_interval_sec = hummock_manager
                .env
                .opts
                .periodic_purge_stale_compaction_group_interval_sec;

            if periodic_purge_stale_compaction_group_interval_sec > 0 {
                let mut purge_stale_compaction_group_trigger_interval = tokio::time::interval(
                    Duration::from_secs(periodic_purge_stale_compaction_group_interval_sec),
                );
                purge_stale_compaction_group_trigger_interval
                    .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                purge_stale_compaction_group_trigger_interval.reset();
                let purge_stale_compaction_group_trigger =
                    IntervalStream::new(purge_stale_compaction_group_trigger_interval)
                        .map(|_| HummockTimerEvent::PurgeStaleCompactionGroup);
                triggers.push(Box::pin(purge_stale_compaction_group_trigger));
            }

            let event_stream = select_all(triggers);
            use futures::pin_mut;
            pin_mut!(event_stream);

            let shutdown_rx_shared = shutdown_rx.shared();

            tracing::info!(
                "Hummock timer task [GroupSchedulingSplit interval {} sec] [GroupSchedulingMerge interval {} sec] [PurgeStaleCompactionGroup interval {} sec] [CheckDeadTask interval {} sec] [Report interval {} sec] [CompactionHeartBeat interval {} sec]",
                periodic_scheduling_compaction_group_split_interval_sec,
                periodic_scheduling_compaction_group_merge_interval_sec,
                periodic_purge_stale_compaction_group_interval_sec,
                CHECK_PENDING_TASK_PERIOD_SEC,
                STAT_REPORT_PERIOD_SEC,
                COMPACTION_HEARTBEAT_PERIOD_SEC
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

                                HummockTimerEvent::GroupScheduleSplit => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.on_handle_schedule_group_split().await;
                                }

                                HummockTimerEvent::GroupScheduleMerge => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.on_handle_schedule_group_merge().await;
                                }

                                HummockTimerEvent::PurgeStaleCompactionGroup => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.purge_stale_compaction_groups().await;
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

                                    {
                                        let group_infos = hummock_manager
                                            .calculate_compaction_group_statistic()
                                            .await;
                                        let compaction_group_count = group_infos.len();
                                        hummock_manager
                                            .metrics
                                            .compaction_group_count
                                            .set(compaction_group_count as i64);

                                        let table_write_throughput_statistic_manager =
                                            hummock_manager
                                                .table_write_throughput_statistic_manager
                                                .read()
                                                .clone();

                                        let current_version_levels = &hummock_manager
                                            .versioning
                                            .read()
                                            .await
                                            .current_version
                                            .levels;

                                        for group_info in group_infos {
                                            hummock_manager
                                                .metrics
                                                .compaction_group_size
                                                .with_label_values(&[&group_info
                                                    .group_id
                                                    .to_string()])
                                                .set(group_info.group_size as _);
                                            // accumulate the throughput of all tables in the group
                                            let mut avg_throuput = 0;
                                            let max_statistic_expired_time = std::cmp::max(
                                                hummock_manager
                                                    .env
                                                    .opts
                                                    .table_stat_throuput_window_seconds_for_split,
                                                hummock_manager
                                                    .env
                                                    .opts
                                                    .table_stat_throuput_window_seconds_for_merge,
                                            );
                                            for table_id in group_info.table_statistic.keys() {
                                                avg_throuput +=
                                                    table_write_throughput_statistic_manager
                                                        .avg_write_throughput(
                                                            *table_id,
                                                            max_statistic_expired_time as i64,
                                                        )
                                                        as u64;
                                            }

                                            hummock_manager
                                                .metrics
                                                .compaction_group_throughput
                                                .with_label_values(&[&group_info
                                                    .group_id
                                                    .to_string()])
                                                .set(avg_throuput as _);

                                            if let Some(group_levels) =
                                                current_version_levels.get(&group_info.group_id)
                                            {
                                                let file_count = group_levels.count_ssts();
                                                hummock_manager
                                                    .metrics
                                                    .compaction_group_file_count
                                                    .with_label_values(&[&group_info
                                                        .group_id
                                                        .to_string()])
                                                    .set(file_count as _);
                                            }
                                        }
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
                                    let expired_tasks: Vec<u64> = compactor_manager
                                        .get_heartbeat_expired_tasks()
                                        .into_iter()
                                        .map(|task| task.task_id)
                                        .collect();
                                    if !expired_tasks.is_empty() {
                                        tracing::info!(
                                            expired_tasks = ?expired_tasks,
                                            "Heartbeat expired compaction tasks detected. Attempting to cancel tasks.",
                                        );
                                        if let Err(e) = hummock_manager
                                            .cancel_compact_tasks(
                                                expired_tasks.clone(),
                                                TaskStatus::HeartbeatCanceled,
                                            )
                                            .await
                                        {
                                            tracing::error!(
                                                expired_tasks = ?expired_tasks,
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

                                    // share the same trigger with SpaceReclaim
                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::VnodeWatermark,
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
                                    let retention_sec =
                                        hummock_manager.env.opts.min_sst_retention_time_sec;
                                    let backup_manager_2 = backup_manager.clone();
                                    let hummock_manager_2 = hummock_manager.clone();
                                    tokio::task::spawn(async move {
                                        use thiserror_ext::AsReport;
                                        let _ = hummock_manager_2
                                            .start_full_gc(
                                                Duration::from_secs(retention_sec),
                                                None,
                                                backup_manager_2,
                                            )
                                            .await
                                            .inspect_err(|e| {
                                                warn!(error = %e.as_report(), "Failed to start GC.")
                                            });
                                    });
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

    async fn purge_stale_compaction_groups(&self) {
        let creating_jobs = match self
            .metadata_manager
            .catalog_controller
            .list_creating_jobs(true, true, None)
            .await
        {
            Ok(creating_jobs) => creating_jobs,
            Err(err) => {
                warn!(
                    error = %err.as_report(),
                    "failed to list creating jobs for stale compaction group purge"
                );
                return;
            }
        };
        if !creating_jobs.is_empty() {
            info!(
                job_count = creating_jobs.len(),
                "skip stale compaction group purge because creating jobs exist"
            );
            return;
        }

        let dropped_table_ids = self
            .metadata_manager
            .catalog_controller
            .list_pending_dropped_table_ids()
            .await;
        if !dropped_table_ids.is_empty() {
            info!(
                table_count = dropped_table_ids.len(),
                "skip stale compaction group purge because dropped tables are pending cleanup"
            );
            return;
        }

        let valid_table_ids: HashSet<_> = match self
            .metadata_manager
            .catalog_controller
            .list_fragment_state_tables()
            .await
        {
            Ok(fragment_state_tables) => fragment_state_tables
                .into_iter()
                .flat_map(|fragment| fragment.state_table_ids.into_inner())
                .collect(),
            Err(err) => {
                warn!(
                    error = %err.as_report(),
                    "failed to list fragment state tables for stale compaction group purge"
                );
                return;
            }
        };

        if let Err(err) = self.purge(&valid_table_ids).await {
            warn!(
                error = %err.as_report(),
                "failed to purge stale compaction groups"
            );
        }
    }
}

impl HummockManager {
    async fn maybe_normalize_compaction_groups(&self, schedule_type: &'static str) {
        if !self.env.opts.enable_compaction_group_normalize {
            return;
        }

        match self
            .normalize_overlapping_compaction_groups_with_limit(
                self.env
                    .opts
                    .max_normalize_splits_per_round
                    .try_into()
                    .unwrap_or(usize::MAX),
            )
            .await
        {
            Ok(split_count) => {
                if split_count > 0 {
                    tracing::info!(
                        "normalize compaction groups finished with {} split(s) before {} scheduling",
                        split_count,
                        schedule_type
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e.as_report(),
                    "failed to normalize compaction groups before {} scheduling",
                    schedule_type
                );
            }
        }
    }

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
        let mut slowdown_groups: HashMap<CompactionGroupId, u64> = HashMap::default();
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
        let mut pending_tasks: HashMap<u64, (CompactionGroupId, usize, RunningCompactTask)> =
            HashMap::default();
        {
            let compaction_guard = self.compaction.read().await;
            for group_id in slowdown_groups.keys() {
                if let Some(status) = compaction_guard.compaction_statuses.get(group_id) {
                    for (idx, level_handler) in status.level_handlers.iter().enumerate() {
                        let tasks = level_handler.pending_tasks().to_vec();
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
                warn!(
                    "COMPACTION SLOW: the task-{} of group-{}(size: {}MB) level-{} has not finished after {:?}, {}, it may cause pending sstable files({:?}) blocking other task.",
                    task_id,
                    group_id,
                    group_size / 1024 / 1024,
                    *level_id,
                    compact_time,
                    status,
                    task.ssts
                );
            }
        }
    }

    /// Try to schedule a compaction `split` for the given compaction groups.
    /// The `split` will be triggered if the following conditions are met:
    /// 1. `state table throughput`: If the table is in a high throughput state and it belongs to a multi table group, then an attempt will be made to split the table into separate compaction groups to increase its throughput and reduce the impact on write amplification.
    /// 2. `group size`: If the group size has exceeded the set upper limit, e.g. `max_group_size` * `split_group_size_ratio`
    async fn on_handle_schedule_group_split(&self) {
        let table_write_throughput = self.table_write_throughput_statistic_manager.read().clone();
        self.maybe_normalize_compaction_groups("split").await;

        let mut group_infos = self.calculate_compaction_group_statistic().await;
        group_infos.sort_by_key(|group| Reverse(group.group_size));

        for group in group_infos {
            if group.table_statistic.len() == 1 {
                // no need to handle the separate compaciton group
                continue;
            }

            self.try_split_compaction_group(&table_write_throughput, group)
                .await;
        }
    }

    #[cfg(test)]
    pub async fn schedule_group_split_for_test(&self) {
        self.on_handle_schedule_group_split().await;
    }

    #[cfg(test)]
    pub async fn schedule_group_merge_for_test(&self) {
        self.on_handle_schedule_group_merge().await;
    }

    async fn on_handle_trigger_multi_group(&self, task_type: compact_task::TaskType) {
        for cg_id in self.compaction_group_ids().await {
            self.compaction_state.try_sched_compaction(
                cg_id,
                task_type,
                super::compaction::ScheduleTrigger::Periodic,
            );
        }
    }

    /// Try to schedule a compaction merge for the given compaction groups.
    /// The merge will be triggered if the following conditions are met:
    /// 1. The compaction group is not contains creating table.
    /// 2. The compaction group is a small group.
    /// 3. All tables in compaction group is in a low throughput state.
    async fn on_handle_schedule_group_merge(&self) {
        self.maybe_normalize_compaction_groups("merge").await;

        let created_tables = match self.metadata_manager.get_created_table_ids().await {
            Ok(created_tables) => HashSet::from_iter(created_tables),
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to fetch created table ids");
                return;
            }
        };
        let table_write_throughput_statistic_manager =
            self.table_write_throughput_statistic_manager.read().clone();
        let mut group_infos = self.calculate_compaction_group_statistic().await;
        // sort by first table id for deterministic merge order
        group_infos.sort_by_key(|group| group.table_statistic.keys().next().copied());

        let group_count = group_infos.len();
        if group_count < 2 {
            return;
        }

        let mut base = 0;
        let mut candidate = 1;

        while candidate < group_count {
            let group = &group_infos[base];
            let next_group = &group_infos[candidate];
            match self
                .try_merge_compaction_group(
                    &table_write_throughput_statistic_manager,
                    group,
                    next_group,
                    &created_tables,
                )
                .await
            {
                Ok(_) => candidate += 1,
                Err(e) => {
                    tracing::debug!(
                        error = %e.as_report(),
                        "Failed to merge compaction group",
                    );
                    base = candidate;
                    candidate = base + 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::CompactionGroupId;
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_meta_model::{
        CreateType, JobId, JobStatus, UserId, WorkerId, object, sink, streaming_job,
    };
    use risingwave_pb::catalog::PbTable;
    use risingwave_pb::common::worker_node::Property;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::user::PbUserInfo;
    use sea_orm::ActiveValue::Set;
    use sea_orm::{ActiveModelTrait, EntityTrait, TransactionTrait};

    use crate::controller::catalog::CatalogController;
    use crate::controller::cluster::{ClusterController, ClusterControllerRef};
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::{CompactorManager, HummockManager, HummockManagerRef};
    use crate::manager::{MetaOpts, MetaSrvEnv};

    async fn setup_compute_env_with_meta_opts(
        port: i32,
        opts: MetaOpts,
    ) -> (
        MetaSrvEnv,
        HummockManagerRef,
        ClusterControllerRef,
        WorkerId,
    ) {
        let env = MetaSrvEnv::for_test_opts(opts, |_| ()).await;
        let cluster_ctl = Arc::new(
            ClusterController::new(env.clone(), Duration::from_secs(1))
                .await
                .unwrap(),
        );
        let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
        let compactor_manager = Arc::new(CompactorManager::for_test());
        let (compactor_streams_change_tx, _compactor_streams_change_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(1)
            .level0_max_compact_file_number(130)
            .level0_sub_level_compact_level_count(1)
            .level0_overlapping_sub_level_compact_level_count(1)
            .build();
        let hummock_manager = HummockManager::with_config(
            env.clone(),
            cluster_ctl.clone(),
            catalog_ctl,
            Arc::new(Default::default()),
            compactor_manager,
            config,
            compactor_streams_change_tx,
        )
        .await;

        let worker_id = cluster_ctl
            .add_worker(
                WorkerType::ComputeNode,
                HostAddress {
                    host: "127.0.0.1".to_owned(),
                    port,
                },
                Property {
                    is_streaming: true,
                    is_serving: true,
                    parallelism: 4,
                    ..Default::default()
                },
                Default::default(),
            )
            .await
            .unwrap();
        (env, hummock_manager, cluster_ctl, worker_id)
    }

    async fn create_creating_sink_job(
        catalog_controller: &CatalogController,
        job_id: JobId,
        owner_id: UserId,
    ) -> crate::MetaResult<()> {
        let inner = catalog_controller.get_inner_read_guard().await;
        let txn = inner.db.begin().await?;
        let now = Utc::now().naive_utc();

        object::ActiveModel {
            oid: Set(job_id.as_object_id()),
            obj_type: Set(object::ObjectType::Sink),
            owner_id: Set(owner_id),
            schema_id: Set(None),
            database_id: Set(None),
            initialized_at: Set(now),
            created_at: Set(now),
            initialized_at_cluster_version: Set(None),
            created_at_cluster_version: Set(None),
        }
        .insert(&txn)
        .await?;

        sink::ActiveModel {
            sink_id: Set(job_id.as_sink_id()),
            name: Set(format!("creating_sink_{}", job_id.as_raw_id())),
            columns: Set(Default::default()),
            plan_pk: Set(Default::default()),
            distribution_key: Set(Default::default()),
            downstream_pk: Set(Default::default()),
            sink_type: Set(sink::SinkType::AppendOnly),
            ignore_delete: Set(false),
            properties: Set(Default::default()),
            definition: Set("create sink".to_owned()),
            connection_id: Set(None),
            db_name: Set(String::new()),
            sink_from_name: Set(String::new()),
            sink_format_desc: Set(None),
            target_table: Set(None),
            secret_ref: Set(None),
            original_target_columns: Set(None),
            auto_refresh_schema_from_table: Set(None),
        }
        .insert(&txn)
        .await?;

        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Creating),
            create_type: Set(CreateType::Background),
            timezone: Set(None),
            config_override: Set(None),
            adaptive_parallelism_strategy: Set(None),
            parallelism: Set(risingwave_meta_model::StreamingParallelism::Adaptive),
            backfill_parallelism: Set(None),
            backfill_orders: Set(None),
            max_parallelism: Set(1),
            specific_resource_group: Set(None),
            is_serverless_backfill: Set(false),
        }
        .insert(&txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }

    async fn get_compaction_group_id_by_table_id(
        hummock_manager: HummockManagerRef,
        table_id: u32,
    ) -> CompactionGroupId {
        hummock_manager
            .get_current_version()
            .await
            .state_table_info
            .info()
            .get(&TableId::new(table_id))
            .unwrap()
            .compaction_group_id
    }

    fn member_table_ids(version: &HummockVersion, group_id: CompactionGroupId) -> Vec<u32> {
        version
            .state_table_info
            .compaction_group_member_table_ids(group_id)
            .iter()
            .map(|table_id| table_id.as_raw_id())
            .collect_vec()
    }

    fn assert_no_group_overlap(version: &HummockVersion) {
        let mut ranges = version
            .levels
            .keys()
            .filter_map(|group_id| {
                let members = member_table_ids(version, *group_id);
                (!members.is_empty()).then(|| (*members.first().unwrap(), *members.last().unwrap()))
            })
            .collect_vec();
        ranges.sort_by_key(|(min_table_id, _)| *min_table_id);
        assert!(ranges.windows(2).all(|window| window[0].1 < window[1].0));
    }

    #[tokio::test]
    async fn test_merge_scheduling_normalizes_when_split_scheduling_is_disabled() {
        let mut opts = MetaOpts::test(false);
        opts.enable_compaction_group_normalize = true;
        opts.periodic_scheduling_compaction_group_split_interval_sec = 0;

        let (_env, hummock_manager, _, _worker_id) =
            setup_compute_env_with_meta_opts(80, opts).await;
        hummock_manager
            .register_table_ids_for_test(&[(64, 2.into()), (80, 2.into())])
            .await
            .unwrap();
        hummock_manager
            .register_table_ids_for_test(&[(65, 3.into()), (81, 3.into()), (83, 3.into())])
            .await
            .unwrap();

        let cg_64 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 64).await;
        let cg_65 = get_compaction_group_id_by_table_id(hummock_manager.clone(), 65).await;

        hummock_manager.on_handle_schedule_group_merge().await;

        let version = hummock_manager.get_current_version().await;
        assert_eq!(member_table_ids(&version, cg_64), vec![64]);
        assert_eq!(member_table_ids(&version, cg_65), vec![65]);
        assert_no_group_overlap(&version);
    }

    #[tokio::test]
    async fn test_purge_stale_compaction_groups_skips_when_creating_jobs_exist() {
        let mut opts = MetaOpts::test(false);
        opts.periodic_purge_stale_compaction_group_interval_sec = 1;

        let (_env, hummock_manager, _, _worker_id) =
            setup_compute_env_with_meta_opts(81, opts).await;
        hummock_manager
            .register_table_ids_for_test(&[(101, 2.into())])
            .await
            .unwrap();

        let catalog_controller = hummock_manager
            .metadata_manager()
            .catalog_controller
            .clone();
        catalog_controller
            .create_user(PbUserInfo {
                name: "purge_skip_user".to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();
        let creating_owner_id = catalog_controller
            .get_user_by_name("purge_skip_user")
            .await
            .unwrap()
            .user_id;
        let creating_job_id = JobId::new(20_001);
        create_creating_sink_job(
            catalog_controller.as_ref(),
            creating_job_id,
            creating_owner_id,
        )
        .await
        .unwrap();

        assert!(
            hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&TableId::new(101))
        );

        hummock_manager.purge_stale_compaction_groups().await;

        assert!(
            hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&TableId::new(101)),
            "stale compaction group should be preserved when creating jobs exist"
        );

        let inner = catalog_controller.get_inner_read_guard().await;
        let txn = inner.db.begin().await.unwrap();
        object::Entity::delete_by_id(creating_job_id.as_object_id())
            .exec(&txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        hummock_manager.purge_stale_compaction_groups().await;

        assert!(
            !hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&TableId::new(101)),
            "stale compaction group should be purged once creating jobs are gone"
        );
    }

    #[tokio::test]
    async fn test_purge_stale_compaction_groups_skips_when_pending_dropped_tables_exist() {
        let mut opts = MetaOpts::test(false);
        opts.periodic_purge_stale_compaction_group_interval_sec = 1;

        let (_env, hummock_manager, _, _worker_id) =
            setup_compute_env_with_meta_opts(82, opts).await;
        hummock_manager
            .register_table_ids_for_test(&[(102, 2.into())])
            .await
            .unwrap();

        let catalog_controller = hummock_manager
            .metadata_manager()
            .catalog_controller
            .clone();
        {
            let mut inner = catalog_controller.get_inner_write_guard().await;
            inner.dropped_tables.insert(
                TableId::new(202),
                PbTable {
                    id: 202.into(),
                    schema_id: Default::default(),
                    database_id: Default::default(),
                    name: "pending_drop_table".to_owned(),
                    columns: vec![],
                    pk: vec![],
                    stream_key: vec![],
                    ..Default::default()
                },
            );
        }

        assert!(
            hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&TableId::new(102))
        );

        hummock_manager.purge_stale_compaction_groups().await;

        assert!(
            hummock_manager
                .get_current_version()
                .await
                .state_table_info
                .info()
                .contains_key(&TableId::new(102)),
            "stale compaction group should be preserved when dropped tables are pending cleanup"
        );
    }
}
