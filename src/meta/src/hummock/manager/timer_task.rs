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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::get_compaction_group_ids;
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
use risingwave_pb::hummock::level_handler::RunningCompactTask;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::backup_restore::BackupManagerRef;
use crate::hummock::metrics_utils::{trigger_lsm_stat, trigger_mv_stat};
use crate::hummock::{HummockManager, TASK_NORMAL};

/// Spawn a periodic background loop.
///
/// Notes on shutdown and cancellation:
/// - At most one handler future is in-flight at any time. If the handler runs longer than the
///   period, the next tick will be delayed (see `MissedTickBehavior::Delay`).
/// - Shutdown can preempt a running handler by dropping its future. Therefore, the handler should
///   avoid long-running synchronous/blocking code and prefer cancellation-safe async operations.
fn spawn_periodic_loop<F, Fut>(
    name: &'static str,
    period: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
    mut handler: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        // `shutdown_rx` is used for the inner select when running the handler. We need another
        // receiver for the outer select to avoid mutable-borrow conflicts.
        let mut shutdown_rx_idle = shutdown_rx.clone();

        let mut trigger_interval = tokio::time::interval(period);
        trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        trigger_interval.reset();

        loop {
            tokio::select! {
                _ = trigger_interval.tick() => {
                    // Allow shutdown to preempt a running handler.
                    let handler_fut = handler();
                    tokio::pin!(handler_fut);

                    tokio::select! {
                        _ = &mut handler_fut => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                tracing::info!("Hummock timer handler loop [{}] is stopped", name);
                                break;
                            }
                        }
                    }
                }
                changed = shutdown_rx_idle.changed() => {
                    if changed.is_err() || *shutdown_rx_idle.borrow() {
                        tracing::info!("Hummock timer handler loop [{}] is stopped", name);
                        break;
                    }
                }
            }
        }
    })
}

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

            let periodic_scheduling_compaction_group_split_interval_sec = hummock_manager
                .env
                .opts
                .periodic_scheduling_compaction_group_split_interval_sec;
            let periodic_scheduling_compaction_group_merge_interval_sec = hummock_manager
                .env
                .opts
                .periodic_scheduling_compaction_group_merge_interval_sec;

            tracing::info!(
                "Hummock timer task [GroupSchedulingSplit interval {} sec] [GroupSchedulingMerge interval {} sec] [CheckDeadTask interval {} sec] [Report interval {} sec] [CompactionHeartBeat interval {} sec]",
                periodic_scheduling_compaction_group_split_interval_sec,
                periodic_scheduling_compaction_group_merge_interval_sec,
                CHECK_PENDING_TASK_PERIOD_SEC,
                STAT_REPORT_PERIOD_SEC,
                COMPACTION_HEARTBEAT_PERIOD_SEC
            );

            let (child_shutdown_tx, child_shutdown_rx) = watch::channel(false);
            let split_merge_mutex = Arc::new(Mutex::new(()));
            let mut child_handles: Vec<(&'static str, JoinHandle<()>)> = Vec::new();

            macro_rules! spawn_loop {
                ($name:literal, $period:expr, $handler:expr $(,)?) => {{
                    child_handles.push((
                        $name,
                        spawn_periodic_loop($name, $period, child_shutdown_rx.clone(), $handler),
                    ));
                }};
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "check_dead_task",
                    Duration::from_secs(CHECK_PENDING_TASK_PERIOD_SEC),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            hummock_manager.check_dead_task().await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "report",
                    Duration::from_secs(STAT_REPORT_PERIOD_SEC),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            hummock_manager.handle_timer_report().await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "compaction_heartbeat_expired_check",
                    Duration::from_secs(COMPACTION_HEARTBEAT_PERIOD_SEC),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            hummock_manager
                                .handle_timer_compaction_heartbeat_expired_check()
                                .await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "dynamic_compaction_trigger",
                    Duration::from_secs(hummock_manager.env.opts.periodic_compaction_interval_sec),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            hummock_manager
                                .on_handle_trigger_multi_group(compact_task::TaskType::Dynamic)
                                .await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "space_reclaim_compaction_trigger",
                    Duration::from_secs(
                        hummock_manager
                            .env
                            .opts
                            .periodic_space_reclaim_compaction_interval_sec,
                    ),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            hummock_manager
                                .on_handle_trigger_multi_group(compact_task::TaskType::SpaceReclaim)
                                .await;
                            // share the same trigger with SpaceReclaim
                            hummock_manager
                                .on_handle_trigger_multi_group(
                                    compact_task::TaskType::VnodeWatermark,
                                )
                                .await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "ttl_compaction_trigger",
                    Duration::from_secs(
                        hummock_manager
                            .env
                            .opts
                            .periodic_ttl_reclaim_compaction_interval_sec,
                    ),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            hummock_manager
                                .on_handle_trigger_multi_group(compact_task::TaskType::Ttl)
                                .await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                spawn_loop!(
                    "tombstone_compaction_trigger",
                    Duration::from_secs(
                        hummock_manager
                            .env
                            .opts
                            .periodic_tombstone_reclaim_compaction_interval_sec,
                    ),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            hummock_manager
                                .on_handle_trigger_multi_group(compact_task::TaskType::Tombstone)
                                .await;
                        }
                    },
                );
            }

            {
                let hummock_manager = hummock_manager.clone();
                let backup_manager = backup_manager.clone();
                spawn_loop!(
                    "full_gc",
                    Duration::from_secs(hummock_manager.env.opts.full_gc_interval_sec),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        let backup_manager = backup_manager.clone();
                        async move {
                            let retention_sec = hummock_manager.env.opts.min_sst_retention_time_sec;
                            let _ = hummock_manager
                                .start_full_gc(
                                    Duration::from_secs(retention_sec),
                                    None,
                                    backup_manager,
                                )
                                .await
                                .inspect_err(
                                    |e| warn!(error = %e.as_report(), "Failed to start GC."),
                                );
                        }
                    },
                );
            }

            if periodic_scheduling_compaction_group_split_interval_sec > 0 {
                let hummock_manager = hummock_manager.clone();
                let split_merge_mutex = split_merge_mutex.clone();
                spawn_loop!(
                    "group_schedule_split",
                    Duration::from_secs(periodic_scheduling_compaction_group_split_interval_sec),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        let split_merge_mutex = split_merge_mutex.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            let _guard = split_merge_mutex.lock().await;
                            hummock_manager.on_handle_schedule_group_split().await;
                        }
                    },
                );
            }

            if periodic_scheduling_compaction_group_merge_interval_sec > 0 {
                let hummock_manager = hummock_manager.clone();
                let split_merge_mutex = split_merge_mutex.clone();
                spawn_loop!(
                    "group_schedule_merge",
                    Duration::from_secs(periodic_scheduling_compaction_group_merge_interval_sec),
                    move || {
                        let hummock_manager = hummock_manager.clone();
                        let split_merge_mutex = split_merge_mutex.clone();
                        async move {
                            if hummock_manager.env.opts.compaction_deterministic_test {
                                return;
                            }
                            let _guard = split_merge_mutex.lock().await;
                            hummock_manager.on_handle_schedule_group_merge().await;
                        }
                    },
                );
            }

            let _ = shutdown_rx.await;
            let _ = child_shutdown_tx.send(true);

            for (name, handle) in child_handles {
                if let Err(e) = handle.await {
                    warn!(
                        handler = name,
                        error = %e.as_report(),
                        "Hummock timer handler loop failed"
                    );
                }
            }

            tracing::info!("Hummock timer loop is stopped");
        });
        (join_handle, shutdown_tx)
    }

    async fn handle_timer_report(&self) {
        // Avoid holding the `versioning` lock across await points to prevent potential deadlocks.
        let (current_version, version_stats) = {
            let versioning_guard = self.versioning.read().await;
            (
                versioning_guard.current_version.clone(),
                versioning_guard.version_stats.clone(),
            )
        };
        let id_to_config = self.get_compaction_group_map().await;

        if let Some(mv_id_to_all_table_ids) = self
            .metadata_manager
            .get_job_id_to_internal_table_ids_mapping()
            .await
        {
            trigger_mv_stat(&self.metrics, &version_stats, mv_id_to_all_table_ids);
        }

        for compaction_group_id in get_compaction_group_ids(&current_version) {
            let Some(compaction_group_config) = id_to_config.get(&compaction_group_id) else {
                warn!(
                    compaction_group_id = %compaction_group_id,
                    "Missing compaction group config when reporting metrics. Skip."
                );
                continue;
            };

            let group_levels =
                current_version.get_compaction_group_levels(compaction_group_config.group_id());

            trigger_lsm_stat(
                &self.metrics,
                compaction_group_config.compaction_config(),
                group_levels,
                compaction_group_config.group_id(),
            )
        }

        let group_infos = self.calculate_compaction_group_statistic().await;
        let compaction_group_count = group_infos.len();
        self.metrics
            .compaction_group_count
            .set(compaction_group_count as i64);

        let table_write_throughput_statistic_manager =
            self.table_write_throughput_statistic_manager.read().clone();
        let versioning_guard = self.versioning.read().await;
        let current_version_levels = &versioning_guard.current_version.levels;

        for group_info in group_infos {
            self.metrics
                .compaction_group_size
                .with_label_values(&[&group_info.group_id.to_string()])
                .set(group_info.group_size as _);
            // accumulate the throughput of all tables in the group
            let mut avg_throuput = 0;
            let max_statistic_expired_time = std::cmp::max(
                self.env.opts.table_stat_throuput_window_seconds_for_split,
                self.env.opts.table_stat_throuput_window_seconds_for_merge,
            );
            for table_id in group_info.table_statistic.keys() {
                avg_throuput += table_write_throughput_statistic_manager
                    .avg_write_throughput(*table_id, max_statistic_expired_time as i64)
                    as u64;
            }

            self.metrics
                .compaction_group_throughput
                .with_label_values(&[&group_info.group_id.to_string()])
                .set(avg_throuput as _);

            if let Some(group_levels) = current_version_levels.get(&group_info.group_id) {
                let file_count = group_levels.count_ssts();
                self.metrics
                    .compaction_group_file_count
                    .with_label_values(&[&group_info.group_id.to_string()])
                    .set(file_count as _);
            }
        }
    }

    async fn handle_timer_compaction_heartbeat_expired_check(&self) {
        // TODO: add metrics to track expired tasks
        // The cancel task has two paths
        // 1. compactor heartbeat cancels the expired task based on task
        // progress (meta + compactor)
        // 2. meta periodically scans the task and performs a cancel on
        // the meta side for tasks that are not updated by heartbeat
        let expired_tasks: Vec<u64> = self
            .compactor_manager
            .get_heartbeat_expired_tasks()
            .into_iter()
            .map(|task| task.task_id)
            .collect();
        if !expired_tasks.is_empty() {
            tracing::info!(
                expired_tasks = ?expired_tasks,
                "Heartbeat expired compaction tasks detected. Attempting to cancel tasks.",
            );
            if let Err(e) = self
                .cancel_compact_tasks(expired_tasks.clone(), TaskStatus::HeartbeatCanceled)
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
}

impl HummockManager {
    async fn check_dead_task(&self) {
        const MAX_COMPACTION_L0_MULTIPLIER: u64 = 32;
        const MAX_COMPACTION_DURATION_SEC: u64 = 20 * 60;
        // Avoid holding the `versioning` lock across await points to prevent potential deadlocks.
        let groups = {
            let versioning_guard = self.versioning.read().await;
            versioning_guard
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
                .collect_vec()
        };
        let configs = self.get_compaction_group_map().await;
        let mut slowdown_groups: HashMap<CompactionGroupId, u64> = HashMap::default();
        {
            for (group_id, l0_file_size) in groups {
                let Some(group) = configs.get(&group_id) else {
                    warn!(
                        group_id = %group_id,
                        "Missing compaction group config when checking dead task. Skip."
                    );
                    continue;
                };
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
        let mut group_infos = self.calculate_compaction_group_statistic().await;
        group_infos.sort_by_key(|group| group.group_size);
        group_infos.reverse();

        for group in group_infos {
            if group.table_statistic.len() == 1 {
                // no need to handle the separate compaciton group
                continue;
            }

            self.try_split_compaction_group(&table_write_throughput, group)
                .await;
        }
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
        group_infos.sort_by_key(|group| {
            let table_ids = group
                .table_statistic
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            table_ids.iter().next().cloned()
        });

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
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::{Mutex, Notify, watch};

    use super::spawn_periodic_loop;

    #[tokio::test]
    async fn test_spawn_periodic_loop_isolated_progress() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let slow_counter = Arc::new(AtomicUsize::new(0));
        let fast_counter = Arc::new(AtomicUsize::new(0));

        let slow_handle = spawn_periodic_loop(
            "test_slow",
            Duration::from_millis(10),
            shutdown_rx.clone(),
            {
                let slow_counter = slow_counter.clone();
                move || {
                    let slow_counter = slow_counter.clone();
                    async move {
                        slow_counter.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(30)).await;
                    }
                }
            },
        );

        let fast_handle =
            spawn_periodic_loop("test_fast", Duration::from_millis(10), shutdown_rx, {
                let fast_counter = fast_counter.clone();
                move || {
                    let fast_counter = fast_counter.clone();
                    async move {
                        fast_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

        tokio::time::sleep(Duration::from_millis(120)).await;
        shutdown_tx.send(true).unwrap();
        slow_handle.await.unwrap();
        fast_handle.await.unwrap();

        let slow = slow_counter.load(Ordering::Relaxed);
        let fast = fast_counter.load(Ordering::Relaxed);
        assert!(fast > 0);
        assert!(fast > slow);
    }

    #[tokio::test]
    async fn test_shared_mutex_guards_concurrent_handlers() {
        struct DecrOnDrop(Arc<AtomicUsize>);

        impl Drop for DecrOnDrop {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let scheduling_mutex = Arc::new(Mutex::new(()));
        let in_critical = Arc::new(AtomicUsize::new(0));
        let max_in_critical = Arc::new(AtomicUsize::new(0));

        let split_handle = spawn_periodic_loop(
            "test_split",
            Duration::from_millis(10),
            shutdown_rx.clone(),
            {
                let scheduling_mutex = scheduling_mutex.clone();
                let in_critical = in_critical.clone();
                let max_in_critical = max_in_critical.clone();
                move || {
                    let scheduling_mutex = scheduling_mutex.clone();
                    let in_critical = in_critical.clone();
                    let max_in_critical = max_in_critical.clone();
                    async move {
                        let _guard = scheduling_mutex.lock().await;
                        let now = in_critical.fetch_add(1, Ordering::SeqCst) + 1;
                        let _decr = DecrOnDrop(in_critical);
                        max_in_critical.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(15)).await;
                    }
                }
            },
        );

        let merge_handle =
            spawn_periodic_loop("test_merge", Duration::from_millis(10), shutdown_rx, {
                let scheduling_mutex = scheduling_mutex.clone();
                let in_critical = in_critical.clone();
                let max_in_critical = max_in_critical.clone();
                move || {
                    let scheduling_mutex = scheduling_mutex.clone();
                    let in_critical = in_critical.clone();
                    let max_in_critical = max_in_critical.clone();
                    async move {
                        let _guard = scheduling_mutex.lock().await;
                        let now = in_critical.fetch_add(1, Ordering::SeqCst) + 1;
                        let _decr = DecrOnDrop(in_critical);
                        max_in_critical.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(15)).await;
                    }
                }
            });

        tokio::time::sleep(Duration::from_millis(120)).await;
        shutdown_tx.send(true).unwrap();
        split_handle.await.unwrap();
        merge_handle.await.unwrap();

        assert_eq!(max_in_critical.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_spawn_periodic_loop_shutdown_preempts_handler() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = Arc::new(Notify::new());

        let handle = spawn_periodic_loop("test_preempt", Duration::from_millis(10), shutdown_rx, {
            let started = started.clone();
            move || {
                let started = started.clone();
                async move {
                    started.notify_one();
                    // A never-ending handler which should be cancelled by shutdown.
                    pending::<()>().await;
                }
            }
        });

        started.notified().await;
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("handler loop should stop promptly")
            .unwrap();
    }
}
