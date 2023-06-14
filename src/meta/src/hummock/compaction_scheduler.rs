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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{Either, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use risingwave_common::util::select_all;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::table_stats::PbTableStatsMap;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio_stream::wrappers::{IntervalStream, UnboundedReceiverStream};

use super::Compactor;
use crate::hummock::compaction::{
    DynamicLevelSelector, LevelSelector, SpaceReclaimCompactionSelector, TtlCompactionSelector,
};
use crate::hummock::error::Error;
use crate::hummock::{CompactorManagerRef, HummockManagerRef};
use crate::manager::{LocalNotification, MetaSrvEnv};
use crate::storage::MetaStore;

pub type CompactionSchedulerRef<S> = Arc<CompactionScheduler<S>>;
pub type CompactionRequestChannelRef = Arc<CompactionRequestChannel>;
const HISTORY_TABLE_INFO_WINDOW_SIZE: usize = 16;

#[derive(Clone, Debug)]
pub enum CompactionRequestItem {
    Compact {
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    },
    SplitLargeGroup(PbTableStatsMap),
}

/// [`CompactionRequestChannel`] wrappers a mpsc channel and deduplicate requests from same
/// compaction groups.
pub struct CompactionRequestChannel {
    request_tx: UnboundedSender<CompactionRequestItem>,
    scheduled: Mutex<HashSet<(CompactionGroupId, compact_task::TaskType)>>,
}

#[derive(Debug, PartialEq)]
pub enum ScheduleStatus {
    Ok,
    NoTask,
    PickFailure,
    AssignFailure(CompactTask),
    SendFailure(CompactTask),
}

impl CompactionRequestChannel {
    pub fn new(request_tx: UnboundedSender<CompactionRequestItem>) -> Self {
        Self {
            request_tx,
            scheduled: Default::default(),
        }
    }

    /// Enqueues only if the target is not yet in queue.
    pub fn try_sched_compaction(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) -> Result<bool, SendError<CompactionRequestItem>> {
        let mut guard = self.scheduled.lock();
        let key = (compaction_group, task_type);
        if guard.contains(&key) {
            return Ok(false);
        }
        self.request_tx.send(CompactionRequestItem::Compact {
            compaction_group,
            task_type,
        })?;
        guard.insert(key);
        Ok(true)
    }

    /// Enqueues only if the target is not yet in queue.
    pub fn try_split_groups(
        &self,
        stats: PbTableStatsMap,
    ) -> Result<(), SendError<CompactionRequestItem>> {
        self.request_tx
            .send(CompactionRequestItem::SplitLargeGroup(stats))
    }

    pub fn unschedule(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) {
        self.scheduled.lock().remove(&(compaction_group, task_type));
    }
}

/// Schedules compaction task picking and assignment.
pub struct CompactionScheduler<S>
where
    S: MetaStore,
{
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
}

impl<S> CompactionScheduler<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            compactor_manager,
        }
    }

    pub async fn start(&self, shutdown_rx: Receiver<()>) {
        let (sched_tx, sched_rx) = tokio::sync::mpsc::unbounded_channel::<CompactionRequestItem>();
        let sched_channel = Arc::new(CompactionRequestChannel::new(sched_tx));

        self.hummock_manager
            .init_compaction_scheduler(sched_channel.clone());
        tracing::info!("Start compaction scheduler.");

        let compaction_selectors = Self::init_selectors();
        let shutdown_rx = shutdown_rx.shared();
        let schedule_event_stream = Self::scheduler_event_stream(
            sched_rx,
            self.env.opts.periodic_space_reclaim_compaction_interval_sec,
            self.env.opts.periodic_ttl_reclaim_compaction_interval_sec,
            self.env.opts.periodic_compaction_interval_sec,
        );
        self.schedule_loop(
            sched_channel,
            shutdown_rx,
            compaction_selectors,
            schedule_event_stream,
        )
        .await;
    }

    fn init_selectors() -> HashMap<compact_task::TaskType, Box<dyn LevelSelector>> {
        let mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn LevelSelector>> =
            HashMap::default();
        compaction_selectors.insert(
            compact_task::TaskType::Dynamic,
            Box::<DynamicLevelSelector>::default(),
        );
        compaction_selectors.insert(
            compact_task::TaskType::SpaceReclaim,
            Box::<SpaceReclaimCompactionSelector>::default(),
        );
        compaction_selectors.insert(
            compact_task::TaskType::Ttl,
            Box::<TtlCompactionSelector>::default(),
        );
        compaction_selectors
    }

    /// Tries to pick a compaction task, schedule it to a compactor.
    ///
    /// Returns true if a task is successfully picked and sent.
    async fn pick_and_assign(
        &self,
        compaction_group: CompactionGroupId,
        compactor: Arc<Compactor>,
        sched_channel: Arc<CompactionRequestChannel>,
        selector: &mut Box<dyn LevelSelector>,
    ) -> ScheduleStatus {
        let schedule_status = self
            .pick_and_assign_impl(compaction_group, compactor, sched_channel, selector)
            .await;

        let cancel_state = match &schedule_status {
            ScheduleStatus::Ok => None,
            ScheduleStatus::NoTask | ScheduleStatus::PickFailure => None,
            ScheduleStatus::AssignFailure(task) => {
                Some((task.clone(), TaskStatus::AssignFailCanceled))
            }
            ScheduleStatus::SendFailure(task) => Some((task.clone(), TaskStatus::SendFailCanceled)),
        };

        if let Some((mut compact_task, task_state)) = cancel_state {
            // Try to cancel task immediately.
            if let Err(err) = self
                .hummock_manager
                .cancel_compact_task(&mut compact_task, task_state)
                .await
            {
                // Cancel task asynchronously.
                tracing::warn!(
                    "Failed to cancel task {}. {}. {:?} It will be cancelled asynchronously.",
                    compact_task.task_id,
                    err,
                    task_state
                );
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::CompactionTaskNeedCancel(
                        compact_task,
                    ))
                    .await;
            }
        }
        schedule_status
    }

    async fn pick_and_assign_impl(
        &self,
        compaction_group: CompactionGroupId,
        compactor: Arc<Compactor>,
        sched_channel: Arc<CompactionRequestChannel>,
        selector: &mut Box<dyn LevelSelector>,
    ) -> ScheduleStatus {
        // 1. Pick a compaction task.
        let compact_task = self
            .hummock_manager
            .get_compact_task(compaction_group, selector)
            .await;

        let compact_task = match compact_task {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                return ScheduleStatus::NoTask;
            }
            Err(err) => {
                tracing::warn!("Failed to get compaction task: {:#?}.", err);
                return ScheduleStatus::PickFailure;
            }
        };
        tracing::trace!(
            "Picked compaction task. {}",
            compact_task_to_string(&compact_task)
        );

        // 2. Assign the compaction task to a compactor.
        match self
            .hummock_manager
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
        {
            Ok(_) => {
                tracing::trace!(
                    "Assigned compaction task. {}",
                    compact_task_to_string(&compact_task)
                );
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to assign {:?} compaction task to compactor {} : {:#?}",
                    compact_task.task_type().as_str_name(),
                    compactor.context_id(),
                    err
                );
                match err {
                    Error::CompactionTaskAlreadyAssigned(_, _) => {
                        panic!("Compaction scheduler is the only tokio task that can assign task.");
                    }
                    Error::InvalidContext(context_id) => {
                        self.compactor_manager.remove_compactor(context_id);
                        return ScheduleStatus::AssignFailure(compact_task);
                    }
                    _ => {
                        return ScheduleStatus::AssignFailure(compact_task);
                    }
                }
            }
        };

        // 3. Send the compaction task.
        if let Err(e) = compactor
            .send_task(Task::CompactTask(compact_task.clone()))
            .await
        {
            tracing::warn!(
                "Failed to send task {} to {}. {:#?}",
                compact_task.task_id,
                compactor.context_id(),
                e
            );
            self.compactor_manager
                .pause_compactor(compactor.context_id());
            return ScheduleStatus::SendFailure(compact_task);
        }

        // Bypass reschedule if we want compaction scheduling in a deterministic way
        if self.env.opts.compaction_deterministic_test {
            return ScheduleStatus::Ok;
        }

        // 4. Reschedule it with best effort, in case there are more tasks.
        if let Err(e) =
            sched_channel.try_sched_compaction(compaction_group, compact_task.task_type())
        {
            tracing::error!(
                "Failed to reschedule compaction group {} after sending new task {}. {:#?}",
                compaction_group,
                compact_task.task_id,
                e
            );
        }
        ScheduleStatus::Ok
    }

    async fn schedule_loop(
        &self,
        sched_channel: Arc<CompactionRequestChannel>,
        shutdown_rx: Shared<Receiver<()>>,
        mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        event_stream: impl Stream<Item = SchedulerEvent>,
    ) {
        use futures::pin_mut;
        pin_mut!(event_stream);

        loop {
            let item = futures::future::select(event_stream.next(), shutdown_rx.clone()).await;
            match item {
                Either::Left((event, _)) => {
                    if let Some(event) = event {
                        match event {
                            SchedulerEvent::Channel(item) => {
                                // recv
                                match item {
                                    CompactionRequestItem::Compact {
                                        compaction_group,
                                        task_type,
                                    } => {
                                        if !self
                                            .on_handle_compact(
                                                compaction_group,
                                                &mut compaction_selectors,
                                                task_type,
                                                sched_channel.clone(),
                                            )
                                            .await
                                        {
                                            self.hummock_manager
                                                .metrics
                                                .compact_skip_frequency
                                                .with_label_values(&["total", "no-compactor"])
                                                .inc();
                                        }
                                    }
                                    CompactionRequestItem::SplitLargeGroup(stats) => {
                                        self.collect_table_write_throughput(
                                            stats,
                                            &mut group_infos,
                                        );
                                    }
                                }
                            }
                            SchedulerEvent::DynamicTrigger => {
                                // Disable periodic trigger for compaction_deterministic_test.
                                if self.env.opts.compaction_deterministic_test {
                                    continue;
                                }
                                // Periodically trigger compaction for all compaction groups.
                                self.on_handle_trigger_multi_grouop(
                                    sched_channel.clone(),
                                    compact_task::TaskType::Dynamic,
                                )
                                .await;
                            }
                            SchedulerEvent::SpaceReclaimTrigger => {
                                // Disable periodic trigger for compaction_deterministic_test.
                                if self.env.opts.compaction_deterministic_test {
                                    continue;
                                }
                                // Periodically trigger compaction for all compaction groups.
                                self.on_handle_trigger_multi_grouop(
                                    sched_channel.clone(),
                                    compact_task::TaskType::SpaceReclaim,
                                )
                                .await;
                            }
                            SchedulerEvent::TtlReclaimTrigger => {
                                // Disable periodic trigger for compaction_deterministic_test.
                                if self.env.opts.compaction_deterministic_test {
                                    continue;
                                }
                                // Periodically trigger compaction for all compaction groups.
                                self.on_handle_trigger_multi_grouop(
                                    sched_channel.clone(),
                                    compact_task::TaskType::Ttl,
                                )
                                .await;
                            }
                        }
                    }
                }

                Either::Right((_, _shutdown)) => {
                    break;
                }
            }
        }
    }

    async fn on_handle_compact(
        &self,
        compaction_group: CompactionGroupId,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        task_type: compact_task::TaskType,
        sched_channel: Arc<CompactionRequestChannel>,
    ) -> bool {
        sync_point::sync_point!("BEFORE_SCHEDULE_COMPACTION_TASK");
        sched_channel.unschedule(compaction_group, task_type);

        self.task_dispatch(
            compaction_group,
            task_type,
            compaction_selectors,
            sched_channel,
        )
        .await
    }

    async fn on_handle_trigger_multi_grouop(
        &self,
        sched_channel: Arc<CompactionRequestChannel>,
        task_type: compact_task::TaskType,
    ) {
        for cg_id in self.hummock_manager.compaction_group_ids().await {
            if let Err(e) = sched_channel.try_sched_compaction(cg_id, task_type) {
                tracing::warn!(
                    "Failed to schedule {:?} compaction for compaction group {}. {}",
                    task_type,
                    cg_id,
                    e
                );
            }
        }
    }

    async fn task_dispatch(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        sched_channel: Arc<CompactionRequestChannel>,
    ) -> bool {
        // Wait for a compactor to become available.
        let compactor = match self.hummock_manager.get_idle_compactor().await {
            Some(compactor) => compactor,
            None => return false,
        };
        let selector = compaction_selectors.get_mut(&task_type).unwrap();
        self.pick_and_assign(compaction_group, compactor, sched_channel, selector)
            .await;

        true
    }

    fn collect_table_write_throughput(
        &self,
        table_stats: PbTableStatsMap,
        table_infos: &mut HashMap<u32, VecDeque<u64>>,
    ) {
        for (table_id, stat) in table_stats {
            let throughput = (stat.total_value_size + stat.total_key_size) as u64;
            let entry = table_infos.entry(table_id).or_default();
            entry.push_back(throughput);
            if entry.len() > HISTORY_TABLE_INFO_WINDOW_SIZE {
                entry.pop_front();
            }
        }
    }

    async fn on_handle_check_split_multi_group(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
    ) {
        let mut group_infos = self
            .hummock_manager
            .calculate_compaction_group_statistic()
            .await;
        group_infos.sort_by_key(|group| group.group_size);
        group_infos.reverse();
        let group_size_limit = self.env.opts.split_group_size_limit;
        let table_split_limit = self.env.opts.move_table_size_limit;
        let default_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault.into();
        let mv_group_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView.into();
        let mut partition_vnode_count = self.env.opts.partition_vnode_count;
        for group in &group_infos {
            if group.table_statistic.len() == 1 {
                continue;
            }

            for (table_id, table_size) in &group.table_statistic {
                let mut is_high_write_throughput = false;
                let mut is_low_write_throughput = true;
                if let Some(history) = table_write_throughput.get(table_id) {
                    if history.len() >= HISTORY_TABLE_INFO_WINDOW_SIZE {
                        let window_total_size = history.iter().sum::<u64>();
                        is_high_write_throughput = history.iter().all(|throughput| {
                            *throughput > self.env.opts.table_write_throughput_threshold
                        });
                        is_low_write_throughput = window_total_size
                            < (HISTORY_TABLE_INFO_WINDOW_SIZE as u64)
                                * self.env.opts.min_table_split_write_throughput;
                    }
                }

                if *table_size < group_size_limit && !is_high_write_throughput {
                    continue;
                }

                let parent_group_id = group.group_id;
                let mut target_compact_group_id = None;
                let mut allow_split_by_table = false;
                if *table_size > group_size_limit && is_low_write_throughput {
                    // do not split a large table and a small table because it would increase IOPS
                    // of small table.
                    if parent_group_id != default_group_id && parent_group_id != mv_group_id {
                        let rest_group_size = group.group_size - *table_size;
                        if rest_group_size < *table_size && rest_group_size < table_split_limit {
                            continue;
                        }
                    } else {
                        for group in &group_infos {
                            // do not move to mv group or state group
                            if !group.split_by_table || group.group_id == mv_group_id
                                || group.group_id == default_group_id
                                || group.group_id == parent_group_id
                                // do not move state-table to a large group.
                                || group.group_size + *table_size > group_size_limit
                                // do not move state-table from group A to group B if this operation would make group B becomes larger than A.
                                || group.group_size + *table_size > group.group_size - table_size
                            {
                                continue;
                            }
                            target_compact_group_id = Some(group.group_id);
                        }
                        allow_split_by_table = true;
                        partition_vnode_count = 1;
                    }
                }

                let ret = self
                    .hummock_manager
                    .move_state_table_to_compaction_group(
                        parent_group_id,
                        &[*table_id],
                        target_compact_group_id,
                        allow_split_by_table,
                        partition_vnode_count,
                    )
                    .await;
                match ret {
                    Ok(_) => {
                        info!(
                        "move state table [{}] from group-{} to group-{:?} success, Allow split by table: {}",
                        table_id, parent_group_id, target_compact_group_id, allow_split_by_table
                    );
                        return;
                    }
                    Err(e) => info!(
                        "failed to move state table [{}] from group-{} to group-{:?} because {:?}",
                        table_id, parent_group_id, target_compact_group_id, e
                    ),
                }
            }
        }
    }
}

#[derive(Clone)]
pub enum SchedulerEvent {
    Channel(CompactionRequestItem),
    DynamicTrigger,
    SpaceReclaimTrigger,
    TtlReclaimTrigger,
}

impl<S> CompactionScheduler<S>
where
    S: MetaStore,
{
    fn scheduler_event_stream(
        sched_rx: UnboundedReceiver<CompactionRequestItem>,
        periodic_space_reclaim_compaction_interval_sec: u64,
        periodic_ttl_reclaim_compaction_interval_sec: u64,
        periodic_compaction_interval_sec: u64,
    ) -> impl Stream<Item = SchedulerEvent> {
        let dynamic_channel_trigger =
            UnboundedReceiverStream::new(sched_rx).map(SchedulerEvent::Channel);

        let mut min_trigger_interval =
            tokio::time::interval(Duration::from_secs(periodic_compaction_interval_sec));
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        min_trigger_interval.reset();
        let dynamic_tick_trigger =
            IntervalStream::new(min_trigger_interval).map(|_| SchedulerEvent::DynamicTrigger);

        let mut min_space_reclaim_trigger_interval = tokio::time::interval(Duration::from_secs(
            periodic_space_reclaim_compaction_interval_sec,
        ));
        min_space_reclaim_trigger_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        min_space_reclaim_trigger_interval.reset();
        let space_reclaim_trigger = IntervalStream::new(min_space_reclaim_trigger_interval)
            .map(|_| SchedulerEvent::SpaceReclaimTrigger);

        let mut min_ttl_reclaim_trigger_interval = tokio::time::interval(Duration::from_secs(
            periodic_ttl_reclaim_compaction_interval_sec,
        ));
        min_ttl_reclaim_trigger_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        min_ttl_reclaim_trigger_interval.reset();
        let ttl_reclaim_trigger = IntervalStream::new(min_ttl_reclaim_trigger_interval)
            .map(|_| SchedulerEvent::TtlReclaimTrigger);

        let triggers: Vec<BoxStream<'static, SchedulerEvent>> = vec![
            Box::pin(dynamic_channel_trigger),
            Box::pin(dynamic_tick_trigger),
            Box::pin(space_reclaim_trigger),
            Box::pin(ttl_reclaim_trigger),
        ];
        select_all(triggers)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;

    use crate::hummock::compaction::default_level_selector;
    use crate::hummock::compaction_scheduler::{
        CompactionRequestChannel, CompactionRequestItem, ScheduleStatus,
    };
    use crate::hummock::test_utils::{add_ssts, setup_compute_env};
    use crate::hummock::CompactionScheduler;

    #[tokio::test]
    async fn test_pick_and_assign() {
        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let compaction_scheduler =
            CompactionScheduler::new(env, hummock_manager.clone(), compactor_manager.clone());

        let (request_tx, _request_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestItem>();
        let request_channel = Arc::new(CompactionRequestChannel::new(request_tx));

        // Add a compactor with invalid context_id.
        let _receiver = compactor_manager.add_compactor(1234, 1, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);

        // No task
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::NoTask,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );

        let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;

        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        // Cannot assign because of invalid compactor
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::AssignFailure(_)
        );
        assert_eq!(compactor_manager.compactor_num(), 0);

        // Add a valid compactor and succeed
        let _receiver = compactor_manager.add_compactor(context_id, 1, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::Ok,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );

        // Add more SSTs for compaction.
        let _sst_infos = add_ssts(2, hummock_manager.as_ref(), context_id).await;

        // No idle compactor
        assert_eq!(
            hummock_manager.get_assigned_tasks_number(context_id).await,
            1
        );
        assert_eq!(compactor_manager.compactor_num(), 1);
        assert_matches!(hummock_manager.get_idle_compactor().await, None);

        // Increase compactor concurrency and succeed
        let _receiver = compactor_manager.add_compactor(context_id, 10, 10);
        assert_eq!(
            hummock_manager.get_assigned_tasks_number(context_id).await,
            1
        );
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::Ok,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );
        assert_eq!(
            hummock_manager.get_assigned_tasks_number(context_id).await,
            2
        );
    }

    #[tokio::test]
    #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints() {
        use risingwave_pb::hummock::compact_task::TaskStatus;

        use crate::manager::LocalNotification;

        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        env.notification_manager().clear_local_sender().await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let compaction_scheduler = CompactionScheduler::new(
            env.clone(),
            hummock_manager.clone(),
            compactor_manager.clone(),
        );

        let (request_tx, _request_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
        let request_channel = Arc::new(CompactionRequestChannel::new(request_tx));

        let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
        let _receiver = compactor_manager.add_compactor(context_id, 1, 1);

        // Pick failure
        let fp_get_compact_task = "fp_get_compact_task";
        fail::cfg(fp_get_compact_task, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::PickFailure,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );
        fail::remove(fp_get_compact_task);

        // Assign failed and task cancelled.
        let fp_assign_compaction_task_fail = "assign_compaction_task_fail";
        fail::cfg(fp_assign_compaction_task_fail, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::AssignFailure(_)
        );
        fail::remove(fp_assign_compaction_task_fail);
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());

        // Send failed and task cancelled.
        let fp_compaction_send_task_fail = "compaction_send_task_fail";
        fail::cfg(fp_compaction_send_task_fail, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::SendFailure(_)
        );
        fail::remove(fp_compaction_send_task_fail);
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());

        // There is no idle compactor, because the compactor is paused after send failure.
        assert_matches!(hummock_manager.get_idle_compactor().await, None);
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());
        let _receiver = compactor_manager.add_compactor(context_id, 1, 1);

        // Assign failed and task cancellation failed.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        env.notification_manager().insert_local_sender(tx).await;
        let fp_cancel_compact_task = "fp_cancel_compact_task";
        fail::cfg(fp_assign_compaction_task_fail, "return").unwrap();
        fail::cfg(fp_cancel_compact_task, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::AssignFailure(_)
        );
        fail::remove(fp_assign_compaction_task_fail);
        fail::remove(fp_cancel_compact_task);
        assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 1);
        // Notified to retry cancellation.
        if let LocalNotification::CompactionTaskNeedCancel(mut task_to_cancel) =
            rx.recv().await.unwrap()
        {
            hummock_manager
                .cancel_compact_task(&mut task_to_cancel, TaskStatus::ManualCanceled)
                .await
                .unwrap();
        };

        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());

        // Succeeded.
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::Ok
        );
        assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 1);
    }
}
