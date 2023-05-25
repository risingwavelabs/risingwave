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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{Either, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use risingwave_common::util::select_all;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::{compact_task, CompactTask};
use tokio::sync::mpsc;
use tokio::sync::oneshot::Receiver;
use tokio_stream::wrappers::{IntervalStream, UnboundedReceiverStream};
use tracing::log::info;

use crate::hummock::compaction::{
    DynamicLevelSelector, LevelSelector, SpaceReclaimCompactionSelector, TtlCompactionSelector,
};
use crate::hummock::compaction_scheduler::{
    CompactionRequestChannel, CompactionRequestChannelItem, SchedulerEvent,
};
use crate::hummock::HummockManagerRef;
use crate::manager::MetaSrvEnv;
use crate::storage::MetaStore;

pub type CompactionManagerRef<S> = Arc<CompactionManager<S>>;
const CHECK_PENDING_TASK_PERIOD_SEC: u64 = 300;

/// Schedules compaction task picking and assignment.
///
/// When no idle compactor is available, the scheduling will be paused until
/// `compaction_resume_notifier` is `notified`. Compaction should only be resumed by calling
/// `HummockManager::try_resume_compaction`. See [`CompactionResumeTrigger`] for all cases that can
/// resume compaction.
pub struct CompactionManager<S>
where
    S: MetaStore,
{
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    compaction_task_tx: mpsc::UnboundedSender<(CompactTask, u64)>,
}

impl<S> CompactionManager<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        compaction_task_tx: mpsc::UnboundedSender<(CompactTask, u64)>,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            compaction_task_tx,
        }
    }

    pub async fn start(mut self, shutdown_rx: Receiver<()>) {
        let (sched_tx, sched_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
        let sched_channel = Arc::new(CompactionRequestChannel::new(sched_tx));

        self.hummock_manager
            .init_compaction_manager(sched_channel.clone());

        tracing::info!("Start compaction scheduler.");

        let compaction_selectors = Self::init_selectors();
        let shutdown_rx = shutdown_rx.shared();
        let schedule_event_stream = Self::scheduler_event_stream(
            sched_rx,
            self.env.opts.periodic_space_reclaim_compaction_interval_sec,
            self.env.opts.periodic_ttl_reclaim_compaction_interval_sec,
            self.env.opts.periodic_compaction_interval_sec,
            self.env.opts.periodic_split_compact_group_interval_sec,
        );
        self.schedule_loop(
            sched_channel.clone(),
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

    async fn schedule_loop(
        &mut self,
        sched_channel: Arc<CompactionRequestChannel>,
        shutdown_rx: Shared<Receiver<()>>,
        mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        event_stream: impl Stream<Item = SchedulerEvent>,
    ) {
        use futures::pin_mut;
        pin_mut!(event_stream);

        let mut group_infos = HashMap::default();
        loop {
            let item = futures::future::select(event_stream.next(), shutdown_rx.clone()).await;
            match item {
                Either::Left((event, _)) => {
                    if let Some(event) = event {
                        match event {
                            SchedulerEvent::Channel((compaction_group, task_type)) => {
                                // recv
                                if !self
                                    .on_handle_compact(
                                        compaction_group,
                                        &mut compaction_selectors,
                                        task_type,
                                        sched_channel.clone(),
                                        shutdown_rx.clone(),
                                    )
                                    .await
                                {
                                    break;
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
                            SchedulerEvent::GroupSplitTrigger => {
                                // Disable periodic trigger for compaction_deterministic_test.
                                if self.env.opts.compaction_deterministic_test {
                                    continue;
                                }
                                self.on_handle_check_split_multi_group(&mut group_infos)
                                    .await;
                            }
                            SchedulerEvent::CheckDeadTaskTrigger => {
                                self.hummock_manager.check_dead_task().await;
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
        &mut self,
        compaction_group: CompactionGroupId,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        task_type: compact_task::TaskType,
        sched_channel: Arc<CompactionRequestChannel>,
        shutdown_rx: Shared<Receiver<()>>,
    ) -> bool {
        sync_point::sync_point!("BEFORE_SCHEDULE_COMPACTION_TASK");
        sched_channel.unschedule(compaction_group, task_type);
        if let Some(compact_task) = self
            .pick_compaction(compaction_group, task_type, compaction_selectors)
            .await
        {
            // send task to task manager

            // FIXME: refactor timeout and handle send error
            const TIMEOUT_SECOND: u64 = 600;
            self.compaction_task_tx
                .send((compact_task, TIMEOUT_SECOND))
                .unwrap();
            true
        } else {
            false
        }
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

    async fn pick_compaction(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
    ) -> Option<CompactTask> {
        let selector: &mut Box<dyn LevelSelector> =
            compaction_selectors.get_mut(&task_type).unwrap();
        let compact_task = self
            .hummock_manager
            .get_compact_task(compaction_group, selector)
            .await;

        let compact_task = match compact_task {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                // return ScheduleStatus::NoTask;
                return None;
            }
            Err(err) => {
                tracing::warn!("Failed to get compaction task: {:#?}.", err);
                // return ScheduleStatus::PickFailure;
                return None;
            }
        };
        tracing::trace!(
            "Picked compaction task. {}",
            compact_task_to_string(&compact_task)
        );

        Some(compact_task)
    }

    async fn on_handle_check_split_multi_group(
        &self,
        history_table_infos: &mut HashMap<StateTableId, VecDeque<u64>>,
    ) {
        const HISTORY_TABLE_INFO_WINDOW_SIZE: usize = 5;
        let mut group_infos = self
            .hummock_manager
            .calculate_compaction_group_statistic()
            .await;
        group_infos.sort_by_key(|group| group.group_size);
        group_infos.reverse();
        let group_size_limit = self.env.opts.split_group_size_limit;
        let table_split_limit = self.env.opts.move_table_size_limit;
        let mut table_infos = vec![];
        // TODO: support move small state-table back to default-group to reduce IOPS.
        for group in &group_infos {
            if group.table_statistic.len() == 1 || group.group_size < group_size_limit {
                continue;
            }
            for (table_id, table_size) in &group.table_statistic {
                let last_table_infos = history_table_infos
                    .entry(*table_id)
                    .or_insert_with(VecDeque::new);
                last_table_infos.push_back(*table_size);
                if last_table_infos.len() > HISTORY_TABLE_INFO_WINDOW_SIZE {
                    last_table_infos.pop_front();
                }
                table_infos.push((*table_id, group.group_id, group.group_size));
            }
        }
        table_infos.sort_by(|a, b| b.2.cmp(&a.2));
        let default_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault.into();
        let mv_group_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView.into();
        for (table_id, parent_group_id, parent_group_size) in table_infos {
            let table_info = history_table_infos.get(&table_id).unwrap();
            let table_size = *table_info.back().unwrap();
            if table_size < table_split_limit
                || (table_size < group_size_limit
                    && table_info.len() < HISTORY_TABLE_INFO_WINDOW_SIZE)
            {
                continue;
            }

            let mut target_compact_group_id = None;
            let mut allow_split_by_table = false;
            if table_size < group_size_limit {
                let mut increase = true;
                for idx in 1..table_info.len() {
                    if table_info[idx] < table_info[idx - 1] {
                        increase = false;
                        break;
                    }
                }

                if !increase {
                    continue;
                }

                // do not split a large table and a small table because it would increase IOPS of
                // small table.
                if parent_group_id != default_group_id && parent_group_id != mv_group_id {
                    let rest_group_size = parent_group_size - table_size;
                    if rest_group_size < table_size && rest_group_size < table_split_limit {
                        continue;
                    }
                }

                let increase_data_size = table_size.saturating_sub(*table_info.front().unwrap());
                let increase_slow = increase_data_size < table_split_limit;

                // if the size of this table increases too fast, we shall create one group for it.
                if increase_slow
                    && (parent_group_id == mv_group_id || parent_group_id == default_group_id)
                {
                    for group in &group_infos {
                        // do not move to mv group or state group
                        if !group.split_by_table || group.group_id == mv_group_id
                            || group.group_id == default_group_id
                            || group.group_id == parent_group_id
                            // do not move state-table to a large group.
                            || group.group_size + table_size > group_size_limit
                            // do not move state-table from group A to group B if this operation would make group B becomes larger than A.
                            || group.group_size + table_size > parent_group_size - table_size
                        {
                            continue;
                        }
                        target_compact_group_id = Some(group.group_id);
                    }
                    allow_split_by_table = true;
                }
            }

            let ret = self
                .hummock_manager
                .move_state_table_to_compaction_group(
                    parent_group_id,
                    &[table_id],
                    target_compact_group_id,
                    allow_split_by_table,
                )
                .await;
            match ret {
                Ok(_) => {
                    info!(
                        "move state table [{}] from group-{} to group-{:?} success",
                        table_id, parent_group_id, target_compact_group_id
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

impl<S> CompactionManager<S>
where
    S: MetaStore,
{
    fn scheduler_event_stream(
        sched_rx: mpsc::UnboundedReceiver<(CompactionGroupId, compact_task::TaskType)>,
        periodic_space_reclaim_compaction_interval_sec: u64,
        periodic_ttl_reclaim_compaction_interval_sec: u64,
        periodic_compaction_interval_sec: u64,
        periodic_check_split_group_interval_sec: u64,
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

        let mut check_compact_trigger_interval =
            tokio::time::interval(Duration::from_secs(CHECK_PENDING_TASK_PERIOD_SEC));
        check_compact_trigger_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        check_compact_trigger_interval.reset();
        let check_compact_trigger = IntervalStream::new(check_compact_trigger_interval)
            .map(|_| SchedulerEvent::CheckDeadTaskTrigger);

        let mut triggers: Vec<BoxStream<'static, SchedulerEvent>> = vec![
            Box::pin(dynamic_channel_trigger),
            Box::pin(dynamic_tick_trigger),
            Box::pin(space_reclaim_trigger),
            Box::pin(ttl_reclaim_trigger),
            Box::pin(check_compact_trigger),
        ];

        if periodic_check_split_group_interval_sec > 0 {
            let mut split_group_trigger_interval =
                tokio::time::interval(Duration::from_secs(periodic_check_split_group_interval_sec));
            split_group_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            split_group_trigger_interval.reset();

            let split_group_trigger = IntervalStream::new(split_group_trigger_interval)
                .map(|_| SchedulerEvent::GroupSplitTrigger);
            triggers.push(Box::pin(split_group_trigger));
        }
        select_all(triggers)
    }
}
