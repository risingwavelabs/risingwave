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
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::compact_task::{self, TaskType};
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio_stream::wrappers::{IntervalStream, UnboundedReceiverStream};

use crate::hummock::compaction::{
    DynamicLevelSelector, LevelSelector, SpaceReclaimCompactionSelector, TtlCompactionSelector,
};
use crate::hummock::{CompactorManagerRef, CompactorPullTaskHandle, HummockManagerRef};
use crate::manager::MetaSrvEnv;
use crate::storage::MetaStore;

pub type CompactionSchedulerRef<S> = Arc<CompactionScheduler<S>>;
pub type CompactionRequestChannelRef = Arc<CompactionRequestChannel>;

type CompactionRequestChannelItem = (CompactionGroupId, compact_task::TaskType);

/// [`CompactionRequestChannel`] wrappers a mpsc channel and deduplicate requests from same
/// compaction groups.
pub struct CompactionRequestChannel {
    request_tx: UnboundedSender<CompactionRequestChannelItem>,
    scheduled: Mutex<HashSet<(CompactionGroupId, compact_task::TaskType)>>,
}

#[derive(Debug, PartialEq)]
pub enum ScheduleStatus {
    Ok,
    NoTask,
    PickFailure,
    SendFailure(Box<CompactTask>),
    FinishPending,
}

impl CompactionRequestChannel {
    pub fn new(request_tx: UnboundedSender<CompactionRequestChannelItem>) -> Self {
        Self {
            request_tx,
            scheduled: Default::default(),
        }
    }

    /// Enqueues only if the target is not yet in queue.
    pub fn try_sched_compaction(
        &self,
        compaction_group: CompactionGroupId,
        task_type: TaskType,
    ) -> Result<bool, SendError<CompactionRequestChannelItem>> {
        let mut guard = self.scheduled.lock();
        let key = (compaction_group, task_type);
        if guard.contains(&key) {
            return Ok(false);
        }
        self.request_tx.send(key)?;
        guard.insert(key);
        Ok(true)
    }

    pub fn unschedule(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) {
        self.scheduled.lock().remove(&(compaction_group, task_type));
    }
}

#[derive(Debug, Default)]
pub struct CompactionState {
    dynamic: bool,
    space_reclaim: bool,
    ttl: bool,
}

impl CompactionState {
    pub fn enable_task_type(&mut self, task_type: compact_task::TaskType) {
        match task_type {
            TaskType::Dynamic => self.dynamic = true,
            TaskType::SpaceReclaim => self.space_reclaim = true,
            TaskType::Ttl => self.ttl = true,
            _ => {}
        }
    }

    pub fn consume_task_type(&mut self, task_type: compact_task::TaskType) {
        match task_type {
            TaskType::Dynamic => self.dynamic = false,
            TaskType::SpaceReclaim => self.space_reclaim = false,
            TaskType::Ttl => self.ttl = false,
            _ => {}
        }
    }

    pub fn auto_pick_type(&mut self) -> Option<compact_task::TaskType> {
        if self.space_reclaim {
            self.consume_task_type(TaskType::SpaceReclaim);
            Some(TaskType::SpaceReclaim)
        } else if self.ttl {
            self.consume_task_type(TaskType::Ttl);
            Some(TaskType::Ttl)
        } else if self.dynamic {
            self.consume_task_type(TaskType::Dynamic);
            Some(TaskType::Dynamic)
        } else {
            None
        }
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
    compaction_state: CompactionState,
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
            compaction_state: Default::default(),
        }
    }

    pub async fn start(&mut self, shutdown_rx: Receiver<()>) {
        let (sched_tx, sched_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
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

    async fn pick_and_send(
        &self,
        compaction_group: CompactionGroupId,
        mut compactor_pull_task_handle: CompactorPullTaskHandle,
        sched_channel: Arc<CompactionRequestChannel>,
        selector: &mut Box<dyn LevelSelector>,
    ) -> ScheduleStatus {
        let mut result = ScheduleStatus::Ok;

        for (send_task_count, _) in
            (0..compactor_pull_task_handle.pending_pull_task_count).enumerate()
        {
            let compact_task = self
                .hummock_manager
                .get_compact_task(compaction_group, selector)
                .await;

            let compact_task = match compact_task {
                Ok(Some(compact_task)) => compact_task,
                Ok(None) => {
                    if send_task_count > 0 {
                        return ScheduleStatus::Ok;
                    } else {
                        return ScheduleStatus::NoTask;
                    }
                }
                Err(err) => {
                    tracing::warn!("Failed to get compaction task: {:#?}.", err);
                    return ScheduleStatus::PickFailure;
                }
            };

            result = compactor_pull_task_handle.consume_task(&compact_task).await;
            if result != ScheduleStatus::Ok {
                break;
            }
        }

        // Reschedule it with best effort, in case there are more tasks.
        if let Err(e) = sched_channel.try_sched_compaction(compaction_group, selector.task_type()) {
            tracing::error!(
                "Failed to reschedule compaction group {} after sending new task. {:#?}",
                compaction_group,
                e
            );
        }

        result
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
        let mut skip_tasks: HashSet<(CompactionGroupId, TaskType)> = HashSet::new();

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
                                    )
                                    .await
                                {
                                    skip_tasks.insert((compaction_group, task_type));
                                    self.hummock_manager
                                        .metrics
                                        .compact_skip_frequency
                                        .with_label_values(&["total", "no-compactor"])
                                        .inc();
                                } else if !skip_tasks.is_empty() {
                                    for (compaction_group, task_type) in skip_tasks.drain() {
                                        let _ = sched_channel
                                            .try_sched_compaction(compaction_group, task_type);
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

                            SchedulerEvent::AutoPickTrigger => {
                                // Disable periodic trigger for compaction_deterministic_test.
                                if self.env.opts.compaction_deterministic_test {
                                    continue;
                                }

                                if let Some(task_type) = self.compaction_state.auto_pick_type() {
                                    // Periodically trigger compaction for all compaction groups.
                                    self.on_handle_trigger_multi_grouop(
                                        sched_channel.clone(),
                                        task_type,
                                    )
                                    .await;
                                }
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
        &mut self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        sched_channel: Arc<CompactionRequestChannel>,
    ) -> bool {
        let selector = compaction_selectors.get_mut(&task_type).unwrap();

        if let Some(compactor_pull_task_handle) = self.compactor_manager.next_idle_compactor() {
            if let ScheduleStatus::Ok = self
                .pick_and_send(
                    compaction_group,
                    compactor_pull_task_handle,
                    sched_channel,
                    selector,
                )
                .await
            {
                self.compaction_state.consume_task_type(task_type);
                return true;
            } else {
                return false;
            }
        } else {
            self.compaction_state.enable_task_type(task_type);
        }

        true
    }
}

#[derive(Clone)]
pub enum SchedulerEvent {
    Channel((CompactionGroupId, compact_task::TaskType)),
    DynamicTrigger,
    SpaceReclaimTrigger,
    TtlReclaimTrigger,
    AutoPickTrigger,
}

impl<S> CompactionScheduler<S>
where
    S: MetaStore,
{
    fn scheduler_event_stream(
        sched_rx: UnboundedReceiver<(CompactionGroupId, compact_task::TaskType)>,
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

        // TODO: use heartbeat to trigger auto pick
        let mut min_auto_pick_interval = tokio::time::interval(Duration::from_secs(5));
        min_auto_pick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        min_auto_pick_interval.reset();
        let min_auto_pick_tick_trigger =
            IntervalStream::new(min_auto_pick_interval).map(|_| SchedulerEvent::AutoPickTrigger);

        let triggers: Vec<BoxStream<'static, SchedulerEvent>> = vec![
            Box::pin(dynamic_channel_trigger),
            Box::pin(dynamic_tick_trigger),
            Box::pin(space_reclaim_trigger),
            Box::pin(ttl_reclaim_trigger),
            Box::pin(min_auto_pick_tick_trigger),
        ];
        select_all(triggers)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;

    use crate::hummock::compaction::default_level_selector;
    use crate::hummock::compaction_scheduler::{
        CompactionRequestChannel, CompactionRequestChannelItem, ScheduleStatus,
    };
    use crate::hummock::test_utils::{
        add_ssts, register_table_ids_to_compaction_group, setup_compute_env,
    };
    use crate::hummock::CompactionScheduler;

    #[tokio::test]
    async fn test_pick_and_send() {
        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let compaction_scheduler =
            CompactionScheduler::new(env, hummock_manager.clone(), compactor_manager.clone());

        let (request_tx, _request_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
        let request_channel = Arc::new(CompactionRequestChannel::new(request_tx));

        // Add a valid compactor and succeed
        let _receiver = compactor_manager.add_compactor(context_id, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);
        let pending_pull_task_count = 10;
        compactor_manager.update_compactor_pending_task(
            context_id,
            Some(pending_pull_task_count),
            false,
        );
        // No task
        {
            let compactor_pull_task_handle = hummock_manager.get_idle_compactor().unwrap();
            assert_eq!(
                ScheduleStatus::NoTask,
                compaction_scheduler
                    .pick_and_send(
                        StaticCompactionGroupId::StateDefault.into(),
                        compactor_pull_task_handle,
                        request_channel.clone(),
                        &mut default_level_selector(),
                    )
                    .await
            );
        }

        register_table_ids_to_compaction_group(
            hummock_manager.as_ref(),
            &[1],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
        let _sst_infos = add_ssts(2, hummock_manager.as_ref(), context_id).await;
        let _sst_infos = add_ssts(3, hummock_manager.as_ref(), context_id).await;
        {
            let compactor_pull_task_handle = hummock_manager.get_idle_compactor().unwrap();
            assert_eq!(
                context_id,
                compactor_pull_task_handle.compactor.context_id()
            );
            assert_eq!(
                ScheduleStatus::Ok,
                compaction_scheduler
                    .pick_and_send(
                        StaticCompactionGroupId::StateDefault.into(),
                        compactor_pull_task_handle,
                        request_channel.clone(),
                        &mut default_level_selector(),
                    )
                    .await
            );
        }
        // Add more SSTs for compaction.
        let _sst_infos = add_ssts(4, hummock_manager.as_ref(), context_id).await;
        assert_eq!(compactor_manager.compactor_num(), 1);

        // Increase compactor concurrency and succeed
        let _receiver = compactor_manager.add_compactor(context_id, 10);
        let compactor = hummock_manager.get_idle_compactor().unwrap();
        assert_eq!(
            ScheduleStatus::Ok,
            compaction_scheduler
                .pick_and_send(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );
    }

    #[tokio::test]
    // #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints() {
        use assert_matches::assert_matches;
        use risingwave_pb::hummock::compact_task::TaskStatus;

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

        register_table_ids_to_compaction_group(
            hummock_manager.as_ref(),
            &[1],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
        let core_num = 1;
        let _receiver = compactor_manager.add_compactor(context_id, core_num);
        compactor_manager.update_compactor_pending_task(context_id, Some(core_num * 2), false);

        let fp_get_compact_task = "fp_get_compact_task";
        let fp_compaction_send_task_fail = "compaction_send_task_fail";
        let fp_cancel_compact_task = "fp_cancel_compact_task";

        // Pick failure
        {
            fail::cfg(fp_get_compact_task, "return").unwrap();
            let compactor_pull_task_handle = hummock_manager.get_idle_compactor().unwrap();
            assert_eq!(
                ScheduleStatus::PickFailure,
                compaction_scheduler
                    .pick_and_send(
                        StaticCompactionGroupId::StateDefault.into(),
                        compactor_pull_task_handle,
                        request_channel.clone(),
                        &mut default_level_selector(),
                    )
                    .await
            );
            fail::remove(fp_get_compact_task);
        }

        // Send failed and task cancelled.
        {
            fail::cfg(fp_compaction_send_task_fail, "return").unwrap();
            let compactor_pull_task_handle = hummock_manager.get_idle_compactor().unwrap();
            let result = compaction_scheduler
                .pick_and_send(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor_pull_task_handle,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await;
            assert_matches!(result, ScheduleStatus::SendFailure(_));

            fail::remove(fp_compaction_send_task_fail);
            // All selected tasks must have progress
            assert!(!hummock_manager.list_all_tasks_ids().await.is_empty());

            if let ScheduleStatus::SendFailure(mut task_to_cancel) = result {
                hummock_manager
                    .cancel_compact_task(&mut task_to_cancel, TaskStatus::ManualCanceled)
                    .await
                    .unwrap();
            }
        }

        // There is no idle compactor, because the compactor is paused after send failure.
        // assert_matches!(hummock_manager.get_idle_compactor(), None);
        let compactor_pull_task_handle = hummock_manager.get_idle_compactor();
        assert!(compactor_pull_task_handle.is_none());
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());
        {
            let core_num = 1;
            let _receiver = compactor_manager.add_compactor(context_id, core_num);
            compactor_manager.update_compactor_pending_task(context_id, Some(core_num * 2), false);
            // Send failed and task cancellation failed.
            fail::cfg(fp_compaction_send_task_fail, "return").unwrap();
            fail::cfg(fp_cancel_compact_task, "return").unwrap();
            let compactor_pull_task_handle = hummock_manager.get_idle_compactor().unwrap();
            println!(
                "compactor_pull_task_handle {}",
                compactor_pull_task_handle.pending_pull_task_count
            );
            let result = compaction_scheduler
                .pick_and_send(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor_pull_task_handle,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await;
            assert_matches!(result, ScheduleStatus::SendFailure(_));
            fail::remove(fp_compaction_send_task_fail);
            fail::remove(fp_cancel_compact_task);
            assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 1);

            if let ScheduleStatus::SendFailure(mut task_to_cancel) = result {
                hummock_manager
                    .cancel_compact_task(&mut task_to_cancel, TaskStatus::ManualCanceled)
                    .await
                    .unwrap();
            }

            assert!(hummock_manager.list_all_tasks_ids().await.is_empty());
        }

        // Succeeded.
        {
            let core_num = 1;
            let _receiver = compactor_manager.add_compactor(context_id, core_num);
            compactor_manager.update_compactor_pending_task(context_id, Some(core_num * 2), false);
            let compactor = hummock_manager.get_idle_compactor().unwrap();
            assert_matches!(
                compaction_scheduler
                    .pick_and_send(
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
}
