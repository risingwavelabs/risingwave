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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use function_name::named;
use futures::future::Shared;
use itertools::Itertools;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    self, Event as RequestEvent, PullTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::{
    Event as ResponseEvent, PullTaskAck,
};
use risingwave_pb::hummock::{
    CompactStatus as PbCompactStatus, CompactTask, CompactTaskAssignment,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Receiver as OneShotReceiver;

use crate::hummock::compaction::selector::level_selector::PickerInfo;
use crate::hummock::compaction::selector::DynamicLevelSelectorCore;
use crate::hummock::compaction::{CompactStatus, CompactionDeveloperConfig, CompactionSelector};
use crate::hummock::error::Result;
use crate::hummock::manager::{init_selectors, read_lock};
use crate::hummock::{Compactor, HummockManager};

const MAX_SKIP_TIMES: usize = 8;
const MAX_REPORT_COUNT: usize = 16;

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,

    pub deterministic_mode: bool,
}

impl HummockManager {
    #[named]
    pub async fn get_assigned_compact_task_num(&self) -> u64 {
        read_lock!(self, compaction)
            .await
            .compact_task_assignment
            .len() as u64
    }

    #[named]
    pub async fn list_all_tasks_ids(&self) -> Vec<HummockCompactionTaskId> {
        let compaction = read_lock!(self, compaction).await;

        compaction
            .compaction_statuses
            .iter()
            .flat_map(|(_, cs)| {
                cs.level_handlers
                    .iter()
                    .flat_map(|lh| lh.pending_tasks_ids())
            })
            .collect_vec()
    }

    #[named]
    pub async fn list_compaction_status(
        &self,
    ) -> (Vec<PbCompactStatus>, Vec<CompactTaskAssignment>) {
        let compaction = read_lock!(self, compaction).await;
        (
            compaction.compaction_statuses.values().map_into().collect(),
            compaction
                .compact_task_assignment
                .values()
                .cloned()
                .collect(),
        )
    }

    #[named]
    pub async fn get_compaction_scores(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Vec<PickerInfo> {
        let (status, levels, group) = {
            let compaction = read_lock!(self, compaction).await;
            let versioning = read_lock!(self, versioning).await;
            let config_manager = self.compaction_group_manager.read().await;
            match (
                compaction.compaction_statuses.get(&compaction_group_id),
                versioning.current_version.levels.get(&compaction_group_id),
                config_manager.try_get_compaction_group_config(compaction_group_id),
            ) {
                (Some(cs), Some(v), Some(cf)) => (cs.to_owned(), v.to_owned(), cf),
                _ => {
                    return vec![];
                }
            }
        };
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            group.compaction_config,
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let ctx = dynamic_level_core.get_priority_levels(&levels, &status.level_handlers);
        ctx.score_levels
    }

    pub async fn handle_pull_task_event(
        &self,
        context_id: u32,
        pull_task_count: usize,
        compaction_selectors: &mut HashMap<TaskType, Box<dyn CompactionSelector>>,
    ) {
        assert_ne!(0, pull_task_count);

        fn tasks_result_handler(
            tasks: Result<(Vec<CompactTask>, Vec<CompactionGroupId>)>,
            compactor: Arc<Compactor>,
            no_task_groups: &mut HashSet<CompactionGroupId>,
            existed_groups: &mut Vec<CompactionGroupId>,
        ) -> (Vec<u64>, usize) {
            let mut generated_task_count = 0;
            let mut failed_tasks = vec![];
            match tasks {
                Ok((compact_tasks, unschedule_groups)) => {
                    if compact_tasks.is_empty() {
                        return (failed_tasks, generated_task_count);
                    }
                    generated_task_count += compact_tasks.len();
                    no_task_groups.extend(unschedule_groups);

                    let mut send_fail = false;
                    for task in &compact_tasks {
                        let task_id = task.task_id;
                        if let Err(e) =
                            compactor.send_event(ResponseEvent::CompactTask(task.clone()))
                        {
                            tracing::warn!(
                                error = %e.as_report(),
                                "Failed to send task {} to {}",
                                task_id,
                                compactor.context_id(),
                            );
                            // failed_tasks.push(task_id);
                            send_fail = true;
                            break;
                        }
                    }

                    existed_groups.retain(|group_id| !no_task_groups.contains(group_id));

                    // cancel all tasks
                    if send_fail {
                        failed_tasks.extend(compact_tasks.iter().map(|t| t.task_id));
                    }

                    (failed_tasks, generated_task_count)
                }
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "Failed to get compaction task");
                    (failed_tasks, generated_task_count)
                }
            }
        }

        async fn handle_pick_tasks(
            hummock_manager: &HummockManager,
            pull_task_count: usize,
            groups: Vec<u64>,
            compactor: Arc<Compactor>,
            selector: &mut Box<dyn CompactionSelector>,
            generated_task_count: &mut usize,
            no_task_groups: &mut HashSet<CompactionGroupId>,
            existed_groups: &mut Vec<CompactionGroupId>,
        ) {
            while *generated_task_count < pull_task_count {
                // todo use batch get compact_task inside single cg
                let compact_ret = hummock_manager
                    .get_compact_tasks(
                        groups.clone(),
                        pull_task_count - *generated_task_count,
                        selector,
                    )
                    .await;
                let (failed_tasks, picked_tasks_count) = tasks_result_handler(
                    compact_ret,
                    compactor.clone(),
                    no_task_groups,
                    existed_groups,
                );

                // if no task or failed task, break
                if picked_tasks_count == 0 || !failed_tasks.is_empty() {
                    break;
                }

                *generated_task_count += picked_tasks_count;
            }
        }

        if let Some(compactor) = self.compactor_manager.get_compactor(context_id) {
            let (groups, mut task_type) = self.auto_pick_compaction_groups_and_type().await;

            let mut generated_task_count = 0;
            let mut existed_groups = groups.clone();
            let mut no_task_groups: HashSet<CompactionGroupId> = HashSet::default();
            let failed_tasks = vec![];

            if groups.len() == 1 && matches!(task_type, TaskType::Emergency) {
                // tips: Emergency is a special state of Dynamic, so you need to use Dynamic to unschedule CG after pick tasks
                task_type = TaskType::Dynamic;

                // When cg encounters a write-stop, we will place it in a separate vec and perform separate processing
                // 1. Try to select dynamic task
                let mut selector: &mut Box<dyn CompactionSelector> =
                    compaction_selectors.get_mut(&task_type).unwrap();
                handle_pick_tasks(
                    self,
                    pull_task_count,
                    groups.clone(),
                    compactor.clone(),
                    selector,
                    &mut generated_task_count,
                    &mut no_task_groups,
                    &mut existed_groups,
                )
                .await;

                // 2. if no task and not enough task, try to pull from emergency selector
                const EXPECTED_PULL_TASK_RATIO: f32 = 0.75;
                if failed_tasks.is_empty()
                    && (generated_task_count
                        < (((pull_task_count as f32) * EXPECTED_PULL_TASK_RATIO) as u32)
                            .try_into()
                            .unwrap())
                {
                    selector = compaction_selectors.get_mut(&TaskType::Emergency).unwrap();
                    handle_pick_tasks(
                        self,
                        pull_task_count,
                        groups.clone(),
                        compactor.clone(),
                        selector,
                        &mut generated_task_count,
                        &mut no_task_groups,
                        &mut existed_groups,
                    )
                    .await;
                }
            } else if !groups.is_empty() {
                // batch get multi groups
                let selector: &mut Box<dyn CompactionSelector> =
                    compaction_selectors.get_mut(&task_type).unwrap();

                handle_pick_tasks(
                    self,
                    pull_task_count,
                    groups.clone(),
                    compactor.clone(),
                    selector,
                    &mut generated_task_count,
                    &mut no_task_groups,
                    &mut existed_groups,
                )
                .await;
            }

            // unschedule cg
            for group in no_task_groups {
                self.compaction_state.unschedule(group, task_type);
            }

            // cancel failed tasks
            if let Err(err) = self
                .cancel_compact_tasks(failed_tasks, TaskStatus::SendFailCanceled)
                .await
            {
                tracing::warn!(error = %err.as_report(), "Failed to cancel compaction task");
            }

            // ack to compactor
            if let Err(e) = compactor.send_event(ResponseEvent::PullTaskAck(PullTaskAck {})) {
                tracing::warn!(
                    error = %e.as_report(),
                    "Failed to send ask to {}",
                    context_id,
                );
                self.compactor_manager.remove_compactor(context_id);
            }
        }
    }

    /// dedicated event runtime for CPU/IO bound event
    pub async fn compact_task_dedicated_event_handler(
        hummock_manager: Arc<HummockManager>,
        mut rx: UnboundedReceiver<(u32, subscribe_compaction_event_request::Event)>,
        shutdown_rx_shared: Shared<OneShotReceiver<()>>,
    ) {
        let mut compaction_selectors = init_selectors();

        tokio::select! {
            _ = shutdown_rx_shared => {}

            _ = async {
                while let Some((context_id, event)) = rx.recv().await {
                    let mut report_events = vec![];
                    let mut skip_times = 0;
                    match event {
                        RequestEvent::PullTask(PullTask { pull_task_count }) => {
                            hummock_manager.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors).await;
                        }

                        RequestEvent::ReportTask(task) => {
                           report_events.push(task);
                        }

                        _ => unreachable!(),
                    }
                    while let Ok((context_id, event)) = rx.try_recv() {
                        match event {
                            RequestEvent::PullTask(PullTask { pull_task_count }) => {
                                hummock_manager.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors).await;
                                if !report_events.is_empty() {
                                    if skip_times > MAX_SKIP_TIMES {
                                        break;
                                    }
                                    skip_times += 1;
                                }
                            }

                            RequestEvent::ReportTask(task) => {
                                report_events.push(task);
                                if report_events.len() >= MAX_REPORT_COUNT {
                                    break;
                                }
                            }
                        _ => unreachable!(),
                        }
                    }
                    if !report_events.is_empty() {
                        if let Err(e) = hummock_manager.report_compact_tasks(report_events).await
                        {
                            tracing::error!(error = %e.as_report(), "report compact_tack fail")
                        }
                    }
                }
            } => {}
        }
    }
}
