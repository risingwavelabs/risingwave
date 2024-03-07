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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use function_name::named;
use futures::future::Shared;
use itertools::Itertools;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId};
use risingwave_pb::hummock::compact_task::TaskType;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    self, Event as RequestEvent, PullTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::{
    Event as ResponseEvent, PullTaskAck,
};
use risingwave_pb::hummock::{CompactStatus as PbCompactStatus, CompactTaskAssignment};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Receiver as OneShotReceiver;

use crate::hummock::compaction::selector::level_selector::PickerInfo;
use crate::hummock::compaction::selector::DynamicLevelSelectorCore;
use crate::hummock::compaction::{CompactStatus, CompactionDeveloperConfig, CompactionSelector};
use crate::hummock::manager::{init_selectors, read_lock};
use crate::hummock::HummockManager;

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
        if let Some(compactor) = self.compactor_manager.get_compactor(context_id) {
            let (groups, task_type) = self.auto_pick_compaction_groups_and_type().await;
            if !groups.is_empty() {
                let selector: &mut Box<dyn CompactionSelector> =
                    compaction_selectors.get_mut(&task_type).unwrap();

                let mut generated_task_count = 0;
                let mut existed_groups = groups.clone();
                let mut meet_error = false;

                while generated_task_count < pull_task_count && !meet_error {
                    let compact_ret = self
                        .get_compact_tasks(
                            std::mem::take(&mut existed_groups),
                            pull_task_count - generated_task_count,
                            selector,
                        )
                        .await;

                    match compact_ret {
                        Ok((compact_tasks, trivial_tasks)) => {
                            if compact_tasks.is_empty() {
                                break;
                            }
                            generated_task_count += compact_tasks.len();
                            for task in trivial_tasks {
                                existed_groups.push(task.compaction_group_id);
                            }
                            for task in compact_tasks {
                                let task_id = task.task_id;
                                existed_groups.push(task.compaction_group_id);
                                if let Err(e) =
                                    compactor.send_event(ResponseEvent::CompactTask(task))
                                {
                                    tracing::warn!(
                                        error = %e.as_report(),
                                        "Failed to send task {} to {}",
                                        task_id,
                                        compactor.context_id(),
                                    );
                                    self.compactor_manager.remove_compactor(context_id);
                                    meet_error = true;
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(), "Failed to get compaction task");
                            break;
                        }
                    };
                }
                if generated_task_count < pull_task_count && !meet_error {
                    // no compact_task to be picked
                    for group in groups {
                        self.compaction_state.unschedule(group, task_type);
                    }
                }
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
