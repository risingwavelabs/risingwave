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

use futures::future::Shared;
use itertools::Itertools;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    self, Event as RequestEvent, PullTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::{
    Event as ResponseEvent, PullTaskAck,
};
use risingwave_pb::hummock::{
    CompactStatus as PbCompactStatus, CompactTask, CompactTaskAssignment, CompactionConfig,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Receiver as OneShotReceiver;

use crate::hummock::compaction::selector::level_selector::PickerInfo;
use crate::hummock::compaction::selector::DynamicLevelSelectorCore;
use crate::hummock::compaction::{CompactStatus, CompactionDeveloperConfig, CompactionSelector};
use crate::hummock::manager::init_selectors;
use crate::hummock::HummockManager;

const MAX_SKIP_TIMES: usize = 8;
const MAX_REPORT_COUNT: usize = 16;

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,

    pub _deterministic_mode: bool,
}

impl HummockManager {
    pub async fn get_assigned_compact_task_num(&self) -> u64 {
        self.compaction.read().await.compact_task_assignment.len() as u64
    }

    pub async fn list_all_tasks_ids(&self) -> Vec<HummockCompactionTaskId> {
        let compaction = self.compaction.read().await;

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

    pub async fn list_compaction_status(
        &self,
    ) -> (Vec<PbCompactStatus>, Vec<CompactTaskAssignment>) {
        let compaction = self.compaction.read().await;
        (
            compaction.compaction_statuses.values().map_into().collect(),
            compaction
                .compact_task_assignment
                .values()
                .cloned()
                .collect(),
        )
    }

    pub async fn get_compaction_scores(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Vec<PickerInfo> {
        let (status, levels, group) = {
            let compaction = self.compaction.read().await;
            let versioning = self.versioning.read().await;
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
        max_get_task_probe_times: usize,
    ) {
        assert_ne!(0, pull_task_count);
        if let Some(compactor) = self.compactor_manager.get_compactor(context_id) {
            let (groups, task_type) = self.auto_pick_compaction_groups_and_type().await;
            if !groups.is_empty() {
                let selector: &mut Box<dyn CompactionSelector> =
                    compaction_selectors.get_mut(&task_type).unwrap();

                let mut generated_task_count = 0;
                let mut existed_groups = groups.clone();
                let mut no_task_groups: HashSet<CompactionGroupId> = HashSet::default();
                let mut failed_tasks = vec![];
                let mut loop_times = 0;

                while generated_task_count < pull_task_count
                    && failed_tasks.is_empty()
                    && loop_times < max_get_task_probe_times
                {
                    loop_times += 1;
                    let compact_ret = self
                        .get_compact_tasks(
                            existed_groups.clone(),
                            pull_task_count - generated_task_count,
                            selector,
                        )
                        .await;

                    match compact_ret {
                        Ok((compact_tasks, unschedule_groups)) => {
                            if compact_tasks.is_empty() {
                                break;
                            }
                            generated_task_count += compact_tasks.len();
                            no_task_groups.extend(unschedule_groups);
                            for task in compact_tasks {
                                let task_id = task.task_id;
                                if let Err(e) =
                                    compactor.send_event(ResponseEvent::CompactTask(task))
                                {
                                    tracing::warn!(
                                        error = %e.as_report(),
                                        "Failed to send task {} to {}",
                                        task_id,
                                        compactor.context_id(),
                                    );
                                    failed_tasks.push(task_id);
                                }
                            }
                            if !failed_tasks.is_empty() {
                                self.compactor_manager.remove_compactor(context_id);
                            }
                            existed_groups.retain(|group_id| !no_task_groups.contains(group_id));
                        }
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(), "Failed to get compaction task");
                            break;
                        }
                    };
                }
                for group in no_task_groups {
                    self.compaction_state.unschedule(group, task_type);
                }
                if let Err(err) = self
                    .cancel_compact_tasks(failed_tasks, TaskStatus::SendFailCanceled)
                    .await
                {
                    tracing::warn!(error = %err.as_report(), "Failed to cancel compaction task");
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
                            hummock_manager.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors, hummock_manager.env.opts.max_get_task_probe_times).await;
                        }

                        RequestEvent::ReportTask(task) => {
                           report_events.push(task);
                        }

                        _ => unreachable!(),
                    }
                    while let Ok((context_id, event)) = rx.try_recv() {
                        match event {
                            RequestEvent::PullTask(PullTask { pull_task_count }) => {
                                hummock_manager.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors, hummock_manager.env.opts.max_get_task_probe_times).await;
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

    pub(crate) async fn calculate_vnode_partition(
        &self,
        compact_task: &mut CompactTask,
        compaction_config: &CompactionConfig,
    ) {
        if compact_task.target_level > compact_task.base_level {
            return;
        }
        if compaction_config.split_weight_by_vnode > 0 {
            for table_id in &compact_task.existing_table_ids {
                compact_task
                    .table_vnode_partition
                    .insert(*table_id, compact_task.split_weight_by_vnode);
            }
        } else {
            let mut table_size_info: HashMap<u32, u64> = HashMap::default();
            let mut existing_table_ids: HashSet<u32> = HashSet::default();
            for input_ssts in &compact_task.input_ssts {
                for sst in &input_ssts.table_infos {
                    existing_table_ids.extend(sst.table_ids.iter());
                    for table_id in &sst.table_ids {
                        *table_size_info.entry(*table_id).or_default() +=
                            sst.file_size / (sst.table_ids.len() as u64);
                    }
                }
            }
            compact_task
                .existing_table_ids
                .retain(|table_id| existing_table_ids.contains(table_id));

            let hybrid_vnode_count = self.env.opts.hybrid_partition_node_count;
            let default_partition_count = self.env.opts.partition_vnode_count;
            // We must ensure the partition threshold large enough to avoid too many small files.
            let less_partition_threshold = self.env.opts.hybrid_few_partition_threshold;
            let several_partition_threshold = self.env.opts.hybrid_more_partition_threshold;
            let params = self.env.system_params_reader().await;
            let barrier_interval_ms = params.barrier_interval_ms() as u64;
            let checkpoint_secs = std::cmp::max(
                1,
                params.checkpoint_frequency() * barrier_interval_ms / 1000,
            );
            // check latest write throughput
            let history_table_throughput = self.history_table_throughput.read();
            for (table_id, compact_table_size) in table_size_info {
                let write_throughput = history_table_throughput
                    .get(&table_id)
                    .map(|que| que.back().cloned().unwrap_or(0))
                    .unwrap_or(0)
                    / checkpoint_secs;
                if compact_table_size > several_partition_threshold {
                    compact_task
                        .table_vnode_partition
                        .insert(table_id, default_partition_count);
                } else {
                    if compact_table_size > less_partition_threshold
                        || (write_throughput > self.env.opts.table_write_throughput_threshold
                            && compact_table_size > compaction_config.target_file_size_base)
                    {
                        // partition for large write throughput table. But we also need to make sure that it can not be too small.
                        compact_task
                            .table_vnode_partition
                            .insert(table_id, hybrid_vnode_count);
                    } else if compact_table_size > compaction_config.target_file_size_base {
                        // partition for small table
                        compact_task.table_vnode_partition.insert(table_id, 1);
                    }
                }
            }
            compact_task
                .table_vnode_partition
                .retain(|table_id, _| compact_task.existing_table_ids.contains(table_id));
        }
    }
}

pub fn check_cg_write_limit(
    levels: &Levels,
    compaction_config: &CompactionConfig,
) -> WriteLimitType {
    let threshold = compaction_config.level0_stop_write_threshold_sub_level_number as usize;
    let l0_sub_level_number = levels.l0.as_ref().unwrap().sub_levels.len();
    if threshold < l0_sub_level_number {
        return WriteLimitType::WriteStop(l0_sub_level_number, threshold);
    }

    WriteLimitType::Unlimited
}

pub enum WriteLimitType {
    Unlimited,

    // (l0_level_count, threshold)
    WriteStop(usize, usize),
}

impl WriteLimitType {
    pub fn as_str(&self) -> String {
        match self {
            Self::Unlimited => "Unlimited".to_string(),
            Self::WriteStop(l0_level_count, threshold) => {
                format!(
                    "WriteStop(l0_level_count: {}, threshold: {}) too many L0 sub levels",
                    l0_level_count, threshold
                )
            }
        }
    }

    pub fn is_write_stop(&self) -> bool {
        matches!(self, Self::WriteStop(_, _))
    }
}
