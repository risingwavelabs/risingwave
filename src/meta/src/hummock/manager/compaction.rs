// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId, HummockContextId};
use risingwave_pb::hummock::{CompactTaskAssignment, CompactionConfig};

use crate::hummock::compaction::CompactStatus;
use crate::hummock::error::Result;
use crate::hummock::manager::read_lock;
use crate::hummock::HummockManager;
use crate::model::BTreeMapTransaction;
use crate::storage::MetaStore;

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,

    pub deterministic_mode: bool,
}

impl Compaction {
    /// Cancels all tasks assigned to `context_id`.
    pub fn cancel_assigned_tasks_for_context_ids(
        &mut self,
        context_ids: &[HummockContextId],
    ) -> Result<(
        BTreeMapTransaction<'_, CompactionGroupId, CompactStatus>,
        BTreeMapTransaction<'_, HummockCompactionTaskId, CompactTaskAssignment>,
    )> {
        let mut compact_statuses = BTreeMapTransaction::new(&mut self.compaction_statuses);
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut self.compact_task_assignment);
        for &context_id in context_ids {
            // Clean up compact_status.
            for assignment in compact_task_assignment.tree_ref().values() {
                if assignment.context_id != context_id {
                    continue;
                }
                let task = assignment
                    .compact_task
                    .as_ref()
                    .expect("compact_task shouldn't be None");
                if let Some(mut compact_status) = compact_statuses.get_mut(task.compaction_group_id)
                {
                    compact_status.report_compact_task(
                        assignment
                            .compact_task
                            .as_ref()
                            .expect("compact_task shouldn't be None"),
                    );
                }
            }
            // Clean up compact_task_assignment.
            let task_ids_to_remove = compact_task_assignment
                .tree_ref()
                .iter()
                .filter_map(|(task_id, v)| {
                    if v.context_id == context_id {
                        Some(*task_id)
                    } else {
                        None
                    }
                })
                .collect_vec();
            for task_id in task_ids_to_remove {
                compact_task_assignment.remove(task_id);
            }
        }
        Ok((compact_statuses, compact_task_assignment))
    }
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    #[named]
    pub async fn get_assigned_compact_task_num(&self) -> u64 {
        read_lock!(self, compaction)
            .await
            .compact_task_assignment
            .len() as u64
    }

    #[named]
    pub async fn get_assigned_tasks_number(&self, context_id: HummockContextId) -> u64 {
        read_lock!(self, compaction)
            .await
            .compact_task_assignment
            .values()
            .filter(|s| s.context_id == context_id)
            .count() as u64
    }

    pub async fn get_compaction_config(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> CompactionConfig {
        self.compaction_group_manager
            .compaction_group(compaction_group_id)
            .await
            .expect("compaction group exists")
            .compaction_config()
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
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::CompactionGroupId;
    use risingwave_pb::hummock::{CompactTask, CompactTaskAssignment, InputLevel, SstableInfo};

    use crate::hummock::compaction::CompactStatus;
    use crate::hummock::manager::compaction::Compaction;

    #[tokio::test]
    async fn test_cancel_assigned_tasks_for_context_ids() {
        let mut compaction = Compaction::default();

        let group_id = 0 as CompactionGroupId;
        let task_id = 111;
        let compact_task = CompactTask {
            task_id,
            input_ssts: vec![InputLevel {
                level_idx: 0,
                ..Default::default()
            }],
            target_level: 1,
            ..Default::default()
        };
        let mut compact_status = CompactStatus::new(group_id, 6);
        compact_status.level_handlers[0].add_pending_task(
            compact_task.task_id,
            compact_task.target_level as usize,
            &[SstableInfo {
                id: 1,
                ..Default::default()
            }],
        );
        compaction
            .compaction_statuses
            .insert(group_id, compact_status);
        compaction.compact_task_assignment.insert(
            task_id,
            CompactTaskAssignment {
                compact_task: Some(compact_task),
                context_id: 11,
            },
        );

        // irrelevant context id
        let (compact_status, assignment) = compaction
            .cancel_assigned_tasks_for_context_ids(&[22])
            .unwrap();
        compact_status.commit_memory();
        assignment.commit_memory();
        assert_eq!(
            compaction
                .compaction_statuses
                .get(&group_id)
                .unwrap()
                .level_handlers[0]
                .pending_tasks_ids(),
            vec![task_id]
        );
        assert_eq!(
            compaction
                .compact_task_assignment
                .get(&task_id)
                .unwrap()
                .context_id,
            11
        );

        // target context id
        let (compact_status, assignment) = compaction
            .cancel_assigned_tasks_for_context_ids(&[11])
            .unwrap();
        compact_status.commit_memory();
        assignment.commit_memory();
        assert_eq!(
            compaction
                .compaction_statuses
                .get(&group_id)
                .unwrap()
                .level_handlers[0]
                .pending_tasks_ids()
                .len(),
            0
        );
        assert_eq!(compaction.compact_task_assignment.len(), 0);
    }
}
