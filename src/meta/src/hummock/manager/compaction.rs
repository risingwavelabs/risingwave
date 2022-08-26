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
use std::ops::DerefMut;

use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId, HummockContextId};
use risingwave_pb::hummock::CompactTaskAssignment;

use crate::hummock::compaction::CompactStatus;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::{commit_multi_var, read_lock, write_lock};
use crate::hummock::HummockManager;
use crate::model::{BTreeMapTransaction, ValTransaction};
use crate::storage::{MetaStore, Transaction};

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,
}

/// Cancels all tasks assigned to `context_id`.
pub fn cancel_all_assigned_tasks(
    context_id: HummockContextId,
    compact_statuses: &mut BTreeMapTransaction<CompactionGroupId, CompactStatus>,
    compact_task_assignment: &mut BTreeMapTransaction<
        HummockCompactionTaskId,
        CompactTaskAssignment,
    >,
) -> Result<()> {
    for assignment in compact_task_assignment.tree_ref().values() {
        if assignment.context_id != context_id {
            continue;
        }
        let task = assignment
            .compact_task
            .as_ref()
            .expect("compact_task shouldn't be None");
        let mut compact_status = compact_statuses
            .get_mut(task.compaction_group_id)
            .ok_or(Error::InvalidCompactionGroup(task.compaction_group_id))?;
        compact_status.report_compact_task(
            assignment
                .compact_task
                .as_ref()
                .expect("compact_task shouldn't be None"),
        );
    }
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
    Ok(())
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    #[named]
    pub async fn get_assigned_tasks_number(&self, context_id: HummockContextId) -> u64 {
        read_lock!(self, compaction)
            .await
            .compact_task_assignment
            .values()
            .filter(|s| s.context_id == context_id)
            .count() as u64
    }

    #[named]
    pub async fn list_assigned_tasks_number(&self) -> Vec<(u32, usize)> {
        let compaction = read_lock!(self, compaction).await;
        compaction
            .compact_task_assignment
            .values()
            .group_by(|s| s.context_id)
            .into_iter()
            .map(|(k, v)| (k, v.count()))
            .collect_vec()
    }

    #[named]
    pub async fn cancel_all_assigned_tasks(&self, context_id: HummockContextId) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        cancel_all_assigned_tasks(
            context_id,
            &mut compact_statuses,
            &mut compact_task_assignment,
        )?;
        commit_multi_var!(self, None, compact_statuses, compact_task_assignment)?;
        Ok(())
    }
}
