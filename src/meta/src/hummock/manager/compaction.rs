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
use risingwave_pb::hummock::CompactTaskAssignment;

use crate::hummock::compaction::CompactStatus;
use crate::hummock::manager::read_lock;
use crate::hummock::HummockManager;
use crate::storage::MetaStore;

#[derive(Default)]
pub struct Compaction {
    /// Compaction task that is already assigned to a compactor
    pub compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    pub compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,
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
}
