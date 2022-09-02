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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse};
use tokio::sync::mpsc::{Receiver, Sender};

use super::compaction_schedule_policy::{
    CompactionSchedulePolicy, LeastPendingBytesPolicy, RoundRobinPolicy,
};
use crate::MetaResult;

pub type CompactorManagerRef = Arc<CompactorManager>;

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
    max_concurrent_task_number: u64,
}

impl Compactor {
    pub fn new(
        context_id: HummockContextId,
        sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
        max_concurrent_task_number: u64,
    ) -> Self {
        Self {
            context_id,
            sender,
            max_concurrent_task_number,
        }
    }

    pub async fn send_task(&self, task: Task) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse { task: Some(task) }))
            .await
            .map_err(|e| anyhow::anyhow!(e).into())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn max_concurrent_task_number(&self) -> u64 {
        self.max_concurrent_task_number
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`.
///
/// A compact task can be in one of these states:
/// - 1. Pending: a compact task is picked but not assigned to a compactor.
///   Pending-->Success/Failed/Canceled.
/// - 2. Success: an assigned task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 3. Failed: an Failed task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 4. Cancelled: a task is reported as cancelled via `CompactStatus::report_compact_task`. It's
///   the final state.
/// We omit Assigned state because there's nothing to be done about this state currently.
///
/// Furthermore, the compactor for a compaction task must be picked with `CompactorManager`,
/// or its internal states might not be correctly maintained.
pub struct CompactorManager {
    // `policy` must be locked before `compactor_assigned_task_num`.
    policy: Mutex<Box<dyn CompactionSchedulePolicy>>,
    // Map compactor to the number of assigned tasks.
    compactor_assigned_task_num: Mutex<HashMap<HummockContextId, u64>>,
}

impl Default for CompactorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactorManager {
    pub fn new() -> Self {
        Self {
            policy: Mutex::new(Box::new(LeastPendingBytesPolicy::new())),
            compactor_assigned_task_num: Mutex::new(HashMap::new()),
        }
    }

    /// Only used for unit test.
    pub fn new_for_test() -> Self {
        Self {
            policy: Mutex::new(Box::new(RoundRobinPolicy::new())),
            compactor_assigned_task_num: Mutex::new(HashMap::new()),
        }
    }

    /// Only used for unit test.
    pub fn new_with_policy(policy: Box<dyn CompactionSchedulePolicy>) -> Self {
        Self {
            policy: Mutex::new(policy),
            compactor_assigned_task_num: Mutex::new(HashMap::new()),
        }
    }

    /// Gets next idle compactor to assign task.
    // Note: If a compactor is returned, the task is considered to be already
    // assigned to it. This may cause inconsistency between `CompactorManager` and `HummockManager,
    // if `HummockManager::assign_compaction_task` and `next_idle_compactor` fail to be called
    // together.
    pub fn next_idle_compactor(
        &self,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>> {
        let need_update_task_num = compact_task.is_some();
        let mut policy = self.policy.lock();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        let compactor = policy.next_idle_compactor(&compactor_assigned_task_num, compact_task);
        if let Some(compactor) = compactor {
            if need_update_task_num {
                Self::update_task_num(&mut compactor_assigned_task_num, compactor.context_id, true);
            }
            Some(compactor)
        } else {
            None
        }
    }

    /// Gets next compactor to assign task.
    // Note: If a compactor is returned, the task is considered to be already
    // assigned to it. This may cause inconsistency between `CompactorManager` and `HummockManager,
    // if `HummockManager::assign_compaction_task` and `next_compactor` fail to be called
    // together.
    pub fn next_compactor(&self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        let need_update_task_num = compact_task.is_some();
        let mut policy = self.policy.lock();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        let compactor = policy.next_compactor(compact_task);
        if let Some(compactor) = compactor {
            if need_update_task_num {
                Self::update_task_num(&mut compactor_assigned_task_num, compactor.context_id, true);
            }
            Some(compactor)
        } else {
            None
        }
    }

    /// Gets next compactor to assign task.
    // Note: If a compactor is returned, the task is considered to be already
    // assigned to it. This may cause inconsistency between `CompactorManager` and `HummockManager,
    // if `HummockManager::assign_compaction_task` and `random_compactor` fail to be called
    // together.
    pub fn random_compactor(&self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        let need_update_task_num = compact_task.is_some();
        let mut policy = self.policy.lock();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        let compactor = policy.random_compactor(compact_task);
        if let Some(compactor) = compactor {
            if need_update_task_num {
                Self::update_task_num(&mut compactor_assigned_task_num, compactor.context_id, true);
            }
            Some(compactor)
        } else {
            None
        }
    }

    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let mut policy = self.policy.lock();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        compactor_assigned_task_num.insert(context_id, 0);
        policy.add_compactor(context_id, max_concurrent_task_number)
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        let mut policy = self.policy.lock();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        compactor_assigned_task_num.remove(&context_id);
        policy.remove_compactor(context_id);
    }

    pub fn report_compact_task(&self, context_id: HummockContextId, task: &CompactTask) {
        let mut policy = self.policy.lock();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        policy.report_compact_task(context_id, task);
        Self::update_task_num(&mut compactor_assigned_task_num, context_id, false);
    }

    fn update_task_num(
        compactor_assigned_task_num: &mut MutexGuard<HashMap<HummockContextId, u64>>,
        context_id: HummockContextId,
        inc: bool,
    ) {
        let task_num = compactor_assigned_task_num.get_mut(&context_id).unwrap();
        if inc {
            *task_num += 1;
        } else {
            debug_assert!(*task_num > 0);
            *task_num -= 1;
        }
    }
}
