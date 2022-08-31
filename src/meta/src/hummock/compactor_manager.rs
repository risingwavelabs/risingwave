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

use std::sync::Arc;

use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse};
use tokio::sync::mpsc::{Receiver, Sender};

use super::compaction_schedule_policy::{
    CompactionSchedulePolicyImpl, LeastPendingBytesPolicy, RoundRobinPolicy,
};
use crate::hummock::HummockManager;
use crate::storage::MetaStore;
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
/// `CompactTaskAssignment`. A compact task can be in one of these states:
/// - 1. Assigned: a compact task is assigned to a compactor via `HummockManager::get_compact_task`.
///   Assigned-->Finished/Cancelled.
/// - 2. Finished: an assigned task is reported as finished via
///   `CompactStatus::report_compact_task`. It's the final state.
/// - 3. Cancelled: an assigned task is reported as cancelled via
///   `CompactStatus::report_compact_task`. It's the final state.
pub struct CompactorManager {
    inner: tokio::sync::Mutex<CompactionSchedulePolicyImpl>,
}

impl Default for CompactorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactorManager {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(CompactionSchedulePolicyImpl::LeastPendingBytes(
                LeastPendingBytesPolicy::new(),
            )),
        }
    }

    pub fn new_for_test() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(CompactionSchedulePolicyImpl::RoundRobin(
                RoundRobinPolicy::new(),
            )),
        }
    }

    pub async fn next_idle_compactor<S>(
        &self,
        hummock_manager: &HummockManager<S>,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>>
    where
        S: MetaStore,
    {
        self.inner
            .lock()
            .await
            .next_idle_compactor(hummock_manager, compact_task)
            .await
    }

    /// Gets next compactor to assign task or do vacuum.
    pub async fn next_compactor(
        &self,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>> {
        self.inner.lock().await.next_compactor(compact_task)
    }

    pub async fn random_compactor(
        &self,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>> {
        self.inner.lock().await.random_compactor(compact_task)
    }

    pub async fn add_compactor(
        &self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        self.inner
            .lock()
            .await
            .add_compactor(context_id, max_concurrent_task_number)
    }

    pub async fn remove_compactor(&self, context_id: HummockContextId) {
        self.inner.lock().await.remove_compactor(context_id);
    }

    pub async fn report_compact_task(&self, context_id: HummockContextId, task: &CompactTask) {
        self.inner
            .lock()
            .await
            .report_compact_task(context_id, task);
    }
}
