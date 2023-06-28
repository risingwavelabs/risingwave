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

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse};
use tokio::sync::mpsc::Receiver;

use super::Compactor;
use crate::hummock::error::{Error, Result};
use crate::MetaResult;

const STREAM_BUFFER_SIZE: usize = 4;

/// The implementation of compaction task scheduling policy.
pub trait CompactionSchedulePolicy: Send + Sync {
    /// Get next idle compactor to assign task.
    fn next_idle_compactor(&self) -> Option<Arc<Compactor>>;

    /// Get next compactor to assign task.
    fn next_compactor(&self) -> Option<Arc<Compactor>>;

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
        cpu_core_num: u32,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>>;

    /// Make sure the compactor with `context_id` will not be scheduled until it resubscribes.
    /// Retain the state related to `context_id` if the implementation is stateful.
    ///
    /// Note: It is allowed to pause a non-existent compactor.
    fn pause_compactor(&mut self, context_id: HummockContextId);

    /// Make sure the compactor with `context_id` will never be scheduled again. Remove any internal
    /// state related to `context_id` if the implementation is stateful.
    ///
    /// Note: It is allowed to remove a non-existent compactor.
    fn remove_compactor(&mut self, context_id: HummockContextId);

    fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>>;

    /// Notify the policy of the assignment of a compaction task to adjust the next compactor to
    /// schedule.
    ///
    /// An error will be returned if `context_id` is removed.
    fn assign_compact_task(
        &mut self,
        context_id: HummockContextId,
        compact_task: &CompactTask,
    ) -> Result<()>;

    /// Notify the policy of the completion of a compaction task to adjust the next compactor
    /// to schedule.
    ///
    /// It's ok if `context_id` does not exist, because a compactor might be removed after it has
    /// reported a task.
    fn report_compact_task(&mut self, context_id: HummockContextId, compact_task: &CompactTask);

    fn compactor_num(&self) -> usize;

    fn max_concurrent_task_num(&self) -> usize;

    fn total_cpu_core_num(&self) -> u32;

    fn total_running_cpu_core_num(&self) -> u32;
}

// This strategy is retained just for reference, it is not used.
pub struct RoundRobinPolicy {
    /// The context ids of compactors.
    compactors: Vec<HummockContextId>,

    /// TODO: Let each compactor have its own Mutex, we should not need to lock whole thing.
    /// The outer lock is a RwLock, so we should still be able to modify each compactor
    compactor_map: HashMap<HummockContextId, Arc<Compactor>>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
}

impl RoundRobinPolicy {
    pub fn new() -> Self {
        Self {
            compactors: vec![],
            compactor_map: HashMap::new(),
            next_compactor: 0,
        }
    }
}

impl CompactionSchedulePolicy for RoundRobinPolicy {
    fn next_idle_compactor(&self) -> Option<Arc<Compactor>> {
        if self.compactors.is_empty() {
            return None;
        }
        let compactor_index = self.next_compactor % self.compactors.len();
        let compactor = self
            .get_compactor(self.compactors[compactor_index])
            .unwrap();

        Some(compactor)
    }

    fn next_compactor(&self) -> Option<Arc<Compactor>> {
        if self.compactors.is_empty() {
            return None;
        }
        let compactor_index = self.next_compactor % self.compactors.len();
        let compactor = self.compactors[compactor_index];
        Some(self.compactor_map.get(&compactor).unwrap().clone())
    }

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
        cpu_core_num: u32,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.compactors.retain(|c| *c != context_id);
        self.compactors.push(context_id);
        self.compactor_map.insert(
            context_id,
            Arc::new(Compactor::new(
                context_id,
                tx,
                max_concurrent_task_number,
                cpu_core_num,
            )),
        );
        rx
    }

    fn pause_compactor(&mut self, context_id: HummockContextId) {
        self.remove_compactor(context_id);
    }

    fn remove_compactor(&mut self, context_id: HummockContextId) {
        self.compactors.retain(|c| *c != context_id);
        self.compactor_map.remove(&context_id);
    }

    fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.compactor_map.get(&context_id).cloned()
    }

    fn assign_compact_task(
        &mut self,
        context_id: HummockContextId,
        _compact_task: &CompactTask,
    ) -> Result<()> {
        if !self.compactor_map.contains_key(&context_id) {
            return Err(Error::InvalidContext(context_id));
        }
        self.next_compactor += 1;
        Ok(())
    }

    fn report_compact_task(&mut self, _context_id: HummockContextId, _compact_task: &CompactTask) {}

    fn compactor_num(&self) -> usize {
        self.compactors.len()
    }

    fn max_concurrent_task_num(&self) -> usize {
        self.compactor_map
            .values()
            .map(|c| c.max_concurrent_task_number() as usize)
            .sum()
    }

    fn total_cpu_core_num(&self) -> u32 {
        self.compactor_map.values().map(|c| c.total_cpu_core).sum()
    }

    fn total_running_cpu_core_num(&self) -> u32 {
        self.compactor_map
            .values()
            .map(|c| c.cpu_ratio.load(Ordering::Acquire) * c.total_cpu_core / 100)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::try_match_expand;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::compact_task::{self, TaskStatus};
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::{CompactTask, InputLevel, SstableInfo};
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::default_level_selector;
    use crate::hummock::compaction_schedule_policy::{CompactionSchedulePolicy, RoundRobinPolicy};
    use crate::hummock::test_utils::{
        commit_from_meta_node, generate_test_tables, get_sst_ids,
        register_sstable_infos_to_compaction_group, setup_compute_env_with_config,
        to_local_sstable_info,
    };
    use crate::hummock::HummockManager;
    use crate::storage::MetaStore;

    async fn add_compact_task<S>(hummock_manager: &HummockManager<S>, _context_id: u32, epoch: u64)
    where
        S: MetaStore,
    {
        let original_tables = generate_test_tables(epoch, get_sst_ids(hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager,
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        commit_from_meta_node(
            hummock_manager,
            epoch,
            to_local_sstable_info(&original_tables),
        )
        .await
        .unwrap();
    }

    fn dummy_compact_task(task_id: u64, input_file_size: u64) -> CompactTask {
        CompactTask {
            // We need the dummy level to calculate input size.
            input_ssts: vec![InputLevel {
                level_idx: 0,
                level_type: 0,
                table_infos: vec![SstableInfo {
                    key_range: None,
                    file_size: input_file_size,
                    table_ids: vec![],
                    uncompressed_file_size: input_file_size,
                    ..Default::default()
                }],
            }],
            splits: vec![],
            watermark: 0,
            sorted_output_ssts: vec![],
            task_id,
            target_level: 0,
            gc_delete_keys: false,
            base_level: 0,
            task_status: TaskStatus::Pending as i32,
            compaction_group_id: StaticCompactionGroupId::StateDefault.into(),
            existing_table_ids: vec![],
            compression_algorithm: 0,
            target_file_size: 1,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
            target_sub_level_id: 0,
            task_type: compact_task::TaskType::Dynamic as i32,
            split_by_state_table: false,
            split_weight_by_vnode: 0,
        }
    }

    #[tokio::test]
    async fn test_rr_add_remove_compactor() {
        let mut policy = RoundRobinPolicy::new();
        // No compactors by default.
        assert_eq!(policy.compactors.len(), 0);
        assert_eq!(policy.max_concurrent_task_num(), 0);

        let mut receiver = policy.add_compactor(1, 1000, 1000);
        assert_eq!(policy.compactors.len(), 1);
        assert_eq!(policy.max_concurrent_task_num(), 1000);
        let _receiver_2 = policy.add_compactor(2, 1000, 1000);
        assert_eq!(policy.compactors.len(), 2);
        assert_eq!(policy.max_concurrent_task_num(), 2000);
        policy.remove_compactor(2);
        assert_eq!(policy.compactors.len(), 1);
        assert_eq!(policy.max_concurrent_task_num(), 1000);

        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123, 0);
        let compactor = {
            let compactor_id = policy.compactors.first().unwrap();
            policy.compactor_map.get(compactor_id).unwrap().clone()
        };
        compactor
            .send_task(Task::CompactTask(task.clone()))
            .await
            .unwrap();
        // Receive a compact task.
        let received_task = receiver.try_recv().unwrap().unwrap().task.unwrap();
        let received_compact_task = try_match_expand!(received_task, Task::CompactTask).unwrap();
        assert_eq!(received_compact_task, task);

        policy.remove_compactor(compactor.context_id());
        assert_eq!(policy.compactors.len(), 0);
        assert_eq!(policy.max_concurrent_task_num(), 0);
        drop(compactor);
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));
    }

    #[tokio::test]
    async fn test_rr_next_compactor() {
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(1)
            .max_bytes_for_level_base(1)
            .level0_sub_level_compact_level_count(1)
            .level0_overlapping_sub_level_compact_level_count(1)
            .build();
        let (_, hummock_manager, _, worker_node) = setup_compute_env_with_config(80, config).await;
        let context_id = worker_node.id;
        let mut compactor_manager = RoundRobinPolicy::new();
        add_compact_task(hummock_manager.as_ref(), context_id, 1).await;

        // No compactor available.
        assert!(compactor_manager.next_compactor().is_none());

        // Add a compactor.
        let mut receiver = compactor_manager.add_compactor(context_id, u64::MAX, 16);
        assert_eq!(compactor_manager.compactors.len(), 1);
        let compactor = compactor_manager.next_compactor().unwrap();
        // No compact task.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = hummock_manager
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap()
            .unwrap();
        compactor
            .send_task(Task::CompactTask(task.clone()))
            .await
            .unwrap();

        // Get a compact task.
        let received_task = receiver.try_recv().unwrap().unwrap().task.unwrap();
        let received_compact_task = try_match_expand!(received_task, Task::CompactTask).unwrap();
        assert_eq!(received_compact_task, task);

        compactor_manager.remove_compactor(compactor.context_id());
        assert_eq!(compactor_manager.compactors.len(), 0);
        assert!(compactor_manager.next_compactor().is_none());
    }

    #[tokio::test]
    async fn test_rr_next_compactor_round_robin() {
        let mut policy = RoundRobinPolicy::new();
        let mut receivers = vec![];
        for context_id in 0..5 {
            receivers.push(policy.add_compactor(context_id, u64::MAX, 16));
        }
        assert_eq!(policy.compactors.len(), 5);
        let task = dummy_compact_task(0, 1);
        for i in 0..receivers.len() * 3 {
            let compactor = policy.next_compactor().unwrap();
            policy
                .assign_compact_task(compactor.context_id(), &task)
                .unwrap();
            assert_eq!(compactor.context_id() as usize, i % receivers.len());
        }
    }
}
