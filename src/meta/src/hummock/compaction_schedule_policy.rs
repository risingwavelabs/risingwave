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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::{CompactTask, CompactTaskAssignment, SubscribeCompactTasksResponse};
use tokio::sync::mpsc::Receiver;

use super::Compactor;
use crate::hummock::error::{Error, Result};
use crate::MetaResult;

const STREAM_BUFFER_SIZE: usize = 4;

/// The implementation of compaction task scheduling policy.
pub trait CompactionSchedulePolicy: Send + Sync {
    /// Get next idle compactor to assign task.
    fn next_idle_compactor(
        &self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
    ) -> Option<Arc<Compactor>>;

    /// Get next compactor to assign task.
    fn next_compactor(&self) -> Option<Arc<Compactor>>;

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>>;

    fn remove_compactor(&mut self, context_id: HummockContextId);

    fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>>;

    /// Notify the policy of the assignment of a compaction task to adjust the next compactor to
    /// schedule.
    ///
    /// It is possible that the compactor with `context_id` does not exist. An error will be
    /// returned in this case.
    fn assign_compact_task(
        &mut self,
        context_id: HummockContextId,
        compact_task: &CompactTask,
    ) -> Result<()>;

    /// Notify the policy of the completion of a compaction task to adjust the next compactor
    /// to schedule.
    fn report_compact_task(&mut self, context_id: HummockContextId, compact_task: &CompactTask);
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
    fn next_idle_compactor(
        &self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
    ) -> Option<Arc<Compactor>> {
        if self.compactors.is_empty() {
            return None;
        }
        let compactor_index = self.next_compactor % self.compactors.len();
        for context_id in self.compactors[compactor_index..]
            .iter()
            .chain(&self.compactors[..compactor_index])
        {
            let compactor = self.compactor_map.get(context_id).unwrap();
            if *compactor_assigned_task_num
                .get(&compactor.context_id())
                .unwrap()
                < compactor.max_concurrent_task_number()
            {
                return Some(compactor.clone());
            }
        }
        None
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
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.compactors.retain(|c| *c != context_id);
        self.compactors.push(context_id);
        self.compactor_map.insert(
            context_id,
            Arc::new(Compactor::new(context_id, tx, max_concurrent_task_number)),
        );
        rx
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
}

/// The score must be linear to the input for easy update.
///
/// Currently the score >= 0, but we use signed type because the score delta might be < 0.
type Score = i64;

/// Give priority to compactors with the least score. Currently the score is composed only of
/// pending bytes compaction tasks.
#[derive(Default)]
pub struct ScoredPolicy {
    // We use `(score, context_id)` as the key to dedup compactor with the same pending
    // bytes.
    //
    // It's possible that the `context_id` is in `compactor_to_score`, but `Compactor` is not in
    // `score_to_compactor` when `CompactorManager` recovers from original state, but the compactor
    // node has not yet subscribed to meta node.
    //
    // That is to say `score_to_compactor` should be a subset of `compactor_to_score`.
    score_to_compactor: BTreeMap<(Score, HummockContextId), Arc<Compactor>>,
    compactor_to_score: HashMap<HummockContextId, Score>,
}

impl ScoredPolicy {
    /// Initialize policy with already assigned tasks.
    pub fn new_with_task_assignment(task_assignment: &[CompactTaskAssignment]) -> Self {
        let mut compactor_to_score = HashMap::new();
        task_assignment.iter().for_each(|assignment| {
            let score_delta =
                Self::calculate_score_delta(assignment.compact_task.as_ref().unwrap());
            compactor_to_score
                .entry(assignment.context_id)
                .and_modify(|old_score| *old_score += score_delta)
                .or_insert(score_delta);
        });
        Self {
            score_to_compactor: BTreeMap::new(),
            compactor_to_score,
        }
    }

    #[cfg(test)]
    fn new_for_test() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn update_compactor_score(
        &mut self,
        context_id: HummockContextId,
        old_score: Score,
        compact_task: &CompactTask,
    ) {
        let new_score = old_score + Self::calculate_score_delta(compact_task);
        debug_assert!(new_score >= 0);

        // The element must exist.
        let compactor = self
            .score_to_compactor
            .remove(&(old_score, context_id))
            .unwrap();
        self.score_to_compactor
            .insert((new_score, context_id), compactor);
        *self.compactor_to_score.get_mut(&context_id).unwrap() = new_score;
    }

    fn calculate_score_delta(compact_task: &CompactTask) -> Score {
        let task_size = compact_task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|table| table.file_size)
            .sum::<u64>() as usize;
        if let TaskStatus::Pending = compact_task.task_status() {
            Score::try_from(task_size).unwrap()
        } else {
            -Score::try_from(task_size).unwrap()
        }
    }
}

impl CompactionSchedulePolicy for ScoredPolicy {
    fn next_idle_compactor(
        &self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
    ) -> Option<Arc<Compactor>> {
        for compactor in self.score_to_compactor.values() {
            if *compactor_assigned_task_num
                .get(&compactor.context_id())
                .unwrap()
                < compactor.max_concurrent_task_number()
            {
                return Some(compactor.clone());
            }
        }
        None
    }

    fn next_compactor(&self) -> Option<Arc<Compactor>> {
        if let Some((_, compactor)) = self.score_to_compactor.first_key_value() {
            Some(compactor.clone())
        } else {
            None
        }
    }

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        // If `context_id` already exists, we only need to update the task channel.
        let score = self.compactor_to_score.entry(context_id).or_insert(0);
        self.score_to_compactor.insert(
            (*score, context_id),
            Arc::new(Compactor::new(context_id, tx, max_concurrent_task_number)),
        );
        rx
    }

    fn remove_compactor(&mut self, context_id: HummockContextId) {
        if let Some(pending_bytes) = self.compactor_to_score.remove(&context_id) {
            self.score_to_compactor
                .remove(&(pending_bytes, context_id))
                .unwrap();
        }
    }

    fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        if let Some(score) = self.compactor_to_score.get(&context_id) {
            let compactor = self.score_to_compactor.get(&(*score, context_id));
            Some(compactor.unwrap().clone())
        } else {
            None
        }
    }

    fn assign_compact_task(
        &mut self,
        context_id: HummockContextId,
        compact_task: &CompactTask,
    ) -> Result<()> {
        if let Some(score) = self.compactor_to_score.get(&context_id) {
            self.update_compactor_score(context_id, *score, compact_task);
            Ok(())
        } else {
            Err(Error::InvalidContext(context_id))
        }
    }

    fn report_compact_task(&mut self, context_id: HummockContextId, task: &CompactTask) {
        debug_assert_ne!(task.task_status(), TaskStatus::Pending);
        if let Some(score) = self.compactor_to_score.get(&context_id) {
            self.update_compactor_score(context_id, *score, task);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::try_match_expand;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::HummockContextId;
    use risingwave_pb::hummock::compact_task::TaskStatus;
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::{CompactTask, CompactTaskAssignment, InputLevel, SstableInfo};
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction_schedule_policy::{
        CompactionSchedulePolicy, RoundRobinPolicy, ScoredPolicy,
    };
    use crate::hummock::test_utils::{
        commit_from_meta_node, generate_test_tables, get_sst_ids,
        register_sstable_infos_to_compaction_group, setup_compute_env_with_config,
        to_local_sstable_info,
    };
    use crate::hummock::HummockManager;
    use crate::model::MetadataModel;
    use crate::storage::MetaStore;

    async fn add_compact_task<S>(hummock_manager: &HummockManager<S>, _context_id: u32, epoch: u64)
    where
        S: MetaStore,
    {
        let original_tables = generate_test_tables(epoch, get_sst_ids(hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager(),
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
                    id: 0,
                    key_range: None,
                    file_size: input_file_size,
                    table_ids: vec![],
                }],
            }],
            splits: vec![],
            watermark: 0,
            sorted_output_ssts: vec![],
            task_id,
            target_level: 0,
            gc_delete_keys: false,
            task_status: TaskStatus::Pending as i32,
            compaction_group_id: StaticCompactionGroupId::StateDefault.into(),
            existing_table_ids: vec![],
            compression_algorithm: 0,
            target_file_size: 1,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
            target_sub_level_id: 0,
        }
    }

    #[tokio::test]
    async fn test_rr_add_remove_compactor() {
        let mut policy = RoundRobinPolicy::new();
        // No compactors by default.
        assert_eq!(policy.compactors.len(), 0);

        let mut receiver = policy.add_compactor(1, u64::MAX);
        assert_eq!(policy.compactors.len(), 1);
        let _receiver_2 = policy.add_compactor(2, u64::MAX);
        assert_eq!(policy.compactors.len(), 2);
        policy.remove_compactor(2);
        assert_eq!(policy.compactors.len(), 1);

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
            .build();
        let (_, hummock_manager, _, worker_node) = setup_compute_env_with_config(80, config).await;
        let context_id = worker_node.id;
        let mut compactor_manager = RoundRobinPolicy::new();
        add_compact_task(hummock_manager.as_ref(), context_id, 1).await;

        // No compactor available.
        assert!(compactor_manager.next_compactor().is_none());

        // Add a compactor.
        let mut receiver = compactor_manager.add_compactor(context_id, u64::MAX);
        assert_eq!(compactor_manager.compactors.len(), 1);
        let compactor = compactor_manager.next_compactor().unwrap();
        // No compact task.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = hummock_manager
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
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
            receivers.push(policy.add_compactor(context_id, u64::MAX));
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

    #[tokio::test]
    async fn test_scored_with_task_assignment() {
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(1)
            .max_bytes_for_level_base(1)
            .build();
        let (meta_env, hummock_manager, _, _) = setup_compute_env_with_config(80, config).await;
        // Assign dummy existing tasks.
        let existing_tasks = vec![dummy_compact_task(0, 1), dummy_compact_task(1, 1)];
        for (context_id, task) in existing_tasks.iter().enumerate() {
            hummock_manager
                .assign_compaction_task(task, context_id as HummockContextId)
                .await
                .unwrap();
        }
        let existing_assignments = CompactTaskAssignment::list(meta_env.meta_store())
            .await
            .unwrap();
        let mut policy = ScoredPolicy::new_with_task_assignment(&existing_assignments);
        assert_eq!(policy.score_to_compactor.len(), 0);
        assert_eq!(policy.compactor_to_score.len(), existing_tasks.len());

        // No compactor available.
        assert!(policy.next_idle_compactor(&HashMap::new()).is_none());
        assert!(policy.next_compactor().is_none());

        // Adding existing compactor does not change score.
        policy.add_compactor(0, u64::MAX);
        assert_eq!(policy.score_to_compactor.len(), 1);
        assert_eq!(policy.compactor_to_score.len(), existing_tasks.len());
        assert_eq!(*policy.compactor_to_score.get(&0).unwrap(), 1);
    }

    #[tokio::test]
    async fn test_scored_add_remove_compactor() {
        let mut policy = ScoredPolicy::new_for_test();
        // No compactors by default.
        assert_eq!(policy.compactor_to_score.len(), 0);
        assert_eq!(policy.score_to_compactor.len(), 0);

        let mut receiver = policy.add_compactor(1, u64::MAX);
        assert_eq!(policy.compactor_to_score.len(), 1);
        assert_eq!(policy.score_to_compactor.len(), 1);
        let _receiver_2 = policy.add_compactor(2, u64::MAX);
        assert_eq!(policy.compactor_to_score.len(), 2);
        assert_eq!(policy.score_to_compactor.len(), 2);
        policy.remove_compactor(2);
        assert_eq!(policy.compactor_to_score.len(), 1);
        assert_eq!(policy.score_to_compactor.len(), 1);

        // Pending bytes are initialized correctly.
        assert_eq!(*policy.compactor_to_score.get(&1).unwrap(), 0);
        assert!(policy.score_to_compactor.contains_key(&(0, 1)));

        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123, 0);
        let compactor = policy
            .score_to_compactor
            .first_entry()
            .unwrap()
            .get()
            .clone();
        compactor
            .send_task(Task::CompactTask(task.clone()))
            .await
            .unwrap();
        // Receive a compact task.
        let received_task = receiver.try_recv().unwrap().unwrap().task.unwrap();
        let received_compact_task = try_match_expand!(received_task, Task::CompactTask).unwrap();
        assert_eq!(received_compact_task, task);

        policy.remove_compactor(compactor.context_id());
        assert_eq!(policy.compactor_to_score.len(), 0);
        assert_eq!(policy.score_to_compactor.len(), 0);
        drop(compactor);
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));
    }

    #[test]
    fn test_scored_next_compactor() {
        let mut policy = ScoredPolicy::new_for_test();

        // No compactor available.
        assert!(policy.next_compactor().is_none());

        // Add 3 compactors.
        for context_id in 0..3 {
            policy.add_compactor(context_id, u64::MAX);
        }

        let task1 = dummy_compact_task(0, 5);
        let mut task2 = dummy_compact_task(1, 10);
        let task3 = dummy_compact_task(2, 7);
        let task4 = dummy_compact_task(3, 1);

        // Now the compactors should be (0, 0), (0, 1), (0, 2).
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task1)
            .unwrap();
        assert_eq!(compactor.context_id(), 0);
        assert_eq!(*policy.compactor_to_score.get(&0).unwrap(), 5);
        assert!(policy.score_to_compactor.contains_key(&(5, 0)));

        // (0, 1), (0, 2), (5, 0).
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task2)
            .unwrap();
        assert_eq!(compactor.context_id(), 1);
        assert_eq!(*policy.compactor_to_score.get(&1).unwrap(), 10);
        assert!(policy.score_to_compactor.contains_key(&(10, 1)));

        // (0, 2), (5, 0), (10, 1).
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task3)
            .unwrap();
        assert_eq!(compactor.context_id(), 2);
        assert_eq!(*policy.compactor_to_score.get(&2).unwrap(), 7);
        assert!(policy.score_to_compactor.contains_key(&(10, 1)));

        // (5, 0), (7, 2), (10, 1).
        task2.set_task_status(TaskStatus::Success);
        policy.report_compact_task(1, &task2);
        assert_eq!(*policy.compactor_to_score.get(&1).unwrap(), 0);
        assert!(policy.score_to_compactor.contains_key(&(0, 1)));

        // (0, 1), (5, 0), (7, 2).
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task4)
            .unwrap();
        assert_eq!(compactor.context_id(), 1);
        assert_eq!(*policy.compactor_to_score.get(&1).unwrap(), 1);
        assert!(policy.score_to_compactor.contains_key(&(1, 1)));
    }

    #[test]
    fn test_scored_next_idle_compactor() {
        let mut policy = Box::new(ScoredPolicy::new_for_test());

        // Add 2 compactors.
        for context_id in 0..2 {
            policy.add_compactor(context_id, 2);
        }

        let task1 = dummy_compact_task(0, 1);
        let task2 = dummy_compact_task(1, 1);
        let task3 = dummy_compact_task(2, 5);

        // Fill compactor 0 with small tasks.
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task1)
            .unwrap();
        assert_eq!(compactor.context_id(), 0);
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task3)
            .unwrap();
        assert_eq!(compactor.context_id(), 1);
        let compactor = policy.next_compactor().unwrap();
        policy
            .assign_compact_task(compactor.context_id(), &task2)
            .unwrap();
        assert_eq!(compactor.context_id(), 0);

        // Next compactor should be compactor 1.
        let mut compactor_assigned_task_num = HashMap::new();
        compactor_assigned_task_num.insert(0, 2);
        compactor_assigned_task_num.insert(1, 1);
        let compactor = policy
            .next_idle_compactor(&compactor_assigned_task_num)
            .unwrap();
        assert_eq!(compactor.context_id(), 1);
    }
}
