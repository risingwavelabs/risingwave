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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use rand::Rng;
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse};
use tokio::sync::mpsc::Receiver;

use super::Compactor;
use crate::MetaResult;

const STREAM_BUFFER_SIZE: usize = 4;

/// The implementation of compaction task scheduling policy.
pub trait CompactionSchedulePolicy: Send + Sync {
    /// Get next idle compactor to assign task.
    fn next_idle_compactor(
        &mut self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>>;

    /// Get next compactor to assign task.
    fn next_compactor(&mut self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>>;

    fn random_compactor(&mut self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>>;

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>>;

    fn remove_compactor(&mut self, context_id: HummockContextId);

    /// Notify the `CompactorManagerInner` of the completion of a task to adjust the next compactor
    /// to schedule.
    fn report_compact_task(&mut self, context_id: HummockContextId, task: &CompactTask);
}

// This strategy is retained just for reference, it is not used.
pub struct RoundRobinPolicy {
    /// Senders of stream to available compactors.
    compactors: Vec<Arc<Compactor>>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
}

impl RoundRobinPolicy {
    pub fn new() -> Self {
        Self {
            compactors: vec![],
            next_compactor: 0,
        }
    }
}
impl CompactionSchedulePolicy for RoundRobinPolicy {
    fn next_idle_compactor(
        &mut self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>> {
        let mut visited = HashSet::new();
        loop {
            match self.next_compactor(compact_task) {
                None => {
                    return None;
                }
                Some(compactor) => {
                    if visited.contains(&compactor.context_id()) {
                        return None;
                    }
                    if *compactor_assigned_task_num
                        .get(&compactor.context_id())
                        .unwrap()
                        < compactor.max_concurrent_task_number()
                    {
                        return Some(compactor);
                    }
                    visited.insert(compactor.context_id());
                }
            }
        }
    }

    fn next_compactor(&mut self, _compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        if self.compactors.is_empty() {
            return None;
        }
        let compactor_index = self.next_compactor % self.compactors.len();
        let compactor = self.compactors[compactor_index].clone();
        self.next_compactor += 1;
        Some(compactor)
    }

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.compactors.retain(|c| c.context_id() != context_id);
        self.compactors.push(Arc::new(Compactor::new(
            context_id,
            tx,
            max_concurrent_task_number,
        )));
        tracing::info!("Added compactor session {}", context_id);
        rx
    }

    fn random_compactor(&mut self, _compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        if self.compactors.is_empty() {
            return None;
        }

        let compactor_index = rand::thread_rng().gen::<usize>() % self.compactors.len();
        let compactor = self.compactors[compactor_index].clone();
        Some(compactor)
    }

    fn remove_compactor(&mut self, context_id: HummockContextId) {
        self.compactors.retain(|c| c.context_id() != context_id);
        tracing::info!("Removed compactor session {}", context_id);
    }

    fn report_compact_task(&mut self, _context_id: HummockContextId, _task: &CompactTask) {}
}

/// Give priority to compactors with the least score. Currently the score is composed only of
/// pending bytes compaction tasks.
#[derive(Default)]
pub struct ScoredPolicy {
    // These two data structures must be consistent.
    // We use `(pending_bytes, context_id)` as the key to dedup compactor with the same pending
    // bytes.
    score_to_compactor: BTreeMap<(usize, HummockContextId), Arc<Compactor>>,
    compactor_to_score: HashMap<HummockContextId, usize>,
}

impl ScoredPolicy {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn update_compactor_score(
        &mut self,
        context_id: HummockContextId,
        old_score: usize,
        compact_task: Option<&CompactTask>,
    ) -> Arc<Compactor> {
        let mut new_score = old_score;
        if let Some(task) = compact_task {
            let task_size = task
                .input_ssts
                .iter()
                .flat_map(|level| level.table_infos.iter())
                .map(|table| table.file_size)
                .sum::<u64>() as usize;
            if let TaskStatus::Pending = task.task_status() {
                new_score += task_size
            } else {
                debug_assert!(old_score >= task_size);
                new_score -= task_size
            }
        };
        // The element must exist.
        let compactor = self
            .score_to_compactor
            .remove(&(old_score, context_id))
            .unwrap();
        self.score_to_compactor
            .insert((new_score, context_id), compactor.clone());
        *self.compactor_to_score.get_mut(&context_id).unwrap() = new_score;
        compactor
    }
}

impl CompactionSchedulePolicy for ScoredPolicy {
    fn next_idle_compactor(
        &mut self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>> {
        let mut next_idle_compactor = None;
        for ((score, context_id), compactor) in &self.score_to_compactor {
            if *compactor_assigned_task_num
                .get(&compactor.context_id())
                .unwrap()
                < compactor.max_concurrent_task_number()
            {
                next_idle_compactor = Some((*score, *context_id));
                break;
            }
        }

        if let Some((score, context_id)) = next_idle_compactor {
            let compactor = self.update_compactor_score(context_id, score, compact_task);
            Some(compactor)
        } else {
            None
        }
    }

    fn next_compactor(&mut self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        let mut next_compactor = None;
        if let Some((score, context_id)) = self
            .score_to_compactor
            .keys()
            .min_by(|(pb1, _), (pb2, _)| pb1.cmp(pb2))
        {
            next_compactor = Some((*score, *context_id));
        }

        if let Some((score, context_id)) = next_compactor {
            let compactor = self.update_compactor_score(context_id, score, compact_task);
            Some(compactor)
        } else {
            None
        }
    }

    fn random_compactor(&mut self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        if self.score_to_compactor.is_empty() {
            return None;
        }

        let compactor_index = rand::thread_rng().gen::<usize>() % self.compactor_to_score.len();
        // `BTreeMap` does not record subtree size, so O(logn) method to find the nth smallest
        // element is not available.
        let (context_id, score) = self.compactor_to_score.iter().nth(compactor_index).unwrap();

        let compactor = self.update_compactor_score(*context_id, *score, compact_task);
        Some(compactor)
    }

    fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        if let Some(pending_bytes) = self.compactor_to_score.remove(&context_id) {
            self.score_to_compactor
                .remove(&(pending_bytes, context_id))
                .unwrap();
        }
        self.score_to_compactor.insert(
            (0, context_id),
            Arc::new(Compactor::new(context_id, tx, max_concurrent_task_number)),
        );
        self.compactor_to_score.insert(context_id, 0);
        tracing::info!("Added compactor session {}", context_id);
        rx
    }

    fn remove_compactor(&mut self, context_id: HummockContextId) {
        if let Some(pending_bytes) = self.compactor_to_score.remove(&context_id) {
            self.score_to_compactor
                .remove(&(pending_bytes, context_id))
                .unwrap();
        }
        tracing::info!("Removed compactor session {}", context_id);
    }

    fn report_compact_task(&mut self, context_id: HummockContextId, task: &CompactTask) {
        debug_assert_ne!(task.task_status(), TaskStatus::Pending);
        if let Some(score) = self.compactor_to_score.get(&context_id) {
            self.update_compactor_score(context_id, *score, Some(task));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::try_match_expand;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::compact_task::TaskStatus;
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::{CompactTask, InputLevel, SstableInfo};
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
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::storage::MetaStore;

    async fn add_compact_task<S>(hummock_manager: &HummockManager<S>, _context_id: u32, epoch: u64)
    where
        S: MetaStore,
    {
        let original_tables = generate_test_tables(epoch, get_sst_ids(hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager_ref_for_test(),
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
        let mut compactor_manager = RoundRobinPolicy::new();
        // No compactors by default.
        assert_eq!(compactor_manager.compactors.len(), 0);

        let mut receiver = compactor_manager.add_compactor(1, u64::MAX);
        assert_eq!(compactor_manager.compactors.len(), 1);
        let _receiver_2 = compactor_manager.add_compactor(2, u64::MAX);
        assert_eq!(compactor_manager.compactors.len(), 2);
        compactor_manager.remove_compactor(2);
        assert_eq!(compactor_manager.compactors.len(), 1);

        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123, 0);
        let compactor = compactor_manager.compactors.first().unwrap().clone();
        compactor
            .send_task(Task::CompactTask(task.clone()))
            .await
            .unwrap();
        // Receive a compact task.
        let received_task = receiver.try_recv().unwrap().unwrap().task.unwrap();
        let received_compact_task = try_match_expand!(received_task, Task::CompactTask).unwrap();
        assert_eq!(received_compact_task, task);

        compactor_manager.remove_compactor(compactor.context_id());
        assert_eq!(compactor_manager.compactors.len(), 0);
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
        assert!(compactor_manager.next_compactor(None).is_none());

        // Add a compactor.
        let mut receiver = compactor_manager.add_compactor(context_id, u64::MAX);
        assert_eq!(compactor_manager.compactors.len(), 1);
        let compactor = compactor_manager.next_compactor(None).unwrap();
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
        assert!(compactor_manager.next_compactor(None).is_none());
    }

    #[tokio::test]
    async fn test_rr_next_compactor_round_robin() {
        let mut compactor_manager = RoundRobinPolicy::new();
        let mut receivers = vec![];
        for context_id in 0..5 {
            receivers.push(compactor_manager.add_compactor(context_id, u64::MAX));
        }
        assert_eq!(compactor_manager.compactors.len(), 5);
        for i in 0..receivers.len() * 3 {
            let compactor = compactor_manager.next_compactor(None).unwrap();
            assert_eq!(compactor.context_id() as usize, i % receivers.len());
        }
    }

    #[tokio::test]
    async fn test_scored_add_remove_compactor() {
        let mut compactor_manager = ScoredPolicy::new();
        // No compactors by default.
        assert_eq!(compactor_manager.compactor_to_score.len(), 0);
        assert_eq!(compactor_manager.score_to_compactor.len(), 0);

        let mut receiver = compactor_manager.add_compactor(1, u64::MAX);
        assert_eq!(compactor_manager.compactor_to_score.len(), 1);
        assert_eq!(compactor_manager.score_to_compactor.len(), 1);
        let _receiver_2 = compactor_manager.add_compactor(2, u64::MAX);
        assert_eq!(compactor_manager.compactor_to_score.len(), 2);
        assert_eq!(compactor_manager.score_to_compactor.len(), 2);
        compactor_manager.remove_compactor(2);
        assert_eq!(compactor_manager.compactor_to_score.len(), 1);
        assert_eq!(compactor_manager.score_to_compactor.len(), 1);

        // Pending bytes are initialized correctly.
        assert_eq!(*compactor_manager.compactor_to_score.get(&1).unwrap(), 0);
        assert!(compactor_manager.score_to_compactor.contains_key(&(0, 1)));

        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123, 0);
        let compactor = compactor_manager
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

        compactor_manager.remove_compactor(compactor.context_id());
        assert_eq!(compactor_manager.compactor_to_score.len(), 0);
        assert_eq!(compactor_manager.score_to_compactor.len(), 0);
        drop(compactor);
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));
    }

    #[test]
    fn test_scored_next_compactor() {
        let mut compactor_manager = ScoredPolicy::new();

        // No compactor available.
        assert!(compactor_manager.next_compactor(None).is_none());

        // Add 3 compactors.
        for context_id in 0..3 {
            compactor_manager.add_compactor(context_id, u64::MAX);
        }

        let task1 = dummy_compact_task(0, 5);
        let mut task2 = dummy_compact_task(1, 10);
        let task3 = dummy_compact_task(2, 7);
        let task4 = dummy_compact_task(3, 1);

        // Now the compactors should be (0, 0), (0, 1), (0, 2).
        let compactor = compactor_manager.next_compactor(Some(&task1)).unwrap();
        assert_eq!(compactor.context_id(), 0);
        assert_eq!(*compactor_manager.compactor_to_score.get(&0).unwrap(), 5);
        assert!(compactor_manager.score_to_compactor.contains_key(&(5, 0)));

        // (0, 1), (0, 2), (5, 0).
        let compactor = compactor_manager.next_compactor(Some(&task2)).unwrap();
        assert_eq!(compactor.context_id(), 1);
        assert_eq!(*compactor_manager.compactor_to_score.get(&1).unwrap(), 10);
        assert!(compactor_manager.score_to_compactor.contains_key(&(10, 1)));

        // (0, 2), (5, 0), (10, 1).
        let compactor = compactor_manager.next_compactor(Some(&task3)).unwrap();
        assert_eq!(compactor.context_id(), 2);
        assert_eq!(*compactor_manager.compactor_to_score.get(&2).unwrap(), 7);
        assert!(compactor_manager.score_to_compactor.contains_key(&(10, 1)));

        // (5, 0), (7, 2), (10, 1).
        task2.set_task_status(TaskStatus::Success);
        compactor_manager.report_compact_task(1, &task2);
        assert_eq!(*compactor_manager.compactor_to_score.get(&1).unwrap(), 0);
        assert!(compactor_manager.score_to_compactor.contains_key(&(0, 1)));

        // (0, 1), (5, 0), (7, 2).
        let compactor = compactor_manager.next_compactor(Some(&task4)).unwrap();
        assert_eq!(compactor.context_id(), 1);
        assert_eq!(*compactor_manager.compactor_to_score.get(&1).unwrap(), 1);
        assert!(compactor_manager.score_to_compactor.contains_key(&(1, 1)));
    }

    #[test]
    fn test_scored_next_idle_compactor() {
        let compactor_manager = CompactorManager::new_with_policy(Box::new(ScoredPolicy::new()));

        // Add 2 compactors.
        for context_id in 0..2 {
            compactor_manager.add_compactor(context_id, 2);
        }

        let task1 = dummy_compact_task(0, 1);
        let task2 = dummy_compact_task(1, 1);
        let task3 = dummy_compact_task(2, 5);

        // Fill compactor 0 with small tasks.
        let compactor = compactor_manager.next_compactor(Some(&task1)).unwrap();
        assert_eq!(compactor.context_id(), 0);
        let compactor = compactor_manager.next_compactor(Some(&task3)).unwrap();
        assert_eq!(compactor.context_id(), 1);
        let compactor = compactor_manager.next_compactor(Some(&task2)).unwrap();
        assert_eq!(compactor.context_id(), 0);

        // Next compactor should be compactor 1.
        let compactor = compactor_manager.next_idle_compactor(None).unwrap();
        assert_eq!(compactor.context_id(), 1);
    }
}
