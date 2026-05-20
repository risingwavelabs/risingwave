// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::Notify;

use super::TaskKey;

#[derive(Debug, Clone)]
pub struct IcebergTaskMeta {
    pub task_id: u64,
    pub plan_index: usize,
    /// Must be in range `1..=max_parallelism`.
    pub required_parallelism: u32,
}

#[derive(Debug)]
pub struct PoppedIcebergTask {
    pub meta: IcebergTaskMeta,
}

impl IcebergTaskMeta {
    pub fn key(&self) -> TaskKey {
        (self.task_id, self.plan_index)
    }
}

struct IcebergTaskQueueInner {
    deque: VecDeque<IcebergTaskMeta>,
    id_map: HashMap<TaskKey, IcebergTaskQueueEntry>,
    waiting_parallelism_sum: u32,
    running_parallelism_sum: u32,
}

#[derive(Debug)]
struct IcebergTaskQueueEntry {
    required_parallelism: u32,
    state: IcebergTaskQueueEntryState,
}

#[derive(Debug, PartialEq, Eq)]
enum IcebergTaskQueueEntryState {
    Waiting,
    Running,
}

/// FIFO task queue with parallelism-based scheduling for Iceberg compaction.
pub struct IcebergTaskQueue {
    inner: IcebergTaskQueueInner,
    max_parallelism: u32,
    pending_parallelism_budget: u32,
    schedule_notify: Arc<Notify>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Added,
    RejectedCapacity,
    RejectedTooLarge,
    RejectedInvalidParallelism,
    RejectedDuplicate,
}

impl IcebergTaskQueue {
    pub fn new(max_parallelism: u32, pending_parallelism_budget: u32) -> Self {
        assert!(max_parallelism > 0, "max_parallelism must be > 0");
        assert!(
            pending_parallelism_budget >= max_parallelism,
            "pending budget should allow at least one task"
        );
        Self {
            inner: IcebergTaskQueueInner {
                deque: VecDeque::new(),
                id_map: HashMap::new(),
                waiting_parallelism_sum: 0,
                running_parallelism_sum: 0,
            },
            max_parallelism,
            pending_parallelism_budget,
            schedule_notify: Arc::new(Notify::new()),
        }
    }

    pub async fn wait_schedulable(&self) -> bool {
        if self.has_schedulable_tasks() {
            return true;
        }
        self.schedule_notify.notified().await;
        self.has_schedulable_tasks()
    }

    fn has_schedulable_tasks(&self) -> bool {
        if let Some(front_task) = self.inner.deque.front() {
            self.available_running_parallelism() >= front_task.required_parallelism
        } else {
            false
        }
    }

    fn notify_schedulable(&self) {
        if self.has_schedulable_tasks() {
            self.schedule_notify.notify_one();
        }
    }

    pub fn running_parallelism_sum(&self) -> u32 {
        self.inner.running_parallelism_sum
    }

    pub fn waiting_parallelism_sum(&self) -> u32 {
        self.inner.waiting_parallelism_sum
    }

    fn available_running_parallelism(&self) -> u32 {
        self.max_parallelism
            .saturating_sub(self.inner.running_parallelism_sum)
    }

    pub fn push(&mut self, meta: IcebergTaskMeta) -> PushResult {
        if meta.required_parallelism == 0 {
            return PushResult::RejectedInvalidParallelism;
        }
        if meta.required_parallelism > self.max_parallelism {
            return PushResult::RejectedTooLarge;
        }

        let key = meta.key();
        if self.inner.id_map.contains_key(&key) {
            return PushResult::RejectedDuplicate;
        }

        let new_total = self.inner.waiting_parallelism_sum + meta.required_parallelism;
        if new_total > self.pending_parallelism_budget {
            return PushResult::RejectedCapacity;
        }

        self.inner.id_map.insert(
            key,
            IcebergTaskQueueEntry {
                required_parallelism: meta.required_parallelism,
                state: IcebergTaskQueueEntryState::Waiting,
            },
        );
        self.inner.waiting_parallelism_sum = new_total;
        self.inner.deque.push_back(meta);

        self.notify_schedulable();
        PushResult::Added
    }

    pub fn pop(&mut self) -> Option<PoppedIcebergTask> {
        let front = self.inner.deque.front()?;
        if front.required_parallelism > self.available_running_parallelism() {
            return None;
        }

        let meta = self.inner.deque.pop_front()?;
        let entry = self
            .inner
            .id_map
            .get_mut(&meta.key())
            .expect("queued task should have an id map entry");
        debug_assert_eq!(entry.state, IcebergTaskQueueEntryState::Waiting);
        entry.state = IcebergTaskQueueEntryState::Running;

        self.inner.waiting_parallelism_sum = self
            .inner
            .waiting_parallelism_sum
            .saturating_sub(meta.required_parallelism);
        self.inner.running_parallelism_sum = self
            .inner
            .running_parallelism_sum
            .saturating_add(meta.required_parallelism);

        Some(PoppedIcebergTask { meta })
    }

    pub fn finish_running(&mut self, task_key: TaskKey) -> bool {
        let Some(entry) = self.inner.id_map.get(&task_key) else {
            tracing::warn!(
                task_id = task_key.0,
                plan_index = task_key.1,
                "finish_running called for unknown task key, possible bug: double-finish or invalid key"
            );
            return false;
        };

        if entry.state != IcebergTaskQueueEntryState::Running {
            tracing::warn!(
                task_id = task_key.0,
                plan_index = task_key.1,
                "finish_running called for a task that has not been scheduled yet"
            );
            return false;
        }

        let required = entry.required_parallelism;
        self.inner.id_map.remove(&task_key);
        self.inner.running_parallelism_sum =
            self.inner.running_parallelism_sum.saturating_sub(required);
        self.notify_schedulable();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_meta(id: u64, plan_index: usize, p: u32) -> IcebergTaskMeta {
        IcebergTaskMeta {
            task_id: id,
            plan_index,
            required_parallelism: p,
        }
    }

    #[test]
    fn test_basic_push_pop() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 4)), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 4);

        let popped = q.pop().expect("should pop");
        assert_eq!(popped.meta.task_id, 1);
        assert_eq!(q.waiting_parallelism_sum(), 0);
        assert_eq!(q.running_parallelism_sum(), 4);

        assert!(q.finish_running((1, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_fifo_ordering() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 2)), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 2)), PushResult::Added);
        assert_eq!(q.push(mk_meta(3, 0, 2)), PushResult::Added);

        assert_eq!(q.pop().unwrap().meta.task_id, 1);
        assert_eq!(q.pop().unwrap().meta.task_id, 2);
        assert_eq!(q.pop().unwrap().meta.task_id, 3);
    }

    #[test]
    fn test_capacity_reject() {
        let mut q = IcebergTaskQueue::new(4, 6);
        assert_eq!(q.push(mk_meta(1, 0, 3)), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 3)), PushResult::Added);
        assert_eq!(q.push(mk_meta(3, 0, 1)), PushResult::RejectedCapacity);
    }

    #[test]
    fn test_invalid_parallelism() {
        let mut q = IcebergTaskQueue::new(4, 10);
        assert_eq!(
            q.push(mk_meta(1, 0, 0)),
            PushResult::RejectedInvalidParallelism
        );
        assert_eq!(q.push(mk_meta(2, 0, 5)), PushResult::RejectedTooLarge);
    }

    #[test]
    fn test_duplicate_key_rejected() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 3)), PushResult::Added);
        assert_eq!(q.push(mk_meta(1, 0, 5)), PushResult::RejectedDuplicate);
        assert_eq!(q.waiting_parallelism_sum(), 3);

        assert_eq!(q.push(mk_meta(1, 1, 2)), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 5);

        let p = q.pop().unwrap();
        assert_eq!(p.meta.task_id, 1);
        assert_eq!(p.meta.plan_index, 0);
        q.finish_running((1, 0));

        assert_eq!(q.push(mk_meta(1, 0, 4)), PushResult::Added);
    }

    #[test]
    fn test_pop_insufficient_parallelism() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 6)), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 4)), PushResult::Added);

        let p1 = q.pop().unwrap();
        assert_eq!(p1.meta.task_id, 1);
        assert!(q.pop().is_none());

        assert!(q.finish_running((1, 0)));
        let p2 = q.pop().unwrap();
        assert_eq!(p2.meta.task_id, 2);
    }

    #[test]
    fn test_finish_nonexistent_task() {
        let mut q = IcebergTaskQueue::new(4, 16);
        assert!(!q.finish_running((999, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_finish_waiting_task_does_not_corrupt_queue() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 4)), PushResult::Added);

        assert!(!q.finish_running((1, 0)));
        assert_eq!(q.waiting_parallelism_sum(), 4);
        assert_eq!(q.running_parallelism_sum(), 0);
        assert_eq!(q.push(mk_meta(1, 0, 2)), PushResult::RejectedDuplicate);

        let popped = q.pop().unwrap();
        assert_eq!(popped.meta.key(), (1, 0));
        assert_eq!(q.waiting_parallelism_sum(), 0);
        assert_eq!(q.running_parallelism_sum(), 4);

        assert!(q.finish_running((1, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_double_finish() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 4)), PushResult::Added);
        q.pop().unwrap();
        assert_eq!(q.running_parallelism_sum(), 4);

        assert!(q.finish_running((1, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);

        assert!(!q.finish_running((1, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_max_parallelism_boundary() {
        let mut q = IcebergTaskQueue::new(4, 4);
        assert_eq!(q.push(mk_meta(1, 0, 4)), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 1)), PushResult::RejectedCapacity);

        let p = q.pop().unwrap();
        assert_eq!(p.meta.required_parallelism, 4);
        assert_eq!(q.running_parallelism_sum(), 4);

        assert_eq!(q.push(mk_meta(3, 0, 1)), PushResult::Added);
        assert!(q.pop().is_none());
    }

    #[test]
    fn test_same_task_id_multiple_plans() {
        let mut q = IcebergTaskQueue::new(10, 30);
        let task_id = 1u64;

        assert_eq!(q.push(mk_meta(task_id, 0, 3)), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 1, 4)), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 2, 2)), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 9);

        for i in 0..3 {
            let p = q.pop().unwrap();
            assert_eq!(p.meta.task_id, task_id);
            assert_eq!(p.meta.plan_index, i);
        }
        assert_eq!(q.running_parallelism_sum(), 9);

        assert!(q.finish_running((task_id, 1)));
        assert_eq!(q.running_parallelism_sum(), 5);
        assert!(q.finish_running((task_id, 0)));
        assert!(q.finish_running((task_id, 2)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_empty_queue_behavior() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert!(q.pop().is_none());
        assert!(!q.finish_running((1, 0)));
        assert_eq!(q.waiting_parallelism_sum(), 0);
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_capacity_reject_keeps_already_queued_tasks() {
        let mut q = IcebergTaskQueue::new(4, 6);
        assert_eq!(q.push(mk_meta(1, 0, 3)), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 2)), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 1, 2)), PushResult::RejectedCapacity);

        assert_eq!(q.waiting_parallelism_sum(), 5);
        assert_eq!(q.pop().unwrap().meta.key(), (1, 0));
        assert!(q.finish_running((1, 0)));
        assert_eq!(q.pop().unwrap().meta.key(), (2, 0));
        assert!(q.pop().is_none());
    }
}
