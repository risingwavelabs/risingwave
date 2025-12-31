// Copyright 2025 RisingWave Labs
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

use crate::hummock::compactor::iceberg_compaction::iceberg_compactor_runner::IcebergCompactionPlanRunner;

pub(crate) mod iceberg_compactor_runner;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::Notify;

/// Unique key combining `(task_id, plan_index)` since one task can have multiple plans.
pub(crate) type TaskKey = (u64, usize);

/// Task metadata for queue operations.
#[derive(Debug, Clone)]
pub struct IcebergTaskMeta {
    pub task_id: u64,
    pub plan_index: usize,
    /// Must be in range `1..=max_parallelism`
    pub required_parallelism: u32,
}

#[derive(Debug)]
pub struct PoppedIcebergTask {
    pub meta: IcebergTaskMeta,
    pub runner: Option<IcebergCompactionPlanRunner>,
}

impl IcebergTaskMeta {
    fn key(&self) -> TaskKey {
        (self.task_id, self.plan_index)
    }
}

/// Internal storage for the task queue.
struct IcebergTaskQueueInner {
    /// FIFO queue of waiting task metadata
    deque: VecDeque<IcebergTaskMeta>,
    /// Maps `(task_id, plan_index)` to `required_parallelism` for tracking
    id_map: HashMap<TaskKey, u32>,
    /// Sum of `required_parallelism` for all waiting tasks
    waiting_parallelism_sum: u32,
    /// Sum of `required_parallelism` for all running tasks
    running_parallelism_sum: u32,
    /// Optional runner payloads indexed by `(task_id, plan_index)`
    runners: HashMap<TaskKey, IcebergCompactionPlanRunner>,
}

/// FIFO task queue with parallelism-based scheduling for Iceberg compaction.
///
/// Tasks execute in submission order when sufficient parallelism is available.
/// The queue tracks waiting and running tasks to prevent over-commitment of resources.
///
/// Constraints:
/// - Each task requires `1..=max_parallelism` units
/// - Total waiting parallelism cannot exceed `pending_parallelism_budget`
/// - Total running parallelism cannot exceed `max_parallelism`
/// - Tasks block until enough parallelism is available
///
/// Note: The queue does NOT deduplicate or reorder tasks. Task management
/// (deduplication, merging, cancellation) is Meta's responsibility.
pub struct IcebergTaskQueue {
    inner: IcebergTaskQueueInner,
    /// Maximum concurrent parallelism for running tasks
    max_parallelism: u32,
    /// Maximum total parallelism for waiting tasks (backpressure limit)
    pending_parallelism_budget: u32,
    /// Notification for event-driven scheduling
    schedule_notify: Arc<Notify>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Added,
    /// Would exceed `pending_parallelism_budget`
    RejectedCapacity,
    /// `required_parallelism` > `max_parallelism`
    RejectedTooLarge,
    /// `required_parallelism` == 0
    RejectedInvalidParallelism,
    /// Task with same `(task_id, plan_index)` already exists
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
                runners: HashMap::new(),
            },
            max_parallelism,
            pending_parallelism_budget,
            schedule_notify: Arc::new(Notify::new()),
        }
    }

    /// Waits until there are tasks that can be scheduled.
    ///
    /// Returns `true` if there are schedulable tasks, `false` otherwise.
    /// Use this in a `tokio::select!` to wake up when tasks become schedulable.
    pub async fn wait_schedulable(&self) -> bool {
        // Check if we have tasks that can be scheduled right now
        if self.has_schedulable_tasks() {
            return true;
        }
        // Otherwise wait for notification
        self.schedule_notify.notified().await;
        self.has_schedulable_tasks()
    }

    fn has_schedulable_tasks(&self) -> bool {
        if let Some(front_task) = self.inner.deque.front() {
            let available_parallelism = self
                .max_parallelism
                .saturating_sub(self.inner.running_parallelism_sum);
            available_parallelism >= front_task.required_parallelism
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

    fn available_parallelism(&self) -> u32 {
        self.max_parallelism
            .saturating_sub(self.inner.running_parallelism_sum)
    }

    /// Push a task into the queue.
    ///
    /// The task is validated and added to the end of the FIFO queue if constraints are met.
    pub fn push(
        &mut self,
        meta: IcebergTaskMeta,
        runner: Option<IcebergCompactionPlanRunner>,
    ) -> PushResult {
        if meta.required_parallelism == 0 {
            return PushResult::RejectedInvalidParallelism;
        }
        if meta.required_parallelism > self.max_parallelism {
            return PushResult::RejectedTooLarge;
        }

        let key = meta.key();

        // Reject duplicate keys to prevent inconsistent state between id_map and deque
        if self.inner.id_map.contains_key(&key) {
            return PushResult::RejectedDuplicate;
        }

        let new_total = self.inner.waiting_parallelism_sum + meta.required_parallelism;
        if new_total > self.pending_parallelism_budget {
            return PushResult::RejectedCapacity;
        }

        self.inner.id_map.insert(key, meta.required_parallelism);
        self.inner.waiting_parallelism_sum = new_total;

        if let Some(r) = runner {
            self.inner.runners.insert(key, r);
        }

        self.inner.deque.push_back(meta);

        self.notify_schedulable();
        PushResult::Added
    }

    /// Pop the next task if sufficient parallelism is available.
    ///
    /// Returns `None` if the queue is empty or the front task cannot fit
    /// within the available parallelism budget.
    pub fn pop(&mut self) -> Option<PoppedIcebergTask> {
        let front = self.inner.deque.front()?;
        if front.required_parallelism > self.available_parallelism() {
            return None;
        }

        let meta = self.inner.deque.pop_front()?;
        self.inner.waiting_parallelism_sum = self
            .inner
            .waiting_parallelism_sum
            .saturating_sub(meta.required_parallelism);
        self.inner.running_parallelism_sum = self
            .inner
            .running_parallelism_sum
            .saturating_add(meta.required_parallelism);

        let runner = self.inner.runners.remove(&meta.key());
        Some(PoppedIcebergTask { meta, runner })
    }

    /// Mark a task as finished, freeing its parallelism for other tasks.
    ///
    /// Returns `true` if the task was found and removed, `false` otherwise.
    pub fn finish_running(&mut self, task_key: TaskKey) -> bool {
        let Some(required) = self.inner.id_map.remove(&task_key) else {
            tracing::warn!(
                task_id = task_key.0,
                plan_index = task_key.1,
                "finish_running called for unknown task key, possible bug: double-finish or invalid key"
            );
            return false;
        };

        self.inner.running_parallelism_sum =
            self.inner.running_parallelism_sum.saturating_sub(required);
        self.inner.runners.remove(&task_key);
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
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
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
        assert_eq!(q.push(mk_meta(1, 0, 2), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 2), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(3, 0, 2), None), PushResult::Added);

        assert_eq!(q.pop().unwrap().meta.task_id, 1);
        assert_eq!(q.pop().unwrap().meta.task_id, 2);
        assert_eq!(q.pop().unwrap().meta.task_id, 3);
    }

    #[test]
    fn test_capacity_reject() {
        let mut q = IcebergTaskQueue::new(4, 6);
        assert_eq!(q.push(mk_meta(1, 0, 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 3), None), PushResult::Added); // sum=6
        assert_eq!(q.push(mk_meta(3, 0, 1), None), PushResult::RejectedCapacity); // would exceed
    }

    #[test]
    fn test_invalid_parallelism() {
        let mut q = IcebergTaskQueue::new(4, 10);
        assert_eq!(
            q.push(mk_meta(1, 0, 0), None),
            PushResult::RejectedInvalidParallelism
        );
        assert_eq!(q.push(mk_meta(2, 0, 5), None), PushResult::RejectedTooLarge); // > max
    }

    #[test]
    fn test_duplicate_key_rejected() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 3), None), PushResult::Added);
        // Same (task_id, plan_index) should be rejected
        assert_eq!(
            q.push(mk_meta(1, 0, 5), None),
            PushResult::RejectedDuplicate
        );
        // Parallelism sum should not have changed
        assert_eq!(q.waiting_parallelism_sum(), 3);

        // Different plan_index is allowed
        assert_eq!(q.push(mk_meta(1, 1, 2), None), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 5);

        // After pop and finish, the key can be reused
        let p = q.pop().unwrap();
        assert_eq!(p.meta.task_id, 1);
        assert_eq!(p.meta.plan_index, 0);
        q.finish_running((1, 0));

        // Now the same key can be pushed again
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
    }

    #[test]
    fn test_pop_insufficient_parallelism() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 6), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, 0, 4), None), PushResult::Added);

        let p1 = q.pop().unwrap();
        assert_eq!(p1.meta.task_id, 1);
        // Not enough remaining parallelism (only 2 left)
        assert!(q.pop().is_none());

        // Finish first, then second becomes schedulable
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
    fn test_double_finish() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
        q.pop().unwrap();
        assert_eq!(q.running_parallelism_sum(), 4);

        // First finish succeeds
        assert!(q.finish_running((1, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);

        // Second finish on same key returns false (triggers warn log)
        assert!(!q.finish_running((1, 0)));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_max_parallelism_boundary() {
        // pending_budget == max_parallelism: minimal valid configuration
        let mut q = IcebergTaskQueue::new(4, 4);

        // Task with required_parallelism == max_parallelism should be accepted
        assert_eq!(q.push(mk_meta(1, 0, 4), None), PushResult::Added);
        // Budget exhausted
        assert_eq!(q.push(mk_meta(2, 0, 1), None), PushResult::RejectedCapacity);

        // Can pop and run at full parallelism
        let p = q.pop().unwrap();
        assert_eq!(p.meta.required_parallelism, 4);
        assert_eq!(q.running_parallelism_sum(), 4);

        // No room for any new running task
        assert_eq!(q.push(mk_meta(3, 0, 1), None), PushResult::Added);
        assert!(q.pop().is_none());
    }

    #[test]
    fn test_same_task_id_multiple_plans() {
        let mut q = IcebergTaskQueue::new(10, 30);
        let task_id = 1u64;

        // Same task_id with different plan_index are independent
        assert_eq!(q.push(mk_meta(task_id, 0, 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 1, 4), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(task_id, 2, 2), None), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 9);

        // Pop all
        for i in 0..3 {
            let p = q.pop().unwrap();
            assert_eq!(p.meta.task_id, task_id);
            assert_eq!(p.meta.plan_index, i);
        }
        assert_eq!(q.running_parallelism_sum(), 9);

        // Finish out of order
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
}
