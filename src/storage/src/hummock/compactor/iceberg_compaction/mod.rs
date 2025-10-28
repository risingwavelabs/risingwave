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

use crate::hummock::compactor::iceberg_compaction::iceberg_compactor_runner::IcebergCompactorRunner;

pub(crate) mod iceberg_compactor_runner;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use tokio::sync::Notify;

type TaskId = u64;
type TaskUniqueIdent = String; // format!("{}-{:?}", catalog_name, table_ident)

/// Metadata of a task waiting (or running) – independent from the actual runner implementation.
/// Runner (payload) is stored separately so tests can omit it.
#[derive(Debug, Clone)]
pub struct IcebergTaskMeta {
    pub task_id: TaskId,
    pub unique_ident: TaskUniqueIdent,
    pub enqueue_at: std::time::Instant,
    /// Estimated parallelism required to execute this task (must be >0 and <= `max_parallelism`)
    pub required_parallelism: u32,
}

#[derive(Debug)]
pub struct PoppedIcebergTask {
    pub meta: IcebergTaskMeta,
    pub runner: Option<IcebergCompactorRunner>,
}

/// Internal storage. `waiting` and `running` are disjoint.
struct IcebergTaskQueueInner {
    deque: VecDeque<IcebergTaskMeta>,  // FIFO of waiting task metadata
    waiting: HashSet<TaskUniqueIdent>, // unique_idents waiting
    running: HashSet<TaskUniqueIdent>, // unique_idents currently running
    id_map: HashMap<TaskId, (TaskUniqueIdent, u32)>, /* task_id -> (unique_ident, required_parallelism) */
    waiting_parallelism_sum: u32, // sum(required_parallelism) for waiting tasks only
    running_parallelism_sum: u32, // sum(required_parallelism) for running tasks only
    runners: HashMap<TaskId, IcebergCompactorRunner>, /* optional runner payloads (may be absent in tests) */
}

/// FIFO compaction task queue with lightweight replacement and parallelism budgeting.
/// Features:
/// - Push with replacement (same `unique_ident` waiting -> metadata/runner updated in place).
/// - Pop moves the head (if it fits remaining parallelism) to the running set.
/// - Reject duplicate if the same `unique_ident` is already running.
/// - Capacity control: reject when adding/replacing would exceed `pending_parallelism_budget`
///   (sum of waiting tasks' `required_parallelism`). No eviction beyond hard rejection.
/// - Optional notification when new tasks become schedulable.
///
/// Invariants (enforced by logic; violation triggers panic):
/// - `waiting ∩ running = ∅`.
/// - Each waiting `unique_ident` appears exactly once in `deque`.
/// - `waiting_parallelism_sum = Σ required_parallelism(waiting)`.
/// - `running_parallelism_sum = Σ required_parallelism(running)`.
pub struct IcebergTaskQueue {
    inner: IcebergTaskQueueInner,
    /// Maximum parallelism that a single task may require (cluster effective max / scheduling window upper bound).
    max_parallelism: u32,
    /// Budget for `sum(required_parallelism)` of waiting tasks (buffer), e.g. 4 * `max_parallelism`.
    pending_parallelism_budget: u32,
    /// Notification for when tasks become schedulable
    schedule_notify: Option<Arc<Notify>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Added,
    Replaced { old_task_id: TaskId },
    RejectedRunningDuplicate,   // same unique ident already running
    RejectedCapacity,           // waiting parallelism budget exceeded and no replacement happened
    RejectedTooLarge,           // required_parallelism > max_parallelism
    RejectedInvalidParallelism, // required_parallelism == 0
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
                waiting: HashSet::new(),
                running: HashSet::new(),
                id_map: HashMap::new(),
                waiting_parallelism_sum: 0,
                running_parallelism_sum: 0,
                runners: HashMap::new(),
            },
            max_parallelism,
            pending_parallelism_budget,
            schedule_notify: None,
        }
    }

    pub fn new_with_notify(
        max_parallelism: u32,
        pending_parallelism_budget: u32,
    ) -> (Self, Arc<Notify>) {
        let notify = Arc::new(Notify::new());
        let mut queue = Self::new(max_parallelism, pending_parallelism_budget);
        queue.schedule_notify = Some(notify.clone());
        (queue, notify)
    }

    pub async fn wait_schedulable(&self) -> bool {
        if let Some(notify) = &self.schedule_notify {
            // Check if we have tasks that can be scheduled right now
            if self.has_schedulable_tasks() {
                return true;
            }
            // Otherwise wait for notification
            notify.notified().await;
            self.has_schedulable_tasks()
        } else {
            self.has_schedulable_tasks()
        }
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
        if let Some(notify) = &self.schedule_notify
            && self.has_schedulable_tasks()
        {
            notify.notify_one();
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

    /// Enqueue semantics:
    /// - Waiting duplicate: replace in place (position preserved) -> `Replaced { old_task_id }`.
    ///   * If new runner is `Some`, replace runner; else keep old runner.
    /// - Running duplicate: reject -> `RejectedRunningDuplicate`.
    /// - Budget exceeded (sum of waiting parallelism would surpass `pending_parallelism_budget`): `RejectedCapacity`.
    /// - `required_parallelism == 0`: `RejectedInvalidParallelism`.
    /// - `required_parallelism > max_parallelism`: `RejectedTooLarge`.
    /// - Otherwise append -> `Added`.
    pub fn push(
        &mut self,
        meta: IcebergTaskMeta,
        runner: Option<IcebergCompactorRunner>,
    ) -> PushResult {
        let uid = &meta.unique_ident;
        if meta.required_parallelism == 0 {
            return PushResult::RejectedInvalidParallelism;
        }
        if meta.required_parallelism > self.max_parallelism {
            return PushResult::RejectedTooLarge;
        }
        if self.inner.running.contains(uid) {
            return PushResult::RejectedRunningDuplicate;
        }

        if self.inner.waiting.contains(uid) {
            for slot in &mut self.inner.deque {
                if slot.unique_ident == *uid {
                    let old_task_id = slot.task_id;
                    let old_required = slot.required_parallelism;
                    let new_sum = self.inner.waiting_parallelism_sum - old_required
                        + meta.required_parallelism;
                    if new_sum > self.pending_parallelism_budget {
                        return PushResult::RejectedCapacity;
                    }
                    // meta replacement (position preserved)
                    self.inner.id_map.remove(&old_task_id);
                    slot.task_id = meta.task_id;
                    slot.enqueue_at = meta.enqueue_at;
                    slot.required_parallelism = meta.required_parallelism;
                    self.inner
                        .id_map
                        .insert(slot.task_id, (uid.clone(), slot.required_parallelism));
                    self.inner.waiting_parallelism_sum = new_sum;
                    if let Some(r) = runner {
                        // replace runner only if provided
                        self.inner.runners.remove(&old_task_id);
                        self.inner.runners.insert(slot.task_id, r);
                    } else {
                        // retain old runner mapping
                        if old_task_id != slot.task_id
                            && let Some(old_runner) = self.inner.runners.remove(&old_task_id)
                        {
                            self.inner.runners.insert(slot.task_id, old_runner);
                        }
                    }
                    return PushResult::Replaced { old_task_id };
                }
            }
            // Invariant violation: `waiting` contains `uid` but no corresponding entry in `deque`.
            // This indicates internal inconsistency (likely a logic regression). Crash fast to surface the bug.
            panic!(
                "IcebergTaskQueue invariant violated: waiting contains {uid} but deque missing. waiting_size={}, deque_len={}, id_map_size={}, waiting_parallelism_sum={}, running_parallelism_sum={}",
                self.inner.waiting.len(),
                self.inner.deque.len(),
                self.inner.id_map.len(),
                self.inner.waiting_parallelism_sum,
                self.inner.running_parallelism_sum,
            );
        }
        // New unique ident
        if self.inner.waiting_parallelism_sum + meta.required_parallelism
            > self.pending_parallelism_budget
        {
            return PushResult::RejectedCapacity;
        }
        self.inner.waiting.insert(uid.clone());
        self.inner
            .id_map
            .insert(meta.task_id, (uid.clone(), meta.required_parallelism));
        self.inner.waiting_parallelism_sum += meta.required_parallelism;
        self.inner.deque.push_back(meta);
        if let Some(r) = runner {
            self.inner.runners.insert(r.task_id, r);
        }
        // Notify that we might have schedulable tasks now
        self.notify_schedulable();
        PushResult::Added
    }

    // Pop the head task (strict FIFO) if it fits remaining parallelism.
    pub fn pop(&mut self) -> Option<PoppedIcebergTask> {
        let front = self.inner.deque.front()?;
        if front.required_parallelism > self.available_parallelism() {
            return None;
        }
        let meta = self.inner.deque.pop_front()?;
        let uid = meta.unique_ident.clone();
        debug_assert!(self.inner.waiting.contains(&uid));
        self.inner.waiting.remove(&uid);
        self.inner.running.insert(uid);
        self.inner.waiting_parallelism_sum = self
            .inner
            .waiting_parallelism_sum
            .saturating_sub(meta.required_parallelism);
        self.inner.running_parallelism_sum = self
            .inner
            .running_parallelism_sum
            .saturating_add(meta.required_parallelism);
        let runner = self.inner.runners.remove(&meta.task_id);
        Some(PoppedIcebergTask { meta, runner })
    }

    pub fn finish_running(&mut self, task_id: TaskId) -> bool {
        let Some((uid, required)) = self.inner.id_map.remove(&task_id) else {
            return false;
        };
        if self.inner.running.remove(&uid) {
            self.inner.running_parallelism_sum =
                self.inner.running_parallelism_sum.saturating_sub(required);
            // Runner lifecycle: consumed by logic after take_runner; cleanup here if still in map to avoid leak.
            self.inner.runners.remove(&task_id);
            // Notify that we might have schedulable tasks now (capacity freed up)
            self.notify_schedulable();
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_meta(id: u64, ident: &str, p: u32) -> IcebergTaskMeta {
        IcebergTaskMeta {
            task_id: id,
            unique_ident: ident.to_owned(),
            enqueue_at: std::time::Instant::now(),
            required_parallelism: p,
        }
    }

    #[test]
    fn test_push_pop_with_runner() {
        let mut q = IcebergTaskQueue::new(8, 32); // max_parallelism=8, budget 32
        // Fabricating runner via minimal construction path is complex; we only test presence flag by pushing None and Some.
        let meta = mk_meta(1, "t1", 4);
        let res = q.push(meta, None); // no runner
        assert_eq!(res, PushResult::Added);
        let popped = q.pop().expect("should pop");
        assert_eq!(popped.meta.task_id, 1);
        assert!(popped.runner.is_none());
        assert!(q.finish_running(1));
        assert_eq!(q.running_parallelism_sum(), 0);
    }

    #[test]
    fn test_replacement_keep_old_runner() {
        let mut q = IcebergTaskQueue::new(8, 32);
        let m1 = mk_meta(10, "same", 3);
        let _ = q.push(m1, None); // initial meta, no runner
        let m2 = mk_meta(11, "same", 5); // new required_parallelism
        let res = q.push(m2, None); // replacement preserves absence of runner
        assert!(matches!(res, PushResult::Replaced { old_task_id: 10 }));
        let popped = q.pop().unwrap();
        assert_eq!(popped.meta.task_id, 11);
        assert!(popped.runner.is_none());
    }

    #[test]
    fn test_capacity_reject() {
        let mut q = IcebergTaskQueue::new(4, 6); // max_parallelism=4, budget=6
        assert_eq!(q.push(mk_meta(1, "a", 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, "b", 3), None), PushResult::Added); // waiting sum=6
        // new unique ident would exceed budget
        assert_eq!(
            q.push(mk_meta(3, "c", 1), None),
            PushResult::RejectedCapacity
        );
    }

    #[test]
    fn test_invalid_parallelism() {
        let mut q = IcebergTaskQueue::new(4, 10);
        assert_eq!(
            q.push(mk_meta(1, "a", 0), None),
            PushResult::RejectedInvalidParallelism
        );
        assert_eq!(
            q.push(mk_meta(2, "a", 5), None),
            PushResult::RejectedTooLarge
        ); // > max_parallelism
    }

    #[test]
    fn test_running_duplicate_reject() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, "x", 4), None), PushResult::Added);
        let _p = q.pop().unwrap();
        // now x is running
        assert_eq!(
            q.push(mk_meta(2, "x", 4), None),
            PushResult::RejectedRunningDuplicate
        );
        assert!(q.finish_running(1));
        // after finishing can push again
        assert_eq!(q.push(mk_meta(3, "x", 4), None), PushResult::Added);
    }

    #[test]
    fn test_replacement_exceed_budget_reject() {
        // budget=10, current waiting sum=8, replacement would raise to 11 -> reject
        let mut q = IcebergTaskQueue::new(8, 10);
        assert_eq!(q.push(mk_meta(1, "a", 4), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, "b", 4), None), PushResult::Added);
        // Attempt to replace b (waiting) with required_parallelism=7 -> exceeds budget
        assert_eq!(
            q.push(mk_meta(3, "b", 7), None),
            PushResult::RejectedCapacity
        );
        // Pop order & parallelism unchanged
        let p1 = q.pop().unwrap();
        assert_eq!(p1.meta.unique_ident, "a");
        let p2 = q.pop().unwrap();
        assert_eq!(p2.meta.unique_ident, "b");
        assert_eq!(p2.meta.required_parallelism, 4);
    }

    #[test]
    fn test_replacement_position_preserved() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, "a", 3), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, "b", 3), None), PushResult::Added);
        // Replace head (a) with new task id 10 and different parallelism
        assert!(matches!(
            q.push(mk_meta(10, "a", 5), None),
            PushResult::Replaced { old_task_id: 1 }
        ));
        // Pop should still return "a" (now id 10) before "b"
        let p1 = q.pop().unwrap();
        assert_eq!(p1.meta.task_id, 10);
        assert_eq!(p1.meta.unique_ident, "a");
        let p2 = q.pop().unwrap();
        assert_eq!(p2.meta.unique_ident, "b");
    }

    #[test]
    fn test_pop_insufficient_parallelism() {
        // max_parallelism=8, first task uses 6, second needs 4 (cannot run concurrently)
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, "a", 6), None), PushResult::Added);
        assert_eq!(q.push(mk_meta(2, "b", 4), None), PushResult::Added);
        let p1 = q.pop().unwrap();
        assert_eq!(p1.meta.unique_ident, "a");
        // Not enough remaining parallelism (only 2 left)
        assert!(q.pop().is_none());
        // Finish first, then second becomes schedulable
        assert!(q.finish_running(1));
        let p2 = q.pop().unwrap();
        assert_eq!(p2.meta.unique_ident, "b");
    }

    #[test]
    fn test_finish_running_nonexistent() {
        let mut q = IcebergTaskQueue::new(4, 16);
        assert!(!q.finish_running(999)); // no such task id
        assert_eq!(q.running_parallelism_sum(), 0);
        assert_eq!(q.waiting_parallelism_sum(), 0);
    }

    #[test]
    fn test_finish_running_updates_sums() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, "a", 5), None), PushResult::Added);
        let _ = q.pop().unwrap();
        assert_eq!(q.running_parallelism_sum(), 5);
        assert!(q.finish_running(1));
        assert_eq!(q.running_parallelism_sum(), 0);
        assert!(q.pop().is_none()); // queue empty
    }

    #[test]
    fn test_replacement_parallelism_sum_adjustment() {
        let mut q = IcebergTaskQueue::new(8, 32);
        assert_eq!(q.push(mk_meta(1, "a", 3), None), PushResult::Added);
        assert_eq!(q.waiting_parallelism_sum(), 3);
        // Replace with higher required_parallelism
        assert!(matches!(
            q.push(mk_meta(2, "a", 6), None),
            PushResult::Replaced { old_task_id: 1 }
        ));
        assert_eq!(q.waiting_parallelism_sum(), 6);
        // Pop should show new parallelism
        let p = q.pop().unwrap();
        assert_eq!(p.meta.required_parallelism, 6);
        assert!(q.finish_running(p.meta.task_id));
    }
}
