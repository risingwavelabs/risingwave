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

type TaskId = u64;
type TaskUniqueIdent = String; // format!("{}-{:?}", catalog_name, table_ident)

/// Metadata of a task waiting (or running) – independent from the actual runner implementation.
/// Runner (payload) is stored separately so tests can omit it.
#[derive(Debug, Clone)]
pub struct IcebergTaskMeta {
    pub task_id: TaskId,
    pub unique_ident: TaskUniqueIdent,
    pub enqueue_at: std::time::Instant,
    /// Estimated parallelism required to execute this task (must be >0 and <= max_parallelism)
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

/// Basic queue abstraction supporting:
/// 1. Enqueue (push) with replacement if a waiting task with same unique ident exists.
/// 2. Dequeue (pop) -> transitions the task to running state.
/// 3. Duplicate detection: if same unique ident already running, new task is rejected.
/// 4. Cancel/remove only affects waiting tasks (not running).
///
/// NOTE: This MVP does not implement capacity eviction policy beyond a hard full rejection.
pub struct IcebergTaskQueue {
    inner: IcebergTaskQueueInner,
    /// Maximum parallelism that a single task may require (cluster effective max / scheduling window upper bound).
    max_parallelism: u32,
    /// Budget for sum(required_parallelism) of waiting tasks (buffer), e.g. 4 * max_parallelism.
    pending_parallelism_budget: u32,
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

    /// Enqueue a task. Behavior:
    /// - If a waiting task with same unique ident exists: replace it (position unchanged), return Replaced.
    /// - If a running task with same unique ident exists: reject (duplicate running).
    /// - Else if queue full: reject.
    /// - Else: push to back.
    /// Push a task meta plus optional runner.
    /// Replacement semantics:
    ///   - If same unique_ident waiting and new runner Some => replace meta + runner.
    ///   - If same unique_ident waiting and new runner None => replace only meta (keep old runner) -> ReplacedMetaOnly.
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
            for slot in self.inner.deque.iter_mut() {
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
                        if old_task_id != slot.task_id {
                            if let Some(old_runner) = self.inner.runners.remove(&old_task_id) {
                                self.inner.runners.insert(slot.task_id, old_runner);
                            }
                        }
                    }
                    return PushResult::Replaced { old_task_id };
                }
            }
            tracing::warn!(unique_ident = %uid, "Inconsistent state: waiting set has ident but not found in deque");
            return PushResult::RejectedCapacity;
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
        PushResult::Added
    }

    /// Pop the head task (strict FIFO) if it fits remaining parallelism.
    /// 内部自行维护 running_parallelism_sum, 不需要调用方传参。
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

    /// Finish a running task, freeing its slot so new tasks with same unique ident can be enqueued.
    pub fn finish_running(&mut self, task_id: TaskId) -> bool {
        let Some((uid, required)) = self.inner.id_map.remove(&task_id) else {
            return false;
        };
        if self.inner.running.remove(&uid) {
            self.inner.running_parallelism_sum =
                self.inner.running_parallelism_sum.saturating_sub(required);
            // runner 生命周期：由 take_runner 后的执行逻辑消费；若还留在 map 中这里清理避免泄漏。
            self.inner.runners.remove(&task_id);
            true
        } else {
            false
        }
    }
}

// Guidance 更新 (简化后):
// 1. push 校验 0 < p <= max_parallelism 且等待并行度预算；重复 waiting 直接原位替换 (可选择性更新 runner)。
// 2. pop 返回 meta + Option<runner>，不需要额外 take_runner。
// 3. available = max_parallelism - running_parallelism_sum；严格 FIFO，不跳过 head。
// 4. head-of-line blocking 暂不优化，后续可加 lookahead。

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_meta(id: u64, ident: &str, p: u32) -> IcebergTaskMeta {
        IcebergTaskMeta {
            task_id: id,
            unique_ident: ident.to_string(),
            enqueue_at: std::time::Instant::now(),
            required_parallelism: p,
        }
    }

    #[test]
    fn test_push_pop_with_runner() {
        let mut q = IcebergTaskQueue::new(8, 32); // max_parallelism=8, budget 32
        // fabricate runner via minimal construction path is complex; we only test presence flag by pushing None and Some.
        let meta = mk_meta(1, "t1", 4);
        let res = q.push(meta.clone(), None); // no runner
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
}
