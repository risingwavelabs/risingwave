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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use fail::fail_point;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compact::estimate_state_for_compaction;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockContextId};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CancelCompactTask, CompactTask, CompactTaskAssignment, CompactTaskProgress,
    SubscribeCompactTasksResponse,
};
use tokio::sync::mpsc::{Receiver, Sender};

use super::compaction_scheduler::ScheduleStatus;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type CompactorManagerRef = Arc<CompactorManager>;

pub const TASK_RUN_TOO_LONG: &str = "running too long";
pub const TASK_NOT_FOUND: &str = "task not found";
pub const TASK_NORMAL: &str = "task is normal, please wait some time";

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
#[derive(Debug)]
pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
    // state
    pub cpu_ratio: AtomicU32,
    pub total_cpu_core: u32,
}

struct TaskHeartbeat {
    task: CompactTask,
    num_ssts_sealed: u32,
    num_ssts_uploaded: u32,
    num_progress_key: u64,
    num_pending_read_io: u64,
    num_pending_write_io: u64,
    create_time: Instant,
    expire_at: u64,
}

impl Compactor {
    pub fn new(
        context_id: HummockContextId,
        sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
        cpu_core_num: u32,
    ) -> Self {
        Self {
            context_id,
            sender,
            cpu_ratio: AtomicU32::new(0),
            total_cpu_core: cpu_core_num,
        }
    }

    pub async fn send_task(&self, task: Task) -> MetaResult<()> {
        fail_point!("compaction_send_task_fail", |_| Err(anyhow::anyhow!(
            "compaction_send_task_fail"
        )
        .into()));

        self.sender
            .send(Ok(SubscribeCompactTasksResponse { task: Some(task) }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }

    pub async fn cancel_task(&self, task_id: u64) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                task: Some(Task::CancelCompactTask(CancelCompactTask {
                    context_id: self.context_id,
                    task_id,
                })),
            }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn is_busy(&self, limit: u32) -> bool {
        self.cpu_ratio.load(Ordering::Acquire) > limit
    }
}

/// `CompactorManagerInner` maintains compactors which can process compact task.
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
/// Furthermore, the compactor for a compaction task must be picked with `CompactorManagerInner`,
/// or its internal states might not be correctly maintained.
pub struct CompactorManagerInner {
    // policy: RwLock<Box<dyn CompactionSchedulePolicy>>,
    pub task_expiry_seconds: u64,
    // A map: { context_id -> { task_id -> heartbeat } }
    // task_heartbeats:
    //     RwLock<HashMap<HummockContextId, HashMap<HummockCompactionTaskId, TaskHeartbeat>>>,
    task_heartbeats: HashMap<HummockCompactionTaskId, TaskHeartbeat>,

    pub compactor_pending_io_counts: HashMap<HummockContextId, u32>,

    /// The context ids of compactors.
    pub compactors: Vec<HummockContextId>,

    /// TODO: Let each compactor have its own Mutex, we should not need to lock whole thing.
    /// The outer lock is a RwLock, so we should still be able to modify each compactor
    pub compactor_map: HashMap<HummockContextId, Arc<Compactor>>,
}

impl CompactorManagerInner {
    pub async fn with_meta<S: MetaStore>(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        // Retrieve the existing task assignments from metastore.
        let task_assignment = CompactTaskAssignment::list(env.meta_store()).await?;
        let mut manager = Self {
            task_expiry_seconds: env.opts.compaction_task_max_heartbeat_interval_secs,
            task_heartbeats: Default::default(),
            compactor_pending_io_counts: Default::default(),
            compactors: Default::default(),
            compactor_map: Default::default(),
        };
        // Initialize heartbeat for existing tasks.
        task_assignment.into_iter().for_each(|assignment| {
            manager.initiate_task_heartbeat(assignment.compact_task.unwrap());
        });
        Ok(manager)
    }

    /// Only used for unit test.
    pub fn for_test() -> Self {
        Self {
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
            compactor_pending_io_counts: Default::default(),
            compactors: Default::default(),
            compactor_map: Default::default(),
        }
    }

    pub fn next_idle_compactor(&self) -> Option<(Arc<Compactor>, u32)> {
        if self.compactors.is_empty() || self.compactor_pending_io_counts.is_empty() {
            return None;
        }

        use rand::Rng;
        let rand_index = rand::thread_rng().gen_range(0..self.compactors.len());

        for context_id in self.compactors[rand_index..]
            .iter()
            .chain(self.compactors[..rand_index].iter())
        {
            if let Some(pending_pull_task_count) = self.compactor_pending_io_counts.get(context_id)
            {
                // Avoid picking compactors that have not yet executed heartbeat
                let compactor = self.compactor_map.get(context_id).unwrap().clone();
                return Some((compactor, *pending_pull_task_count));
            }
        }

        None
    }

    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        use rand::Rng;

        if self.compactors.is_empty() {
            return None;
        }

        if self.compactor_pending_io_counts.is_empty() {
            return None;
        }

        let rand_index = rand::thread_rng().gen_range(0..self.compactors.len());
        let context_id = self.compactors[rand_index];

        let compactor = self.compactor_map.get(&context_id).unwrap().clone();

        Some(compactor)
    }

    /// Retrieve a receiver of tasks for the compactor identified by `context_id`. The sender should
    /// be obtained by calling one of the compactor getters.
    ///
    ///  If `add_compactor` is called with the same `context_id` more than once, the only cause
    /// would be compactor re-subscription, as `context_id` is a monotonically increasing
    /// sequence.
    pub fn add_compactor(
        &mut self,
        context_id: HummockContextId,
        cpu_core_num: u32,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        const STREAM_BUFFER_SIZE: usize = 4;
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.compactors.retain(|c| *c != context_id);
        self.compactors.push(context_id);
        self.compactor_map.insert(
            context_id,
            Arc::new(Compactor::new(context_id, tx, cpu_core_num)),
        );

        tracing::info!(
            "Added compactor session {} cpu_core_num {}",
            context_id,
            cpu_core_num
        );
        rx
    }

    /// Used when meta exiting to support graceful shutdown.
    pub fn abort_all_compactors(&mut self) {
        while let Some(compactor) = self.next_compactor() {
            self.remove_compactor(compactor.context_id);
        }
    }

    pub fn pause_compactor(&mut self, context_id: HummockContextId) {
        self.remove_compactor(context_id);
        tracing::info!("Paused compactor session {}", context_id);
    }

    pub fn remove_compactor(&mut self, context_id: HummockContextId) {
        self.compactors.retain(|c| *c != context_id);
        self.compactor_map.remove(&context_id);
        self.compactor_pending_io_counts.remove(&context_id);

        // To remove the heartbeats, they need to be forcefully purged,
        // which is only safe when the context has been completely removed from meta.
        tracing::info!("Removed compactor session {}", context_id);
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.compactor_map.get(&context_id).cloned()
    }

    pub fn check_tasks_status(
        &self,
        tasks: &[HummockCompactionTaskId],
        slow_task_duration: Duration,
    ) -> HashMap<HummockCompactionTaskId, (Duration, &'static str)> {
        let tasks_ids: HashSet<u64> = HashSet::from_iter(tasks.to_vec());
        let mut ret = HashMap::default();
        for TaskHeartbeat {
            task, create_time, ..
        } in self.task_heartbeats.values()
        {
            if !tasks_ids.contains(&task.task_id) {
                continue;
            }
            let pending_time = create_time.elapsed();
            if pending_time > slow_task_duration {
                ret.insert(task.task_id, (pending_time, TASK_RUN_TOO_LONG));
            } else {
                ret.insert(task.task_id, (pending_time, TASK_NORMAL));
            }
        }

        for task_id in tasks {
            if !ret.contains_key(task_id) {
                ret.insert(*task_id, (Duration::from_secs(0), TASK_NOT_FOUND));
            }
        }
        ret
    }

    pub fn get_expired_tasks(&self, interval_sec: Option<u64>) -> Vec<CompactTask> {
        let interval = interval_sec.unwrap_or(0);
        let now: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs()
            + interval;
        Self::get_heartbeat_expired_tasks(&self.task_heartbeats, now)
    }

    fn get_heartbeat_expired_tasks(
        task_heartbeats: &HashMap<HummockCompactionTaskId, TaskHeartbeat>,
        now: u64,
    ) -> Vec<CompactTask> {
        let mut cancellable_tasks = vec![];
        const MAX_TASK_DURATION_SEC: u64 = 2700;

        for TaskHeartbeat {
            expire_at,
            task,
            create_time,
            num_ssts_sealed,
            num_ssts_uploaded,
            num_progress_key,
            num_pending_read_io,
            num_pending_write_io,
        } in task_heartbeats.values()
        {
            let task_duration_too_long = create_time.elapsed().as_secs() > MAX_TASK_DURATION_SEC;
            if *expire_at < now || task_duration_too_long {
                // 1. task heartbeat expire
                // 2. task duration is too long
                cancellable_tasks.push(task.clone());

                if task_duration_too_long {
                    let (need_quota, total_file_count, total_key_count) =
                        estimate_state_for_compaction(task);
                    tracing::info!(
                                "CompactionGroupId {} Task {} duration too long create_time {:?} num_ssts_sealed {} num_ssts_uploaded {} num_progress_key {} \
                                pending_read_io_count {} pending_write_io_count {} need_quota {} total_file_count {} total_key_count {} target_level {} \
                                base_level {} target_sub_level_id {} task_type {}",
                                task.compaction_group_id,
                                task.task_id,
                                create_time,
                                num_ssts_sealed,
                                num_ssts_uploaded,
                                num_progress_key,
                                num_pending_read_io,
                                num_pending_write_io,
                                need_quota,
                                total_file_count,
                                total_key_count,
                                task.target_level,
                                task.base_level,
                                task.target_sub_level_id,
                                task.task_type,
                            );
                }
            }
        }
        cancellable_tasks
    }

    pub fn initiate_task_heartbeat(&mut self, task: CompactTask) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        self.task_heartbeats.insert(
            task.task_id,
            TaskHeartbeat {
                task,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                num_progress_key: 0,
                num_pending_read_io: 0,
                num_pending_write_io: 0,
                create_time: Instant::now(),
                expire_at: now + self.task_expiry_seconds,
            },
        );
    }

    pub fn remove_task_heartbeat(&mut self, task_id: u64) {
        self.task_heartbeats.remove(&task_id).unwrap();
    }

    pub fn update_task_heartbeats(
        &mut self,
        progress_list: &Vec<CompactTaskProgress>,
    ) -> Vec<CompactTask> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut cancel_tasks = vec![];
        for progress in progress_list {
            if let Some(task_ref) = self.task_heartbeats.get_mut(&progress.task_id) {
                if task_ref.num_ssts_sealed < progress.num_ssts_sealed
                    || task_ref.num_ssts_uploaded < progress.num_ssts_uploaded
                    || task_ref.num_progress_key < progress.num_progress_key
                {
                    // Refresh the expiry of the task as it is showing progress.
                    task_ref.expire_at = now + self.task_expiry_seconds;
                    task_ref.num_ssts_sealed = progress.num_ssts_sealed;
                    task_ref.num_ssts_uploaded = progress.num_ssts_uploaded;
                    task_ref.num_progress_key = progress.num_progress_key;
                }
                task_ref.num_pending_read_io = progress.num_pending_read_io;
                task_ref.num_pending_write_io = progress.num_pending_write_io;

                // timeout check
                if task_ref.expire_at < now {
                    // cancel
                    cancel_tasks.push(task_ref.task.clone())
                }
            }
        }

        cancel_tasks
    }

    pub fn compactor_num(&self) -> usize {
        self.compactors.len()
    }

    pub fn update_compactor_pending_task(
        &mut self,
        context_id: HummockContextId,
        pull_task_count: Option<u32>,
        check_exist: bool,
    ) {
        use std::collections::hash_map::Entry::{Occupied, Vacant};
        // update_compactor_pending_task is called by two paths
        // 1. compactor heartbeat will pass in pull_task_count, which is not processed when
        // pull_task_count is None, and inserted into memory when it is not zero
        // 2. `CompactorPullTaskHandle` returns the count when it is
        // resolved, and we need to clean up the corresponding item when the count is 0
        //
        // The above restriction ensures that the selected `compactor_pending_io_count` must not be
        // 0. This restriction is also reflected in the `CompactorPullTaskHandle`.
        if let Some(pull_task_count) = pull_task_count {
            if pull_task_count == 0 {
                let _ = self.compactor_pending_io_counts.remove(&context_id);
            } else {
                match self.compactor_pending_io_counts.entry(context_id) {
                    Occupied(mut value) => *value.get_mut() = pull_task_count,
                    Vacant(entry) => {
                        if !check_exist {
                            // Avoid `CompactorPullTaskHandle` incorrectly updating the state of a
                            // deprecated compactor
                            entry.insert(pull_task_count);
                        }
                    }
                }
            }
        }
    }

    pub fn get_progress(&self) -> Vec<CompactTaskProgress> {
        self.task_heartbeats
            .values()
            // .flat_map(|m| m.values())
            .map(|hb| CompactTaskProgress {
                task_id: hb.task.task_id,
                num_ssts_sealed: hb.num_ssts_sealed,
                num_ssts_uploaded: hb.num_ssts_uploaded,
                num_progress_key: hb.num_progress_key,
                num_pending_read_io: hb.num_pending_read_io,
                num_pending_write_io: hb.num_pending_write_io,
            })
            .collect()
    }
}

pub struct CompactorManager {
    inner: Arc<RwLock<CompactorManagerInner>>,
}

impl CompactorManager {
    pub async fn with_meta<S: MetaStore>(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let inner = CompactorManagerInner::with_meta(env).await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    /// Only used for unit test.
    pub fn for_test() -> Self {
        let inner = CompactorManagerInner::for_test();
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn next_idle_compactor(&self) -> Option<CompactorPullTaskHandle> {
        if let Some((compactor, pending_pull_task_count)) = self.inner.read().next_idle_compactor()
        {
            assert!(pending_pull_task_count > 0);
            // `update_compactor_pending_task` ensures that the compactor that can be
            // selected must exist and that the value of pending_pull_task_count is not 0.
            Some(CompactorPullTaskHandle {
                compactor_manager: self.inner.clone(),
                compactor,
                pending_pull_task_count,
            })
        } else {
            None
        }
    }

    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        self.inner.read().next_compactor()
    }

    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
        cpu_core_num: u32,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        self.inner.write().add_compactor(context_id, cpu_core_num)
    }

    pub fn abort_all_compactors(&self) {
        self.inner.write().abort_all_compactors();
    }

    pub fn pause_compactor(&self, context_id: HummockContextId) {
        self.inner.write().pause_compactor(context_id)
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        self.inner.write().remove_compactor(context_id)
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.inner.read().get_compactor(context_id)
    }

    pub fn check_tasks_status(
        &self,
        tasks: &[HummockCompactionTaskId],
        slow_task_duration: Duration,
    ) -> HashMap<HummockCompactionTaskId, (Duration, &'static str)> {
        self.inner
            .read()
            .check_tasks_status(tasks, slow_task_duration)
    }

    pub fn get_expired_tasks(&self, interval_sec: Option<u64>) -> Vec<CompactTask> {
        self.inner.read().get_expired_tasks(interval_sec)
    }

    pub fn initiate_task_heartbeat(&self, task: CompactTask) {
        self.inner.write().initiate_task_heartbeat(task);
    }

    pub fn remove_task_heartbeat(&self, task_id: u64) {
        self.inner.write().remove_task_heartbeat(task_id);
    }

    pub fn update_task_heartbeats(
        &self,
        progress_list: &Vec<CompactTaskProgress>,
    ) -> Vec<CompactTask> {
        self.inner.write().update_task_heartbeats(progress_list)
    }

    pub fn compactor_num(&self) -> usize {
        self.inner.read().compactor_num()
    }

    pub fn update_compactor_pending_task(
        &self,
        context_id: HummockContextId,
        pull_task_count: Option<u32>,
        check_exist: bool,
    ) {
        self.inner
            .write()
            .update_compactor_pending_task(context_id, pull_task_count, check_exist)
    }

    pub fn get_progress(&self) -> Vec<CompactTaskProgress> {
        self.inner.read().get_progress()
    }
}

pub struct CompactorPullTaskHandle {
    compactor_manager: Arc<RwLock<CompactorManagerInner>>,

    pub compactor: Arc<Compactor>,

    pub pending_pull_task_count: u32,
}

impl CompactorPullTaskHandle {
    pub async fn consume_task(&mut self, compact_task: &CompactTask) -> ScheduleStatus {
        // By design, it is guaranteed that no `CompactorPullTaskHandle` with
        // `pending_pull_task_count` < 1 will be constructed and that the
        // `CompactorPullTaskHandle` will not be over-consumed on the usage side. This is a
        // defensive logic, and when the `CompactorPullTaskHandle` is misused, we return an Error
        // and rely on expire to recycle the task
        if self.pending_pull_task_count < 1 {
            tracing::warn!(
                "WARN: CompactorPullTaskHandle is used incorrectly context_id {}",
                self.compactor.context_id
            );
            return ScheduleStatus::SendFailure(Box::new(compact_task.clone()));
        }

        // 2. Send the compaction task.
        if let Err(e) = self
            .compactor
            .send_task(Task::CompactTask(compact_task.clone()))
            .await
        {
            tracing::warn!(
                "Failed to send task {} to {}. {:#?}",
                compact_task.task_id,
                self.compactor.context_id(),
                e
            );

            // try cancel on compactor
            let _ = self.compactor.cancel_task(compact_task.task_id).await;

            self.compactor_manager
                .write()
                .pause_compactor(self.compactor.context_id());

            self.pending_pull_task_count = 0;
            return ScheduleStatus::SendFailure(Box::new(compact_task.clone()));
        }

        self.pending_pull_task_count -= 1;
        ScheduleStatus::Ok
    }
}

impl Drop for CompactorPullTaskHandle {
    fn drop(&mut self) {
        // When destructing the `CompactorPullTaskHandle`, we need to reclaim count. and remove the
        // item when count is 0
        self.compactor_manager
            .write()
            .update_compactor_pending_task(
                self.compactor.context_id,
                Some(self.pending_pull_task_count),
                true, // check exist
            );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::compaction::default_level_selector;
    use crate::hummock::test_utils::{
        add_ssts, register_table_ids_to_compaction_group, setup_compute_env,
    };
    use crate::hummock::CompactorManager;

    #[tokio::test]
    async fn test_compactor_manager() {
        // Initialize metastore with task assignment.
        let (env, context_id) = {
            let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
            let context_id = worker_node.id;
            let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
            register_table_ids_to_compaction_group(
                hummock_manager.as_ref(),
                &[1],
                StaticCompactionGroupId::StateDefault.into(),
            )
            .await;
            let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
            let _receiver = compactor_manager.add_compactor(context_id, 1);
            hummock_manager
                .get_compact_task(
                    StaticCompactionGroupId::StateDefault.into(),
                    &mut default_level_selector(),
                )
                .await
                .unwrap()
                .unwrap();
            (env, context_id)
        };

        // Restart. Set task_expiry_seconds to 0 only to speed up test.
        let compactor_manager = CompactorManager::with_meta(env).await.unwrap();
        // Because task assignment exists.
        // assert_eq!(compactor_manager.task_heartbeats.len(), 1);
        // Because compactor gRPC is not established yet.
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());

        // Ensure task is expired.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let expired = compactor_manager.get_expired_tasks(None);
        assert_eq!(expired.len(), 1);

        // Mimic no-op compaction heartbeat
        compactor_manager.update_task_heartbeats(&vec![CompactTaskProgress {
            task_id: expired[0].task_id,
            ..Default::default()
        }]);
        assert_eq!(compactor_manager.get_expired_tasks(None).len(), 1);

        // Mimic compaction heartbeat with invalid task id
        compactor_manager.update_task_heartbeats(&vec![CompactTaskProgress {
            task_id: expired[0].task_id + 1,
            num_ssts_sealed: 1,
            num_ssts_uploaded: 1,
            num_progress_key: 100,
            ..Default::default()
        }]);
        assert_eq!(compactor_manager.get_expired_tasks(None).len(), 1);

        // Mimic effective compaction heartbeat
        compactor_manager.update_task_heartbeats(&vec![CompactTaskProgress {
            task_id: expired[0].task_id,
            num_ssts_sealed: 1,
            num_ssts_uploaded: 1,
            num_progress_key: 100,
            ..Default::default()
        }]);
        assert_eq!(compactor_manager.get_expired_tasks(None).len(), 0);

        // Test add
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
        compactor_manager.add_compactor(context_id, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);
        assert_eq!(
            compactor_manager
                .get_compactor(context_id)
                .unwrap()
                .context_id(),
            context_id
        );
        // Test pause
        compactor_manager.pause_compactor(context_id);
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
        for _ in 0..3 {
            compactor_manager.add_compactor(context_id, 1);
            assert_eq!(compactor_manager.compactor_num(), 1);
            assert_eq!(
                compactor_manager
                    .get_compactor(context_id)
                    .unwrap()
                    .context_id(),
                context_id
            );
        }
        // Test remove
        compactor_manager.remove_compactor(context_id);
        assert_eq!(compactor_manager.compactor_num(), 0);
        assert!(compactor_manager.get_compactor(context_id).is_none());
    }
}
