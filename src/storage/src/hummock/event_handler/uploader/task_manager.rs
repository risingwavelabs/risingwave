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

use super::*;

#[derive(Debug)]
pub(super) enum UploadingTaskStatus {
    Spilling(HashSet<TableId>),
    Sync(SyncId),
}

#[derive(Debug)]
struct TaskEntry {
    task: UploadingTask,
    status: UploadingTaskStatus,
}

#[derive(Default, Debug)]
pub(super) struct TaskManager {
    tasks: HashMap<UploadingTaskId, TaskEntry>,
    // newer task at the front
    task_order: VecDeque<UploadingTaskId>,
    next_task_id: usize,
}

impl TaskManager {
    fn add_task(
        &mut self,
        task: UploadingTask,
        status: UploadingTaskStatus,
    ) -> &UploadingTaskStatus {
        let task_id = task.task_id;
        self.task_order.push_front(task.task_id);
        assert!(
            self.tasks
                .insert(task.task_id, TaskEntry { task, status })
                .is_none()
        );
        &self.tasks.get(&task_id).expect("should exist").status
    }

    fn poll_task(
        &mut self,
        cx: &mut Context<'_>,
        task_id: UploadingTaskId,
    ) -> Poll<Result<Arc<StagingSstableInfo>, (SyncId, HummockError)>> {
        let entry = self.tasks.get_mut(&task_id).expect("should exist");
        let result = match &entry.status {
            UploadingTaskStatus::Spilling(_) => {
                let sst = ready!(entry.task.poll_ok_with_retry(cx));
                Ok(sst)
            }
            UploadingTaskStatus::Sync(sync_id) => {
                let result = ready!(entry.task.poll_result(cx));
                result.map_err(|e| (*sync_id, e))
            }
        };
        Poll::Ready(result)
    }

    fn get_next_task_id(&mut self) -> UploadingTaskId {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        UploadingTaskId(task_id)
    }

    #[expect(clippy::type_complexity)]
    pub(super) fn poll_task_result(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        Option<(
            UploadingTaskId,
            UploadingTaskStatus,
            Result<Arc<StagingSstableInfo>, (SyncId, HummockError)>,
        )>,
    > {
        if let Some(task_id) = self.task_order.back() {
            let task_id = *task_id;
            let result = ready!(self.poll_task(cx, task_id));
            self.task_order.pop_back();
            let entry = self.tasks.remove(&task_id).expect("should exist");

            Poll::Ready(Some((task_id, entry.status, result)))
        } else {
            Poll::Ready(None)
        }
    }

    pub(super) fn abort_all_tasks(self) {
        for task in self.tasks.into_values() {
            task.task.join_handle.abort();
        }
    }

    pub(super) fn abort_task(&mut self, task_id: UploadingTaskId) -> Option<UploadingTaskStatus> {
        self.tasks.remove(&task_id).map(|entry| {
            entry.task.join_handle.abort();
            self.task_order
                .retain(|inflight_task_id| *inflight_task_id != task_id);
            entry.status
        })
    }

    pub(super) fn spill(
        &mut self,
        context: &UploaderContext,
        table_ids: HashSet<TableId>,
        imms: HashMap<LocalInstanceId, Vec<UploaderImm>>,
    ) -> (UploadingTaskId, usize, &HashSet<TableId>) {
        assert!(!imms.is_empty());
        let task = UploadingTask::new(self.get_next_task_id(), imms, context);
        context.stats.spill_task_counts_from_unsealed.inc();
        context
            .stats
            .spill_task_size_from_unsealed
            .inc_by(task.task_info.task_size as u64);
        info!("Spill data. Task: {}", task.get_task_info());
        let size = task.task_info.task_size;
        let id = task.task_id;
        let status = self.add_task(task, UploadingTaskStatus::Spilling(table_ids));
        (
            id,
            size,
            must_match!(status, UploadingTaskStatus::Spilling(table_ids) => table_ids),
        )
    }

    pub(super) fn sync(
        &mut self,
        context: &UploaderContext,
        sync_id: SyncId,
        unflushed_payload: UploadTaskInput,
        spill_task_ids: impl Iterator<Item = UploadingTaskId>,
        sync_table_ids: &HashSet<TableId>,
    ) -> Option<UploadingTaskId> {
        let task = if unflushed_payload.is_empty() {
            None
        } else {
            Some(UploadingTask::new(
                self.get_next_task_id(),
                unflushed_payload,
                context,
            ))
        };

        for task_id in spill_task_ids {
            let entry = self.tasks.get_mut(&task_id).expect("should exist");
            must_match!(&entry.status, UploadingTaskStatus::Spilling(table_ids) => {
                assert!(table_ids.is_subset(sync_table_ids), "spill table_ids: {table_ids:?}, sync_table_ids: {sync_table_ids:?}");
            });
            entry.status = UploadingTaskStatus::Sync(sync_id);
        }

        task.map(|task| {
            let id = task.task_id;
            self.add_task(task, UploadingTaskStatus::Sync(sync_id));
            id
        })
    }

    #[cfg(debug_assertions)]
    pub(super) fn tasks(&self) -> impl Iterator<Item = (UploadingTaskId, &UploadingTaskStatus)> {
        self.tasks
            .iter()
            .map(|(task_id, entry)| (*task_id, &entry.status))
    }
}
