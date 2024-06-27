// Copyright 2024 RisingWave Labs
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
    Sync(HummockEpoch),
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
}

impl TaskManager {
    fn add_task(
        &mut self,
        task: UploadingTask,
        status: UploadingTaskStatus,
    ) -> &UploadingTaskStatus {
        let task_id = task.task_id;
        self.task_order.push_front(task.task_id);
        self.tasks.insert(task.task_id, TaskEntry { task, status });
        &self.tasks.get(&task_id).expect("should exist").status
    }

    fn poll_task(
        &mut self,
        cx: &mut Context<'_>,
        task_id: UploadingTaskId,
    ) -> Poll<Result<Arc<StagingSstableInfo>, ErrState>> {
        let entry = self.tasks.get_mut(&task_id).expect("should exist");
        let result = match &entry.status {
            UploadingTaskStatus::Spilling(_) => {
                let sst = ready!(entry.task.poll_ok_with_retry(cx));
                Ok(sst)
            }
            UploadingTaskStatus::Sync(epoch) => {
                let epoch = *epoch;
                let result = ready!(entry.task.poll_result(cx));
                result.map_err(|e| ErrState {
                    failed_epoch: epoch,
                    reason: e.as_report().to_string(),
                })
            }
        };
        Poll::Ready(result)
    }

    #[expect(clippy::type_complexity)]
    pub(super) fn poll_task_result(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        Option<(
            UploadingTaskId,
            UploadingTaskStatus,
            Result<Arc<StagingSstableInfo>, ErrState>,
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

    pub(super) fn abort(self) {
        for task in self.tasks.into_values() {
            task.task.join_handle.abort();
        }
    }

    pub(super) fn spill(
        &mut self,
        context: &UploaderContext,
        table_ids: HashSet<TableId>,
        imms: HashMap<LocalInstanceId, Vec<UploaderImm>>,
    ) -> (UploadingTaskId, usize, &HashSet<TableId>) {
        assert!(!imms.is_empty());
        let task = UploadingTask::new(imms, context);
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

    pub(super) fn remove_table_spill_tasks(
        &mut self,
        table_id: TableId,
        task_ids: impl Iterator<Item = UploadingTaskId>,
    ) {
        for task_id in task_ids {
            let entry = self.tasks.get_mut(&task_id).expect("should exist");
            let empty = must_match!(&mut entry.status, UploadingTaskStatus::Spilling(table_ids) => {
                assert!(table_ids.remove(&table_id));
                table_ids.is_empty()
            });
            if empty {
                let task = self.tasks.remove(&task_id).expect("should exist").task;
                task.join_handle.abort();
            }
        }
    }

    pub(super) fn sync(
        &mut self,
        context: &UploaderContext,
        epoch: HummockEpoch,
        unflushed_payload: UploadTaskInput,
        spill_task_ids: impl Iterator<Item = UploadingTaskId>,
        sync_table_ids: &HashSet<TableId>,
    ) -> Option<UploadingTaskId> {
        let task = if unflushed_payload.is_empty() {
            None
        } else {
            Some(UploadingTask::new(unflushed_payload, context))
        };

        for task_id in spill_task_ids {
            let entry = self.tasks.get_mut(&task_id).expect("should exist");
            must_match!(&entry.status, UploadingTaskStatus::Spilling(table_ids) => {
                assert!(table_ids.is_subset(sync_table_ids), "spill table_ids: {table_ids:?}, sync_table_ids: {sync_table_ids:?}");
            });
            entry.status = UploadingTaskStatus::Sync(epoch);
        }

        task.map(|task| {
            let id = task.task_id;
            self.add_task(task, UploadingTaskStatus::Sync(epoch));
            id
        })
    }

    #[cfg(debug_assertions)]
    pub(super) fn spilling_task(
        &self,
    ) -> impl Iterator<Item = (UploadingTaskId, &HashSet<TableId>)> {
        self.tasks
            .iter()
            .filter_map(|(task_id, entry)| match &entry.status {
                UploadingTaskStatus::Spilling(table_ids) => Some((*task_id, table_ids)),
                UploadingTaskStatus::Sync(_) => None,
            })
    }
}
