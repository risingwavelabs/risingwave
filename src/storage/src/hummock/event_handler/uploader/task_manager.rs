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
    Spilling,
    Sync(HummockEpoch),
}

#[derive(Debug)]
enum TaskStatus {
    Uploading(UploadingTaskStatus),
    Spilled(Arc<StagingSstableInfo>),
}

#[derive(Default, Debug)]
pub(super) struct TaskManager {
    // newer task at the front
    uploading_tasks: VecDeque<UploadingTask>,
    task_status: HashMap<UploadingTaskId, TaskStatus>,
}

impl TaskManager {
    fn add_task(&mut self, task: UploadingTask, status: UploadingTaskStatus) {
        self.task_status
            .insert(task.task_id, TaskStatus::Uploading(status));
        self.uploading_tasks.push_front(task);
    }

    #[expect(clippy::type_complexity)]
    pub(super) fn poll_task_result(
        &mut self,
        cx: &mut Context<'_>,
        _context: &UploaderContext,
    ) -> Poll<
        Option<
            Result<
                (
                    UploadingTaskId,
                    UploadingTaskStatus,
                    Arc<StagingSstableInfo>,
                ),
                ErrState,
            >,
        >,
    > {
        if let Some(task) = self.uploading_tasks.back_mut() {
            let result = match self.task_status.get(&task.task_id).expect("should exist") {
                TaskStatus::Uploading(UploadingTaskStatus::Spilling) => {
                    let sst = ready!(task.poll_ok_with_retry(cx));
                    self.task_status
                        .insert(task.task_id, TaskStatus::Spilled(sst.clone()));
                    Ok((task.task_id, UploadingTaskStatus::Spilling, sst))
                }
                TaskStatus::Uploading(UploadingTaskStatus::Sync(epoch)) => {
                    let epoch = *epoch;
                    let result = ready!(task.poll_result(cx));
                    let _status = self.task_status.remove(&task.task_id);
                    result
                        .map(|sst| (task.task_id, UploadingTaskStatus::Sync(epoch), sst))
                        .map_err(|e| ErrState {
                            failed_epoch: epoch,
                            reason: e.as_report().to_string(),
                        })
                }
                TaskStatus::Spilled(_) => {
                    unreachable!("should be uploading task")
                }
            };

            let _task = self.uploading_tasks.pop_back().expect("non-empty");
            Poll::Ready(Some(result))
        } else {
            Poll::Ready(None)
        }
    }

    pub(super) fn abort(self) {
        for task in self.uploading_tasks {
            task.join_handle.abort();
        }
    }

    pub(super) fn spill(
        &mut self,
        context: &UploaderContext,
        imms: HashMap<LocalInstanceId, Vec<UploaderImm>>,
    ) -> (UploadingTaskId, usize) {
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
        self.add_task(task, UploadingTaskStatus::Spilling);
        (id, size)
    }

    pub(super) fn sync(
        &mut self,
        context: &UploaderContext,
        epoch: HummockEpoch,
        spilled_task: BTreeSet<UploadingTaskId>,
        unflushed_payload: UploadTaskInput,
    ) -> (HashSet<UploadingTaskId>, VecDeque<Arc<StagingSstableInfo>>) {
        let mut remaining_tasks = HashSet::new();
        let total_task_count = if unflushed_payload.is_empty() {
            spilled_task.len()
        } else {
            let task = UploadingTask::new(unflushed_payload, context);
            remaining_tasks.insert(task.task_id);
            self.task_status.insert(
                task.task_id,
                TaskStatus::Uploading(UploadingTaskStatus::Sync(epoch)),
            );
            self.add_task(task, UploadingTaskStatus::Sync(epoch));
            spilled_task.len() + 1
        };
        let mut uploaded = VecDeque::with_capacity(total_task_count);

        // iterate from small task id to large, which means from old data to new data
        for task_id in spilled_task {
            let status = self.task_status.remove(&task_id).expect("should exist");
            match status {
                TaskStatus::Uploading(UploadingTaskStatus::Spilling) => {
                    self.task_status.insert(
                        task_id,
                        TaskStatus::Uploading(UploadingTaskStatus::Sync(epoch)),
                    );
                    remaining_tasks.insert(task_id);
                }
                TaskStatus::Uploading(UploadingTaskStatus::Sync(_)) => {
                    unreachable!("cannot be synced again")
                }
                TaskStatus::Spilled(sst) => {
                    self.task_status.remove(&task_id);
                    uploaded.push_front(sst);
                }
            }
        }
        (remaining_tasks, uploaded)
    }
}
