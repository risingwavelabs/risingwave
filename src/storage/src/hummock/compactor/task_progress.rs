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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_hummock_sdk::HummockCompactionTaskId;

pub type TaskProgressManagerRef = Arc<Mutex<HashMap<HummockCompactionTaskId, Arc<TaskProgress>>>>;

/// The progress of a compaction task.
#[derive(Default)]
pub struct TaskProgress {
    pub num_ssts_sealed: AtomicU32,
    pub num_ssts_uploaded: AtomicU32,
}

impl TaskProgress {
    pub fn inc_ssts_sealed(&self) {
        self.num_ssts_sealed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ssts_uploaded(&self) {
        self.num_ssts_uploaded.fetch_add(1, Ordering::Relaxed);
    }
}

/// An RAII object that contains a [`TaskProgress`] and shares it to all the splits of the task.
#[derive(Default)]
pub struct TaskProgressGuard {
    task_id: HummockCompactionTaskId,
    progress_manager: TaskProgressManagerRef,

    pub progress: Arc<TaskProgress>,
}

impl TaskProgressGuard {
    pub fn new(task_id: HummockCompactionTaskId, progress_manager: TaskProgressManagerRef) -> Self {
        let progress = progress_manager.lock().entry(task_id).or_default().clone();
        Self {
            task_id,
            progress_manager,
            progress,
        }
    }
}

impl Drop for TaskProgressGuard {
    fn drop(&mut self) {
        // The entry is created along with `TaskProgress`, so it must exist.
        self.progress_manager
            .lock()
            .remove(&self.task_id)
            .expect("task progress should exist when task is finished");
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskProgressGuard, TaskProgressManagerRef};

    #[test]
    fn test_progress_removal() {
        let task_progress_manager = TaskProgressManagerRef::default();
        {
            let _guard = TaskProgressGuard::new(1, task_progress_manager.clone());
            assert_eq!(task_progress_manager.lock().len(), 1);
        }
        assert_eq!(task_progress_manager.lock().len(), 0);
    }
}
