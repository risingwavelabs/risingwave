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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;
use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::CompactTaskProgress;

pub type TaskProgressManagerRef = Arc<Mutex<HashMap<HummockCompactionTaskId, Arc<TaskProgress>>>>;

/// The progress of a compaction task.
#[derive(Default)]
pub struct TaskProgress {
    num_ssts_sealed: AtomicU32,
    num_ssts_uploaded: AtomicU32,
    num_progress_key: AtomicU64,
    num_pending_read_io: AtomicUsize,
    num_pending_write_io: AtomicUsize,
}

impl TaskProgress {
    pub fn inc_ssts_sealed(&self) {
        self.num_ssts_sealed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ssts_uploaded(&self) {
        self.num_ssts_uploaded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_progress_key(&self, inc_key_num: u64) {
        self.num_progress_key
            .fetch_add(inc_key_num, Ordering::Relaxed);
    }

    pub fn inc_num_pending_read_io(&self) {
        self.num_pending_read_io.fetch_add(1, Ordering::SeqCst);
    }

    pub fn inc_num_pending_write_io(&self) {
        self.num_pending_write_io.fetch_add(1, Ordering::SeqCst);
    }

    pub fn dec_num_pending_read_io(&self) {
        self.num_pending_read_io.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn dec_num_pending_write_io(&self) {
        self.num_pending_write_io.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn snapshot(&self, task_id: u64) -> CompactTaskProgress {
        CompactTaskProgress {
            task_id,
            num_ssts_sealed: self.num_ssts_sealed.load(Ordering::Relaxed),
            num_ssts_uploaded: self.num_ssts_uploaded.load(Ordering::Relaxed),
            num_pending_read_io: self.num_pending_read_io.load(Ordering::Relaxed) as u64,
            num_pending_write_io: self.num_pending_write_io.load(Ordering::Relaxed) as u64,
            num_progress_key: self.num_progress_key.load(Ordering::Relaxed),
            ..Default::default()
        }
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
