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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_pb::task_service::PbTaskStats;

pub type TaskStatsRef = Arc<TaskStats>;

pub struct TaskStats {
    pub row_scan_count: AtomicU64,
}

impl TaskStats {
    pub fn new() -> Self {
        Self {
            row_scan_count: AtomicU64::new(0),
        }
    }
}

impl From<&TaskStats> for PbTaskStats {
    fn from(t: &TaskStats) -> Self {
        Self {
            row_scan_count: t.row_scan_count.load(Ordering::Relaxed),
        }
    }
}
