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

mod compaction_manager;
mod compaction_task_manager;
mod compactor_backend;

use std::time::Instant;

pub use compaction_manager::CompactionManager;
pub use compaction_task_manager::CompactionTaskManager;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactTask;

pub enum CompactionTaskEvent {
    Register(CompactionGroupId, CompactTask, u64),
    TaskProgress(CompactTask),
    Cancel(CompactTask),
    Timer(u64, Instant),

    // TODO add event for risectl
    // ListCompactionTask
    // GetCompactionTask
}
