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

pub use self::event_loop::start_iceberg_compactor;
pub use self::iceberg_compactor_runner::create_task_execution;
pub(crate) use self::queue::{IcebergTaskMeta, IcebergTaskQueue, PushResult};
pub(crate) use self::report::{
    IcebergPlanCompletion, IcebergTaskReport, IcebergTaskTracker, ReportSendResult,
    build_iceberg_task_report, flush_pending_iceberg_task_reports,
    send_or_buffer_iceberg_task_report,
};

mod event_loop;
pub(crate) mod iceberg_compactor_runner;
mod queue;
pub(crate) mod report;

/// Unique key combining `(task_id, plan_index)` since one task can have multiple plans.
pub(crate) type TaskKey = (u64, usize);
