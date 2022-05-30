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

use std::sync::Arc;

use crate::session::SessionImpl;

mod distributed;
mod hummock_snapshot_manager;
pub use hummock_snapshot_manager::*;
pub mod plan_fragmenter;
mod query_manager;
pub use query_manager::*;
mod local;
pub mod worker_node_manager;
pub use local::*;
mod task_context;

/// Context for mpp query execution.
pub struct ExecutionContext {
    session: Arc<SessionImpl>,
}

pub type ExecutionContextRef = Arc<ExecutionContext>;

impl ExecutionContext {
    pub fn new(session: Arc<SessionImpl>) -> Self {
        Self { session }
    }

    pub fn session(&self) -> &SessionImpl {
        &self.session
    }
}
