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

use anyhow::{Context, Result};

use super::{ExecuteContext, Task};

/// Check if a log pattern is found in the log output indicating the service is ready.
pub struct LogReadyCheckTask {
    pattern: String,
}

impl LogReadyCheckTask {
    pub fn new(pattern: impl Into<String>) -> Result<Self> {
        Ok(Self {
            pattern: pattern.into(),
        })
    }
}

impl Task for LogReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        let Some(id) = ctx.id.clone() else {
            panic!("Service should be set before executing LogReadyCheckTask");
        };

        ctx.pb.set_message("waiting for ready...");
        ctx.wait_log_contains(&self.pattern)
            .with_context(|| format!("failed to wait for service `{id}` to be ready"))?;

        ctx.complete_spin();

        Ok(())
    }
}
