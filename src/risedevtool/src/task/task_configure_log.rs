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

pub struct ConfigureLogTask {
    pattern: String,
    user_managed: bool,
}

impl ConfigureLogTask {
    pub fn new(pattern: impl Into<String>, user_managed: bool) -> Result<Self> {
        Ok(Self {
            pattern: pattern.into(),
            user_managed,
        })
    }
}

impl Task for ConfigureLogTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        let Some(id) = ctx.id.clone() else {
            panic!("Service should be set before executing ConfigureLogTask");
        };

        if self.user_managed {
            ctx.pb.set_message(
                "waiting for user-managed service ready... (see `risedev.log` for cli args)",
            );
            ctx.wait_log_include(&self.pattern).with_context(|| {
                format!("failed to wait for user-managed service `{id}` to be ready")
            })?;
        } else {
            ctx.pb.set_message("waiting for ready...");
            ctx.wait_log_include(&self.pattern)
                .with_context(|| format!("failed to wait for service `{id}` to be ready"))?;
        }

        ctx.complete_spin();

        Ok(())
    }
}
