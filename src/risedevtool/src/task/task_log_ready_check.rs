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

use std::io::{Read as _, Seek as _, SeekFrom};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use fs_err::File;

use super::{ExecuteContext, Task};
use crate::wait::wait;

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

impl<W> ExecuteContext<W>
where
    W: std::io::Write,
{
    fn wait_log_contains(&mut self, pattern: impl AsRef<str>) -> anyhow::Result<()> {
        let pattern = pattern.as_ref();
        let log_path = self.log_path().to_path_buf();

        let mut content = String::new();
        let mut offset = 0;

        wait(
            || {
                let mut file = File::open(&log_path).context("log file does not exist")?;
                file.seek(SeekFrom::Start(offset as u64))?;
                offset += file.read_to_string(&mut content)?;

                // Always going through the whole log file could be stupid, but it's reliable.
                if content.contains(pattern) {
                    Ok(())
                } else {
                    bail!("pattern \"{}\" not found in log", pattern)
                }
            },
            &mut self.log,
            self.status_file.as_ref().unwrap(),
            self.id.as_ref().unwrap(),
            Some(Duration::from_secs(30)),
            true,
        )?;

        Ok(())
    }
}
