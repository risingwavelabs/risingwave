// Copyright 2026 RisingWave Labs
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

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, ensure};

use crate::wait::wait;
use crate::{CassandraConfig, ExecuteContext, Task};

pub struct CassandraReadyCheckTask {
    config: CassandraConfig,
}

impl CassandraReadyCheckTask {
    pub fn new(config: CassandraConfig) -> Self {
        Self { config }
    }

    fn command(&self) -> Command {
        let mut cmd = if self.config.user_managed {
            let mut cmd = Command::new(self.config.cqlsh.as_deref().unwrap_or("cqlsh"));
            cmd.arg(&self.config.address)
                .arg(self.config.port.to_string());
            cmd
        } else {
            let mut cmd = Command::new("docker");
            cmd.arg("exec")
                .arg(format!("risedev-{}", self.config.id))
                .args(["cqlsh", "127.0.0.1", "9042"]);
            cmd
        };
        cmd.args([
            "--connect-timeout=5",
            "--request-timeout=5",
            "-e",
            "SELECT release_version FROM system.local;",
        ]);
        cmd
    }
}

async fn check_cql(cmd: Command, timeout: Duration) -> Result<()> {
    let output = tokio::time::timeout(
        timeout,
        tokio::process::Command::from(cmd)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .context("Cassandra readiness command timed out")?
    .context("failed to run Cassandra readiness command")?;
    ensure!(
        output.status.success(),
        "Cassandra readiness query failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

impl Task for CassandraReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.pb.set_message("waiting for CQL readiness...");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        // Cassandra's JVM and storage initialization can exceed the usual 30-second wait.
        wait(
            || rt.block_on(check_cql(self.command(), Duration::from_secs(15))),
            &mut ctx.log,
            ctx.status_file.as_ref().unwrap(),
            &self.config.id,
            Some(Duration::from_secs(120)),
            !self.config.user_managed,
        )
        .with_context(|| {
            format!(
                "failed to wait for Cassandra service `{}` to be ready",
                self.config.id
            )
        })?;

        ctx.complete_spin();
        Ok(())
    }
}
