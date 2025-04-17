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

use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use console::style;

use crate::util::{risedev_cmd, stylized_risedev_subcmd};
use crate::{ExecuteContext, Task};

pub struct ConfigureTmuxTask {
    env: Vec<String>,
}

pub const RISEDEV_NAME: &str = "risedev";

pub fn new_tmux_command() -> Command {
    let mut cmd = Command::new("tmux");
    cmd.arg("-L").arg(RISEDEV_NAME); // `-L` specifies a dedicated tmux server
    cmd
}

impl ConfigureTmuxTask {
    pub fn new(env: Vec<String>) -> Result<Self> {
        Ok(Self { env })
    }
}

impl Task for ConfigureTmuxTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);

        ctx.pb.set_message("starting...");

        let prefix_path = env::var("PREFIX")?;
        let prefix_bin = env::var("PREFIX_BIN")?;

        let mut cmd = new_tmux_command();
        cmd.arg("-V");
        ctx.run_command(cmd).with_context(|| {
            format!(
                "Failed to execute {} command. Did you install tmux?",
                style("tmux").blue().bold()
            )
        })?;

        let mut cmd = new_tmux_command();
        cmd.arg("list-sessions");
        if ctx.run_command(cmd).is_ok() {
            ctx.pb.set_message("killing previous session...");

            let mut cmd = Command::new(risedev_cmd());
            cmd.arg("k");
            ctx.run_command(cmd).with_context(|| {
                format!(
                    "A previous cluster is already running while `risedev-dev` failed to kill it. \
                     Please kill it manually with {}.",
                    stylized_risedev_subcmd("k")
                )
            })?;
        }

        ctx.pb.set_message("creating new session...");

        let mut cmd = new_tmux_command();
        cmd.arg("new-session") // this will automatically create the `risedev` tmux server
            .arg("-d")
            .arg("-s")
            .arg(RISEDEV_NAME);
        for e in &self.env {
            cmd.arg("-e").arg(e);
        }
        cmd.arg("-c")
            .arg(Path::new(&prefix_path))
            .arg(Path::new(&prefix_bin).join("welcome.sh"));

        ctx.run_command(cmd)?;

        ctx.complete_spin();

        ctx.pb.set_message(format!("session {}", RISEDEV_NAME));

        Ok(())
    }

    fn id(&self) -> String {
        "tmux-configure".into()
    }
}
