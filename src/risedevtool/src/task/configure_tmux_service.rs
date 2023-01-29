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

use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use console::style;
use itertools::Itertools;

use crate::{ExecuteContext, Task};

pub struct ConfigureTmuxTask;

pub const RISEDEV_SESSION_NAME: &str = "risedev";

impl ConfigureTmuxTask {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    fn tmux(&self) -> Command {
        Command::new("tmux")
    }
}

impl Task for ConfigureTmuxTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);

        ctx.pb.set_message("starting...");

        let prefix_path = env::var("PREFIX")?;
        let prefix_bin = env::var("PREFIX_BIN")?;

        let mut cmd = self.tmux();
        cmd.arg("-V");
        ctx.run_command(cmd).with_context(|| {
            format!(
                "Failed to execute {} command. Did you install tmux?",
                style("tmux").blue().bold()
            )
        })?;

        // List previous windows and kill them
        let mut cmd = self.tmux();
        cmd.arg("list-windows")
            .arg("-t")
            .arg(RISEDEV_SESSION_NAME)
            .arg("-F")
            .arg("#{pane_id} #{pane_pid} #{window_name}");

        if let Ok(output) = ctx.run_command(cmd) {
            for line in String::from_utf8(output.stdout)?.split('\n') {
                if line.trim().is_empty() {
                    continue;
                }
                let [pane_id, pid, name, ..] = line.split(' ').collect_vec()[..] else {
                    return Err(anyhow!("failed to parse tmux list-windows output"));
                };
                ctx.pb
                    .set_message(format!("killing {} {} {}...", pane_id, pid, name));

                // Send ^C & ^D
                let mut cmd = self.tmux();
                cmd.arg("send-keys")
                    .arg("-t")
                    .arg(pane_id)
                    .arg("C-c")
                    .arg("C-d");
                ctx.run_command(cmd)?;
            }
        }

        let mut cmd = self.tmux();
        cmd.arg("kill-session").arg("-t").arg(RISEDEV_SESSION_NAME);
        ctx.run_command(cmd).ok();

        ctx.pb.set_message("creating new session...");

        let mut cmd = self.tmux();
        cmd.arg("new-session")
            .arg("-d")
            .arg("-s")
            .arg(RISEDEV_SESSION_NAME)
            .arg("-c")
            .arg(Path::new(&prefix_path))
            .arg(Path::new(&prefix_bin).join("welcome.sh"));

        ctx.run_command(cmd)?;

        ctx.complete_spin();

        ctx.pb
            .set_message(format!("session {}", RISEDEV_SESSION_NAME));

        Ok(())
    }

    fn id(&self) -> String {
        "tmux".into()
    }
}
