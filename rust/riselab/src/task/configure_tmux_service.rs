use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

use crate::{ExecuteContext, Task};

pub struct ConfigureTmuxTask;

pub const RISELAB_SESSION_NAME: &str = "riselab";

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
        ctx.run_command(cmd)?;

        // List previous windows and kill them
        let mut cmd = self.tmux();
        cmd.arg("list-windows")
            .arg("-t")
            .arg(RISELAB_SESSION_NAME)
            .arg("-F")
            .arg("#{pane_pid} #{window_name}");
        if let Ok(output) = ctx.run_command(cmd) {
            for line in String::from_utf8(output.stdout)?.split('\n') {
                if line.trim().is_empty() {
                    continue;
                }
                let (pid, name) = line
                    .split_once(' ')
                    .ok_or_else(|| anyhow!("failed to parse tmux list-windows output"))?;
                let mut cmd = Command::new("kill");
                ctx.pb.set_message(format!("killing {} {}...", pid, name));
                cmd.arg("-SIGINT");
                cmd.arg(format!("-{}", pid));
                ctx.run_command(cmd)?;
            }
        }

        let mut cmd = self.tmux();
        cmd.arg("kill-session").arg("-t").arg(RISELAB_SESSION_NAME);
        ctx.run_command(cmd).ok();

        ctx.pb.set_message("creating new session...");

        let mut cmd = self.tmux();
        cmd.arg("new-session")
            .arg("-d")
            .arg("-s")
            .arg(RISELAB_SESSION_NAME)
            .arg("-c")
            .arg(Path::new(&prefix_path))
            .arg(Path::new(&prefix_bin).join("welcome.sh"));

        ctx.run_command(cmd)?;

        ctx.complete_spin();

        ctx.pb
            .set_message(format!("session {}", RISELAB_SESSION_NAME));

        Ok(())
    }

    fn id(&self) -> String {
        "tmux".into()
    }
}
