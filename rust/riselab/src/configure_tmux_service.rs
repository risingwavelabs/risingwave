use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

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

        let mut cmd = self.tmux();
        cmd.arg("-V");
        ctx.run_command(cmd)?;

        let mut cmd = self.tmux();
        cmd.arg("kill-session").arg("-t").arg(RISELAB_SESSION_NAME);
        ctx.run_command(cmd).ok();

        let mut cmd = self.tmux();
        cmd.arg("new-session")
            .arg("-d")
            .arg("-s")
            .arg(RISELAB_SESSION_NAME)
            .arg("-c")
            .arg(Path::new(&prefix_path));
        ctx.run_command(cmd)?;

        ctx.complete_spin();

        ctx.pb.set_message(format!(
            "session {} started successfully",
            RISELAB_SESSION_NAME
        ));

        Ok(())
    }

    fn id(&self) -> String {
        "tmux".into()
    }
}
