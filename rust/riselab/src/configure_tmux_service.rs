use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use indicatif::ProgressBar;

use crate::util::{pb_success, run_command};

pub struct ConfigureTmuxTask;

pub const RISELAB_SESSION_NAME: &str = "riselab";

impl ConfigureTmuxTask {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    fn tmux(&self) -> Command {
        Command::new("tmux")
    }

    pub fn execute(&mut self, f: &mut impl std::io::Write, pb: ProgressBar) -> Result<()> {
        pb.set_message("starting tmux...");

        let prefix_path = env::var("PREFIX")?;

        let mut cmd = self.tmux();
        cmd.arg("kill-session").arg("-t").arg(RISELAB_SESSION_NAME);
        run_command(cmd, f).ok();

        let mut cmd = self.tmux();
        cmd.arg("new-session")
            .arg("-d")
            .arg("-s")
            .arg(RISELAB_SESSION_NAME)
            .arg("-c")
            .arg(Path::new(&prefix_path));
        run_command(cmd, f)?;

        pb_success(&pb);

        pb.set_message(format!(
            "session {} started successfully",
            RISELAB_SESSION_NAME
        ));

        Ok(())
    }
}
