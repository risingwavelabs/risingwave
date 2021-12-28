use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use indicatif::ProgressBar;

use crate::util::{run_command, tmux_run};

#[derive(Default)]
pub struct FrontendService;

impl FrontendService {
    pub fn new() -> Result<Self> {
        Ok(Self::default())
    }

    fn frontend(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        let prefix_conf = env::var("PREFIX_CONFIG")?;
        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = Command::new("java");
        cmd.arg("-cp")
            .arg(Path::new(&prefix_bin).join("risingwave-fe-runnable.jar"))
            .arg("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5005")
            .arg(format!(
                "-Dlogback.configurationFile={}",
                Path::new(&prefix_conf)
                    .join("logback.xml")
                    .to_string_lossy()
            ))
            .arg("com.risingwave.pgserver.FrontendServer")
            .arg("-c")
            .arg(Path::new(&prefix_config).join("server.properties"));
        Ok(cmd)
    }

    pub fn execute(&mut self, f: &mut impl std::io::Write, pb: ProgressBar) -> Result<()> {
        pb.enable_steady_tick(100);
        pb.set_message("starting...");

        let cmd = self.frontend()?;
        run_command(tmux_run(cmd)?, f)?;

        pb.set_message("started");

        Ok(())
    }

    pub fn id(&self) -> String {
        "frontend".into()
    }
}
