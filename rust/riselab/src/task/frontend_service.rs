use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};
use crate::{FrontendConfig, FrontendGen};

pub struct FrontendService {
    config: FrontendConfig,
}

impl FrontendService {
    pub fn new(config: FrontendConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn frontend(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        let prefix_config = env::var("PREFIX_CONFIG")?;

        std::fs::write(
            Path::new(&prefix_config).join("server.properties"),
            &FrontendGen.gen_server_properties(&self.config),
        )?;

        let mut cmd = Command::new("java");
        cmd.arg("-cp")
            .arg(Path::new(&prefix_bin).join("risingwave-fe-runnable.jar"))
            // Enable JRE remote debugging functionality
            .arg("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005")
            .arg(format!(
                "-Dlogback.configurationFile={}",
                Path::new(&prefix_config)
                    .join("logback.xml")
                    .to_string_lossy()
            ))
            .arg("com.risingwave.pgserver.FrontendServer")
            .arg("-c")
            .arg(Path::new(&prefix_config).join("server.properties"));
        Ok(cmd)
    }
}

impl Task for FrontendService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let cmd = self.frontend()?;

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
        }

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
