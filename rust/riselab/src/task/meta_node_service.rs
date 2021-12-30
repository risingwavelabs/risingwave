use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};
use crate::MetaNodeConfig;

pub struct MetaNodeService {
    config: MetaNodeConfig,
}

impl MetaNodeService {
    pub fn new(config: MetaNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn meta_node(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(Path::new(&prefix_bin).join("meta-node")))
    }
}

impl Task for MetaNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.meta_node()?;
        cmd.arg("--log4rs-config")
            .arg(Path::new(&prefix_config).join("log4rs.yaml"))
            .arg("--host")
            .arg(format!("{}:{}", self.config.address, self.config.port))
            .arg("--dashboard-host")
            .arg(format!(
                "{}:{}",
                self.config.dashboard_address, self.config.dashboard_port
            ));

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
        }

        Ok(())
    }

    fn id(&self) -> String {
        format!("meta-node-{}", self.config.port)
    }
}
