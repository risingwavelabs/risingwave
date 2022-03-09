use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::FrontendConfig;

pub struct FrontendServiceV2 {
    config: FrontendConfig,
}

impl FrontendServiceV2 {
    pub fn new(config: FrontendConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn frontend_v2(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(Path::new(&prefix_bin).join("risingwave").join("frontend-v2")))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("frontend-v2")))
        }
    }
}

impl Task for FrontendServiceV2 {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.frontend_v2()?;

        cmd.arg("--host")
            .arg(format!("{}:{}", self.config.address, self.config.port));

        let provide_meta_node = self.config.provide_meta_node.as_ref().unwrap();
        match provide_meta_node.len() {
            0 => {
                return Err(anyhow!(
                    "Cannot start node: no meta node found in this configuration."
                ));
            }
            1 => {
                let meta_node = &provide_meta_node[0];
                cmd.arg("--meta-addr")
                    .arg(format!("http://{}:{}", meta_node.address, meta_node.port));
            }
            other_size => {
                return Err(anyhow!(
                    "Cannot start node: {} meta nodes found in this configuration, but only 1 is needed.",
                    other_size
                ));
            }
        };

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
