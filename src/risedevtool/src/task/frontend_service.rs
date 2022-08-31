// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::FrontendConfig;

pub struct FrontendService {
    config: FrontendConfig,
}

impl FrontendService {
    pub fn new(config: FrontendConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn frontend(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(Path::new(&prefix_bin).join("risingwave").join("frontend-node")))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("frontend")))
        }
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &FrontendConfig) -> Result<()> {
        cmd.arg("--host")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--client-address")
            .arg(format!("{}:{}", config.address, config.port));

        let prefix_config = env::var("PREFIX_CONFIG")?;
        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"));

        let provide_meta_node = config.provide_meta_node.as_ref().unwrap();
        match provide_meta_node.len() {
            0 => {
                return Err(anyhow!(
                    "Cannot configure node: no meta node found in this configuration."
                ));
            }
            1 => {
                let meta_node = &provide_meta_node[0];
                cmd.arg("--meta-addr")
                    .arg(format!("http://{}:{}", meta_node.address, meta_node.port));
            }
            other_size => {
                return Err(anyhow!(
                    "Cannot configure node: {} meta nodes found in this configuration, but only 1 is needed.",
                    other_size
                ));
            }
        };

        Ok(())
    }
}

impl Task for FrontendService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.frontend()?;

        cmd.env("RUST_BACKTRACE", "1");
        Self::apply_command_args(&mut cmd, &self.config)?;

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
