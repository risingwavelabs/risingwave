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
//
use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

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

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(Path::new(&prefix_bin).join("risingwave").join("meta-node")))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("meta-node")))
        }
    }
}

impl Task for MetaNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.meta_node()?;

        cmd.env("RUST_BACKTRACE", "1");

        cmd.arg("--host")
            .arg(format!("{}:{}", self.config.address, self.config.port))
            .arg("--dashboard-host")
            .arg(format!(
                "{}:{}",
                self.config.dashboard_address, self.config.dashboard_port
            ));

        cmd.arg("--prometheus-host").arg(format!(
            "{}:{}",
            self.config.exporter_address, self.config.exporter_port
        ));

        match self.config.provide_etcd_backend.as_ref().map(|v| &v[..]) {
            Some([]) => {
                cmd.arg("--backend").arg("mem");
            }
            Some([etcd]) => {
                cmd.arg("--backend")
                    .arg("etcd")
                    .arg("--etcd-endpoints")
                    .arg(format!("{}:{}", etcd.address, etcd.port));
            }
            _ => {
                return Err(anyhow!(
                    "unexpected etcd config {:?}",
                    self.config.provide_etcd_backend
                ))
            }
        }

        if self.config.enable_dashboard_v2 {
            cmd.arg("--dashboard-ui-path").arg(env::var("PREFIX_UI")?);
        }

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
