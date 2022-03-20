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
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::{GrafanaConfig, GrafanaGen};

pub struct GrafanaService {
    config: GrafanaConfig,
    grafana_root: PathBuf,
}

impl GrafanaService {
    pub fn new(config: GrafanaConfig) -> Result<Self> {
        let grafana_root = Self::grafana_root_path()?;
        Ok(Self {
            config,
            grafana_root,
        })
    }

    fn grafana_root_path() -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("grafana"))
    }

    fn grafana_server_path(&self) -> Result<PathBuf> {
        Ok(self.grafana_root.join("bin").join("grafana-server"))
    }

    fn grafana(&self) -> Result<Command> {
        let mut command = Command::new(self.grafana_root.join("bin").join("grafana-server"));
        command.current_dir(self.grafana_root.join("bin"));
        Ok(command)
    }
}

impl Task for GrafanaService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.grafana_server_path()?;
        if !path.exists() {
            return Err(anyhow!("grafana-server binary not found in {:?}\nDid you enable monitoring feature in `./risedev configure`?", path));
        }

        std::fs::write(
            self.grafana_root.join("conf").join("custom.ini"),
            &GrafanaGen.gen_custom_ini(&self.config),
        )?;

        std::fs::write(
            self.grafana_root
                .join("conf")
                .join("provisioning")
                .join("datasources")
                .join("risedev-prometheus.yml"),
            &GrafanaGen.gen_datasource_yml(&self.config)?,
        )?;

        std::fs::write(
            self.grafana_root
                .join("conf")
                .join("provisioning")
                .join("dashboards")
                .join("risingwave-dashboards.yaml"),
            &GrafanaGen.gen_dashboard_yml(&self.config)?,
        )?;
        std::fs::write(
            self.grafana_root
                .join("conf")
                .join("provisioning")
                .join("dashboards")
                .join("aws-s3-dashboards.yaml"),
            &GrafanaGen.gen_s3_dashboard_yml(&self.config)?,
        )?;

        let cmd = self.grafana()?;

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
