// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    pub fn write_config_files(
        config: &GrafanaConfig,
        config_root: impl AsRef<Path>,
        prefix_config: impl AsRef<Path>,
    ) -> Result<()> {
        let config_root = config_root.as_ref();

        fs_err::write(
            config_root.join("custom.ini"),
            GrafanaGen.gen_custom_ini(config),
        )?;

        let config_datasources_dir = config_root.join("provisioning").join("datasources");
        fs_err::remove_dir_all(&config_datasources_dir)?;
        fs_err::create_dir_all(&config_datasources_dir)?;

        fs_err::write(
            config_datasources_dir.join("risedev-prometheus.yml"),
            GrafanaGen.gen_prometheus_datasource_yml(config)?,
        )?;

        if !config.provide_jaeger.as_ref().unwrap().is_empty() {
            fs_err::write(
                config_datasources_dir.join("risedev-jaeger.yml"),
                GrafanaGen.gen_jaeger_datasource_yml(config)?,
            )?;
        }

        let prefix_config = prefix_config.as_ref();
        let config_dashboards_dir = config_root.join("provisioning").join("dashboards");
        fs_err::remove_dir_all(&config_dashboards_dir)?;
        fs_err::create_dir_all(&config_dashboards_dir)?;
        fs_err::write(
            config_dashboards_dir.join("risingwave-dashboard.yaml"),
            GrafanaGen.gen_dashboard_yml(config, prefix_config, prefix_config)?,
        )?;
        // fs_err::write(
        //     config_dashboards_dir.join("aws-s3-dashboards.yaml"),
        //     &GrafanaGen.gen_s3_dashboard_yml(config, prefix_config)?,
        // )?;

        Ok(())
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

        Self::write_config_files(
            &self.config,
            self.grafana_root.join("conf"),
            env::var("PREFIX_CONFIG")?,
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
