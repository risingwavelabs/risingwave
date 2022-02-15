use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Result;

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
                .join("dashboards.yaml"),
            &GrafanaGen.gen_dashboard_yml(&self.config)?,
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
