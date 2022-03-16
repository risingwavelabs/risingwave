use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::{PrometheusConfig, PrometheusGen};

pub struct PrometheusService {
    config: PrometheusConfig,
}

impl PrometheusService {
    pub fn new(config: PrometheusConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn prometheus_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("prometheus").join("prometheus"))
    }

    fn prometheus(&self) -> Result<Command> {
        Ok(Command::new(self.prometheus_path()?))
    }
}

impl Task for PrometheusService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.prometheus_path()?;
        if !path.exists() {
            return Err(anyhow!("prometheus binary not found in {:?}\nDid you enable monitoring feature in `./risedev configure`?", path));
        }

        let prefix_config = env::var("PREFIX_CONFIG")?;
        let prefix_data = env::var("PREFIX_DATA")?;

        std::fs::write(
            Path::new(&prefix_config).join("prometheus.yml"),
            &PrometheusGen.gen_prometheus_yml(&self.config),
        )?;

        let mut cmd = self.prometheus()?;

        cmd.arg(format!(
            "--web.listen-address={}:{}",
            self.config.address, self.config.port
        ))
        .arg(format!(
            "--config.file={}",
            Path::new(&prefix_config)
                .join("prometheus.yml")
                .to_string_lossy()
        ))
        .arg(format!(
            "--storage.tsdb.path={}",
            Path::new(&prefix_data).join("prometheus").to_string_lossy()
        ));

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
