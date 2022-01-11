use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};
use crate::{PrometheusConfig, PrometheusGen};

pub struct PrometheusService {
    config: PrometheusConfig,
}

impl PrometheusService {
    pub fn new(config: PrometheusConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn prometheus(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(
            Path::new(&prefix_bin).join("prometheus").join("prometheus"),
        ))
    }
}

impl Task for PrometheusService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

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
