use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::ComputeNodeConfig;

pub struct ComputeNodeService {
    config: ComputeNodeConfig,
}

impl ComputeNodeService {
    pub fn new(config: ComputeNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn compute_node(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(Path::new(&prefix_bin).join("compute-node")))
    }
}

impl Task for ComputeNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.compute_node()?;

        cmd.arg("--log4rs-config")
            .arg(Path::new(&prefix_config).join("log4rs.yaml"))
            .arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"))
            .arg("--host")
            .arg(format!("{}:{}", self.config.address, self.config.port))
            .arg("--prometheus-listener-addr")
            .arg(format!(
                "{}:{}",
                self.config.exporter_address, self.config.exporter_port
            ))
            .arg("--metrics-level")
            .arg("1");

        let provide_minio = self.config.provide_minio.as_ref().unwrap();
        match provide_minio.len() {
            0 => {}
            1 => {
                let minio = &provide_minio[0];
                cmd.arg("--state-store").arg(format!(
                    "hummock+minio://{}:{}@{minio_addr}:{minio_port}/{}",
                    env::var("MINIO_HUMMOCK_USER")?,
                    env::var("MINIO_HUMMOCK_PASSWORD")?,
                    env::var("MINIO_BUCKET_NAME")?,
                    minio_addr = minio.address,
                    minio_port = minio.port,
                ));
            }
            other_size => {
                return Err(anyhow!(
                    "{} minio instance found in config, but only 1 is needed",
                    other_size
                ))
            }
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
        format!("compute-node-{}", self.config.port)
    }
}
