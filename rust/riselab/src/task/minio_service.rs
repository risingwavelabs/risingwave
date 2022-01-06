use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};
use crate::MinioConfig;

pub struct MinioService {
    config: MinioConfig,
}

impl MinioService {
    pub fn new(config: MinioConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn minio(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(Path::new(&prefix_bin).join("minio")))
    }
}

impl Task for MinioService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.minio()?;

        let prefix_config = env::var("PREFIX_CONFIG")?;

        cmd.arg("server")
            .arg(env::var("HUMMOCK_PATH")?)
            .arg("--address")
            .arg(format!("{}:{}", self.config.address, self.config.port))
            .arg("--console-address")
            .arg(format!(
                "{}:{}",
                self.config.console_address, self.config.console_port
            ))
            .arg("--config-dir")
            .arg(Path::new(&prefix_config).join("minio"))
            .env("MINIO_ROOT_USER", &self.config.root_user)
            .env("MINIO_ROOT_PASSWORD", &self.config.root_password);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        "minio".to_string()
    }
}
